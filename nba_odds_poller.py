#!/usr/bin/env python3
"""
NBA Odds Poller
Fetches NBA betting lines from The Odds API and appends to Google Sheets.
Designed to run hourly via GitHub Actions.
"""

import requests
import os
import json
import base64
from datetime import datetime

import gspread
from google.oauth2.service_account import Credentials

# ── Config ──────────────────────────────────────────────────────────────────
API_KEY = os.environ.get("ODDS_API_KEY", "")
GOOGLE_SHEET_ID = "1LzkU7rH3OtrJckV5oMvFHyuLAnbRn9E74FO1uyfM65k"
WORKSHEET_NAME = "nba_odds"

# Filter to specific books, or leave empty [] for all available
BOOKMAKERS = ["betonlineag"]

# Alert thresholds — set to None to disable
SPREAD_MOVE_ALERT = 1.5    # alert if spread moves by this many points
TOTAL_MOVE_ALERT  = 2.0    # alert if total moves by this many points
ML_MOVE_ALERT     = 20     # alert if moneyline moves by this many American odds points

FIELDNAMES = [
    "timestamp", "commence_time", "home_team", "away_team",
    "bookmaker", "market", "team_or_side", "price", "point"
]

# ── Google Sheets Setup ──────────────────────────────────────────────────────
def get_gspread_client():
    """Authenticate with Google Sheets using service account credentials."""
    creds_b64 = os.environ.get("GOOGLE_CREDENTIALS", "")
    if not creds_b64:
        raise ValueError("GOOGLE_CREDENTIALS environment variable not set")
    
    creds_json = base64.b64decode(creds_b64).decode("utf-8")
    creds_dict = json.loads(creds_json)
    
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]
    credentials = Credentials.from_service_account_info(creds_dict, scopes=scopes)
    return gspread.authorize(credentials)


def get_worksheet():
    """Get or create the worksheet for storing odds data."""
    client = get_gspread_client()
    spreadsheet = client.open_by_key(GOOGLE_SHEET_ID)
    
    try:
        worksheet = spreadsheet.worksheet(WORKSHEET_NAME)
    except gspread.WorksheetNotFound:
        worksheet = spreadsheet.add_worksheet(title=WORKSHEET_NAME, rows=1000, cols=len(FIELDNAMES))
        worksheet.append_row(FIELDNAMES)  # Add header row
    
    return worksheet


# ── Helpers ──────────────────────────────────────────────────────────────────
def fetch_odds():
    params = {
        "apiKey": API_KEY,
        "regions": "us",
        "markets": "h2h,spreads,totals",
        "oddsFormat": "american",
    }
    if BOOKMAKERS:
        params["bookmakers"] = ",".join(BOOKMAKERS)

    r = requests.get(
        "https://api.the-odds-api.com/v4/sports/basketball_nba/odds/",
        params=params,
        timeout=15
    )
    r.raise_for_status()
    remaining = r.headers.get("x-requests-remaining", "?")
    used = r.headers.get("x-requests-used", "?")
    return r.json(), remaining, used


def parse_rows(data, timestamp):
    rows = []
    for game in data:
        home = game["home_team"]
        away = game["away_team"]
        commence = game["commence_time"]
        for bm in game.get("bookmakers", []):
            for market in bm["markets"]:
                for outcome in market["outcomes"]:
                    rows.append({
                        "timestamp":     timestamp,
                        "commence_time": commence,
                        "home_team":     home,
                        "away_team":     away,
                        "bookmaker":     bm["key"],
                        "market":        market["key"],
                        "team_or_side":  outcome["name"],
                        "price":         outcome["price"],
                        "point":         outcome.get("point", ""),
                    })
    return rows


def write_rows_to_sheet(worksheet, rows):
    """Append rows to the Google Sheet."""
    if not rows:
        return
    
    # Convert dicts to lists in fieldname order
    row_values = []
    for row in rows:
        row_values.append([row.get(field, "") for field in FIELDNAMES])
    
    worksheet.append_rows(row_values, value_input_option="USER_ENTERED")


def load_last_snapshot(worksheet):
    """Return dict of {(bookmaker, game_key, market, team): (price, point)} from last poll."""
    all_values = worksheet.get_all_values()
    
    if len(all_values) <= 1:  # Only header or empty
        return {}
    
    header = all_values[0]
    data_rows = all_values[1:]
    
    if not data_rows:
        return {}
    
    # Find the last timestamp
    ts_idx = header.index("timestamp") if "timestamp" in header else 0
    last_ts = data_rows[-1][ts_idx]
    
    snapshot = {}
    for row in data_rows:
        if row[ts_idx] == last_ts:
            row_dict = dict(zip(header, row))
            key = (
                row_dict["bookmaker"],
                row_dict["home_team"] + " vs " + row_dict["away_team"],
                row_dict["market"],
                row_dict["team_or_side"],
            )
            try:
                price = float(row_dict["price"])
                point = float(row_dict["point"]) if row_dict["point"] else None
                snapshot[key] = (price, point)
            except ValueError:
                pass
    
    return snapshot


def check_movements(new_rows, prev_snapshot):
    if not prev_snapshot:
        return
    alerts = []
    for row in new_rows:
        key = (
            row["bookmaker"],
            row["home_team"] + " vs " + row["away_team"],
            row["market"],
            row["team_or_side"],
        )
        if key not in prev_snapshot:
            continue
        old_price, old_point = prev_snapshot[key]
        try:
            new_price = float(row["price"])
            new_point = float(row["point"]) if row["point"] else None
        except (ValueError, TypeError):
            continue

        market = row["market"]
        game = f"{row['away_team']} @ {row['home_team']}"
        book = row["bookmaker"]
        side = row["team_or_side"]

        if market == "spreads" and SPREAD_MOVE_ALERT and new_point is not None and old_point is not None:
            if abs(new_point - old_point) >= SPREAD_MOVE_ALERT:
                alerts.append(
                    f"  🔔 SPREAD MOVE [{book}] {game} | {side}: "
                    f"{old_point:+.1f} → {new_point:+.1f} "
                    f"(Δ{new_point - old_point:+.1f})"
                )
        elif market == "totals" and TOTAL_MOVE_ALERT and new_point is not None and old_point is not None:
            if abs(new_point - old_point) >= TOTAL_MOVE_ALERT:
                alerts.append(
                    f"  🔔 TOTAL MOVE  [{book}] {game} | {side}: "
                    f"{old_point:.1f} → {new_point:.1f} "
                    f"(Δ{new_point - old_point:+.1f})"
                )
        elif market == "h2h" and ML_MOVE_ALERT:
            if abs(new_price - old_price) >= ML_MOVE_ALERT:
                alerts.append(
                    f"  🔔 ML MOVE     [{book}] {game} | {side}: "
                    f"{old_price:+.0f} → {new_price:+.0f} "
                    f"(Δ{new_price - old_price:+.0f})"
                )

    if alerts:
        print("\n" + "="*60)
        print("  ⚡ LINE MOVEMENT DETECTED")
        print("="*60)
        for a in alerts:
            print(a)
        print("="*60 + "\n")
    else:
        print("  No significant line movements detected.")


# ── Main poll function ────────────────────────────────────────────────────────
def poll():
    timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    print(f"\n[{timestamp}] Polling...")

    try:
        worksheet = get_worksheet()
        prev_snapshot = load_last_snapshot(worksheet)
        data, remaining, used = fetch_odds()
        rows = parse_rows(data, timestamp)

        if not rows:
            print("  No lines returned (games may not have opened yet).")
            return

        check_movements(rows, prev_snapshot)
        write_rows_to_sheet(worksheet, rows)

        games = len(set((r["home_team"], r["away_team"]) for r in rows))
        print(f"  ✅ Logged {len(rows)} rows across {games} games → Google Sheet")
        print(f"  API usage: {used} used, {remaining} remaining this month")

    except requests.HTTPError as e:
        print(f"  ❌ HTTP error: {e}")
        raise
    except Exception as e:
        print(f"  ❌ Error: {e}")
        raise


# ── Entry point ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("NBA Odds Poller - Single Run")
    print(f"Google Sheet ID: {GOOGLE_SHEET_ID}")
    print(f"Alert thresholds → Spread: {SPREAD_MOVE_ALERT} pts | Total: {TOTAL_MOVE_ALERT} pts | ML: {ML_MOVE_ALERT}")
    
    poll()
