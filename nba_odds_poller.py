#!/usr/bin/env python3
"""
NBA Odds Poller
Fetches NBA betting lines from The Odds API and appends to Google Sheets.
Designed to run hourly via GitHub Actions.
Only writes new rows if the value changed or 4+ hours have passed.
"""

import requests
import os
import json
import base64
from datetime import datetime, timedelta

import gspread
from google.oauth2.service_account import Credentials

# ── Config ──────────────────────────────────────────────────────────────────
API_KEY = os.environ.get("ODDS_API_KEY", "")
GOOGLE_SHEET_ID = "1LzkU7rH3OtrJckV5oMvFHyuLAnbRn9E74FO1uyfM65k"
WORKSHEET_NAME = "nba_odds"

# Filter to specific books, or leave empty [] for all available
BOOKMAKERS = ["betonlineag"]

# Only write a new row if value changed OR this many hours have passed
HOURS_BEFORE_FORCE_UPDATE = 4

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


def load_bet_history(worksheet):
    """
    Return dict of {bet_key: (timestamp, price, point)} for the most recent entry of each bet.
    bet_key = (bookmaker, home_team, away_team, market, team_or_side)
    """
    all_values = worksheet.get_all_values()
    
    if len(all_values) <= 1:  # Only header or empty
        return {}
    
    header = all_values[0]
    data_rows = all_values[1:]
    
    if not data_rows:
        return {}
    
    # Build index mapping
    idx = {name: header.index(name) for name in FIELDNAMES if name in header}
    
    history = {}
    for row in data_rows:
        try:
            key = (
                row[idx["bookmaker"]],
                row[idx["home_team"]],
                row[idx["away_team"]],
                row[idx["market"]],
                row[idx["team_or_side"]],
            )
            timestamp_str = row[idx["timestamp"]]
            price = float(row[idx["price"]])
            point_str = row[idx["point"]]
            point = float(point_str) if point_str else None
            
            # Always keep the most recent entry (rows are in chronological order)
            history[key] = (timestamp_str, price, point)
        except (ValueError, IndexError, KeyError):
            continue
    
    return history


def filter_rows_to_write(new_rows, history, current_timestamp):
    """
    Filter rows to only include those where:
    - It's a new bet (not in history)
    - The price or point changed
    - 4+ hours have passed since the last entry
    """
    rows_to_write = []
    current_dt = datetime.strptime(current_timestamp, "%Y-%m-%d %H:%M:%S")
    
    for row in new_rows:
        key = (
            row["bookmaker"],
            row["home_team"],
            row["away_team"],
            row["market"],
            row["team_or_side"],
        )
        
        try:
            new_price = float(row["price"])
            new_point = float(row["point"]) if row["point"] else None
        except (ValueError, TypeError):
            new_price = row["price"]
            new_point = row.get("point")
        
        if key not in history:
            # New bet, always write
            rows_to_write.append(row)
            continue
        
        last_ts_str, last_price, last_point = history[key]
        
        # Check if value changed
        value_changed = (new_price != last_price) or (new_point != last_point)
        
        # Check if 4+ hours passed
        try:
            last_dt = datetime.strptime(last_ts_str, "%Y-%m-%d %H:%M:%S")
            hours_passed = (current_dt - last_dt).total_seconds() / 3600
            time_threshold_met = hours_passed >= HOURS_BEFORE_FORCE_UPDATE
        except ValueError:
            time_threshold_met = True  # If we can't parse, write anyway
        
        if value_changed or time_threshold_met:
            rows_to_write.append(row)
    
    return rows_to_write


def load_last_snapshot_for_alerts(history):
    """Convert history to snapshot format for alert checking."""
    snapshot = {}
    for key, (ts, price, point) in history.items():
        # Convert to old key format: (bookmaker, game_key, market, team)
        bookmaker, home, away, market, team = key
        old_key = (bookmaker, f"{home} vs {away}", market, team)
        snapshot[old_key] = (price, point)
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
        history = load_bet_history(worksheet)
        prev_snapshot = load_last_snapshot_for_alerts(history)
        
        data, remaining, used = fetch_odds()
        rows = parse_rows(data, timestamp)

        if not rows:
            print("  No lines returned (games may not have opened yet).")
            return

        check_movements(rows, prev_snapshot)
        
        # Filter to only rows that need to be written
        rows_to_write = filter_rows_to_write(rows, history, timestamp)
        
        if rows_to_write:
            write_rows_to_sheet(worksheet, rows_to_write)
            games = len(set((r["home_team"], r["away_team"]) for r in rows_to_write))
            print(f"  ✅ Logged {len(rows_to_write)} rows across {games} games → Google Sheet")
        else:
            print("  ℹ️  No changes detected, nothing written.")
        
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
