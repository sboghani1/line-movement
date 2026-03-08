#!/usr/bin/env python3
"""
NBA Odds Poller
Fetches NBA betting lines from The Odds API and appends to Google Sheets.
Runs every 3 hours via GitHub Actions.
"""

import requests
import os
import json
import base64
from datetime import datetime

from dotenv import load_dotenv
load_dotenv()

import gspread
from google.oauth2.service_account import Credentials

# ── Config ──────────────────────────────────────────────────────────────────
API_KEY = os.environ.get("ODDS_API_KEY", "")
GOOGLE_SHEET_ID = "1LzkU7rH3OtrJckV5oMvFHyuLAnbRn9E74FO1uyfM65k"
WORKSHEET_NAME = "nba_odds"

# Filter to specific books, or leave empty [] for all available
BOOKMAKERS = ["betonlineag"]

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


# ── Main poll function ────────────────────────────────────────────────────────
def poll():
    timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    print(f"\n[{timestamp}] Polling...")

    try:
        worksheet = get_worksheet()
        data, remaining, used = fetch_odds()
        rows = parse_rows(data, timestamp)

        if not rows:
            print("  No lines returned (games may not have opened yet).")
            return

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
    
    poll()
