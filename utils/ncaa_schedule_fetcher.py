#!/usr/bin/env python3
"""
╔═══════════════════════════════════════════════════════════════════════════════╗
║  ONE-TIME UTILITY: NCAA Basketball Schedule Fetcher                           ║
╠═══════════════════════════════════════════════════════════════════════════════╣
║                                                                               ║
║  PURPOSE:                                                                     ║
║  Backfill NCAA college basketball schedules from ESPN for a date range.       ║
║  Used to populate historical schedule data for pick validation.               ║
║                                                                               ║
║  HOW IT WORKS:                                                                ║
║  1. Iterates through date range (configurable start/end dates)                ║
║  2. Fetches ESPN scoreboard API for each date                                 ║
║  3. Extracts game info: teams, time, location, status                         ║
║  4. Writes to Google Sheets with batch operations (rate limit safe)           ║
║                                                                               ║
║  USAGE:                                                                       ║
║  Run manually when you need to backfill schedule data.                        ║
║  Edit START_DATE and END_DATE variables before running.                       ║
║                                                                               ║
║  OUTPUT:                                                                      ║
║  Writes to cbb_schedule tab in the main Google Sheet.                         ║
║                                                                               ║
║  NOTE: For daily schedule fetching, use espn_schedule_fetcher.py instead.     ║
╚═══════════════════════════════════════════════════════════════════════════════╝
"""

import base64
import json
import os
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from zoneinfo import ZoneInfo

import requests
from dotenv import load_dotenv

load_dotenv()

import gspread
from google.oauth2.service_account import Credentials

# ── Config ──────────────────────────────────────────────────────────────────
GOOGLE_SHEET_ID = "1LzkU7rH3OtrJckV5oMvFHyuLAnbRn9E74FO1uyfM65k"
WORKSHEET_NAME = "2026_ncaa_schedule"

FIELDNAMES = [
    "fetch_date",
    "game_date",
    "away_team",
    "home_team",
    "game_time",
    "spread",
    "over_under",
    "tv_network",
    "venue",
    "score",
]

# Default date range: November 11-30, 2026
DEFAULT_START_DATE = "2026-11-11"
DEFAULT_END_DATE = "2026-11-30"


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
        "https://www.googleapis.com/auth/drive",
    ]
    credentials = Credentials.from_service_account_info(creds_dict, scopes=scopes)
    return gspread.authorize(credentials)


def get_worksheet(spreadsheet):
    """Get the worksheet (assumes headers already exist in row 1)."""
    try:
        worksheet = spreadsheet.worksheet(WORKSHEET_NAME)
        return worksheet
    except gspread.WorksheetNotFound:
        raise ValueError(f"Worksheet '{WORKSHEET_NAME}' not found. Please create it with headers first.")


def get_existing_games(worksheet) -> Dict[str, Dict]:
    """Get existing games with their row index and current odds.
    
    Returns dict mapping game key (game_date|away_team|home_team) to:
    {row_idx, spread, over_under, score}
    """
    try:
        all_values = worksheet.get_all_values()
        games = {}
        for i, row in enumerate(all_values[1:], start=2):  # Skip header, 1-indexed rows
            if len(row) >= 4 and row[1] and row[2] and row[3]:
                # Key is: game_date + away_team + home_team
                key = f"{row[1]}|{row[2]}|{row[3]}"
                games[key] = {
                    'row_idx': i,
                    'spread': row[5] if len(row) > 5 else '',
                    'over_under': row[6] if len(row) > 6 else '',
                    'score': row[9] if len(row) > 9 else ''
                }
        return games
    except Exception:
        return {}


# ── ESPN API ─────────────────────────────────────────────────────────────────
def fetch_ncaa_schedule_for_date(date_str: str) -> List[Dict]:
    """Fetch NCAA basketball schedule from ESPN API for a single date.
    
    Args:
        date_str: Date in YYYYMMDD format
        
    Returns:
        List of game dicts
    """
    # ESPN API endpoint for men's college basketball
    # groups=50 gets Division I games, limit=1000 for busy tournament days
    api_url = f"https://site.api.espn.com/apis/site/v2/sports/basketball/mens-college-basketball/scoreboard?dates={date_str}&groups=50&limit=1000"

    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
    }

    response = requests.get(api_url, headers=headers)
    response.raise_for_status()
    data = response.json()

    games = []
    game_date_formatted = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"

    for event in data.get("events", []):
        try:
            competition = event.get("competitions", [{}])[0]
            competitors = competition.get("competitors", [])

            if len(competitors) != 2:
                continue

            # Find away (home=false) and home (home=true) teams
            away_team = ""
            home_team = ""
            for comp in competitors:
                team_name = comp.get("team", {}).get(
                    "displayName", comp.get("team", {}).get("name", "")
                )
                if comp.get("homeAway") == "away":
                    away_team = team_name
                else:
                    home_team = team_name

            if not away_team or not home_team:
                continue

            # Get game time
            game_time = ""
            date_obj = event.get("date", "")
            status = event.get("status", {})
            status_type = status.get("type", {}).get("name", "")

            if status_type == "STATUS_SCHEDULED":
                try:
                    dt = datetime.fromisoformat(date_obj.replace("Z", "+00:00"))
                    eastern = ZoneInfo("America/New_York")
                    dt_eastern = dt.astimezone(eastern)
                    game_time = dt_eastern.strftime("%I:%M %p").lstrip("0")
                except Exception:
                    game_time = "TBD"
            elif status_type in ["STATUS_IN_PROGRESS", "STATUS_HALFTIME"]:
                game_time = "LIVE"
            else:
                game_time = "Final"

            # Get betting odds
            spread = ""
            over_under = ""
            odds = competition.get("odds", [])
            if odds:
                odds_data = odds[0]
                spread_line = odds_data.get("details", "")
                if spread_line:
                    spread = spread_line
                ou = odds_data.get("overUnder")
                if ou:
                    over_under = str(ou)

            # Get venue
            venue = ""
            venue_data = competition.get("venue", {})
            if venue_data:
                venue_name = venue_data.get("fullName", venue_data.get("shortName", ""))
                city = venue_data.get("address", {}).get("city", "")
                state = venue_data.get("address", {}).get("state", "")
                if venue_name:
                    venue = venue_name
                    if city:
                        venue += f", {city}"
                        if state:
                            venue += f", {state}"

            # Get TV network
            tv_network = ""
            broadcasts = competition.get("broadcasts", [])
            if broadcasts:
                for b in broadcasts:
                    names = b.get("names", [])
                    if names:
                        tv_network = names[0]
                        break

            # Get score if game is final
            score = ""
            if status_type == "STATUS_FINAL":
                away_score = ""
                home_score = ""
                for comp in competitors:
                    if comp.get("homeAway") == "away":
                        away_score = comp.get("score", "")
                    else:
                        home_score = comp.get("score", "")
                if away_score and home_score:
                    score = f"{home_team} {home_score}, {away_team} {away_score}"

            games.append({
                "game_date": game_date_formatted,
                "away_team": away_team,
                "home_team": home_team,
                "game_time": game_time,
                "spread": spread,
                "over_under": over_under,
                "tv_network": tv_network,
                "venue": venue,
                "score": score,
            })

        except Exception as e:
            print(f"  Error parsing event: {e}")
            continue

    return games


def generate_date_range(start_date: str, end_date: str) -> List[str]:
    """Generate list of dates between start and end (inclusive).
    
    Args:
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format
        
    Returns:
        List of dates in YYYYMMDD format (for ESPN API)
    """
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    
    dates = []
    current = start
    while current <= end:
        dates.append(current.strftime("%Y%m%d"))
        current += timedelta(days=1)
    
    return dates


def write_games_to_sheet(
    worksheet,
    games: List[Dict],
    existing_games: Dict[str, Dict],
    fetch_timestamp: str,
) -> tuple[int, int, int, List[str]]:
    """Write new games to sheet (append only). Returns (new_count, skipped_count, scores_updated, change_details)."""
    new_rows = []
    skipped_count = 0
    score_updates = []  # List of (row_idx, score) for batch update
    change_details = []

    for game in games:
        key = f"{game['game_date']}|{game['away_team']}|{game['home_team']}"
        game_label = f"{game['away_team']} @ {game['home_team']}"
        
        if key in existing_games:
            existing = existing_games[key]
            # Check if we have a score now but didn't before
            if game['score'] and not existing.get('score'):
                score_updates.append((existing['row_idx'], game['score']))
                print(f"  🔄 Score update: {game_label} -> {game['score']}")
            skipped_count += 1
            continue

        row = [
            fetch_timestamp,
            game["game_date"],
            game["away_team"],
            game["home_team"],
            game["game_time"],
            game["spread"],
            game["over_under"],
            game["tv_network"],
            game["venue"],
            game["score"],
        ]

        new_rows.append(row)
        existing_games[key] = {'row_idx': -1, 'spread': game['spread'], 'over_under': game['over_under']}
        change_details.append(f"{game['game_date']}: {game_label}")
        print(f"  ✅ {game['game_date']}: {game_label} - {game['game_time']}")

    # Batch write all new rows at once
    if new_rows:
        time.sleep(1)  # Rate limit
        worksheet.append_rows(new_rows, value_input_option="USER_ENTERED")

    # Batch update scores for existing rows
    if score_updates:
        time.sleep(1)  # Rate limit
        cells = [gspread.Cell(row_idx, 10, score) for row_idx, score in score_updates]  # Column J = 10
        worksheet.update_cells(cells)

    return len(new_rows), skipped_count, len(score_updates), change_details


def main(start_date: Optional[str] = None, end_date: Optional[str] = None):
    eastern = ZoneInfo("America/New_York")
    now_eastern = datetime.now(eastern)
    timestamp = now_eastern.strftime("%Y-%m-%d %H:%M:%S")

    # Use provided dates or defaults
    start = start_date or DEFAULT_START_DATE
    end = end_date or DEFAULT_END_DATE

    print(f"\n[{timestamp}] Fetching NCAA basketball schedule for {start} to {end}...")

    try:
        client = get_gspread_client()
        spreadsheet = client.open_by_key(GOOGLE_SHEET_ID)
        worksheet = get_worksheet(spreadsheet)

        # Get existing games to avoid duplicates
        existing_games = get_existing_games(worksheet)
        print(f"Found {len(existing_games)} existing games in sheet")

        # Generate date range
        dates = generate_date_range(start, end)
        print(f"Will fetch {len(dates)} days of games")

        total_fetched = 0
        total_added = 0
        total_skipped = 0
        total_scores_updated = 0
        all_changes = []

        # Fetch games for each date
        for i, date_str in enumerate(dates):
            formatted_date = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
            print(f"\n📅 [{i+1}/{len(dates)}] Fetching games for {formatted_date}...")

            games = fetch_ncaa_schedule_for_date(date_str)
            total_fetched += len(games)

            if games:
                print(f"  Found {len(games)} games")
                new_count, skipped, scores_updated, changes = write_games_to_sheet(
                    worksheet, games, existing_games, timestamp
                )
                total_added += new_count
                total_skipped += skipped
                total_scores_updated += scores_updated
                all_changes.extend(changes)
                
                if skipped > 0 and scores_updated == 0:
                    print(f"  Skipped {skipped} existing games")
            else:
                print(f"  No games found")

            # Rate limit between API calls
            if i < len(dates) - 1:
                time.sleep(0.5)

        print(f"\n✅ Done!")
        print(f"   Total games fetched: {total_fetched}")
        print(f"   New games added: {total_added}")
        print(f"   Scores updated: {total_scores_updated}")
        print(f"   Existing games skipped: {total_skipped}")

    except ValueError as e:
        print(f"Error: {e}")
        exit(1)


if __name__ == "__main__":
    import sys

    # Usage: python ncaa_schedule_fetcher.py [start_date] [end_date]
    # Dates in YYYY-MM-DD format
    # Example: python ncaa_schedule_fetcher.py 2026-11-11 2026-11-30
    
    start_date = sys.argv[1] if len(sys.argv) > 1 else None
    end_date = sys.argv[2] if len(sys.argv) > 2 else None
    
    main(start_date, end_date)
