#!/usr/bin/env python3
"""
ESPN Schedule Fetcher
Fetches NBA and College Basketball schedules from ESPN for the current date.
Extracts game times, teams, and betting lines, then writes to Google Sheets.
"""

import base64
import json
import os
import re
from datetime import datetime
from typing import Dict, List, Optional
from zoneinfo import ZoneInfo

import requests
from dotenv import load_dotenv

load_dotenv()

import gspread
from bs4 import BeautifulSoup
from google.oauth2.service_account import Credentials

from activity_logger import log_activity

# ── Config ──────────────────────────────────────────────────────────────────
GOOGLE_SHEET_ID = "1LzkU7rH3OtrJckV5oMvFHyuLAnbRn9E74FO1uyfM65k"
NBA_WORKSHEET_NAME = "nba_schedule"
CBB_WORKSHEET_NAME = "cbb_schedule"
NHL_WORKSHEET_NAME = "nhl_schedule"

NBA_FIELDNAMES = [
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
CBB_FIELDNAMES = [
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
NHL_FIELDNAMES = [
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


def get_or_create_worksheet(spreadsheet, worksheet_name: str, fieldnames: List[str]):
    """Get or create a worksheet with the given name and headers."""
    try:
        worksheet = spreadsheet.worksheet(worksheet_name)
        # Check if header row exists and is correct
        first_row = worksheet.row_values(1)
        if first_row != fieldnames and first_row != fieldnames[:len(first_row)]:
            # Headers are genuinely wrong (not just missing a new trailing column
            # like 'score' that was added to FIELDNAMES but not yet to the sheet).
            # Clear the sheet and add proper header
            print(f"  Resetting {worksheet_name} with proper headers...")
            worksheet.clear()
            worksheet.append_row(fieldnames)
    except gspread.WorksheetNotFound:
        worksheet = spreadsheet.add_worksheet(
            title=worksheet_name, rows=1000, cols=len(fieldnames)
        )
        worksheet.append_row(fieldnames)

    return worksheet


def get_existing_games(worksheet) -> Dict[str, Dict]:
    """Get existing games with their row index and current odds.
    
    Returns dict mapping game key (game_date|away_team|home_team) to:
    {row_idx, spread, over_under}
    """
    try:
        all_values = worksheet.get_all_values()
        games = {}
        for i, row in enumerate(all_values[1:], start=2):  # Skip header, 1-indexed rows
            if len(row) >= 7:
                # Key is: game_date + away_team + home_team
                key = f"{row[1]}|{row[2]}|{row[3]}"
                games[key] = {
                    'row_idx': i,
                    'spread': row[5] if len(row) > 5 else '',
                    'over_under': row[6] if len(row) > 6 else ''
                }
        return games
    except Exception:
        return {}


# ── ESPN Schedule Fetching ───────────────────────────────────────────────────
def fetch_espn_schedule(url: str) -> str:
    """Fetch the ESPN schedule page HTML."""
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    return response.text


def parse_betting_line(line_text: str) -> tuple[str, str]:
    """Parse betting line text to extract spread and over/under.

    Example: "Line: UCLA -4.5O/U: 151.5" -> ("-4.5 UCLA", "151.5")
    """
    spread = ""
    over_under = ""

    if not line_text:
        return spread, over_under

    # Pattern: Line: TEAM -X.X or Line: TEAM +X.X
    line_match = re.search(r"Line:\s*([A-Z]+)\s*([+-]?\d+\.?\d*)", line_text)
    if line_match:
        team_abbr = line_match.group(1)
        spread_num = line_match.group(2)
        spread = f"{team_abbr} {spread_num}"

    # Pattern: O/U: XXX.X
    ou_match = re.search(r"O/U:\s*(\d+\.?\d*)", line_text)
    if ou_match:
        over_under = ou_match.group(1)

    return spread, over_under


def parse_nba_schedule(html: str, game_date: str) -> List[Dict]:
    """Parse NBA schedule HTML to extract game information."""
    soup = BeautifulSoup(html, "html.parser")
    games = []

    # Find all schedule tables
    tables = soup.find_all("div", class_="ResponsiveTable")
    if not tables:
        # Try alternative approach - look for ScheduleTables
        tables = soup.find_all("section", class_="Card")

    # Find tbody elements containing game rows
    for tbody in soup.find_all("tbody", class_="Table__TBODY"):
        for row in tbody.find_all("tr", class_="Table__TR"):
            try:
                cells = row.find_all("td")
                if len(cells) < 2:
                    continue

                # Extract teams from the matchup cell
                matchup_cell = cells[0]
                teams_text = matchup_cell.get_text(separator=" ", strip=True)

                # Pattern: "Away Team @ Home Team" or similar
                away_team = ""
                home_team = ""

                # Find team links
                team_links = matchup_cell.find_all("a", class_="AnchorLink")
                if len(team_links) >= 2:
                    away_team = team_links[0].get_text(strip=True)
                    home_team = team_links[1].get_text(strip=True)
                elif "@" in teams_text:
                    parts = teams_text.split("@")
                    if len(parts) == 2:
                        away_team = parts[0].strip()
                        home_team = parts[1].strip()

                if not away_team or not home_team:
                    continue

                # Extract time/result
                game_time = ""
                if len(cells) > 1:
                    time_cell = cells[1]
                    game_time = time_cell.get_text(strip=True)
                    # Skip completed games (scores have numbers like "119, MIN 92")
                    if re.search(r"\d{2,3}.*\d{2,3}", game_time):
                        game_time = "Final"

                # Extract TV network
                tv_network = ""
                if len(cells) > 2:
                    tv_cell = cells[2]
                    tv_network = tv_cell.get_text(strip=True)

                # Extract betting line
                spread = ""
                over_under = ""
                for cell in cells:
                    cell_text = cell.get_text(strip=True)
                    if "Line:" in cell_text:
                        spread, over_under = parse_betting_line(cell_text)
                        break

                # Extract venue if present
                venue = ""
                for cell in cells:
                    cell_text = cell.get_text(strip=True)
                    # Venues often contain Arena, Center, Stadium
                    if any(
                        word in cell_text
                        for word in ["Arena", "Center", "Stadium", "Garden", "Court"]
                    ):
                        venue = cell_text
                        break

                games.append(
                    {
                        "game_date": game_date,
                        "away_team": away_team,
                        "home_team": home_team,
                        "game_time": game_time,
                        "spread": spread,
                        "over_under": over_under,
                        "tv_network": tv_network,
                        "venue": venue,
                    }
                )

            except Exception as e:
                print(f"Error parsing row: {e}")
                continue

    return games


def parse_cbb_schedule(html: str, game_date: str) -> List[Dict]:
    """Parse College Basketball schedule HTML to extract game information."""
    # The parsing logic is very similar to NBA
    return parse_nba_schedule(html, game_date)


def parse_schedule_from_text(text: str, game_date: str, sport: str) -> List[Dict]:
    """Alternative parsing using regex on the raw text content."""
    games = []

    # Split by date sections and find the target date section
    date_pattern = r"(Saturday|Sunday|Monday|Tuesday|Wednesday|Thursday|Friday),\s+(January|February|March|April|May|June|July|August|September|October|November|December)\s+\d+,\s+\d+"

    # Look for game patterns: "Team1   @   Team2 | Time | TV | Venue | Line"
    # Pattern variations from ESPN tables

    lines = text.split("\n")
    i = 0
    while i < len(lines):
        line = lines[i].strip()

        # Look for matchup patterns
        if " @ " in line or " v " in line:
            # This might be a game line
            parts = re.split(r"\s+[@v]\s+", line)
            if len(parts) == 2:
                away_team = parts[0].strip()
                remaining = parts[1]

                # Parse the remaining part for home team, time, etc.
                # Pattern: "Home Team | Time | TV | Venue | Line: X -X.X O/U: XXX"
                home_parts = re.split(r"\s*\|\s*", remaining)
                home_team = home_parts[0].strip() if home_parts else ""

                game_time = ""
                tv_network = ""
                venue = ""
                spread = ""
                over_under = ""

                for part in home_parts[1:]:
                    part = part.strip()
                    # Check if it's a time
                    if re.match(r"\d+:\d+\s*(AM|PM)", part):
                        game_time = part
                    # Check for betting line
                    elif "Line:" in part:
                        spread, over_under = parse_betting_line(part)
                    # Check for TV network
                    elif part in [
                        "ESPN",
                        "ESPN2",
                        "ABC",
                        "TNT",
                        "NBATV",
                        "FS1",
                        "CBS",
                        "CBSSN",
                        "truTV",
                        "BTN",
                        "Peacock",
                        "NBCSN",
                    ]:
                        tv_network = part
                    # Check for venue
                    elif any(
                        word in part
                        for word in [
                            "Arena",
                            "Center",
                            "Stadium",
                            "Garden",
                            "Court",
                            "Coliseum",
                        ]
                    ):
                        venue = part

                if away_team and home_team:
                    games.append(
                        {
                            "game_date": game_date,
                            "away_team": away_team,
                            "home_team": home_team,
                            "game_time": game_time,
                            "spread": spread,
                            "over_under": over_under,
                            "tv_network": tv_network,
                            "venue": venue,
                        }
                    )

        i += 1

    return games


def fetch_and_parse_schedule_api(sport: str, date_str: str) -> List[Dict]:
    """Fetch schedule using ESPN's unofficial API endpoint."""

    # ESPN has an API endpoint that returns JSON for schedules
    if sport == "nba":
        api_url = f"https://site.api.espn.com/apis/site/v2/sports/basketball/nba/scoreboard?dates={date_str}"
    elif sport == "nhl":
        api_url = f"https://site.api.espn.com/apis/site/v2/sports/hockey/nhl/scoreboard?dates={date_str}"
    else:
        api_url = f"https://site.api.espn.com/apis/site/v2/sports/basketball/mens-college-basketball/scoreboard?dates={date_str}&groups=50"

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

            # Get game time
            game_time = ""
            date_obj = event.get("date", "")
            status = event.get("status", {})
            status_type = status.get("type", {}).get("name", "")

            if status_type == "STATUS_SCHEDULED":
                # Parse the time from the date
                try:
                    dt = datetime.fromisoformat(date_obj.replace("Z", "+00:00"))
                    eastern = ZoneInfo("America/New_York")
                    dt_eastern = dt.astimezone(eastern)
                    game_time = dt_eastern.strftime("%I:%M %p").lstrip("0")
                except:
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

            games.append(
                {
                    "game_date": game_date_formatted,
                    "away_team": away_team,
                    "home_team": home_team,
                    "game_time": game_time,
                    "spread": spread,
                    "over_under": over_under,
                    "tv_network": tv_network,
                    "venue": venue,
                }
            )

        except Exception as e:
            print(f"Error parsing event: {e}")
            continue

    return games


def write_games_to_sheet(
    worksheet,
    games: List[Dict],
    existing_games: Dict[str, Dict],
    fetch_timestamp: str,
    fieldnames: List[str],
) -> tuple[int, int, List[str]]:
    """Write new games and update changed odds. Returns (new_count, updated_count, change_details)."""
    new_rows = []
    updates = []  # List of (row_idx, col, value) for batch update
    change_details = []  # Track specific changes for logging

    for game in games:
        key = f"{game['game_date']}|{game['away_team']}|{game['home_team']}"
        game_label = f"{game['away_team']} @ {game['home_team']}"
        
        if key in existing_games:
            existing = existing_games[key]
            # Check if spread or over_under changed
            spread_changed = game['spread'] and game['spread'] != existing['spread']
            ou_changed = game['over_under'] and game['over_under'] != existing['over_under']
            
            if spread_changed or ou_changed:
                row_idx = existing['row_idx']
                if spread_changed:
                    updates.append((row_idx, 6, game['spread']))  # Column F (1-indexed)
                    change_details.append(f"{game_label}: spread {existing['spread']} → {game['spread']}")
                if ou_changed:
                    updates.append((row_idx, 7, game['over_under']))  # Column G (1-indexed)
                    change_details.append(f"{game_label}: O/U {existing['over_under']} → {game['over_under']}")
                print(
                    f"  🔄 Will update: {game['away_team']} @ {game['home_team']} - "
                    f"Spread: {existing['spread']} → {game['spread']}, "
                    f"O/U: {existing['over_under']} → {game['over_under']}"
                )
            else:
                print(f"  Skipping (no changes): {game['away_team']} @ {game['home_team']}")
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
        ]

        new_rows.append(row)
        existing_games[key] = {'row_idx': -1, 'spread': game['spread'], 'over_under': game['over_under']}
        change_details.append(f"{game_label}: NEW ({game['spread']}, O/U {game['over_under']})")
        print(
            f"  ✅ Will add: {game['away_team']} @ {game['home_team']} - {game['game_time']} - {game['spread']} O/U {game['over_under']}"
        )

    # Batch write all new rows at once
    if new_rows:
        worksheet.append_rows(new_rows, value_input_option="USER_ENTERED")
    
    # Apply updates for changed odds
    for row_idx, col, value in updates:
        worksheet.update_cell(row_idx, col, value)

    return len(new_rows), len(updates), change_details


def main(target_date: Optional[str] = None):
    eastern = ZoneInfo("America/New_York")
    now_eastern = datetime.now(eastern)
    timestamp = now_eastern.strftime("%Y-%m-%d %H:%M:%S")

    # Use target_date if provided, otherwise use today
    if target_date:
        date_str = target_date.replace("-", "")
        formatted_date = target_date
    else:
        date_str = now_eastern.strftime("%Y%m%d")
        formatted_date = now_eastern.strftime("%Y-%m-%d")

    print(f"\n[{timestamp}] Fetching ESPN schedules for {formatted_date}...")

    try:
        client = get_gspread_client()
        spreadsheet = client.open_by_key(GOOGLE_SHEET_ID)

        # ── NBA Schedule ────────────────────────────────────────────────────
        print("\n📊 Fetching NBA schedule...")
        nba_worksheet = get_or_create_worksheet(
            spreadsheet, NBA_WORKSHEET_NAME, NBA_FIELDNAMES
        )
        nba_existing_games = get_existing_games(nba_worksheet)

        nba_games = fetch_and_parse_schedule_api("nba", date_str)
        print(f"Found {len(nba_games)} NBA games")

        nba_new, nba_updated, nba_changes = write_games_to_sheet(
            nba_worksheet, nba_games, nba_existing_games, timestamp, NBA_FIELDNAMES
        )
        print(
            f"✅ NBA: Added {nba_new} new games, updated {nba_updated} odds"
        )
        
        # Log NBA fetch
        nba_url = f"https://site.api.espn.com/apis/site/v2/sports/basketball/nba/scoreboard?dates={date_str}"
        log_activity(spreadsheet, "fetch_schedule", f"NBA {formatted_date}: {nba_new} rows added, {nba_updated} rows updated", {"url": nba_url, "games_fetched": len(nba_games), "details": ", ".join(nba_changes) if nba_changes else "no changes"})

        # ── College Basketball Schedule ─────────────────────────────────────
        print("\n🏀 Fetching College Basketball schedule...")
        cbb_worksheet = get_or_create_worksheet(
            spreadsheet, CBB_WORKSHEET_NAME, CBB_FIELDNAMES
        )
        cbb_existing_games = get_existing_games(cbb_worksheet)

        cbb_games = fetch_and_parse_schedule_api("cbb", date_str)
        print(f"Found {len(cbb_games)} College Basketball games")

        cbb_new, cbb_updated, cbb_changes = write_games_to_sheet(
            cbb_worksheet, cbb_games, cbb_existing_games, timestamp, CBB_FIELDNAMES
        )
        print(
            f"✅ CBB: Added {cbb_new} new games, updated {cbb_updated} odds"
        )
        
        # Log CBB fetch
        cbb_url = f"https://site.api.espn.com/apis/site/v2/sports/basketball/mens-college-basketball/scoreboard?dates={date_str}&groups=50"
        log_activity(spreadsheet, "fetch_schedule", f"CBB {formatted_date}: {cbb_new} rows added, {cbb_updated} rows updated", {"url": cbb_url, "games_fetched": len(cbb_games), "details": ", ".join(cbb_changes) if cbb_changes else "no changes"})

        # ── NHL Schedule ────────────────────────────────────────────────────
        print("\n🏒 Fetching NHL schedule...")
        nhl_worksheet = get_or_create_worksheet(
            spreadsheet, NHL_WORKSHEET_NAME, NHL_FIELDNAMES
        )
        nhl_existing_games = get_existing_games(nhl_worksheet)

        nhl_games = fetch_and_parse_schedule_api("nhl", date_str)
        print(f"Found {len(nhl_games)} NHL games")

        nhl_new, nhl_updated, nhl_changes = write_games_to_sheet(
            nhl_worksheet, nhl_games, nhl_existing_games, timestamp, NHL_FIELDNAMES
        )
        print(
            f"✅ NHL: Added {nhl_new} new games, updated {nhl_updated} odds"
        )
        
        # Log NHL fetch
        nhl_url = f"https://site.api.espn.com/apis/site/v2/sports/hockey/nhl/scoreboard?dates={date_str}"
        log_activity(spreadsheet, "fetch_schedule", f"NHL {formatted_date}: {nhl_new} rows added, {nhl_updated} rows updated", {"url": nhl_url, "games_fetched": len(nhl_games), "details": ", ".join(nhl_changes) if nhl_changes else "no changes"})

        print(
            f"\n✅ Done! Processed {len(nba_games)} NBA, {len(cbb_games)} CBB, and {len(nhl_games)} NHL games."
        )

    except ValueError as e:
        print(f"Error: {e}")
        exit(1)


if __name__ == "__main__":
    import sys

    target_date = sys.argv[1] if len(sys.argv) > 1 else None
    main(target_date)
