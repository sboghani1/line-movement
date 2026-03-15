#!/usr/bin/env python3
"""
ESPN Schedule Fetcher
Fetches NBA, CBB, NHL, NFL, and CFB schedules from ESPN for the current date.
Writes new games to Google Sheets and backfills scores for completed past games.

Usage:
    python espn_schedule_fetcher.py [date] [--sport nba|cbb|nhl|nfl|cfb]

    date     Optional date override in YYYY-MM-DD format (default: today)
    --sport  Run only this sport (default: all five)
"""

import argparse
import base64
import json
import os
from datetime import datetime, date
from typing import Dict, List, Optional
from zoneinfo import ZoneInfo

import requests
from dotenv import load_dotenv

load_dotenv()

import gspread
from google.oauth2.service_account import Credentials

from activity_logger import log_activity

# ── Config ──────────────────────────────────────────────────────────────────
GOOGLE_SHEET_ID = "1LzkU7rH3OtrJckV5oMvFHyuLAnbRn9E74FO1uyfM65k"
NBA_WORKSHEET_NAME = "nba_schedule"
CBB_WORKSHEET_NAME = "cbb_schedule"
NHL_WORKSHEET_NAME = "nhl_schedule"
NFL_WORKSHEET_NAME = "nfl_schedule"
CFB_WORKSHEET_NAME = "cfb_schedule"

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
    "period_scores",
]

SPORT_WORKSHEETS = {
    "nba": NBA_WORKSHEET_NAME,
    "cbb": CBB_WORKSHEET_NAME,
    "nhl": NHL_WORKSHEET_NAME,
    "nfl": NFL_WORKSHEET_NAME,
    "cfb": CFB_WORKSHEET_NAME,
}


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


def get_or_create_worksheet(spreadsheet, worksheet_name: str):
    """Get or create a worksheet with the given name and headers."""
    try:
        worksheet = spreadsheet.worksheet(worksheet_name)
        first_row = worksheet.row_values(1)
        if len(first_row) < len(FIELDNAMES):
            for col_idx, name in enumerate(FIELDNAMES[len(first_row):], start=len(first_row) + 1):
                worksheet.update_cell(1, col_idx, name)
        return worksheet
    except gspread.WorksheetNotFound:
        worksheet = spreadsheet.add_worksheet(
            title=worksheet_name, rows=1000, cols=len(FIELDNAMES)
        )
        worksheet.append_row(FIELDNAMES)
        return worksheet


def get_existing_games(worksheet) -> Dict[str, Dict]:
    """Get existing games keyed by game_date|away_team|home_team.

    Returns dict with {row_idx, spread, over_under, score}.
    """
    try:
        all_values = worksheet.get_all_values()
        games = {}
        for i, row in enumerate(all_values[1:], start=2):
            if len(row) >= 4 and row[1] and row[2] and row[3]:
                key = f"{row[1]}|{row[2]}|{row[3]}"
                games[key] = {
                    "row_idx": i,
                    "spread": row[5] if len(row) > 5 else "",
                    "over_under": row[6] if len(row) > 6 else "",
                    "score": row[9] if len(row) > 9 else "",
                    "period_scores": row[10] if len(row) > 10 else "",
                }
        return games
    except Exception:
        return {}


# ── Score Backfill ───────────────────────────────────────────────────────────
def fetch_espn_results(sport: str, date_str: str) -> Dict:
    """Fetch completed game scores from ESPN for a given sport and date (YYYYMMDD).

    Returns dict keyed by (away_team, home_team) with score strings like
    "Memphis Grizzlies 110, Detroit Pistons 126" (away first).
    """
    if sport == "nba":
        url = f"https://site.api.espn.com/apis/site/v2/sports/basketball/nba/scoreboard?dates={date_str}"
    elif sport == "nhl":
        url = f"https://site.api.espn.com/apis/site/v2/sports/hockey/nhl/scoreboard?dates={date_str}"
    elif sport == "cbb":
        url = f"https://site.api.espn.com/apis/site/v2/sports/basketball/mens-college-basketball/scoreboard?dates={date_str}&groups=50"
    elif sport == "nfl":
        url = f"https://site.api.espn.com/apis/site/v2/sports/football/nfl/scoreboard?dates={date_str}"
    elif sport == "cfb":
        url = f"https://site.api.espn.com/apis/site/v2/sports/football/college-football/scoreboard?dates={date_str}&groups=80"
    else:
        raise ValueError(f"Unsupported sport: {sport!r}")

    response = requests.get(url)
    response.raise_for_status()
    data = response.json()
    results = {}

    for event in data.get("events", []):
        if not event.get("status", {}).get("type", {}).get("completed", False):
            continue

        competition = event.get("competitions", [{}])[0]
        away_team, home_team, away_score, home_score = "", "", "", ""
        away_linescores, home_linescores = [], []

        for comp in competition.get("competitors", []):
            name = comp.get("team", {}).get("displayName", "")
            score = comp.get("score", "")
            period_vals = [str(int(ls.get("value", 0))) for ls in comp.get("linescores", [])]
            if comp.get("homeAway") == "away":
                away_team, away_score = name, score
                away_linescores = period_vals
            else:
                home_team, home_score = name, score
                home_linescores = period_vals

        if away_team and home_team:
            period_scores = ""
            if away_linescores and home_linescores and len(away_linescores) == len(home_linescores):
                period_scores = " | ".join(
                    f"{away_team} {a}, {home_team} {h}"
                    for a, h in zip(away_linescores, home_linescores)
                )
            results[(away_team, home_team)] = (
                f"{away_team} {away_score}, {home_team} {home_score}",
                period_scores,
            )

    return results


def update_scores_for_sheet(
    spreadsheet, worksheet_name: str, sport: str, limit: Optional[int] = None
) -> tuple[int, List[str]]:
    """Backfill the score column for any past rows that are missing a score.

    Scans all existing rows where score is blank and game_date < today,
    groups by date, and makes one ESPN API call per date to fill them in.
    Returns (updated_count, change_details).
    """
    worksheet = get_or_create_worksheet(spreadsheet, worksheet_name)
    all_rows = worksheet.get_all_values()
    headers = all_rows[0]

    try:
        game_date_col = headers.index("game_date")
        away_team_col = headers.index("away_team")
        home_team_col = headers.index("home_team")
        score_col = headers.index("score")
    except ValueError as e:
        print(f"  ❌ Missing column in {worksheet_name}: {e}")
        return 0, []

    period_scores_col = headers.index("period_scores") if "period_scores" in headers else None

    today = date.today()

    dates_needing_update: Dict[str, List] = {}
    for i, row in enumerate(all_rows[1:], start=2):
        while len(row) <= score_col:
            row.append("")

        game_date_str = row[game_date_col]
        away_team = row[away_team_col]
        home_team = row[home_team_col]
        score = row[score_col]

        if score:
            continue
        if away_team == "TBD" or home_team == "TBD":
            continue

        try:
            game_date = datetime.strptime(game_date_str, "%Y-%m-%d").date()
        except ValueError:
            continue

        if game_date >= today:
            continue

        date_key = game_date_str.replace("-", "")
        dates_needing_update.setdefault(date_key, []).append(
            (i, away_team, home_team)
        )

    if not dates_needing_update:
        print(f"  ✅ {worksheet_name}: no missing scores")
        return 0, []

    print(
        f"  {worksheet_name}: missing scores on {len(dates_needing_update)} date(s) "
        f"— {list(dates_needing_update.keys())}"
    )

    total_written = 0
    change_details = []

    rows_processed = 0
    for date_key, rows_for_date in dates_needing_update.items():
        if limit is not None and rows_processed >= limit:
            break
        espn_results = fetch_espn_results(sport, date_key)
        date_batch = []
        for row_idx, away_team, home_team in rows_for_date:
            if limit is not None and rows_processed >= limit:
                break
            rows_processed += 1
            result = espn_results.get((away_team, home_team))
            if result:
                score_str, period_scores_str = result
                date_batch.append({
                    "range": gspread.utils.rowcol_to_a1(row_idx, score_col + 1),
                    "values": [[score_str]],
                })
                if period_scores_col is not None and period_scores_str:
                    date_batch.append({
                        "range": gspread.utils.rowcol_to_a1(row_idx, period_scores_col + 1),
                        "values": [[period_scores_str]],
                    })
                change_details.append(score_str)
            else:
                # Mark as N/A so we don't retry on every future run
                print(f"    ⚠️  No ESPN result for: {away_team} @ {home_team} on {date_key} — marking N/A")
                date_batch.append({
                    "range": gspread.utils.rowcol_to_a1(row_idx, score_col + 1),
                    "values": [["N/A"]],
                })

        if date_batch:
            worksheet.batch_update(date_batch)
            total_written += len(date_batch)
            print(f"    {date_key}: wrote {len(date_batch)} updates")

    if total_written:
        print(f"  ✅ {worksheet_name}: wrote {total_written} score updates total")

    return total_written, change_details


# ── ESPN Schedule Fetching ───────────────────────────────────────────────────
def fetch_and_parse_schedule_api(sport: str, date_str: str) -> List[Dict]:
    """Fetch schedule from ESPN's JSON API for a given sport and date (YYYYMMDD)."""

    if sport == "nba":
        api_url = f"https://site.api.espn.com/apis/site/v2/sports/basketball/nba/scoreboard?dates={date_str}"
    elif sport == "nhl":
        api_url = f"https://site.api.espn.com/apis/site/v2/sports/hockey/nhl/scoreboard?dates={date_str}"
    elif sport == "cbb":
        api_url = f"https://site.api.espn.com/apis/site/v2/sports/basketball/mens-college-basketball/scoreboard?dates={date_str}&groups=50"
    elif sport == "nfl":
        api_url = f"https://site.api.espn.com/apis/site/v2/sports/football/nfl/scoreboard?dates={date_str}"
    elif sport == "cfb":
        api_url = f"https://site.api.espn.com/apis/site/v2/sports/football/college-football/scoreboard?dates={date_str}&groups=80"
    else:
        raise ValueError(f"Unsupported sport: {sport!r}")

    response = requests.get(
        api_url,
        headers={"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"},
    )
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

            # Game time
            date_obj = event.get("date", "")
            status_type = event.get("status", {}).get("type", {}).get("name", "")

            if status_type == "STATUS_SCHEDULED":
                try:
                    dt = datetime.fromisoformat(date_obj.replace("Z", "+00:00"))
                    dt_eastern = dt.astimezone(ZoneInfo("America/New_York"))
                    game_time = dt_eastern.strftime("%I:%M %p").lstrip("0")
                except Exception:
                    game_time = "TBD"
            elif status_type in ("STATUS_IN_PROGRESS", "STATUS_HALFTIME"):
                game_time = "LIVE"
            else:
                game_time = "Final"

            # Spread and over/under
            # odds.details uses abbreviations (e.g. "OKC -9.5"), so we build the
            # spread string ourselves using the full team names already extracted.
            # odds.spread is the home team's signed point spread:
            #   negative = home favored, positive = home is underdog (away favored).
            spread = ""
            over_under = ""
            odds_list = competition.get("odds", [])
            if odds_list:
                odds_data = odds_list[0]
                spread_val = odds_data.get("spread")  # home team's spread (signed float)
                ou = odds_data.get("overUnder")
                if ou is not None:
                    over_under = str(ou)
                if spread_val is not None:
                    if spread_val == 0:
                        spread = "PK"
                    elif odds_data.get("homeTeamOdds", {}).get("favorite"):
                        # home is favored; spread_val is negative
                        spread = f"{home_team} {spread_val:g}"
                    elif odds_data.get("awayTeamOdds", {}).get("favorite"):
                        # away is favored; negate home's positive spread to get away's negative
                        spread = f"{away_team} {-spread_val:g}"

            # Venue
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

            # TV network
            tv_network = ""
            broadcasts = competition.get("broadcasts", [])
            if broadcasts:
                for b in broadcasts:
                    names = b.get("names", [])
                    if names:
                        tv_network = names[0]
                        break

            games.append({
                "game_date": game_date_formatted,
                "away_team": away_team,
                "home_team": home_team,
                "game_time": game_time,
                "spread": spread,
                "over_under": over_under,
                "tv_network": tv_network,
                "venue": venue,
            })

        except Exception as e:
            print(f"Error parsing event: {e}")
            continue

    return games


def write_games_to_sheet(
    worksheet,
    games: List[Dict],
    existing_games: Dict[str, Dict],
    fetch_timestamp: str,
) -> tuple[int, int, List[str]]:
    """Write new games and update changed odds. Returns (new_count, updated_count, change_details)."""
    new_rows = []
    odds_updates = []
    change_details = []

    for game in games:
        key = f"{game['game_date']}|{game['away_team']}|{game['home_team']}"
        game_label = f"{game['away_team']} @ {game['home_team']}"

        if key in existing_games:
            existing = existing_games[key]
            spread_changed = game["spread"] and game["spread"] != existing["spread"]
            ou_changed = game["over_under"] and game["over_under"] != existing["over_under"]

            if spread_changed or ou_changed:
                row_idx = existing["row_idx"]
                if spread_changed:
                    odds_updates.append((row_idx, 6, game["spread"]))
                    change_details.append(f"{game_label}: spread {existing['spread']} → {game['spread']}")
                if ou_changed:
                    odds_updates.append((row_idx, 7, game["over_under"]))
                    change_details.append(f"{game_label}: O/U {existing['over_under']} → {game['over_under']}")
                print(
                    f"  🔄 {game_label} — "
                    f"spread: {existing['spread']} → {game['spread']}, "
                    f"O/U: {existing['over_under']} → {game['over_under']}"
                )
            else:
                print(f"  Skipping (no changes): {game_label}")
            continue

        new_rows.append([
            fetch_timestamp,
            game["game_date"],
            game["away_team"],
            game["home_team"],
            game["game_time"],
            game["spread"],
            game["over_under"],
            game["tv_network"],
            game["venue"],
            "",  # score — filled in by update_scores_for_sheet once game completes
            "",  # period_scores — filled in by update_scores_for_sheet once game completes
        ])
        existing_games[key] = {
            "row_idx": -1,
            "spread": game["spread"],
            "over_under": game["over_under"],
            "score": "",
            "period_scores": "",
        }
        change_details.append(f"{game_label}: NEW ({game['spread']}, O/U {game['over_under']})")
        print(f"  ✅ {game['game_date']}: {game_label} — {game['game_time']} — {game['spread']} O/U {game['over_under']}")

    if new_rows:
        worksheet.append_rows(new_rows, value_input_option="USER_ENTERED")

    if odds_updates:
        batch = [
            {
                "range": gspread.utils.rowcol_to_a1(row_idx, col),
                "values": [[value]],
            }
            for row_idx, col, value in odds_updates
        ]
        worksheet.batch_update(batch)

    return len(new_rows), len(odds_updates), change_details


def run_sport(
    spreadsheet, sport: str, date_str: str, formatted_date: str, timestamp: str,
    score_limit: Optional[int] = None,
):
    """Run score backfill + schedule fetch for a single sport."""
    worksheet_name = SPORT_WORKSHEETS[sport]

    score_count, score_details = update_scores_for_sheet(spreadsheet, worksheet_name, sport, limit=score_limit)
    log_activity(
        spreadsheet,
        "backfill_scores",
        f"{sport.upper()} scores: {score_count} rows updated",
        {"details": ", ".join(score_details) if score_details else "no updates"},
    )

    worksheet = get_or_create_worksheet(spreadsheet, worksheet_name)
    existing_games = get_existing_games(worksheet)
    games = fetch_and_parse_schedule_api(sport, date_str)
    print(f"Found {len(games)} {sport.upper()} games")

    new_count, updated_count, changes = write_games_to_sheet(
        worksheet, games, existing_games, timestamp
    )
    print(f"✅ {sport.upper()}: Added {new_count} new games, updated {updated_count} odds")

    log_activity(
        spreadsheet,
        "fetch_schedule",
        f"{sport.upper()} {formatted_date}: {new_count} rows added, {updated_count} rows updated",
        {"games_fetched": len(games), "details": ", ".join(changes) if changes else "no changes"},
    )


def main(target_date: Optional[str] = None, sport: Optional[str] = None, score_limit: Optional[int] = None):
    eastern = ZoneInfo("America/New_York")
    now_eastern = datetime.now(eastern)
    timestamp = now_eastern.strftime("%Y-%m-%d %H:%M:%S")

    if target_date:
        date_str = target_date.replace("-", "")
        formatted_date = target_date
    else:
        date_str = now_eastern.strftime("%Y%m%d")
        formatted_date = now_eastern.strftime("%Y-%m-%d")

    sports = [sport] if sport else list(SPORT_WORKSHEETS.keys())
    print(f"\n[{timestamp}] Fetching ESPN schedules for {formatted_date} — sports: {', '.join(sports)}")

    try:
        client = get_gspread_client()
        spreadsheet = client.open_by_key(GOOGLE_SHEET_ID)

        labels = {"nba": "📊 NBA", "cbb": "🏀 College Basketball", "nhl": "🏒 NHL", "nfl": "🏈 NFL", "cfb": "🏈 College Football"}
        for s in sports:
            print(f"\n{labels[s]}...")
            run_sport(spreadsheet, s, date_str, formatted_date, timestamp, score_limit=score_limit)

        print(f"\n✅ Done!")

    except ValueError as e:
        print(f"Error: {e}")
        exit(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch ESPN schedules and backfill scores")
    parser.add_argument("date", nargs="?", help="Target date in YYYY-MM-DD format (default: today)")
    parser.add_argument("--sport", choices=["nba", "cbb", "nhl", "nfl", "cfb"], help="Only run this sport (default: all)")
    parser.add_argument("--limit", type=int, help="Max rows to backfill scores for (default: all)")
    args = parser.parse_args()

    main(target_date=args.date, sport=args.sport, score_limit=args.limit)
