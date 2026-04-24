#!/usr/bin/env python3
"""
Backfill NBA playoff games (2016–2025) into nba_schedule sheet.

Fetches schedule + scores from ESPN for each date in each playoff window,
writes to Google Sheets, then tags each season with round/game numbers.

Usage:
    python backfill_nba_playoffs.py [--dry-run] [--season YYYY] [--limit N]
"""

import argparse
import os
import time
from datetime import datetime, timedelta
from pathlib import Path

from dotenv import load_dotenv
# Load .env from the main repo if running in a worktree
env_path = Path(__file__).resolve().parent / ".env"
if not env_path.exists():
    env_path = Path(__file__).resolve().parent.parent / "line-movement" / ".env"
load_dotenv(env_path)

import gspread

from espn_schedule_fetcher import (
    fetch_and_parse_schedule_api,
    fetch_espn_results,
    get_existing_games,
    get_gspread_client,
    get_or_create_worksheet,
    write_games_to_sheet,
    GOOGLE_SHEET_ID,
    NBA_WORKSHEET_NAME,
)
from sheets_utils import sheets_call
from tag_nba_playoffs import tag_playoff_games

# Hardcoded playoff date windows (inclusive)
PLAYOFF_WINDOWS = {
    2016: ("2016-04-16", "2016-06-19"),
    2017: ("2017-04-15", "2017-06-12"),
    2018: ("2018-04-14", "2018-06-08"),
    2019: ("2019-04-13", "2019-06-13"),
    2020: ("2020-08-17", "2020-10-11"),
    2021: ("2021-05-22", "2021-07-20"),
    2022: ("2022-04-16", "2022-06-16"),
    2023: ("2023-04-15", "2023-06-12"),
    2024: ("2024-04-20", "2024-06-17"),
    2025: ("2025-04-19", "2025-06-22"),
}


def date_range(start_str, end_str):
    """Yield YYYY-MM-DD strings from start to end inclusive."""
    start = datetime.strptime(start_str, "%Y-%m-%d").date()
    end = datetime.strptime(end_str, "%Y-%m-%d").date()
    d = start
    while d <= end:
        yield d.strftime("%Y-%m-%d")
        d += timedelta(days=1)


def backfill_season(spreadsheet, season, dry_run=False, limit=None):
    """Backfill one playoff season."""
    start_str, end_str = PLAYOFF_WINDOWS[season]
    dates = list(date_range(start_str, end_str))
    print(f"\n{'='*60}")
    print(f"Season {season}: {start_str} → {end_str} ({len(dates)} days)")
    print(f"{'='*60}")

    if dry_run:
        print(f"  (dry run — would fetch {len(dates)} days)")
        return

    worksheet = get_or_create_worksheet(spreadsheet, NBA_WORKSHEET_NAME)
    existing_games = get_existing_games(worksheet)
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    total_new = 0
    score_batch = []  # collect score updates to write after schedule rows

    for date_str in dates:
        date_yyyymmdd = date_str.replace("-", "")

        # Fetch schedule
        try:
            games = fetch_and_parse_schedule_api("nba", date_yyyymmdd)
        except Exception as e:
            print(f"  ⚠ {date_str}: schedule fetch failed: {e}")
            continue

        if not games:
            continue

        # Fetch scores for completed games
        try:
            espn_results = fetch_espn_results("nba", date_yyyymmdd)
        except Exception as e:
            print(f"  ⚠ {date_str}: score fetch failed: {e}")
            espn_results = {}

        # Re-read existing games periodically to pick up rows we just wrote
        if total_new > 0 and total_new % 50 == 0:
            existing_games = get_existing_games(worksheet)

        new_count, updated_count, _ = write_games_to_sheet(
            worksheet, games, existing_games, timestamp
        )
        total_new += new_count

        if new_count > 0:
            print(f"  {date_str}: +{new_count} games ({len(games)} found)")

        # After writing rows, re-read to get row indices for score updates
        if new_count > 0 and espn_results:
            # We'll batch score updates per date and write them after all rows are in
            score_batch.append((date_str, espn_results))

        if limit and total_new >= limit:
            print(f"  Reached limit of {limit} new games")
            break

        # Brief pause to respect rate limits
        time.sleep(0.5)

    # Now backfill scores — re-read all rows to get correct row indices
    if score_batch:
        print(f"\nBackfilling scores for {len(score_batch)} dates...")
        existing_games = get_existing_games(worksheet)
        batch_updates = []
        for date_str, espn_results in score_batch:
            for (away, home), (score_str, period_str) in espn_results.items():
                key = f"{date_str}|{away}|{home}"
                if key in existing_games:
                    row_idx = existing_games[key]["row_idx"]
                    if not existing_games[key]["score"]:
                        batch_updates.append({
                            "range": gspread.utils.rowcol_to_a1(row_idx, 10),  # score col J
                            "values": [[score_str]],
                        })
                        if period_str:
                            batch_updates.append({
                                "range": gspread.utils.rowcol_to_a1(row_idx, 11),  # period_scores col K
                                "values": [[period_str]],
                            })

        if batch_updates:
            # Write in chunks to avoid huge batch requests
            chunk_size = 100
            for i in range(0, len(batch_updates), chunk_size):
                chunk = batch_updates[i:i + chunk_size]
                sheets_call(worksheet.batch_update, chunk)
                print(f"  Wrote {len(chunk)} score updates")
                time.sleep(1)

    print(f"\nSeason {season} complete: {total_new} new games written")

    # Tag playoff games for this season
    if total_new > 0:
        print(f"Tagging playoff games for {season}...")
        tag_playoff_games(spreadsheet, start_date_str=start_str, end_date_str=end_str)


def main():
    parser = argparse.ArgumentParser(description="Backfill NBA playoff games 2016-2025")
    parser.add_argument("--dry-run", action="store_true", help="Preview without writing")
    parser.add_argument("--season", type=int, help="Only backfill this season (e.g. 2016)")
    parser.add_argument("--limit", type=int, help="Max new games per season")
    args = parser.parse_args()

    if args.season and args.season not in PLAYOFF_WINDOWS:
        print(f"Unknown season {args.season}. Available: {sorted(PLAYOFF_WINDOWS.keys())}")
        return

    seasons = [args.season] if args.season else sorted(PLAYOFF_WINDOWS.keys())

    if args.dry_run:
        total_days = 0
        for s in seasons:
            start, end = PLAYOFF_WINDOWS[s]
            days = len(list(date_range(start, end)))
            total_days += days
            print(f"  {s}: {start} → {end} ({days} days)")
        print(f"\nTotal: {len(seasons)} seasons, {total_days} days to fetch")
        return

    print("Connecting to Google Sheets...")
    gc = get_gspread_client()
    spreadsheet = gc.open_by_key(GOOGLE_SHEET_ID)

    for season in seasons:
        backfill_season(spreadsheet, season, dry_run=args.dry_run, limit=args.limit)

    print(f"\n✅ Backfill complete!")


if __name__ == "__main__":
    main()
