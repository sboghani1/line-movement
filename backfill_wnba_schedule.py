#!/usr/bin/env python3
"""
Backfill WNBA schedule data from ESPN for historical seasons.

WNBA season runs roughly May-October. This script fetches day-by-day
from ESPN's API and writes to the WNBA Google Sheet.

Usage:
    python backfill_wnba_schedule.py [--start-date YYYY-MM-DD] [--end-date YYYY-MM-DD] [--dry-run] [--limit N]

    Defaults to 2021-05-01 through yesterday.
"""

import argparse
import time
import sys
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

from dotenv import load_dotenv
load_dotenv()

from espn_schedule_fetcher import (
    WNBA_SHEET_ID,
    WNBA_WORKSHEET_NAME,
    get_gspread_client,
    get_or_create_worksheet,
    get_existing_games,
    fetch_and_parse_schedule_api,
    fetch_espn_results,
)
from sheets_utils import sheets_call


def main():
    parser = argparse.ArgumentParser(description="Backfill WNBA schedule from ESPN")
    parser.add_argument("--start-date", default="2021-05-01", help="Start date (default: 2021-05-01)")
    parser.add_argument("--end-date", help="End date (default: yesterday)")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--limit", type=int, help="Max dates to process")
    args = parser.parse_args()

    start = date.fromisoformat(args.start_date)
    end = date.fromisoformat(args.end_date) if args.end_date else date.today() - timedelta(days=1)

    dates = []
    d = start
    while d <= end:
        # WNBA season: May through October
        if 5 <= d.month <= 10:
            dates.append(d)
        d += timedelta(days=1)

    print(f"WNBA backfill: {len(dates)} dates from {start} to {end}")

    if args.limit:
        dates = dates[:args.limit]
        print(f"  Limited to {args.limit} dates")

    client = get_gspread_client()
    ss = client.open_by_key(WNBA_SHEET_ID)
    ws = get_or_create_worksheet(ss, WNBA_WORKSHEET_NAME)
    time.sleep(1.5)
    existing = get_existing_games(ws)
    print(f"  {len(existing)} existing rows in sheet")

    fetch_timestamp = datetime.now(ZoneInfo("America/New_York")).strftime("%Y-%m-%d %H:%M:%S")
    total_written = 0
    start_time = time.monotonic()

    for i, game_date in enumerate(dates):
        date_str = game_date.isoformat().replace("-", "")

        try:
            games = fetch_and_parse_schedule_api("wnba", date_str)
        except Exception as e:
            print(f"  [{i+1}/{len(dates)}] {game_date}: ERROR fetching schedule: {e}")
            time.sleep(2)
            continue

        if not games:
            # Print progress every 30 days even if no games
            if i % 30 == 0:
                elapsed = time.monotonic() - start_time
                print(f"  [{i+1}/{len(dates)}] {game_date}: 0 games  [{elapsed:.0f}s elapsed]")
            continue

        # Fetch scores
        try:
            raw_scores = fetch_espn_results("wnba", date_str)
        except Exception as e:
            print(f"  [{i+1}/{len(dates)}] {game_date}: ERROR fetching scores: {e}")
            raw_scores = {}

        for game in games:
            result = raw_scores.get((game["away_team"], game["home_team"]))
            if result:
                raw, period_scores_str = result
                parts = raw.split(", ")
                if len(parts) == 2:
                    away_score = parts[0].rsplit(" ", 1)[-1]
                    home_score = parts[1].rsplit(" ", 1)[-1]
                    game["score"] = f"{game['away_team']} {away_score}, {game['home_team']} {home_score}"
                else:
                    game["score"] = raw
                game["period_scores"] = period_scores_str
            else:
                game["score"] = ""
                game["period_scores"] = ""

        # Filter to new games only
        new_rows = []
        for game in games:
            key = f"{game['game_date']}|{game['away_team']}|{game['home_team']}"
            if key in existing:
                continue
            new_rows.append([
                fetch_timestamp,
                game["game_date"],
                game["away_team"],
                game["home_team"],
                game["game_time"],
                game.get("spread", ""),
                game.get("over_under", ""),
                game["tv_network"],
                game["venue"],
                game.get("score", ""),
                game.get("period_scores", ""),
                "",  # tags
            ])
            existing[key] = {"row_idx": -1, "spread": "", "over_under": ""}

        if new_rows and not args.dry_run:
            sheets_call(ws.append_rows, new_rows, value_input_option="USER_ENTERED")
            time.sleep(1.5)  # rate limit

        total_written += len(new_rows)
        elapsed = time.monotonic() - start_time
        eta = ""
        if i > 0:
            eta_sec = elapsed / (i + 1) * (len(dates) - i - 1)
            eta = f"  eta {int(eta_sec // 60)}m{int(eta_sec % 60):02d}s"

        if new_rows or i % 30 == 0:
            prefix = "[dry-run] " if args.dry_run else ""
            print(f"  {prefix}[{i+1}/{len(dates)}] {game_date}: {len(new_rows)} new rows  (total: {total_written}){eta}")

    # Sort sheet by game_date ascending
    if not args.dry_run and total_written > 0:
        print("  Sorting sheet by date...")
        time.sleep(2)
        all_vals = sheets_call(ws.get_all_values)
        headers = all_vals[0]
        data = all_vals[1:]
        gi = headers.index("game_date")
        ti = headers.index("game_time")
        data.sort(key=lambda r: (r[gi], r[ti]))
        sheets_call(ws.update, f"A2:L{1 + len(data)}", data)
        print("  Sorted.")

    print(f"\nDone. {total_written} rows written in {time.monotonic() - start_time:.0f}s")


if __name__ == "__main__":
    main()
