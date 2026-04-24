#!/usr/bin/env python3
"""
Tag NBA playoff games in nba_schedule with round and game numbers.

Round is determined by tracking unique opponents per team in chronological order.
Game number counts occurrences of each matchup (unordered pair) since playoff start.

Usage:
    python tag_nba_playoffs.py [--start-date 2026-04-18] [--dry-run] [--validate-only]
"""

import argparse
from collections import defaultdict
from datetime import datetime

from sheets_utils import (
    GOOGLE_SHEET_ID,
    get_gspread_client,
    sheets_read,
    sheets_write,
)

WORKSHEET_NAME = "nba_schedule"
DEFAULT_PLAYOFF_START = "2026-04-18"
TAGS_COL_INDEX = 11  # 0-based index for column L


def parse_args():
    parser = argparse.ArgumentParser(description="Tag NBA playoff games with round/game numbers")
    parser.add_argument("--start-date", default=DEFAULT_PLAYOFF_START,
                        help="Playoff start date YYYY-MM-DD (default: %(default)s)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Preview tags without writing")
    parser.add_argument("--validate-only", action="store_true",
                        help="Only check existing tags against computed values")
    return parser.parse_args()


def matchup_key(team_a, team_b):
    """Canonical key for an unordered pair of teams."""
    return tuple(sorted([team_a, team_b]))


def compute_tags(rows, start_date, end_date=None):
    """Compute playoff tags for rows on or after start_date (and before end_date if given).

    Args:
        rows: list of dicts with keys: row_num (1-based sheet row), game_date, away_team, home_team, tags
        start_date: datetime.date for playoff start
        end_date: optional datetime.date upper bound (exclusive) for season isolation

    Returns:
        list of (row_num, computed_tag, existing_tag) for playoff rows
    """
    # Filter and sort playoff games by date
    playoff_rows = []
    for r in rows:
        try:
            gd = datetime.strptime(r["game_date"], "%Y-%m-%d").date()
        except (ValueError, KeyError):
            continue
        if gd >= start_date and (end_date is None or gd <= end_date):
            playoff_rows.append({**r, "_parsed_date": gd})

    playoff_rows.sort(key=lambda r: r["_parsed_date"])

    # Track unique opponents per team (chronological order) → round mapping
    team_opponents = defaultdict(list)  # team → [opponent1, opponent2, ...]
    matchup_count = defaultdict(int)    # matchup_key → count so far

    results = []
    for r in playoff_rows:
        away = r["away_team"]
        home = r["home_team"]
        mk = matchup_key(away, home)

        # Determine round: based on when this opponent first appears for each team
        for team, opp in [(away, home), (home, away)]:
            if opp not in team_opponents[team]:
                team_opponents[team].append(opp)

        # Round = position of this opponent in team's unique opponent list (1-based)
        # Both teams should agree; use away team's perspective (they must match)
        round_num = team_opponents[away].index(home) + 1
        # Sanity check: home team should agree
        home_round = team_opponents[home].index(away) + 1
        if round_num != home_round:
            print(f"  ⚠ Round mismatch row {r['row_num']}: {away}=R{round_num}, {home}=R{home_round}. Using {round_num}.")

        # Game number: increment count for this matchup
        matchup_count[mk] += 1
        game_num = matchup_count[mk]

        tag = f"round_{round_num},game_{game_num}"
        results.append((r["row_num"], tag, r["tags"]))

    return results


def tag_playoff_games(spreadsheet, start_date_str=DEFAULT_PLAYOFF_START, end_date_str=None, dry_run=False, validate_only=False):
    """Tag playoff games in nba_schedule. Can be called standalone or from another script.

    Args:
        spreadsheet: gspread Spreadsheet object (already authenticated)
        start_date_str: playoff start date as YYYY-MM-DD string
        end_date_str: optional end date as YYYY-MM-DD string (inclusive) for season isolation
        dry_run: preview without writing
        validate_only: only report mismatches
    """
    start_date = datetime.strptime(start_date_str, "%Y-%m-%d").date()
    end_date = datetime.strptime(end_date_str, "%Y-%m-%d").date() if end_date_str else None
    worksheet = sheets_read(spreadsheet.worksheet, WORKSHEET_NAME)

    print(f"Reading {WORKSHEET_NAME} for playoff tags...")
    all_values = sheets_read(worksheet.get_all_values)

    if not all_values:
        print("Sheet is empty.")
        return

    # Parse rows (skip header row)
    rows = []
    for i, row in enumerate(all_values[1:], start=2):  # row 2 in sheet = index 1
        # Pad row to ensure tags column exists
        while len(row) <= TAGS_COL_INDEX:
            row.append("")
        rows.append({
            "row_num": i,
            "game_date": row[1],   # column B
            "away_team": row[2],   # column C
            "home_team": row[3],   # column D
            "tags": row[TAGS_COL_INDEX],  # column L
        })

    print(f"Found {len(rows)} data rows. Playoff start: {start_date}" + (f", end: {end_date}" if end_date else ""))
    results = compute_tags(rows, start_date, end_date)
    print(f"Found {len(results)} playoff games.")

    if not results:
        print("No playoff games found.")
        return

    # Categorize
    correct = [(rn, tag, ex) for rn, tag, ex in results if ex == tag]
    missing = [(rn, tag, ex) for rn, tag, ex in results if ex == ""]
    mismatched = [(rn, tag, ex) for rn, tag, ex in results if ex != "" and ex != tag]
    to_write = missing + mismatched

    print(f"\nPlayoff tag results:")
    print(f"  Already correct: {len(correct)}")
    print(f"  Missing (empty): {len(missing)}")
    print(f"  Mismatched:      {len(mismatched)}")

    if mismatched:
        print(f"\nMismatched tags:")
        for rn, tag, ex in mismatched:
            print(f"  Row {rn}: existing='{ex}' → computed='{tag}'")

    if validate_only:
        if not mismatched and not missing:
            print("\n✅ All playoff tags are correct.")
        return

    if not to_write:
        print("\n✅ Nothing to update.")
        return

    # Show preview
    print(f"\nTags to write ({len(to_write)} rows):")
    for rn, tag, ex in to_write[:20]:
        action = "FIX" if ex else "SET"
        print(f"  Row {rn}: {action} → '{tag}'" + (f" (was '{ex}')" if ex else ""))
    if len(to_write) > 20:
        print(f"  ... and {len(to_write) - 20} more")

    if dry_run:
        print("\n(dry run — no changes written)")
        return

    # Write tags via batch_update (column L)
    batch = []
    for rn, tag, _ in to_write:
        batch.append({
            "range": f"L{rn}",
            "values": [[tag]],
        })

    print(f"\nWriting {len(batch)} tags...")
    sheets_write(worksheet.batch_update, batch, value_input_option="USER_ENTERED")
    print("✅ Playoff tags updated.")


def main():
    args = parse_args()
    print(f"Connecting to Google Sheets...")
    gc = get_gspread_client()
    spreadsheet = gc.open_by_key(GOOGLE_SHEET_ID)
    tag_playoff_games(spreadsheet, start_date_str=args.start_date,
                      dry_run=args.dry_run, validate_only=args.validate_only)


if __name__ == "__main__":
    main()
