#!/usr/bin/env python3
"""
validate_resolver.py — Validate TeamResolver against historical master_sheet data.

Loads all rows from master_sheet, runs each through the resolver, and compares
the resolved team name and game against the existing values.

Reports:
  - Match rate (resolver found the same ESPN name as the sheet)
  - Method breakdown (exact, alias, substring, next_day, wrong_sport)
  - Failures (resolver returned None or a different team)
  - Mismatches (resolver found a team, but it doesn't match the sheet)

Usage:
  .venv/bin/python3 validate_resolver.py                 # full validation
  .venv/bin/python3 validate_resolver.py --limit 500     # first N rows only
  .venv/bin/python3 validate_resolver.py --failures-only # only show failures
"""

import argparse
from collections import Counter

from dotenv import load_dotenv

from sheets_utils import (
    GOOGLE_SHEET_ID,
    get_gspread_client,
    sheets_read,
)
from team_resolver import TeamResolver

load_dotenv()


def main():
    parser = argparse.ArgumentParser(
        description="Validate TeamResolver against master_sheet"
    )
    parser.add_argument(
        "--limit", type=int, default=0,
        help="Process only the first N data rows (0 = all)"
    )
    parser.add_argument(
        "--failures-only", action="store_true",
        help="Only print rows where resolver failed or mismatched"
    )
    args = parser.parse_args()

    print("Connecting to Google Sheets...")
    gc = get_gspread_client()
    ss = gc.open_by_key(GOOGLE_SHEET_ID)

    # Initialize resolver (loads schedules + aliases)
    resolver = TeamResolver(ss)

    # Load master_sheet
    print("\nLoading master_sheet...")
    ws = sheets_read(ss.worksheet, "master_sheet")
    all_values = sheets_read(ws.get_all_values)

    header = all_values[0]
    col = {h: i for i, h in enumerate(header)}

    date_col = col.get("date", 0)
    sport_col = col.get("sport", 2)
    pick_col = col.get("pick", 3)
    game_col = col.get("game", 5)

    data_rows = all_values[1:]
    if args.limit > 0:
        data_rows = data_rows[:args.limit]

    print(f"  {len(data_rows)} data rows to validate\n")

    # Counters
    total = 0
    resolved = 0
    failed = 0
    skipped = 0
    method_counts = Counter()
    match_counts = Counter()  # exact_match, name_match, game_match, mismatch

    failures = []
    mismatches = []

    for row in data_rows:
        while len(row) <= game_col:
            row.append("")

        date = row[date_col].strip()
        sport = row[sport_col].strip().lower()
        pick = row[pick_col].strip()
        game = row[game_col].strip()

        if not date or not sport or not pick:
            skipped += 1
            continue

        total += 1

        result = resolver.resolve(pick=pick, sport=sport, date=date)

        if result is None:
            failed += 1
            failures.append((date, sport, pick, game))
            continue

        resolved += 1
        method_counts[result.method] += 1

        # Compare result vs sheet
        if result.espn_name == pick and result.game == game:
            match_counts["exact_match"] += 1
        elif result.espn_name == pick:
            # Same team name, different game string (formatting?)
            match_counts["name_match"] += 1
        elif result.game == game:
            # Same game, but resolved to a different team name (alias applied)
            match_counts["game_match"] += 1
        else:
            match_counts["mismatch"] += 1
            mismatches.append((date, sport, pick, game, result.espn_name, result.game, result.method))

    # ── Report ────────────────────────────────────────────────────────────
    print("=" * 70)
    print("VALIDATION RESULTS")
    print("=" * 70)
    print(f"  Total rows:      {total}")
    print(f"  Resolved:        {resolved} ({resolved/total*100:.1f}%)" if total else "")
    print(f"  Failed:          {failed} ({failed/total*100:.1f}%)" if total else "")
    print(f"  Skipped (empty): {skipped}")

    print(f"\n  Resolution methods:")
    for method, count in sorted(method_counts.items(), key=lambda x: -x[1]):
        print(f"    {method:15s}: {count:5d} ({count/resolved*100:.1f}%)" if resolved else "")

    print(f"\n  Match quality (vs existing sheet data):")
    for match_type, count in sorted(match_counts.items(), key=lambda x: -x[1]):
        print(f"    {match_type:15s}: {count:5d} ({count/resolved*100:.1f}%)" if resolved else "")

    if not args.failures_only:
        if mismatches:
            print(f"\n  Mismatches ({len(mismatches)}):")
            for date, sport, pick, game, res_name, res_game, method in mismatches[:30]:
                print(f"    [{date}] {sport:3s} | pick={pick!r} game={game!r}")
                print(f"           resolved: name={res_name!r} game={res_game!r} method={method}")
            if len(mismatches) > 30:
                print(f"    ... and {len(mismatches) - 30} more")

    if failures:
        print(f"\n  Failures ({len(failures)}):")
        # Group failures by pick for readability
        fail_by_pick = Counter()
        for date, sport, pick, game in failures:
            fail_by_pick[(sport, pick)] += 1

        for (sport, pick), count in fail_by_pick.most_common(40):
            # Find one example date for context
            example = next(
                (date, game) for date, s, p, game in failures
                if s == sport and p == pick
            )
            print(f"    [{sport:3s}] {pick!r:45s} × {count} (e.g. {example[0]}, game={example[1]!r})")
        if len(fail_by_pick) > 40:
            print(f"    ... and {len(fail_by_pick) - 40} more unique pick values")

    print()


if __name__ == "__main__":
    main()
