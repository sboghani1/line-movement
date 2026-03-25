#!/usr/bin/env python3
"""
backfill_spread.py — Fix the `spread` column in master_sheet (and optionally
parsed_picks_new) by looking up the correct spread from the ESPN schedule sheets.

Spread is a property of the game (ESPN consensus line), not the bet type.
All rows get the schedule spread, including ML bets.

Composite key: game column ("Away @ Home") → exact match in schedule.
Fallback: fuzzy match on pick team (only for rows with empty game column).

Usage:
  .venv/bin/python3 backfill_spread.py --dry-run          # preview mismatches
  .venv/bin/python3 backfill_spread.py                     # fix master_sheet + parsed_picks_new
  .venv/bin/python3 backfill_spread.py --sheet master_sheet --dry-run  # one sheet only
"""

import re
import time
import argparse
from collections import defaultdict
from typing import Optional, Tuple

import gspread
from dotenv import load_dotenv

from sheets_utils import (
    GOOGLE_SHEET_ID,
    SPORT_TO_SCHED,
    get_gspread_client,
    sheets_read,
    sheets_write,
)

load_dotenv()

MASTER_SHEET = "master_sheet"
PICKS_NEW_SHEET = "parsed_picks_new"

# ── Team-name matching (from populate_stage2.py) ────────────────────────────
_NOISE = re.compile(
    r'\b(blue devils?|crimson tide|golden eagles?|golden bears?|golden gophers?|'
    r'golden knights?|golden flashes?|golden panthers?|golden bulls?|'
    r'red hawks?|red raiders?|red storm|red foxes?|'
    r'fighting irish|fighting illini|fighting hawks?|'
    r'tar heels?|hoyas?|retrievers?|'
    r'wolverines?|buckeyes?|badgers?|hawkeyes?|cyclones?|'
    r'cornhuskers?|huskers?|longhorns?|sooners?|jayhawks?|wildcats?|'
    r'bulldogs?|tigers?|bears?|wolves?|timberwolves?|'
    r'cavaliers?|cavs?|celtics?|nets?|knicks?|bucks?|bulls?|'
    r'lakers?|clippers?|suns?|heat|magic|pistons?|pacers?|'
    r'hawks?|hornets?|wizards?|raptors?|grizzlies?|pelicans?|'
    r'spurs?|mavericks?|mavs?|rockets?|thunder|nuggets?|jazz|'
    r'trail blazers?|blazers?|kings?|warriors?|'
    r'lightning|panthers?|rangers?|islanders?|devils?|flyers?|'
    r'penguins?|capitals?|caps?|hurricanes?|canes?|'
    r'blue jackets?|red wings?|bruins?|canadiens?|habs?|'
    r'senators?|maple leafs?|leafs?|sabres?|wild|blackhawks?|'
    r'blues?|avalanche?|avs?|stars?|predators?|preds?|jets?|'
    r'coyotes?|canucks?|flames?|oilers?|sharks?|ducks?|kraken|'
    r'trojans?|rebels?|miners?|roadrunners?|'
    r'ramblers?|racers?|chippewas?|broncos?|falcons?|'
    r'antelopes?|lions?|billikens?|midshipmen|black knights?|'
    r'tommies?|owls?|jaguars?|rams?|aztecs?|spartans?|'
    r'aggies?|monarchs?|hilltoppers?|eagles?|cardinals?|'
    r'seminoles?|wolfpack|mountaineers?|bearcats?|huskies?|'
    r'terrapins?|terps?|nittany lions?|boilermakers?|hoosiers?|illini|'
    r'commodores?|volunteers?|gators?|gamecocks?|razorbacks?|'
    r'horned frogs?|cowboys?|sun devils?|utes?|lobos?|pokes?|'
    r'lumberjacks?|thunderbirds?|greyhounds?|retrievers?|'
    r'shockers?|spiders?|flyers?|musketeers?|friars?|hoyas?|'
    r'chanticleers?|penguins?|redhawks?|buffaloes?|buffs?|'
    r'hokies?|demon deacons?|yellow jackets?|scarlet knights?|'
    r'mean green|49ers?|aggies?|pride|ospreys?)\b',
    re.IGNORECASE,
)


def normalize(name: str) -> str:
    n = name.lower().strip()
    n = _NOISE.sub('', n)
    n = re.sub(r'\s+', ' ', n).strip()
    return n


def team_matches(pick_name: str, schedule_name: str) -> bool:
    """Fuzzy team-name match: exact, substring, or normalized."""
    p = pick_name.lower().strip()
    s = schedule_name.lower().strip()
    if p == s:
        return True
    if p in s or s in p:
        return True
    pn = normalize(pick_name)
    sn = normalize(schedule_name)
    if pn and sn and (pn == sn or pn in sn or sn in pn):
        return True
    return False


# ── Schedule loader ──────────────────────────────────────────────────────────
def load_schedules(ss) -> dict:
    """
    Load all schedule data from all sport schedule sheets.

    Returns:
        {sport: {game_date: [(away_team, home_team, spread), ...]}}
    """
    schedules = {}
    for sport, sheet_name in SPORT_TO_SCHED.items():
        try:
            ws = sheets_read(ss.worksheet, sheet_name)
            rows = sheets_read(ws.get_all_values)
        except gspread.exceptions.WorksheetNotFound:
            print(f"  {sheet_name}: not found, skipping")
            schedules[sport] = {}
            continue

        if not rows:
            schedules[sport] = {}
            continue

        headers = rows[0]
        hcol = {h: i for i, h in enumerate(headers)}
        by_date = defaultdict(list)

        for row in rows[1:]:
            while len(row) < len(headers):
                row.append("")
            game_date = row[hcol.get("game_date", 1)].strip()
            away = row[hcol.get("away_team", 2)].strip()
            home = row[hcol.get("home_team", 3)].strip()
            spread = row[hcol.get("spread", 5)].strip()
            if game_date and away and home:
                by_date[game_date].append((away, home, spread))

        schedules[sport] = by_date
        total = sum(len(v) for v in by_date.values())
        print(f"  {sheet_name}: {total} games loaded")

    return schedules


def find_game_by_game_col(
    game: str, date: str, sport: str, schedules: dict
) -> Optional[Tuple[str, str, str]]:
    """Return (away, home, spread) using the game column (exact key match).

    The game column is formatted as 'Away Team @ Home Team'. We parse it and
    look for an exact match in the schedule, falling back to fuzzy matching
    against the parsed away/home names.
    """
    game_parts = game.split(" @ ", 1)
    if len(game_parts) != 2:
        return None

    game_away, game_home = game_parts[0].strip(), game_parts[1].strip()

    for away, home, spread in schedules.get(sport, {}).get(date, []):
        # Exact match on both teams (most reliable)
        if away == game_away and home == game_home:
            return away, home, spread

    # Fallback: fuzzy match on the game column teams
    for away, home, spread in schedules.get(sport, {}).get(date, []):
        if team_matches(game_away, away) and team_matches(game_home, home):
            return away, home, spread

    return None


def find_game_by_pick(
    pick: str, date: str, sport: str, schedules: dict
) -> Optional[Tuple[str, str, str]]:
    """Return (away, home, spread) by fuzzy-matching the pick team name.

    Only used as a fallback when the game column is empty.
    """
    for away, home, spread in schedules.get(sport, {}).get(date, []):
        if team_matches(pick, away) or team_matches(pick, home):
            return away, home, spread
    return None


# ── Sheet processor ──────────────────────────────────────────────────────────
def process_sheet(
    ss,
    sheet_name: str,
    header_row_index: int,
    schedules: dict,
    dry_run: bool,
) -> dict:
    """
    Compare and optionally fix the spread column for every row in the sheet.

    Returns a summary dict with counts.
    """
    ws = sheets_read(ss.worksheet, sheet_name)
    all_values = sheets_read(ws.get_all_values)

    if len(all_values) <= header_row_index:
        print(f"  {sheet_name}: no data rows")
        return {"already_correct": 0, "fixed": 0,
                "no_match": 0, "skipped_empty": 0}

    header = all_values[header_row_index]
    col = {h: i for i, h in enumerate(header)}

    date_col = col.get("date", 0)
    sport_col = col.get("sport", 2)
    pick_col = col.get("pick", 3)
    line_col = col.get("line", 4)
    game_col = col.get("game", 5)
    spread_col = col.get("spread", 6)

    data_start = header_row_index + 1

    already_correct = 0
    fixed = 0
    no_match = 0
    skipped_empty = 0
    batch_updates = []

    mismatches = []  # for dry-run reporting

    for offset, row in enumerate(all_values[data_start:]):
        # Skip blank separator rows
        if not any(cell.strip() for cell in row):
            continue

        # Ensure row has enough columns
        while len(row) <= spread_col:
            row.append("")

        date = row[date_col].strip() if len(row) > date_col else ""
        sport = row[sport_col].strip().lower() if len(row) > sport_col else ""
        pick = row[pick_col].strip() if len(row) > pick_col else ""
        line = row[line_col].strip() if len(row) > line_col else ""
        game = row[game_col].strip() if len(row) > game_col else ""
        current_spread = row[spread_col].strip() if len(row) > spread_col else ""

        if not date or not sport or not pick:
            skipped_empty += 1
            continue

        # Look up game from schedule — prefer game column (exact), fall back to pick (fuzzy)
        if game:
            result = find_game_by_game_col(game, date, sport, schedules)
        else:
            result = find_game_by_pick(pick, date, sport, schedules)
        if result is None:
            no_match += 1
            continue

        _away, _home, sched_spread = result

        if current_spread == sched_spread:
            already_correct += 1
        else:
            fixed += 1
            sheet_row = data_start + offset + 1  # 1-based
            result_cell = gspread.utils.rowcol_to_a1(sheet_row, spread_col + 1)
            if dry_run:
                mismatches.append(
                    f"  [{date}] {pick} {line} | {repr(current_spread)} → {repr(sched_spread)}"
                )
            else:
                batch_updates.append({"range": result_cell, "values": [[sched_spread]]})

    # Print mismatches in dry-run mode
    if dry_run and mismatches:
        print(f"\n  Mismatches ({len(mismatches)}):")
        for m in mismatches[:50]:
            print(m)
        if len(mismatches) > 50:
            print(f"  ... and {len(mismatches) - 50} more")

    # Write updates
    if not dry_run and batch_updates:
        chunk_size = 500
        for i in range(0, len(batch_updates), chunk_size):
            sheets_write(ws.batch_update, batch_updates[i : i + chunk_size])
            if i + chunk_size < len(batch_updates):
                time.sleep(1)
        print(f"  Wrote {len(batch_updates)} spread updates to {sheet_name}")

    return {
        "already_correct": already_correct,
        "fixed": fixed,
        "no_match": no_match,
        "skipped_empty": skipped_empty,
    }


# ── Main ─────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(
        description="Backfill spread column in master_sheet from schedule sheets"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview mismatches without writing",
    )
    parser.add_argument(
        "--sheet",
        choices=["master_sheet", "parsed_picks_new", "both"],
        default="both",
        help="Which sheet to update (default: both)",
    )
    args = parser.parse_args()

    print("Connecting to Google Sheets...")
    gc = get_gspread_client()
    ss = gc.open_by_key(GOOGLE_SHEET_ID)

    print("\nLoading schedules from all sport sheets...")
    schedules = load_schedules(ss)

    sheets_to_run = []
    if args.sheet in ("master_sheet", "both"):
        # master_sheet: header on row 1 (index 0), data from row 2
        sheets_to_run.append((MASTER_SHEET, 0))
    if args.sheet in ("parsed_picks_new", "both"):
        # parsed_picks_new: metadata rows 1-2, header on row 3 (index 2), data from row 4
        sheets_to_run.append((PICKS_NEW_SHEET, 2))

    for sheet_name, header_row_index in sheets_to_run:
        print(f"\nProcessing {sheet_name}...")
        stats = process_sheet(ss, sheet_name, header_row_index, schedules, args.dry_run)
        print(f"\n  Summary for {sheet_name}:")
        print(f"    Already correct:  {stats['already_correct']}")
        print(f"    Fixed (updated):  {stats['fixed']}")
        print(f"    No schedule match:{stats['no_match']}")
        print(f"    Skipped (empty):  {stats['skipped_empty']}")

    if args.dry_run:
        print("\n[dry-run] No changes written.")
    else:
        print("\nDone.")


if __name__ == "__main__":
    main()
