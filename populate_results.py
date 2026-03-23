#!/usr/bin/env python3
"""
populate_results.py — Fill the `result` column (win/lose/push) in master_sheet
and parsed_picks_new by reading completed game scores from the schedule sheets.

Score source: {sport}_schedule sheets, `score` column.
Score format: "Away Team SCORE, Home Team SCORE"
  e.g. "Memphis Grizzlies 110, Detroit Pistons 126"

Result logic:
  ML bet:     pick team wins the game outright → win; loses outright → lose
  Spread bet: pick team covers the spread → win; fails to cover → lose; exact → push
              e.g. pick=TeamA line=-3.5: TeamA must win by >3.5 to cover
              e.g. pick=TeamA line=+7:  TeamA must not lose by more than 7 to cover

Only rows with a non-empty `game` and empty `result` are updated.
Rows where the schedule score is blank, "N/A", or the game is not yet complete
are skipped (left blank so a future run can fill them in).

Usage:
  .venv/bin/python3 populate_results.py --dry-run   # preview without writing
  .venv/bin/python3 populate_results.py              # write to both sheets
  .venv/bin/python3 populate_results.py --sheet master_sheet  # one sheet only
"""

import os
import re
import time
import argparse
from collections import defaultdict
from typing import Optional

import gspread
from dotenv import load_dotenv

from sheets_utils import GOOGLE_SHEET_ID, get_gspread_client, sheets_call, SPORT_TO_SCHED

load_dotenv()

# master_sheet columns (0-indexed, header is row 1)
MASTER_HEADERS  = ["date", "capper", "sport", "pick", "line", "game", "spread", "side", "result"]
# parsed_picks_new header is on row 3 (index 2), data starts row 4
PICKS_NEW_SHEET    = "parsed_picks_new"
MASTER_SHEET       = "master_sheet"
MASTER_SHEET_NEW   = "master_sheet_new"



# ── Score parsing ─────────────────────────────────────────────────────────────
def parse_score_string(score_str: str) -> Optional[tuple[str, int, str, int]]:
    """
    Parse ESPN score string into (away_team, away_score, home_team, home_score).

    Format: "Away Team SCORE, Home Team SCORE"
    e.g.   "Memphis Grizzlies 110, Detroit Pistons 126"
           "Colorado Avalanche 3, Dallas Stars 4"

    Returns None if the string can't be parsed or scores are non-numeric.
    """
    # Split on the comma that separates the two teams
    parts = score_str.split(", ", 1)
    if len(parts) != 2:
        return None

    # Each part ends with a score (integer or decimal for OT hockey)
    # e.g. "Memphis Grizzlies 110"  or  "Colorado Avalanche 3"
    pattern = re.compile(r'^(.+?)\s+(\d+(?:\.\d+)?)$')
    m1 = pattern.match(parts[0].strip())
    m2 = pattern.match(parts[1].strip())
    if not m1 or not m2:
        return None

    away_team  = m1.group(1).strip()
    away_score = float(m1.group(2))
    home_team  = m2.group(1).strip()
    home_score = float(m2.group(2))
    return away_team, away_score, home_team, home_score


def team_matches(pick_name: str, schedule_name: str) -> bool:
    """Fuzzy team name match: exact, substring, or acronym."""
    p = pick_name.lower().strip()
    s = schedule_name.lower().strip()
    return p == s or p in s or s in p


def determine_result(
    pick: str,
    line: str,
    game: str,
    score_str: str,
) -> Optional[str]:
    """
    Determine win/lose/push for a single pick row given the final score string.

    Returns "win", "lose", "push", or None if the result can't be determined
    (missing data, incomplete game, unrecognised score format).
    """
    if not score_str or score_str.strip() in ("", "N/A"):
        return None

    parsed = parse_score_string(score_str.strip())
    if parsed is None:
        return None

    away_name, away_score, home_name, home_score = parsed

    # Identify which side the pick is on by matching against game string
    # game format: "Away Team @ Home Team"
    game_parts = game.split(" @ ", 1)
    if len(game_parts) != 2:
        return None

    game_away, game_home = game_parts[0].strip(), game_parts[1].strip()

    # Determine which team in the score belongs to the pick
    # Use game column to identify away/home order, then match pick to one side
    if team_matches(pick, game_away):
        pick_score  = away_score
        opp_score   = home_score
    elif team_matches(pick, game_home):
        pick_score  = home_score
        opp_score   = away_score
    else:
        # Pick team not found in game string — can't resolve
        return None

    line_clean = line.strip().upper()

    if line_clean == "ML":
        # Moneyline: did the pick team win outright?
        if pick_score > opp_score:
            return "win"
        elif pick_score < opp_score:
            return "lose"
        else:
            return "push"  # tie (rare but possible in some formats)

    # Spread bet: parse the numeric line
    try:
        spread_val = float(line_clean.lstrip("+"))
    except ValueError:
        return None

    # Apply the spread to the pick team's score
    # pick covers if (pick_score + spread_val) > opp_score
    margin = pick_score + spread_val - opp_score
    if margin > 0:
        return "win"
    elif margin < 0:
        return "lose"
    else:
        return "push"


# ── Schedule loader ───────────────────────────────────────────────────────────
def load_scores(ss) -> dict:
    """
    Load completed scores from all schedule sheets.
    Returns {sport: {game_date: {(away_team, home_team): score_str}}}
    """
    scores = {}
    for sport, sheet_name in SPORT_TO_SCHED.items():
        try:
            ws = sheets_call(ss.worksheet, sheet_name)
            rows = sheets_call(ws.get_all_values)
            if not rows:
                scores[sport] = {}
                continue
            headers = rows[0]
            hcol = {h: i for i, h in enumerate(headers)}
            date_idx  = hcol.get("game_date", 1)
            away_idx  = hcol.get("away_team",  2)
            home_idx  = hcol.get("home_team",  3)
            score_idx = hcol.get("score",      9)

            by_date = defaultdict(dict)
            for row in rows[1:]:
                while len(row) <= max(date_idx, away_idx, home_idx, score_idx):
                    row.append("")
                gdate = row[date_idx].strip()
                away  = row[away_idx].strip()
                home  = row[home_idx].strip()
                score = row[score_idx].strip()
                if gdate and away and home and score and score != "N/A":
                    by_date[gdate][(away, home)] = score

            scores[sport] = by_date
            total = sum(len(v) for v in by_date.values())
            print(f"  {sheet_name}: {total} scored games loaded")
        except gspread.exceptions.WorksheetNotFound:
            print(f"  {sheet_name}: sheet not found, skipping")
            scores[sport] = {}
    return scores


def find_score(pick: str, date: str, sport: str, game: str, scores: dict) -> Optional[str]:
    """
    Look up the score string for a pick row.

    Uses the `game` column (Away @ Home) to find the exact match in the
    schedule. Falls back to team-name fuzzy matching if exact key not found.
    """
    sport_scores = scores.get(sport.lower(), {})
    date_scores  = sport_scores.get(date, {})

    if not date_scores:
        return None

    # Prefer exact key match using the game column (Away @ Home)
    game_parts = game.split(" @ ", 1)
    if len(game_parts) == 2:
        away_g, home_g = game_parts[0].strip(), game_parts[1].strip()
        if (away_g, home_g) in date_scores:
            return date_scores[(away_g, home_g)]

    # Fallback: fuzzy match on pick team name
    for (away, home), score_str in date_scores.items():
        if team_matches(pick, away) or team_matches(pick, home):
            return score_str

    return None


# ── Sheet updater ─────────────────────────────────────────────────────────────
def process_sheet(
    ss,
    sheet_name: str,
    header_row_index: int,   # 0-based index in get_all_values() output
    scores: dict,
    dry_run: bool,
) -> tuple[int, int, int]:
    """
    Update the result column for all rows with missing results.

    Returns (resolved, skipped_no_score, skipped_already_set).
    """
    ws = sheets_call(ss.worksheet, sheet_name)
    all_values = sheets_call(ws.get_all_values)

    header = all_values[header_row_index]
    col = {h: i for i, h in enumerate(header)}

    date_col   = col.get("date",   0)
    sport_col  = col.get("sport",  2)
    pick_col   = col.get("pick",   3)
    line_col   = col.get("line",   4)
    game_col   = col.get("game",   5)
    result_col = col.get("result", 8)

    data_start_row_index = header_row_index + 1  # first data row in all_values

    resolved           = 0
    skipped_no_score   = 0
    skipped_already    = 0
    batch_updates      = []

    for offset, row in enumerate(all_values[data_start_row_index:]):
        if not any(cell.strip() for cell in row):
            continue  # blank separator row

        while len(row) <= result_col:
            row.append("")

        existing_result = row[result_col].strip().lower()
        if existing_result in ("win", "lose", "push"):
            skipped_already += 1
            continue

        date  = row[date_col].strip()  if len(row) > date_col  else ""
        sport = row[sport_col].strip() if len(row) > sport_col else ""
        pick  = row[pick_col].strip()  if len(row) > pick_col  else ""
        line  = row[line_col].strip()  if len(row) > line_col  else ""
        game  = row[game_col].strip()  if len(row) > game_col  else ""

        if not game or not pick or not date or not sport:
            skipped_no_score += 1
            continue

        score_str = find_score(pick, date, sport, game, scores)
        if not score_str:
            skipped_no_score += 1
            continue

        result = determine_result(pick, line, game, score_str)
        if result is None:
            skipped_no_score += 1
            continue

        # sheet row number (1-based): data_start_row_index + 1 (convert to 1-based)
        # + header_row_index accounts for any metadata rows above header
        sheet_row = data_start_row_index + offset + 1  # +1 for 1-based indexing
        result_cell = gspread.utils.rowcol_to_a1(sheet_row, result_col + 1)

        if dry_run:
            print(f"  [{date}] {pick} {line} | score: {score_str} → {result}")
        else:
            batch_updates.append({"range": result_cell, "values": [[result]]})

        resolved += 1

    if not dry_run and batch_updates:
        # Write in chunks to avoid API limits
        chunk_size = 500
        for i in range(0, len(batch_updates), chunk_size):
            ws.batch_update(batch_updates[i:i + chunk_size])
            if i + chunk_size < len(batch_updates):
                time.sleep(1)
        print(f"  Wrote {len(batch_updates)} result updates to {sheet_name}")

    return resolved, skipped_no_score, skipped_already


# ── Main ─────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true", help="Preview without writing")
    parser.add_argument(
        "--sheet",
        choices=["master_sheet", "master_sheet_new", "parsed_picks_new", "both"],
        default="both",
        help="Which sheet to update (default: both)",
    )
    args = parser.parse_args()

    print("Connecting to Google Sheets...")
    gc = get_gspread_client()
    ss = gc.open_by_key(GOOGLE_SHEET_ID)

    print("\nLoading scores from schedule sheets...")
    scores = load_scores(ss)

    sheets_to_run = []
    if args.sheet in ("master_sheet", "both"):
        # master_sheet: header on row 1 (index 0), data from row 2
        sheets_to_run.append((MASTER_SHEET, 0))
    if args.sheet in ("master_sheet_new", "both"):
        # master_sheet_new: header on row 1 (index 0), data from row 2
        sheets_to_run.append((MASTER_SHEET_NEW, 0))
    if args.sheet in ("parsed_picks_new", "both"):
        # parsed_picks_new: metadata rows 1-2, header on row 3 (index 2), data from row 4
        sheets_to_run.append((PICKS_NEW_SHEET, 2))

    for sheet_name, header_row_index in sheets_to_run:
        print(f"\nProcessing {sheet_name}...")
        resolved, skipped_no_score, skipped_already = process_sheet(
            ss, sheet_name, header_row_index, scores, args.dry_run
        )
        print(f"  Results filled:  {resolved}")
        print(f"  Already set:     {skipped_already}")
        print(f"  No score yet:    {skipped_no_score}")

    if args.dry_run:
        print("\n[dry-run] No changes written.")
    else:
        print("\nDone.")


if __name__ == "__main__":
    main()
