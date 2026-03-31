#!/usr/bin/env python3
"""
daily_audit.py — Nightly audit of yesterday's picks in master_sheet.

Runs a series of programmatic checks on every pick from the previous day,
auto-fixes what it can, flags ambiguous cases for Opus review, and writes
everything to the `audit_results` sheet with a status column that progresses:

    programmatic checks
        |-- all pass            --> (no audit_results row)
        |-- auto-fixed          --> status = "auto_fixed"
        |-- flagged (ambiguous) --> status = "needs_review"
                                        |
                                   Opus reviews (time-gated)
                                        |-- confident fix --> status = "opus_approved"
                                        |-- unsure        --> status = "needs_human"
                                                                |
                                                           Human edits status directly
                                                                |--> "human_approved"
                                                                |--> "human_rejected"

Checks implemented:
  1. missing_columns — every required column must have a value; result is
     auto-filled from scores when possible.
  2. next_day_game   — if game is empty, try matching pick against D+1 schedule.
  3. unresolved_team — game column is "team_not_found" sentinel from Python resolver.
  4. unresolved_capper — capper column is "capper_not_found" sentinel from Python resolver.

Checks documented but not yet implemented:
  5. result_correctness    (recompute result from score + line, compare to stored)
  6. game_match            (pick team must appear in schedule for that sport/date)
  7. ambiguous_team        (pick team substring-matches multiple schedule games)
  8. wrong_game            (pick team in schedule but game column points elsewhere)
  9. spread_consistency    (spread must match ESPN schedule spread for the game)
  10. ocr_grounding        (pick team must appear in raw ocr_text)
  11. duplicate_detection  (same date+capper+pick+line appears more than once)

Usage:
  .venv/bin/python3 daily_audit.py              # normal run
  .venv/bin/python3 daily_audit.py --dry-run    # preview without writing
  .venv/bin/python3 daily_audit.py --force-opus # run Opus regardless of time gate
  .venv/bin/python3 daily_audit.py --date 2026-03-17
"""

import csv
import os
import re
import json
import argparse
import time
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Tuple, Optional
from collections import defaultdict

import anthropic
import gspread
from dotenv import load_dotenv

from audit_hallucinations import pick_in_ocr, ABBREV_MAP
from activity_logger import log_activity
from git_utils import git_push_csv
from populate_results import determine_result, find_score, load_scores, team_matches
from sheets_utils import GOOGLE_SHEET_ID, get_gspread_client, sheets_call, SPORT_TO_SCHED
from stage2_python import TEAM_NOT_FOUND, CAPPER_NOT_FOUND

load_dotenv()


# ── Constants ─────────────────────────────────────────────────────────────────
MASTER_SHEET    = "master_sheet"
AUDIT_SHEET     = "audit_results"
LOCAL_CSV_PATH  = "gh-pages/data/master_sheet.csv"

# Audit statuses that a new auto-fix is allowed to overwrite.
# Human-reviewed and Opus-reviewed statuses are never touched.
AUTO_FIX_ELIGIBLE = {"needs_review"}
PICKS_NEW_SHEET = "parsed_picks_new"   # read-only; used to look up ocr_text

MASTER_HEADERS = ["date", "capper", "sport", "pick", "line", "game", "spread", "result"]

# audit_results schema: status is col B for easy scanning; ms_row links back to master_sheet
AUDIT_HEADERS = [
    "date", "status", "ms_row",
    "capper", "sport", "pick", "line",
    "game", "spread", "result",
    "check_failed", "details", "suggested_fix",
    "ocr_text",
]

# Valid status values (applied as data-validation dropdown in the sheet)
VALID_STATUSES = [
    "auto_fixed",
    "needs_review",
    "opus_approved",
    "needs_human",
    "human_approved",
    "human_rejected",
    "remediated",
]

# Required columns that must be non-empty for a pick to be considered complete.
# "spread" is excluded — backfill script handles spread separately via schedule lookup.
REQUIRED_COLUMNS = ["date", "capper", "sport", "pick", "line", "game", "result"]

# PST = UTC-8 (standard time); PDT = UTC-7; we use UTC-8 conservatively.
PST_OFFSET = timezone(timedelta(hours=-8))

# Opus time gate: only run within first 15 minutes after midnight PST.
OPUS_GATE_MINUTES = 15
OPUS_BATCH_SIZE   = 10


# ── Time helpers ──────────────────────────────────────────────────────────────
def within_midnight_window() -> bool:
    """Return True if current PST time is in the first OPUS_GATE_MINUTES of the day."""
    now_pst = datetime.now(PST_OFFSET)
    minutes_past_midnight = now_pst.hour * 60 + now_pst.minute
    return minutes_past_midnight < OPUS_GATE_MINUTES


def yesterday_str() -> str:
    """Return yesterday's date as YYYY-MM-DD in PST."""
    yesterday = datetime.now(PST_OFFSET) - timedelta(days=1)
    return yesterday.strftime("%Y-%m-%d")


# ── Google Sheets helpers ─────────────────────────────────────────────────────
def get_or_create_audit_sheet(ss) -> gspread.Worksheet:
    """Get or create the audit_results worksheet with the correct headers and status dropdown."""
    try:
        ws = ss.worksheet(AUDIT_SHEET)
        existing = ws.row_values(5)
        if existing != AUDIT_HEADERS:
            # Schema changed — overwrite header row
            sheets_call(ws.update, "A5", [AUDIT_HEADERS])
            _apply_status_validation(ws)
        return ws
    except gspread.exceptions.WorksheetNotFound:
        ws = ss.add_worksheet(title=AUDIT_SHEET, rows=1000, cols=len(AUDIT_HEADERS))
        ws.append_row(AUDIT_HEADERS, value_input_option="USER_ENTERED")
        _apply_status_validation(ws)
        return ws


def _apply_status_validation(ws: gspread.Worksheet):
    """Apply data-validation dropdown for the status column (B6:B1000)."""
    from gspread.utils import ValidationConditionType

    status_col_idx = AUDIT_HEADERS.index("status")  # 0-based
    col_letter = chr(ord("A") + status_col_idx)      # "B"
    validation_range = f"{col_letter}6:{col_letter}1000"

    sheets_call(
        ws.add_validation,
        validation_range,
        ValidationConditionType.one_of_list,
        VALID_STATUSES,
        strict=False,       # allow blanks
        showCustomUi=True,  # show dropdown arrow
    )
    print(f"  Applied status dropdown validation to {validation_range}")


# ── Schedule loader ───────────────────────────────────────────────────────────
def load_schedule_for_date(ss, target_date: str) -> Dict[str, List[Tuple[str, str, str]]]:
    """
    Returns {sport: [(away_team, home_team, spread), ...]} for games on target_date.
    """
    schedule = {}
    for sport, sheet_name in SPORT_TO_SCHED.items():
        try:
            ws = ss.worksheet(sheet_name)
            rows = ws.get_all_values()
            if not rows:
                schedule[sport] = []
                continue
            headers = rows[0]
            hcol = {h: i for i, h in enumerate(headers)}
            date_idx = hcol.get("game_date", 1)
            away_idx = hcol.get("away_team", 2)
            home_idx = hcol.get("home_team", 3)
            spread_idx = hcol.get("spread", 5)
            games = []
            for row in rows[1:]:
                while len(row) <= max(date_idx, away_idx, home_idx, spread_idx):
                    row.append("")
                if row[date_idx].strip() == target_date:
                    away = row[away_idx].strip()
                    home = row[home_idx].strip()
                    spread = row[spread_idx].strip()
                    if away and home:
                        games.append((away, home, spread))
            schedule[sport] = games
            print(f"  {sheet_name}: {len(games)} games on {target_date}")
        except gspread.exceptions.WorksheetNotFound:
            schedule[sport] = []
    return schedule


def format_schedule_context(schedule: Dict[str, List[Tuple[str, str, str]]]) -> str:
    """Format a schedule dict as a compact string for Opus prompts."""
    lines = []
    for sport, games in schedule.items():
        if games:
            game_strs = [f"{a} @ {h}" for a, h, _s in games]
            lines.append(f"{sport.upper()}: {', '.join(game_strs)}")
    return "\n".join(lines) if lines else "(no games found)"


# ── OCR text lookup ───────────────────────────────────────────────────────────
def load_ocr_index(ss, target_date: str) -> Dict[tuple, str]:
    """
    Load OCR text from parsed_picks_new for target_date.
    Returns {(date, capper, sport, pick, line): ocr_text}.
    """
    try:
        ws = ss.worksheet(PICKS_NEW_SHEET)
        all_values = sheets_call(ws.get_all_values)
    except gspread.exceptions.WorksheetNotFound:
        return {}

    if len(all_values) < 4:
        return {}

    header = all_values[2]  # header is row 3
    col = {h: i for i, h in enumerate(header)}
    date_col   = col.get("date", 0)
    capper_col = col.get("capper", 1)
    sport_col  = col.get("sport", 2)
    pick_col   = col.get("pick", 3)
    line_col   = col.get("line", 4)
    ocr_col    = col.get("ocr_text", 9)

    index = {}
    for row in all_values[3:]:
        if not any(cell.strip() for cell in row):
            continue
        while len(row) <= max(date_col, ocr_col):
            row.append("")
        if row[date_col].strip() != target_date:
            continue
        key = (
            row[date_col].strip(),
            row[capper_col].strip(),
            row[sport_col].strip(),
            row[pick_col].strip(),
            row[line_col].strip(),
        )
        index[key] = row[ocr_col].strip()
    return index


# ── Audit row builder ─────────────────────────────────────────────────────────
def make_audit_row(
    pick_row: dict,
    check_failed: str,
    details: str,
    suggested_fix: str,
    status: str,
    ms_row: int = 0,
    ocr_text: str = "",
) -> list:
    """Build a list suitable for appending to audit_results (matches AUDIT_HEADERS order)."""
    return [
        pick_row.get("date", ""),
        status,
        ms_row,
        pick_row.get("capper", ""),
        pick_row.get("sport", ""),
        pick_row.get("pick", ""),
        pick_row.get("line", ""),
        pick_row.get("game", ""),
        pick_row.get("spread", ""),
        pick_row.get("result", ""),
        check_failed,
        details,
        suggested_fix,
        ocr_text,
    ]


# ── Check 1: Missing columns ─────────────────────────────────────────────────
def check_missing_columns(
    pick: dict,
    scores: dict,
    ms_ws: gspread.Worksheet,
    ms_row_num: int,
    dry_run: bool,
) -> Optional[dict]:
    """
    Check 1: Every required column must have a value.

    Required columns: date, capper, sport, pick, line, game, result.
    (spread is excluded — handled separately by spread_consistency check.)

    If only `result` is missing and a score exists, auto-fill it and return
    an audit row with status="auto_fixed".

    If other columns are missing, return an audit row with status="needs_review".

    Returns None if the row passes (all required columns present).
    """
    missing = [col for col in REQUIRED_COLUMNS if not pick.get(col, "").strip()]

    if not missing:
        return None  # row is complete

    # Special case: only result is missing — try to auto-fill
    if missing == ["result"]:
        score_str = find_score(
            pick["pick"], pick["date"], pick["sport"], pick["game"], scores
        )
        new_result = None
        if score_str:
            new_result = determine_result(
                pick["pick"], pick["line"], pick["game"], score_str
            )

        if new_result:
            # Auto-fix: write result to master_sheet
            if not dry_run:
                result_cell = gspread.utils.rowcol_to_a1(
                    ms_row_num, MASTER_HEADERS.index("result") + 1
                )
                sheets_call(ms_ws.update, result_cell, [[new_result]])

            return {
                "pick_row": {**pick, "result": new_result},
                "check_failed": "missing_columns",
                "details": "result was blank; auto-filled from score",
                "suggested_fix": f"result={new_result}",
                "status": "auto_fixed",
            }
        else:
            # Result missing and no score available — flag for review
            detail = "result is blank"
            if not score_str:
                detail += "; no score found in schedule"
            else:
                detail += f"; score found ({score_str}) but could not compute result"
            return {
                "pick_row": pick,
                "check_failed": "missing_columns",
                "details": detail,
                "suggested_fix": "",
                "status": "needs_review",
            }

    # Other columns missing — always flag for review
    return {
        "pick_row": pick,
        "check_failed": "missing_columns",
        "details": f"missing: {', '.join(missing)}",
        "suggested_fix": "",
        "status": "needs_review",
    }


# ── Check 2: Next day game ────────────────────────────────────────────────────

# Cache so we only fetch the D+1 schedule once per audit run, regardless of
# how many picks have an empty game column.
_schedule_cache: Dict[str, Tuple[str, Dict]] = {}


def check_next_day_game(
    pick: dict,
    scores: dict,
    ms_ws: gspread.Worksheet,
    ms_row_num: int,
    dry_run: bool,
    ss,
    target_date: str,
) -> Optional[dict]:
    """
    If game is empty or team_not_found, try to match the pick against the D+1 schedule.

    Handles cappers who post picks the night before a game (advance picks) or
    UTC boundary cases where the pick's stored date is one day before the game.
    team_not_found sentinels are treated as "no game" because the resolver only
    checks the pick's stored date; advance picks land on D+1, not D+0.

    - 0 matches on D+1 → return None
    - 1 match on D+1  → auto-fix: patch date/game/spread/result in master_sheet
    - 2+ matches      → needs_review with match details
    """
    # Fast-path: game already matched, nothing to do.
    game_val = pick.get("game", "").strip()
    if game_val and game_val != TEAM_NOT_FOUND:
        return None

    # Lazy-load D+1 schedule (cached across picks in the same audit run)
    if target_date not in _schedule_cache:
        from datetime import date as _date
        d1_date = str(_date.fromisoformat(target_date) + timedelta(days=1))
        print(f"\nLoading D+1 schedule for {d1_date}...")
        schedule_d1 = sheets_call(load_schedule_for_date, ss, d1_date)
        _schedule_cache[target_date] = (d1_date, schedule_d1)
    d1_date, schedule_d1 = _schedule_cache[target_date]

    sport = pick.get("sport", "").strip().lower()
    pick_team = pick.get("pick", "").strip()
    line = pick.get("line", "").strip()

    # Normalize: strip parentheticals like "(OH)" before fuzzy-matching so that
    # "Miami RedHawks" matches "Miami (OH) RedHawks".
    def _norm(name: str) -> str:
        return re.sub(r'\s*\([^)]*\)', '', name).strip()

    pick_norm = _norm(pick_team)
    d1_games = schedule_d1.get(sport, [])
    matches = []
    for away, home, sched_spread in d1_games:
        if team_matches(pick_norm, _norm(away)) or team_matches(pick_norm, _norm(home)):
            matches.append((away, home, sched_spread))

    if not matches:
        return None  # no D+1 game found

    if len(matches) > 1:
        match_strs = [f"{a} @ {h}" for a, h, _ in matches]
        return {
            "pick_row": pick,
            "check_failed": "next_day_game",
            "details": f"pick matches {len(matches)} games on D+1 ({d1_date}): {'; '.join(match_strs)}",
            "suggested_fix": "",
            "status": "needs_review",
        }

    # Exactly one match — compute corrected values
    away_team, home_team, sched_spread = matches[0]
    matched_game = f"{away_team} @ {home_team}"

    if team_matches(pick_norm, _norm(away_team)):
        matched_team = away_team
    else:
        matched_team = home_team

    # Use schedule spread for all rows (spread is a game property, not bet-type)
    new_spread = sched_spread

    # Attempt result from D+1 scores (game may already be played).
    # Use matched_team (the schedule name) for determine_result so that the
    # team-name lookup inside it matches the game string exactly — the raw
    # pick_team may differ (e.g. "Miami RedHawks" vs "Miami (OH) RedHawks").
    new_result = None
    score_str = find_score(matched_team, d1_date, sport, matched_game, scores)
    if score_str:
        new_result = determine_result(matched_team, line, matched_game, score_str)

    original_date = pick.get("date", "")

    updates = {
        "date":   d1_date,
        "game":   matched_game,
        "spread": new_spread,
    }
    if new_result:
        updates["result"] = new_result

    if not dry_run:
        batch = [
            {
                "range":  gspread.utils.rowcol_to_a1(ms_row_num, MASTER_HEADERS.index(field) + 1),
                "values": [[value]],
            }
            for field, value in updates.items()
        ]
        sheets_call(ms_ws.batch_update, batch)

    corrected_pick = {**pick, **updates}

    fix_parts = [f"date={d1_date}", f"game={matched_game}"]
    if new_spread:
        fix_parts.append(f"spread={new_spread}")
    if new_result:
        fix_parts.append(f"result={new_result}")

    # The date/game/spread fix is complete — always auto_fixed.
    # If result couldn't be computed (score not yet available), check_missing_columns
    # will catch and fill it on the next nightly run once the score is available.
    status = "auto_fixed"
    details = f"game unresolved; matched '{pick_team}' to D+1 ({d1_date}): {matched_game}"
    if not new_result:
        details += "; result pending — check_missing_columns will fill on next run"

    return {
        "pick_row": corrected_pick,
        "check_failed": "next_day_game",
        "details": details,
        "suggested_fix": "; ".join(fix_parts),
        "status": status,
    }


# ── Check 3: Unresolved team ──────────────────────────────────────────────────
def check_unresolved_team(
    pick: dict,
    scores: dict,
    ms_ws: gspread.Worksheet,
    ms_row_num: int,
    dry_run: bool,
    **ctx,
) -> Optional[dict]:
    """
    Check 3: Flag rows where the game column is the sentinel "team_not_found".

    This means the Python TeamResolver could not resolve the raw pick name
    to a canonical ESPN team name. The row needs a human to:
      1. Identify the correct ESPN team name and game.
      2. Fix the master_sheet row (game, spread, possibly pick).
      3. Add the raw name as an alias to team_name_resolution so the
         resolver handles it automatically next time.

    Returns None if game is NOT the sentinel (row is fine).
    """
    game = pick.get("game", "").strip()
    if game != TEAM_NOT_FOUND:
        return None

    raw_pick = pick.get("pick", "").strip()
    sport = pick.get("sport", "").strip()
    date = pick.get("date", "").strip()

    return {
        "pick_row": pick,
        "check_failed": "unresolved_team",
        "details": (
            f"team resolver could not match pick '{raw_pick}' "
            f"(sport={sport}, date={date}) to any ESPN team; "
            f"game/spread are blank"
        ),
        "suggested_fix": (
            f"set game and spread to correct values; "
            f"add '{raw_pick}' as alias to team_name_resolution"
        ),
        "status": "needs_review",
    }


# ── Check 4: Unresolved capper ───────────────────────────────────────────────
def check_unresolved_capper(
    pick: dict,
    scores: dict,
    ms_ws: gspread.Worksheet,
    ms_row_num: int,
    dry_run: bool,
    **ctx,
) -> Optional[dict]:
    """
    Check 4: Flag rows where the capper column is the sentinel "capper_not_found".

    This means the CapperResolver could not match the raw capper name to any
    canonical unique_capper_name. The row needs a human to:
      1. Identify the correct capper.
      2. Fix the master_sheet row (capper column).
      3. Add the raw name as an alias to capper_name_resolution so the
         resolver handles it automatically next time.

    Returns None if capper is NOT the sentinel (row is fine).
    """
    capper = pick.get("capper", "").strip()
    if capper != CAPPER_NOT_FOUND:
        return None

    raw_pick = pick.get("pick", "").strip()
    date = pick.get("date", "").strip()

    return {
        "pick_row": pick,
        "check_failed": "unresolved_capper",
        "details": (
            f"capper resolver could not match raw capper name "
            f"(date={date}, pick={raw_pick}); "
            f"capper is set to sentinel '{CAPPER_NOT_FOUND}'"
        ),
        "suggested_fix": (
            f"set capper to correct unique_capper_name; "
            f"add raw name as alias to capper_name_resolution"
        ),
        "status": "needs_review",
    }


# ══════════════════════════════════════════════════════════════════════════════
# FUTURE CHECKS — detailed specifications
#
# Each check function should follow the same signature:
#   def check_XXX(pick, scores, ms_ws, ms_row_num, dry_run, **ctx) -> Optional[dict]
# Return None if the pick passes, or a dict with keys:
#   pick_row, check_failed, details, suggested_fix, status
#
# ── Check 2: result_correctness ──────────────────────────────────────────────
# Recompute win/lose/push from the score string + the pick's line, then compare
# to the stored result value.  If they disagree, auto-fix (overwrite the stored
# result with the recomputed one).
#
# Why it matters:
#   populate_results.py fills results, but if the game/spread were wrong at
#   the time it ran, the result could be wrong too.  This check catches stale or
#   incorrect results after game-column corrections.
#
# Logic:
#   1. Look up score via find_score(pick, date, sport, game, scores).
#   2. Recompute result via determine_result(pick, line, game, score_str).
#   3. Compare recomputed vs stored result (case-insensitive).
#   4. If they match → pass (return None).
#   5. If they differ → auto-fix: overwrite result in master_sheet.
#
# Examples:
#   Stored result="win", recomputed="lose"  →  auto_fixed, suggested_fix="result=lose"
#   Stored result="win", recomputed="win"   →  pass
#   No score available                      →  pass (can't verify, skip)
#
# Edge cases:
#   - If recomputed is None (score format unrecognized), skip — don't flag.
#   - If stored result is blank, Check 1 already handles it.
#
# ── Check 3: game_match ──────────────────────────────────────────────────────
# The pick team must appear in at least one game in the schedule for that
# sport on that date.  If the team isn't scheduled at all, the pick is likely
# a hallucination or wrong-date assignment.
#
# Logic:
#   1. Load schedule for (sport, target_date).
#   2. Fuzzy-match pick team name against every (away, home) pair.
#   3. If no match found in the tagged sport, search other sports (catches
#      wrong-sport tags, e.g. NBA team tagged CBB).
#   4. If found in another sport → needs_review with suggested sport fix.
#   5. If found nowhere → needs_review ("team not scheduled on this date").
#
# Examples:
#   pick="Michigan Wolverines", sport=CBB, date=2026-03-17
#     schedule has Michigan @ Ohio State on 2026-03-17 in cbb_schedule → pass
#
#   pick="Orlando Magic", sport=NBA, date=2026-03-17
#     no Orlando game on schedule that day → needs_review
#     details="Orlando Magic not found in any sport schedule for 2026-03-17"
#
#   pick="Duke Blue Devils", sport=NBA, date=2026-03-17
#     not in nba_schedule, but found in cbb_schedule → needs_review
#     details="tagged NBA but found in CBB schedule"
#     suggested_fix="sport=cbb"
#
# Edge cases:
#   - O/U (totals) lines: skip this check — two-team picks don't map to one game.
#   - If no schedule sheet exists for the sport, skip (benefit of the doubt).
#
# ── Check 4: ambiguous_team ──────────────────────────────────────────────────
# The pick team substring-matches MORE THAN ONE game on the schedule.  This
# happens with teams like Michigan/Michigan State, New York (Knicks/Rangers),
# or similar partial-name collisions.
#
# Logic:
#   1. For the pick team, count how many schedule games (away, home) it matches
#      via substring (same fuzzy logic as check 3).
#   2. If exactly 1 match → pass (unambiguous).
#   3. If 2+ matches → needs_review.
#
# Examples:
#   pick="Michigan", schedule has both "Michigan Wolverines @ Ohio State"
#     AND "Indiana @ Michigan State Spartans" on same date
#     → needs_review
#     details="pick matches 2 games: Michigan Wolverines @ Ohio State Buckeyes, Indiana Hoosiers @ Michigan State Spartans"
#     suggested_fix=""  (Opus or human must decide)
#
#   pick="New York Knicks", schedule has Knicks game AND Rangers game
#     "New York" substring matches both → needs_review
#     (though "Knicks" should disambiguate — check should use full pick string)
#
# Edge cases:
#   - If the full pick name (e.g. "Michigan Wolverines") only matches one game
#     but a partial ("Michigan") matches two, the full-name match wins → pass.
#   - This check runs AFTER check 3.  If check 3 already flagged "not found",
#     skip this check for that row.
#
# ── Check 5: wrong_game ──────────────────────────────────────────────────────
# The pick team IS in the schedule, but the stored `game` column points to a
# DIFFERENT game than the one the team is actually playing in.
#
# Why it matters:
#   Stage 2 (finalization) uses Claude to match picks to games.  If the schedule
#   has similar team names or Claude makes a mistake, the game column can be wrong.
#   A wrong game means the spread and result are all derived from the wrong
#   matchup.
#
# Logic:
#   1. Find the correct game from the schedule (same as check 3 matching).
#   2. Compare stored game column to "Away @ Home" from the schedule.
#   3. If they match → pass.
#   4. If they differ and the correct game is unambiguous (check 4 passed) →
#      auto_fixed: overwrite game, recompute spread/result.
#   5. If they differ and ambiguous (check 4 flagged) → needs_review.
#
# Examples:
#   pick="Michigan Wolverines", game="Indiana Hoosiers @ Michigan State Spartans"
#     Schedule shows Michigan Wolverines @ Ohio State Buckeyes
#     → auto_fixed (if unambiguous)
#     suggested_fix="game=Michigan Wolverines @ Ohio State Buckeyes"
#
#   pick="Michigan", game="Indiana @ Michigan State"
#     Schedule has both Michigan and Michigan State games
#     → needs_review (ambiguous — can't auto-fix)
#
# What gets corrected on auto-fix:
#   - game = "Away @ Home" from schedule
#   - spread = schedule spread (for all rows including ML)
#   - result = recomputed from correct game's score (if available)
#
# ── Check 6: spread_consistency ──────────────────────────────────────────────
# The `spread` column should match the consensus spread from the ESPN schedule
# sheet for the game on this date. Spread is a property of the game, not the
# bet type, so it applies to ALL rows including ML bets.
#
# Logic:
#   1. Look up the schedule spread for this game (by date + sport + team).
#   2. If spread ≠ schedule spread → auto_fixed with the schedule value.
#   3. (Soft check) If line sign seems wrong for the side, flag needs_review
#      rather than auto-fix (line sign is set by the capper, not derivable).
#
# Examples:
#   pick="Duke", line="-3.5", schedule_spread="Duke Blue Devils -3.5", spread="Duke Blue Devils -3.5" → pass
#   pick="Duke", line="-3.5", schedule_spread="Duke Blue Devils -3.5", spread="UNC +3.5"  → auto_fixed
#   pick="Duke", line="ML",   schedule_spread="Duke Blue Devils -3.5", spread=""          → auto_fixed
#   pick="Duke", line="ML",   schedule_spread="Duke Blue Devils -3.5", spread="Duke Blue Devils -3.5" → pass
#
# ── Check 7: ocr_grounding ──────────────────────────────────────────────────
# The pick team name (or abbreviation/nickname) must appear somewhere in the
# raw OCR text from the original image.  If it doesn't, the pick may have been
# hallucinated by Claude during the OCR/parsing step.
#
# Logic:
#   1. Look up ocr_text from parsed_picks_new for this pick (via ocr_index).
#   2. If no OCR text available → skip (can't verify).
#   3. Call pick_in_ocr(pick, ocr_text) — uses substring matching + ABBREV_MAP.
#   4. If True → pass.
#   5. If False → needs_review (escalate to Opus if time-gated window is open).
#
# Examples:
#   pick="Cleveland Cavaliers", ocr="Cavs ML (-103) / Bucks -4"
#     pick_in_ocr returns True (Cavs is in ABBREV_MAP for Cavaliers) → pass
#
#   pick="Orlando Magic", ocr="Detroit Pistons +4 / Utah Jazz ML"
#     pick_in_ocr returns False → needs_review
#     details="pick 'Orlando Magic' not found in OCR text"
#
# Opus escalation:
#   When the Opus time gate is open, all needs_review rows from this check
#   are batched and sent to Opus with the OCR text + schedule context.
#   Opus returns VALID or HALLUCINATION for each.
#   - VALID → status updated to "opus_approved" (false positive cleared)
#   - HALLUCINATION → status stays "needs_human" (confirmed by AI, needs human)
#
# ── Check 8: duplicate_detection ─────────────────────────────────────────────
# The same (date, capper, pick, line) appears more than once in master_sheet
# for the target date.  Exact duplicates are likely double-inserts from retries
# or overlapping batches.
#
# Logic:
#   1. Group yesterday's picks by (date, capper, pick, line).
#   2. Any group with count > 1 → flag ALL rows in that group as needs_review.
#   3. Don't auto-delete — human decides which to keep.
#
# Examples:
#   Two rows: date=2026-03-17, capper=BEEZO, pick="Duke", line="-3.5"
#     → both rows get needs_review
#     details="duplicate: 2 rows with same date+capper+pick+line"
#     suggested_fix="delete duplicate row"
#
# Edge cases:
#   - Same team, different lines (e.g. -3.5 and -4): NOT duplicates.
#   - Same team, same line, different cappers: NOT duplicates.
#   - A capper might legitimately pick the same team at different times
#     if the line moved — but same line = almost certainly a dupe.
#
# ══════════════════════════════════════════════════════════════════════════════


# ── Main audit logic ─────────────────────────────────────────────────────────
def run_audit(
    ss,
    target_date: str = None,
    dry_run: bool = False,
    force_opus: bool = False,
):
    """
    Run the nightly audit on yesterday's picks in master_sheet.

    Runs all programmatic checks, writes findings to audit_results, and
    optionally escalates ambiguous cases to Opus (time-gated).

    Args:
        ss:          Open gspread Spreadsheet object.
        target_date: YYYY-MM-DD to audit; defaults to yesterday PST.
        dry_run:     Preview without writing.
        force_opus:  Skip midnight time gate for Opus.
    """
    target_date = target_date or yesterday_str()
    run_opus = force_opus or within_midnight_window()

    print(f"Daily audit -- target date: {target_date}")
    now_pst = datetime.now(PST_OFFSET)
    print(f"Current PST time: {now_pst.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Opus escalation: {'YES' if run_opus else 'NO (outside 15-min window)'}")

    # ── Load data ────────────────────────────────────────────────────────────
    print(f"\nLoading master_sheet...")
    ms_ws = sheets_call(ss.worksheet, MASTER_SHEET)
    ms_vals = sheets_call(ms_ws.get_all_values)
    if len(ms_vals) < 2:
        print("master_sheet has no data rows — nothing to audit.")
        return

    ms_header = ms_vals[0]
    ms_col = {h: i for i, h in enumerate(ms_header)}

    # Filter to yesterday's rows, keeping track of 1-based sheet row numbers
    yesterday_picks = []  # list of (dict, 1-based-row-number)
    for idx, row in enumerate(ms_vals[1:], start=2):
        while len(row) < len(ms_header):
            row.append("")
        if row[ms_col.get("date", 0)].strip() != target_date:
            continue
        pick_dict = {h: row[ms_col[h]].strip() for h in ms_header if h in ms_col}
        yesterday_picks.append((pick_dict, idx))

    print(f"  {len(yesterday_picks)} picks for {target_date}")
    if not yesterday_picks:
        print("No picks found — nothing to audit.")
        return

    # Load scores for result auto-fill
    print(f"\nLoading scores from schedule sheets...")
    scores = sheets_call(load_scores, ss)

    # Load schedule for context (used by future checks and Opus)
    print(f"\nLoading schedule for {target_date}...")
    schedule = sheets_call(load_schedule_for_date, ss, target_date)
    schedule_context = format_schedule_context(schedule)
    print(f"Schedule context:\n{schedule_context}")

    # Load OCR index for ocr_text column in audit rows
    print(f"\nLoading OCR text from {PICKS_NEW_SHEET}...")
    ocr_index = load_ocr_index(ss, target_date)
    print(f"  {len(ocr_index)} OCR entries loaded")

    # ── Run checks ───────────────────────────────────────────────────────────
    audit_results = []   # list of dicts with keys: pick_row, check_failed, details, suggested_fix, status, ms_row
    clean_count = 0

    print(f"\nRunning checks...")

    for pick_dict, row_num in yesterday_picks:
        findings = []

        # Check 1: missing columns
        result = check_missing_columns(pick_dict, scores, ms_ws, row_num, dry_run)
        if result:
            findings.append(result)

        # Check 2: advance pick date (only relevant when game is empty)
        result = check_next_day_game(
            pick_dict, scores, ms_ws, row_num, dry_run,
            ss=ss, target_date=target_date,
        )
        if result:
            findings.append(result)

        # Check 3: unresolved team (game = "team_not_found" sentinel)
        result = check_unresolved_team(pick_dict, scores, ms_ws, row_num, dry_run)
        if result:
            findings.append(result)

        # Check 4: unresolved capper (capper = "capper_not_found" sentinel)
        result = check_unresolved_capper(pick_dict, scores, ms_ws, row_num, dry_run)
        if result:
            findings.append(result)

        # Future checks would be added here:
        # result = check_result_correctness(pick_dict, scores, ms_ws, row_num, dry_run)
        # if result: findings.append(result)
        #
        # result = check_game_match(pick_dict, schedule, ...)
        # if result: findings.append(result)
        # ... etc.

        # Per-pick deduplication:
        # - auto_fixed supersedes needs_review (fully fixed row)
        # - multiple needs_review: keep only the last one (later checks are
        #   more specific and have more context than earlier ones)
        has_auto_fix = any(f["status"] == "auto_fixed" for f in findings)
        if has_auto_fix:
            findings = [f for f in findings if f["status"] != "needs_review"]
        else:
            review = [f for f in findings if f["status"] == "needs_review"]
            if len(review) > 1:
                findings = [f for f in findings if f["status"] != "needs_review"] + [review[-1]]

        if not findings:
            clean_count += 1
        else:
            for f in findings:
                f["ms_row"] = row_num  # tag each finding with master_sheet row
            audit_results.extend(findings)

    # Tally by status
    auto_fixed = [r for r in audit_results if r["status"] == "auto_fixed"]
    needs_review = [r for r in audit_results if r["status"] == "needs_review"]

    print(f"\nCheck results:")
    print(f"  Clean (no issues):     {clean_count}")
    print(f"  Auto-fixed:            {len(auto_fixed)}")
    print(f"  Needs review:          {len(needs_review)}")

    if auto_fixed:
        print(f"\n  Auto-fixed details:")
        for r in auto_fixed:
            p = r["pick_row"]
            print(f"    [{p['date']}] {p['capper']} | {p['sport']} | {p['pick']} {p['line']} -- {r['details']}")

    if needs_review:
        print(f"\n  Needs review:")
        for r in needs_review:
            p = r["pick_row"]
            print(f"    [{p['date']}] {p['capper']} | {p['sport']} | {p['pick']} {p['line']} -- {r['details']}")

    if not audit_results:
        print("\nAll picks clean — nothing to write to audit_results.")
        return

    if dry_run:
        print(f"\n[dry-run] Would write/upgrade {len(audit_results)} row(s) to {AUDIT_SHEET}.")
        if auto_fixed:
            print(f"[dry-run] Would sync CSV ({len(auto_fixed)} auto-fix(es)).")
        return

    # ── Write to audit_results ──────────────────────────────────────────────────
    # Order: needs_review first, then auto_fixed at the bottom
    ordered = needs_review + auto_fixed

    ws_audit = get_or_create_audit_sheet(ss)
    time.sleep(1)

    # Build index of existing audit rows: ms_row_str → [(audit_row_1based, status)]
    existing_audit_index: Dict[str, List[Tuple[int, str]]] = {}
    existing_audit = sheets_call(ws_audit.get_all_values)
    if len(existing_audit) > 5:
        audit_hdr  = existing_audit[4]  # header is row 5 (0-indexed: 4)
        ms_row_idx = audit_hdr.index("ms_row") if "ms_row" in audit_hdr else None
        status_idx = audit_hdr.index("status") if "status" in audit_hdr else 1
        if ms_row_idx is not None:
            for i, row in enumerate(existing_audit[5:], start=6):
                while len(row) <= max(ms_row_idx, status_idx):
                    row.append("")
                ms_row_val = row[ms_row_idx].strip()
                if ms_row_val:
                    existing_audit_index.setdefault(ms_row_val, []).append(
                        (i, row[status_idx].strip())
                    )

    def _audit_row(r: dict) -> list:
        p = r["pick_row"]
        return make_audit_row(
            pick_row=p,
            check_failed=r["check_failed"],
            details=r["details"],
            suggested_fix=r["suggested_fix"],
            status=r["status"],
            ms_row=r["ms_row"],
            ocr_text=ocr_index.get((p["date"], p["capper"], p["sport"], p["pick"], p["line"]), ""),
        )

    # ── Pass 1: upgrade existing needs_review rows that were auto-fixed ───────
    rows_to_update = []  # [(audit_row_1based, new_row_values)]
    upgraded_ms_rows: set = set()
    for r in auto_fixed:
        ms_row_str = str(r["ms_row"])
        if ms_row_str not in existing_audit_index:
            continue
        new_row = _audit_row(r)
        for audit_row_num, current_status in existing_audit_index[ms_row_str]:
            if current_status in AUTO_FIX_ELIGIBLE:
                rows_to_update.append((audit_row_num, new_row))
        upgraded_ms_rows.add(ms_row_str)

    if rows_to_update:
        print(f"\nUpgrading {len(rows_to_update)} existing audit rows (needs_review -> auto_fixed)...")
        for audit_row_num, new_row_vals in rows_to_update:
            sheets_call(ws_audit.update, f"A{audit_row_num}", [new_row_vals])
        print(f"  Upgraded {len(rows_to_update)} rows in {AUDIT_SHEET}")

    # ── Pass 2: append findings that have no existing audit row ───────────────
    rows_to_write = []
    skipped_existing = 0
    for r in ordered:
        ms_row_str = str(r["ms_row"])
        if ms_row_str in existing_audit_index:
            if ms_row_str not in upgraded_ms_rows:
                skipped_existing += 1
            continue
        rows_to_write.append(_audit_row(r))

    if skipped_existing:
        print(f"  Skipped {skipped_existing} rows already in {AUDIT_SHEET}")

    if rows_to_write:
        print(f"\nWriting {len(rows_to_write)} rows to {AUDIT_SHEET}...")
        sheets_call(ws_audit.append_rows, rows_to_write, value_input_option="USER_ENTERED")
        print(f"  Appended {len(rows_to_write)} rows to {AUDIT_SHEET}")

    if not rows_to_update and not rows_to_write:
        print(f"\nNo new rows to write to {AUDIT_SHEET}.")

    # ── Log to activity_log ──────────────────────────────────────────────────
    log_activity(
        ss,
        category="daily_audit",
        trace=(
            f"Audit for {target_date}: "
            f"clean={clean_count} auto_fixed={len(auto_fixed)} "
            f"needs_review={len(needs_review)}"
        ),
        metadata={
            "date": target_date,
            "total_picks": len(yesterday_picks),
            "clean": clean_count,
            "auto_fixed": len(auto_fixed),
            "needs_review": len(needs_review),
        },
    )

    # ── Re-sort master_sheet and recalculate ms_rows if any fixes were applied ──
    # Any auto-fix may change date or other sort-relevant fields.
    sorted_ms_vals: List[List[str]] = []
    if auto_fixed:
        print(f"\nRe-sorting master_sheet ({len(auto_fixed)} fix(es) applied)...")
        ms_ws_fresh = sheets_call(ss.worksheet, MASTER_SHEET)
        sorted_ms_vals = resort_master_sheet(ms_ws_fresh, dry_run=dry_run)
        recalculate_ms_rows(ws_audit, sorted_ms_vals, dry_run=dry_run)

    # ── Sync CSV if any rows were auto-fixed ─────────────────────────────────
    if auto_fixed:
        print(f"\nSyncing CSV ({len(auto_fixed)} auto-fix(es) applied)...")
        if sorted_ms_vals:
            git_push_csv(LOCAL_CSV_PATH, "audit: sync master_sheet CSV after auto-fixes", csv_content=sorted_ms_vals)

    print(f"\nDone. Review {AUDIT_SHEET} sheet for flagged rows.")


# ── master_sheet sort + audit_results ms_row recalculation ───────────────────

def resort_master_sheet(ms_ws: gspread.Worksheet, dry_run: bool = False) -> List[List[str]]:
    """Re-sort master_sheet rows by (date, capper, game) ascending and write back.

    Sort order:
      1. date (YYYY-MM-DD ascending)
      2. capper (A-Z ascending)
      3. game  (A-Z ascending)

    Returns the sorted values (header + data) for use in subsequent steps.
    In dry-run mode, computes and prints the changes without writing.
    """
    all_vals = sheets_call(ms_ws.get_all_values)
    if len(all_vals) < 2:
        return all_vals
    header = all_vals[0]
    if "date" not in header:
        print("  Warning: 'date' column not found in master_sheet; skipping sort")
        return all_vals
    date_idx = header.index("date")
    capper_idx = header.index("capper") if "capper" in header else None
    game_idx = header.index("game") if "game" in header else None
    data = all_vals[1:]

    def sort_key(r):
        d = r[date_idx] if len(r) > date_idx else ""
        c = r[capper_idx].lower() if capper_idx is not None and len(r) > capper_idx else ""
        g = r[game_idx].lower() if game_idx is not None and len(r) > game_idx else ""
        return (d, c, g)

    original_ids = [id(r) for r in data]
    data.sort(key=sort_key)
    sorted_ids = [id(r) for r in data]
    moved = sum(1 for a, b in zip(original_ids, sorted_ids) if a != b)

    sorted_vals = [header] + data
    if dry_run:
        print(f"  [dry-run] Would re-sort master_sheet ({len(data)} rows by date/capper/game, {moved} row(s) would move)")
    else:
        sheets_call(ms_ws.update, "A1", sorted_vals)
        print(f"  Re-sorted master_sheet ({len(data)} rows by date/capper/game, {moved} row(s) moved)")
    return sorted_vals


def recalculate_ms_rows(ws_audit: gspread.Worksheet, sorted_ms_vals: List[List[str]], dry_run: bool = False) -> None:
    """Update ms_row in all audit_results rows after master_sheet has been re-sorted.

    Looks up each audit row's composite key (date, capper, sport, pick, line)
    in the sorted master_sheet and writes ALL matching row numbers as a
    comma-separated string. Multiple values make pre-cleanup collisions visible
    and flag unexpected future collisions.
    In dry-run mode, prints what would change without writing.
    """
    if len(sorted_ms_vals) < 2:
        return

    ms_header = sorted_ms_vals[0]
    composite_cols = ["date", "capper", "sport", "pick", "line"]
    try:
        col_idxs = [ms_header.index(c) for c in composite_cols]
    except ValueError:
        print("  Warning: master_sheet missing expected columns; skipping ms_row recalculation")
        return

    # Build composite key → all matching 1-based row numbers
    key_to_ms_rows: Dict[Tuple, List[int]] = {}
    for i, row in enumerate(sorted_ms_vals[1:], start=2):
        key = tuple(row[idx] if len(row) > idx else "" for idx in col_idxs)
        key_to_ms_rows.setdefault(key, []).append(i)

    audit_vals = sheets_call(ws_audit.get_all_values)
    if len(audit_vals) <= 5:
        return

    audit_hdr = audit_vals[4]  # header is row 5 (0-indexed: 4)
    if "ms_row" not in audit_hdr:
        return

    ms_row_col_idx = audit_hdr.index("ms_row")
    col_letter = chr(ord("A") + ms_row_col_idx)

    try:
        audit_composite_idxs = [audit_hdr.index(c) for c in composite_cols]
    except ValueError:
        print("  Warning: audit_results missing expected columns; skipping ms_row recalculation")
        return

    updates = []
    for i, row in enumerate(audit_vals[5:], start=6):
        key = tuple(row[idx] if len(row) > idx else "" for idx in audit_composite_idxs)
        matches = key_to_ms_rows.get(key)
        if matches is None:
            continue
        new_val = ",".join(str(r) for r in matches)
        current = row[ms_row_col_idx].strip() if len(row) > ms_row_col_idx else ""
        if new_val != current:
            updates.append((f"{col_letter}{i}", [[new_val]]))

    if updates:
        if dry_run:
            print(f"  [dry-run] Would update ms_row for {len(updates)} audit_results row(s):")
            for cell, val in updates:
                print(f"    {cell} → {val[0][0]}")
        else:
            print(f"  Recalculating ms_row for {len(updates)} audit_results row(s)...")
            for cell, val in updates:
                sheets_call(ws_audit.update, cell, val)
            print(f"  Updated {len(updates)} ms_row value(s) in {AUDIT_SHEET}")
    else:
        print(f"  ms_row values already up to date")


def main():
    parser = argparse.ArgumentParser(description="Nightly audit of yesterday's picks")
    parser.add_argument("--dry-run",    action="store_true", help="Preview without writing")
    parser.add_argument("--force-opus", action="store_true", help="Run Opus regardless of time gate")
    parser.add_argument("--date",       type=str,            help="Override target date (YYYY-MM-DD)")
    parser.add_argument("--resort",     action="store_true", help="Re-sort master_sheet by date and recalculate ms_row in audit_results, then sync CSV. Skips the audit.")
    args = parser.parse_args()

    print("Connecting to Google Sheets...")
    gc = get_gspread_client()
    ss = gc.open_by_key(GOOGLE_SHEET_ID)

    if args.resort:
        ms_ws    = sheets_call(ss.worksheet, MASTER_SHEET)
        ws_audit = get_or_create_audit_sheet(ss)
        print("Re-sorting master_sheet...")
        sorted_vals = resort_master_sheet(ms_ws, dry_run=args.dry_run)
        recalculate_ms_rows(ws_audit, sorted_vals, dry_run=args.dry_run)
        if args.dry_run:
            print("[dry-run] Skipping CSV push.")
        else:
            git_push_csv(LOCAL_CSV_PATH, "audit: sync master_sheet CSV after re-sort", csv_content=sorted_vals)
        return

    run_audit(ss, target_date=args.date, dry_run=args.dry_run, force_opus=args.force_opus)


if __name__ == "__main__":
    main()
