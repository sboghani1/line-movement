#!/usr/bin/env python3
"""
daily_audit.py — Nightly audit of yesterday's picks in master_sheet.

Runs a series of programmatic checks on every pick from the previous day,
auto-fixes what it can, flags ambiguous cases for Opus review, and writes
everything to the `audit_data` sheet with a status column that progresses:

    programmatic checks
        |-- all pass            --> (no audit_data row)
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

Checks documented but not yet implemented:
  2. result_correctness    (recompute result from score + line, compare to stored)
  3. game_match            (pick team must appear in schedule for that sport/date)
  4. ambiguous_team        (pick team substring-matches multiple schedule games)
  5. wrong_game            (pick team in schedule but game column points elsewhere)
  6. side_consistency      (side home/away must match team position in game column)
  7. spread_consistency    (spread field should equal "pick line", sign matches side)
  8. ocr_grounding         (pick team must appear in raw ocr_text)
  9. duplicate_detection   (same date+capper+pick+line appears more than once)

Usage:
  .venv/bin/python3 daily_audit.py              # normal run
  .venv/bin/python3 daily_audit.py --dry-run    # preview without writing
  .venv/bin/python3 daily_audit.py --force-opus # run Opus regardless of time gate
  .venv/bin/python3 daily_audit.py --date 2026-03-17
"""

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
from populate_results import determine_result, find_score, load_scores
from sheets_utils import GOOGLE_SHEET_ID, get_gspread_client, sheets_call, SPORT_TO_SCHED

load_dotenv()


# ── Constants ─────────────────────────────────────────────────────────────────
MASTER_SHEET    = "master_sheet"
AUDIT_SHEET     = "audit_data"
PICKS_NEW_SHEET = "parsed_picks_new"   # read-only; used to look up ocr_text

MASTER_HEADERS = ["date", "capper", "sport", "pick", "line", "game", "spread", "side", "result"]

# New audit_data schema: status is col B for easy scanning; ms_row links back to master_sheet
AUDIT_HEADERS = [
    "date", "status", "ms_row",
    "capper", "sport", "pick", "line",
    "game", "spread", "side", "result",
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
]

# Required columns that must be non-empty for a pick to be considered complete.
# "spread" is intentionally excluded — ML picks have an empty spread.
REQUIRED_COLUMNS = ["date", "capper", "sport", "pick", "line", "game", "side", "result"]

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
    """Get or create the audit_data worksheet with the correct headers and status dropdown."""
    try:
        ws = ss.worksheet(AUDIT_SHEET)
        existing = ws.row_values(1)
        if existing != AUDIT_HEADERS:
            # Schema changed — overwrite header row
            sheets_call(ws.update, "A1", [AUDIT_HEADERS])
            _apply_status_validation(ws)
        return ws
    except gspread.exceptions.WorksheetNotFound:
        ws = ss.add_worksheet(title=AUDIT_SHEET, rows=1000, cols=len(AUDIT_HEADERS))
        ws.append_row(AUDIT_HEADERS, value_input_option="USER_ENTERED")
        _apply_status_validation(ws)
        return ws


def _apply_status_validation(ws: gspread.Worksheet):
    """Apply data-validation dropdown for the status column (B2:B1000)."""
    from gspread.utils import ValidationConditionType

    status_col_idx = AUDIT_HEADERS.index("status")  # 0-based
    col_letter = chr(ord("A") + status_col_idx)      # "B"
    validation_range = f"{col_letter}2:{col_letter}1000"

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
def load_schedule_for_date(ss, target_date: str) -> Dict[str, List[Tuple[str, str]]]:
    """
    Returns {sport: [(away_team, home_team), ...]} for games on target_date.
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
            games = []
            for row in rows[1:]:
                while len(row) <= max(date_idx, away_idx, home_idx):
                    row.append("")
                if row[date_idx].strip() == target_date:
                    away = row[away_idx].strip()
                    home = row[home_idx].strip()
                    if away and home:
                        games.append((away, home))
            schedule[sport] = games
            print(f"  {sheet_name}: {len(games)} games on {target_date}")
        except gspread.exceptions.WorksheetNotFound:
            schedule[sport] = []
    return schedule


def format_schedule_context(schedule: Dict[str, List[Tuple[str, str]]]) -> str:
    """Format a schedule dict as a compact string for Opus prompts."""
    lines = []
    for sport, games in schedule.items():
        if games:
            game_strs = [f"{a} @ {h}" for a, h in games]
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
    """Build a list suitable for appending to audit_data (matches AUDIT_HEADERS order)."""
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
        pick_row.get("side", ""),
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

    Required columns: date, capper, sport, pick, line, game, side, result.
    (spread is excluded — ML picks legitimately have empty spread.)

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
#   populate_results.py fills results, but if the game/spread/side were wrong at
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
#   A wrong game means the spread, side, and result are all derived from the wrong
#   matchup.
#
# Logic:
#   1. Find the correct game from the schedule (same as check 3 matching).
#   2. Compare stored game column to "Away @ Home" from the schedule.
#   3. If they match → pass.
#   4. If they differ and the correct game is unambiguous (check 4 passed) →
#      auto_fixed: overwrite game, recompute spread/side/result.
#   5. If they differ and ambiguous (check 4 flagged) → needs_review.
#
# Examples:
#   pick="Michigan Wolverines", game="Indiana Hoosiers @ Michigan State Spartans"
#     Schedule shows Michigan Wolverines @ Ohio State Buckeyes
#     → auto_fixed (if unambiguous)
#     suggested_fix="game=Michigan Wolverines @ Ohio State Buckeyes, side=away"
#
#   pick="Michigan", game="Indiana @ Michigan State"
#     Schedule has both Michigan and Michigan State games
#     → needs_review (ambiguous — can't auto-fix)
#
# What gets corrected on auto-fix:
#   - game = "Away @ Home" from schedule
#   - side = "away" or "home" based on pick team position
#   - spread = "PickTeam LINE" (or "" for ML)
#   - result = recomputed from correct game's score (if available)
#
# ── Check 6: side_consistency ────────────────────────────────────────────────
# The `side` column (home/away) must match the pick team's position in the
# `game` column.  If game="Duke @ UNC" and pick="Duke", side must be "away".
#
# Logic:
#   1. Parse game column: "Away @ Home" → away_team, home_team.
#   2. Fuzzy-match pick against away_team → expected side = "away".
#   3. Fuzzy-match pick against home_team → expected side = "home".
#   4. Compare stored side to expected.
#   5. If match → pass.  If mismatch → auto_fixed.
#
# Examples:
#   game="Duke Blue Devils @ North Carolina Tar Heels", pick="Duke", side="home"
#     → auto_fixed, suggested_fix="side=away"
#
#   game="Duke Blue Devils @ North Carolina Tar Heels", pick="Duke", side="away"
#     → pass
#
#   game="Duke Blue Devils @ North Carolina Tar Heels", pick="Syracuse"
#     → pick not found in game string — this is a check 5 problem, skip here.
#
# Edge cases:
#   - If game column is blank, skip (check 1 already flags missing game).
#   - If pick doesn't match either team in the game string, skip (check 5 handles).
#
# ── Check 7: spread_consistency ──────────────────────────────────────────────
# The `spread` column should equal "{pick} {line}" for spread bets, or be
# empty for ML bets.  The spread's sign should also be consistent with the
# side (home favorites typically have negative lines, etc., though this is
# not a hard rule — just a soft sanity check).
#
# Logic:
#   1. If line is "ML" → spread must be empty.  If not → auto_fixed.
#   2. If line is numeric → spread must be "{pick} {line}".  If not → auto_fixed.
#   3. (Soft check) If line sign seems wrong for the side, flag needs_review
#      rather than auto-fix (line sign is set by the capper, not derivable).
#
# Examples:
#   pick="Duke", line="-3.5", spread="Duke -3.5" → pass
#   pick="Duke", line="-3.5", spread="UNC +3.5"  → auto_fixed, suggested_fix="spread=Duke -3.5"
#   pick="Duke", line="ML",   spread="Duke ML"   → auto_fixed, suggested_fix="spread=" (blank)
#   pick="Duke", line="ML",   spread=""           → pass
#
# ── Check 8: ocr_grounding ──────────────────────────────────────────────────
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
# ── Check 9: duplicate_detection ─────────────────────────────────────────────
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

    Runs all programmatic checks, writes findings to audit_data, and
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

        # Future checks would be added here:
        # result = check_result_correctness(pick_dict, scores, ms_ws, row_num, dry_run)
        # if result: findings.append(result)
        #
        # result = check_game_match(pick_dict, schedule, ...)
        # if result: findings.append(result)
        # ... etc.

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
        print("\nAll picks clean — nothing to write to audit_data.")
        return

    if dry_run:
        print(f"\n[dry-run] Would write {len(audit_results)} rows to {AUDIT_SHEET}.")
        return

    # ── Write to audit_data ──────────────────────────────────────────────────
    # Order: needs_review first, then auto_fixed at the bottom
    ordered = needs_review + auto_fixed

    rows_to_write = []
    for r in ordered:
        p = r["pick_row"]
        ocr_key = (p["date"], p["capper"], p["sport"], p["pick"], p["line"])
        ocr_text = ocr_index.get(ocr_key, "")
        rows_to_write.append(make_audit_row(
            pick_row=p,
            check_failed=r["check_failed"],
            details=r["details"],
            suggested_fix=r["suggested_fix"],
            status=r["status"],
            ms_row=r["ms_row"],
            ocr_text=ocr_text,
        ))

    print(f"\nWriting {len(rows_to_write)} rows to {AUDIT_SHEET}...")
    ws_audit = get_or_create_audit_sheet(ss)
    time.sleep(1)
    sheets_call(ws_audit.append_rows, rows_to_write, value_input_option="USER_ENTERED")
    print(f"  Appended {len(rows_to_write)} rows to {AUDIT_SHEET}")

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

    print(f"\nDone. Review {AUDIT_SHEET} sheet for flagged rows.")


def main():
    parser = argparse.ArgumentParser(description="Nightly audit of yesterday's picks")
    parser.add_argument("--dry-run",    action="store_true", help="Preview without writing")
    parser.add_argument("--force-opus", action="store_true", help="Run Opus regardless of time gate")
    parser.add_argument("--date",       type=str,            help="Override target date (YYYY-MM-DD)")
    args = parser.parse_args()

    print("Connecting to Google Sheets...")
    gc = get_gspread_client()
    ss = gc.open_by_key(GOOGLE_SHEET_ID)

    run_audit(ss, target_date=args.date, dry_run=args.dry_run, force_opus=args.force_opus)


if __name__ == "__main__":
    main()
