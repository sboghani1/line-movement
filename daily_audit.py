#!/usr/bin/env python3
"""
daily_audit.py — Audit yesterday's parsed picks for hallucinations and bad game assignments.

Run once a day (e.g. at midnight PST via cron).
Pass 4 (Opus) only fires if the script runs within the first 15 minutes after
midnight PST; otherwise only Passes 1–3 (free Python checks) run.

Pass 1: Game-match check — pick team must appear in a schedule game for that sport/date.
Pass 2: Wrong-game fix — picks that failed Pass 1 are checked for a correct game
        elsewhere in the schedule; if found, game/spread/side/result are corrected
        in master_sheet and parsed_picks_new.
Pass 3: OCR substring check via pick_in_ocr() — pick team must appear in OCR text.
Pass 4: Claude Opus confirmation of Pass 3 suspects, with yesterday's schedule
        context included in each batch prompt.

Failures are appended to the 'audit_data' sheet:
  date, capper, sport, pick, line, game, spread, side, result, reason, ocr_text

Usage:
  .venv/bin/python3 daily_audit.py             # normal run
  .venv/bin/python3 daily_audit.py --dry-run   # preview without writing
  .venv/bin/python3 daily_audit.py --force-opus  # run Opus regardless of time
"""

import os
import re
import json
import argparse
import time
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Tuple
from collections import defaultdict

import anthropic
import gspread
from dotenv import load_dotenv

# Import reusable helpers from audit_hallucinations
from audit_hallucinations import pick_in_ocr, opus_audit_suspects, ABBREV_MAP
from activity_logger import log_activity
from populate_results import determine_result, find_score, load_scores
from sheets_utils import GOOGLE_SHEET_ID, get_gspread_client, sheets_call, SPORT_TO_SCHED

load_dotenv()


# parsed_picks_new is the append-only source of truth for the audit.
# It mirrors master_sheet but also carries ocr_text (col 10) so the auditor
# can verify each pick against the raw image text.  Unlike parsed_picks (which
# is cleared after Stage 2), this sheet is NEVER truncated or rewritten —
# rows accumulate indefinitely so audit history is preserved across sessions.
PICKS_NEW_SHEET    = "parsed_picks_new"

AUDIT_SHEET        = "audit_data"

PICKS_HEADERS = ["date", "capper", "sport", "pick", "line", "game", "spread", "side", "result", "ocr_text"]
AUDIT_HEADERS = ["date", "capper", "sport", "pick", "line", "game", "spread", "side", "result", "reason", "ocr_text"]

# PST = UTC-8 (standard time); PDT = UTC-7; we use UTC-8 conservatively.
# The fixed -8 offset means the gate fires slightly late during daylight saving
# (00:00–00:15 PDT = 07:00–07:15 UTC, but we treat it as 08:00–08:15 UTC).
# This is intentional: a small miss is cheaper than an accidental double-run.
PST_OFFSET = timezone(timedelta(hours=-8))

# Opus only runs if the script fires within this window after midnight PST.
# capper_analyzer.py is scheduled nightly, so the gate ensures Opus
# runs exactly once per day (the midnight invocation) without needing a
# separate cron job for the audit. Any run outside the window still does
# Pass 1 (free) but skips the expensive Opus call.
OPUS_GATE_MINUTES = 15

# Smaller batch than the backfill scripts: the schedule context string added to
# each prompt is ~500–1000 chars, so keeping batches at 10 stays well under the
# token limit and produces cleaner per-verdict JSON from the model.
OPUS_BATCH_SIZE   = 10


# ── Time gate ─────────────────────────────────────────────────────────────────
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
    """Get or create the audit_data worksheet with the correct headers."""
    try:
        ws = ss.worksheet(AUDIT_SHEET)
        existing = ws.row_values(1)
        if existing != AUDIT_HEADERS:
            ws.insert_row(AUDIT_HEADERS, index=1)
        return ws
    except gspread.exceptions.WorksheetNotFound:
        ws = ss.add_worksheet(title=AUDIT_SHEET, rows=1000, cols=len(AUDIT_HEADERS))
        ws.append_row(AUDIT_HEADERS, value_input_option="USER_ENTERED")
        return ws


# ── Schedule loader ───────────────────────────────────────────────────────────
def load_schedule_for_date(ss, target_date: str) -> Dict[str, List[Tuple[str, str]]]:
    """
    Returns {sport: [(away_team, home_team), ...]} for games on target_date.
    Only loads home_team + away_team (no spread) to keep context lean.
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
    """Format yesterday's schedule as a compact string for Opus prompts."""
    lines = []
    for sport, games in schedule.items():
        if games:
            game_strs = [f"{a} @ {h}" for a, h in games]
            lines.append(f"{sport.upper()}: {', '.join(game_strs)}")
    return "\n".join(lines) if lines else "(no games found)"


def pick_team_in_schedule(pick: str, sport: str, schedule: Dict[str, List[Tuple[str, str]]]) -> bool:
    """Return True if the pick team fuzzy-matches any team in the schedule for that sport."""
    return find_correct_game(pick, sport, schedule) is not None


def find_correct_game(
    pick: str,
    sport: str,
    schedule: Dict[str, List[Tuple[str, str]]],
) -> Tuple[str, str, str] | None:
    """
    Return (away, home, matched_sport) for the first schedule game matching
    the pick team, or None if not found.

    Searches the tagged sport first; if not found, searches other sports to
    catch wrong-sport tags (e.g. an NBA team tagged as CBB).
    matched_sport is the sport key where the match was found (may differ from
    the tagged sport).
    """
    pick_lower = pick.lower().strip()
    pick_parts = [p.strip().lower() for p in pick_lower.split("/")]

    def _match_in(games):
        for away, home in games:
            away_l = away.lower()
            home_l = home.lower()
            for part in pick_parts:
                if part in away_l or away_l in part or part in home_l or home_l in part:
                    return (away, home)
        return None

    # Try tagged sport first
    games = schedule.get(sport.lower(), [])
    if games:
        result = _match_in(games)
        if result:
            return (result[0], result[1], sport.lower())

    # No match in tagged sport — try other sports (wrong-sport tag)
    for other_sport, other_games in schedule.items():
        if other_sport == sport.lower() or not other_games:
            continue
        result = _match_in(other_games)
        if result:
            return (result[0], result[1], other_sport)

    # Only return None if we had a schedule for the tagged sport and found nothing anywhere
    if not schedule.get(sport.lower()):
        return None  # benefit of the doubt — no schedule loaded

    return None


def derive_game_spread_side(pick: str, line: str, away: str, home: str) -> Tuple[str, str, str]:
    """
    Given correct away/home teams and the pick's line, derive:
      game   = "Away @ Home"
      spread = "PickTeam LINE"  (or empty for ML)
      side   = "away" or "home"
    """
    game = f"{away} @ {home}"
    pick_l = pick.lower().strip()
    away_l = away.lower()
    home_l = home.lower()

    if pick_l in away_l or away_l in pick_l:
        side = "away"
    elif pick_l in home_l or home_l in pick_l:
        side = "home"
    else:
        side = ""

    line_clean = line.strip().upper()
    spread = f"{pick} {line}" if line_clean != "ML" else ""

    return game, spread, side


# ── Pass 4: Opus with schedule context ────────────────────────────────────────
def opus_audit_with_schedule(
    suspects: List[Dict],
    schedule_context: str,
    dry_run: bool = False,
) -> Tuple[List[Dict], List[Dict], float]:
    """
    Pass 4: Opus audit that includes yesterday's schedule in the prompt for better context.
    Returns (confirmed_hallucinations, false_positives, estimated_cost_usd).
    """
    if dry_run or not suspects:
        return suspects, [], 0.0

    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not api_key:
        raise ValueError("ANTHROPIC_API_KEY not set")

    client = anthropic.Anthropic(api_key=api_key)

    confirmed = []
    false_positives = []
    total_input_tokens  = 0
    total_output_tokens = 0

    for batch_start in range(0, len(suspects), OPUS_BATCH_SIZE):
        batch = suspects[batch_start: batch_start + OPUS_BATCH_SIZE]
        batch_end = batch_start + len(batch)
        print(f"  Opus batch {batch_start+1}–{batch_end} of {len(suspects)}...")

        rows_text = ""
        for i, s in enumerate(batch):
            rows_text += (
                f"[{i}] date={s['date']} capper={s['capper']} sport={s['sport']} "
                f"pick=\"{s['pick']}\" line={s['line']}\n"
                f"    OCR: {s['ocr_text'][:600]}\n\n"
            )

        prompt = f"""You are auditing betting pick data for hallucinations.

Yesterday's actual games (from official schedule):
{schedule_context}

For each numbered row below, determine: does the PICK team appear in the OCR text?
A pick is VALID if the team name, a standard abbreviation, or a common nickname
for that team appears anywhere in the OCR text — even partially.
Cross-reference the schedule above to confirm whether the team was actually playing.

Examples of VALID matches (not hallucinations):
- pick="Duke Blue Devils" OCR="Duke -15.5" → VALID (Duke in OCR)
- pick="BYU Cougars" OCR="BYU ML -120" → VALID (BYU abbreviation)
- pick="Cleveland Cavaliers" OCR="Cavs ML (-103)" → VALID (Cavs nickname)
- pick="USC Trojans" OCR="USC +6.5" → VALID (USC abbreviation)

Examples of HALLUCINATIONS (pick NOT in OCR):
- pick="Orlando Magic" OCR="Detroit Pistons +4 / Utah Jazz ML" → HALLUCINATION
- pick="Sacramento Kings" OCR="Utah Jazz -125 Detroit Pistons +4" → HALLUCINATION

Respond with a JSON array. Each element: {{"idx": <row index>, "verdict": "VALID" or "HALLUCINATION", "reason": "<brief>"}}
Output ONLY the JSON array, no other text.

ROWS TO AUDIT:
{rows_text}"""

        try:
            response = client.messages.create(
                model="claude-opus-4-6",
                max_tokens=2000,
                messages=[{"role": "user", "content": prompt}],
            )
            total_input_tokens  += response.usage.input_tokens
            total_output_tokens += response.usage.output_tokens

            text = response.content[0].text.strip()
            if text.startswith("```"):
                text = text.split("```")[1]
                if text.startswith("json"):
                    text = text[4:]
                text = text.strip()

            verdicts = json.loads(text)
            verdict_map = {v["idx"]: v for v in verdicts}

            for i, suspect in enumerate(batch):
                entry = verdict_map.get(i, {})
                verdict = entry.get("verdict", "VALID")
                reason  = entry.get("reason", "")
                if verdict == "HALLUCINATION":
                    suspect["reason"] = reason
                    confirmed.append(suspect)
                    print(f"    HALLUCINATION [{suspect['date']}] {suspect['capper']} | {suspect['pick']} — {reason}")
                else:
                    false_positives.append(suspect)

        except Exception as e:
            print(f"  Warning: Opus API error on batch {batch_start}–{batch_end}: {e}")
            for s in batch:
                s.setdefault("reason", f"Opus error: {e}")
            confirmed.extend(batch)

        if batch_end < len(suspects):
            time.sleep(1)

    cost = (
        (total_input_tokens  / 1_000_000) * 15.00 +
        (total_output_tokens / 1_000_000) * 75.00
    )
    print(f"  Opus tokens  input: {total_input_tokens:,}  output: {total_output_tokens:,}")
    print(f"  Estimated Opus cost: ${cost:.4f}")
    print(f"  Confirmed: {len(confirmed)}  |  False positives cleared: {len(false_positives)}")

    return confirmed, false_positives, cost


# ── Main ───────────────────────────────────────────────────────────────────────
def run_audit(ss, target_date: str = None, dry_run: bool = False, force_opus: bool = False):
    """
    Run the daily hallucination audit. Designed to be called from capper_analyzer.py
    with an already-open gspread spreadsheet object.

    Args:
        ss:          Open gspread Spreadsheet object.
        target_date: Date string YYYY-MM-DD to audit; defaults to yesterday in PST.
        dry_run:     Preview without writing to audit_data.
        force_opus:  Skip the midnight time gate and always run Opus.
    """
    target_date = target_date or yesterday_str()
    run_opus = force_opus or within_midnight_window()

    print(f"Daily audit — target date: {target_date}")
    now_pst = datetime.now(PST_OFFSET)
    print(f"Current PST time: {now_pst.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Opus Pass 4: {'YES' if run_opus else 'NO (outside 15-min window after midnight PST)'}")

    # Load yesterday's schedule for context
    print(f"\nLoading schedule for {target_date}...")
    schedule = sheets_call(load_schedule_for_date, ss, target_date)
    schedule_context = format_schedule_context(schedule)
    print(f"Schedule context:\n{schedule_context}")

    # Load picks
    print(f"\nLoading {PICKS_NEW_SHEET}...")
    ws_picks = ss.worksheet(PICKS_NEW_SHEET)
    all_values = sheets_call(ws_picks.get_all_values)

    # Header is row 3 (index 2) per the sheet layout
    if len(all_values) < 4:
        print("Sheet has fewer than 4 rows — nothing to audit.")
        return

    header_row = all_values[2]
    col = {h: i for i, h in enumerate(header_row)}
    date_col   = col.get("date",     0)
    capper_col = col.get("capper",   1)
    sport_col  = col.get("sport",    2)
    pick_col   = col.get("pick",     3)
    line_col   = col.get("line",     4)
    game_col   = col.get("game",     5)
    spread_col = col.get("spread",   6)
    side_col   = col.get("side",     7)
    result_col = col.get("result",   8)
    ocr_col    = col.get("ocr_text", 9)

    data_rows = [r for r in all_values[3:] if any(cell.strip() for cell in r)]
    yesterday_rows = [r for r in data_rows if len(r) > date_col and r[date_col].strip() == target_date]
    print(f"  {len(data_rows)} total data rows, {len(yesterday_rows)} for {target_date}")

    if not yesterday_rows:
        print("No picks found for target date — nothing to audit.")
        return

    # Load scores for Pass 2 result-filling after wrong-game correction
    print(f"\nLoading scores from schedule sheets...")
    scores = sheets_call(load_scores, ss)

    # ── Pass 1: game-match check ──────────────────────────────────────────────
    # Flag picks where either:
    #   (a) the pick team can't be found in any schedule game for that sport/date, or
    #   (b) the stored game column doesn't match where the pick team is actually playing
    #       (Stage 2 assigned the wrong game via fuzzy substring match).
    # Totals (O/U lines) are exempt.
    pass1_no_game  = []   # team not in schedule at all, or stored game is wrong
    pass1_ok       = []

    for row in yesterday_rows:
        while len(row) <= max(pick_col, sport_col, line_col, game_col, ocr_col):
            row.append("")
        sport_val      = row[sport_col].strip().lower()
        pick_val       = row[pick_col].strip()
        line_val       = row[line_col].strip().upper()
        stored_game    = row[game_col].strip()

        # Skip totals — two-team picks don't map cleanly to a single schedule entry
        if line_val.startswith("O ") or line_val.startswith("U "):
            pass1_ok.append(row)
            continue

        correct_game = find_correct_game(pick_val, sport_val, schedule)

        if not pick_val or not schedule.get(sport_val):
            # No schedule loaded for this sport — benefit of the doubt
            pass1_ok.append(row)
            continue

        if correct_game is None:
            # Pick team not found in schedule at all
            pass1_no_game.append(row)
            continue

        # Check whether the stored game column matches the schedule-derived game
        correct_game_str = f"{correct_game[0]} @ {correct_game[1]}"
        if stored_game and stored_game != correct_game_str:
            # Stored game is wrong — Stage 2 assigned a different game
            pass1_no_game.append(row)
        else:
            pass1_ok.append(row)

    print(f"\nPass 1 (game-match check):")
    print(f"  Matched a schedule game: {len(pass1_ok)}")
    print(f"  No/wrong game found:     {len(pass1_no_game)}")
    if pass1_no_game:
        for row in pass1_no_game:
            print(f"    [{row[date_col]}] {row[capper_col]} | {row[sport_col]} | {row[pick_col]} {row[line_col]} | stored={row[game_col]}")

    # ── Pass 2: wrong-game detection and fix ─────────────────────────────────
    # For each Pass 1 failure, check if the pick team DOES appear in a different
    # game on the schedule — meaning Stage 2 assigned the wrong game.
    # If corrected, update game/spread/side/result in master_sheet + parsed_picks_new.
    wrong_game_fixed    = []   # corrected + result filled
    wrong_game_no_score = []   # corrected but no score available yet
    genuine_no_game     = []   # pick team not playing at all that day

    # We need sheet row indices to do targeted updates, so reload both sheets
    # and build a key→sheet_row_number index.
    def row_key(r):
        """Stable identity key for a pick row: date+capper+sport+pick+line."""
        return (
            r[date_col]   if len(r) > date_col   else "",
            r[capper_col] if len(r) > capper_col else "",
            r[sport_col]  if len(r) > sport_col  else "",
            r[pick_col]   if len(r) > pick_col   else "",
            r[line_col]   if len(r) > line_col   else "",
        )

    print(f"\nPass 2 (wrong-game detection)...")

    # Build index: key → list of 1-based sheet row numbers in master_sheet
    ms_ws   = sheets_call(ss.worksheet, "master_sheet")
    ms_vals = sheets_call(ms_ws.get_all_values)
    ms_header = ms_vals[0]
    ms_col = {h: i for i, h in enumerate(ms_header)}
    ms_key_to_rows: Dict[tuple, List[int]] = defaultdict(list)
    for idx, r in enumerate(ms_vals[1:], start=2):  # 1-based, row 1 = header
        while len(r) <= max(ms_col.get("line", 4), ms_col.get("result", 8)):
            r.append("")
        k = (
            r[ms_col.get("date",   0)],
            r[ms_col.get("capper", 1)],
            r[ms_col.get("sport",  2)],
            r[ms_col.get("pick",   3)],
            r[ms_col.get("line",   4)],
        )
        ms_key_to_rows[k].append(idx)

    # Build index for parsed_picks_new (header at row 3, data from row 4)
    pn_ws   = sheets_call(ss.worksheet, PICKS_NEW_SHEET)
    pn_vals = sheets_call(pn_ws.get_all_values)
    pn_header = pn_vals[2]
    pn_col = {h: i for i, h in enumerate(pn_header)}
    pn_key_to_rows: Dict[tuple, List[int]] = defaultdict(list)
    for idx, r in enumerate(pn_vals[3:], start=4):  # 1-based, rows 1-3 = meta+header
        while len(r) <= max(pn_col.get("line", 4), pn_col.get("result", 8)):
            r.append("")
        k = (
            r[pn_col.get("date",   0)],
            r[pn_col.get("capper", 1)],
            r[pn_col.get("sport",  2)],
            r[pn_col.get("pick",   3)],
            r[pn_col.get("line",   4)],
        )
        pn_key_to_rows[k].append(idx)

    ms_batch  = []  # [{range, values}, ...] for master_sheet
    pn_batch  = []  # [{range, values}, ...] for parsed_picks_new

    for row in pass1_no_game:
        pick_val  = row[pick_col].strip()
        sport_val = row[sport_col].strip().lower()
        line_val  = row[line_col].strip()
        date_val  = row[date_col].strip()

        correct_game = find_correct_game(pick_val, sport_val, schedule)
        if correct_game is None:
            genuine_no_game.append(row)
            continue

        away, home, matched_sport = correct_game
        new_game, new_spread, new_side = derive_game_spread_side(pick_val, line_val, away, home)
        sport_changed = matched_sport != sport_val

        # Try to fill result using the corrected game and matched sport
        score_str = find_score(pick_val, date_val, matched_sport, new_game, scores)
        new_result = determine_result(pick_val, line_val, new_game, score_str) if score_str else None

        key = row_key(row)

        if dry_run:
            label = "wrong_game_fixed" if new_result else "wrong_game_no_score"
            sport_note = f" [sport: {sport_val} → {matched_sport}]" if sport_changed else ""
            print(f"  [{label}] [{date_val}] {row[capper_col]} | {sport_val} | {pick_val} {line_val}{sport_note}")
            print(f"    was:  {row[game_col] if len(row) > game_col else ''}")
            print(f"    now:  {new_game}  result={new_result or '(pending)'}")
        else:
            # Queue updates for master_sheet
            for sheet_row in ms_key_to_rows.get(key, []):
                game_cell   = gspread.utils.rowcol_to_a1(sheet_row, ms_col.get("game",   5) + 1)
                spread_cell = gspread.utils.rowcol_to_a1(sheet_row, ms_col.get("spread", 6) + 1)
                side_cell   = gspread.utils.rowcol_to_a1(sheet_row, ms_col.get("side",   7) + 1)
                ms_batch.append({"range": game_cell,   "values": [[new_game]]})
                ms_batch.append({"range": spread_cell, "values": [[new_spread]]})
                ms_batch.append({"range": side_cell,   "values": [[new_side]]})
                if new_result:
                    result_cell = gspread.utils.rowcol_to_a1(sheet_row, ms_col.get("result", 8) + 1)
                    ms_batch.append({"range": result_cell, "values": [[new_result]]})
                if sport_changed and "sport" in ms_col:
                    sport_cell = gspread.utils.rowcol_to_a1(sheet_row, ms_col["sport"] + 1)
                    ms_batch.append({"range": sport_cell, "values": [[matched_sport]]})

            # Queue updates for parsed_picks_new
            for sheet_row in pn_key_to_rows.get(key, []):
                game_cell   = gspread.utils.rowcol_to_a1(sheet_row, pn_col.get("game",   5) + 1)
                spread_cell = gspread.utils.rowcol_to_a1(sheet_row, pn_col.get("spread", 6) + 1)
                side_cell   = gspread.utils.rowcol_to_a1(sheet_row, pn_col.get("side",   7) + 1)
                pn_batch.append({"range": game_cell,   "values": [[new_game]]})
                pn_batch.append({"range": spread_cell, "values": [[new_spread]]})
                pn_batch.append({"range": side_cell,   "values": [[new_side]]})
                if new_result:
                    result_cell = gspread.utils.rowcol_to_a1(sheet_row, pn_col.get("result", 8) + 1)
                    pn_batch.append({"range": result_cell, "values": [[new_result]]})
                if sport_changed and "sport" in pn_col:
                    sport_cell = gspread.utils.rowcol_to_a1(sheet_row, pn_col["sport"] + 1)
                    pn_batch.append({"range": sport_cell, "values": [[matched_sport]]})

        if new_result:
            wrong_game_fixed.append(row)
        else:
            wrong_game_no_score.append(row)

    print(f"  Wrong game corrected + result filled: {len(wrong_game_fixed)}")
    print(f"  Wrong game corrected, no score yet:   {len(wrong_game_no_score)}")
    print(f"  Genuine no-game (team not scheduled): {len(genuine_no_game)}")

    if not dry_run and (ms_batch or pn_batch):
        chunk_size = 500
        if ms_batch:
            for i in range(0, len(ms_batch), chunk_size):
                sheets_call(ms_ws.batch_update, ms_batch[i:i + chunk_size])
                if i + chunk_size < len(ms_batch):
                    time.sleep(1)
            print(f"  Wrote {len(ms_batch)} cell updates to master_sheet")
        if pn_batch:
            for i in range(0, len(pn_batch), chunk_size):
                sheets_call(pn_ws.batch_update, pn_batch[i:i + chunk_size])
                if i + chunk_size < len(pn_batch):
                    time.sleep(1)
            print(f"  Wrote {len(pn_batch)} cell updates to {PICKS_NEW_SHEET}")

    # Pass 3 and 4 operate only on rows that passed Pass 1
    # ── Pass 3: OCR substring check ───────────────────────────────────────────
    pass3_suspects = []
    pass3_clean    = []
    no_ocr_rows    = []

    for row in pass1_ok:
        while len(row) <= max(pick_col, ocr_col):
            row.append("")
        pick = row[pick_col].strip()
        ocr  = row[ocr_col].strip()

        if not ocr:
            no_ocr_rows.append(row)
        elif not pick or pick_in_ocr(pick, ocr):
            pass3_clean.append(row)
        else:
            pass3_suspects.append(row)

    print(f"\nPass 3 (OCR substring check):")
    print(f"  Clean:    {len(pass3_clean)}")
    print(f"  Suspects: {len(pass3_suspects)}")
    print(f"  No OCR:   {len(no_ocr_rows)}")

    # ── Pass 4: Opus (time-gated) ─────────────────────────────────────────────
    confirmed_hallucinations = []

    if pass3_suspects and run_opus:
        print(f"\nPass 4 (Opus audit of {len(pass3_suspects)} suspects)...")
        suspect_dicts = [
            {
                "date":     row[date_col],
                "capper":   row[capper_col],
                "sport":    row[sport_col],
                "pick":     row[pick_col],
                "line":     row[line_col],
                "game":     row[game_col]   if len(row) > game_col   else "",
                "spread":   row[spread_col] if len(row) > spread_col else "",
                "side":     row[side_col]   if len(row) > side_col   else "",
                "result":   row[result_col] if len(row) > result_col else "",
                "ocr_text": row[ocr_col],
                "reason":   "",
            }
            for row in pass3_suspects
        ]
        confirmed_dicts, cleared_dicts, opus_cost = opus_audit_with_schedule(
            suspect_dicts,
            schedule_context=schedule_context,
            dry_run=dry_run,
        )
        confirmed_hallucinations = confirmed_dicts

        # Log cost + full audit counts to activity_log
        log_activity(
            ss,
            category="daily_audit",
            trace=(
                f"Audit for {target_date}: "
                f"pass2_fixed={len(wrong_game_fixed)} pass2_no_score={len(wrong_game_no_score)} "
                f"pass2_genuine_no_game={len(genuine_no_game)} "
                f"pass4_hallucinations={len(confirmed_dicts)} pass4_cleared={len(cleared_dicts)} "
                f"${opus_cost:.4f}"
            ),
            metadata={
                "date": target_date,
                "wrong_game_fixed":     len(wrong_game_fixed),
                "wrong_game_no_score":  len(wrong_game_no_score),
                "genuine_no_game":      len(genuine_no_game),
                "pass4_suspects":       len(suspect_dicts),
                "pass4_confirmed":      len(confirmed_dicts),
                "pass4_cleared":        len(cleared_dicts),
                "opus_cost_usd":        round(opus_cost, 6),
            },
        )

    elif pass3_suspects and not run_opus:
        print(f"\nSkipping Pass 4 (outside 15-min window). {len(pass3_suspects)} Pass 3 suspects not escalated.")

    # ── Summary ───────────────────────────────────────────────────────────────
    print(f"\nAudit summary for {target_date}:")
    print(f"  Total picks:                        {len(yesterday_rows)}")
    print(f"  Pass 1 no game found:               {len(pass1_no_game)}")
    print(f"  Pass 2 wrong game fixed (w/ result):{len(wrong_game_fixed)}")
    print(f"  Pass 2 wrong game fixed (no score): {len(wrong_game_no_score)}")
    print(f"  Pass 2 genuine no-game:             {len(genuine_no_game)}")
    print(f"  Pass 3 clean:                       {len(pass3_clean)}")
    print(f"  Pass 3 suspects:                    {len(pass3_suspects)}")
    print(f"  No OCR (skipped):                   {len(no_ocr_rows)}")
    print(f"  Pass 4 confirmed hallucinations:    {len(confirmed_hallucinations)}")

    # Write audit_data rows for: confirmed hallucinations + genuine no-game +
    # wrong-game rows (both fixed and no-score, for traceability)
    audit_write_rows = confirmed_hallucinations + wrong_game_fixed + wrong_game_no_score + genuine_no_game

    if not audit_write_rows:
        print("\nNothing to write to audit_data.")
        return

    if dry_run:
        print("\n[dry-run] Would write these rows to audit_data:")
        for s in confirmed_hallucinations:
            print(f"  [hallucination]       [{s['date']}] {s['capper']} | {s['sport']} | {s['pick']} — {s.get('reason','')}")
        for row in wrong_game_fixed:
            print(f"  [wrong_game_fixed]    [{row[date_col]}] {row[capper_col]} | {row[sport_col]} | {row[pick_col]}")
        for row in wrong_game_no_score:
            print(f"  [wrong_game_no_score] [{row[date_col]}] {row[capper_col]} | {row[sport_col]} | {row[pick_col]}")
        for row in genuine_no_game:
            print(f"  [no_game_found]       [{row[date_col]}] {row[capper_col]} | {row[sport_col]} | {row[pick_col]}")
        return

    # ── Write to audit_data ───────────────────────────────────────────────────
    def _row_from_dict(s, reason):
        return [
            s.get("date", ""), s.get("capper", ""), s.get("sport", ""),
            s.get("pick", ""), s.get("line", ""), s.get("game", ""),
            s.get("spread", ""), s.get("side", ""), s.get("result", ""),
            reason, s.get("ocr_text", ""),
        ]

    def _row_from_list(r, reason):
        return [
            r[date_col]   if len(r) > date_col   else "",
            r[capper_col] if len(r) > capper_col else "",
            r[sport_col]  if len(r) > sport_col  else "",
            r[pick_col]   if len(r) > pick_col   else "",
            r[line_col]   if len(r) > line_col   else "",
            r[game_col]   if len(r) > game_col   else "",
            r[spread_col] if len(r) > spread_col else "",
            r[side_col]   if len(r) > side_col   else "",
            r[result_col] if len(r) > result_col else "",
            reason,
            r[ocr_col]    if len(r) > ocr_col   else "",
        ]

    all_audit_rows = (
        [_row_from_dict(s, s.get("reason", "hallucination")) for s in confirmed_hallucinations] +
        [_row_from_list(r, "wrong_game_fixed")    for r in wrong_game_fixed] +
        [_row_from_list(r, "wrong_game_no_score") for r in wrong_game_no_score] +
        [_row_from_list(r, "no_game_found")       for r in genuine_no_game]
    )

    print(f"\nWriting {len(all_audit_rows)} rows to {AUDIT_SHEET}...")
    ws_audit = get_or_create_audit_sheet(ss)
    time.sleep(1)
    sheets_call(ws_audit.append_rows, all_audit_rows, value_input_option="USER_ENTERED")
    print(f"  Appended {len(all_audit_rows)} rows to {AUDIT_SHEET}")

    # Log no-game / wrong-game counts if no Opus ran (Opus path already logged above)
    if not (pass3_suspects and run_opus):
        log_activity(
            ss,
            category="daily_audit",
            trace=(
                f"Audit for {target_date}: "
                f"pass2_fixed={len(wrong_game_fixed)} pass2_no_score={len(wrong_game_no_score)} "
                f"pass2_genuine_no_game={len(genuine_no_game)}"
            ),
            metadata={
                "date": target_date,
                "wrong_game_fixed":    len(wrong_game_fixed),
                "wrong_game_no_score": len(wrong_game_no_score),
                "genuine_no_game":     len(genuine_no_game),
            },
        )

    print(f"\nDone. Review {AUDIT_SHEET} sheet for flagged rows.")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run",    action="store_true", help="Preview without writing to audit_data")
    parser.add_argument("--force-opus", action="store_true", help="Run Opus regardless of time-of-day gate")
    parser.add_argument("--date",       type=str,            help="Override target date (YYYY-MM-DD); defaults to yesterday")
    args = parser.parse_args()

    print("Connecting to Google Sheets...")
    gc = get_gspread_client()
    ss = gc.open_by_key(GOOGLE_SHEET_ID)

    run_audit(ss, target_date=args.date, dry_run=args.dry_run, force_opus=args.force_opus)


if __name__ == "__main__":
    main()
