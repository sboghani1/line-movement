#!/usr/bin/env python3
"""
daily_audit.py — Audit yesterday's parsed picks for hallucinations.

Run once a day (e.g. at midnight PST via cron).
Opus Pass 2 only fires if the script runs within the first 15 minutes after
midnight PST; otherwise only Pass 1 (free Python checks) runs.

Pass 1: Python substring/abbreviation check via pick_in_ocr()
Pass 2: Claude Opus confirmation of Pass 1 suspects, with yesterday's schedule
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
import base64
import argparse
import time
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Tuple
from collections import defaultdict

import anthropic
import gspread
from dotenv import load_dotenv
from google.oauth2.service_account import Credentials

# Import reusable helpers from audit_hallucinations
from audit_hallucinations import pick_in_ocr, opus_audit_suspects, ABBREV_MAP
from activity_logger import log_activity

load_dotenv()

GOOGLE_SHEET_ID    = "1LzkU7rH3OtrJckV5oMvFHyuLAnbRn9E74FO1uyfM65k"

# parsed_picks_new is the append-only source of truth for the audit.
# It mirrors master_sheet but also carries ocr_text (col 10) so the auditor
# can verify each pick against the raw image text.  Unlike parsed_picks (which
# is cleared after Stage 2), this sheet is NEVER truncated or rewritten —
# rows accumulate indefinitely so audit history is preserved across sessions.
PICKS_NEW_SHEET    = "parsed_picks_new"

AUDIT_SHEET        = "audit_data"
SPORT_TO_SCHED     = {"nba": "nba_schedule", "cbb": "cbb_schedule", "nhl": "nhl_schedule"}

PICKS_HEADERS = ["date", "capper", "sport", "pick", "line", "game", "spread", "side", "result", "ocr_text"]
AUDIT_HEADERS = ["date", "capper", "sport", "pick", "line", "game", "spread", "side", "result", "reason", "ocr_text"]

# PST = UTC-8 (standard time); PDT = UTC-7; we use UTC-8 conservatively.
# The fixed -8 offset means the gate fires slightly late during daylight saving
# (00:00–00:15 PDT = 07:00–07:15 UTC, but we treat it as 08:00–08:15 UTC).
# This is intentional: a small miss is cheaper than an accidental double-run.
PST_OFFSET = timezone(timedelta(hours=-8))

# Opus only runs if the script fires within this window after midnight PST.
# discord_image_fetcher.py is scheduled nightly, so the gate ensures Opus
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


# ── Google Sheets auth ─────────────────────────────────────────────────────────
def get_gspread_client():
    creds_b64 = os.environ.get("GOOGLE_CREDENTIALS", "")
    if not creds_b64:
        raise ValueError("GOOGLE_CREDENTIALS not set")
    creds_dict = json.loads(base64.b64decode(creds_b64).decode())
    creds = Credentials.from_service_account_info(creds_dict, scopes=[
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ])
    return gspread.authorize(creds)


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
    """
    Return True if the pick team fuzzy-matches any team in the schedule for that sport.
    Uses the same substring logic as populate_results.team_matches().
    Totals picks (pick contains '/') are checked against both halves.
    """
    games = schedule.get(sport.lower(), [])
    if not games:
        # No schedule loaded for this sport — can't flag, give benefit of the doubt
        return True

    pick_lower = pick.lower().strip()

    # Handle totals like "Georgia Southern/Marshall" — check each team separately
    pick_parts = [p.strip().lower() for p in pick_lower.split("/")]

    for away, home in games:
        away_l = away.lower()
        home_l = home.lower()
        for part in pick_parts:
            if part in away_l or away_l in part or part in home_l or home_l in part:
                return True
    return False


# ── Pass 2: Opus with schedule context ────────────────────────────────────────
def opus_audit_with_schedule(
    suspects: List[Dict],
    schedule_context: str,
    dry_run: bool = False,
) -> Tuple[List[Dict], List[Dict], float]:
    """
    Opus audit that includes yesterday's schedule in the prompt for better context.
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
    Run the daily hallucination audit. Designed to be called from discord_image_fetcher.py
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
    print(f"Opus Pass 2: {'YES' if run_opus else 'NO (outside 15-min window after midnight PST)'}")

    # Load yesterday's schedule for context
    print(f"\nLoading schedule for {target_date}...")
    schedule = load_schedule_for_date(ss, target_date)
    schedule_context = format_schedule_context(schedule)
    print(f"Schedule context:\n{schedule_context}")

    # Load picks
    print(f"\nLoading {PICKS_NEW_SHEET}...")
    ws_picks = ss.worksheet(PICKS_NEW_SHEET)
    all_values = ws_picks.get_all_values()

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

    # ── Pass 0: game-match check ──────────────────────────────────────────────
    # Flag picks where the pick team cannot be found in any game on the schedule
    # for that sport on target_date. These are written to audit_data and logged
    # regardless of the Opus gate — they don't require Opus to detect.
    no_game_rows   = []
    game_check_rows = []  # rows that passed (or have no schedule loaded)

    for row in yesterday_rows:
        while len(row) <= max(pick_col, sport_col, ocr_col):
            row.append("")
        sport_val = row[sport_col].strip().lower()
        pick_val  = row[pick_col].strip()

        # Skip totals (over/under) — they name two teams and game matching is unreliable
        line_val = row[line_col].strip().upper() if len(row) > line_col else ""
        if line_val.startswith("O ") or line_val.startswith("U "):
            game_check_rows.append(row)
            continue

        if pick_val and not pick_team_in_schedule(pick_val, sport_val, schedule):
            no_game_rows.append(row)
        else:
            game_check_rows.append(row)

    print(f"\nPass 0 (game-match check):")
    print(f"  Matched a schedule game: {len(game_check_rows)}")
    print(f"  No game found:           {len(no_game_rows)}")
    if no_game_rows:
        for row in no_game_rows:
            print(f"    [{row[date_col]}] {row[capper_col]} | {row[sport_col]} | {row[pick_col]} {row[line_col] if len(row) > line_col else ''}")

    # ── Pass 1: Python substring check ───────────────────────────────────────
    pass1_suspects = []
    pass1_clean    = []
    no_ocr_rows    = []

    for row in game_check_rows:
        while len(row) <= max(pick_col, ocr_col):
            row.append("")
        pick = row[pick_col].strip()
        ocr  = row[ocr_col].strip()

        if not ocr:
            no_ocr_rows.append(row)
        elif not pick or pick_in_ocr(pick, ocr):
            pass1_clean.append(row)
        else:
            pass1_suspects.append(row)

    print(f"\nPass 1 (substring check):")
    print(f"  Clean:    {len(pass1_clean)}")
    print(f"  Suspects: {len(pass1_suspects)}")
    print(f"  No OCR:   {len(no_ocr_rows)}")

    # ── Pass 2: Opus (time-gated) ─────────────────────────────────────────────
    confirmed_hallucinations = []

    if pass1_suspects and run_opus:
        print(f"\nPass 2 (Opus audit of {len(pass1_suspects)} suspects)...")
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
            for row in pass1_suspects
        ]
        confirmed_dicts, cleared_dicts, opus_cost = opus_audit_with_schedule(
            suspect_dicts,
            schedule_context=schedule_context,
            dry_run=dry_run,
        )
        confirmed_hallucinations = confirmed_dicts

        # Log cost to activity_log sheet
        log_activity(
            ss,
            category="daily_audit",
            trace=f"Opus audit for {target_date}: {len(confirmed_dicts)} hallucinations, "
                  f"{len(cleared_dicts)} cleared, ${opus_cost:.4f}",
            metadata={
                "date": target_date,
                "suspects": len(suspect_dicts),
                "confirmed": len(confirmed_dicts),
                "cleared": len(cleared_dicts),
                "opus_cost_usd": round(opus_cost, 6),
            },
        )

    elif pass1_suspects and not run_opus:
        print(f"\nSkipping Opus (outside 15-min window). {len(pass1_suspects)} Pass 1 suspects not escalated.")

    print(f"\nAudit summary for {target_date}:")
    print(f"  Total picks:              {len(yesterday_rows)}")
    print(f"  Pass 0 no game found:     {len(no_game_rows)}")
    print(f"  Pass 1 clean:             {len(pass1_clean)}")
    print(f"  Pass 1 suspects:          {len(pass1_suspects)}")
    print(f"  No OCR (skipped):         {len(no_ocr_rows)}")
    print(f"  Confirmed hallucinations: {len(confirmed_hallucinations)}")

    if not confirmed_hallucinations and not no_game_rows:
        print("\nNothing to write to audit_data.")
        return

    if dry_run:
        print("\n[dry-run] Would write these rows to audit_data:")
        for s in confirmed_hallucinations:
            print(f"  [hallucination] [{s['date']}] {s['capper']} | {s['sport']} | {s['pick']} — {s.get('reason','')}")
        for row in no_game_rows:
            print(f"  [no_game_found] [{row[date_col]}] {row[capper_col]} | {row[sport_col]} | {row[pick_col]}")
        return

    # ── Write confirmed hallucinations + no-game rows to audit_data ───────────
    all_audit_rows = []

    for s in confirmed_hallucinations:
        all_audit_rows.append([
            s.get("date",     ""),
            s.get("capper",   ""),
            s.get("sport",    ""),
            s.get("pick",     ""),
            s.get("line",     ""),
            s.get("game",     ""),
            s.get("spread",   ""),
            s.get("side",     ""),
            s.get("result",   ""),
            s.get("reason",   ""),
            s.get("ocr_text", ""),
        ])

    for row in no_game_rows:
        all_audit_rows.append([
            row[date_col]                          if len(row) > date_col   else "",
            row[capper_col]                        if len(row) > capper_col else "",
            row[sport_col]                         if len(row) > sport_col  else "",
            row[pick_col]                          if len(row) > pick_col   else "",
            row[line_col]                          if len(row) > line_col   else "",
            row[game_col]                          if len(row) > game_col   else "",
            row[spread_col]                        if len(row) > spread_col else "",
            row[side_col]                          if len(row) > side_col   else "",
            row[result_col]                        if len(row) > result_col else "",
            "no_game_found",
            row[ocr_col]                           if len(row) > ocr_col   else "",
        ])

    if not all_audit_rows:
        print("\nNothing to write to audit_data.")
        return

    print(f"\nWriting {len(all_audit_rows)} rows to {AUDIT_SHEET} "
          f"({len(confirmed_hallucinations)} hallucinations, {len(no_game_rows)} no-game)...")
    ws_audit = get_or_create_audit_sheet(ss)
    time.sleep(1)
    ws_audit.append_rows(all_audit_rows, value_input_option="USER_ENTERED")
    print(f"  Appended {len(all_audit_rows)} rows to {AUDIT_SHEET}")

    if no_game_rows:
        log_activity(
            ss,
            category="daily_audit",
            trace=f"No-game picks for {target_date}: {len(no_game_rows)} picks had no matching schedule game",
            metadata={
                "date": target_date,
                "no_game_count": len(no_game_rows),
                "picks": [
                    f"{row[capper_col]}|{row[sport_col]}|{row[pick_col]}"
                    for row in no_game_rows
                ],
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
