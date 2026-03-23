#!/usr/bin/env python3
"""
Backfill Stage 1: image_pull_new -> parsed_picks_new

Steps:
  1. Dedup image_pull_new in-sheet: remove exact-duplicate rows by
     (pick_date, capper_name, ocr_text) — keeps the first occurrence.
  2. Parse each remaining row's OCR text with Claude Sonnet using
     [ROW:N] anchoring (same pattern as capper_analyzer.py).
  3. Write parsed picks to parsed_picks_new (headers on row 3, data row 4+).
  4. After each batch: deduplicate ML vs spread picks (same capper+date+pick).

image_pull_new columns: timestamp, message_sent_at, capper_name, image_url, ocr_text, stage
The pick date is derived from message_sent_at (YYYY-MM-DD).

Run with:
  .venv/bin/python3 backfill_stage1.py           # resume from saved offset
  .venv/bin/python3 backfill_stage1.py --reset   # reset offset to 0, keep sheet
  .venv/bin/python3 backfill_stage1.py --clear   # clear output sheet + reset offset
(.env is loaded automatically)
"""

import os
import sys
import json
import base64
import re
import time
import argparse
from typing import List, Dict, Tuple, Optional

from dotenv import load_dotenv
load_dotenv()

import gspread
from google.oauth2.service_account import Credentials
import anthropic

# ── Config ───────────────────────────────────────────────────────────────────
GOOGLE_SHEET_ID = "1LzkU7rH3OtrJckV5oMvFHyuLAnbRn9E74FO1uyfM65k"
INPUT_SHEET = "image_pull_new"
OUTPUT_SHEET = "parsed_picks_new"
OFFSET_FILE = "backfill_stage1_offset.txt"

NBA_SCHEDULE_SHEET = "nba_schedule"
CBB_SCHEDULE_SHEET = "cbb_schedule"
NHL_SCHEDULE_SHEET = "nhl_schedule"

# Max rows per Sonnet call — keep small to prevent cross-row hallucination
STAGE_BATCH_SIZE = 10

OUTPUT_COLUMNS = ["date", "capper", "sport", "pick", "line", "game", "spread", "result", "ocr_text"]

# Claude usage tracking (Sonnet 4.6: $3.00/M input, $15.00/M output)
CLAUDE_USAGE = {"input_tokens": 0, "output_tokens": 0}
SONNET_INPUT_COST_PER_M = 3.00
SONNET_OUTPUT_COST_PER_M = 15.00


# ── Google Sheets Auth ────────────────────────────────────────────────────────
def get_gspread_client():
    creds_b64 = os.environ.get("GOOGLE_CREDENTIALS", "")
    if not creds_b64:
        raise ValueError("GOOGLE_CREDENTIALS environment variable not set")
    creds_dict = json.loads(base64.b64decode(creds_b64).decode())
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
    return gspread.authorize(creds)


# ── Schedule helpers ──────────────────────────────────────────────────────────
def get_schedule_for_date(spreadsheet, sheet_name: str, date: str) -> List[Dict]:
    try:
        ws = spreadsheet.worksheet(sheet_name)
    except gspread.WorksheetNotFound:
        return []

    all_values = ws.get_all_values()
    if not all_values:
        return []

    headers = all_values[0]
    try:
        date_idx = headers.index("game_date")
        away_idx = headers.index("away_team")
        home_idx = headers.index("home_team")
    except ValueError:
        return []

    games = []
    for row in all_values[1:]:
        if len(row) <= max(date_idx, away_idx, home_idx):
            continue
        if row[date_idx] == date:
            games.append({"away_team": row[away_idx], "home_team": row[home_idx]})
    return games


def format_schedule_for_prompt(games: List[Dict], sport: str) -> str:
    if not games:
        return f"No {sport} games scheduled"
    return "\n".join(f"{g['away_team']} @ {g['home_team']}" for g in games)


# ── Claude Sonnet call ────────────────────────────────────────────────────────
def call_sonnet(prompt: str, max_tokens: int = 8192) -> str:
    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not api_key:
        raise ValueError("ANTHROPIC_API_KEY environment variable not set")

    client = anthropic.Anthropic(api_key=api_key)
    message = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=max_tokens,
        messages=[{"role": "user", "content": prompt}],
    )
    CLAUDE_USAGE["input_tokens"] += message.usage.input_tokens
    CLAUDE_USAGE["output_tokens"] += message.usage.output_tokens
    return message.content[0].text


# ── Build Stage 1 prompt ──────────────────────────────────────────────────────
# Known limitation: Claude still misses "TeamName MI" picks when MI could be
# interpreted as a team abbreviation (e.g. Michigan). Example that is NOT parsed:
#   SBK, 2026-03-07: "[CBB] New Mexico St MI (-140)" → expected: New Mexico St ML, actual: skipped
# The model treats "MI" as a second team (Michigan) rather than a bet type (ML misread).
# There are likely a small number of similar cases throughout the dataset.
def build_stage1_prompt(
    picks_to_parse: List[Tuple[str, str, str, int]],  # (capper, date, ocr_text, row_id)
    schedule_data: Dict[str, str],
) -> str:
    picks_section = ""
    for capper, date, ocr_text, row_id in picks_to_parse:
        picks_section += f"\n[ROW:{row_id}] [Capper: {capper}, Date: {date}]\n{ocr_text}\n"

    return f"""You are parsing betting picks from OCR text extracted from images.

INCLUDE ONLY:
- Sports: NBA, CBB (college basketball — also called NCAA, NCAAM, NCAAB, college hoops), NHL
- Bet types: moneyline (ML) and point spreads only
- EXCLUDE everything else: UFC, MMA, NFL, MLB, Tennis, Soccer, NASCAR, and any other sport
- EXCLUDE props, game totals (O/U), halftime/quarter/first-half lines, parlays, teasers, live bets

OUTPUT FORMAT: CSV with columns: row_id,date,capper,sport,pick,line
- One row per pick. No header row. No markdown.
- Use the EXACT [ROW:N] value from the input block — NEVER sequential numbers.

COLUMN DEFINITIONS:
- row_id: The [ROW:N] number from the block (critical — used for dedup)
- date: YYYY-MM-DD (from the date in the block header)
- capper: Exact capper name from the block header
- sport: NBA / CBB / NHL — normalize NCAAB/NCAAM/NCAA/college hoops to CBB
- pick: A single FULL team name (the team being bet on — NEVER "Team A @ Team B")
- line: ML for moneyline | spread like -4 or +3.5 (strip odds, units, opponent refs)

PICK PARSING RULES:
- A number like "-4" or "+3.5" after a team name is a point spread — include it
- "ML", "MI", "moneyline", "money line" means the team to win outright — include as line=ML ("MI" is a common OCR misread of "ML")
- "Team MI" or "Team ML" on a single line = one pick (the team to win). "MI"/"ML" here is the bet type, NOT a second team abbreviation. e.g. "New Mexico St MI (-140)" = pick: New Mexico St, line: ML
- NHL "3-way", "3way", "3-way ML", "3way moneyline", "3 way ml" = regulation win bet — include as line=ML
- Ignore odds in parentheses like (-125) and unit sizes like (1.5U)
- "v ", "vs", "v." followed by a team = context to ignore, not part of the line
- "-8 O Texas A&M" style: the "O" means "over" (opponent), NOT a total — extract ONLY the spread: line="-8"
- "Team A over Team B" with NO number = ML pick for Team A, line = ML
- If a line has "1H", "2H", "HH", or "half" anywhere — SKIP it (halftime bet)
- O or U followed by a number (with or without space): O 220.5, O220.5, U5.5, U 150 — SKIP (total)
- Player prop lines (points, assists, rebounds, saves, etc.) — SKIP
- Parlay/teaser/SGP/combo/multi-leg picks — SKIP entirely. Common patterns:
  - "Parlay", "Teaser", "SGP", "Same Game Parlay", "MLP"
  - "Whale Parlay", "Golden Whale", "Whale" bundles listing multiple teams
  - Lists labeled with numbers or bullets that group multiple teams as one bet
  - "Leg 1/Leg 2" style notation
  - Two teams combined with "/" on the same line WITH a spread/ML: "Towson ML / William & Mary ML" or "Wake Forest MI / Purdue MI" — SKIP BOTH (parlay)
  - Two teams combined with "+" on the same line: "Ohio St + UCLA ML" — SKIP
  - Two teams combined with "x" on the same line: "Indiana x Kentucky ML" — SKIP BOTH
  - NOTE: "TeamA/TeamB O 149" is a TOTAL (not a parlay) — skip as total
  - Multiple bets on SEPARATE lines are standalone bets — extract each one separately

ABBREVIATION RESOLUTION:
- Resolve abbreviations to full team names using the schedule below
- Common: BKN=Brooklyn Nets, CHI=Chicago Bulls, NO/NOP=New Orleans Pelicans, MEM=Memphis Grizzlies,
  IND=Indiana Pacers, CBJ=Columbus Blue Jackets, EDM=Edmonton Oilers, OKC=Oklahoma City Thunder,
  PHI=Philadelphia 76ers or Flyers (use schedule to determine which)

NEVER INVERT PICKS (CRITICAL):
- The pick MUST be the EXACT team named in the text — NEVER the opponent
- If text says "Virginia +8", pick = Virginia, line = "+8". Do NOT pick Duke.
- Keep line sign EXACTLY as written (+8 stays +8, -7 stays -7)
- Do NOT interpret "Fade" or "Against" labels to mean bet the opponent
- ALWAYS record the team that is explicitly named

ROW ATTRIBUTION (CRITICAL):
- Each OCR block is tagged [ROW:N] — ONLY parse picks from that block
- NEVER copy picks from other rows or invent picks not in the OCR text
- If an OCR block has no qualifying NBA/CBB/NHL spread or ML picks, output nothing for that row
- Do NOT hallucinate picks from the schedule — schedule is only for expanding abbreviations of teams EXPLICITLY named in the OCR
- NEVER use the schedule to infer opponents, game context, or add teams that are not literally written in the OCR text
- Example of what NOT to do: OCR says "Jazz -125" → do NOT also output Utah Jazz's opponent from the schedule
- Every single pick output must correspond to a team/player token that exists word-for-word (or as a clear abbreviation) in the OCR block

TODAY'S SCHEDULE:
NBA:
{schedule_data.get("nba", "No NBA games scheduled")}

CBB:
{schedule_data.get("cbb", "No CBB games scheduled")}

NHL:
{schedule_data.get("nhl", "No NHL games scheduled")}

--- OCR TEXT BLOCKS ---
{picks_section}
--- END ---

Respond with ONLY CSV rows (no headers, no markdown):"""


# ── Parse CSV response ────────────────────────────────────────────────────────
def parse_stage1_response(
    response: str,
    row_id_to_input: Dict[int, Tuple[str, str, str]],  # row_id -> (capper, date, ocr_text)
) -> List[List[str]]:
    result = []
    for line in response.strip().splitlines():
        line = line.strip()
        if not line or line.lower().startswith("row_id"):
            continue

        # Simple CSV parse handling quoted commas
        parts = []
        current = ""
        in_quotes = False
        for ch in line:
            if ch == '"':
                in_quotes = not in_quotes
            elif ch == "," and not in_quotes:
                parts.append(current.strip())
                current = ""
            else:
                current += ch
        parts.append(current.strip())

        if len(parts) < 5:
            continue

        # Detect format: row_id,date,... OR date,capper,...
        if parts[0] and len(parts[0]) == 10 and parts[0][4] == "-":
            row_id_str = ""
            date = parts[0]
            capper = parts[1] if len(parts) > 1 else ""
            sport = parts[2] if len(parts) > 2 else ""
            pick = parts[3] if len(parts) > 3 else ""
            line_val = parts[4] if len(parts) > 4 else ""
        else:
            row_id_str = parts[0]
            date = parts[1] if len(parts) > 1 else ""
            capper = parts[2] if len(parts) > 2 else ""
            sport = parts[3] if len(parts) > 3 else ""
            pick = parts[4] if len(parts) > 4 else ""
            line_val = parts[5] if len(parts) > 5 else ""

        ocr_text = ""
        if row_id_str:
            try:
                row_id_int = int(row_id_str)
                if row_id_int in row_id_to_input:
                    ocr_text = row_id_to_input[row_id_int][2]
            except ValueError:
                pass

        result.append([
            date,
            capper,
            sport.upper(),
            pick,
            line_val,
            "",      # game (blank — Stage 2 fills)
            "",      # spread
            "",      # result
            ocr_text,
        ])

    return result


# ── ML vs Spread dedup (backfill version) ────────────────────────────────────
def deduplicate_ml_vs_spread_backfill(rows: List[List[str]]) -> List[List[str]]:
    """Drop ML picks when a spread pick exists for the same capper+date+pick.

    At Stage 1 the 'game' column is blank, so we group by (date, capper, pick)
    instead of (date, capper, game).

    Columns: date(0), capper(1), sport(2), pick(3), line(4), ...
    """
    def is_spread(line: str) -> bool:
        l = line.strip().upper()
        if not l or l == "ML":
            return False
        if l.startswith("O ") or l.startswith("U "):
            return False
        return bool(re.match(r"^[+-]?\d", l))

    def row_key(row: List[str]) -> tuple:
        date = row[0].strip() if len(row) > 0 else ""
        capper = row[1].strip().upper() if len(row) > 1 else ""
        pick = row[3].strip().upper() if len(row) > 3 else ""
        return (date, capper, pick)

    # Find all keys that have a spread
    spread_keys: set = set()
    for row in rows:
        if len(row) > 4 and is_spread(row[4]):
            spread_keys.add(row_key(row))

    filtered = []
    for row in rows:
        line = row[4].strip().upper() if len(row) > 4 else ""
        if line == "ML" and row_key(row) in spread_keys:
            print(f"    [dedup] Dropping ML for {row[1]} - {row[3]} on {row[0]} (spread exists)")
            continue
        filtered.append(row)

    return filtered


# ── Dedup image_pull_new ──────────────────────────────────────────────────────
def dedup_image_pull_new(ws) -> Tuple[List[Dict], int]:
    """Dedup image_pull_new in-place by (pick_date, capper_name, ocr_text)."""
    all_values = ws.get_all_values()
    if len(all_values) < 4:
        print("  No data rows to dedup")
        return [], 0

    headers = all_values[2]  # Row 3 = headers
    data_rows = all_values[3:]  # Row 4+ = data

    col = {h: i for i, h in enumerate(headers)}
    msg_date_col = col.get("message_sent_at", 1)
    capper_col = col.get("capper_name", 2)
    ocr_col = col.get("ocr_text", 4)

    print(f"  Headers: {headers}")
    print(f"  Data rows before dedup: {len(data_rows)}")

    seen = set()
    rows_to_delete = []

    for i, row in enumerate(data_rows):
        msg_date = row[msg_date_col] if len(row) > msg_date_col else ""
        capper = row[capper_col] if len(row) > capper_col else ""
        ocr = row[ocr_col] if len(row) > ocr_col else ""

        if not msg_date and not capper and not ocr:
            continue

        # Normalize date to YYYY-MM-DD for dedup key
        pick_date = msg_date[:10] if msg_date else ""
        key = (pick_date, capper.strip().upper(), ocr.strip())
        if key in seen:
            rows_to_delete.append(4 + i)  # 1-based sheet row
        else:
            seen.add(key)

    if rows_to_delete:
        print(f"  Deleting {len(rows_to_delete)} duplicate rows...")
        for sheet_row in sorted(rows_to_delete, reverse=True):
            ws.delete_rows(sheet_row)
            time.sleep(0.3)
        print(f"  Done.")
    else:
        print("  No duplicates found.")

    # Reload after deletions
    all_values = ws.get_all_values()
    headers = all_values[2] if len(all_values) > 2 else []
    data_rows = all_values[3:] if len(all_values) > 3 else []
    col = {h: i for i, h in enumerate(headers)}
    msg_date_col = col.get("message_sent_at", 1)
    capper_col = col.get("capper_name", 2)
    ocr_col = col.get("ocr_text", 4)

    unique_rows = []
    for row in data_rows:
        if not any(row):
            continue
        msg_date = row[msg_date_col] if len(row) > msg_date_col else ""
        unique_rows.append({
            "date": msg_date[:10] if msg_date else "",  # YYYY-MM-DD
            "capper_name": row[capper_col] if len(row) > capper_col else "",
            "ocr_text": row[ocr_col] if len(row) > ocr_col else "",
        })

    return unique_rows, len(rows_to_delete)


# ── Sheet write rate limiter + retry on 429 ──────────────────────────────────
# Google Sheets allows 60 write requests/min per user. We cap at 50 to stay safe.
_write_times: list = []  # timestamps of recent ws.update() calls
WRITE_LIMIT_PER_MIN = 50


def sheets_update_with_retry(ws, cell_range, values, retries=5):
    """Call ws.update() respecting a 50-writes/min rate limit, with exponential
    backoff on any 429 that still slips through."""
    # Rate limit: if we've made WRITE_LIMIT_PER_MIN writes in the last 60s, wait
    now = time.monotonic()
    cutoff = now - 60.0
    # Drop entries older than 60s
    while _write_times and _write_times[0] < cutoff:
        _write_times.pop(0)
    if len(_write_times) >= WRITE_LIMIT_PER_MIN:
        wait = 60.0 - (now - _write_times[0]) + 1.0
        if wait > 0:
            print(f"    [rate limit] {len(_write_times)} writes in last 60s — waiting {wait:.1f}s...")
            time.sleep(wait)
        # Re-prune after sleep
        now2 = time.monotonic()
        cutoff2 = now2 - 60.0
        while _write_times and _write_times[0] < cutoff2:
            _write_times.pop(0)

    delay = 30
    for attempt in range(retries):
        try:
            ws.update(cell_range, values, value_input_option="USER_ENTERED")
            _write_times.append(time.monotonic())
            return
        except gspread.exceptions.APIError as e:
            if "429" in str(e) and attempt < retries - 1:
                print(f"    [429] Rate limited — waiting {delay}s before retry {attempt + 2}/{retries}...")
                time.sleep(delay)
                delay = min(delay * 2, 120)
            else:
                raise



def ensure_output_headers(ws):
    all_values = ws.get_all_values()
    if len(all_values) < 3 or all_values[2] != OUTPUT_COLUMNS:
        print(f"  Writing headers to {OUTPUT_SHEET} row 3...")
        sheets_update_with_retry(ws, "A3", [OUTPUT_COLUMNS])
        time.sleep(1)


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--reset", action="store_true", help="Reset batch offset to 0 (keep sheet data)")
    parser.add_argument("--clear", action="store_true", help="Clear output sheet + reset offset to 0")
    parser.add_argument("--max-batches", type=int, default=0, help="Stop after N batches (0 = no limit)")
    args = parser.parse_args()

    if args.reset and not args.clear:
        if os.path.exists(OFFSET_FILE):
            os.remove(OFFSET_FILE)
        print("Offset reset to 0.")
        return

    print("=" * 60)
    print("Backfill Stage 1: image_pull_new -> parsed_picks_new")
    print("=" * 60)

    print("\nConnecting to Google Sheets...")
    client = get_gspread_client()
    ss = client.open_by_key(GOOGLE_SHEET_ID)

    # --clear: wipe parsed_picks_new and reset offset
    if args.clear:
        print(f"\n── Clear {OUTPUT_SHEET} ──")
        try:
            output_ws = ss.worksheet(OUTPUT_SHEET)
            output_ws.clear()
            sheets_update_with_retry(output_ws, "A3", [OUTPUT_COLUMNS])
            print(f"  {OUTPUT_SHEET} cleared and headers reset.")
        except gspread.WorksheetNotFound:
            print(f"  {OUTPUT_SHEET} not found — nothing to clear.")
        if os.path.exists(OFFSET_FILE):
            os.remove(OFFSET_FILE)
        print("  Offset reset to 0.")
        print("  Re-run without --clear to start parsing.")
        return

    # Step 1: Dedup
    print(f"\n── Step 1: Dedup {INPUT_SHEET} ──")
    input_ws = ss.worksheet(INPUT_SHEET)
    unique_rows, deleted = dedup_image_pull_new(input_ws)
    print(f"  Unique rows remaining: {len(unique_rows)}")

    rows_with_ocr = [r for r in unique_rows if r.get("ocr_text", "").strip()]
    print(f"  Rows with OCR text: {len(rows_with_ocr)}")

    if not rows_with_ocr:
        print("Nothing to parse. Exiting.")
        return

    # Step 2: Prepare output sheet
    print(f"\n── Step 2: Prepare {OUTPUT_SHEET} ──")
    try:
        output_ws = ss.worksheet(OUTPUT_SHEET)
        print(f"  Found existing {OUTPUT_SHEET}")
    except gspread.WorksheetNotFound:
        output_ws = ss.add_worksheet(title=OUTPUT_SHEET, rows=5000, cols=len(OUTPUT_COLUMNS))
        print(f"  Created {OUTPUT_SHEET}")
    ensure_output_headers(output_ws)

    existing = output_ws.get_all_values()
    print(f"  Existing data rows in output: {max(0, len(existing) - 3)}")

    # Step 3: Parse in batches
    print(f"\n── Step 3: Parse {len(rows_with_ocr)} rows with Sonnet ──")
    total_batches = (len(rows_with_ocr) + STAGE_BATCH_SIZE - 1) // STAGE_BATCH_SIZE
    print(f"  Batch size: {STAGE_BATCH_SIZE}, total batches: {total_batches}")

    # Cache all schedule data upfront (one read per sheet)
    print("  Pre-loading schedule sheets...")
    def load_all_schedule(spreadsheet, sheet_name):
        try:
            ws = spreadsheet.worksheet(sheet_name)
            all_values = ws.get_all_values()
        except gspread.WorksheetNotFound:
            return {}
        if not all_values:
            return {}
        headers = all_values[0]
        try:
            date_idx = headers.index("game_date")
            away_idx = headers.index("away_team")
            home_idx = headers.index("home_team")
        except ValueError:
            return {}
        by_date = {}
        for row in all_values[1:]:
            if len(row) <= max(date_idx, away_idx, home_idx):
                continue
            d = row[date_idx]
            if d not in by_date:
                by_date[d] = []
            by_date[d].append({"away_team": row[away_idx], "home_team": row[home_idx]})
        return by_date

    nba_by_date = load_all_schedule(ss, NBA_SCHEDULE_SHEET)
    time.sleep(1)
    cbb_by_date = load_all_schedule(ss, CBB_SCHEDULE_SHEET)
    time.sleep(1)
    nhl_by_date = load_all_schedule(ss, NHL_SCHEDULE_SHEET)
    print(f"  Schedule loaded: {len(nba_by_date)} NBA dates, {len(cbb_by_date)} CBB dates, {len(nhl_by_date)} NHL dates")

    # Resume from saved offset
    start_batch = 0
    if os.path.exists(OFFSET_FILE):
        try:
            start_batch = int(open(OFFSET_FILE).read().strip())
            print(f"  Resuming from batch {start_batch + 1}")
        except ValueError:
            pass

    total_written = 0

    # Track next output row in memory to avoid get_all_values() on every batch
    existing_out = output_ws.get_all_values()
    next_output_row = max(4, len(existing_out) + 1)

    for batch_num in range(start_batch, total_batches):
        batch_start = batch_num * STAGE_BATCH_SIZE
        batch = rows_with_ocr[batch_start: batch_start + STAGE_BATCH_SIZE]

        print(f"\n  Batch {batch_num + 1}/{total_batches} ({len(batch)} rows)...")

        pick_dates = sorted({r["date"] for r in batch if r.get("date")})

        all_nba = [g for d in pick_dates for g in nba_by_date.get(d, [])]
        all_cbb = [g for d in pick_dates for g in cbb_by_date.get(d, [])]
        all_nhl = [g for d in pick_dates for g in nhl_by_date.get(d, [])]

        schedule_data = {
            "nba": format_schedule_for_prompt(all_nba, "NBA"),
            "cbb": format_schedule_for_prompt(all_cbb, "CBB"),
            "nhl": format_schedule_for_prompt(all_nhl, "NHL"),
        }

        picks_to_parse = []
        row_id_to_input = {}
        for i, row in enumerate(batch):
            row_id = batch_start + i  # unique across all batches
            capper = row.get("capper_name", "")
            date = row.get("date", "")
            ocr_text = row.get("ocr_text", "")
            picks_to_parse.append((capper, date, ocr_text, row_id))
            row_id_to_input[row_id] = (capper, date, ocr_text)

        prompt = build_stage1_prompt(picks_to_parse, schedule_data)

        try:
            response = call_sonnet(prompt)
            parsed_batch = parse_stage1_response(response, row_id_to_input)
            print(f"    -> {len(parsed_batch)} picks parsed")
        except Exception as e:
            print(f"    ERROR in batch {batch_num + 1}: {e}")
            continue

        # Dedup ML vs spread within this batch
        if parsed_batch:
            before = len(parsed_batch)
            parsed_batch = deduplicate_ml_vs_spread_backfill(parsed_batch)
            if len(parsed_batch) < before:
                print(f"    -> {len(parsed_batch)} after ML/spread dedup (dropped {before - len(parsed_batch)})")

        # Exact duplicate dedup: drop identical (date, capper, pick, line) within this batch
        if parsed_batch:
            seen_keys: set = set()
            deduped = []
            for row in parsed_batch:
                key = (row[0], row[1].upper(), row[3].upper(), row[4].upper())
                if key in seen_keys:
                    print(f"    [dedup] Dropping exact duplicate: {row[1]} - {row[3]} {row[4]} on {row[0]}")
                    continue
                seen_keys.add(key)
                deduped.append(row)
            if len(deduped) < len(parsed_batch):
                print(f"    -> {len(deduped)} after exact dedup (dropped {len(parsed_batch) - len(deduped)})")
            parsed_batch = deduped

        # Write incrementally after each batch, expanding sheet if needed
        if parsed_batch:
            end_row = next_output_row + len(parsed_batch) - 1
            if end_row > output_ws.row_count:
                output_ws.add_rows(max(500, end_row - output_ws.row_count + 100))
                time.sleep(2)
            cell_range = f"A{next_output_row}:J{end_row}"
            sheets_update_with_retry(output_ws, cell_range, parsed_batch)
            total_written += len(parsed_batch)
            next_output_row = end_row + 1

        # Save progress after each successful batch
        with open(OFFSET_FILE, "w") as f:
            f.write(str(batch_num + 1))

        # Stop early if --max-batches set
        if args.max_batches and (batch_num - start_batch + 1) >= args.max_batches:
            print(f"\n  Stopping after {args.max_batches} batches (--max-batches).")
            break

        time.sleep(3)

    # Clear offset file when done
    if os.path.exists(OFFSET_FILE):
        os.remove(OFFSET_FILE)

    # Summary
    input_cost = CLAUDE_USAGE["input_tokens"] / 1_000_000 * SONNET_INPUT_COST_PER_M
    output_cost = CLAUDE_USAGE["output_tokens"] / 1_000_000 * SONNET_OUTPUT_COST_PER_M
    total_cost = input_cost + output_cost

    print(f"\n{'=' * 60}")
    print("SUMMARY")
    print(f"{'=' * 60}")
    print(f"  Duplicates removed from {INPUT_SHEET}: {deleted}")
    print(f"  OCR rows processed: {len(rows_with_ocr)}")
    print(f"  Picks written to {OUTPUT_SHEET}: {total_written}")
    print(f"  Sonnet tokens  input: {CLAUDE_USAGE['input_tokens']:,}  output: {CLAUDE_USAGE['output_tokens']:,}")
    print(f"  Estimated cost: ${total_cost:.4f}")


if __name__ == "__main__":
    main()
