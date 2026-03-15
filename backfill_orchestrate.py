#!/usr/bin/env python3
"""
Backfill orchestrator: reads image_pull_new in batches of 20,
sends each batch to Claude (this process) for parsing, writes to parsed_picks_new.

This script IS the parser — no Anthropic API key needed.
Claude Code runs this script and parses each batch inline via subprocess stdout/stdin.

Usage:
  .venv/bin/python3 backfill_orchestrate.py --dedup   # first run: dedup then parse all
  .venv/bin/python3 backfill_orchestrate.py            # resume from saved offset
  .venv/bin/python3 backfill_orchestrate.py --reset    # reset offset to 0
"""

import os
import sys
import json
import base64
import time
import argparse
from typing import List, Dict, Tuple

from dotenv import load_dotenv
load_dotenv()

import gspread
from google.oauth2.service_account import Credentials

GOOGLE_SHEET_ID = "1LzkU7rH3OtrJckV5oMvFHyuLAnbRn9E74FO1uyfM65k"
INPUT_SHEET = "image_pull_new"
OUTPUT_SHEET = "parsed_picks_new"
BATCH_SIZE = 20
OFFSET_FILE = "backfill_offset.txt"

NBA_SCHEDULE_SHEET = "nba_schedule"
CBB_SCHEDULE_SHEET = "cbb_schedule"
NHL_SCHEDULE_SHEET = "nhl_schedule"

OUTPUT_COLUMNS = ["date", "capper", "sport", "pick", "line", "game", "spread", "side", "result", "ocr_text"]


# ── Auth ──────────────────────────────────────────────────────────────────────
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


# ── Schedule ──────────────────────────────────────────────────────────────────
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
    return [
        {"away_team": row[away_idx], "home_team": row[home_idx]}
        for row in all_values[1:]
        if len(row) > max(date_idx, away_idx, home_idx) and row[date_idx] == date
    ]


# ── Dedup ─────────────────────────────────────────────────────────────────────
def dedup_sheet(ws) -> int:
    all_values = ws.get_all_values()
    if len(all_values) < 4:
        return 0
    headers = all_values[2]
    data_rows = all_values[3:]
    col = {h: i for i, h in enumerate(headers)}
    msg_date_col = col.get("message_sent_at", 1)
    capper_col = col.get("capper_name", 2)
    ocr_col = col.get("ocr_text", 4)

    seen = set()
    rows_to_delete = []
    for i, row in enumerate(data_rows):
        msg_date = row[msg_date_col] if len(row) > msg_date_col else ""
        capper = row[capper_col] if len(row) > capper_col else ""
        ocr = row[ocr_col] if len(row) > ocr_col else ""
        if not msg_date and not capper and not ocr:
            continue
        key = (msg_date[:10], capper.strip().upper(), ocr.strip())
        if key in seen:
            rows_to_delete.append(4 + i)
        else:
            seen.add(key)

    if rows_to_delete:
        print(f"  Deleting {len(rows_to_delete)} duplicates...")
        for sheet_row in sorted(rows_to_delete, reverse=True):
            ws.delete_rows(sheet_row)
            time.sleep(0.3)
        print(f"  Dedup done.")
    else:
        print("  No duplicates found.")
    return len(rows_to_delete)


# ── Load rows ─────────────────────────────────────────────────────────────────
def load_rows(ws) -> List[Dict]:
    all_values = ws.get_all_values()
    if len(all_values) < 4:
        return []
    headers = all_values[2]
    col = {h: i for i, h in enumerate(headers)}
    msg_date_col = col.get("message_sent_at", 1)
    capper_col = col.get("capper_name", 2)
    ocr_col = col.get("ocr_text", 4)

    rows = []
    for row in all_values[3:]:
        if not any(row):
            continue
        ocr = row[ocr_col] if len(row) > ocr_col else ""
        if not ocr.strip():
            continue
        msg_date = row[msg_date_col] if len(row) > msg_date_col else ""
        rows.append({
            "date": msg_date[:10] if msg_date else "",
            "capper_name": row[capper_col] if len(row) > capper_col else "",
            "ocr_text": ocr,
        })
    return rows


# ── Offset tracking ───────────────────────────────────────────────────────────
def read_offset() -> int:
    if os.path.exists(OFFSET_FILE):
        try:
            return int(open(OFFSET_FILE).read().strip())
        except ValueError:
            pass
    return 0


def write_offset(n: int):
    with open(OFFSET_FILE, "w") as f:
        f.write(str(n))


# ── Build prompt for a batch ──────────────────────────────────────────────────
def build_prompt(batch: List[Dict], offset: int, schedule_data: Dict) -> str:
    picks_section = ""
    for i, row in enumerate(batch):
        row_id = offset + i
        picks_section += f"\n[ROW:{row_id}] [Capper: {row['capper_name']}, Date: {row['date']}]\n{row['ocr_text']}\n"

    return f"""Parse betting picks from OCR text. Output CSV only: row_id,date,capper,sport,pick,line

Rules:
- row_id: use the exact [ROW:N] value shown
- sport: NBA/CBB/NHL/NFL/NCAAF/MLB/Tennis/etc — infer from context
- pick: single team or player name (never "Team A @ Team B")
- line: ML | spread like -4 or +3.5 | total like O 167.5 or U 145.5 | prop as written
- Ignore odds (-125) and unit sizes (1.5U)
- For game totals use "Team1/Team2" format
- NEVER invert picks — record the exact team/player named, not the opponent
- NEVER output picks not present in the OCR text
- If a block has no picks, skip it entirely
- Use schedule only to confirm game context, not to invent picks

SCHEDULE:
NBA: {schedule_data['nba']}
CBB: {schedule_data['cbb']}
NHL: {schedule_data['nhl']}

OCR BLOCKS:
{picks_section}
END

CSV output only (no headers, no markdown):"""


# ── Parse CSV response ────────────────────────────────────────────────────────
def parse_csv(response: str, batch: List[Dict], offset: int) -> List[List[str]]:
    row_id_to_ocr = {offset + i: row["ocr_text"] for i, row in enumerate(batch)}
    result = []
    for line in response.strip().splitlines():
        line = line.strip()
        if not line or line.lower().startswith("row_id"):
            continue
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

        if parts[0] and len(parts[0]) == 10 and parts[0][4] == "-":
            row_id_str, date, capper, sport, pick = "", parts[0], parts[1] if len(parts)>1 else "", parts[2] if len(parts)>2 else "", parts[3] if len(parts)>3 else ""
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
                ocr_text = row_id_to_ocr.get(int(row_id_str), "")
            except ValueError:
                pass

        result.append([date, capper, sport.upper(), pick, line_val, "", "", "", "", ocr_text])
    return result


# ── Write to sheet ────────────────────────────────────────────────────────────
def write_rows(ws, rows: List[List[str]]):
    if not rows:
        return
    existing = ws.get_all_values()
    next_row = max(4, len(existing) + 1)
    cell_range = f"A{next_row}:J{next_row + len(rows) - 1}"
    ws.update(cell_range, rows, value_input_option="USER_ENTERED")


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dedup", action="store_true")
    parser.add_argument("--reset", action="store_true")
    args = parser.parse_args()

    if args.reset:
        write_offset(0)
        print("Offset reset.")
        return

    client = get_gspread_client()
    ss = client.open_by_key(GOOGLE_SHEET_ID)

    if args.dedup:
        print("── Dedup ──")
        input_ws = ss.worksheet(INPUT_SHEET)
        dedup_sheet(input_ws)

    input_ws = ss.worksheet(INPUT_SHEET)
    all_rows = load_rows(input_ws)
    total = len(all_rows)
    print(f"Total rows with OCR: {total}")

    # Output sheet
    try:
        output_ws = ss.worksheet(OUTPUT_SHEET)
    except gspread.WorksheetNotFound:
        output_ws = ss.add_worksheet(title=OUTPUT_SHEET, rows=5000, cols=len(OUTPUT_COLUMNS))
        output_ws.update("A3", [OUTPUT_COLUMNS])
        time.sleep(1)

    offset = read_offset()
    total_written = 0

    while offset < total:
        batch = all_rows[offset: offset + BATCH_SIZE]
        batch_num = offset // BATCH_SIZE + 1
        total_batches = (total + BATCH_SIZE - 1) // BATCH_SIZE
        print(f"\nBatch {batch_num}/{total_batches} (rows {offset+1}–{offset+len(batch)})...", flush=True)

        pick_dates = sorted({r["date"] for r in batch if r["date"]})
        all_nba, all_cbb, all_nhl = [], [], []
        for d in pick_dates:
            all_nba.extend(get_schedule_for_date(ss, NBA_SCHEDULE_SHEET, d))
            all_cbb.extend(get_schedule_for_date(ss, CBB_SCHEDULE_SHEET, d))
            all_nhl.extend(get_schedule_for_date(ss, NHL_SCHEDULE_SHEET, d))

        schedule_data = {
            "nba": "\n".join(f"{g['away_team']} @ {g['home_team']}" for g in all_nba) or "No NBA games",
            "cbb": "\n".join(f"{g['away_team']} @ {g['home_team']}" for g in all_cbb) or "No CBB games",
            "nhl": "\n".join(f"{g['away_team']} @ {g['home_team']}" for g in all_nhl) or "No NHL games",
        }

        prompt = build_prompt(batch, offset, schedule_data)

        # Print prompt to stdout with sentinel so Claude Code can read it
        print("<<<PARSE_REQUEST>>>")
        print(prompt)
        print("<<<END_PARSE_REQUEST>>>", flush=True)

        # Read Claude's response from stdin
        csv_lines = []
        for line in sys.stdin:
            line = line.rstrip("\n")
            if line == "<<<END_PARSE_RESPONSE>>>":
                break
            csv_lines.append(line)

        response = "\n".join(csv_lines)
        parsed = parse_csv(response, batch, offset)
        print(f"  -> {len(parsed)} picks parsed")

        if parsed:
            write_rows(output_ws, parsed)
            total_written += len(parsed)
            time.sleep(0.5)

        offset += len(batch)
        write_offset(offset)

    print(f"\nDone. Total picks written: {total_written}")


if __name__ == "__main__":
    main()
