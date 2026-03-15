#!/usr/bin/env python3
"""
Dump image_pull_new rows in batches for in-conversation parsing.

Usage:
  .venv/bin/python3 dump_backfill_batch.py            # dump next 20 unparsed rows
  .venv/bin/python3 dump_backfill_batch.py --batch 3  # dump a specific batch number
  .venv/bin/python3 dump_backfill_batch.py --dedup    # run dedup first, then dump batch 1

Reads BATCH_OFFSET from offset.txt to track progress across runs.
"""

import os
import sys
import json
import base64
import time
import argparse
from typing import List, Dict

from dotenv import load_dotenv
load_dotenv()

import gspread
from google.oauth2.service_account import Credentials

GOOGLE_SHEET_ID = "1LzkU7rH3OtrJckV5oMvFHyuLAnbRn9E74FO1uyfM65k"
INPUT_SHEET = "image_pull_new"
BATCH_SIZE = 20
OFFSET_FILE = "backfill_offset.txt"

NBA_SCHEDULE_SHEET = "nba_schedule"
CBB_SCHEDULE_SHEET = "cbb_schedule"
NHL_SCHEDULE_SHEET = "nhl_schedule"


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


def dedup_sheet(ws) -> int:
    """Dedup image_pull_new by (date, capper_name, ocr_text). Returns num deleted."""
    all_values = ws.get_all_values()
    if len(all_values) < 4:
        print("No data rows to dedup")
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
        print(f"Deleting {len(rows_to_delete)} duplicates...")
        for sheet_row in sorted(rows_to_delete, reverse=True):
            ws.delete_rows(sheet_row)
            time.sleep(0.3)
        print("Dedup done.")
    else:
        print("No duplicates found.")
    return len(rows_to_delete)


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


def read_offset() -> int:
    if os.path.exists(OFFSET_FILE):
        try:
            return int(open(OFFSET_FILE).read().strip())
        except ValueError:
            pass
    return 0


def write_offset(offset: int):
    with open(OFFSET_FILE, "w") as f:
        f.write(str(offset))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dedup", action="store_true", help="Run dedup before dumping")
    parser.add_argument("--batch", type=int, default=None, help="Dump a specific batch number (1-based)")
    parser.add_argument("--reset", action="store_true", help="Reset offset to 0")
    args = parser.parse_args()

    client = get_gspread_client()
    ss = client.open_by_key(GOOGLE_SHEET_ID)
    ws = ss.worksheet(INPUT_SHEET)

    if args.reset:
        write_offset(0)
        print("Offset reset to 0.")
        return

    if args.dedup:
        dedup_sheet(ws)

    rows = load_rows(ws)
    total = len(rows)

    if args.batch is not None:
        offset = (args.batch - 1) * BATCH_SIZE
    else:
        offset = read_offset()

    batch_num = offset // BATCH_SIZE + 1
    batch = rows[offset: offset + BATCH_SIZE]

    if not batch:
        print(f"No more rows. Total processed: {offset}/{total}")
        return

    # Fetch schedule for dates in this batch
    pick_dates = sorted({r["date"] for r in batch if r["date"]})
    all_nba, all_cbb, all_nhl = [], [], []
    for d in pick_dates:
        all_nba.extend(get_schedule_for_date(ss, NBA_SCHEDULE_SHEET, d))
        all_cbb.extend(get_schedule_for_date(ss, CBB_SCHEDULE_SHEET, d))
        all_nhl.extend(get_schedule_for_date(ss, NHL_SCHEDULE_SHEET, d))

    nba_str = "\n".join(f"{g['away_team']} @ {g['home_team']}" for g in all_nba) or "No NBA games"
    cbb_str = "\n".join(f"{g['away_team']} @ {g['home_team']}" for g in all_cbb) or "No CBB games"
    nhl_str = "\n".join(f"{g['away_team']} @ {g['home_team']}" for g in all_nhl) or "No NHL games"

    # Print the block to paste to Claude
    print(f"\n{'='*60}")
    print(f"BATCH {batch_num}  (rows {offset+1}–{offset+len(batch)} of {total})")
    print(f"{'='*60}")
    print(f"\nSCHEDULE (dates: {', '.join(pick_dates)}):")
    print(f"NBA:\n{nba_str}\n")
    print(f"CBB:\n{cbb_str}\n")
    print(f"NHL:\n{nhl_str}\n")
    print("--- OCR TEXT BLOCKS ---")
    for i, row in enumerate(batch):
        row_id = offset + i
        print(f"\n[ROW:{row_id}] [Capper: {row['capper_name']}, Date: {row['date']}]")
        print(row["ocr_text"])
    print("\n--- END ---")
    print(f"\nNext offset will be: {offset + len(batch)}")

    # Advance offset for next run (unless --batch was specified)
    if args.batch is None:
        write_offset(offset + len(batch))
        print(f"Offset saved: {offset + len(batch)}/{total}")


if __name__ == "__main__":
    main()
