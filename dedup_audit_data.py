#!/usr/bin/env python3
"""
dedup_audit_data.py — Remove duplicate rows from audit_data sheet.

For each (date, capper, sport, pick, line) key, keeps only the LAST occurrence
(i.e. the row that appears latest in the sheet). Earlier duplicates are deleted.

Rows are deleted bottom-up so row indices don't shift as we go.

Usage:
  .venv/bin/python3 dedup_audit_data.py --dry-run   # preview
  .venv/bin/python3 dedup_audit_data.py              # write
"""

import os
import json
import base64
import argparse
import time

import gspread
from dotenv import load_dotenv
from google.oauth2.service_account import Credentials

load_dotenv()

GOOGLE_SHEET_ID = "1LzkU7rH3OtrJckV5oMvFHyuLAnbRn9E74FO1uyfM65k"
AUDIT_SHEET     = "audit_data"


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


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true", help="Preview without deleting")
    args = parser.parse_args()

    print("Connecting to Google Sheets...")
    gc = get_gspread_client()
    ss = gc.open_by_key(GOOGLE_SHEET_ID)
    ws = ss.worksheet(AUDIT_SHEET)

    print(f"Loading {AUDIT_SHEET}...")
    all_values = ws.get_all_values()
    if not all_values:
        print("Sheet is empty.")
        return

    header = all_values[0]
    col = {h: i for i, h in enumerate(header)}
    date_col   = col.get("date",   0)
    capper_col = col.get("capper", 1)
    sport_col  = col.get("sport",  2)
    pick_col   = col.get("pick",   3)
    line_col   = col.get("line",   4)

    # Map key -> list of 1-based sheet row numbers (row 1 = header)
    key_to_rows = {}
    for idx, row in enumerate(all_values[1:], start=2):  # 1-based, skip header
        while len(row) <= max(date_col, capper_col, sport_col, pick_col, line_col):
            row.append("")
        key = (
            row[date_col].strip(),
            row[capper_col].strip(),
            row[sport_col].strip(),
            row[pick_col].strip(),
            row[line_col].strip(),
        )
        key_to_rows.setdefault(key, []).append(idx)

    # Collect rows to delete: all but the last occurrence for each key
    rows_to_delete = []
    for key, row_nums in key_to_rows.items():
        if len(row_nums) > 1:
            # Keep the last; delete the earlier ones
            rows_to_delete.extend(row_nums[:-1])
            if args.dry_run:
                print(f"  dup key={key}  rows={row_nums}  keeping={row_nums[-1]}  deleting={row_nums[:-1]}")

    if not rows_to_delete:
        print("No duplicates found.")
        return

    # Sort descending so deleting doesn't shift remaining indices
    rows_to_delete.sort(reverse=True)
    print(f"\n{'Would delete' if args.dry_run else 'Deleting'} {len(rows_to_delete)} duplicate rows (bottom-up)...")

    if args.dry_run:
        print("[dry-run] No changes written.")
        return

    # Delete one at a time (gspread doesn't support batch row deletion)
    for i, row_num in enumerate(rows_to_delete):
        ws.delete_rows(row_num)
        if i % 10 == 9:
            time.sleep(1)  # avoid quota limits

    print(f"Done. Deleted {len(rows_to_delete)} rows.")


if __name__ == "__main__":
    main()
