#!/usr/bin/env python3
"""
backfill_ocr_text.py — One-time fix to populate the ocr_text column in
parsed_picks_new starting at row 11688 using data from image_pull.

Match key: date (from message_sent_at) + normalized capper name.
One image can produce multiple picks, so all picks for a given capper+date
get the same ocr_text from image_pull.

Usage:
    .venv/bin/python3 backfill_ocr_text.py --dry-run   # preview
    .venv/bin/python3 backfill_ocr_text.py              # write
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

GOOGLE_SHEET_ID     = "1LzkU7rH3OtrJckV5oMvFHyuLAnbRn9E74FO1uyfM65k"
PICKS_NEW_SHEET     = "parsed_picks_new"
IMAGE_PULL_SHEET    = "image_pull"

# parsed_picks_new: row 3 = header, data from row 4
# Start backfilling from sheet row 11688
BACKFILL_FROM_SHEET_ROW = 11688

# Column indices in parsed_picks_new (0-based)
PN_DATE   = 0
PN_CAPPER = 1
PN_OCR    = 9  # ocr_text column


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


def normalize(name: str) -> str:
    return name.strip().upper()


def build_ocr_lookup(image_pull_ws) -> dict:
    """
    Build a lookup: {(date_str, normalized_capper): ocr_text}
    from image_pull.

    image_pull layout:
      row 1 = last-run timestamp
      row 2 = header: timestamp, message_sent_at, capper_name, image_url, ocr_text, committed_stage
      row 3+ = data
    """
    print("Loading image_pull sheet...")
    rows = image_pull_ws.get_all_values()

    if len(rows) < 3:
        return {}

    header = rows[1]
    hcol = {h: i for i, h in enumerate(header)}
    sent_idx   = hcol.get("message_sent_at", 1)
    capper_idx = hcol.get("capper_name",     2)
    ocr_idx    = hcol.get("ocr_text",        4)

    lookup = {}
    for row in rows[2:]:
        while len(row) <= max(sent_idx, capper_idx, ocr_idx):
            row.append("")
        sent_at = row[sent_idx].strip()
        capper  = row[capper_idx].strip()
        ocr     = row[ocr_idx].strip()

        if not sent_at or not capper or not ocr:
            continue

        # Extract date portion (YYYY-MM-DD)
        date_str = sent_at[:10]
        key = (date_str, normalize(capper))

        # Multiple images for same capper+date: concatenate
        if key in lookup:
            lookup[key] = lookup[key] + "\n---\n" + ocr
        else:
            lookup[key] = ocr

    print(f"  Built OCR lookup with {len(lookup)} capper+date entries")
    return lookup


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true", help="Preview without writing")
    args = parser.parse_args()

    print("Connecting to Google Sheets...")
    gc = get_gspread_client()
    ss = gc.open_by_key(GOOGLE_SHEET_ID)

    image_pull_ws = ss.worksheet(IMAGE_PULL_SHEET)
    picks_new_ws  = ss.worksheet(PICKS_NEW_SHEET)

    ocr_lookup = build_ocr_lookup(image_pull_ws)

    print(f"\nLoading {PICKS_NEW_SHEET}...")
    all_values = picks_new_ws.get_all_values()
    print(f"  Total rows in sheet: {len(all_values)}")

    # Convert 1-based sheet row to 0-based index
    start_idx = BACKFILL_FROM_SHEET_ROW - 1

    if start_idx >= len(all_values):
        print(f"  Sheet only has {len(all_values)} rows, nothing to backfill from row {BACKFILL_FROM_SHEET_ROW}")
        return

    data_slice = all_values[start_idx:]
    print(f"  Rows to check (from sheet row {BACKFILL_FROM_SHEET_ROW}): {len(data_slice)}")

    batch_updates    = []
    filled           = 0
    skipped_already  = 0
    skipped_no_match = 0

    for offset, row in enumerate(data_slice):
        sheet_row = BACKFILL_FROM_SHEET_ROW + offset

        while len(row) <= PN_OCR:
            row.append("")

        if row[PN_OCR].strip():
            skipped_already += 1
            continue

        date_str = row[PN_DATE].strip()   if len(row) > PN_DATE   else ""
        capper   = row[PN_CAPPER].strip() if len(row) > PN_CAPPER else ""

        if not date_str or not capper:
            skipped_no_match += 1
            continue

        ocr = ocr_lookup.get((date_str, normalize(capper)), "")

        if not ocr:
            skipped_no_match += 1
            continue

        cell = gspread.utils.rowcol_to_a1(sheet_row, PN_OCR + 1)  # column J

        if args.dry_run:
            print(f"  [{sheet_row}] {date_str} | {capper} -> {ocr[:80]}")
        else:
            batch_updates.append({"range": cell, "values": [[ocr]]})

        filled += 1

    print(f"\nSummary:")
    print(f"  Would fill / filled: {filled}")
    print(f"  Already had ocr_text: {skipped_already}")
    print(f"  No match in image_pull: {skipped_no_match}")

    if not args.dry_run and batch_updates:
        print(f"\nWriting {len(batch_updates)} updates in chunks...")
        chunk_size = 500
        for i in range(0, len(batch_updates), chunk_size):
            picks_new_ws.batch_update(batch_updates[i:i + chunk_size])
            print(f"  Wrote chunk {i // chunk_size + 1} ({len(batch_updates[i:i+chunk_size])} cells)")
            if i + chunk_size < len(batch_updates):
                time.sleep(1)
        print("Done.")
    elif args.dry_run:
        print("\n[dry-run] No changes written.")
    else:
        print("\nNothing to write.")


if __name__ == "__main__":
    main()
