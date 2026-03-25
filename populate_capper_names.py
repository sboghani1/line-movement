#!/usr/bin/env python3
"""
populate_capper_names.py — One-time script to populate the capper_name_resolution
sheet from all distinct capper values in master_sheet.

For each unique normalized key, writes:
  unique_capper_name: lowercase a-z0-9 + underscore (spaces → underscores, strip rest)
  aliases: comma-separated list of all raw name variants seen in the data

Usage:
  .venv/bin/python3 populate_capper_names.py --dry-run   # preview without writing
  .venv/bin/python3 populate_capper_names.py              # write to sheet
"""

import re
import argparse
from collections import defaultdict

from dotenv import load_dotenv

from sheets_utils import (
    GOOGLE_SHEET_ID,
    get_gspread_client,
    sheets_read,
    sheets_write,
)

load_dotenv()

RESOLUTION_SHEET = "capper_name_resolution"


def normalize_capper_key(name: str) -> str:
    """Convert a raw capper name to the canonical key format.

    Rules: lowercase, a-z0-9 only, spaces become underscores.
    """
    key = name.lower().strip()
    key = key.replace(" ", "_")
    key = re.sub(r"[^a-z0-9_]", "", key)
    # Collapse multiple underscores
    key = re.sub(r"_+", "_", key).strip("_")
    return key


def main():
    parser = argparse.ArgumentParser(
        description="Populate capper_name_resolution sheet from master_sheet"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview without writing to sheet",
    )
    args = parser.parse_args()

    print("Connecting to Google Sheets...")
    gc = get_gspread_client()
    ss = gc.open_by_key(GOOGLE_SHEET_ID)

    print("Loading master_sheet...")
    ws = sheets_read(ss.worksheet, "master_sheet")
    all_values = sheets_read(ws.get_all_values)

    header = all_values[0]
    col = {h: i for i, h in enumerate(header)}
    capper_col = col.get("capper", 1)

    # Collect all raw capper names → group by normalized key
    by_key = defaultdict(set)
    for row in all_values[1:]:
        if len(row) > capper_col:
            raw = row[capper_col].strip()
            if raw and raw.lower() not in ("capper", "unknown", ""):
                key = normalize_capper_key(raw)
                if key:
                    by_key[key].add(raw)

    print(f"\nFound {len(by_key)} unique cappers from {len(all_values) - 1} data rows\n")

    # Build output rows sorted alphabetically by key
    output_rows = []
    for key in sorted(by_key):
        aliases = sorted(by_key[key])
        output_rows.append([key, ", ".join(aliases)])

    # Print preview
    for row in output_rows:
        print(f"  {row[0]:30s} → {row[1]}")

    if args.dry_run:
        print(f"\n[dry-run] Would write {len(output_rows)} rows. No changes made.")
        return

    # Write to sheet
    print(f"\nWriting to {RESOLUTION_SHEET}...")
    ws_res = sheets_read(ss.worksheet, RESOLUTION_SHEET)

    # Clear existing data, write header + rows
    sheets_write(ws_res.clear)
    all_data = [["unique_capper_name", "aliases"]] + output_rows
    sheets_write(
        ws_res.update,
        range_name=f"A1:B{len(all_data)}",
        values=all_data,
        value_input_option="RAW",
    )

    print(f"Done. Wrote {len(output_rows)} cappers to {RESOLUTION_SHEET}.")


if __name__ == "__main__":
    main()
