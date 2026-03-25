#!/usr/bin/env python3
"""
backfill_capper_names.py — One-time capper name normalisation across sheets.

The old Claude-based Stage 2 wrote capper names in raw uppercase format
(e.g. "THIS GIRL BETZ", "NICKY CASHIN").  The new Python Stage 2 normalises
them to canonical unique_capper_name format (e.g. "this_girl_betz",
"nicky_cashin") using CapperResolver.

This script:
  1. Loads the capper name mapping from capper_name_resolution
  2. Scans master_sheet, finalized_picks, and parsed_picks_new
  3. Batch-updates the capper column in each sheet to canonical form
  4. Reports how many rows were updated in each sheet

Usage:
  .venv/bin/python3 backfill_capper_names.py               # preview (dry-run)
  .venv/bin/python3 backfill_capper_names.py --execute      # actually write
"""

import argparse
import time
from typing import Dict, List, Tuple

from dotenv import load_dotenv

from sheets_utils import GOOGLE_SHEET_ID, get_gspread_client, sheets_read, sheets_write
from capper_resolver import CapperResolver

load_dotenv()


# Sheets to update and their header layout
# (sheet_name, header_row_index_0based, capper_col_name)
SHEETS_TO_UPDATE = [
    ("master_sheet",      0, "capper"),
    ("finalized_picks",   2, "capper"),  # header on row 3
    ("parsed_picks_new",  2, "capper"),  # header on row 3
]


def build_mapping(resolver: CapperResolver) -> Dict[str, str]:
    """Build a mapping from every known alias (lowercased) to canonical name.

    Also maps normalized uppercase forms so we catch the old Claude output
    format like "THIS GIRL BETZ" → "this_girl_betz".
    """
    mapping = {}

    # Direct alias→canonical from the resolver
    for alias_lower, canonical in resolver.alias_to_key.items():
        mapping[alias_lower] = canonical

    # Also map the canonical name to itself (no-op, but safe)
    for name in resolver.canonical_names:
        mapping[name] = name

    return mapping


def update_sheet(ss, sheet_name: str, header_row_idx: int, capper_col_name: str,
                 mapping: Dict[str, str], dry_run: bool) -> Tuple[int, int]:
    """Update capper column in a single sheet.

    Returns (total_data_rows, rows_updated).
    """
    print(f"\n{'─' * 60}")
    print(f"Processing: {sheet_name} (header at row {header_row_idx + 1})")

    ws = sheets_read(ss.worksheet, sheet_name)
    all_values = sheets_read(ws.get_all_values)

    if len(all_values) <= header_row_idx + 1:
        print(f"  No data rows found")
        return 0, 0

    header = all_values[header_row_idx]
    try:
        capper_col_idx = header.index(capper_col_name)
    except ValueError:
        print(f"  ERROR: column '{capper_col_name}' not found in header: {header}")
        return 0, 0

    data_start = header_row_idx + 1
    data_rows = all_values[data_start:]
    total = len(data_rows)

    # Find rows that need updating
    updates = []  # list of (sheet_row_1based, old_value, new_value)
    already_canonical = 0
    not_found = 0

    for i, row in enumerate(data_rows):
        if len(row) <= capper_col_idx:
            continue

        current = row[capper_col_idx].strip()
        if not current:
            continue

        # Already canonical?
        if current in mapping and mapping[current] == current:
            already_canonical += 1
            continue

        # Try to resolve via mapping
        current_lower = current.lower()
        if current_lower in mapping:
            canonical = mapping[current_lower]
            if canonical != current:
                sheet_row = data_start + i + 1  # 1-based sheet row
                updates.append((sheet_row, current, canonical))
                continue

        # Not in mapping — might be a new capper or already correct
        # Check if it IS a canonical name
        if current in {mapping[k] for k in mapping}:
            already_canonical += 1
        else:
            not_found += 1

    print(f"  Total data rows:    {total}")
    print(f"  Already canonical:  {already_canonical}")
    print(f"  Need updating:      {len(updates)}")
    print(f"  Not in mapping:     {not_found}")

    if not updates:
        return total, 0

    # Show sample of updates
    print(f"\n  Sample updates (first 10):")
    for sheet_row, old, new in updates[:10]:
        print(f"    Row {sheet_row}: '{old}' → '{new}'")

    if dry_run:
        print(f"\n  [dry-run] Would update {len(updates)} rows")
        return total, len(updates)

    # Batch update using cell range updates to minimize API calls
    # Group into batches of 500 to avoid hitting API limits
    BATCH_SIZE = 500
    updated = 0

    for batch_start in range(0, len(updates), BATCH_SIZE):
        batch = updates[batch_start:batch_start + BATCH_SIZE]

        # Build cell list for batch update
        import gspread
        col_letter = gspread.utils.rowcol_to_a1(1, capper_col_idx + 1).rstrip("1")

        cells_to_update = []
        for sheet_row, old, new in batch:
            cell_ref = f"{col_letter}{sheet_row}"
            cells_to_update.append({
                "range": cell_ref,
                "values": [[new]]
            })

        # Use batch_update for efficiency
        sheets_write(ws.batch_update, cells_to_update)
        updated += len(batch)
        print(f"  Updated {updated}/{len(updates)} rows...")

    print(f"  ✅ Updated {updated} rows in {sheet_name}")
    return total, updated


def main():
    parser = argparse.ArgumentParser(
        description="One-time backfill: normalize capper names to canonical format"
    )
    parser.add_argument(
        "--execute", action="store_true",
        help="Actually write changes (default is dry-run preview)"
    )
    args = parser.parse_args()
    dry_run = not args.execute

    if dry_run:
        print("=" * 60)
        print("DRY RUN — no changes will be written")
        print("Run with --execute to apply changes")
        print("=" * 60)
    else:
        print("=" * 60)
        print("LIVE RUN — changes will be written to sheets")
        print("=" * 60)

    print("\nConnecting to Google Sheets...")
    gc = get_gspread_client()
    ss = gc.open_by_key(GOOGLE_SHEET_ID)

    print("Initializing capper resolver...")
    resolver = CapperResolver(ss)

    print(f"\nBuilding mapping from {len(resolver.canonical_names)} canonical names "
          f"and {len(resolver.alias_to_key)} aliases...")
    mapping = build_mapping(resolver)
    print(f"Total mapping entries: {len(mapping)}")

    # Process each sheet
    grand_total = 0
    grand_updated = 0

    for sheet_name, header_row, col_name in SHEETS_TO_UPDATE:
        try:
            total, updated = update_sheet(ss, sheet_name, header_row, col_name,
                                          mapping, dry_run)
            grand_total += total
            grand_updated += updated
        except Exception as e:
            print(f"\n  ERROR processing {sheet_name}: {e}")

    print(f"\n{'═' * 60}")
    print(f"Summary:")
    print(f"  Total rows scanned: {grand_total}")
    print(f"  Total rows {'would be ' if dry_run else ''}updated: {grand_updated}")
    if dry_run:
        print(f"\nRun with --execute to apply these changes")


if __name__ == "__main__":
    main()
