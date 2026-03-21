"""
Step 0 migration: add 'source' column to master_sheet and parsed_picks_new.

- Idempotent: safe to run multiple times (skips if column already exists)
- Adds 'source' as the last column header
- Backfills all existing data rows with "discord_all_in_one"
- Prints a dry-run summary before writing; prompts for confirmation
"""
import os
import sys
import time

from dotenv import load_dotenv

load_dotenv()

from sheets_utils import GOOGLE_SHEET_ID, get_gspread_client, sheets_call

SHEETS_TO_MIGRATE = ["master_sheet", "parsed_picks_new"]

# In master_sheet: row 1 = headers, row 2+ = data  (header_row_index=0)
# In parsed_picks_new: row 1 = timestamp, row 2 = DO NOT EDIT, row 3 = headers, row 4+ = data
SHEET_CONFIG = {
    "master_sheet":     {"header_row_index": 0},
    "parsed_picks_new": {"header_row_index": 2},
}


def migrate_sheet(ss, sheet_name: str, dry_run: bool) -> int:
    ws = sheets_call(ss.worksheet, sheet_name)
    all_values = sheets_call(ws.get_all_values)

    cfg = SHEET_CONFIG[sheet_name]
    header_row_index = cfg["header_row_index"]  # 0-based index in all_values

    if len(all_values) <= header_row_index:
        print(f"  [{sheet_name}] Sheet appears empty, skipping.")
        return 0

    header = all_values[header_row_index]

    if "source" in header:
        print(f"  [{sheet_name}] 'source' column already exists at index {header.index('source')} — skipping.")
        return 0

    source_col = len(header) + 1  # 1-based column number for the new column
    print(f"  [{sheet_name}] Will add 'source' at column {source_col} (after '{header[-1]}')")

    data_rows = all_values[header_row_index + 1:]
    non_empty_rows = [r for r in data_rows if any(c.strip() for c in r)]
    print(f"  [{sheet_name}] {len(non_empty_rows)} data rows to backfill with 'discord'")

    if dry_run:
        return len(non_empty_rows)

    # Write the header cell
    header_sheet_row = header_row_index + 1  # convert to 1-based
    header_cell = f"{col_letter(source_col)}{header_sheet_row}"
    sheets_call(ws.update_acell, header_cell, "source")
    print(f"  [{sheet_name}] Wrote header 'source' to {header_cell}")
    time.sleep(1)

    # Backfill data rows in batches
    import gspread
    cells_to_update = []
    for row_offset, row in enumerate(data_rows):
        sheet_row = header_row_index + 2 + row_offset  # 1-based
        if not any(c.strip() for c in row):
            continue  # skip blank rows
        cells_to_update.append(gspread.Cell(sheet_row, source_col, "discord_all_in_one"))

    if cells_to_update:
        # update_cells has a limit; batch in chunks of 500
        for i in range(0, len(cells_to_update), 500):
            chunk = cells_to_update[i:i + 500]
            sheets_call(ws.update_cells, chunk)
            time.sleep(1)
            print(f"  [{sheet_name}] Backfilled rows {i+1}–{i+len(chunk)}")

    return len(cells_to_update)


def col_letter(n: int) -> str:
    """Convert 1-based column number to letter(s). e.g. 1→A, 26→Z, 27→AA."""
    result = ""
    while n > 0:
        n, remainder = divmod(n - 1, 26)
        result = chr(65 + remainder) + result
    return result


def main():
    dry_run = "--dry-run" in sys.argv

    print("=" * 60)
    print("migrate_add_source_column.py")
    print("Dry run:" if dry_run else "LIVE run — will write to Google Sheets")
    print("=" * 60)

    gc = get_gspread_client()
    ss = gc.open_by_key(GOOGLE_SHEET_ID)

    totals = {}
    for sheet_name in SHEETS_TO_MIGRATE:
        print(f"\nChecking [{sheet_name}]...")
        count = migrate_sheet(ss, sheet_name, dry_run=True)
        totals[sheet_name] = count

    print("\n" + "=" * 60)
    print("SUMMARY (dry run):")
    for sheet_name, count in totals.items():
        print(f"  {sheet_name}: {count} rows to backfill")

    if dry_run:
        print("\nRe-run without --dry-run to apply changes.")
        return

    print("\nProceed? This will write to Google Sheets. (y/N): ", end="")
    answer = input().strip().lower()
    if answer != "y":
        print("Aborted.")
        return

    print("\nApplying changes...")
    for sheet_name in SHEETS_TO_MIGRATE:
        print(f"\nMigrating [{sheet_name}]...")
        migrate_sheet(ss, sheet_name, dry_run=False)

    print("\nDone. Verify in Google Sheets before proceeding to Step 1.")


if __name__ == "__main__":
    main()
