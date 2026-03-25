#!/usr/bin/env python3
"""
remediate.py — Alias feedback mechanism for self-improving resolution.

When a human fixes a "team_not_found" or "capper_not_found" row in
master_sheet, this script:
  1. Reads the audit_results rows with status "human_approved"
  2. For each fix, extracts the original raw name and the corrected value
  3. Adds the raw name as an alias to team_name_resolution or
     capper_name_resolution so the resolver handles it automatically next time
  4. Marks the audit row as "remediated" so it's not processed again

This closes the self-improvement loop:
    Stage 2 resolver fails → sentinel written → nightly audit flags it →
    human provides correct value → remediate.py adds alias → resolver
    handles it automatically next time.

Usage:
  .venv/bin/python3 remediate.py                # process all human_approved rows
  .venv/bin/python3 remediate.py --dry-run      # preview without writing
"""

import argparse
import time
from typing import Dict, List, Optional, Tuple

from dotenv import load_dotenv

import gspread
from sheets_utils import GOOGLE_SHEET_ID, get_gspread_client, sheets_call
from stage2_python import TEAM_NOT_FOUND, CAPPER_NOT_FOUND

load_dotenv()

AUDIT_SHEET = "audit_results"
MASTER_SHEET = "master_sheet"
TEAM_RESOLUTION_SHEET = "team_name_resolution"
CAPPER_RESOLUTION_SHEET = "capper_name_resolution"


def _load_audit_rows(ss) -> Tuple[gspread.Worksheet, List[dict], List[int]]:
    """Load human_approved audit rows that need remediation.

    Returns:
        (ws_audit, approved_rows, audit_row_numbers)
        Each approved_row is a dict with audit header keys.
        audit_row_numbers are 1-based sheet row numbers.
    """
    ws_audit = sheets_call(ss.worksheet, AUDIT_SHEET)
    all_values = sheets_call(ws_audit.get_all_values)

    if len(all_values) <= 5:
        return ws_audit, [], []

    header = all_values[4]  # header is row 5 (0-indexed: 4)
    hcol = {h: i for i, h in enumerate(header)}

    approved_rows = []
    row_numbers = []

    for i, row in enumerate(all_values[5:], start=6):
        while len(row) < len(header):
            row.append("")

        status = row[hcol.get("status", 1)].strip()
        check_failed = row[hcol.get("check_failed", 10)].strip()

        if status != "human_approved":
            continue

        if check_failed not in ("unresolved_team", "unresolved_capper"):
            continue

        row_dict = {h: row[hcol[h]].strip() for h in header if h in hcol}
        approved_rows.append(row_dict)
        row_numbers.append(i)

    return ws_audit, approved_rows, row_numbers


def _get_master_row(ss, ms_row_str: str) -> Optional[dict]:
    """Load a single master_sheet row by its 1-based row number.

    Returns dict with master_sheet column keys, or None if row is invalid.
    """
    try:
        ms_row_num = int(ms_row_str.split(",")[0])  # take first if comma-separated
    except (ValueError, IndexError):
        return None

    ms_ws = sheets_call(ss.worksheet, MASTER_SHEET)
    all_values = sheets_call(ms_ws.get_all_values)

    if ms_row_num < 2 or ms_row_num > len(all_values):
        return None

    header = all_values[0]
    row = all_values[ms_row_num - 1]  # convert 1-based to 0-based
    while len(row) < len(header):
        row.append("")

    return {h: row[i].strip() for i, h in enumerate(header)}


def _add_team_alias(ss, sport: str, espn_name: str, raw_alias: str, dry_run: bool) -> bool:
    """Add a raw alias to the team_name_resolution sheet.

    If the ESPN name already exists for this sport, appends the alias to the
    existing aliases cell. Otherwise, creates a new row.

    Returns True if the alias was added (or would be added in dry-run).
    """
    ws = sheets_call(ss.worksheet, TEAM_RESOLUTION_SHEET)
    all_values = sheets_call(ws.get_all_values)

    if not all_values:
        print(f"  Warning: {TEAM_RESOLUTION_SHEET} is empty")
        return False

    header = all_values[0]
    hcol = {h: i for i, h in enumerate(header)}
    sport_col = hcol.get("sport", 0)
    name_col = hcol.get("espn_team_name", 1)
    alias_col = hcol.get("aliases", 2)

    # Search for existing row with this sport + ESPN name
    for row_idx, row in enumerate(all_values[1:], start=2):
        while len(row) <= alias_col:
            row.append("")

        if (row[sport_col].strip().lower() == sport.lower() and
                row[name_col].strip() == espn_name):
            # Found existing row — check if alias already present
            existing_aliases = row[alias_col].strip()
            existing_list = [a.strip().lower() for a in existing_aliases.split(",") if a.strip()]

            if raw_alias.lower() in existing_list:
                print(f"  Alias '{raw_alias}' already exists for {espn_name} ({sport})")
                return False

            # Append the new alias
            if existing_aliases:
                new_aliases = f"{existing_aliases}, {raw_alias}"
            else:
                new_aliases = raw_alias

            if dry_run:
                print(f"  [dry-run] Would update aliases for {espn_name} ({sport}): +'{raw_alias}'")
            else:
                alias_cell = gspread.utils.rowcol_to_a1(row_idx, alias_col + 1)
                sheets_call(ws.update, alias_cell, [[new_aliases]])
                print(f"  Updated aliases for {espn_name} ({sport}): +'{raw_alias}'")
            return True

    # No existing row — create a new one
    new_row = [""] * len(header)
    new_row[sport_col] = sport.lower()
    new_row[name_col] = espn_name
    new_row[alias_col] = raw_alias

    if dry_run:
        print(f"  [dry-run] Would add new team row: {sport} | {espn_name} | alias='{raw_alias}'")
    else:
        sheets_call(ws.append_row, new_row, value_input_option="USER_ENTERED")
        print(f"  Added new team row: {sport} | {espn_name} | alias='{raw_alias}'")
    return True


def _add_capper_alias(ss, canonical_name: str, raw_alias: str, dry_run: bool) -> bool:
    """Add a raw alias to the capper_name_resolution sheet.

    If the canonical name already exists, appends the alias to the existing
    aliases cell. Otherwise, creates a new row.

    Returns True if the alias was added (or would be added in dry-run).
    """
    ws = sheets_call(ss.worksheet, CAPPER_RESOLUTION_SHEET)
    all_values = sheets_call(ws.get_all_values)

    if not all_values:
        print(f"  Warning: {CAPPER_RESOLUTION_SHEET} is empty")
        return False

    header = all_values[0]
    hcol = {h: i for i, h in enumerate(header)}
    name_col = hcol.get("unique_capper_name", 0)
    alias_col = hcol.get("aliases", 1)

    # Search for existing row with this canonical name
    for row_idx, row in enumerate(all_values[1:], start=2):
        while len(row) <= alias_col:
            row.append("")

        if row[name_col].strip() == canonical_name:
            # Found existing row — check if alias already present
            existing_aliases = row[alias_col].strip()
            existing_list = [a.strip().lower() for a in existing_aliases.split(",") if a.strip()]

            if raw_alias.lower() in existing_list:
                print(f"  Alias '{raw_alias}' already exists for {canonical_name}")
                return False

            # Append the new alias
            if existing_aliases:
                new_aliases = f"{existing_aliases}, {raw_alias}"
            else:
                new_aliases = raw_alias

            if dry_run:
                print(f"  [dry-run] Would update aliases for {canonical_name}: +'{raw_alias}'")
            else:
                alias_cell = gspread.utils.rowcol_to_a1(row_idx, alias_col + 1)
                sheets_call(ws.update, alias_cell, [[new_aliases]])
                print(f"  Updated aliases for {canonical_name}: +'{raw_alias}'")
            return True

    # No existing row — create a new one
    new_row = [""] * len(header)
    new_row[name_col] = canonical_name
    new_row[alias_col] = raw_alias

    if dry_run:
        print(f"  [dry-run] Would add new capper row: {canonical_name} | alias='{raw_alias}'")
    else:
        sheets_call(ws.append_row, new_row, value_input_option="USER_ENTERED")
        print(f"  Added new capper row: {canonical_name} | alias='{raw_alias}'")
    return True


def _mark_remediated(ws_audit: gspread.Worksheet, audit_row_num: int, dry_run: bool):
    """Update audit row status from human_approved to remediated."""
    if dry_run:
        print(f"  [dry-run] Would mark audit row {audit_row_num} as 'remediated'")
    else:
        # Status is column B
        sheets_call(ws_audit.update, f"B{audit_row_num}", [["remediated"]])
        print(f"  Marked audit row {audit_row_num} as 'remediated'")


def remediate(ss, dry_run: bool = False):
    """Process all human_approved unresolved_team/unresolved_capper audit rows.

    For each:
      1. Read the corrected value from master_sheet (human already fixed it)
      2. Extract the original raw name from the audit details
      3. Add raw name as alias to the appropriate resolution sheet
      4. Mark audit row as remediated
    """
    print("Loading human_approved audit rows...")
    ws_audit, approved_rows, row_numbers = _load_audit_rows(ss)

    if not approved_rows:
        print("No human_approved unresolved rows to remediate.")
        return

    print(f"Found {len(approved_rows)} human_approved row(s) to remediate\n")

    team_count = 0
    capper_count = 0
    skipped = 0

    for audit_row, audit_row_num in zip(approved_rows, row_numbers):
        check_failed = audit_row.get("check_failed", "")
        ms_row_str = audit_row.get("ms_row", "")
        original_pick = audit_row.get("pick", "")
        sport = audit_row.get("sport", "")

        # Load the current (corrected) master_sheet row
        ms_row = _get_master_row(ss, ms_row_str)
        if ms_row is None:
            print(f"  Warning: could not load master_sheet row {ms_row_str}; skipping")
            skipped += 1
            continue

        if check_failed == "unresolved_team":
            # The human corrected the game column in master_sheet
            corrected_game = ms_row.get("game", "").strip()
            corrected_pick = ms_row.get("pick", "").strip()

            if not corrected_game or corrected_game == TEAM_NOT_FOUND:
                print(f"  Warning: master_sheet row {ms_row_str} still has "
                      f"game='{corrected_game}'; skipping (not yet fixed by human)")
                skipped += 1
                continue

            # The corrected pick (ESPN name) is the team the human chose.
            # The original raw pick that failed resolution is the alias to add.
            # If the human also corrected the pick column, use the corrected pick
            # as the ESPN name and the original pick as the alias.
            espn_name = corrected_pick
            raw_alias = original_pick

            # Only add alias if they differ (otherwise it's the same name, no alias needed)
            if raw_alias.lower().strip() == espn_name.lower().strip():
                print(f"  Pick '{raw_alias}' matches corrected name '{espn_name}'; "
                      f"no alias needed (game was the issue)")
                # Still mark as remediated since the fix is in place
                _mark_remediated(ws_audit, audit_row_num, dry_run)
                team_count += 1
                continue

            added = _add_team_alias(ss, sport, espn_name, raw_alias, dry_run)
            _mark_remediated(ws_audit, audit_row_num, dry_run)
            if added:
                team_count += 1

        elif check_failed == "unresolved_capper":
            # The human corrected the capper column in master_sheet
            corrected_capper = ms_row.get("capper", "").strip()

            if not corrected_capper or corrected_capper == CAPPER_NOT_FOUND:
                print(f"  Warning: master_sheet row {ms_row_str} still has "
                      f"capper='{corrected_capper}'; skipping (not yet fixed by human)")
                skipped += 1
                continue

            # Extract the original raw capper name from the audit row
            # The audit row's capper field contains the sentinel "capper_not_found",
            # but we need the ORIGINAL raw name that was fed into the resolver.
            # The original raw name was written to the details field.
            # For now, we can't reliably recover it from audit_results alone,
            # so we check parsed_picks_new or use the audit ocr_text context.
            #
            # However, the original pick's raw capper was lost when replaced by
            # the sentinel. The audit row's capper column = "capper_not_found".
            # We need a way to get the original raw name.
            #
            # Solution: look at parsed_picks_new for the same date/pick/line
            # to find the raw capper name that was before normalization.
            raw_capper = _find_raw_capper(ss, ms_row)
            if not raw_capper:
                print(f"  Warning: could not find raw capper name for "
                      f"master_sheet row {ms_row_str}; skipping")
                skipped += 1
                continue

            canonical_name = corrected_capper

            if raw_capper.lower().strip() == canonical_name.lower().strip():
                print(f"  Raw capper '{raw_capper}' matches corrected name; "
                      f"no alias needed")
                _mark_remediated(ws_audit, audit_row_num, dry_run)
                capper_count += 1
                continue

            added = _add_capper_alias(ss, canonical_name, raw_capper, dry_run)
            _mark_remediated(ws_audit, audit_row_num, dry_run)
            if added:
                capper_count += 1

    print(f"\nRemediation complete:")
    print(f"  Team aliases added:   {team_count}")
    print(f"  Capper aliases added: {capper_count}")
    print(f"  Skipped:              {skipped}")


def _find_raw_capper(ss, ms_row: dict) -> Optional[str]:
    """Find the original raw capper name from parsed_picks_new.

    Looks up by (date, sport, pick, line) to find what the raw capper
    name was before the resolver replaced it with the sentinel.
    """
    PICKS_NEW_SHEET = "parsed_picks_new"
    try:
        ws = sheets_call(ss.worksheet, PICKS_NEW_SHEET)
        all_values = sheets_call(ws.get_all_values)
    except Exception:
        return None

    if len(all_values) < 4:
        return None

    header = all_values[2]  # header is row 3
    hcol = {h: i for i, h in enumerate(header)}
    date_col = hcol.get("date", 0)
    capper_col = hcol.get("capper", 1)
    sport_col = hcol.get("sport", 2)
    pick_col = hcol.get("pick", 3)
    line_col = hcol.get("line", 4)

    target_date = ms_row.get("date", "")
    target_sport = ms_row.get("sport", "")
    target_pick = ms_row.get("pick", "")
    target_line = ms_row.get("line", "")

    for row in all_values[3:]:
        while len(row) <= max(date_col, capper_col, sport_col, pick_col, line_col):
            row.append("")

        if (row[date_col].strip() == target_date and
                row[sport_col].strip().lower() == target_sport.lower() and
                row[pick_col].strip() == target_pick and
                row[line_col].strip() == target_line):
            raw = row[capper_col].strip()
            if raw:
                return raw

    return None


def main():
    parser = argparse.ArgumentParser(
        description="Remediate unresolved team/capper names by adding aliases"
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Preview without writing to sheets"
    )
    args = parser.parse_args()

    print("Connecting to Google Sheets...")
    gc = get_gspread_client()
    ss = gc.open_by_key(GOOGLE_SHEET_ID)

    remediate(ss, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
