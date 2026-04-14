#!/usr/bin/env python3
"""
Reprocess picks for a specific sport from image_pull OCR text.

Use case: when a new sport is added (e.g. MLB) but image_pull rows were
already marked stage_1_parsed before the sport was in _is_valid_sport,
so those picks were silently dropped.  This script re-runs Stage 1 + 2
on the OCR text for the given date(s) and sport, writing only new picks
to finalized_picks, master_sheet, and parsed_picks_new.

Usage:
  # Reprocess yesterday's MLB picks
  .venv/bin/python3 reprocess_sport.py --date 2026-04-13 --sport MLB

  # Reprocess a date range
  .venv/bin/python3 reprocess_sport.py --date 2026-04-10 --end-date 2026-04-13 --sport MLB

  # Dry run — parse and print, don't write to sheets
  .venv/bin/python3 reprocess_sport.py --date 2026-04-13 --sport MLB --dry-run
"""

import argparse
import re
import sys
from datetime import datetime, timedelta

from dotenv import load_dotenv

load_dotenv()

from sheets_utils import (
    GOOGLE_SHEET_ID,
    SPORT_TO_SCHED,
    get_gspread_client,
    sheets_read,
    sheets_write,
    get_schedule_for_date,
)
from pick_parser import (
    CLAUDE_USAGE,
    get_claude_cost,
    call_sonnet_text,
    build_stage1_prompt,
    parse_csv_response,
    validate_and_fix_pick_column,
    deduplicate_ml_vs_spread,
)
from stage2_python import finalize_picks_python
from team_resolver import TeamResolver
from capper_resolver import CapperResolver
from capper_analyzer import (
    FINALIZED_PICKS_SHEET,
    MASTER_SHEET,
    PARSED_PICKS_NEW_SHEET,
    STAGE_BATCH_SIZE,
    get_or_create_picks_worksheet,
    format_schedule_for_prompt,
    fetch_schedule_data,
)

# Regex that matches any totals (over/under) line value.
_TOTAL_LINE_RE = re.compile(r"^[OoUu](?:ver|nder)?\s*\d", re.IGNORECASE)


def date_range(start: str, end: str):
    """Yield YYYY-MM-DD strings from start to end (inclusive)."""
    cur = datetime.strptime(start, "%Y-%m-%d")
    stop = datetime.strptime(end, "%Y-%m-%d")
    while cur <= stop:
        yield cur.strftime("%Y-%m-%d")
        cur += timedelta(days=1)


def load_image_pull_rows(spreadsheet, target_dates: set):
    """Read image_pull rows for the given dates that have OCR text.

    Returns list of (capper_name, message_date, ocr_text) tuples.
    """
    ws = sheets_read(spreadsheet.worksheet, "image_pull")
    all_values = sheets_read(ws.get_all_values)

    rows = []
    # Row 1 = timestamp, Row 2 = DO NOT EDIT, Row 3+ = data
    # Columns: timestamp(0), message_sent_at(1), capper_name(2),
    #          image_url(3), ocr_text(4), committed_stage(5)
    for row in all_values[2:]:
        if len(row) < 5:
            continue
        ocr_text = row[4].strip() if len(row) > 4 else ""
        if not ocr_text:
            continue
        message_sent_at = row[1] if len(row) > 1 else ""
        message_date = message_sent_at.split(" ")[0] if message_sent_at else ""
        if message_date not in target_dates:
            continue
        capper_name = row[2] if len(row) > 2 else "Unknown"
        rows.append((capper_name, message_date, ocr_text))

    return rows


def load_existing_picks(spreadsheet, sheet_name):
    """Load existing picks from a sheet for dedup checking.

    Returns list of (date, capper, sport, pick, line) tuples.
    """
    try:
        ws = sheets_read(spreadsheet.worksheet, sheet_name)
        all_values = sheets_read(ws.get_all_values)
    except Exception:
        return set()

    existing = set()
    # Data starts at row 4 (index 3)
    for row in all_values[3:] if len(all_values) > 3 else []:
        if len(row) >= 5:
            key = (
                row[0].strip(),           # date
                row[1].strip().upper(),    # capper
                row[2].strip().upper(),    # sport
                row[3].strip().upper(),    # pick
                row[4].strip().upper(),    # line
            )
            existing.add(key)
    return existing


def main():
    parser = argparse.ArgumentParser(
        description="Reprocess picks for a specific sport from image_pull OCR"
    )
    parser.add_argument(
        "--date", required=True,
        help="Start date (YYYY-MM-DD). Required.",
    )
    parser.add_argument(
        "--end-date", default=None,
        help="End date (YYYY-MM-DD). Defaults to --date (single day).",
    )
    parser.add_argument(
        "--sport", required=True,
        help="Sport to reprocess (e.g. MLB, NBA, CBB, NHL).",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Parse and print picks but don't write to sheets.",
    )
    args = parser.parse_args()

    sport_upper = args.sport.upper()
    end_date = args.end_date or args.date
    target_dates = set(date_range(args.date, end_date))

    print(f"Reprocessing {sport_upper} picks for dates: {sorted(target_dates)}")
    if args.dry_run:
        print("  (DRY RUN — will not write to sheets)")

    # ── Connect ──────────────────────────────────────────────────────────
    client = get_gspread_client()
    spreadsheet = client.open_by_key(GOOGLE_SHEET_ID)

    # ── Load image_pull rows ─────────────────────────────────────────────
    image_rows = load_image_pull_rows(spreadsheet, target_dates)
    print(f"Found {len(image_rows)} image_pull rows with OCR for those dates")

    if not image_rows:
        print("Nothing to reprocess.")
        return

    # ── Load existing picks for dedup ────────────────────────────────────
    existing_keys = load_existing_picks(spreadsheet, PARSED_PICKS_NEW_SHEET)
    print(f"Loaded {len(existing_keys)} existing picks for dedup")

    # ── Stage 1: Parse OCR → structured rows ─────────────────────────────
    print(f"\n── Stage 1: Parsing OCR for {sport_upper} picks ──")

    all_parsed_rows = []

    for batch_start in range(0, len(image_rows), STAGE_BATCH_SIZE):
        batch = image_rows[batch_start : batch_start + STAGE_BATCH_SIZE]
        print(f"\nBatch {batch_start // STAGE_BATCH_SIZE + 1}: {len(batch)} row(s)")

        # Fetch schedules for dates in this batch
        message_dates = set(date for _, date, _ in batch)
        schedule_data = fetch_schedule_data(spreadsheet, message_dates)

        # Build picks list with row_id anchoring
        picks_to_parse = [
            (capper, date, ocr, batch_start + i)
            for i, (capper, date, ocr) in enumerate(batch)
        ]
        ocr_lookup = {(capper, date): ocr for capper, date, ocr in batch}

        prompt = build_stage1_prompt(picks_to_parse, schedule_data)
        print(f"Calling Sonnet to parse {len(picks_to_parse)} OCR blocks...")

        try:
            response = call_sonnet_text(prompt)
            parsed_rows = parse_csv_response(response)
            print(f"  Parsed {len(parsed_rows)} raw pick row(s)")

            # Filter to target sport only
            sport_rows = [
                row for row in parsed_rows
                if len(row) > 2 and row[2].strip().upper() == sport_upper
            ]
            print(f"  {len(sport_rows)} are {sport_upper}")

            # Attach ocr_text (col 8)
            for row in sport_rows:
                while len(row) < 8:
                    row.append("")
                capper_key = row[1].strip() if len(row) > 1 else ""
                date_key = row[0].strip() if len(row) > 0 else ""
                row.append(ocr_lookup.get((capper_key, date_key), ""))

            all_parsed_rows.extend(sport_rows)

        except Exception as e:
            print(f"  Stage 1 batch failed: {e}")
            continue

    if not all_parsed_rows:
        print(f"\nNo {sport_upper} picks found in OCR text.")
        return

    print(f"\n── Stage 1 complete: {len(all_parsed_rows)} {sport_upper} picks parsed ──")

    # ── Dedup against existing picks ─────────────────────────────────────
    before_dedup = len(all_parsed_rows)
    deduped = []
    for row in all_parsed_rows:
        key = (
            row[0].strip(),           # date
            row[1].strip().upper(),   # capper
            row[2].strip().upper(),   # sport
            row[3].strip().upper(),   # pick
            row[4].strip().upper(),   # line
        )
        if key in existing_keys:
            print(f"  [dedup] Skipping duplicate: {row[1]} {row[3]} {row[4]}")
            continue
        existing_keys.add(key)
        deduped.append(row)
    all_parsed_rows = deduped
    skipped = before_dedup - len(all_parsed_rows)
    if skipped:
        print(f"  Deduped: {skipped} already exist, {len(all_parsed_rows)} new")

    if not all_parsed_rows:
        print("All picks already exist in sheets. Nothing to write.")
        return

    # ── Stage 2: Resolve teams/cappers/games (Python) ────────────────────
    print(f"\n── Stage 2: Finalizing {len(all_parsed_rows)} picks ──")

    team_resolver = TeamResolver(spreadsheet)
    capper_resolver = CapperResolver(spreadsheet)

    # Preserve ocr_text before Stage 2 (it only returns 8 cols)
    ocr_texts = [row[8] if len(row) > 8 else "" for row in all_parsed_rows]

    finalized_rows = finalize_picks_python(
        team_resolver, capper_resolver, all_parsed_rows
    )
    finalized_rows = validate_and_fix_pick_column(finalized_rows)

    # Re-attach ocr_text (col 8)
    for j, row in enumerate(finalized_rows):
        ocr = ocr_texts[j] if j < len(ocr_texts) else ""
        if len(row) < 9:
            row.append(ocr)
        else:
            row[8] = ocr

    # Print results
    print(f"\nFinalized {len(finalized_rows)} pick(s):")
    for row in finalized_rows:
        print(f"  {row[0]} | {row[1]} | {row[2]} | {row[3]} {row[4]} | {row[5]}")

    if args.dry_run:
        print(f"\n── DRY RUN complete. {len(finalized_rows)} picks would be written. ──")
        cost = get_claude_cost()
        print(f"Claude API cost: ${cost:.4f}")
        return

    # ── Write to sheets ──────────────────────────────────────────────────
    print(f"\n── Writing {len(finalized_rows)} picks to sheets ──")

    # 1. finalized_picks — with ML/spread dedup
    finalized_ws = get_or_create_picks_worksheet(spreadsheet, FINALIZED_PICKS_SHEET)
    existing_fp_values = sheets_read(finalized_ws.get_all_values)
    existing_fp_data = (
        existing_fp_values[3:] if len(existing_fp_values) > 3 else []
    )

    finalized_rows, ml_rows_to_delete = deduplicate_ml_vs_spread(
        finalized_rows, existing_fp_data, 4
    )
    for sheet_row in sorted(ml_rows_to_delete, reverse=True):
        sheets_write(finalized_ws.delete_rows, sheet_row)

    if finalized_rows:
        sheets_write(
            finalized_ws.append_rows,
            finalized_rows,
            value_input_option="USER_ENTERED",
        )
        print(f"  finalized_picks: appended {len(finalized_rows)} rows")

    # Tag source (col 9)
    source = f"reprocess_{sport_upper.lower()}"
    for row in finalized_rows:
        while len(row) < 9:
            row.append("")
        if len(row) < 10:
            row.append(source)

    # 2. master_sheet — cols 0-7 + source, skip totals
    if finalized_rows:
        master_ws = get_or_create_picks_worksheet(spreadsheet, MASTER_SHEET)
        master_rows = [
            row[:8] + [row[9]]
            for row in finalized_rows
            if not _TOTAL_LINE_RE.match(str(row[4]))
        ]
        if master_rows:
            sheets_write(
                master_ws.append_rows,
                master_rows,
                value_input_option="USER_ENTERED",
            )
            print(f"  master_sheet: appended {len(master_rows)} rows")

    # 3. parsed_picks_new — full rows with ocr_text + source
    if finalized_rows:
        picks_new_ws = get_or_create_picks_worksheet(
            spreadsheet, PARSED_PICKS_NEW_SHEET
        )
        sheets_write(
            picks_new_ws.append_rows,
            finalized_rows,
            value_input_option="USER_ENTERED",
        )
        print(f"  parsed_picks_new: appended {len(finalized_rows)} rows")

    # ── Summary ──────────────────────────────────────────────────────────
    cost = get_claude_cost()
    print(f"\n── Done ──")
    print(f"  {len(finalized_rows)} {sport_upper} picks written")
    print(f"  Claude API cost: ${cost:.4f}")


if __name__ == "__main__":
    main()
