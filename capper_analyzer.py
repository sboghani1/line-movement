#!/usr/bin/env python3
"""
Capper Analyzer
Fetches the most recent image from a Discord channel using a user token,
OCRs pick images with Claude, parses and finalizes picks with schedule
matching, and appends results to Google Sheets + local CSV.
"""

import base64
import csv
import io
import json
import os
import re
import time
from datetime import datetime
from typing import List, Optional, Tuple
from zoneinfo import ZoneInfo

import requests
from dotenv import load_dotenv

load_dotenv()

import anthropic
import gspread
from PIL import Image

from activity_logger import log_activity
from git_utils import git_push_csv
from sheets_utils import (
    GOOGLE_SHEET_ID, SPORT_TO_SCHED, get_gspread_client, sheets_read,
    sheets_write, get_schedule_for_date,
)
import daily_audit
import populate_results
from discord_fetcher import get_messages_with_images_since
from stage2_python import finalize_picks_python, TEAM_NOT_FOUND, CAPPER_NOT_FOUND
from team_resolver import TeamResolver
from capper_resolver import CapperResolver
from pick_parser import (
    PICKS_COLUMNS,
    DISCORD_SOURCE,
    CLAUDE_USAGE,
    get_claude_cost,
    log_claude_usage,
    call_sonnet_text,
    build_stage1_prompt,
    build_stage2_prompt,
    parse_csv_response,
    parse_stage2_response,
    assemble_finalized_rows,
    validate_and_fix_pick_column,
    deduplicate_ml_vs_spread,
    lookup_spread_from_schedule,
)

# ── Config ──────────────────────────────────────────────────────────────────
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
WORKSHEET_NAME = "image_pull"

FIELDNAMES = [
    "timestamp",
    "message_sent_at",
    "capper_name",
    "image_url",
    "ocr_text",
    "committed_stage",
]

# Worksheet names
PARSED_PICKS_SHEET = "parsed_picks"
PARSED_PICKS_NEW_SHEET = "parsed_picks_new"
FINALIZED_PICKS_SHEET = "finalized_picks"
MASTER_SHEET = "master_sheet"
MANUAL_PICKS_QUEUE_SHEET = "manual_picks_queue"

# Maximum number of messages to process per run
MAX_MESSAGES_PER_RUN = 500

# Regex that matches any totals (over/under) line value so they can be
# filtered out before writing to master_sheet.  Catches all formats seen
# in practice: "U138.5", "O 136.5", "under 6.5", "over 151.5".
_TOTAL_LINE_RE = re.compile(r'^[OoUu](?:ver|nder)?\s*\d', re.IGNORECASE)

# Maximum images per OCR batch (Claude supports up to 20)
OCR_BATCH_SIZE = 15

# Maximum OCR rows per Stage 1/2 parsing call.
# Keeping this small prevents the model from losing track of which OCR text
# belongs to which capper and hallucinating picks across rows.
STAGE_BATCH_SIZE = 10

# Local CSV path for GitHub Pages
LOCAL_CSV_PATH = "gh-pages/data/master_sheet.csv"


def sync_master_to_csv(ss) -> int:
    """Overwrite the local CSV with the current contents of master_sheet in Google Sheets."""
    ws = sheets_read(ss.worksheet, populate_results.MASTER_SHEET)
    all_values = sheets_read(ws.get_all_values)
    if not all_values:
        print("  sync_master_to_csv: master_sheet is empty, skipping")
        return 0
    header = all_values[0]
    data   = all_values[1:]
    with open(LOCAL_CSV_PATH, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(data)
    print(f"  Synced {len(data)} rows → {LOCAL_CSV_PATH}")
    return len(data)






# ── Google Sheets Setup ──────────────────────────────────────────────────────
def get_worksheet():
    """Get or create the worksheet for storing image URLs."""
    client = get_gspread_client()
    spreadsheet = client.open_by_key(GOOGLE_SHEET_ID)

    try:
        worksheet = sheets_read(spreadsheet.worksheet, WORKSHEET_NAME)
    except gspread.WorksheetNotFound:
        worksheet = spreadsheet.add_worksheet(
            title=WORKSHEET_NAME, rows=1000, cols=len(FIELDNAMES)
        )
        sheets_write(worksheet.append_row, FIELDNAMES)

    return worksheet


def get_existing_urls(worksheet) -> set:
    """Get a set of existing image URLs (column 4) to avoid duplicates."""
    try:
        # Get all values from column 4 (image_url)
        col_values = sheets_read(worksheet.col_values, 4)
        # Skip header row (row 2 is first data row since row 1 is timestamp)
        return set(col_values[2:]) if len(col_values) > 2 else set()
    except Exception:
        return set()


def get_urls_with_ocr(worksheet) -> set:
    """Get URLs that already have OCR text (column 5 is not empty)."""
    try:
        all_values = sheets_read(worksheet.get_all_values)
        # Skip row 1 (timestamp) and row 2 (header)
        urls_with_ocr = set()
        for row in all_values[2:]:
            if len(row) >= 5 and row[3] and row[4]:  # Has URL and OCR text
                urls_with_ocr.add(row[3])
        return urls_with_ocr
    except Exception:
        return set()


def get_or_create_picks_worksheet(spreadsheet, sheet_name: str):
    """Get or create a picks worksheet with proper structure."""
    try:
        worksheet = sheets_read(spreadsheet.worksheet, sheet_name)
    except gspread.WorksheetNotFound:
        worksheet = spreadsheet.add_worksheet(
            title=sheet_name, rows=1000, cols=len(PICKS_COLUMNS)
        )
        # Row 1: timestamp placeholder
        sheets_write(
            worksheet.update_acell,
            "A1", datetime.now(ZoneInfo("UTC")).strftime("%Y-%m-%d %H:%M:%S")
        )
        # Row 2: DO NOT EDIT label
        sheets_write(worksheet.update_acell, "A2", "DO NOT EDIT ANYTHING ABOVE THIS ROW")
        # Row 3: column headers
        sheets_write(worksheet.append_row, PICKS_COLUMNS)
    return worksheet


def get_known_cappers(spreadsheet) -> List[str]:
    """Get list of known capper names from finalized_picks.

    Returns unique capper names to help normalize manual queue cappers.
    """
    try:
        worksheet = sheets_read(spreadsheet.worksheet, FINALIZED_PICKS_SHEET)
        all_values = sheets_read(worksheet.get_all_values)
        # Data starts at row 4 (0-indexed: row 3+)
        # Column B (index 1) is capper
        cappers = set()
        for row in all_values[3:]:
            if len(row) > 1 and row[1]:
                capper = row[1].strip()
                if capper and capper.lower() not in ("capper", "unknown"):
                    cappers.add(capper)
        return sorted(cappers)
    except gspread.WorksheetNotFound:
        return []




def format_schedule_for_prompt(games: List[dict], sport: str) -> str:
    """Format game schedule into a string for the prompt.

    Only includes team names (away @ home). Spread and O/U are intentionally
    omitted — they are unused by the prompt and the spread column in particular
    contradicts the 'NEVER get the spread from the schedule' instruction, which
    causes model confusion.
    """
    if not games:
        return f"No {sport} games scheduled"

    lines = [f"{g['away_team']} @ {g['home_team']}" for g in games]
    return "\n".join(lines)


def fetch_schedule_data(spreadsheet, message_dates):
    """Fetch schedule data for all sports and dates, formatted for prompts."""
    all_games = {sport: [] for sport in SPORT_TO_SCHED}
    for msg_date in sorted(message_dates):
        for sport, sheet_name in SPORT_TO_SCHED.items():
            all_games[sport].extend(
                get_schedule_for_date(spreadsheet, sheet_name, msg_date)
            )
    return {
        sport: format_schedule_for_prompt(games, sport.upper())
        for sport, games in all_games.items()
    }



def get_last_run_timestamp(worksheet) -> Optional[datetime]:
    """Get the last run timestamp from cell A1."""
    try:
        value = worksheet.acell("A1").value
        if value:
            # Parse UTC timestamp from A1
            return datetime.strptime(value, "%Y-%m-%d %H:%M:%S").replace(
                tzinfo=ZoneInfo("UTC")
            )
    except Exception:
        pass
    return None


def update_last_run_timestamp(worksheet, utc_time: datetime):
    """Update cell A1 with the current UTC timestamp."""
    sheets_write(worksheet.update_acell, "A1", utc_time.strftime("%Y-%m-%d %H:%M:%S"))


# ── Claude OCR ───────────────────────────────────────────────────────────────
def get_media_type(image_url: str, content_type: str) -> str:
    """Determine media type from URL or content-type header."""
    if "png" in image_url.lower() or "png" in content_type:
        return "image/png"
    elif "gif" in image_url.lower() or "gif" in content_type:
        return "image/gif"
    elif "webp" in image_url.lower() or "webp" in content_type:
        return "image/webp"
    return "image/jpeg"


def extract_capper_from_ocr(ocr_text: str) -> Optional[str]:
    """Extract capper name from OCR text.

    Patterns:
    - "CAPPER Whale Exclusive" -> CAPPER
    - "CAPPER\n..." -> CAPPER (first line if short and uppercase)
    """
    if not ocr_text:
        return None

    text = ocr_text.strip()

    # Pattern 1: "NAME Whale Exclusive" or "NAME Whale"
    import re

    whale_match = re.match(r"^([A-Za-z0-9_]+)\s+Whale", text, re.IGNORECASE)
    if whale_match:
        return whale_match.group(1).upper()

    # Pattern 2: First line is a short name (likely capper)
    first_line = text.split("\n")[0].strip()
    # If first line is short (1-20 chars) and doesn't look like a sport/pick
    if first_line and len(first_line) <= 20:
        # Skip if it's a sport name
        sports = ["NBA", "NCAAB", "NHL", "NFL", "MLB", "SOCCER", "WNBA", "MLS"]
        if first_line.upper() not in sports:
            # Skip if it contains pick-like words
            pick_words = [
                "over",
                "under",
                "spread",
                "ml",
                "moneyline",
                "+",
                "-",
                "pts",
                "points",
            ]
            if not any(w in first_line.lower() for w in pick_words):
                return first_line.upper()

    return None



# Claude's 5MB limit applies to base64-encoded data (~33% larger than raw bytes).
# 3.5MB raw -> ~4.7MB base64, safely under the limit.
_OCR_MAX_IMAGE_BYTES = int(3.5 * 1024 * 1024)


def _compress_image_if_needed(
    image_bytes: bytes, media_type: str, label: str = ""
) -> tuple[bytes, str] | None:
    """Compress image bytes to fit within _OCR_MAX_IMAGE_BYTES.

    Returns (compressed_bytes, new_media_type), or None if compression failed.
    """
    if len(image_bytes) <= _OCR_MAX_IMAGE_BYTES:
        return image_bytes, media_type
    original_mb = len(image_bytes) / (1024 * 1024)
    print(f"  ⚠️ {label}exceeds 3.5MB ({original_mb:.2f}MB), compressing...")
    img = Image.open(io.BytesIO(image_bytes))
    if img.mode in ("RGBA", "P"):
        img = img.convert("RGB")
    quality = 85
    for _ in range(20):
        buffer = io.BytesIO()
        img.save(buffer, format="JPEG", quality=quality, optimize=True)
        image_bytes = buffer.getvalue()
        if len(image_bytes) <= _OCR_MAX_IMAGE_BYTES:
            print(f"    Compressed to {len(image_bytes) / (1024 * 1024):.2f}MB (quality={quality})")
            return image_bytes, "image/jpeg"
        if quality > 20:
            quality -= 10
        else:
            img = img.resize(
                (img.width * 3 // 4, img.height * 3 // 4),
                Image.Resampling.LANCZOS,
            )
            quality = 50
    print(f"    ⚠️ Could not compress below 3.5MB, skipping")
    return None


def _run_ocr_api(image_contents: list, processed_count: int, total: int, skipped: set) -> list[str]:
    """Call Claude Haiku with pre-built image_contents and parse the OCR response."""
    prompt_text = (
        f"Read and transcribe every word visible in each of the {processed_count} images above.\n"
        "For each image, output in this exact format:\n\n"
        "[Image 1]\n<transcribed text here>\n\n"
        "[Image 2]\n<transcribed text here>\n\n"
        "...and so on. Preserve the layout of each image's text as much as possible."
    )
    image_contents.append({"type": "text", "text": prompt_text})
    # Use direct Anthropic API -- the LiteLLM proxy (localhost:4000) strips image content.
    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY, base_url="https://api.anthropic.com")
    message = client.messages.create(
        model="claude-haiku-4-5-20251001",
        max_tokens=4096,
        messages=[{"role": "user", "content": image_contents}],
    )
    log_claude_usage(message)
    parts = re.split(r"\[Image \d+\]\s*", message.content[0].text)
    ocr_results = [p.strip() for p in parts[1:]]
    while len(ocr_results) < processed_count:
        ocr_results.append("")
    results: list[str] = []
    ocr_idx = 0
    for i in range(total):
        if i in skipped:
            results.append("")
        else:
            results.append(ocr_results[ocr_idx] if ocr_idx < len(ocr_results) else "")
            ocr_idx += 1
    return results


def extract_text_from_images_batch(image_urls: List[str]) -> List[str]:
    """Use Claude Haiku to OCR multiple images in a single batch call.

    Args:
        image_urls: List of image URLs to OCR (max 15 recommended, 20 max supported)

    Returns:
        List of OCR text results, one per image in the same order
    """
    if not ANTHROPIC_API_KEY:
        raise ValueError("ANTHROPIC_API_KEY environment variable not set")
    if not image_urls:
        return []
    image_contents: list = []
    skipped_indices: set = set()
    processed_count = 0
    for i, url in enumerate(image_urls):
        response = requests.get(url)
        response.raise_for_status()
        content_type = response.headers.get("content-type", "image/jpeg")
        media_type = get_media_type(url, content_type)
        compressed = _compress_image_if_needed(
            response.content, media_type, label=f"Image {i + 1} ({url[:80]}...) "
        )
        if compressed is None:
            skipped_indices.add(i)
            continue
        image_bytes, media_type = compressed
        image_data = base64.standard_b64encode(image_bytes).decode("utf-8")
        processed_count += 1
        image_contents.append({"type": "text", "text": f"[Image {processed_count}]"})
        image_contents.append(
            {"type": "image", "source": {"type": "base64", "media_type": media_type, "data": image_data}}
        )
    if processed_count == 0:
        return [""] * len(image_urls)
    return _run_ocr_api(image_contents, processed_count, len(image_urls), skipped_indices)




def get_rows_needing_stage1(worksheet) -> List[Tuple[int, str, str, str]]:
    """Get image_pull rows that need Stage 1 processing.

    Returns:
        List of (row_index, capper_name, message_date, ocr_text) tuples
    """
    all_values = sheets_read(worksheet.get_all_values)
    rows_to_process = []

    # Row 1 = timestamp, Row 2 = DO NOT EDIT, Row 3+ = data
    # Columns: timestamp(0), message_sent_at(1), capper_name(2), image_url(3), ocr_text(4), committed_stage(5)
    for i, row in enumerate(all_values[2:], start=3):  # Start at row 3 (1-indexed)
        if len(row) < 5:
            continue

        ocr_text = row[4].strip() if len(row) > 4 else ""
        committed_stage = row[5].strip() if len(row) > 5 else ""

        # Skip if no OCR text
        if not ocr_text:
            continue

        # Skip if already processed to stage 1 or stage 2
        if committed_stage in ("stage_1_parsed", "stage_2_finalized"):
            continue

        # Check for failed attempts
        if committed_stage.startswith("parse_failed_attempt_count_"):
            try:
                attempt_count = int(committed_stage.split("_")[-1])
                if attempt_count >= 5:
                    continue  # Max retries reached
            except ValueError:
                pass

        capper_name = row[2] if len(row) > 2 else "Unknown"
        message_sent_at = row[1] if len(row) > 1 else ""
        # Extract date from message_sent_at (format: YYYY-MM-DD HH:MM:SS)
        message_date = message_sent_at.split(" ")[0] if message_sent_at else ""

        rows_to_process.append((i, capper_name, message_date, ocr_text))

    return rows_to_process


def run_stage1(spreadsheet, image_pull_ws):
    """Run Stage 1: Parse OCR text into structured pick rows.

    Reads unprocessed rows from image_pull, sends them to Sonnet in batches of
    STAGE_BATCH_SIZE, and appends results to parsed_picks.

    Batching rationale: sending all rows in one prompt causes the model to
    hallucinate picks that "bleed" across cappers, or invent picks not present
    in the OCR.  Small batches keep each prompt focused.

    Each parsed row gets ocr_text attached as col 9 via an ocr_lookup dict
    keyed by (capper, date).  This is the only opportunity to link OCR source
    text to the parsed output — once rows move to finalized_picks the raw OCR
    is no longer available from that sheet, so it must travel with the row.

    Row anchoring: each OCR block is tagged [ROW:N] in the prompt so Sonnet
    only attributes picks to the block they appear in.  This prevents the model
    from "borrowing" a team name from one capper's image and attaching it to
    another's row.
    """
    eastern = ZoneInfo("America/New_York")
    now_eastern = datetime.now(eastern)

    # Get rows needing Stage 1 processing
    rows_to_process = get_rows_needing_stage1(image_pull_ws)

    if not rows_to_process:
        print("No rows need Stage 1 parsing")
        return

    print(f"\n── Stage 1: Parsing {len(rows_to_process)} OCR result(s) ──")

    # Process in batches to keep each prompt focused and prevent hallucination
    all_parsed_rows = []
    all_processed_row_idxs = []

    for batch_start in range(0, len(rows_to_process), STAGE_BATCH_SIZE):
        batch = rows_to_process[batch_start : batch_start + STAGE_BATCH_SIZE]
        print(f"\nBatch {batch_start // STAGE_BATCH_SIZE + 1}: {len(batch)} row(s)")

        # Get unique dates in this batch to fetch only relevant schedules
        message_dates = set()
        for _, _, date, _ in batch:
            if date:
                message_dates.add(date)

        print(f"Fetching schedules for dates: {sorted(message_dates)}")
        schedule_data = fetch_schedule_data(spreadsheet, message_dates)

        # Build picks list with row_id anchoring
        picks_to_parse = [
            (capper, date, ocr, row_idx)
            for row_idx, capper, date, ocr in batch
        ]
        # Map (capper, date) -> ocr_text so we can attach it to parsed rows
        ocr_lookup = {(capper, date): ocr for _, capper, date, ocr in batch}

        prompt = build_stage1_prompt(picks_to_parse, schedule_data)
        print(f"Calling Sonnet to parse {len(picks_to_parse)} picks...")

        try:
            response = call_sonnet_text(prompt)
            parsed_rows = parse_csv_response(response)
            print(f"Parsed {len(parsed_rows)} pick row(s)")
            # Attach ocr_text as col 8 keyed by (capper, date).
            # One image can produce multiple pick rows (one per bet in the image),
            # so all picks from the same image share the same ocr_text.
            for row in parsed_rows:
                while len(row) < 8:
                    row.append("")
                capper_key = row[1].strip() if len(row) > 1 else ""
                date_key   = row[0].strip() if len(row) > 0 else ""
                row.append(ocr_lookup.get((capper_key, date_key), ""))
            all_parsed_rows.extend(parsed_rows)
            all_processed_row_idxs.extend([row_idx for row_idx, _, _, _ in batch])

        except Exception as e:
            print(f"Stage 1 batch failed: {e}")
            # Mark failed rows
            all_values = sheets_read(image_pull_ws.get_all_values)
            cells_to_update = []
            for row_idx, _, _, _ in batch:
                current_stage = (
                    all_values[row_idx - 1][5] if len(all_values[row_idx - 1]) > 5 else ""
                )
                if current_stage.startswith("parse_failed_attempt_count_"):
                    try:
                        count = int(current_stage.split("_")[-1]) + 1
                    except ValueError:
                        count = 1
                else:
                    count = 1
                cells_to_update.append(
                    gspread.Cell(row_idx, 6, f"parse_failed_attempt_count_{count}")
                )
            if cells_to_update:
                sheets_write(image_pull_ws.update_cells, cells_to_update)
            continue

    if all_parsed_rows:
        # Get or create parsed_picks worksheet
        parsed_picks_ws = get_or_create_picks_worksheet(
            spreadsheet, PARSED_PICKS_SHEET
        )

        # Batch append rows to parsed_picks
        sheets_write(parsed_picks_ws.append_rows, all_parsed_rows, value_input_option="USER_ENTERED")
        for row in all_parsed_rows:
            print(f"  Added: {row[1]} - {row[2]} {row[3]} {row[4]}")

        # Update timestamp in parsed_picks A1
        sheets_write(
            parsed_picks_ws.update_acell,
            "A1", now_eastern.strftime("%Y-%m-%d %H:%M:%S")
        )

    # Batch update successfully processed rows as stage_1_parsed
    if all_processed_row_idxs:
        cells_to_update = [
            gspread.Cell(row_idx, 6, "stage_1_parsed")
            for row_idx in all_processed_row_idxs
        ]
        sheets_write(image_pull_ws.update_cells, cells_to_update)

    # Log activity
    log_activity(
        spreadsheet,
        "process_ocr",
        f"Processed {len(rows_to_process)} rows into {len(all_parsed_rows)} picks",
    )

    print(f"✅ Stage 1 complete: {len(all_parsed_rows)} picks parsed")


def run_stage2(spreadsheet, image_pull_ws,
               team_resolver: TeamResolver = None,
               capper_resolver: CapperResolver = None):
    """Run Stage 2: Finalize parsed picks with Python resolvers (no Claude API).

    Reads rows from parsed_picks, resolves capper names and team names using
    deterministic Python (TeamResolver + CapperResolver), and fills game/spread
    columns from schedule data. Writes to three destinations:

      finalized_picks  — staging sheet (all 9 cols incl. ocr_text); deduped
      master_sheet     — permanent history (cols 0–7, no ocr_text)
      parsed_picks_new — append-only audit sheet (all 9 cols incl. ocr_text)

    Sentinel values when resolution fails:
      - capper = "capper_not_found" → caught by nightly audit
      - game   = "team_not_found"   → caught by nightly audit

    ML/spread deduplication: cappers sometimes post a pick twice — once as ML
    and once as a spread.  deduplicate_ml_vs_spread() drops the ML copy when
    both exist for the same capper+game+date, checking both the incoming batch
    and the existing sheet so the rule holds regardless of arrival order.
    """
    eastern = ZoneInfo("America/New_York")
    utc = ZoneInfo("UTC")
    now_eastern = datetime.now(eastern)
    now_utc = datetime.now(utc)

    # Get parsed_picks worksheet
    try:
        parsed_picks_ws = sheets_read(spreadsheet.worksheet, PARSED_PICKS_SHEET)
    except gspread.WorksheetNotFound:
        print("No parsed_picks sheet found, skipping Stage 2")
        return

    # Get all rows from parsed_picks (row 4+)
    all_values = sheets_read(parsed_picks_ws.get_all_values)
    if len(all_values) < 4:
        print("No rows in parsed_picks to finalize")
        return

    # Row 1 = timestamp, Row 2 = DO NOT EDIT, Row 3 = headers, Row 4+ = data
    data_rows = all_values[3:]  # Starting from row 4
    if not data_rows:
        print("No rows in parsed_picks to finalize")
        return

    print(f"\n── Stage 2: Finalizing {len(data_rows)} parsed pick(s) (Python) ──")

    # Initialize resolvers if not provided (lazy init for reuse across calls)
    if team_resolver is None:
        team_resolver = TeamResolver(spreadsheet)
    if capper_resolver is None:
        capper_resolver = CapperResolver(spreadsheet)

    valid_rows = [row for row in data_rows if row]
    # Preserve ocr_text (col 8) from input rows
    ocr_texts = [row[8] if len(row) > 8 else "" for row in valid_rows]

    # Run Python Stage 2 — no Claude API call needed
    all_finalized_rows = finalize_picks_python(
        team_resolver, capper_resolver, valid_rows
    )
    all_finalized_rows = validate_and_fix_pick_column(all_finalized_rows)

    # Re-attach ocr_text to each finalized row (positional match)
    for j, row in enumerate(all_finalized_rows):
        ocr = ocr_texts[j] if j < len(ocr_texts) else ""
        if len(row) < 9:
            row.append(ocr)
        else:
            row[8] = ocr

    # Report resolution stats
    not_found_teams = sum(1 for r in all_finalized_rows if len(r) > 5 and r[5] == TEAM_NOT_FOUND)
    not_found_cappers = sum(1 for r in all_finalized_rows if len(r) > 1 and r[1] == CAPPER_NOT_FOUND)
    print(f"Finalized {len(all_finalized_rows)} pick row(s)")
    if not_found_teams:
        print(f"  ⚠ {not_found_teams} pick(s) with unresolved team (game=team_not_found)")
    if not_found_cappers:
        print(f"  ⚠ {not_found_cappers} pick(s) with unresolved capper (capper=capper_not_found)")

    if all_finalized_rows:
        # Get or create finalized_picks worksheet
        finalized_picks_ws = get_or_create_picks_worksheet(
            spreadsheet, FINALIZED_PICKS_SHEET
        )

        # Deduplicate: if a spread pick exists for the same capper+game+date,
        # drop any ML pick for that same group (handles both orderings).
        existing_fp_values = sheets_read(finalized_picks_ws.get_all_values)
        # Sheet layout: row 1 = timestamp, row 2 = DO NOT EDIT, row 3 = headers, row 4+ = data
        existing_fp_data = existing_fp_values[3:] if len(existing_fp_values) > 3 else []
        existing_data_start_row = 4  # 1-based sheet row of existing_fp_data[0]

        all_finalized_rows, ml_rows_to_delete = deduplicate_ml_vs_spread(
            all_finalized_rows, existing_fp_data, existing_data_start_row
        )

        # Delete existing ML rows superseded by incoming spread picks (reverse order
        # so row indices stay valid as we delete from bottom up)
        for sheet_row in sorted(ml_rows_to_delete, reverse=True):
            sheets_write(finalized_picks_ws.delete_rows, sheet_row)

        # Batch append rows to finalized_picks
        if all_finalized_rows:
            sheets_write(
                finalized_picks_ws.append_rows,
                all_finalized_rows, value_input_option="USER_ENTERED"
            )
            for row in all_finalized_rows:
                print(f"  Finalized: {row[1]} - {row[5]} | {row[3]} {row[4]}")

        # Tag all rows with source (index 9).
        for row in all_finalized_rows:
            while len(row) < 9:
                row.append("")
            if len(row) < 10:
                row.append(DISCORD_SOURCE)

        # Also append to master_sheet (cols 0-7 + source at 9, strip ocr_text).
        # Filter out totals (O/U lines) — master_sheet is sides-only.
        if all_finalized_rows:
            master_ws = get_or_create_picks_worksheet(spreadsheet, MASTER_SHEET)
            master_rows = [
                row[:8] + [row[9]] for row in all_finalized_rows
                if not _TOTAL_LINE_RE.match(str(row[4]))
            ]
            skipped_totals = len(all_finalized_rows) - len(master_rows)
            if skipped_totals:
                print(f"  Skipped {skipped_totals} total (O/U) rows — not written to master_sheet")
            sheets_write(master_ws.append_rows, master_rows, value_input_option="USER_ENTERED")
            print(f"  Also appended {len(master_rows)} rows to master_sheet")

        # Append full rows (with ocr_text + source) to parsed_picks_new for daily audit
        if all_finalized_rows:
            picks_new_ws = get_or_create_picks_worksheet(spreadsheet, PARSED_PICKS_NEW_SHEET)
            sheets_write(picks_new_ws.append_rows, all_finalized_rows, value_input_option="USER_ENTERED")
            print(f"  Also appended {len(all_finalized_rows)} rows to parsed_picks_new")

        # Update timestamp in finalized_picks A1
        sheets_write(
            finalized_picks_ws.update_acell,
            "A1", now_eastern.strftime("%Y-%m-%d %H:%M:%S")
        )

    # Delete processed rows from parsed_picks (rows 4+) in one batch
    if len(all_values) > 3:
        sheets_write(parsed_picks_ws.delete_rows, 4, len(all_values))

    # Batch update image_pull rows to stage_2_finalized
    image_pull_values = sheets_read(image_pull_ws.get_all_values)
    cells_to_update = []
    for i, row in enumerate(image_pull_values[2:], start=3):
        if len(row) > 5 and row[5] == "stage_1_parsed":
            cells_to_update.append(gspread.Cell(i, 6, "stage_2_finalized"))
    if cells_to_update:
        sheets_write(image_pull_ws.update_cells, cells_to_update)

    # Log activity
    log_activity(
        spreadsheet, "finalize_picks", f"Finalized {len(all_finalized_rows)} picks"
    )

    print(f"✅ Stage 2 complete: {len(all_finalized_rows)} picks finalized")


def backfill_ocr(worksheet):
    """Run OCR on existing rows that are missing OCR text."""
    all_values = sheets_read(worksheet.get_all_values)

    # Find rows needing OCR (row index, url) - skip row 1 (timestamp) and row 2 (header)
    rows_needing_ocr = []
    rows_with_ocr = 0
    for i, row in enumerate(all_values[2:], start=3):  # Start at row 3 (1-indexed)
        if len(row) >= 4 and row[3]:  # Has source ref
            # Skip non-URL source refs (e.g. "telegram:channel:msg_id") for now.
            # TODO (Step 3): implement Telegram backfill by re-downloading via
            # Telethon using the channel_id + msg_id encoded in the source ref.
            if not row[3].startswith("http"):
                continue
            has_ocr = len(row) >= 5 and row[4].strip()
            if not has_ocr:
                rows_needing_ocr.append((i, row[3]))
            else:
                rows_with_ocr += 1

    if not rows_needing_ocr:
        print("No rows need OCR backfill")
        log_activity(
            worksheet.spreadsheet,
            "ocr_images",
            f"No OCR needed, {rows_with_ocr} already processed",
        )
        return

    print(f"Found {len(rows_needing_ocr)} rows needing OCR")

    # Process in batches
    all_ocr_updates = []  # Collect all updates for batch
    for batch_start in range(0, len(rows_needing_ocr), OCR_BATCH_SIZE):
        batch = rows_needing_ocr[batch_start : batch_start + OCR_BATCH_SIZE]
        urls = [url for _, url in batch]

        print(
            f"\nProcessing batch {batch_start // OCR_BATCH_SIZE + 1} ({len(batch)} images)..."
        )
        ocr_results = extract_text_from_images_batch(urls)  # Returns OCR text strings

        # Log OCR batch
        log_activity(
            worksheet.spreadsheet, "ocr_images", f"Completed OCR for {len(batch)} rows"
        )

        # Collect cells to update
        for (row_idx, url), ocr_text in zip(batch, ocr_results):
            print(
                f"  Row {row_idx}: {ocr_text[:60]}..."
                if len(ocr_text) > 60
                else f"  Row {row_idx}: {ocr_text}"
            )
            all_ocr_updates.append(gspread.Cell(row_idx, 5, ocr_text))

    # Batch update all OCR results
    if all_ocr_updates:
        sheets_write(worksheet.update_cells, all_ocr_updates)

    print(f"\n✅ Backfilled OCR for {len(rows_needing_ocr)} rows")


def process_manual_picks_queue(spreadsheet):
    """Process manual picks from the manual_picks_queue sheet.

    Columns in manual_picks_queue (row 3 = headers, row 4+ = data):
    A: timestamp, B: message_sent_at, C: capper_name, D: manual_text,
    E: image_url, F: image (skip), G: ocr_text, H: stage

    Stage values:
    - empty: needs processing
    - about_to_finalize: OCR done, ready for Stage 1/2
    - finalized: fully processed

    This function:
    1. Reads rows where stage is empty
    2. Uses manual_text as OCR text, or runs OCR on image_url
    3. Updates ocr_text and sets stage to "about_to_finalize"
    4. Runs Stage 1 (parse to parsed_picks) on those rows
    5. Runs Stage 2 (finalize with known cappers)
    6. Updates stage to "finalized"
    """
    eastern = ZoneInfo("America/New_York")
    now_eastern = datetime.now(eastern)

    # Get manual_picks_queue worksheet
    try:
        manual_ws = sheets_read(spreadsheet.worksheet, MANUAL_PICKS_QUEUE_SHEET)
    except gspread.WorksheetNotFound:
        print("No manual_picks_queue sheet found, skipping manual processing")
        return

    # Get all rows (row 3 = headers, row 4+ = data)
    all_values = sheets_read(manual_ws.get_all_values)
    if len(all_values) < 4:
        print("No rows in manual_picks_queue to process")
        return

    data_rows = all_values[3:]  # Starting from row 4 (0-indexed: 3)

    # Track stats
    stats = {
        "ocr_count": 0,
        "text_count": 0,
        "stage1_parsed": 0,
        "stage2_finalized": 0,
    }

    # Step 1: Find rows needing OCR/text extraction
    rows_needing_processing = []  # (row_idx, capper, date, manual_text, image_url)
    for i, row in enumerate(data_rows, start=4):  # Row 4 = first data row
        # Pad row to 8 columns
        row = row + [""] * (8 - len(row))
        stage = row[7].strip() if len(row) > 7 else ""

        # Skip already processed rows
        if stage in ("about_to_finalize", "finalized"):
            continue

        capper_name = row[2].strip() if row[2] else "Unknown"
        message_sent_at = row[1].strip() if row[1] else ""
        manual_text = row[3].strip() if row[3] else ""
        image_url = row[4].strip() if row[4] else ""
        # Skip column F (embedded image) for now

        # Must have either manual_text or image_url
        if not manual_text and not image_url:
            continue

        # Extract date from message_sent_at (format: YYYY-MM-DD HH:MM:SS)
        message_date = message_sent_at.split(" ")[0] if message_sent_at else (
            now_eastern.strftime("%Y-%m-%d")
        )

        rows_needing_processing.append(
            (i, capper_name, message_date, manual_text, image_url)
        )

    if not rows_needing_processing:
        print("No rows in manual_picks_queue need processing")
        return

    print(f"\n── Manual Picks Queue: Processing {len(rows_needing_processing)} row(s) ──")

    # Step 2: Process OCR for rows that need it
    rows_needing_ocr = [
        (row_idx, image_url)
        for row_idx, _, _, manual_text, image_url in rows_needing_processing
        if not manual_text and image_url
    ]

    ocr_results = {}  # row_idx -> ocr_text
    if rows_needing_ocr:
        print(f"Running batch OCR on {len(rows_needing_ocr)} image(s)...")
        image_urls = [url for _, url in rows_needing_ocr]

        for batch_start in range(0, len(image_urls), OCR_BATCH_SIZE):
            batch = image_urls[batch_start : batch_start + OCR_BATCH_SIZE]
            batch_row_idxs = [row_idx for row_idx, _ in rows_needing_ocr[batch_start:batch_start + OCR_BATCH_SIZE]]

            print(f"  Processing batch {batch_start // OCR_BATCH_SIZE + 1} ({len(batch)} images)...")
            batch_results = extract_text_from_images_batch(batch)

            for row_idx, ocr_text in zip(batch_row_idxs, batch_results):
                ocr_results[row_idx] = ocr_text
                stats["ocr_count"] += 1

        log_activity(
            spreadsheet,
            "manual_queue_ocr",
            f"Completed OCR for {len(rows_needing_ocr)} images",
        )

    # Step 3: Update ocr_text and stage columns, build Stage 1 input
    cells_to_update = []
    picks_to_parse = []  # (capper, date, ocr_text) for Stage 1

    for row_idx, capper_name, message_date, manual_text, image_url in rows_needing_processing:
        # Determine OCR text
        if manual_text:
            ocr_text = manual_text
            stats["text_count"] += 1
        elif row_idx in ocr_results:
            ocr_text = ocr_results[row_idx]
        else:
            continue  # Skip if no text available

        # Update ocr_text (column G = 7)
        cells_to_update.append(gspread.Cell(row_idx, 7, ocr_text))
        # Update stage (column H = 8)
        cells_to_update.append(gspread.Cell(row_idx, 8, "about_to_finalize"))

        # Add to Stage 1 input
        picks_to_parse.append((capper_name, message_date, ocr_text))

    # Batch update cells
    if cells_to_update:
        sheets_write(manual_ws.update_cells, cells_to_update)
        print(f"Updated {len(cells_to_update)} cells (ocr_text + stage)")

    if not picks_to_parse:
        print("No picks to parse from manual queue")
        return

    # Step 4: Run Stage 1 - Parse OCR to structured picks
    print(f"\n── Manual Queue Stage 1: Parsing {len(picks_to_parse)} pick(s) ──")

    # Get unique dates for schedule
    message_dates = set(date for _, date, _ in picks_to_parse)
    print(f"Fetching schedules for dates: {sorted(message_dates)}")
    schedule_data = fetch_schedule_data(spreadsheet, message_dates)

    all_manual_parsed_rows = []
    for batch_start in range(0, len(picks_to_parse), STAGE_BATCH_SIZE):
        batch = picks_to_parse[batch_start : batch_start + STAGE_BATCH_SIZE]
        # Add row_id for anchoring (use batch index as row_id)
        batch_with_ids = [
            (capper, date, ocr, batch_start + i)
            for i, (capper, date, ocr) in enumerate(batch)
        ]
        # Map (capper, date) -> ocr_text for attaching to parsed rows
        ocr_lookup = {(capper, date): ocr for capper, date, ocr in batch}
        prompt = build_stage1_prompt(batch_with_ids, schedule_data)
        print(f"Calling Sonnet to parse batch of {len(batch)} picks...")
        try:
            response = call_sonnet_text(prompt)
            parsed_rows = parse_csv_response(response)
            print(f"Parsed {len(parsed_rows)} pick row(s)")
            for row in parsed_rows:
                while len(row) < 9:
                    row.append("")
                capper_key = row[1].strip() if len(row) > 1 else ""
                date_key   = row[0].strip() if len(row) > 0 else ""
                row.append(ocr_lookup.get((capper_key, date_key), ""))
            all_manual_parsed_rows.extend(parsed_rows)
        except Exception as e:
            print(f"Manual queue Stage 1 batch failed: {e}")
            continue

    parsed_rows = all_manual_parsed_rows
    stats["stage1_parsed"] = len(parsed_rows)

    if parsed_rows:
        # Get or create parsed_picks worksheet
        parsed_picks_ws = get_or_create_picks_worksheet(
            spreadsheet, PARSED_PICKS_SHEET
        )

        # Batch append rows to parsed_picks
        sheets_write(parsed_picks_ws.append_rows, parsed_rows, value_input_option="USER_ENTERED")
        for row in parsed_rows:
            print(f"  Added: {row[1]} - {row[2]} {row[3]} {row[4]}")

        # Update timestamp in parsed_picks A1
        sheets_write(
            parsed_picks_ws.update_acell,
            "A1", now_eastern.strftime("%Y-%m-%d %H:%M:%S")
        )

    log_activity(
        spreadsheet,
        "manual_queue_stage1",
        f"Parsed {len(picks_to_parse)} rows into {len(parsed_rows)} picks",
    )

    # Step 5: Run Stage 2 - Finalize with Python resolvers (no Claude API)
    print("\n── Manual Queue Stage 2: Finalizing picks (Python) ──")

    try:
        parsed_picks_ws = sheets_read(spreadsheet.worksheet, PARSED_PICKS_SHEET)
    except gspread.WorksheetNotFound:
        print("No parsed_picks sheet found after Stage 1")
        return

    all_parsed = sheets_read(parsed_picks_ws.get_all_values)
    if len(all_parsed) < 4:
        print("No rows in parsed_picks to finalize")
        return

    parsed_data_rows = all_parsed[3:]  # Row 4+
    if not parsed_data_rows:
        print("No rows in parsed_picks to finalize")
        return

    # Initialize resolvers
    team_res = TeamResolver(spreadsheet)
    capper_res = CapperResolver(spreadsheet)

    valid_parsed_rows = [row for row in parsed_data_rows if row]
    # Preserve ocr_text (col 8)
    ocr_texts = [row[8] if len(row) > 8 else "" for row in valid_parsed_rows]

    # Run Python Stage 2 — no Claude API call needed
    all_manual_finalized = finalize_picks_python(
        team_res, capper_res, valid_parsed_rows
    )
    all_manual_finalized = validate_and_fix_pick_column(all_manual_finalized)

    # Re-attach ocr_text positionally
    for j, row in enumerate(all_manual_finalized):
        ocr = ocr_texts[j] if j < len(ocr_texts) else ""
        if len(row) < 9:
            row.append(ocr)
        else:
            row[8] = ocr

    # Report resolution stats
    not_found_teams = sum(1 for r in all_manual_finalized if len(r) > 5 and r[5] == TEAM_NOT_FOUND)
    not_found_cappers = sum(1 for r in all_manual_finalized if len(r) > 1 and r[1] == CAPPER_NOT_FOUND)
    print(f"Finalized {len(all_manual_finalized)} pick row(s)")
    if not_found_teams:
        print(f"  ⚠ {not_found_teams} pick(s) with unresolved team (game=team_not_found)")
    if not_found_cappers:
        print(f"  ⚠ {not_found_cappers} pick(s) with unresolved capper (capper=capper_not_found)")

    finalized_rows = all_manual_finalized
    stats["stage2_finalized"] = len(finalized_rows)

    if finalized_rows:
        # Get or create finalized_picks worksheet
        finalized_picks_ws = get_or_create_picks_worksheet(
            spreadsheet, FINALIZED_PICKS_SHEET
        )

        # Deduplicate: if a spread pick exists for the same capper+game+date,
        # drop any ML pick for that same group (handles both orderings).
        existing_fp_values = sheets_read(finalized_picks_ws.get_all_values)
        existing_fp_data = existing_fp_values[3:] if len(existing_fp_values) > 3 else []
        existing_data_start_row = 4  # 1-based sheet row of existing_fp_data[0]

        finalized_rows, ml_rows_to_delete = deduplicate_ml_vs_spread(
            finalized_rows, existing_fp_data, existing_data_start_row
        )

        # Delete existing ML rows superseded by incoming spread picks
        for sheet_row in sorted(ml_rows_to_delete, reverse=True):
            sheets_write(finalized_picks_ws.delete_rows, sheet_row)

        # Batch append rows to finalized_picks
        if finalized_rows:
            sheets_write(
                finalized_picks_ws.append_rows,
                finalized_rows, value_input_option="USER_ENTERED"
            )
            for row in finalized_rows:
                print(f"  Finalized: {row[1]} - {row[5]} | {row[3]} {row[4]}")

        # Tag all rows with source (index 9). Manual picks queue is always "discord_all_in_one"
        # until multi-source wiring is added in Step 3 of the integration plan.
        for row in finalized_rows:
            while len(row) < 9:
                row.append("")
            if len(row) < 10:
                row.append("discord_all_in_one")

        # Also append to master_sheet (cols 0-7 + source at 9, strip ocr_text).
        # Filter out totals (O/U lines) — master_sheet is sides-only.
        if finalized_rows:
            master_ws = get_or_create_picks_worksheet(spreadsheet, MASTER_SHEET)
            master_rows = [
                row[:8] + [row[9]] for row in finalized_rows
                if not _TOTAL_LINE_RE.match(str(row[4]))
            ]
            skipped_totals = len(finalized_rows) - len(master_rows)
            if skipped_totals:
                print(f"  Skipped {skipped_totals} total (O/U) rows — not written to master_sheet")
            sheets_write(master_ws.append_rows, master_rows, value_input_option="USER_ENTERED")
            print(f"  Also appended {len(master_rows)} rows to master_sheet")

        # Append full rows (with ocr_text + source) to parsed_picks_new for daily audit
        if finalized_rows:
            picks_new_ws = get_or_create_picks_worksheet(spreadsheet, PARSED_PICKS_NEW_SHEET)
            sheets_write(picks_new_ws.append_rows, finalized_rows, value_input_option="USER_ENTERED")
            print(f"  Also appended {len(finalized_rows)} rows to parsed_picks_new")

        # Update timestamp in finalized_picks A1
        sheets_write(
            finalized_picks_ws.update_acell,
            "A1", now_eastern.strftime("%Y-%m-%d %H:%M:%S")
        )

    # Delete processed rows from parsed_picks (rows 4+)
    if len(all_parsed) > 3:
        sheets_write(parsed_picks_ws.delete_rows, 4, len(all_parsed))

    log_activity(
        spreadsheet,
        "manual_queue_stage2",
        f"Finalized {len(finalized_rows)} picks",
    )

    # Step 6: Update stage to "finalized" in manual_picks_queue
    final_cells = []
    for row_idx, _, _, _, _ in rows_needing_processing:
        final_cells.append(gspread.Cell(row_idx, 8, "finalized"))

    if final_cells:
        sheets_write(manual_ws.update_cells, final_cells)

    # Print summary
    print("\n── Manual Queue Summary ──")
    print(f"  Text entries used: {stats['text_count']}")
    print(f"  Images OCR'd: {stats['ocr_count']}")
    print(f"  Picks parsed (Stage 1): {stats['stage1_parsed']}")
    print(f"  Picks finalized (Stage 2): {stats['stage2_finalized']}")

    log_activity(
        spreadsheet,
        "manual_queue_complete",
        f"Text: {stats['text_count']}, OCR: {stats['ocr_count']}, Parsed: {stats['stage1_parsed']}, Finalized: {stats['stage2_finalized']}",
    )

    # Update timestamp in manual_picks_queue A1
    sheets_write(manual_ws.update_acell, "A1", now_eastern.strftime("%Y-%m-%d %H:%M:%S"))

    print("✅ Manual picks queue processing complete")


def cleanup_old_rows(spreadsheet, image_pull_ws):
    """No-op: Row cleanup disabled. Schedule and image_pull rows are retained."""
    print("\n── Cleanup: Disabled (rows retained in schedule sheets and image_pull) ──")


def main():
    eastern = ZoneInfo("America/New_York")
    utc = ZoneInfo("UTC")
    now_eastern = datetime.now(eastern)
    now_utc = datetime.now(utc)
    timestamp = now_eastern.strftime("%Y-%m-%d %H:%M:%S")
    print(f"\n[{timestamp}] Fetching Discord images...")
    print("Claude API usage tracking: Starting run (tokens: 0, cost: $0.00)")

    try:
        # Get the spreadsheet and worksheet
        client = get_gspread_client()
        spreadsheet = client.open_by_key(GOOGLE_SHEET_ID)
        worksheet = sheets_read(spreadsheet.worksheet, WORKSHEET_NAME)

        # Get the last run timestamp from A1
        last_run = get_last_run_timestamp(worksheet)
        if last_run:
            print(f"Last run: {last_run.strftime('%Y-%m-%d %H:%M:%S')} UTC")
        else:
            print("No previous run timestamp found, fetching all recent images")

        # Get all messages with images since last run
        messages_with_images = get_messages_with_images_since(last_run)

        # Log Discord query
        log_activity(
            spreadsheet,
            "query_discord",
            f"Found {len(messages_with_images)} new images",
        )

        if messages_with_images:
            print(f"Found {len(messages_with_images)} new message(s) with images")

            existing_urls = get_existing_urls(worksheet)

            # Filter out duplicates and limit to MAX_MESSAGES_PER_RUN
            to_process = []
            for (
                image_url,
                message_sent_at,
                capper_name,
                message_content,
                _,
            ) in messages_with_images:
                if image_url in existing_urls:
                    print(f"Skipping duplicate: {image_url[:60]}...")
                    continue
                to_process.append(
                    (image_url, message_sent_at, capper_name, message_content)
                )
                if len(to_process) >= MAX_MESSAGES_PER_RUN:
                    print(f"Limiting to {MAX_MESSAGES_PER_RUN} messages per run")
                    break

            if to_process:
                # Check which URLs already have OCR to avoid re-processing
                urls_with_ocr = get_urls_with_ocr(worksheet)
                needs_ocr = [
                    (url, sent, name, content)
                    for url, sent, name, content in to_process
                    if url not in urls_with_ocr
                ]

                # Batch OCR - just extract text (capper comes from Discord embed)
                ocr_results = []  # List of OCR text strings
                if needs_ocr:
                    image_urls_for_ocr = [img[0] for img in needs_ocr]
                    print(f"\nRunning batch OCR on {len(image_urls_for_ocr)} images...")
                    for i in range(0, len(image_urls_for_ocr), OCR_BATCH_SIZE):
                        batch_urls = image_urls_for_ocr[i : i + OCR_BATCH_SIZE]
                        print(
                            f"  Processing batch {i // OCR_BATCH_SIZE + 1} ({len(batch_urls)} images)..."
                        )
                        ocr_results.extend(extract_text_from_images_batch(batch_urls))
                        # Log OCR batch
                        log_activity(
                            spreadsheet,
                            "ocr_images",
                            f"Completed OCR for {len(batch_urls)} rows",
                        )
                    print(f"OCR complete. Got {len(ocr_results)} results.")

                # Insert rows (now with 6 columns including committed_stage)
                # Collect all rows to insert
                rows_to_insert = []
                for idx, (
                    image_url,
                    message_sent_at,
                    capper_name,
                    message_content,
                ) in enumerate(to_process):
                    print(f"\nProcessing: {capper_name} @ {message_sent_at} ET")
                    print(f"URL: {image_url[:80]}...")

                    # Get OCR text
                    ocr_text = ""
                    if image_url in urls_with_ocr:
                        print("OCR already exists, skipping")
                    else:
                        # Find index in needs_ocr list
                        try:
                            ocr_idx = [x[0] for x in needs_ocr].index(image_url)
                            if ocr_idx < len(ocr_results):
                                ocr_text = ocr_results[ocr_idx]
                            print(
                                f"OCR: {ocr_text[:100]}..."
                                if len(ocr_text) > 100
                                else f"OCR: {ocr_text}"
                            )
                        except (ValueError, IndexError):
                            pass

                    # Capper always comes from Discord embed description (### heading)
                    # Fall back to regex extraction from OCR text only if embed didn't have it
                    if capper_name != "UNKNOWN":
                        final_capper = capper_name
                    else:
                        # Last resort: try regex extraction from OCR text
                        extracted_capper = extract_capper_from_ocr(ocr_text)
                        final_capper = (
                            extracted_capper if extracted_capper else "UNKNOWN"
                        )
                    print(f"Capper: {final_capper}")

                    rows_to_insert.append(
                        [
                            timestamp,
                            message_sent_at,
                            final_capper,
                            image_url,
                            ocr_text,
                            "",
                        ]
                    )
                    existing_urls.add(image_url)

                # Batch insert all rows
                if rows_to_insert:
                    next_row = len(sheets_read(worksheet.get_all_values)) + 1
                    end_row = next_row + len(rows_to_insert) - 1

                    # Expand sheet if needed
                    if end_row > worksheet.row_count:
                        new_row_count = end_row + 100  # Add buffer
                        print(f"Expanding sheet from {worksheet.row_count} to {new_row_count} rows")
                        sheets_write(worksheet.resize, rows=new_row_count)

                    sheets_write(
                        worksheet.update,
                        range_name=f"A{next_row}:F{end_row}",
                        values=rows_to_insert,
                        value_input_option="USER_ENTERED",
                    )
                    print(f"\n✅ Inserted {len(rows_to_insert)} rows to sheet")

                print(f"\n✅ Processed {len(to_process)} new message(s).")
            else:
                print("All images already exist in sheet")
        else:
            print("No new messages with images found since last run")

        # Update the last run timestamp
        update_last_run_timestamp(worksheet, now_utc)

        # Backfill OCR for any existing rows that don't have it
        backfill_ocr(worksheet)

        # Log OCR cost (before Stage 1/2 parsing)
        ocr_tokens = CLAUDE_USAGE["input_tokens"] + CLAUDE_USAGE["output_tokens"]
        ocr_cost = get_claude_cost()
        if ocr_tokens > 0:
            print("\n── OCR Processing Cost ──")
            print(f"  Tokens: {ocr_tokens:,} | Cost: ${ocr_cost:.4f}")
            log_activity(
                spreadsheet,
                "process_ocr",
                f"Tokens: {ocr_tokens:,} | Cost: ${ocr_cost:.4f}",
            )

        # Run Stage 1: Parse OCR to structured picks
        run_stage1(spreadsheet, worksheet)

        # Run Stage 2: Finalize picks
        run_stage2(spreadsheet, worksheet)

        # Process manual picks queue
        process_manual_picks_queue(spreadsheet)

        # Run cleanup: delete old rows from schedules and image_pull
        cleanup_old_rows(spreadsheet, worksheet)

        # Log Claude API usage summary
        total_tokens = CLAUDE_USAGE["input_tokens"] + CLAUDE_USAGE["output_tokens"]
        total_cost = get_claude_cost()
        print("\n── Claude API Usage Summary ──")
        print(f"  Input tokens:  {CLAUDE_USAGE['input_tokens']:,}")
        print(f"  Output tokens: {CLAUDE_USAGE['output_tokens']:,}")
        print(f"  Total tokens:  {total_tokens:,}")
        print(f"  Estimated cost: ${total_cost:.4f}")

        # Log to activity sheet
        log_activity(
            spreadsheet,
            "claude_usage",
            f"Tokens: {total_tokens:,} | Cost: ${total_cost:.4f}",
        )

        # Populate results for any picks with completed game scores
        print("\n── Populate Results ──")
        try:
            scores = populate_results.load_scores(spreadsheet)
            for sheet_name, header_row_index in [(populate_results.MASTER_SHEET, 0), (populate_results.PICKS_NEW_SHEET, 2)]:
                resolved, skipped_no_score, skipped_already = populate_results.process_sheet(
                    spreadsheet, sheet_name, header_row_index, scores, dry_run=False
                )
                print(f"  {sheet_name}: {resolved} results filled, {skipped_already} already set, {skipped_no_score} no score yet")
            sync_master_to_csv(spreadsheet)
        except Exception as e:
            print(f"  populate_results error (non-fatal): {e}")

        # Push CSV — no-op if nothing changed vs origin/main
        print("\n── Git Commit & Push ──")
        git_push_csv(LOCAL_CSV_PATH, "Auto-append picks from Discord")

        # Brief pause to let Sheets read quota window reset before audit
        print("\n  [quota cooldown] sleeping 30s before audit...")
        time.sleep(30)

        # Daily hallucination audit (Opus pass only fires within 15 min after midnight PST)
        print("\n── Daily Audit ──")
        daily_audit.run_audit(spreadsheet)

    except ValueError as e:
        print(f"Error: {e}")
        exit(1)


if __name__ == "__main__":
    main()
