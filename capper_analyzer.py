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
    GOOGLE_SHEET_ID, get_gspread_client, sheets_read, sheets_write,
    get_schedule_for_date,
)
import daily_audit
import populate_results
from discord_fetcher import get_messages_with_images_since

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
NBA_SCHEDULE_SHEET = "nba_schedule"
CBB_SCHEDULE_SHEET = "cbb_schedule"
NHL_SCHEDULE_SHEET = "nhl_schedule"
MANUAL_PICKS_QUEUE_SHEET = "manual_picks_queue"

# CSV columns for picks
PICKS_COLUMNS = [
    "date",
    "capper",
    "sport",
    "pick",
    "line",
    "game",
    "spread",
    "side",
    "result",
    "ocr_text",
    "source",   # index 10 — "discord_all_in_one" or "telegram"
]

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

# Claude API usage tracking (Sonnet 4.6 pricing: $3.00/M input, $15.00/M output)
# NOTE: If daily pick volume grows dramatically (e.g. 10x current volume causing
# token counts to spike), consider switching back to Haiku ($0.80/$4.00 per M) to
# keep costs in check. At current volumes (~20-100 picks/day) Sonnet's quality
# improvement is worth the ~3.75x price difference.
CLAUDE_USAGE = {"input_tokens": 0, "output_tokens": 0}
SONNET_INPUT_COST_PER_M = 3.00
SONNET_OUTPUT_COST_PER_M = 15.00

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




def get_claude_cost():
    """Calculate estimated cost from token usage."""
    input_cost = (CLAUDE_USAGE["input_tokens"] / 1_000_000) * SONNET_INPUT_COST_PER_M
    output_cost = (CLAUDE_USAGE["output_tokens"] / 1_000_000) * SONNET_OUTPUT_COST_PER_M
    return input_cost + output_cost


def log_claude_usage(message):
    """Track token usage from a Claude API response."""
    if hasattr(message, "usage"):
        CLAUDE_USAGE["input_tokens"] += message.usage.input_tokens
        CLAUDE_USAGE["output_tokens"] += message.usage.output_tokens


# Example rows for prompts (spread and ML per sport - NO totals)
# Includes examples for tricky formats: Porter Picks "O opponent" and Analytics Capper "v opponent"
EXAMPLE_PICKS_ROWS = """2026-02-01,BEEZO WINS,CBB,Iowa State Cyclones,-11.5,Iowa State Cyclones vs Kansas State Wildcats,,Iowa State Cyclones,
2026-02-01,DARTH FADER,NBA,LA Clippers,+2,LA Clippers @ Phoenix Suns,,LA Clippers,
2026-02-01,A11 BETS,NBA,LA Clippers,ML,LA Clippers @ Phoenix Suns,,LA Clippers,
2026-02-03,ANALYTICS CAPPER,NHL,Philadelphia Flyers,ML,Washington Capitals @ Philadelphia Flyers,,Philadelphia Flyers,
2026-02-04,PORTER PICKS,CBB,Alabama Crimson Tide,-8,Alabama Crimson Tide vs Texas A&M Aggies,,Alabama Crimson Tide,
2026-02-03,HAMMERING HANK,NBA,Brooklyn Nets,+8.5,Los Angeles Lakers @ Brooklyn Nets,,Brooklyn Nets,
2026-02-01,HAMMERING HANK,CBB,Florida Gators,-8.5,Florida Gators vs Alabama Crimson Tide,,Florida Gators,"""

EXAMPLE_FINALIZED_ROWS = """2026-02-01,BEEZO WINS,CBB,Iowa State Cyclones,-11.5,Iowa State Cyclones vs Kansas State Wildcats,,Iowa State Cyclones,
2026-02-01,DARTH FADER,NBA,LA Clippers,+2,LA Clippers @ Phoenix Suns,,LA Clippers,
2026-02-03,ANALYTICS CAPPER,NHL,Philadelphia Flyers,ML,Washington Capitals @ Philadelphia Flyers,,Philadelphia Flyers,
2026-02-04,PORTER PICKS,CBB,Alabama Crimson Tide,-8,Alabama Crimson Tide vs Texas A&M Aggies,,Alabama Crimson Tide,
2026-02-01,HAMMERING HANK,CBB,Florida Gators,-8.5,Florida Gators vs Alabama Crimson Tide,,Florida Gators,"""


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


def lookup_spread_from_schedule(
    pick_team: str, date: str, sport: str, schedule_games: List[dict]
) -> str:
    """Look up the consensus spread from the schedule for a pick.

    Args:
        pick_team: The team name from the pick column
        date: Game date (unused here but kept for clarity — games already filtered)
        sport: Sport code (unused here — games already filtered by sport)
        schedule_games: List of game dicts from get_schedule_for_date()

    Returns:
        The spread string from the schedule (e.g. "Duke Blue Devils -8"), or ""
        if no matching game found.
    """
    pt = pick_team.lower().strip()
    for game in schedule_games:
        away = game.get("away_team", "")
        home = game.get("home_team", "")
        if (pt and away and home and
            (pt in away.lower() or away.lower() in pt or
             pt in home.lower() or home.lower() in pt)):
            return game.get("spread", "")
    return ""


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

    # Download and encode all images
    # Claude's 5MB limit applies to base64-encoded data, which is ~33% larger than raw bytes
    # So we limit raw bytes to 3.5MB which becomes ~4.7MB after base64 encoding
    MAX_IMAGE_SIZE = (
        3.5 * 1024 * 1024
    )  # 3.5MB raw = ~4.7MB base64 (under Claude's 5MB limit)

    image_contents = []
    skipped_indices = set()  # Track which images were skipped
    processed_count = 0

    for i, url in enumerate(image_urls):
        response = requests.get(url)
        response.raise_for_status()

        content_type = response.headers.get("content-type", "image/jpeg")
        media_type = get_media_type(url, content_type)
        image_bytes = response.content

        # Resize if image is too large
        if len(image_bytes) > MAX_IMAGE_SIZE:
            original_size_mb = len(image_bytes) / (1024 * 1024)
            print(
                f"  ⚠️ Image {i + 1} exceeds 3.5MB ({original_size_mb:.2f}MB): {url[:100]}..."
            )
            img = Image.open(io.BytesIO(image_bytes))
            # Convert to RGB if necessary (for PNG with alpha)
            if img.mode in ("RGBA", "P"):
                img = img.convert("RGB")

            # Reduce quality/size until under limit
            quality = 85
            attempts = 0
            max_attempts = 20  # Safety limit
            while len(image_bytes) > MAX_IMAGE_SIZE and attempts < max_attempts:
                attempts += 1
                buffer = io.BytesIO()
                img.save(buffer, format="JPEG", quality=quality, optimize=True)
                image_bytes = buffer.getvalue()

                if len(image_bytes) <= MAX_IMAGE_SIZE:
                    break

                # Reduce quality first
                if quality > 20:
                    quality -= 10
                else:
                    # Quality is already low, reduce dimensions
                    img = img.resize(
                        (img.width * 3 // 4, img.height * 3 // 4),
                        Image.Resampling.LANCZOS,
                    )
                    quality = 50  # Reset quality after resize

            new_size_mb = len(image_bytes) / (1024 * 1024)
            if len(image_bytes) > MAX_IMAGE_SIZE:
                print(
                    f"    ⚠️ Could not compress below 3.5MB ({new_size_mb:.2f}MB), skipping image"
                )
                skipped_indices.add(i)
                continue
            print(f"    Compressed to {new_size_mb:.2f}MB (quality={quality})")
            media_type = "image/jpeg"

        image_data = base64.standard_b64encode(image_bytes).decode("utf-8")
        processed_count += 1

        image_contents.append(
            {
                "type": "image",
                "source": {
                    "type": "base64",
                    "media_type": media_type,
                    "data": image_data,
                },
            }
        )
        image_contents.append({"type": "text", "text": f"[Image {processed_count}]"})

    # If all images were skipped, return empty strings
    if processed_count == 0:
        return [""] * len(image_urls)

    # Simple OCR prompt - just read text
    # Note: "Extract all text" phrasing causes the github_copilot-routed model
    # to respond as if no images were attached. Use "Read and transcribe" instead.
    prompt_text = f"""Read and transcribe every word visible in each of the {processed_count} images above.
For each image, output in this exact format:

[Image 1]
<transcribed text here>

[Image 2]
<transcribed text here>

...and so on. Preserve the layout of each image's text as much as possible."""

    image_contents.append({"type": "text", "text": prompt_text})

    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

    message = client.messages.create(
        model="claude-haiku-4-5-20251001",
        max_tokens=4096,
        messages=[
            {
                "role": "user",
                "content": image_contents,
            }
        ],
    )

    # Track token usage
    log_claude_usage(message)

    # Parse the response to extract text for each image
    response_text = message.content[0].text
    ocr_results = []

    # Split by [Image N] markers
    parts = re.split(r"\[Image \d+\]\s*", response_text)
    # First part is empty or intro text, skip it
    for part in parts[1:]:
        ocr_results.append(part.strip())

    # Pad with empty strings if we got fewer results than processed images
    while len(ocr_results) < processed_count:
        ocr_results.append("")

    # Now rebuild full results list, inserting empty strings for skipped images
    results = []
    ocr_idx = 0
    for i in range(len(image_urls)):
        if i in skipped_indices:
            results.append("")
        else:
            results.append(ocr_results[ocr_idx] if ocr_idx < len(ocr_results) else "")
            ocr_idx += 1

    return results


# ── Pick Parsing (Stage 1 & Stage 2) ─────────────────────────────────────────


def call_sonnet_text(prompt: str, max_tokens: int = 8192) -> str:
    """Call Claude Sonnet with a text-only prompt."""
    if not ANTHROPIC_API_KEY:
        raise ValueError("ANTHROPIC_API_KEY environment variable not set")

    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

    message = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=max_tokens,
        messages=[{"role": "user", "content": prompt}],
    )

    # Track token usage
    log_claude_usage(message)

    return message.content[0].text


def build_stage1_prompt(
    picks_to_parse: List[Tuple[str, str, str, int]], schedule_data: dict
) -> str:
    """Build the Stage 1 parsing prompt.

    Args:
        picks_to_parse: List of (capper_name, message_date, ocr_text, row_id) tuples
        schedule_data: Dict with 'nba', 'cbb', 'nhl' schedule strings

    Returns:
        The full prompt string
    """
    picks_section = ""
    for capper, date, ocr_text, row_id in picks_to_parse:
        picks_section += f"\n[ROW:{row_id}] [Capper: {capper}, Date: {date}]\n{ocr_text}\n"

    prompt = f"""Parse the following betting picks from OCR text into CSV rows.

OUTPUT FORMAT (one row per pick, comma-separated):
date,capper,sport,pick,line,game,spread,side,result

COLUMN DEFINITIONS:
- date: YYYY-MM-DD format (use the message date provided with each pick)
- capper: Name of the person making the pick (provided with each pick)
- sport: NBA, CBB, or NHL only. Normalize NCAAB to CBB.
- pick: A SINGLE team name (the team being bet on). NEVER use "Team A @ Team B" format. Use the schedule to resolve abbreviations (e.g., "TROY -6.5" means bet on "Troy Trojans").
- line: ONLY the spread number or "ML". Strip all extra text (odds, units, opponent names).
- game: Leave empty for now
- spread: Leave empty for now
- side: Leave empty for now
- result: Leave empty

ROW ATTRIBUTION (CRITICAL):
- Each OCR block is tagged [ROW:N] — ONLY parse picks that appear in that block
- NEVER invent picks, copy picks from other rows, or use the schedule to fabricate bets
- If an OCR block is unreadable or contains no valid picks, output nothing for that row
- Do NOT hallucinate picks that are not explicitly present in the OCR text

COMMON PARSING PATTERNS (may appear with ANY capper):
- "-8 O Texas A&M 6-UNITS" format: The "O" means "over" (against opponent), NOT an over/under total. Extract ONLY the spread: line="-8"
- "ML -130 v Capitals 5u POTD" format: Extra text after bet type. Extract ONLY: line="ML". Ignore odds (-130), opponent references (v Capitals), and unit sizes (5u).
- Any "v ", "v.", or "vs" followed by a team name is context to ignore, not part of the line.
- NHL "3-way", "3way", "3-way ML", "3way moneyline", "3 way ml" = regulation win bet — treat as line=ML. This is NOT a parlay.
- "MI" after a team name is a common OCR misread of "ML" — treat it as ML (e.g. "Kentucky MI (-140)" = Kentucky ML, "New Mexico St MI (-140)" = New Mexico St ML). "MI"/"ML" is the bet type, NOT a second team — "Team MI" on one line = one pick.

ABBREVIATION RESOLUTION (MANDATORY):
- The pick column MUST contain the FULL official team name from the schedule (e.g., "Oklahoma City Thunder" not "OKC")
- ALWAYS resolve abbreviations using the schedule provided below
- Common examples: BKN=Brooklyn Nets, CHI=Chicago Bulls, NO/NOP=New Orleans Pelicans, MEM=Memphis Grizzlies, IND=Indiana Pacers, CBJ=Columbus Blue Jackets, EDM=Edmonton Oilers, OKC=Oklahoma City Thunder, PHI=Philadelphia 76ers/Flyers

TOTAL BET DETECTION (SKIP THESE ENTIRELY):
- O or U followed by a number with OR without space: O 220.5, O220.5, O5.5, U5.5, U 150
- These are total bets - do NOT include them in output

CRITICAL - PICK COLUMN MUST BE:
- A single FULL team name like "Troy Trojans" or "Oklahoma City Thunder"
- NEVER a game format like "Georgia Southern Eagles @ Troy Trojans"
- NEVER an abbreviation like "OKC" or "BKN"

NEVER INVERT PICKS (CRITICAL):
- The pick MUST be the EXACT team mentioned in the text - NEVER the opponent
- Under a "Fades:" header, if text says "Virginia +8", pick = Virginia Cavaliers, line = +8. Do NOT pick Duke (the fade target).
- Under a "Fades:" header, if text says "Houston +3", pick = Houston Cougars, line = +3. Do NOT pick Arizona (the fade target).
- Keep the line sign EXACTLY as written (+8 stays +8, -7 stays -7)
- Do NOT "correct" picks based on who is favored in the schedule
- Do NOT interpret "Fades", "Fade", or "Against" labels to mean bet the opponent
- ALWAYS record the team that is explicitly named in the OCR text

FILTERING RULES - ONLY INCLUDE:
- Sports: NBA, NHL, CBB (college basketball) ONLY. Skip ATP, NFL, soccer, etc.
- Bet types: Spread or Moneyline (ML) ONLY
- Skip: Totals (O/U), player props, team totals, first half bets, quarter bets, parlays, live bets

EXAMPLE ROWS (note pick column is always a single FULL team name):
{EXAMPLE_PICKS_ROWS}

TODAY'S SCHEDULE (use to resolve team name abbreviations):
NBA: {schedule_data.get("nba", "No games")}
CBB: {schedule_data.get("cbb", "No games")}
NHL: {schedule_data.get("nhl", "No games")}

PICKS TO PARSE:
{picks_section}

OUTPUT (CSV rows only, no headers, no explanation):"""

    return prompt


def build_stage2_prompt(
    rows_to_finalize: List[str],
    schedule_data: dict,
    known_cappers: Optional[List[str]] = None,
) -> str:
    """Build the Stage 2 finalization prompt.

    Args:
        rows_to_finalize: List of CSV row strings to finalize
        schedule_data: Dict with 'nba', 'cbb', 'nhl' schedule strings
        known_cappers: Optional list of known capper names for normalization

    Returns:
        The full prompt string
    """
    rows_section = "\n".join(rows_to_finalize)

    # Build known cappers section if provided
    cappers_section = ""
    if known_cappers:
        cappers_section = f"""
KNOWN CAPPERS (normalize capper names to these if similar):
{chr(10).join(f"- {c}" for c in known_cappers)}

CAPPER NORMALIZATION RULES:
- If input capper name is similar to a known capper (case-insensitive, partial match, or close spelling), use the EXACT known capper name
- Examples: "anthony walters" → "Anthony Walters", "A. Walters" → "Anthony Walters", "walters" → "Anthony Walters"
- If no match found, keep the original capper name (properly capitalized)
"""

    prompt = f"""Finalize these parsed betting picks by filling in the 'game' and 'side' columns based on the scheduled games. Leave the 'spread' column EMPTY — it will be filled automatically by a post-processing step.

COLUMN ORDER: date,capper,sport,pick,line,game,spread,side,result
{cappers_section}

CRITICAL RULES:
1. FIX pick column: If pick contains "@" (game format) OR is an abbreviation, it's WRONG. Use FULL team name:
   - Line "TROY -6.5" → pick should be "Troy Trojans" (find in schedule)
   - Line "OKC -5" → pick should be "Oklahoma City Thunder"
   - pick "BKN" → should be "Brooklyn Nets"
   - pick "CBJ" → should be "Columbus Blue Jackets"
   - Line "ML" → use the game column to identify which team, use FULL name

2. game: "away_team @ home_team" using EXACT team names from schedule columns C and D

3. spread: ALWAYS leave empty. A Python post-pass fills this from the schedule.

4. side: Copy the corrected pick value (FULL team name). For ML bets, side MUST match pick.

5. For ML bets: pick=team name, side=team name (same as pick), spread=empty

ABBREVIATION RESOLUTION (MANDATORY):
- pick and side columns MUST contain FULL official team names from the schedule
- NEVER leave abbreviations like OKC, BKN, CHI, CBJ, EDM, NO, MEM, IND in pick or side
- Resolve using the schedules below

VALIDATION:
- pick column must NEVER contain "@" or be an abbreviation
- pick and side should BOTH have the FULL team name
- spread column must be EMPTY (will be filled by Python)
- side should match pick exactly

NEVER INVERT PICKS (CRITICAL):
- The pick MUST match the original team from Stage 1 - NEVER switch to the opponent
- Under a "Fades:" header, if Stage 1 says pick="Virginia Cavaliers" line="+8", keep it as Virginia Cavaliers +8. Do NOT flip to Duke (the fade target).
- Under a "Fades:" header, if Stage 1 says pick="Houston Cougars" line="+3", keep it as Houston Cougars +3. Do NOT flip to Arizona (the fade target).
- Do NOT "correct" the pick based on who is favored in the schedule
- Do NOT flip underdog/favorite - keep the exact team and line from input
- The side column should match the pick column exactly

NBA SCHEDULE:
{schedule_data.get("nba", "No games")}

NHL SCHEDULE:
{schedule_data.get("nhl", "No games")}

CBB SCHEDULE:
{schedule_data.get("cbb", "No games")}

EXAMPLE CORRECTLY FINALIZED ROWS:
{EXAMPLE_FINALIZED_ROWS}

ROWS TO FINALIZE:
{rows_section}

OUTPUT (finalized CSV rows only, one per line, no headers, no explanation):"""

    return prompt


def parse_csv_response(response: str) -> List[List[str]]:
    """Parse CSV response from Haiku into list of row lists."""
    rows = []
    for line in response.strip().split("\n"):
        line = line.strip()
        if not line or line.startswith("date,"):  # Skip empty or header lines
            continue
        # Simple CSV parsing (handles basic cases)
        # Use csv module for more robust parsing
        try:
            reader = csv.reader(io.StringIO(line))
            for row in reader:
                if len(row) >= 5:  # At least date, capper, sport, pick, line
                    # Pad to 9 columns if needed
                    while len(row) < 9:
                        row.append("")
                    rows.append(row[:9])
        except Exception:
            continue
    return rows


def validate_and_fix_pick_column(rows: List[List[str]]) -> List[List[str]]:
    """Fix any rows where pick column incorrectly contains game format (Team A @ Team B).

    The pick column should be a single team name, not a game format.
    If pick contains '@', we try to extract the correct team from the side or game column.
    For ML bets, pick and side should both have the team name.
    """
    fixed_rows = []
    for row in rows:
        # Columns: date(0), capper(1), sport(2), pick(3), line(4), game(5), spread(6), side(7), result(8)
        pick = row[3] if len(row) > 3 else ""
        line = row[4] if len(row) > 4 else ""
        game = row[5] if len(row) > 5 else ""
        side = row[7] if len(row) > 7 else ""

        line_upper = line.upper().strip()

        # If pick contains '@', it's a game format - try to fix it
        if "@" in pick:
            fixed_team = None

            # For ML bets, use side if it has a valid team name
            if line_upper == "ML":
                if side and "@" not in side:
                    fixed_team = side
            else:
                # Try to extract team name from line column
                # Line formats: "TROY -6.5", "OKC -5", "PHI -135"
                abbrev_match = re.match(r"^([A-Z]{2,5})\s*[+-]", line_upper)
                if abbrev_match:
                    abbrev = abbrev_match.group(1)
                    # Try to find matching team in the game column
                    if game:
                        teams = game.split(" @ ")
                        for team in teams:
                            # Check if abbreviation matches team name
                            team_words = team.upper().split()
                            for word in team_words:
                                if abbrev in word or word.startswith(abbrev):
                                    fixed_team = team
                                    break
                            if fixed_team:
                                break

            # Apply the fix
            if fixed_team:
                row[3] = fixed_team  # Fix the pick column
                if len(row) > 7:
                    row[7] = fixed_team  # Fix side too

        # For ML bets, ensure side matches pick (if pick is valid)
        if line_upper == "ML" and len(row) > 7:
            if row[3] and "@" not in row[3] and not row[7]:
                row[7] = row[3]  # Copy pick to side

        fixed_rows.append(row)

    return fixed_rows


def deduplicate_ml_vs_spread(
    new_rows: List[List[str]],
    existing_rows: Optional[List[List[str]]] = None,
    existing_row_offset: int = 0,
) -> Tuple[List[List[str]], List[int]]:
    """Drop ML picks when a spread pick exists for the same capper+game+date.

    Handles both orderings:
    - Spread is new, ML already exists in the sheet → returns sheet row indices to delete
    - ML is new, spread already exists in the sheet → filters ML from new_rows

    Args:
        new_rows: Rows about to be appended (each is a list of column values).
        existing_rows: All data rows already in finalized_picks (list of lists).
            Pass None (or []) if not available / not needed.
        existing_row_offset: The 1-based sheet row index of existing_rows[0].
            e.g. if the sheet has a 3-row header and data starts at row 4, pass 4.

    Returns:
        (filtered_new_rows, existing_indices_to_delete)
        - filtered_new_rows: new_rows with ML entries removed where a spread exists
        - existing_indices_to_delete: 1-based sheet row indices of existing ML rows
          to delete because a spread pick is arriving in new_rows
    """
    # Columns: date(0), capper(1), sport(2), pick(3), line(4), game(5), ...
    def is_spread(line: str) -> bool:
        """Return True if line is a spread (not ML, not O/U total)."""
        l = line.strip().upper()
        if not l or l == "ML":
            return False
        # Totals: "O 145.5" or "U 167.5"
        if l.startswith("O ") or l.startswith("U "):
            return False
        # Spread: starts with +/- and a number, or just a number
        return bool(re.match(r"^[+-]?\d", l))

    def row_key(row: List[str]) -> tuple:
        """(date, capper_upper, game_upper) — case-insensitive matching."""
        date = row[0].strip() if len(row) > 0 else ""
        capper = row[1].strip().upper() if len(row) > 1 else ""
        game = row[5].strip().upper() if len(row) > 5 else ""
        return (date, capper, game)

    existing_rows = existing_rows or []

    # Build a set of group keys that have a spread in the existing sheet
    existing_spread_keys: set = set()
    for row in existing_rows:
        if len(row) > 4 and is_spread(row[4]):
            existing_spread_keys.add(row_key(row))

    # Build a set of group keys that have a spread in the incoming new rows
    new_spread_keys: set = set()
    for row in new_rows:
        if len(row) > 4 and is_spread(row[4]):
            new_spread_keys.add(row_key(row))

    # 1. Filter new_rows: drop ML rows whose group already has a spread
    #    (either in existing sheet or arriving in the same batch)
    all_spread_keys = existing_spread_keys | new_spread_keys
    filtered_new: List[List[str]] = []
    for row in new_rows:
        line = row[4].strip().upper() if len(row) > 4 else ""
        if line == "ML" and row_key(row) in all_spread_keys:
            print(
                f"  [dedup] Dropping ML pick for {row[1]} - {row[5]} "
                f"(spread exists for this game)"
            )
            continue
        filtered_new.append(row)

    # 2. Find existing ML rows to delete because a spread is arriving in new_rows
    existing_indices_to_delete: List[int] = []
    for i, row in enumerate(existing_rows):
        line = row[4].strip().upper() if len(row) > 4 else ""
        if line == "ML" and row_key(row) in new_spread_keys:
            sheet_row = existing_row_offset + i
            print(
                f"  [dedup] Marking existing ML row {sheet_row} for deletion: "
                f"{row[1]} - {row[5]} (spread arriving in new batch)"
            )
            existing_indices_to_delete.append(sheet_row)

    return filtered_new, existing_indices_to_delete


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

    Each parsed row gets ocr_text attached as col 10 via an ocr_lookup dict
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

        all_nba_games = []
        all_cbb_games = []
        all_nhl_games = []

        for msg_date in sorted(message_dates):
            all_nba_games.extend(
                get_schedule_for_date(spreadsheet, NBA_SCHEDULE_SHEET, msg_date)
            )
            all_cbb_games.extend(
                get_schedule_for_date(spreadsheet, CBB_SCHEDULE_SHEET, msg_date)
            )
            all_nhl_games.extend(
                get_schedule_for_date(spreadsheet, NHL_SCHEDULE_SHEET, msg_date)
            )

        schedule_data = {
            "nba": format_schedule_for_prompt(all_nba_games, "NBA"),
            "cbb": format_schedule_for_prompt(all_cbb_games, "CBB"),
            "nhl": format_schedule_for_prompt(all_nhl_games, "NHL"),
        }

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
            # Attach ocr_text as col 10 keyed by (capper, date).
            # One image can produce multiple pick rows (one per bet in the image),
            # so all picks from the same image share the same ocr_text.
            # The lookup by (capper, date) is reliable because each image comes
            # from a single capper and carries a single message date.
            for row in parsed_rows:
                while len(row) < 9:
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


def run_stage2(spreadsheet, image_pull_ws):
    """Run Stage 2: Finalize parsed picks with game/side data + Python spread lookup.

    Reads rows from parsed_picks, sends them to Sonnet in batches to fill
    game and side columns by cross-referencing the schedule, then fills spread
    via a Python post-pass that looks up the consensus spread from the ESPN
    schedule sheets. Writes to three destinations:

      finalized_picks  — staging sheet (all 10 cols incl. ocr_text); deduped
      master_sheet     — permanent history (cols 0–8, no ocr_text)
      parsed_picks_new — append-only audit sheet (all 10 cols incl. ocr_text)

    The dual-write design keeps master_sheet lean (no large OCR strings) while
    parsed_picks_new retains the full row for the nightly Opus hallucination
    audit.  parsed_picks_new is never truncated or rewritten — rows only ever
    accumulate so audit history is preserved across sessions.

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

    print(f"\n── Stage 2: Finalizing {len(data_rows)} parsed pick(s) ──")

    all_finalized_rows = []

    for batch_start in range(0, len(data_rows), STAGE_BATCH_SIZE):
        batch = data_rows[batch_start : batch_start + STAGE_BATCH_SIZE]
        print(f"\nBatch {batch_start // STAGE_BATCH_SIZE + 1}: {len(batch)} row(s)")

        # Get unique dates in this batch to fetch only relevant schedules
        pick_dates = set()
        for row in batch:
            if row and row[0]:
                pick_dates.add(row[0])

        print(f"Fetching schedules for dates: {sorted(pick_dates)}")

        all_nba_games = []
        all_cbb_games = []
        all_nhl_games = []

        for pick_date in sorted(pick_dates):
            all_nba_games.extend(
                get_schedule_for_date(spreadsheet, NBA_SCHEDULE_SHEET, pick_date)
            )
            all_cbb_games.extend(
                get_schedule_for_date(spreadsheet, CBB_SCHEDULE_SHEET, pick_date)
            )
            all_nhl_games.extend(
                get_schedule_for_date(spreadsheet, NHL_SCHEDULE_SHEET, pick_date)
            )

        schedule_data = {
            "nba": format_schedule_for_prompt(all_nba_games, "NBA"),
            "cbb": format_schedule_for_prompt(all_cbb_games, "CBB"),
            "nhl": format_schedule_for_prompt(all_nhl_games, "NHL"),
        }

        # Keep raw game dicts for Python spread lookup post-pass
        schedule_games_by_sport = {
            "nba": all_nba_games,
            "cbb": all_cbb_games,
            "nhl": all_nhl_games,
        }

        valid_batch = [row for row in batch if row]
        # Preserve ocr_text (col 9) from input rows — Stage 2 only fills
        # game/side and doesn't re-output ocr_text
        ocr_texts = [row[9] if len(row) > 9 else "" for row in valid_batch]

        rows_as_csv = [",".join(row) for row in valid_batch]

        prompt = build_stage2_prompt(rows_as_csv, schedule_data)
        print(f"Calling Sonnet to finalize {len(rows_as_csv)} picks...")

        try:
            response = call_sonnet_text(prompt)
            finalized_batch = parse_csv_response(response)
            finalized_batch = validate_and_fix_pick_column(finalized_batch)
            # Python post-pass: fill spread from schedule (Claude no longer does this)
            for row in finalized_batch:
                pick_team = row[3] if len(row) > 3 else ""
                sport = row[2].lower().strip() if len(row) > 2 else ""
                games = schedule_games_by_sport.get(sport, [])
                row[6] = lookup_spread_from_schedule(pick_team, "", sport, games)
            # Re-attach ocr_text to each finalized row (positional match)
            for j, row in enumerate(finalized_batch):
                ocr = ocr_texts[j] if j < len(ocr_texts) else ""
                if len(row) < 10:
                    row.append(ocr)
                else:
                    row[9] = ocr
            print(f"Finalized {len(finalized_batch)} pick row(s)")
            all_finalized_rows.extend(finalized_batch)
        except Exception as e:
            print(f"Stage 2 batch failed: {e}")
            continue

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

        # Tag all rows with source (index 10). Hardcoded "discord_all_in_one" until
        # Telegram wiring is added in Step 3 of the integration plan.
        for row in all_finalized_rows:
            while len(row) < 10:
                row.append("")
            if len(row) < 11:
                row.append("discord_all_in_one")

        # Also append to master_sheet (cols 0-8 + source at 10, strip ocr_text).
        # Filter out totals (O/U lines) — master_sheet is sides-only.
        if all_finalized_rows:
            master_ws = get_or_create_picks_worksheet(spreadsheet, MASTER_SHEET)
            master_rows = [
                row[:9] + [row[10]] for row in all_finalized_rows
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

    all_nba_games = []
    all_cbb_games = []
    all_nhl_games = []

    for msg_date in sorted(message_dates):
        all_nba_games.extend(
            get_schedule_for_date(spreadsheet, NBA_SCHEDULE_SHEET, msg_date)
        )
        all_cbb_games.extend(
            get_schedule_for_date(spreadsheet, CBB_SCHEDULE_SHEET, msg_date)
        )
        all_nhl_games.extend(
            get_schedule_for_date(spreadsheet, NHL_SCHEDULE_SHEET, msg_date)
        )

    schedule_data = {
        "nba": format_schedule_for_prompt(all_nba_games, "NBA"),
        "cbb": format_schedule_for_prompt(all_cbb_games, "CBB"),
        "nhl": format_schedule_for_prompt(all_nhl_games, "NHL"),
    }

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

    # Step 5: Run Stage 2 - Finalize with known cappers
    print("\n── Manual Queue Stage 2: Finalizing picks ──")

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

    # Get unique dates for schedules
    pick_dates = set(row[0] for row in parsed_data_rows if row and row[0])
    print(f"Fetching schedules for dates: {sorted(pick_dates)}")

    all_nba_games = []
    all_cbb_games = []
    all_nhl_games = []

    for pick_date in sorted(pick_dates):
        all_nba_games.extend(
            get_schedule_for_date(spreadsheet, NBA_SCHEDULE_SHEET, pick_date)
        )
        all_cbb_games.extend(
            get_schedule_for_date(spreadsheet, CBB_SCHEDULE_SHEET, pick_date)
        )
        all_nhl_games.extend(
            get_schedule_for_date(spreadsheet, NHL_SCHEDULE_SHEET, pick_date)
        )

    schedule_data = {
        "nba": format_schedule_for_prompt(all_nba_games, "NBA"),
        "cbb": format_schedule_for_prompt(all_cbb_games, "CBB"),
        "nhl": format_schedule_for_prompt(all_nhl_games, "NHL"),
    }

    # Keep raw game dicts for Python spread lookup post-pass
    schedule_games_by_sport = {
        "nba": all_nba_games,
        "cbb": all_cbb_games,
        "nhl": all_nhl_games,
    }

    # Get known cappers for normalization
    known_cappers = get_known_cappers(spreadsheet)
    if known_cappers:
        print(f"Using {len(known_cappers)} known cappers for normalization")

    # Call Sonnet with known cappers, batched to prevent hallucination
    all_manual_finalized = []
    valid_parsed_rows = [row for row in parsed_data_rows if row]
    for batch_start in range(0, len(valid_parsed_rows), STAGE_BATCH_SIZE):
        batch = valid_parsed_rows[batch_start : batch_start + STAGE_BATCH_SIZE]
        # Preserve ocr_text (col 9) — Stage 2 doesn't re-output it
        ocr_texts = [row[9] if len(row) > 9 else "" for row in batch]
        batch_csv = [",".join(row) for row in batch]
        prompt = build_stage2_prompt(batch_csv, schedule_data, known_cappers)
        print(f"Calling Sonnet to finalize batch of {len(batch_csv)} picks...")
        try:
            response = call_sonnet_text(prompt)
            batch_finalized = parse_csv_response(response)
            batch_finalized = validate_and_fix_pick_column(batch_finalized)
            # Python post-pass: fill spread from schedule (Claude no longer does this)
            for row in batch_finalized:
                pick_team = row[3] if len(row) > 3 else ""
                sport = row[2].lower().strip() if len(row) > 2 else ""
                games = schedule_games_by_sport.get(sport, [])
                row[6] = lookup_spread_from_schedule(pick_team, "", sport, games)
            # Re-attach ocr_text positionally
            for j, row in enumerate(batch_finalized):
                ocr = ocr_texts[j] if j < len(ocr_texts) else ""
                if len(row) < 10:
                    row.append(ocr)
                else:
                    row[9] = ocr
            print(f"Finalized {len(batch_finalized)} pick row(s)")
            all_manual_finalized.extend(batch_finalized)
        except Exception as e:
            print(f"Manual queue Stage 2 batch failed: {e}")
            continue

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

        # Tag all rows with source (index 10). Manual picks queue is always "discord_all_in_one"
        # until multi-source wiring is added in Step 3 of the integration plan.
        for row in finalized_rows:
            while len(row) < 10:
                row.append("")
            if len(row) < 11:
                row.append("discord_all_in_one")

        # Also append to master_sheet (cols 0-8 + source at 10, strip ocr_text).
        # Filter out totals (O/U lines) — master_sheet is sides-only.
        if finalized_rows:
            master_ws = get_or_create_picks_worksheet(spreadsheet, MASTER_SHEET)
            master_rows = [
                row[:9] + [row[10]] for row in finalized_rows
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
