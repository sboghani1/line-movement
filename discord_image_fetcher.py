#!/usr/bin/env python3
"""
Discord Image Fetcher
Fetches the most recent image from a Discord channel using a user token.
Works for channels you can view as a regular member.
Inserts the image URL into a Google Sheet.
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
from google.oauth2.service_account import Credentials
from PIL import Image

from activity_logger import log_activity

# ── Config ──────────────────────────────────────────────────────────────────
DISCORD_USER_TOKEN = os.environ.get("DISCORD_USER_TOKEN", "")
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
DISCORD_CHANNEL_ID = "1384768734727508019"
GOOGLE_SHEET_ID = "1LzkU7rH3OtrJckV5oMvFHyuLAnbRn9E74FO1uyfM65k"
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
FINALIZED_PICKS_SHEET = "finalized_picks"
NBA_SCHEDULE_SHEET = "nba_schedule"
CBB_SCHEDULE_SHEET = "cbb_schedule"
NHL_SCHEDULE_SHEET = "nhl_schedule"

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
]

# Image extensions to look for
IMAGE_EXTENSIONS = (".png", ".jpg", ".jpeg", ".gif", ".webp")

# Maximum number of messages to process per run
MAX_MESSAGES_PER_RUN = 100

# Maximum images per OCR batch (Claude supports up to 20)
OCR_BATCH_SIZE = 15

# Claude API usage tracking (Haiku 4.5 pricing: $0.80/M input, $4.00/M output)
CLAUDE_USAGE = {"input_tokens": 0, "output_tokens": 0}
HAIKU_INPUT_COST_PER_M = 0.80
HAIKU_OUTPUT_COST_PER_M = 4.00


def get_claude_cost():
    """Calculate estimated cost from token usage."""
    input_cost = (CLAUDE_USAGE["input_tokens"] / 1_000_000) * HAIKU_INPUT_COST_PER_M
    output_cost = (CLAUDE_USAGE["output_tokens"] / 1_000_000) * HAIKU_OUTPUT_COST_PER_M
    return input_cost + output_cost


def log_claude_usage(message):
    """Track token usage from a Claude API response."""
    if hasattr(message, "usage"):
        CLAUDE_USAGE["input_tokens"] += message.usage.input_tokens
        CLAUDE_USAGE["output_tokens"] += message.usage.output_tokens


# Example rows for prompts (spread, ML, total per sport)
EXAMPLE_PICKS_ROWS = """2026-02-01,BEEZO WINS,CBB,Iowa State,-11.5,Iowa State vs Kansas State,Iowa State -12,Iowa State,
2026-02-01,DARTH FADER,NBA,Clippers,+2,Clippers @ Suns,Suns -2,Clippers,
2026-02-01,A11 BETS,NBA,Clippers,ML,Clippers @ Suns,,Clippers,
2026-02-01,PARDON MY PICK,CBB,Illinois/Nebraska,O 151,Illinois @ Nebraska,O/U 151,,
2026-02-03,ANALYTICS CAPPER,NHL,Flyers,ML,Capitals @ Flyers,,Flyers,
2026-02-03,HAMMERING HANK,NBA,Nets,+8.5,Lakers @ Nets,Lakers -8.5,Nets,
2026-02-01,HAMMERING HANK,CBB,Florida,-8.5,Florida vs Alabama,Florida -8.5,Florida,"""

EXAMPLE_FINALIZED_ROWS = """2026-02-01,BEEZO WINS,CBB,Iowa State,-11.5,Iowa State vs Kansas State,Iowa State -12,Iowa State,
2026-02-01,DARTH FADER,NBA,Clippers,+2,Clippers @ Suns,Suns -2,Clippers,
2026-02-03,ANALYTICS CAPPER,NHL,Flyers,ML,Capitals @ Flyers,,Flyers,
2026-02-01,HAMMERING HANK,CBB,Florida,-8.5,Florida vs Alabama,Florida -8.5,Florida,"""


# ── Google Sheets Setup ──────────────────────────────────────────────────────
def get_gspread_client():
    """Authenticate with Google Sheets using service account credentials."""
    creds_b64 = os.environ.get("GOOGLE_CREDENTIALS", "")
    if not creds_b64:
        raise ValueError("GOOGLE_CREDENTIALS environment variable not set")

    creds_json = base64.b64decode(creds_b64).decode("utf-8")
    creds_dict = json.loads(creds_json)

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    credentials = Credentials.from_service_account_info(creds_dict, scopes=scopes)
    return gspread.authorize(credentials)


def get_worksheet():
    """Get or create the worksheet for storing image URLs."""
    client = get_gspread_client()
    spreadsheet = client.open_by_key(GOOGLE_SHEET_ID)

    try:
        worksheet = spreadsheet.worksheet(WORKSHEET_NAME)
    except gspread.WorksheetNotFound:
        worksheet = spreadsheet.add_worksheet(
            title=WORKSHEET_NAME, rows=1000, cols=len(FIELDNAMES)
        )
        worksheet.append_row(FIELDNAMES)

    return worksheet


def get_existing_urls(worksheet) -> set:
    """Get a set of existing image URLs (column 4) to avoid duplicates."""
    try:
        # Get all values from column 4 (image_url)
        col_values = worksheet.col_values(4)
        # Skip header row (row 2 is first data row since row 1 is timestamp)
        return set(col_values[2:]) if len(col_values) > 2 else set()
    except Exception:
        return set()


def get_urls_with_ocr(worksheet) -> set:
    """Get URLs that already have OCR text (column 5 is not empty)."""
    try:
        all_values = worksheet.get_all_values()
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
        worksheet = spreadsheet.worksheet(sheet_name)
    except gspread.WorksheetNotFound:
        worksheet = spreadsheet.add_worksheet(
            title=sheet_name, rows=1000, cols=len(PICKS_COLUMNS)
        )
        # Row 1: timestamp placeholder
        worksheet.update_acell(
            "A1", datetime.now(ZoneInfo("UTC")).strftime("%Y-%m-%d %H:%M:%S")
        )
        # Row 2: DO NOT EDIT label
        worksheet.update_acell("A2", "DO NOT EDIT ANYTHING ABOVE THIS ROW")
        # Row 3: column headers
        worksheet.append_row(PICKS_COLUMNS)
    return worksheet


def get_schedule_for_date(spreadsheet, sheet_name: str, target_date: str) -> List[dict]:
    """Get games from a schedule sheet for a specific date.

    Args:
        spreadsheet: The gspread spreadsheet object
        sheet_name: Name of the schedule sheet
        target_date: Date in YYYY-MM-DD format

    Returns:
        List of game dicts with away_team, home_team, spread, over_under
    """
    try:
        worksheet = spreadsheet.worksheet(sheet_name)
        all_values = worksheet.get_all_values()
        if len(all_values) < 2:
            return []

        # Row 1 is headers: fetch_date, game_date, away_team, home_team, game_time, spread, over_under, ...
        headers = all_values[0]
        games = []

        for row in all_values[1:]:
            if len(row) < 4:
                continue
            row_dict = dict(zip(headers, row))
            game_date = row_dict.get("game_date", "")

            if game_date == target_date:
                games.append(
                    {
                        "away_team": row_dict.get("away_team", ""),
                        "home_team": row_dict.get("home_team", ""),
                        "spread": row_dict.get("spread", ""),
                        "over_under": row_dict.get("over_under", ""),
                        "game_time": row_dict.get("game_time", ""),
                    }
                )

        return games
    except gspread.WorksheetNotFound:
        return []
    except Exception as e:
        print(f"Error fetching schedule from {sheet_name}: {e}")
        return []


def format_schedule_for_prompt(games: List[dict], sport: str) -> str:
    """Format game schedule into a string for the prompt."""
    if not games:
        return f"No {sport} games scheduled"

    lines = []
    for g in games:
        line = f"{g['away_team']} @ {g['home_team']}"
        if g.get("spread"):
            line += f" | {g['spread']}"
        if g.get("over_under"):
            line += f" | O/U {g['over_under']}"
        lines.append(line)

    return "\n".join(lines)


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
    worksheet.update_acell("A1", utc_time.strftime("%Y-%m-%d %H:%M:%S"))


# ── Discord API ──────────────────────────────────────────────────────────────


def fetch_recent_messages(limit: int = 100) -> list:
    """Fetch recent messages from the Discord channel using user token."""
    if not DISCORD_USER_TOKEN:
        raise ValueError("DISCORD_USER_TOKEN environment variable not set")

    headers = {
        "Authorization": DISCORD_USER_TOKEN,  # User token, no "Bot" prefix
        "Content-Type": "application/json",
    }

    url = f"https://discord.com/api/v10/channels/{DISCORD_CHANNEL_ID}/messages"
    params = {"limit": limit}

    response = requests.get(url, headers=headers, params=params)

    if response.status_code == 401:
        raise ValueError("Invalid token or unauthorized access")
    elif response.status_code == 403:
        raise ValueError("No permission to access this channel")
    elif response.status_code != 200:
        raise ValueError(f"Discord API error: {response.status_code} - {response.text}")

    return response.json()


def parse_discord_timestamp(timestamp_str: str) -> str:
    """Convert Discord ISO timestamp to Eastern time formatted string."""
    # Discord timestamps are ISO 8601 format
    dt = datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))
    eastern = ZoneInfo("America/New_York")
    dt_eastern = dt.astimezone(eastern)
    return dt_eastern.strftime("%Y-%m-%d %H:%M:%S")


def get_most_recent_image() -> Optional[Tuple[str, str]]:
    """Find and return the URL and sent time of the most recent image in the channel."""
    messages = fetch_recent_messages(limit=100)

    for message in messages:
        message_time = message.get("timestamp", "")

        # Check attachments
        for attachment in message.get("attachments", []):
            url = attachment.get("url", "")
            if url.lower().endswith(IMAGE_EXTENSIONS):
                sent_at = parse_discord_timestamp(message_time) if message_time else ""
                return url, sent_at

        # Check embeds (for linked images)
        for embed in message.get("embeds", []):
            if embed.get("type") == "image":
                image_url = embed.get("url") or embed.get("thumbnail", {}).get("url")
                if image_url:
                    sent_at = (
                        parse_discord_timestamp(message_time) if message_time else ""
                    )
                    return image_url, sent_at
            # Also check embed image field
            if "image" in embed:
                sent_at = parse_discord_timestamp(message_time) if message_time else ""
                return embed["image"].get("url"), sent_at

    return None


def get_messages_with_images_since(
    since_timestamp: Optional[datetime],
) -> List[Tuple[str, str, str, str, datetime]]:
    """Get all messages with images since the given timestamp.

    Returns list of (image_url, message_sent_at_eastern, capper_name, message_content, message_datetime_utc) tuples.
    """
    messages = fetch_recent_messages(limit=100)
    results = []

    for message in messages:
        message_time_str = message.get("timestamp", "")
        if not message_time_str:
            continue

        # Parse message time to datetime
        message_dt = datetime.fromisoformat(message_time_str.replace("Z", "+00:00"))

        # Skip if message is older than last run
        if since_timestamp and message_dt <= since_timestamp:
            continue

        # Try to extract capper name from message content
        # Filter out Discord role mentions like <@&1234567890>
        import re

        content = message.get("content", "").strip()
        # Remove role mentions and user mentions
        clean_content = re.sub(r"<@[&!]?\d+>", "", content).strip()

        # Get capper from first non-empty line of cleaned content
        capper_name = "UNKNOWN"
        if clean_content:
            for line in clean_content.split("\n"):
                line = line.strip()
                # Strip markdown bold markers
                line = re.sub(r"\*\*(.+?)\*\*", r"\1", line)
                line = line.strip()
                if line and len(line) <= 30:
                    # Skip URLs, common non-capper text, and links
                    skip_patterns = ["http", "tracker", "click here", "[", "]"]
                    if not any(p in line.lower() for p in skip_patterns):
                        capper_name = line.upper()
                        break

        # Check attachments
        for attachment in message.get("attachments", []):
            url = attachment.get("url", "")
            if url.lower().endswith(IMAGE_EXTENSIONS):
                sent_at = parse_discord_timestamp(message_time_str)
                results.append((url, sent_at, capper_name, clean_content, message_dt))

        # Check embeds (for linked images)
        for embed in message.get("embeds", []):
            image_url = None
            if embed.get("type") == "image":
                image_url = embed.get("url") or embed.get("thumbnail", {}).get("url")
            elif "image" in embed:
                image_url = embed["image"].get("url")

            if image_url:
                sent_at = parse_discord_timestamp(message_time_str)
                # Try to get capper from embed title/description/author if not found in content
                embed_capper = capper_name
                if embed_capper == "UNKNOWN":
                    # Check embed title
                    embed_title = embed.get("title", "").strip()
                    if embed_title and len(embed_title) <= 30 and not embed_title.startswith("http"):
                        embed_capper = embed_title.upper()
                    # Check embed description first line
                    if embed_capper == "UNKNOWN":
                        embed_desc = embed.get("description", "").strip()
                        if embed_desc:
                            first_line = embed_desc.split("\n")[0].strip()
                            if first_line and len(first_line) <= 30 and not first_line.startswith("http"):
                                embed_capper = first_line.upper()
                    # Check embed author name
                    if embed_capper == "UNKNOWN":
                        author_name = embed.get("author", {}).get("name", "").strip()
                        if author_name and len(author_name) <= 30:
                            embed_capper = author_name.upper()
                
                # Build full content including embed text for OCR context
                embed_content = clean_content
                if embed.get("title"):
                    embed_content = f"{embed.get('title')}\n{embed_content}"
                if embed.get("description"):
                    embed_content = f"{embed_content}\n{embed.get('description')}"
                
                results.append(
                    (image_url, sent_at, embed_capper, embed_content, message_dt)
                )

    # Return in chronological order (oldest first)
    return sorted(results, key=lambda x: x[3])


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


def extract_text_from_images_batch(
    image_urls: List[str], message_contents: List[str] = None
) -> List[Tuple[str, str]]:
    """Use Claude Haiku to OCR multiple images in a single batch call.

    Args:
        image_urls: List of image URLs to OCR (max 15 recommended, 20 max supported)
        message_contents: Optional list of Discord message text for each image (for capper identification)

    Returns:
        List of (capper_name, ocr_text) tuples, one per image in the same order
    """
    if not ANTHROPIC_API_KEY:
        raise ValueError("ANTHROPIC_API_KEY environment variable not set")

    if not image_urls:
        return []

    # Download and encode all images
    MAX_IMAGE_SIZE = 4 * 1024 * 1024  # 4MB limit (Claude max is 5MB)

    image_contents = []
    for i, url in enumerate(image_urls, 1):
        response = requests.get(url)
        response.raise_for_status()

        content_type = response.headers.get("content-type", "image/jpeg")
        media_type = get_media_type(url, content_type)
        image_bytes = response.content

        # Resize if image is too large
        if len(image_bytes) > MAX_IMAGE_SIZE:
            original_size_mb = len(image_bytes) / (1024 * 1024)
            print(
                f"  ⚠️ Image {i} exceeds 4MB ({original_size_mb:.2f}MB): {url[:100]}..."
            )
            img = Image.open(io.BytesIO(image_bytes))
            # Convert to RGB if necessary (for PNG with alpha)
            if img.mode in ("RGBA", "P"):
                img = img.convert("RGB")

            # Reduce quality/size until under limit
            quality = 85
            while quality >= 20:
                buffer = io.BytesIO()
                img.save(buffer, format="JPEG", quality=quality, optimize=True)
                image_bytes = buffer.getvalue()
                if len(image_bytes) <= MAX_IMAGE_SIZE:
                    break
                quality -= 10
                # Also reduce dimensions if still too large
                if quality < 50:
                    img = img.resize(
                        (img.width // 2, img.height // 2), Image.Resampling.LANCZOS
                    )

            new_size_mb = len(image_bytes) / (1024 * 1024)
            print(f"    Compressed to {new_size_mb:.2f}MB (quality={quality})")
            media_type = "image/jpeg"

        image_data = base64.standard_b64encode(image_bytes).decode("utf-8")

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
        # Add message context if available
        if message_contents and i <= len(message_contents) and message_contents[i - 1]:
            image_contents.append(
                {
                    "type": "text",
                    "text": f"[Image {i}]\nDiscord message: {message_contents[i - 1]}",
                }
            )
        else:
            image_contents.append({"type": "text", "text": f"[Image {i}]"})

    # Build prompt based on whether we have message contents
    if message_contents:
        prompt_text = f"""Extract text from each of the {len(image_urls)} images above and identify the capper (person making the pick).

For each image, you're given the Discord message that accompanied the image and the image itself.

Output format for each image:
[Image 1]
CAPPER: <capper name - uppercase, e.g., BEEZO WINS, DARTH FADER, HAMMERING HANK>
TEXT: <extracted text from image>

Capper identification rules (in priority order):
1. If the Discord message contains a clear name/handle (not a URL or role mention), use that
2. Look for header text in the image like "NAME Whale Exclusive", "NAME VIP Pick", "NAME's Pick"
3. First prominent name or handle shown in the image
4. "UNKNOWN" if no capper can be determined

IMPORTANT: "All In One Cappers" or "AIO Cappers" is NOT a capper - it's the aggregation channel name. Look for the actual capper name within the image.

Example cappers: BEEZO WINS, DARTH FADER, HAMMERING HANK, A11 BETS, ANALYTICS CAPPER, PARDON MY PICK"""
    else:
        prompt_text = f"""Extract all text from each of the {len(image_urls)} images above.
For each image, output in this exact format:

[Image 1]
CAPPER: <capper name if visible in image header, otherwise UNKNOWN>
TEXT: <extracted text here>

IMPORTANT: "All In One Cappers" or "AIO Cappers" is NOT a capper - it's the aggregation channel name. Look for the actual capper name.

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

    # Parse the response to extract capper and text for each image
    response_text = message.content[0].text
    results = []

    # Split by [Image N] markers
    parts = re.split(r"\[Image \d+\]\s*", response_text)
    # First part is empty or intro text, skip it
    for part in parts[1:]:
        part = part.strip()
        capper = "UNKNOWN"
        text = part

        # Try to extract CAPPER: and TEXT: fields
        capper_match = re.search(r"^CAPPER:\s*(.+?)(?:\n|$)", part, re.IGNORECASE)
        text_match = re.search(r"TEXT:\s*(.+)", part, re.IGNORECASE | re.DOTALL)

        if capper_match:
            capper = capper_match.group(1).strip().upper()
        if text_match:
            text = text_match.group(1).strip()

        results.append((capper, text))

    # Pad with empty tuples if we got fewer results than images
    while len(results) < len(image_urls):
        results.append(("UNKNOWN", ""))

    return results[: len(image_urls)]


# ── Pick Parsing (Stage 1 & Stage 2) ─────────────────────────────────────────


def call_haiku_text(prompt: str, max_tokens: int = 8192) -> str:
    """Call Claude Haiku with a text-only prompt."""
    if not ANTHROPIC_API_KEY:
        raise ValueError("ANTHROPIC_API_KEY environment variable not set")

    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

    message = client.messages.create(
        model="claude-haiku-4-5-20251001",
        max_tokens=max_tokens,
        messages=[{"role": "user", "content": prompt}],
    )

    # Track token usage
    log_claude_usage(message)

    return message.content[0].text


def build_stage1_prompt(
    picks_to_parse: List[Tuple[str, str, str]], schedule_data: dict
) -> str:
    """Build the Stage 1 parsing prompt.

    Args:
        picks_to_parse: List of (capper_name, message_date, ocr_text) tuples
        schedule_data: Dict with 'nba', 'cbb', 'nhl' schedule strings

    Returns:
        The full prompt string
    """
    picks_section = ""
    for capper, date, ocr_text in picks_to_parse:
        picks_section += f"\n[Capper: {capper}, Date: {date}]\n{ocr_text}\n"

    prompt = f"""Parse the following betting picks from OCR text into CSV rows.

OUTPUT FORMAT (one row per pick, comma-separated):
date,capper,sport,pick,line,game,spread,side,result

COLUMN DEFINITIONS:
- date: YYYY-MM-DD format (use the message date provided with each pick)
- capper: Name of the person making the pick (provided with each pick)
- sport: NBA, CBB, or NHL only. Normalize NCAAB to CBB.
- pick: Team name being bet on
- line: The line taken (e.g., +3.5, -110, ML, O 220.5)
- game: Leave empty for now (will be filled later)
- spread: Leave empty for now (will be filled later)  
- side: Leave empty for now (will be filled later)
- result: Leave empty

FILTERING RULES - ONLY INCLUDE:
- Sports: NBA, NHL, CBB (college basketball) ONLY. Skip ATP, NFL, soccer, etc.
- Bet types: Spread, Moneyline (ML), or Game Total (O/U) ONLY
- Skip: Player props, team totals, first half bets, quarter bets, parlays, live bets

IMPORTANT:
- Each individual pick should be a separate row
- If one image has multiple picks, output multiple rows for qualifying picks only
- Skip any non-betting content (promotional text, headers, etc.)
- If no picks qualify after filtering, output nothing

EXAMPLE ROWS:
{EXAMPLE_PICKS_ROWS}

TODAY'S SCHEDULE:
NBA: {schedule_data.get("nba", "No games")}
CBB: {schedule_data.get("cbb", "No games")}
NHL: {schedule_data.get("nhl", "No games")}

PICKS TO PARSE:
{picks_section}

OUTPUT (CSV rows only, no headers, no explanation):"""

    return prompt


def build_stage2_prompt(rows_to_finalize: List[str], schedule_data: dict) -> str:
    """Build the Stage 2 finalization prompt.

    Args:
        rows_to_finalize: List of CSV row strings to finalize
        schedule_data: Dict with 'nba', 'cbb', 'nhl' schedule strings

    Returns:
        The full prompt string
    """
    rows_section = "\n".join(rows_to_finalize)

    prompt = f"""Finalize these parsed betting picks by filling in the 'game', 'spread', and 'side' columns based on the scheduled games.

RULES:
- game: Format as "Away Team @ Home Team" matching the schedule
- spread: The official spread from schedule (e.g., "Team -3.5") or leave empty for ML/totals
- side: The team being bet on (should match the 'pick' column)
- Keep all other columns exactly as they are
- If you can't match a game to the schedule, leave game/spread empty but still include the row

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


def get_rows_needing_stage1(worksheet) -> List[Tuple[int, str, str, str]]:
    """Get image_pull rows that need Stage 1 processing.

    Returns:
        List of (row_index, capper_name, message_date, ocr_text) tuples
    """
    all_values = worksheet.get_all_values()
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
    """Run Stage 1: Parse OCR text to structured picks."""
    eastern = ZoneInfo("America/New_York")
    now_eastern = datetime.now(eastern)

    # Get rows needing Stage 1 processing
    rows_to_process = get_rows_needing_stage1(image_pull_ws)

    if not rows_to_process:
        print("No rows need Stage 1 parsing")
        return

    print(f"\n── Stage 1: Parsing {len(rows_to_process)} OCR result(s) ──")

    # Get unique dates from messages to fetch relevant schedules
    message_dates = set()
    for _, _, date, _ in rows_to_process:
        if date:
            message_dates.add(date)

    print(f"Fetching schedules for dates: {sorted(message_dates)}")

    # Fetch schedules for all message dates
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

    # Build the picks to parse
    picks_to_parse = [(capper, date, ocr) for _, capper, date, ocr in rows_to_process]

    # Call Haiku
    prompt = build_stage1_prompt(picks_to_parse, schedule_data)
    print(f"Calling Haiku to parse {len(picks_to_parse)} picks...")

    try:
        response = call_haiku_text(prompt)
        parsed_rows = parse_csv_response(response)
        print(f"Parsed {len(parsed_rows)} pick row(s)")

        if parsed_rows:
            # Get or create parsed_picks worksheet
            parsed_picks_ws = get_or_create_picks_worksheet(
                spreadsheet, PARSED_PICKS_SHEET
            )

            # Batch append rows to parsed_picks
            time.sleep(1)  # Rate limit
            parsed_picks_ws.append_rows(parsed_rows, value_input_option="USER_ENTERED")
            for row in parsed_rows:
                print(f"  Added: {row[1]} - {row[2]} {row[3]} {row[4]}")

            # Update timestamp in parsed_picks A1
            time.sleep(1)  # Rate limit
            parsed_picks_ws.update_acell(
                "A1", now_eastern.strftime("%Y-%m-%d %H:%M:%S")
            )

        # Batch update rows as stage_1_parsed in image_pull
        time.sleep(1)  # Rate limit
        cells_to_update = [
            gspread.Cell(row_idx, 6, "stage_1_parsed")
            for row_idx, _, _, _ in rows_to_process
        ]
        image_pull_ws.update_cells(cells_to_update)

        # Log activity
        log_activity(
            spreadsheet,
            "process_ocr",
            f"Processed {len(rows_to_process)} rows into {len(parsed_rows)} picks",
        )

        print(f"✅ Stage 1 complete: {len(parsed_rows)} picks parsed")

    except Exception as e:
        print(f"Stage 1 parsing failed: {e}")
        # Batch mark rows with failure count
        time.sleep(2)  # Rate limit - extra delay after error
        all_values = image_pull_ws.get_all_values()
        cells_to_update = []
        for row_idx, _, _, _ in rows_to_process:
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
            image_pull_ws.update_cells(cells_to_update)


def run_stage2(spreadsheet, image_pull_ws):
    """Run Stage 2: Finalize parsed picks with game/spread/side data."""
    eastern = ZoneInfo("America/New_York")
    utc = ZoneInfo("UTC")
    now_eastern = datetime.now(eastern)
    now_utc = datetime.now(utc)

    # Get parsed_picks worksheet
    try:
        parsed_picks_ws = spreadsheet.worksheet(PARSED_PICKS_SHEET)
    except gspread.WorksheetNotFound:
        print("No parsed_picks sheet found, skipping Stage 2")
        return

    # Check if enough time has passed since last Stage 2 run (using finalized_picks timestamp)
    try:
        finalized_picks_ws = spreadsheet.worksheet(FINALIZED_PICKS_SHEET)
        last_run_str = finalized_picks_ws.acell("A1").value
    except gspread.WorksheetNotFound:
        last_run_str = None

    if last_run_str:
        try:
            last_run = datetime.strptime(last_run_str, "%Y-%m-%d %H:%M:%S")
            last_run = last_run.replace(tzinfo=eastern)
            hours_since = (now_eastern - last_run).total_seconds() / 3600
            if hours_since < 1:
                minutes_remaining = int((1 - hours_since) * 60)
                print(
                    f"Stage 2: Only {hours_since:.1f}h since last run, skipping (need 1h)"
                )
                log_activity(
                    spreadsheet,
                    "finalize_picks",
                    f"Skipped, cooldown for {minutes_remaining} more minutes",
                )
                return
        except Exception:
            pass

    # Get all rows from parsed_picks (row 4+)
    all_values = parsed_picks_ws.get_all_values()
    if len(all_values) < 4:
        print("No rows in parsed_picks to finalize")
        return

    # Row 1 = timestamp, Row 2 = DO NOT EDIT, Row 3 = headers, Row 4+ = data
    data_rows = all_values[3:]  # Starting from row 4
    if not data_rows:
        print("No rows in parsed_picks to finalize")
        return

    print(f"\n── Stage 2: Finalizing {len(data_rows)} parsed pick(s) ──")

    # Get unique dates from picks to fetch relevant schedules
    pick_dates = set()
    for row in data_rows:
        if row and row[0]:
            pick_dates.add(row[0])

    print(f"Fetching schedules for dates: {sorted(pick_dates)}")

    # Fetch schedules for all pick dates
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

    # Convert rows to CSV strings
    rows_as_csv = [",".join(row) for row in data_rows if row]

    # Call Haiku
    prompt = build_stage2_prompt(rows_as_csv, schedule_data)
    print(f"Calling Haiku to finalize {len(rows_as_csv)} picks...")

    try:
        response = call_haiku_text(prompt)
        finalized_rows = parse_csv_response(response)
        print(f"Finalized {len(finalized_rows)} pick row(s)")

        if finalized_rows:
            # Get or create finalized_picks worksheet
            finalized_picks_ws = get_or_create_picks_worksheet(
                spreadsheet, FINALIZED_PICKS_SHEET
            )

            # Batch append rows to finalized_picks
            time.sleep(1)  # Rate limit
            finalized_picks_ws.append_rows(
                finalized_rows, value_input_option="USER_ENTERED"
            )
            for row in finalized_rows:
                print(f"  Finalized: {row[1]} - {row[5]} | {row[3]} {row[4]}")

            # Update timestamp in finalized_picks A1
            time.sleep(1)  # Rate limit
            finalized_picks_ws.update_acell(
                "A1", now_eastern.strftime("%Y-%m-%d %H:%M:%S")
            )

        # Delete processed rows from parsed_picks (rows 4+) in one batch
        if len(all_values) > 3:
            time.sleep(1)  # Rate limit
            parsed_picks_ws.delete_rows(4, len(all_values))

        # Batch update image_pull rows to stage_2_finalized
        time.sleep(1)  # Rate limit
        image_pull_values = image_pull_ws.get_all_values()
        cells_to_update = []
        for i, row in enumerate(image_pull_values[2:], start=3):
            if len(row) > 5 and row[5] == "stage_1_parsed":
                cells_to_update.append(gspread.Cell(i, 6, "stage_2_finalized"))
        if cells_to_update:
            time.sleep(1)  # Rate limit
            image_pull_ws.update_cells(cells_to_update)

        # Log activity
        log_activity(
            spreadsheet, "finalize_picks", f"Finalized {len(finalized_rows)} picks"
        )

        print(f"✅ Stage 2 complete: {len(finalized_rows)} picks finalized")

    except Exception as e:
        print(f"Stage 2 finalization failed: {e}")


def backfill_ocr(worksheet):
    """Run OCR on existing rows that are missing OCR text."""
    all_values = worksheet.get_all_values()

    # Find rows needing OCR (row index, url) - skip row 1 (timestamp) and row 2 (header)
    rows_needing_ocr = []
    rows_with_ocr = 0
    for i, row in enumerate(all_values[2:], start=3):  # Start at row 3 (1-indexed)
        if len(row) >= 4 and row[3]:  # Has URL
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

    # Process in batches (no message contents available for backfill)
    all_ocr_updates = []  # Collect all updates for batch
    all_capper_updates = []  # Also update capper if detected
    for batch_start in range(0, len(rows_needing_ocr), OCR_BATCH_SIZE):
        batch = rows_needing_ocr[batch_start : batch_start + OCR_BATCH_SIZE]
        urls = [url for _, url in batch]

        print(
            f"\nProcessing batch {batch_start // OCR_BATCH_SIZE + 1} ({len(batch)} images)..."
        )
        ocr_results = extract_text_from_images_batch(
            urls
        )  # Returns (capper, text) tuples

        # Log OCR batch
        log_activity(
            worksheet.spreadsheet, "ocr_images", f"Completed OCR for {len(batch)} rows"
        )

        # Collect cells to update
        for (row_idx, url), (ocr_capper, ocr_text) in zip(batch, ocr_results):
            print(
                f"  Row {row_idx}: {ocr_text[:60]}..."
                if len(ocr_text) > 60
                else f"  Row {row_idx}: {ocr_text}"
            )
            all_ocr_updates.append(gspread.Cell(row_idx, 5, ocr_text))
            # Also update capper (column 3) if OCR detected one and current is UNKNOWN
            if ocr_capper != "UNKNOWN":
                # Get current capper from row data
                current_row = (
                    all_values[row_idx - 1] if row_idx <= len(all_values) else []
                )
                current_capper = current_row[2] if len(current_row) > 2 else "UNKNOWN"
                if current_capper == "UNKNOWN":
                    print(f"    Updating capper: {current_capper} -> {ocr_capper}")
                    all_capper_updates.append(gspread.Cell(row_idx, 3, ocr_capper))

    # Batch update all OCR results
    if all_ocr_updates:
        time.sleep(1)  # Rate limit
        worksheet.update_cells(all_ocr_updates)

    # Batch update capper names
    if all_capper_updates:
        time.sleep(1)  # Rate limit
        worksheet.update_cells(all_capper_updates)
        print(f"  Updated {len(all_capper_updates)} capper names")

    print(f"\n✅ Backfilled OCR for {len(rows_needing_ocr)} rows")


def cleanup_old_rows(spreadsheet, image_pull_ws):
    """Delete rows older than 1 week from schedules and image_pull."""
    from datetime import timedelta

    eastern = ZoneInfo("America/New_York")
    cutoff_date = datetime.now(eastern).date() - timedelta(days=7)
    cutoff_str = cutoff_date.strftime("%Y-%m-%d")

    print(f"\n── Cleanup: Removing rows older than {cutoff_str} ──")

    # Clean up schedule sheets (game_date in column A)
    schedule_sheets = [
        (NBA_SCHEDULE_SHEET, "nba_schedule"),
        (CBB_SCHEDULE_SHEET, "cbb_schedule"),
        (NHL_SCHEDULE_SHEET, "nhl_schedule"),
    ]

    for sheet_name, log_name in schedule_sheets:
        try:
            ws = spreadsheet.worksheet(sheet_name)
            all_values = ws.get_all_values()
            if len(all_values) <= 1:  # Only header or empty
                continue

            # Find rows to delete (oldest first to maintain indices)
            rows_to_delete = []
            for i, row in enumerate(all_values[1:], start=2):  # Skip header
                if row and row[0]:
                    try:
                        game_date = datetime.strptime(row[0], "%Y-%m-%d").date()
                        if game_date < cutoff_date:
                            rows_to_delete.append(i)
                    except ValueError:
                        continue

            if rows_to_delete:
                # Delete rows in reverse order to maintain indices
                for row_idx in reversed(rows_to_delete):
                    ws.delete_rows(row_idx)
                    time.sleep(0.5)  # Rate limit

                remaining_rows = len(all_values) - 1 - len(rows_to_delete)
                print(
                    f"  {log_name}: Deleted {len(rows_to_delete)} old rows, {remaining_rows} remaining"
                )
                log_activity(
                    spreadsheet,
                    "cleanup",
                    f"{log_name}: Deleted {len(rows_to_delete)} rows, {remaining_rows} remaining",
                )
            else:
                remaining_rows = len(all_values) - 1
                print(
                    f"  {log_name}: No old rows to delete, {remaining_rows} remaining"
                )
        except gspread.WorksheetNotFound:
            continue

    # Clean up image_pull (message_sent_at in column B - format: "2026-03-08 10:30:00")
    try:
        all_values = image_pull_ws.get_all_values()
        if len(all_values) <= 2:  # Timestamp row + header row
            return

        rows_to_delete = []
        for i, row in enumerate(all_values[2:], start=3):  # Skip timestamp and header
            if row and len(row) >= 2 and row[1]:
                try:
                    # Parse datetime from column B (format: YYYY-MM-DD HH:MM:SS)
                    message_date = datetime.strptime(row[1][:10], "%Y-%m-%d").date()
                    if message_date < cutoff_date:
                        rows_to_delete.append(i)
                except ValueError:
                    continue

        if rows_to_delete:
            # Delete rows in reverse order to maintain indices
            for row_idx in reversed(rows_to_delete):
                image_pull_ws.delete_rows(row_idx)
                time.sleep(0.5)  # Rate limit

            remaining_rows = len(all_values) - 2 - len(rows_to_delete)
            print(
                f"  image_pull: Deleted {len(rows_to_delete)} old rows, {remaining_rows} remaining"
            )
            log_activity(
                spreadsheet,
                "cleanup",
                f"image_pull: Deleted {len(rows_to_delete)} rows, {remaining_rows} remaining",
            )
        else:
            remaining_rows = len(all_values) - 2
            print(f"  image_pull: No old rows to delete, {remaining_rows} remaining")
    except Exception as e:
        print(f"  image_pull cleanup failed: {e}")


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
        worksheet = spreadsheet.worksheet(WORKSHEET_NAME)

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

                # Batch OCR with message contents for capper detection
                ocr_results = []  # List of (capper, text) tuples
                if needs_ocr:
                    image_urls_for_ocr = [img[0] for img in needs_ocr]
                    message_contents_for_ocr = [img[3] for img in needs_ocr]
                    print(f"\nRunning batch OCR on {len(image_urls_for_ocr)} images...")
                    for i in range(0, len(image_urls_for_ocr), OCR_BATCH_SIZE):
                        batch_urls = image_urls_for_ocr[i : i + OCR_BATCH_SIZE]
                        batch_contents = message_contents_for_ocr[
                            i : i + OCR_BATCH_SIZE
                        ]
                        print(
                            f"  Processing batch {i // OCR_BATCH_SIZE + 1} ({len(batch_urls)} images)..."
                        )
                        ocr_results.extend(
                            extract_text_from_images_batch(batch_urls, batch_contents)
                        )
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

                    # Get OCR text and capper if available
                    ocr_text = ""
                    ocr_capper = "UNKNOWN"
                    if image_url in urls_with_ocr:
                        print("OCR already exists, skipping")
                    else:
                        # Find index in needs_ocr list
                        try:
                            ocr_idx = [x[0] for x in needs_ocr].index(image_url)
                            if ocr_idx < len(ocr_results):
                                ocr_capper, ocr_text = ocr_results[ocr_idx]
                            print(
                                f"OCR: {ocr_text[:100]}..."
                                if len(ocr_text) > 100
                                else f"OCR: {ocr_text}"
                            )
                            print(f"OCR detected capper: {ocr_capper}")
                        except (ValueError, IndexError):
                            pass

                    # Use the capper from OCR (Haiku detected from message content + image)
                    # If OCR returned UNKNOWN, fall back to the message-content capper
                    if ocr_capper != "UNKNOWN":
                        final_capper = ocr_capper
                    elif capper_name != "UNKNOWN":
                        final_capper = capper_name
                    else:
                        # Last resort: try regex extraction from OCR text
                        extracted_capper = extract_capper_from_ocr(ocr_text)
                        final_capper = (
                            extracted_capper if extracted_capper else "UNKNOWN"
                        )
                    print(f"Final capper: {final_capper}")

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
                    next_row = len(worksheet.get_all_values()) + 1
                    end_row = next_row + len(rows_to_insert) - 1
                    time.sleep(1)  # Rate limit
                    worksheet.update(
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

        # Run Stage 2: Finalize picks (only if 1+ hour since last run)
        run_stage2(spreadsheet, worksheet)

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

    except ValueError as e:
        print(f"Error: {e}")
        exit(1)


if __name__ == "__main__":
    main()
