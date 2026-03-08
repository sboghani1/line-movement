#!/usr/bin/env python3
"""
Discord Image Fetcher
Fetches the most recent image from a Discord channel using a user token.
Works for channels you can view as a regular member.
Inserts the image URL into a Google Sheet.
"""

import os
import json
import base64
import re
import csv
import io
import requests
from datetime import datetime
from typing import Optional, Tuple, List
from zoneinfo import ZoneInfo

from dotenv import load_dotenv
load_dotenv()

import anthropic
import gspread
from google.oauth2.service_account import Credentials

from activity_logger import log_activity

# ── Config ──────────────────────────────────────────────────────────────────
DISCORD_USER_TOKEN = os.environ.get("DISCORD_USER_TOKEN", "")
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
DISCORD_CHANNEL_ID = "1384768734727508019"
GOOGLE_SHEET_ID = "1LzkU7rH3OtrJckV5oMvFHyuLAnbRn9E74FO1uyfM65k"
WORKSHEET_NAME = "image_pull"

FIELDNAMES = ["timestamp", "message_sent_at", "capper_name", "image_url", "ocr_text", "committed_stage"]

# Worksheet names
PARSED_PICKS_SHEET = "parsed_picks"
FINALIZED_PICKS_SHEET = "finalized_picks"
NBA_SCHEDULE_SHEET = "nba_schedule"
CBB_SCHEDULE_SHEET = "cbb_schedule"
NHL_SCHEDULE_SHEET = "nhl_schedule"

# CSV columns for picks
PICKS_COLUMNS = ["date", "capper", "sport", "pick", "line", "game", "spread", "side", "result"]

# Image extensions to look for
IMAGE_EXTENSIONS = ('.png', '.jpg', '.jpeg', '.gif', '.webp')

# Maximum number of messages to process per run
MAX_MESSAGES_PER_RUN = 5

# Maximum images per OCR batch (Claude supports up to 20)
OCR_BATCH_SIZE = 15

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
        "https://www.googleapis.com/auth/drive"
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
        worksheet = spreadsheet.add_worksheet(title=WORKSHEET_NAME, rows=1000, cols=len(FIELDNAMES))
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
        worksheet = spreadsheet.add_worksheet(title=sheet_name, rows=1000, cols=len(PICKS_COLUMNS))
        # Row 1: timestamp placeholder
        worksheet.update_acell('A1', datetime.now(ZoneInfo("UTC")).strftime("%Y-%m-%d %H:%M:%S"))
        # Row 2: DO NOT EDIT label
        worksheet.update_acell('A2', "DO NOT EDIT ANYTHING ABOVE THIS ROW")
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
                games.append({
                    "away_team": row_dict.get("away_team", ""),
                    "home_team": row_dict.get("home_team", ""),
                    "spread": row_dict.get("spread", ""),
                    "over_under": row_dict.get("over_under", ""),
                    "game_time": row_dict.get("game_time", ""),
                })
        
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
        if g.get('spread'):
            line += f" | {g['spread']}"
        if g.get('over_under'):
            line += f" | O/U {g['over_under']}"
        lines.append(line)
    
    return "\n".join(lines)


def get_last_run_timestamp(worksheet) -> Optional[datetime]:
    """Get the last run timestamp from cell A1."""
    try:
        value = worksheet.acell('A1').value
        if value:
            # Parse UTC timestamp from A1
            return datetime.strptime(value, "%Y-%m-%d %H:%M:%S").replace(tzinfo=ZoneInfo("UTC"))
    except Exception:
        pass
    return None


def update_last_run_timestamp(worksheet, utc_time: datetime):
    """Update cell A1 with the current UTC timestamp."""
    worksheet.update_acell('A1', utc_time.strftime("%Y-%m-%d %H:%M:%S"))


# ── Discord API ──────────────────────────────────────────────────────────────


def fetch_recent_messages(limit: int = 100) -> list:
    """Fetch recent messages from the Discord channel using user token."""
    if not DISCORD_USER_TOKEN:
        raise ValueError("DISCORD_USER_TOKEN environment variable not set")
    
    headers = {
        "Authorization": DISCORD_USER_TOKEN,  # User token, no "Bot" prefix
        "Content-Type": "application/json"
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
                    sent_at = parse_discord_timestamp(message_time) if message_time else ""
                    return image_url, sent_at
            # Also check embed image field
            if "image" in embed:
                sent_at = parse_discord_timestamp(message_time) if message_time else ""
                return embed["image"].get("url"), sent_at
    
    return None


def get_messages_with_images_since(since_timestamp: Optional[datetime]) -> List[Tuple[str, str, str, datetime]]:
    """Get all messages with images since the given timestamp.
    
    Returns list of (image_url, message_sent_at_eastern, capper_name, message_datetime_utc) tuples.
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
        
        # Get capper name from message author
        author = message.get("author", {})
        capper_name = author.get("global_name") or author.get("username", "Unknown")
        
        # Check attachments
        for attachment in message.get("attachments", []):
            url = attachment.get("url", "")
            if url.lower().endswith(IMAGE_EXTENSIONS):
                sent_at = parse_discord_timestamp(message_time_str)
                results.append((url, sent_at, capper_name, message_dt))
        
        # Check embeds (for linked images)
        for embed in message.get("embeds", []):
            image_url = None
            if embed.get("type") == "image":
                image_url = embed.get("url") or embed.get("thumbnail", {}).get("url")
            elif "image" in embed:
                image_url = embed["image"].get("url")
            
            if image_url:
                sent_at = parse_discord_timestamp(message_time_str)
                results.append((image_url, sent_at, capper_name, message_dt))
    
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
    image_contents = []
    for i, url in enumerate(image_urls, 1):
        response = requests.get(url)
        response.raise_for_status()
        
        content_type = response.headers.get("content-type", "image/jpeg")
        media_type = get_media_type(url, content_type)
        image_data = base64.standard_b64encode(response.content).decode("utf-8")
        
        image_contents.append({
            "type": "image",
            "source": {
                "type": "base64",
                "media_type": media_type,
                "data": image_data,
            },
        })
        image_contents.append({
            "type": "text",
            "text": f"[Image {i}]"
        })
    
    # Add instruction at the end
    image_contents.append({
        "type": "text",
        "text": f"""Extract all text from each of the {len(image_urls)} images above.
For each image, output in this exact format:

[Image 1]
<extracted text here>

[Image 2]
<extracted text here>

...and so on. Preserve the layout of each image's text as much as possible."""
    })
    
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
    
    # Parse the response to extract text for each image
    response_text = message.content[0].text
    results = []
    
    # Split by [Image N] markers
    parts = re.split(r'\[Image \d+\]\s*', response_text)
    # First part is empty or intro text, skip it
    for part in parts[1:]:
        results.append(part.strip())
    
    # Pad with empty strings if we got fewer results than images
    while len(results) < len(image_urls):
        results.append("")
    
    return results[:len(image_urls)]


# ── Pick Parsing (Stage 1 & Stage 2) ─────────────────────────────────────────


def call_haiku_text(prompt: str, max_tokens: int = 8192) -> str:
    """Call Claude Haiku with a text-only prompt."""
    if not ANTHROPIC_API_KEY:
        raise ValueError("ANTHROPIC_API_KEY environment variable not set")
    
    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
    
    message = client.messages.create(
        model="claude-haiku-4-5-20251001",
        max_tokens=max_tokens,
        messages=[
            {"role": "user", "content": prompt}
        ],
    )
    
    return message.content[0].text


def build_stage1_prompt(picks_to_parse: List[Tuple[str, str, str]], schedule_data: dict) -> str:
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
NBA: {schedule_data.get('nba', 'No games')}
CBB: {schedule_data.get('cbb', 'No games')}
NHL: {schedule_data.get('nhl', 'No games')}

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
{schedule_data.get('nba', 'No games')}

NHL SCHEDULE:
{schedule_data.get('nhl', 'No games')}

CBB SCHEDULE:
{schedule_data.get('cbb', 'No games')}

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
        all_nba_games.extend(get_schedule_for_date(spreadsheet, NBA_SCHEDULE_SHEET, msg_date))
        all_cbb_games.extend(get_schedule_for_date(spreadsheet, CBB_SCHEDULE_SHEET, msg_date))
        all_nhl_games.extend(get_schedule_for_date(spreadsheet, NHL_SCHEDULE_SHEET, msg_date))
    
    schedule_data = {
        'nba': format_schedule_for_prompt(all_nba_games, "NBA"),
        'cbb': format_schedule_for_prompt(all_cbb_games, "CBB"),
        'nhl': format_schedule_for_prompt(all_nhl_games, "NHL"),
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
            parsed_picks_ws = get_or_create_picks_worksheet(spreadsheet, PARSED_PICKS_SHEET)
            
            # Append rows to parsed_picks (after row 3 which has headers)
            for row in parsed_rows:
                parsed_picks_ws.append_row(row, value_input_option="USER_ENTERED")
                print(f"  Added: {row[1]} - {row[2]} {row[3]} {row[4]}")
            
            # Update timestamp in parsed_picks A1
            parsed_picks_ws.update_acell('A1', now_eastern.strftime("%Y-%m-%d %H:%M:%S"))
        
        # Mark rows as stage_1_parsed in image_pull
        for row_idx, _, _, _ in rows_to_process:
            image_pull_ws.update_cell(row_idx, 6, "stage_1_parsed")
        
        # Log activity
        log_activity(spreadsheet, "process_ocr", f"Processed {len(rows_to_process)} rows into {len(parsed_rows)} picks")
        
        print(f"✅ Stage 1 complete: {len(parsed_rows)} picks parsed")
        
    except Exception as e:
        print(f"Stage 1 parsing failed: {e}")
        # Mark rows with failure count
        for row_idx, _, _, _ in rows_to_process:
            current_stage = image_pull_ws.cell(row_idx, 6).value or ""
            if current_stage.startswith("parse_failed_attempt_count_"):
                try:
                    count = int(current_stage.split("_")[-1]) + 1
                except ValueError:
                    count = 1
            else:
                count = 1
            image_pull_ws.update_cell(row_idx, 6, f"parse_failed_attempt_count_{count}")


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
        last_run_str = finalized_picks_ws.acell('A1').value
    except gspread.WorksheetNotFound:
        last_run_str = None
    
    if last_run_str:
        try:
            last_run = datetime.strptime(last_run_str, "%Y-%m-%d %H:%M:%S")
            last_run = last_run.replace(tzinfo=eastern)
            hours_since = (now_eastern - last_run).total_seconds() / 3600
            if hours_since < 1:
                minutes_remaining = int((1 - hours_since) * 60)
                print(f"Stage 2: Only {hours_since:.1f}h since last run, skipping (need 1h)")
                log_activity(spreadsheet, "finalize_picks", f"Skipped, cooldown for {minutes_remaining} more minutes")
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
        all_nba_games.extend(get_schedule_for_date(spreadsheet, NBA_SCHEDULE_SHEET, pick_date))
        all_cbb_games.extend(get_schedule_for_date(spreadsheet, CBB_SCHEDULE_SHEET, pick_date))
        all_nhl_games.extend(get_schedule_for_date(spreadsheet, NHL_SCHEDULE_SHEET, pick_date))
    
    schedule_data = {
        'nba': format_schedule_for_prompt(all_nba_games, "NBA"),
        'cbb': format_schedule_for_prompt(all_cbb_games, "CBB"),
        'nhl': format_schedule_for_prompt(all_nhl_games, "NHL"),
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
            finalized_picks_ws = get_or_create_picks_worksheet(spreadsheet, FINALIZED_PICKS_SHEET)
            
            # Append rows to finalized_picks
            for row in finalized_rows:
                finalized_picks_ws.append_row(row, value_input_option="USER_ENTERED")
                print(f"  Finalized: {row[1]} - {row[5]} | {row[3]} {row[4]}")
            
            # Update timestamp in finalized_picks A1
            finalized_picks_ws.update_acell('A1', now_eastern.strftime("%Y-%m-%d %H:%M:%S"))
        
        # Delete processed rows from parsed_picks (rows 4+)
        # Delete from bottom up to avoid index shifting
        for row_idx in range(len(all_values), 3, -1):
            if row_idx > 3:  # Don't delete header row
                parsed_picks_ws.delete_rows(row_idx)
        
        # Update image_pull rows to stage_2_finalized
        # We need to find rows that were stage_1_parsed
        image_pull_values = image_pull_ws.get_all_values()
        for i, row in enumerate(image_pull_values[2:], start=3):
            if len(row) > 5 and row[5] == "stage_1_parsed":
                image_pull_ws.update_cell(i, 6, "stage_2_finalized")
        
        # Log activity
        log_activity(spreadsheet, "finalize_picks", f"Finalized {len(finalized_rows)} picks")
        
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
        log_activity(worksheet.spreadsheet, "ocr_images", f"No OCR needed, {rows_with_ocr} already processed")
        return
    
    print(f"Found {len(rows_needing_ocr)} rows needing OCR")
    
    # Process in batches
    for batch_start in range(0, len(rows_needing_ocr), OCR_BATCH_SIZE):
        batch = rows_needing_ocr[batch_start:batch_start + OCR_BATCH_SIZE]
        urls = [url for _, url in batch]
        
        print(f"\nProcessing batch {batch_start // OCR_BATCH_SIZE + 1} ({len(batch)} images)...")
        ocr_results = extract_text_from_images_batch(urls)
        
        # Log OCR batch
        log_activity(worksheet.spreadsheet, "ocr_images", f"Completed OCR for {len(batch)} rows")
        
        # Update each row with OCR result
        for (row_idx, url), ocr_text in zip(batch, ocr_results):
            print(f"  Row {row_idx}: {ocr_text[:60]}..." if len(ocr_text) > 60 else f"  Row {row_idx}: {ocr_text}")
            worksheet.update_cell(row_idx, 5, ocr_text)
    
    print(f"\n✅ Backfilled OCR for {len(rows_needing_ocr)} rows")


def main():
    eastern = ZoneInfo("America/New_York")
    utc = ZoneInfo("UTC")
    now_eastern = datetime.now(eastern)
    now_utc = datetime.now(utc)
    timestamp = now_eastern.strftime("%Y-%m-%d %H:%M:%S")
    print(f"\n[{timestamp}] Fetching Discord images...")
    
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
        log_activity(spreadsheet, "query_discord", f"Found {len(messages_with_images)} new images")
        
        if messages_with_images:
            print(f"Found {len(messages_with_images)} new message(s) with images")
            
            existing_urls = get_existing_urls(worksheet)
            
            # Filter out duplicates and limit to MAX_MESSAGES_PER_RUN
            to_process = []
            for image_url, message_sent_at, capper_name, _ in messages_with_images:
                if image_url in existing_urls:
                    print(f"Skipping duplicate: {image_url[:60]}...")
                    continue
                to_process.append((image_url, message_sent_at, capper_name))
                if len(to_process) >= MAX_MESSAGES_PER_RUN:
                    print(f"Limiting to {MAX_MESSAGES_PER_RUN} messages per run")
                    break
            
            if to_process:
                # Check which URLs already have OCR to avoid re-processing
                urls_with_ocr = get_urls_with_ocr(worksheet)
                needs_ocr = [(url, sent, name) for url, sent, name in to_process if url not in urls_with_ocr]
                
                # Batch OCR
                ocr_results = []
                if needs_ocr:
                    image_urls_for_ocr = [img[0] for img in needs_ocr]
                    print(f"\nRunning batch OCR on {len(image_urls_for_ocr)} images...")
                    for i in range(0, len(image_urls_for_ocr), OCR_BATCH_SIZE):
                        batch = image_urls_for_ocr[i:i + OCR_BATCH_SIZE]
                        print(f"  Processing batch {i // OCR_BATCH_SIZE + 1} ({len(batch)} images)...")
                        ocr_results.extend(extract_text_from_images_batch(batch))
                        # Log OCR batch
                        log_activity(spreadsheet, "ocr_images", f"Completed OCR for {len(batch)} rows")
                    print(f"OCR complete. Got {len(ocr_results)} results.")
                
                # Insert rows (now with 6 columns including committed_stage)
                for idx, (image_url, message_sent_at, capper_name) in enumerate(to_process):
                    print(f"\nProcessing: {capper_name} @ {message_sent_at} ET")
                    print(f"URL: {image_url[:80]}...")
                    
                    # Get OCR text if available
                    ocr_text = ""
                    if image_url in urls_with_ocr:
                        print("OCR already exists, skipping")
                    else:
                        # Find index in needs_ocr list
                        try:
                            ocr_idx = [x[0] for x in needs_ocr].index(image_url)
                            ocr_text = ocr_results[ocr_idx] if ocr_idx < len(ocr_results) else ""
                            print(f"OCR: {ocr_text[:100]}..." if len(ocr_text) > 100 else f"OCR: {ocr_text}")
                        except (ValueError, IndexError):
                            pass
                    
                    # Append to sheet: timestamp, message_sent_at, capper_name, image_url, ocr_text, committed_stage (empty)
                    worksheet.append_row([timestamp, message_sent_at, capper_name, image_url, ocr_text, ""], value_input_option="USER_ENTERED")
                    existing_urls.add(image_url)
                    print(f"✅ Inserted")
                
                print(f"\n✅ Processed {len(to_process)} new message(s).")
            else:
                print("All images already exist in sheet")
        else:
            print("No new messages with images found since last run")
        
        # Update the last run timestamp
        update_last_run_timestamp(worksheet, now_utc)
        
        # Backfill OCR for any existing rows that don't have it
        backfill_ocr(worksheet)
        
        # Run Stage 1: Parse OCR to structured picks
        run_stage1(spreadsheet, worksheet)
        
        # Run Stage 2: Finalize picks (only if 1+ hour since last run)
        run_stage2(spreadsheet, worksheet)
        
    except ValueError as e:
        print(f"Error: {e}")
        exit(1)


if __name__ == "__main__":
    main()
