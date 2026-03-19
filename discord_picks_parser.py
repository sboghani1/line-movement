#!/usr/bin/env python3
"""
╔═══════════════════════════════════════════════════════════════════════════════╗
║  DEPRECATED - OUR FALLEN FRIEND                                               ║
╠═══════════════════════════════════════════════════════════════════════════════╣
║  Discord Picks Parser (Bot-based)                                             ║
║                                                                               ║
║  WHAT IT DID:                                                                 ║
║  - Used Discord BOT token to fetch images from a channel                      ║
║  - OCR'd images with Claude to extract betting picks                          ║
║  - Inserted parsed picks into Google Sheets                                   ║
║                                                                               ║
║  WHY DEPRECATED:                                                              ║
║  - Discord bot tokens require server integration/permissions                  ║
║  - Replaced by capper_analyzer.py which uses USER token                       ║
║  - User token approach is simpler for personal automation                     ║
║                                                                               ║
║  REPLACEMENT: capper_analyzer.py                                              ║
║  - Uses DISCORD_USER_TOKEN instead of DISCORD_BOT_TOKEN                       ║
║  - Same OCR/parsing logic, better channel access                              ║
╚═══════════════════════════════════════════════════════════════════════════════╝
"""

import os
import json
import base64
import requests
import httpx
from datetime import datetime, timedelta, timezone
from typing import Optional

import gspread
from google.oauth2.service_account import Credentials
import anthropic

# ── Config ──────────────────────────────────────────────────────────────────
DISCORD_BOT_TOKEN = os.environ.get("DISCORD_BOT_TOKEN", "")
DISCORD_CHANNEL_ID = os.environ.get("DISCORD_CHANNEL_ID", "")
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
GOOGLE_SHEET_ID = os.environ.get("GOOGLE_SHEET_ID", "1LzkU7rH3OtrJckV5oMvFHyuLAnbRn9E74FO1uyfM65k")
WORKSHEET_NAME = "picks_data"

# How far back to look for messages (in hours)
LOOKBACK_HOURS = 2

FIELDNAMES = ["date", "capper", "sport", "pick", "line", "game", "spread", "side", "result"]

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
    """Get or create the worksheet for storing picks data."""
    client = get_gspread_client()
    spreadsheet = client.open_by_key(GOOGLE_SHEET_ID)
    
    try:
        worksheet = spreadsheet.worksheet(WORKSHEET_NAME)
    except gspread.WorksheetNotFound:
        worksheet = spreadsheet.add_worksheet(title=WORKSHEET_NAME, rows=1000, cols=len(FIELDNAMES))
        worksheet.append_row(FIELDNAMES)
    
    return worksheet


def get_existing_entries(worksheet) -> set:
    """Get a set of existing (date, capper, pick, line) tuples to avoid duplicates."""
    all_rows = worksheet.get_all_records()
    return {(row["date"], row["capper"], row["pick"], row["line"]) for row in all_rows}


# ── Discord API ──────────────────────────────────────────────────────────────
def fetch_recent_messages(limit: int = 100) -> list:
    """Fetch recent messages from the Discord channel."""
    if not DISCORD_BOT_TOKEN or not DISCORD_CHANNEL_ID:
        raise ValueError("DISCORD_BOT_TOKEN and DISCORD_CHANNEL_ID must be set")
    
    headers = {
        "Authorization": f"Bot {DISCORD_BOT_TOKEN}",
        "Content-Type": "application/json"
    }
    
    url = f"https://discord.com/api/v10/channels/{DISCORD_CHANNEL_ID}/messages"
    params = {"limit": limit}
    
    response = requests.get(url, headers=headers, params=params, timeout=30)
    response.raise_for_status()
    
    return response.json()


def filter_messages_with_images(messages: list, cutoff_time: datetime) -> list:
    """Filter messages to only those with image attachments posted after cutoff time."""
    filtered = []
    
    for msg in messages:
        # Parse message timestamp
        msg_time = datetime.fromisoformat(msg["timestamp"].replace("Z", "+00:00"))
        
        if msg_time < cutoff_time:
            continue
        
        # Check for image attachments
        attachments = msg.get("attachments", [])
        image_attachments = [
            att for att in attachments 
            if att.get("content_type", "").startswith("image/")
        ]
        
        if image_attachments:
            filtered.append({
                "message_id": msg["id"],
                "timestamp": msg_time,
                "content": msg.get("content", ""),  # This will be the capper name
                "author": msg.get("author", {}).get("username", "Unknown"),
                "images": [att["url"] for att in image_attachments]
            })
    
    return filtered


# ── Claude Vision OCR ────────────────────────────────────────────────────────
def download_image_as_base64(url: str) -> tuple[str, str]:
    """Download an image and return as base64 with media type."""
    response = httpx.get(url, timeout=30)
    response.raise_for_status()
    
    content_type = response.headers.get("content-type", "image/png")
    base64_data = base64.b64encode(response.content).decode("utf-8")
    
    return base64_data, content_type


def parse_picks_with_claude(image_url: str, capper_name: str) -> list[dict]:
    """Use Claude to parse betting picks from an image."""
    if not ANTHROPIC_API_KEY:
        raise ValueError("ANTHROPIC_API_KEY must be set")
    
    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
    
    # Download image and encode as base64
    image_data, media_type = download_image_as_base64(image_url)
    
    prompt = """Analyze this betting picks image and extract each bet as structured data.

For each bet in the image, extract:
1. sport: The sport (NBA, NFL, CBB, NHL, etc.) - infer from team names if not explicit
2. pick: The team or side being picked (e.g., "Michigan", "South Alabama", "Georgia Southern/Marshall")
3. line: The betting line (e.g., "ML", "-4", "+3.5", "O 167.5", "U 145.5")

Rules for parsing:
- "ML" means moneyline (just the team to win)
- A number like "-4" or "+3.5" is a spread
- "over" or "o" followed by a number is an over total (format as "O 167.5")
- "under" or "u" followed by a number is an under total (format as "U 145.5")
- The odds in parentheses like "-125" or the unit size like "(1.5U)" should be ignored
- If two teams are listed with a total (over/under), combine them like "Georgia Southern/Marshall"

NEVER INVERT PICKS (CRITICAL):
- The pick MUST be the EXACT team mentioned in the text - NEVER the opponent
- Under a "Fades:" header, if text says "Virginia +8", pick = "Virginia", line = "+8". Do NOT pick Duke (the fade target).
- Under a "Fades:" header, if text says "Houston +3", pick = "Houston", line = "+3". Do NOT pick Arizona (the fade target).
- Keep the line sign EXACTLY as written (+8 stays +8, -7 stays -7)
- Do NOT interpret "Fades", "Fade", or "Against" labels to mean bet the opponent
- ALWAYS record the team that is explicitly named in the image

Respond with ONLY a JSON array of objects, each with keys: sport, pick, line
Example response:
[
  {"sport": "CBB", "pick": "Michigan", "line": "ML"},
  {"sport": "CBB", "pick": "South Alabama", "line": "-4"},
  {"sport": "CBB", "pick": "Georgia Southern/Marshall", "line": "O 167.5"}
]

If you cannot read the image or there are no picks, return an empty array: []"""

    response = client.messages.create(
        model="claude-sonnet-4-20250514",
        max_tokens=1024,
        messages=[
            {
                "role": "user",
                "content": [
                    {
                        "type": "image",
                        "source": {
                            "type": "base64",
                            "media_type": media_type,
                            "data": image_data
                        }
                    },
                    {
                        "type": "text",
                        "text": prompt
                    }
                ]
            }
        ]
    )
    
    # Parse Claude's response
    response_text = response.content[0].text.strip()
    
    # Try to extract JSON from the response
    try:
        # Handle case where response might have markdown code blocks
        if "```json" in response_text:
            response_text = response_text.split("```json")[1].split("```")[0].strip()
        elif "```" in response_text:
            response_text = response_text.split("```")[1].split("```")[0].strip()
        
        picks = json.loads(response_text)
        return picks if isinstance(picks, list) else []
    except json.JSONDecodeError as e:
        print(f"Failed to parse Claude response: {e}")
        print(f"Response was: {response_text}")
        return []


# ── Main Logic ───────────────────────────────────────────────────────────────
def process_discord_picks():
    """Main function to fetch Discord images, parse picks, and update Google Sheets."""
    print(f"Starting Discord picks parser at {datetime.now(timezone.utc).isoformat()}")
    
    # Calculate cutoff time
    cutoff_time = datetime.now(timezone.utc) - timedelta(hours=LOOKBACK_HOURS)
    print(f"Looking for messages after {cutoff_time.isoformat()}")
    
    # Fetch recent messages
    messages = fetch_recent_messages()
    print(f"Fetched {len(messages)} messages from Discord")
    
    # Filter to messages with images
    image_messages = filter_messages_with_images(messages, cutoff_time)
    print(f"Found {len(image_messages)} messages with images in the lookback period")
    
    if not image_messages:
        print("No new images to process")
        return
    
    # Get worksheet and existing entries
    worksheet = get_worksheet()
    existing_entries = get_existing_entries(worksheet)
    print(f"Found {len(existing_entries)} existing entries in sheet")
    
    today_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    new_rows = []
    
    for msg in image_messages:
        # Use message content as capper name, or author if content is empty
        capper_name = msg["content"].strip().upper() if msg["content"].strip() else msg["author"].upper()
        print(f"Processing message from {capper_name} with {len(msg['images'])} images")
        
        for image_url in msg["images"]:
            try:
                picks = parse_picks_with_claude(image_url, capper_name)
                print(f"  Parsed {len(picks)} picks from image")
                
                for pick in picks:
                    # Check for duplicates
                    entry_key = (today_date, capper_name, pick.get("pick", ""), pick.get("line", ""))
                    if entry_key in existing_entries:
                        print(f"  Skipping duplicate: {entry_key}")
                        continue
                    
                    row = {
                        "date": today_date,
                        "capper": capper_name,
                        "sport": pick.get("sport", ""),
                        "pick": pick.get("pick", ""),
                        "line": pick.get("line", ""),
                        "game": "",  # Could be filled in with additional logic
                        "spread": "",
                        "side": pick.get("pick", ""),
                        "result": ""
                    }
                    new_rows.append(row)
                    existing_entries.add(entry_key)
                    
            except Exception as e:
                print(f"  Error processing image: {e}")
                continue
    
    # Append new rows to worksheet
    if new_rows:
        rows_to_append = [[row[field] for field in FIELDNAMES] for row in new_rows]
        worksheet.append_rows(rows_to_append)
        print(f"Added {len(new_rows)} new picks to the sheet")
    else:
        print("No new picks to add")
    
    print("Done!")


if __name__ == "__main__":
    process_discord_picks()
