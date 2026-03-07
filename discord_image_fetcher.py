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
import requests
from datetime import datetime
from typing import Optional, Tuple
from zoneinfo import ZoneInfo

from dotenv import load_dotenv
load_dotenv()

import anthropic
import gspread
from google.oauth2.service_account import Credentials

# ── Config ──────────────────────────────────────────────────────────────────
DISCORD_USER_TOKEN = os.environ.get("DISCORD_USER_TOKEN", "")
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
DISCORD_CHANNEL_ID = "1384768734727508019"
GOOGLE_SHEET_ID = "1LzkU7rH3OtrJckV5oMvFHyuLAnbRn9E74FO1uyfM65k"
WORKSHEET_NAME = "image_pull"

FIELDNAMES = ["timestamp", "message_sent_at", "image_url"]

# Image extensions to look for
IMAGE_EXTENSIONS = ('.png', '.jpg', '.jpeg', '.gif', '.webp')


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
    """Get a set of existing image URLs (column 3) to avoid duplicates."""
    try:
        # Get all values from column 3 (image_url)
        col_values = worksheet.col_values(3)
        # Skip header row
        return set(col_values[1:]) if len(col_values) > 1 else set()
    except Exception:
        return set()


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


# ── Claude OCR ───────────────────────────────────────────────────────────────
def extract_text_from_image(image_url: str) -> str:
    """Use Claude 3.5 Haiku to OCR the image and extract text."""
    if not ANTHROPIC_API_KEY:
        raise ValueError("ANTHROPIC_API_KEY environment variable not set")
    
    # Download the image
    response = requests.get(image_url)
    response.raise_for_status()
    
    # Determine media type from content-type header or URL
    content_type = response.headers.get("content-type", "image/jpeg")
    if "png" in image_url.lower() or "png" in content_type:
        media_type = "image/png"
    elif "gif" in image_url.lower() or "gif" in content_type:
        media_type = "image/gif"
    elif "webp" in image_url.lower() or "webp" in content_type:
        media_type = "image/webp"
    else:
        media_type = "image/jpeg"
    
    # Base64 encode the image
    image_data = base64.standard_b64encode(response.content).decode("utf-8")
    
    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
    
    message = client.messages.create(
        model="claude-haiku-4-5-20251001",
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
                            "data": image_data,
                        },
                    },
                    {
                        "type": "text",
                        "text": "Extract all text from this image. Return only the raw text content, preserving the layout as much as possible."
                    }
                ],
            }
        ],
    )
    
    return message.content[0].text


def main():
    eastern = ZoneInfo("America/New_York")
    timestamp = datetime.now(eastern).strftime("%Y-%m-%d %H:%M:%S")
    print(f"\n[{timestamp}] Fetching Discord images...")
    
    try:
        result = get_most_recent_image()
        if not result:
            print("No images found in recent messages")
            return
        
        image_url, message_sent_at = result
        print(f"Most recent image URL:\n{image_url}")
        print(f"Message sent at: {message_sent_at} ET")
        
        worksheet = get_worksheet()
        existing_urls = get_existing_urls(worksheet)
        
        if image_url in existing_urls:
            print("Image URL already exists in sheet, skipping.")
            return
        
        # OCR the image using Claude
        print("Running OCR on image...")
        ocr_text = extract_text_from_image(image_url)
        print(f"\n--- OCR Output ---\n{ocr_text}\n--- End OCR ---\n")
        
        worksheet.append_row([timestamp, message_sent_at, image_url], value_input_option="USER_ENTERED")
        print(f"✅ Inserted image URL into Google Sheet (tab: {WORKSHEET_NAME})")
        
    except ValueError as e:
        print(f"Error: {e}")
        exit(1)


if __name__ == "__main__":
    main()
