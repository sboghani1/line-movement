#!/usr/bin/env python3
"""
Local Picks Parser
Processes betting pick images from a local folder, uses Claude to OCR/parse,
and inserts them into a Google Sheet.

Usage:
1. Save images from Discord to the 'picks_inbox' folder
2. Name files like: CAPPER_NAME.png (or .jpg, .jpeg, .webp)
3. Run this script
4. Processed images are moved to 'picks_processed' folder
"""

import os
import sys
import json
import base64
import shutil
from datetime import datetime, timezone
from pathlib import Path

import gspread
from google.oauth2.service_account import Credentials
import anthropic

# ── Config ──────────────────────────────────────────────────────────────────
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
GOOGLE_SHEET_ID = os.environ.get("GOOGLE_SHEET_ID", "1LzkU7rH3OtrJckV5oMvFHyuLAnbRn9E74FO1uyfM65k")
WORKSHEET_NAME = "picks_data"

# Folders
SCRIPT_DIR = Path(__file__).parent
INBOX_FOLDER = SCRIPT_DIR / "picks_inbox"
PROCESSED_FOLDER = SCRIPT_DIR / "picks_processed"

SUPPORTED_EXTENSIONS = {".png", ".jpg", ".jpeg", ".webp", ".gif"}

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


# ── Claude Vision OCR ────────────────────────────────────────────────────────
def get_media_type(filepath: Path) -> str:
    """Get the media type for an image file."""
    ext = filepath.suffix.lower()
    return {
        ".png": "image/png",
        ".jpg": "image/jpeg",
        ".jpeg": "image/jpeg",
        ".webp": "image/webp",
        ".gif": "image/gif"
    }.get(ext, "image/png")


def parse_picks_with_claude(image_path: Path, capper_name: str) -> list[dict]:
    """Use Claude to parse betting picks from an image."""
    if not ANTHROPIC_API_KEY:
        raise ValueError("ANTHROPIC_API_KEY must be set")
    
    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
    
    # Read and encode image
    with open(image_path, "rb") as f:
        image_data = base64.b64encode(f.read()).decode("utf-8")
    
    media_type = get_media_type(image_path)
    
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


def extract_capper_from_filename(filepath: Path) -> str:
    """Extract capper name from filename (without extension)."""
    # Remove extension and clean up
    name = filepath.stem
    # Replace underscores/hyphens with spaces, then uppercase
    name = name.replace("_", " ").replace("-", " ")
    return name.upper().strip()


# ── Main Logic ───────────────────────────────────────────────────────────────
def process_local_picks():
    """Main function to process local images and update Google Sheets."""
    print(f"Starting local picks parser at {datetime.now(timezone.utc).isoformat()}")
    
    # Create folders if they don't exist
    INBOX_FOLDER.mkdir(exist_ok=True)
    PROCESSED_FOLDER.mkdir(exist_ok=True)
    
    # Find image files in inbox
    image_files = [
        f for f in INBOX_FOLDER.iterdir() 
        if f.is_file() and f.suffix.lower() in SUPPORTED_EXTENSIONS
    ]
    
    if not image_files:
        print(f"No images found in {INBOX_FOLDER}")
        print(f"Save images with filenames like: CAPPER_NAME.png")
        return
    
    print(f"Found {len(image_files)} images to process")
    
    # Get worksheet and existing entries
    worksheet = get_worksheet()
    existing_entries = get_existing_entries(worksheet)
    print(f"Found {len(existing_entries)} existing entries in sheet")
    
    today_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    new_rows = []
    processed_files = []
    
    for image_path in image_files:
        capper_name = extract_capper_from_filename(image_path)
        print(f"\nProcessing: {image_path.name} (Capper: {capper_name})")
        
        try:
            picks = parse_picks_with_claude(image_path, capper_name)
            print(f"  Parsed {len(picks)} picks")
            
            for pick in picks:
                # Check for duplicates
                entry_key = (today_date, capper_name, pick.get("pick", ""), pick.get("line", ""))
                if entry_key in existing_entries:
                    print(f"  Skipping duplicate: {pick.get('pick')} {pick.get('line')}")
                    continue
                
                row = {
                    "date": today_date,
                    "capper": capper_name,
                    "sport": pick.get("sport", ""),
                    "pick": pick.get("pick", ""),
                    "line": pick.get("line", ""),
                    "game": "",
                    "spread": "",
                    "side": pick.get("pick", ""),
                    "result": ""
                }
                new_rows.append(row)
                existing_entries.add(entry_key)
                print(f"  Added: {pick.get('sport')} - {pick.get('pick')} {pick.get('line')}")
            
            processed_files.append(image_path)
            
        except Exception as e:
            print(f"  Error processing image: {e}")
            continue
    
    # Append new rows to worksheet
    if new_rows:
        rows_to_append = [[row[field] for field in FIELDNAMES] for row in new_rows]
        worksheet.append_rows(rows_to_append)
        print(f"\nAdded {len(new_rows)} new picks to the sheet")
    else:
        print("\nNo new picks to add")
    
    # Move processed files
    for image_path in processed_files:
        dest = PROCESSED_FOLDER / f"{datetime.now().strftime('%Y%m%d_%H%M%S')}_{image_path.name}"
        shutil.move(str(image_path), str(dest))
        print(f"Moved {image_path.name} to processed folder")
    
    print("\nDone!")


if __name__ == "__main__":
    process_local_picks()
