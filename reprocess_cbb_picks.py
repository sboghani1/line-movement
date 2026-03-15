#!/usr/bin/env python3
"""
Reprocess CBB Picks Script
Reads OCR text from image_pull_new sheet and reprocesses all CBB picks
using improved prompts and the 2026_ncaa_schedule for context.
Outputs to parsed_picks_new sheet.
"""

import os
import sys
import time
import json
import re
from datetime import datetime, timedelta
from typing import List, Dict, Set, Tuple
from collections import defaultdict

import anthropic
import gspread
from google.oauth2.service_account import Credentials
from dotenv import load_dotenv

load_dotenv()

# ── Config ──────────────────────────────────────────────────────────────────
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
GOOGLE_SHEET_ID = "1LzkU7rH3OtrJckV5oMvFHyuLAnbRn9E74FO1uyfM65k"

# Sheet names
INPUT_SHEET = "image_pull_new"
OUTPUT_SHEET = "parsed_picks_new"
SCHEDULE_SHEET = "cbb_schedule"

# Processing config
BATCH_SIZE = 25
START_ROW = 4  # Skip header rows
SKIP_ROWS = 300  # Skip first N input rows (already processed)

# Output columns for parsed_picks_new
OUTPUT_COLUMNS = [
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
]

# Claude API tracking
CLAUDE_USAGE = {"input_tokens": 0, "output_tokens": 0}
HAIKU_INPUT_COST_PER_M = 0.80
HAIKU_OUTPUT_COST_PER_M = 4.00


def get_gspread_client():
    """Get authenticated gspread client."""
    import base64
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


def get_or_create_worksheet(spreadsheet, name: str, headers: List[str] = None):
    """Get worksheet or create it if it doesn't exist."""
    try:
        worksheet = spreadsheet.worksheet(name)
        return worksheet
    except gspread.WorksheetNotFound:
        print(f"Creating worksheet: {name}")
        worksheet = spreadsheet.add_worksheet(title=name, rows=10000, cols=len(headers or OUTPUT_COLUMNS))
        if headers:
            worksheet.update('A1', [headers])
        return worksheet


def load_schedule_for_dates(spreadsheet, dates: Set[str]) -> Dict[str, List[Dict]]:
    """Load NCAA schedule games for the given dates.
    
    Returns:
        Dict mapping date -> list of game dicts
    """
    try:
        schedule_ws = spreadsheet.worksheet(SCHEDULE_SHEET)
        # Use expected_headers to avoid duplicate header issues
        expected_headers = ['fetch_date', 'game_date', 'away_team', 'home_team', 
                           'game_time', 'spread', 'over_under', 'tv_network', 'venue', 'score']
        all_rows = schedule_ws.get_all_records(expected_headers=expected_headers)
    except gspread.WorksheetNotFound:
        print(f"Warning: {SCHEDULE_SHEET} worksheet not found")
        return {}
    except Exception as e:
        print(f"Warning: Error loading schedule: {e}")
        return {}
    
    games_by_date = defaultdict(list)
    
    for row in all_rows:
        game_date = row.get("game_date", "")
        if game_date in dates:
            games_by_date[game_date].append({
                "away_team": row.get("away_team", ""),
                "home_team": row.get("home_team", ""),
                "spread": row.get("spread", ""),
                "score": row.get("score", ""),
            })
    
    return games_by_date


def format_schedule_for_prompt(games_by_date: Dict[str, List[Dict]]) -> str:
    """Format schedule games for the prompt."""
    if not games_by_date:
        return "No games found"
    
    lines = []
    for date in sorted(games_by_date.keys()):
        lines.append(f"\n=== {date} ===")
        for game in games_by_date[date]:
            away = game["away_team"]
            home = game["home_team"]
            spread = game.get("spread", "")
            if spread:
                lines.append(f"  {away} @ {home} (spread: {spread})")
            else:
                lines.append(f"  {away} @ {home}")
    
    return "\n".join(lines)


def build_parse_prompt(ocr_rows: List[Dict], schedule_text: str) -> str:
    """Build the prompt for parsing OCR text into picks."""
    
    # Build the OCR section with unique row IDs
    ocr_section_parts = []
    for idx, row in enumerate(ocr_rows):
        date = row.get("date", "")
        capper = row.get("capper", "")
        ocr_text = row.get("ocr_text", "")
        row_id = row.get("row_id", idx)
        if ocr_text and ocr_text.strip():
            ocr_section_parts.append(f"[ROW:{row_id}] [{date}] [{capper}]\n{ocr_text}")
    
    ocr_section = "\n\n---\n\n".join(ocr_section_parts)
    
    prompt = f"""Parse the following OCR text from betting picks images into structured CSV data.

OUTPUT FORMAT: row_id,date,capper,sport,pick,line,game
(One row per pick. No headers. No quotes unless needed for commas.)

COLUMN DEFINITIONS:
- row_id: CRITICAL - Use the EXACT number from the [ROW:X] tag where you found the pick
  - If one OCR block contains 3 picks, ALL 3 picks must have the SAME row_id
  - Example: [ROW:5] has "Duke -7, Kentucky +3, Purdue -9" → all 3 picks use row_id=5
  - NEVER use sequential numbers (1,2,3) - use the actual [ROW:X] value
- date: YYYY-MM-DD format (use the date in brackets before each OCR block)
- capper: The capper name (use the name in brackets before each OCR block)
- sport: Always "CBB" for college basketball
- pick: The FULL official team name (e.g., "Virginia Cavaliers", NOT "Virginia" or "UVA")
- line: The point spread OR "ML" for moneyline bets
  - SPREADS are 1-2 digit numbers (college basketball spreads are NEVER 3 digits): +3.5, -7, +8, -15, +2.5
  - MONEYLINE ODDS are always 3+ digits: -155, +140, -120, +180, -105
  - If you see a 3-digit number like (-155), (-130), (-120), (+140), the LINE should be "ML", NOT the number
  - CRITICAL: "CINCINNATI (-130)" → line = "ML" (the -130 is moneyline odds, NOT a spread)
  - If NO spread number (1-2 digits) appears, but a 3-digit number does, it's MONEYLINE
  - Example: "Iowa State (-155)" → line = "ML" (the -155 is moneyline odds, not a spread)
  - Example: "CINCINNATI (-130) over UCF" → line = "ML" (-130 is ML odds, no spread given)
  - Example: "Duke -7 (-110)" → line = "-7" (the -7 is the spread, -110 is the juice/vig)
  - Example: "Kentucky ML" or "Kentucky (-125)" → line = "ML"
  - NEVER output a 3-digit number as the line - if the only number is 3 digits, output "ML"
- game: "away_team @ home_team" format using full team names from schedule (leave empty if unknown)

CRITICAL - NEVER INVERT PICKS (THIS IS THE MOST IMPORTANT RULE):
- OUTPUT THE EXACT TEAM NAME FROM THE OCR TEXT - NEVER SUBSTITUTE THE OPPONENT
- If OCR says "Kentucky +6", pick = Kentucky Wildcats, line = +6. DO NOT output Florida.
- If OCR says "Virginia +8", pick = Virginia Cavaliers, line = +8. DO NOT output Duke.
- If OCR says "Houston +3", pick = Houston Cougars, line = +3. DO NOT output Arizona.
- The schedule shows who is favored - IGNORE IT when determining the pick
- Even if Kentucky is -6 in the schedule, if OCR says "Kentucky +6", output Kentucky +6
- The bettor may be taking the other side of the line - that's their choice, not an error
- Keep the line sign EXACTLY as written (+6 stays +6, -7 stays -7)
- Do NOT "correct" picks based on schedule spreads
- Do NOT flip team names to match schedule favorites
- NEVER interpret "Fades", "Fade", or "Against" to mean bet the opponent

FILTERING - ONLY INCLUDE THESE:
- Sport: CBB (college basketball) ONLY
- Bet types: STANDALONE spread bets (with +/- point lines) or Moneyline (ML) ONLY

FILTERING - SKIP THESE COMPLETELY (DO NOT EXTRACT):
- NBA, NHL, NFL, ATP, soccer, UFC, or any other sport
- Totals (Over/Under, O/U) - e.g., "under 148.5", "over 145", any line without a team name
- Player props (any player name + stat)
- Team totals
- First half, second half, quarter bets
- PARLAYS - if text says "Parlay", "Whale Parlay", "MLP", or combines multiple teams with "/" or "+" on one line
- Live bets

TOTALS DETECTION (CRITICAL - DO NOT EXTRACT):
- ANY line containing "Under", "Over", "O/U", "U/O", or standalone "O" followed by a number is a TOTAL - SKIP IT
- "Rhode Island vs Fordham Under 132.5" = TOTAL, SKIP (even though teams are mentioned)
- "Loyola Chi. vs Richmond Under 142.5" = TOTAL, SKIP - do NOT extract Loyola Chicago ML
- "Team A/Team B over 145" = TOTAL, SKIP
- "Penn St/Rutgers O 149" or "O .149" = TOTAL (the "O" means Over), SKIP
- "Team O 160" or "O 160" = TOTAL, SKIP
- Totals are bets on combined scores, not on who wins - NEVER extract them
- If a line has "TeamA/TeamB" or "TeamA vs TeamB" followed by Over/Under/O/U and a number, it's a total - SKIP
- If the ONLY bet in an OCR block is a total, output NOTHING for that row - do NOT invent a team pick

PARLAY DETECTION (CRITICAL):
- Skip if text explicitly contains "Parlay" or "MLP"
- Skip if two teams are combined with "/" on the same line WITH a spread/ML (e.g., "Towson ML / William & Mary ML" is a 2-leg parlay - SKIP BOTH)
- BUT: "TeamA/TeamB O 149" is a TOTAL (not a parlay) - skip as total
- Skip if two teams are combined with "+" on the same line (e.g., "Ohio St + UCLA ML" is a 2-leg parlay - SKIP)
- Skip if two teams are combined with "x" on the same line (e.g., "Indiana x Kentucky ML" is a 2-leg parlay - SKIP BOTH)
- Multiple bets on SEPARATE lines are standalone bets - extract each one separately
- Example standalone: "Wichita State -7" on one line, "Idaho State -4.5" on another = 2 separate bets, extract BOTH
- Example parlay to SKIP: "Towson ML / William & Mary ML 1.5u -103" = parlay, SKIP
- Example parlay to SKIP: "Ohio St + UCLA ML (+115)" = parlay, SKIP
- Example parlay to SKIP: "Indiana x Kentucky ML -120" = parlay, SKIP BOTH teams

NEVER HALLUCINATE (CRITICAL - MOST IMPORTANT RULE):
- ONLY extract picks that are EXPLICITLY written in the OCR text
- If the OCR text does not mention a CBB team, output NOTHING for that row
- If OCR is about ATP, NHL, NFL, or other sports with NO CBB content, skip the entire row
- Example: OCR says "ATP Indian Wells De Minaur ML" - this is tennis, NO CBB picks exist - output nothing
- If OCR says "Mercer ML", extract MERCER - do NOT output Missouri or any other team
- NEVER look at the schedule and invent picks that aren't in the OCR
- The schedule is ONLY for finding game matchups, NOT for generating picks
- NEVER get the spread from the schedule - use ONLY the line from the OCR text
- If OCR says "CINCINNATI (-130)" with no spread, output ML - do NOT look up Cincinnati's spread from schedule
- VERIFICATION: Before outputting ANY pick, verify the team name (or abbreviation) appears in THAT row's OCR text
- Example: If OCR for row 4 says "Wichita -7.5, Colorado State -125, Minnesota -3.5" do NOT output "Iowa State" just because it's in the schedule
- If OCR says "Idaho -6.5" do NOT output "Iowa State" - these are different teams (Idaho Vandals ≠ Iowa State Cyclones)

TEAM NAME RESOLUTION:
- Use the schedule below to resolve abbreviations to FULL official names
- Examples: "UVA" → "Virginia Cavaliers", "UConn" → "UConn Huskies"
- "Zona" or "AZ" → "Arizona Wildcats", "FSU" → "Florida State Seminoles"
- "Cuse" → "Syracuse Orange", "Bama" → "Alabama Crimson Tide"
- FUZZY MATCH: If exact team name not in schedule, search for partial/alternate names
  - "Illinois Chicago" matches "UIC Flames" (same school, different name format)
  - "UNLV" matches "UNLV Rebels", "Saint Johns" matches "St. John's Red Storm"
  - Look for city names, abbreviations, or mascots that match

GAME COLUMN (CRITICAL - NEVER LEAVE BLANK):
- Search the schedule for the picked team to find the game matchup
- Format: "Away Team @ Home Team" (e.g., "Drake Bulldogs @ UIC Flames")
- Use fuzzy matching - if "Illinois Chicago -5" appears, find a schedule game involving UIC/Illinois-Chicago
- ALWAYS fill in the game column using the schedule data

TODAY'S CBB SCHEDULE (use to resolve team names and find game matchups):
{schedule_text}

OCR TEXT TO PARSE:
{ocr_section}

OUTPUT (CSV rows only, no headers, no explanation):"""

    return prompt


def call_haiku(prompt: str) -> str:
    """Call Claude Haiku to process the prompt."""
    global CLAUDE_USAGE
    
    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
    
    response = client.messages.create(
        model="claude-haiku-4-5-20251001",
        max_tokens=16000,
        temperature=0,
        messages=[{"role": "user", "content": prompt}]
    )
    
    # Track usage
    CLAUDE_USAGE["input_tokens"] += response.usage.input_tokens
    CLAUDE_USAGE["output_tokens"] += response.usage.output_tokens
    
    return response.content[0].text


def parse_csv_response(response: str, ocr_rows: List[Dict]) -> List[List[str]]:
    """Parse the CSV response from Haiku into rows.
    
    Also adds the ocr_text column and result (empty) column.
    """
    result_rows = []
    
    # Build mappings: row_id -> ocr_text AND (date, capper) -> ocr_text
    ocr_map_by_id = {}
    ocr_map_by_key = {}
    capper_by_id = {}  # row_id -> capper
    for idx, row in enumerate(ocr_rows):
        row_id = row.get("row_id", idx)
        ocr_text = row.get("ocr_text", "")
        capper = row.get("capper", "")
        ocr_map_by_id[str(row_id)] = ocr_text
        capper_by_id[str(row_id)] = capper
        key = (row.get("date", ""), capper)
        if key not in ocr_map_by_key:
            ocr_map_by_key[key] = ocr_text
    
    lines = response.strip().split("\n")
    
    for line in lines:
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        
        # Skip any header-like lines
        if line.lower().startswith("row_id,") or "capper,sport" in line.lower():
            continue
        
        # Parse CSV line (handle quoted fields)
        parts = []
        current = ""
        in_quotes = False
        for char in line:
            if char == '"':
                in_quotes = not in_quotes
            elif char == ',' and not in_quotes:
                parts.append(current.strip())
                current = ""
            else:
                current += char
        parts.append(current.strip())
        
        # We expect either:
        # row_id,date,capper,sport,pick,line,game (7 cols with row_id)
        # OR date,capper,sport,pick,line,game (6 cols without row_id)
        if len(parts) >= 5:
            # Detect format by checking if first column looks like a date
            if parts[0] and len(parts[0]) == 10 and parts[0][4] == '-':
                # No row_id format: date,capper,sport,pick,line,game
                row_id = ""
                date = parts[0]
                capper = parts[1] if len(parts) > 1 else ""
                sport = parts[2] if len(parts) > 2 else ""
                pick = parts[3] if len(parts) > 3 else ""
                line_val = parts[4] if len(parts) > 4 else ""
                game = parts[5] if len(parts) > 5 else ""
            else:
                # With row_id format: row_id,date,capper,sport,pick,line,game
                row_id = parts[0]
                date = parts[1] if len(parts) > 1 else ""
                capper = parts[2] if len(parts) > 2 else ""
                sport = parts[3] if len(parts) > 3 else ""
                pick = parts[4] if len(parts) > 4 else ""
                line_val = parts[5] if len(parts) > 5 else ""
                game = parts[6] if len(parts) > 6 else ""
            
            # Get original OCR text using row_id (if available) or date+capper fallback
            if row_id:
                ocr_text = ocr_map_by_id.get(str(row_id), "")
            else:
                ocr_text = ocr_map_by_key.get((date, capper), "")
            
            # Only include CBB
            if sport.upper() in ["CBB", "NCAAB", "NCAA", "COLLEGE BASKETBALL"]:
                result_rows.append([
                    date,
                    capper,
                    "CBB",
                    pick,
                    line_val,
                    game,
                    "",  # spread (empty)
                    "",  # side (empty)
                    "",  # result (empty)
                    ocr_text,
                ])
    
    return result_rows


def process_batch(spreadsheet, batch_rows: List[Dict], batch_num: int) -> List[List[str]]:
    """Process a batch of OCR rows.
    
    Args:
        spreadsheet: gspread spreadsheet object
        batch_rows: List of row dicts from image_pull_new
        batch_num: Batch number for logging
        
    Returns:
        List of parsed pick rows
    """
    print(f"\n{'='*60}")
    print(f"Processing batch {batch_num} ({len(batch_rows)} rows)")
    
    # Get unique dates from message_sent_at
    dates = set()
    for row in batch_rows:
        msg_sent = row.get("message_sent_at", "")
        if msg_sent and len(msg_sent) >= 10:
            # Extract date part (YYYY-MM-DD)
            date = msg_sent[:10]
            dates.add(date)
    
    print(f"  Dates in batch: {sorted(dates)}")
    
    # Load schedule for these dates
    games_by_date = load_schedule_for_dates(spreadsheet, dates)
    total_games = sum(len(g) for g in games_by_date.values())
    print(f"  Found {total_games} schedule games for {len(games_by_date)} dates")
    
    # Format schedule for prompt
    schedule_text = format_schedule_for_prompt(games_by_date)
    
    # Prepare OCR rows with date info and unique row_id
    ocr_rows = []
    for idx, row in enumerate(batch_rows):
        msg_sent = row.get("message_sent_at", "")
        date = msg_sent[:10] if msg_sent and len(msg_sent) >= 10 else ""
        ocr_rows.append({
            "row_id": idx,  # Unique within this batch
            "date": date,
            "capper": row.get("capper_name", ""),
            "ocr_text": row.get("ocr_text", ""),
        })
    
    # Build and call Haiku
    prompt = build_parse_prompt(ocr_rows, schedule_text)
    print(f"  Prompt length: {len(prompt)} chars")
    
    try:
        response = call_haiku(prompt)
        print(f"  Response length: {len(response)} chars")
        print(f"  Response preview: {response[:500]}...")
    except Exception as e:
        print(f"  ERROR calling Haiku: {e}")
        return []
    
    # Parse response
    parsed_rows = parse_csv_response(response, ocr_rows)
    print(f"  Parsed {len(parsed_rows)} CBB picks from batch")
    
    return parsed_rows


def main():
    """Main entry point."""
    print("=" * 60)
    print("CBB Picks Reprocessing Script")
    print("=" * 60)
    
    # Parse command line args
    start_batch = 1
    end_batch = None
    
    if len(sys.argv) >= 2:
        start_batch = int(sys.argv[1])
    if len(sys.argv) >= 3:
        end_batch = int(sys.argv[2])
    
    print(f"Starting from batch {start_batch}")
    if end_batch:
        print(f"Ending at batch {end_batch}")
    
    # Connect to Google Sheets
    print("\nConnecting to Google Sheets...")
    client = get_gspread_client()
    spreadsheet = client.open_by_key(GOOGLE_SHEET_ID)
    
    # Get input worksheet
    try:
        input_ws = spreadsheet.worksheet(INPUT_SHEET)
    except gspread.WorksheetNotFound:
        print(f"ERROR: Input worksheet '{INPUT_SHEET}' not found")
        return
    
    # Get all values (raw) since headers are on row 3
    print(f"Loading data from {INPUT_SHEET}...")
    all_values = input_ws.get_all_values()
    print(f"Total rows in {INPUT_SHEET}: {len(all_values)}")
    
    # Row 3 (index 2) has headers, data starts at row 4 (index 3)
    headers = all_values[2] if len(all_values) > 2 else []
    print(f"Headers: {headers}")
    
    # Convert data rows to dicts
    all_rows = []
    for row_values in all_values[3:]:  # Start from row 4 (index 3)
        row_dict = {}
        for i, header in enumerate(headers):
            if header and i < len(row_values):
                row_dict[header] = row_values[i]
        all_rows.append(row_dict)
    
    print(f"Total data rows in sheet: {len(all_rows)}")
    
    # Skip already processed rows
    if SKIP_ROWS > 0:
        all_rows = all_rows[SKIP_ROWS:]
        print(f"Skipping first {SKIP_ROWS} rows, {len(all_rows)} rows remaining")
    
    # Get or create output worksheet
    output_ws = get_or_create_worksheet(spreadsheet, OUTPUT_SHEET, OUTPUT_COLUMNS)
    
    # Check if output sheet already has data
    existing_output = output_ws.get_all_values()
    if len(existing_output) > 3:
        print(f"Output sheet already has {len(existing_output) - 3} data rows")
    
    # Ensure headers are on row 3
    if len(existing_output) < 3 or existing_output[2] != OUTPUT_COLUMNS:
        print("Writing headers to row 3...")
        output_ws.update('A3', [OUTPUT_COLUMNS])
        
    # Process in batches
    total_batches = (len(all_rows) + BATCH_SIZE - 1) // BATCH_SIZE
    print(f"\nTotal batches: {total_batches}")
    
    all_parsed_rows = []
    
    for batch_num in range(1, total_batches + 1):
        # Skip batches before start_batch
        if batch_num < start_batch:
            continue
        
        # Stop at end_batch if specified
        if end_batch and batch_num > end_batch:
            break
        
        start_idx = (batch_num - 1) * BATCH_SIZE
        end_idx = min(start_idx + BATCH_SIZE, len(all_rows))
        batch_rows = all_rows[start_idx:end_idx]
        
        if not batch_rows:
            break
        
        parsed_rows = process_batch(spreadsheet, batch_rows, batch_num)
        all_parsed_rows.extend(parsed_rows)
        
        # Write this batch's results immediately
        if parsed_rows:
            # Find the next empty row (data starts on row 4)
            existing = output_ws.get_all_values()
            next_row = len(existing) + 1 if len(existing) > 2 else 4
            
            # Write in chunks to avoid API limits
            chunk_size = 1000
            for i in range(0, len(parsed_rows), chunk_size):
                chunk = parsed_rows[i:i + chunk_size]
                cell_range = f"A{next_row}:J{next_row + len(chunk) - 1}"
                output_ws.update(cell_range, chunk)
                next_row += len(chunk)
            print(f"  Wrote {len(parsed_rows)} rows to sheet")
        
        # Report progress and cost
        input_cost = CLAUDE_USAGE["input_tokens"] / 1_000_000 * HAIKU_INPUT_COST_PER_M
        output_cost = CLAUDE_USAGE["output_tokens"] / 1_000_000 * HAIKU_OUTPUT_COST_PER_M
        total_cost = input_cost + output_cost
        print(f"  Running total: {len(all_parsed_rows)} picks, ${total_cost:.4f} API cost")
        
        # Rate limiting - avoid hitting API limits
        if batch_num < total_batches:
            print("  Waiting 2 seconds before next batch...")
            time.sleep(2)
    
    # Final summary
    print(f"\n{'='*60}")
    print("SUMMARY")
    print(f"{'='*60}")
    print(f"Batches processed: {min(end_batch or total_batches, total_batches) - start_batch + 1}")
    print(f"Total picks parsed: {len(all_parsed_rows)}")
    print(f"API tokens used: {CLAUDE_USAGE['input_tokens']:,} input, {CLAUDE_USAGE['output_tokens']:,} output")
    input_cost = CLAUDE_USAGE["input_tokens"] / 1_000_000 * HAIKU_INPUT_COST_PER_M
    output_cost = CLAUDE_USAGE["output_tokens"] / 1_000_000 * HAIKU_OUTPUT_COST_PER_M
    print(f"Total API cost: ${input_cost + output_cost:.4f}")


if __name__ == "__main__":
    main()
