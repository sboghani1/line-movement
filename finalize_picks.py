#!/usr/bin/env python3
"""
Finalize picks from parsed_picks_new to master_sheet_new.
Simple copy with spread lookup from schedule and side = pick.
"""

import os
import json
import base64
import gspread
from google.oauth2.service_account import Credentials
from collections import defaultdict

GOOGLE_SHEET_ID = "1LzkU7rH3OtrJckV5oMvFHyuLAnbRn9E74FO1uyfM65k"
INPUT_SHEET = "parsed_picks_new"
OUTPUT_SHEET = "master_sheet_new"
SCHEDULE_SHEET = "cbb_schedule"

OUTPUT_COLUMNS = ["date", "capper", "sport", "pick", "line", "game", "spread", "side", "result"]


def get_gspread_client():
    """Get authenticated gspread client."""
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


def load_schedule_spreads(spreadsheet):
    """Load consensus spreads from schedule.
    
    Returns:
        Dict mapping (date, team) -> spread string (e.g., "Duke Blue Devils -8")
    """
    try:
        sched_ws = spreadsheet.worksheet(SCHEDULE_SHEET)
        all_values = sched_ws.get_all_values()
    except gspread.WorksheetNotFound:
        print(f"Warning: {SCHEDULE_SHEET} not found")
        return {}
    
    # Find column indices
    headers = all_values[0] if all_values else []
    try:
        date_idx = headers.index("game_date")
        away_idx = headers.index("away_team")
        home_idx = headers.index("home_team")
        spread_idx = headers.index("spread")
    except ValueError as e:
        print(f"Warning: Missing schedule column: {e}")
        return {}
    
    spreads = {}
    for row in all_values[1:]:
        if len(row) <= max(date_idx, away_idx, home_idx, spread_idx):
            continue
        
        game_date = row[date_idx]
        away_team = row[away_idx]
        home_team = row[home_idx]
        spread_val = row[spread_idx]
        
        if game_date and spread_val:
            # Map both teams to the spread for this game
            spreads[(game_date, away_team)] = spread_val
            spreads[(game_date, home_team)] = spread_val
    
    return spreads


def main():
    print("=" * 60)
    print("Finalize Picks: parsed_picks_new -> master_sheet_new")
    print("=" * 60)
    
    # Connect
    print("\nConnecting to Google Sheets...")
    client = get_gspread_client()
    spreadsheet = client.open_by_key(GOOGLE_SHEET_ID)
    
    # Load schedule spreads
    print("Loading schedule spreads...")
    spreads = load_schedule_spreads(spreadsheet)
    print(f"  Loaded {len(spreads)} date+team -> spread mappings")
    
    # Load input
    print(f"\nLoading {INPUT_SHEET}...")
    input_ws = spreadsheet.worksheet(INPUT_SHEET)
    all_values = input_ws.get_all_values()
    
    # Headers on row 3, data starts row 4
    if len(all_values) < 4:
        print("No data rows to process")
        return
    
    headers = all_values[2]
    data_rows = all_values[3:]
    print(f"  Found {len(data_rows)} data rows")
    print(f"  Headers: {headers}")
    
    # Build column mapping
    col_map = {h: i for i, h in enumerate(headers)}
    
    # Process rows
    output_rows = []
    for row in data_rows:
        date = row[col_map.get("date", 0)] if "date" in col_map else ""
        capper = row[col_map.get("capper", 1)] if "capper" in col_map else ""
        sport = row[col_map.get("sport", 2)] if "sport" in col_map else ""
        pick = row[col_map.get("pick", 3)] if "pick" in col_map else ""
        line = row[col_map.get("line", 4)] if "line" in col_map else ""
        game = row[col_map.get("game", 5)] if "game" in col_map else ""
        
        # Look up spread from schedule
        spread = spreads.get((date, pick), "")
        
        # side = pick
        side = pick
        
        # result = empty
        result = ""
        
        output_rows.append([date, capper, sport, pick, line, game, spread, side, result])
    
    print(f"\nProcessed {len(output_rows)} rows")
    
    # Write to output sheet
    print(f"\nWriting to {OUTPUT_SHEET}...")
    output_ws = spreadsheet.worksheet(OUTPUT_SHEET)
    
    # Clear and write headers
    output_ws.clear()
    output_ws.update('A1', [OUTPUT_COLUMNS])
    
    # Write data starting row 2
    if output_rows:
        cell_range = f"A2:I{1 + len(output_rows)}"
        output_ws.update(cell_range, output_rows)
        print(f"  Wrote {len(output_rows)} rows")
    
    # Summary
    spreads_filled = sum(1 for r in output_rows if r[6])  # spread column
    print(f"\n{'='*60}")
    print("SUMMARY")
    print(f"{'='*60}")
    print(f"Total rows: {len(output_rows)}")
    print(f"Rows with spread filled: {spreads_filled}")
    print(f"Rows without spread: {len(output_rows) - spreads_filled}")


if __name__ == "__main__":
    main()
