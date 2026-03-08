#!/usr/bin/env python3
"""
Activity Logger
Logs activities to the activity_log sheet in Google Sheets.
"""

import json
from datetime import datetime
from typing import Optional
from zoneinfo import ZoneInfo

import gspread

ACTIVITY_LOG_SHEET = "activity_log"


def log_activity(
    spreadsheet,
    category: str,
    trace: str,
    metadata: Optional[dict] = None
):
    """Log an activity to the activity_log sheet.
    
    Args:
        spreadsheet: The gspread spreadsheet object
        category: One of: fetch_schedule, query_discord, ocr_images, process_ocr, finalize_picks
        trace: A short description of what happened
        metadata: Optional dict to be JSON-serialized (for fetch_schedule)
    """
    try:
        worksheet = spreadsheet.worksheet(ACTIVITY_LOG_SHEET)
    except gspread.WorksheetNotFound:
        print(f"Warning: {ACTIVITY_LOG_SHEET} worksheet not found, skipping log")
        return
    
    # Get current time in EST
    now_est = datetime.now(ZoneInfo("America/New_York"))
    date_str = now_est.strftime("%Y-%m-%d")
    time_str = now_est.strftime("%H:%M:%S")
    
    # Format metadata as JSON string or empty
    metadata_str = json.dumps(metadata) if metadata else ""
    
    # Create the row
    row = [date_str, time_str, category, trace, metadata_str]
    
    # Append to the bottom of the sheet
    worksheet.append_row(row, value_input_option='RAW')
