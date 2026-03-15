#!/usr/bin/env python3
"""
Insert parsed CSV rows into parsed_picks_new.

Usage:
  Pipe or redirect Claude's CSV output into this script:
    .venv/bin/python3 insert_backfill_rows.py << 'EOF'
    2026-03-07,A1 FANTASY,NBA,Kelly Oubre Jr.,o16.5 points,,,,
    2026-03-07,BANKROLL BILL,NHL,Montreal Canadiens,ML,,,,
    EOF

  Or from a file:
    .venv/bin/python3 insert_backfill_rows.py parsed_batch1.csv

Columns expected: date,capper,sport,pick,line,game,spread,side,result
(ocr_text column is optional — will be left blank if not provided)
"""

import os
import sys
import json
import base64
import time

from dotenv import load_dotenv
load_dotenv()

import gspread
from google.oauth2.service_account import Credentials

GOOGLE_SHEET_ID = "1LzkU7rH3OtrJckV5oMvFHyuLAnbRn9E74FO1uyfM65k"
OUTPUT_SHEET = "parsed_picks_new"
OUTPUT_COLUMNS = ["date", "capper", "sport", "pick", "line", "game", "spread", "side", "result", "ocr_text"]


def get_gspread_client():
    creds_b64 = os.environ.get("GOOGLE_CREDENTIALS", "")
    if not creds_b64:
        raise ValueError("GOOGLE_CREDENTIALS not set")
    creds_dict = json.loads(base64.b64decode(creds_b64).decode())
    creds = Credentials.from_service_account_info(creds_dict, scopes=[
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ])
    return gspread.authorize(creds)


def parse_csv_lines(text: str):
    rows = []
    for line in text.strip().splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        # Skip header-like lines
        if line.lower().startswith("date,capper"):
            continue

        parts = []
        current = ""
        in_quotes = False
        for ch in line:
            if ch == '"':
                in_quotes = not in_quotes
            elif ch == "," and not in_quotes:
                parts.append(current.strip())
                current = ""
            else:
                current += ch
        parts.append(current.strip())

        if len(parts) < 5:
            continue

        # Pad to 10 columns
        while len(parts) < 10:
            parts.append("")

        rows.append(parts[:10])
    return rows


def main():
    if len(sys.argv) > 1:
        with open(sys.argv[1]) as f:
            text = f.read()
    else:
        print("Paste CSV rows (Ctrl+D when done):")
        text = sys.stdin.read()

    rows = parse_csv_lines(text)
    if not rows:
        print("No valid rows found.")
        return

    print(f"Parsed {len(rows)} rows")
    for r in rows[:3]:
        print(f"  {r[:5]}")
    if len(rows) > 3:
        print(f"  ... and {len(rows)-3} more")

    client = get_gspread_client()
    ss = client.open_by_key(GOOGLE_SHEET_ID)

    try:
        ws = ss.worksheet(OUTPUT_SHEET)
    except gspread.WorksheetNotFound:
        ws = ss.add_worksheet(title=OUTPUT_SHEET, rows=5000, cols=len(OUTPUT_COLUMNS))
        ws.update("A3", [OUTPUT_COLUMNS])
        time.sleep(1)

    existing = ws.get_all_values()
    next_row = max(4, len(existing) + 1)

    cell_range = f"A{next_row}:J{next_row + len(rows) - 1}"
    ws.update(cell_range, rows, value_input_option="USER_ENTERED")
    print(f"Wrote {len(rows)} rows to {OUTPUT_SHEET} starting at row {next_row}")


if __name__ == "__main__":
    main()
