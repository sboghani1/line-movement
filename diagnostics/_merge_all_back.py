#!/usr/bin/env python3
"""Merge all rows after separators back into sorted clean section."""
import os, json, base64, time
from dotenv import load_dotenv
import gspread
from google.oauth2.service_account import Credentials
load_dotenv()

GOOGLE_SHEET_ID = "1LzkU7rH3OtrJckV5oMvFHyuLAnbRn9E74FO1uyfM65k"
SHEET_NAME = "parsed_picks_new"

creds_dict = json.loads(base64.b64decode(os.environ['GOOGLE_CREDENTIALS']).decode())
creds = Credentials.from_service_account_info(creds_dict, scopes=[
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive'
])
gc = gspread.authorize(creds)
ss = gc.open_by_key(GOOGLE_SHEET_ID)
ws = ss.worksheet(SHEET_NAME)

print("Loading sheet...")
all_values = ws.get_all_values()
print(f"Total rows: {len(all_values)}")

top_rows   = all_values[:2]
header_row = all_values[2]
col = {h: i for i, h in enumerate(header_row)}
date_col = col.get("date", 0)

# Collect all non-blank data rows (skip rows 0-2)
data_rows = [r for r in all_values[3:] if any(cell.strip() for cell in r)]
print(f"Non-blank data rows: {len(data_rows)}")

data_rows.sort(key=lambda r: r[date_col] if len(r) > date_col else "")

new_data = list(top_rows) + [header_row] + data_rows
print(f"Rewriting {len(new_data)} total rows (no separators)...")
ws.clear()

chunk_size = 500
col_letter = chr(ord("A") + len(header_row) - 1)
for i in range(0, len(new_data), chunk_size):
    chunk = new_data[i: i + chunk_size]
    start_row = i + 1
    end_row = start_row + len(chunk) - 1
    ws.update(range_name=f"A{start_row}:{col_letter}{end_row}", values=chunk, value_input_option="USER_ENTERED")
    print(f"  Wrote rows {start_row}-{end_row}")
    if i + chunk_size < len(new_data):
        time.sleep(1)

print(f"\nDone. {len(data_rows)} rows sorted by date, no separators.")
