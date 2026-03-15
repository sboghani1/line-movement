#!/usr/bin/env python3
"""
Merge suspect rows (after blank separator) back into the clean sorted section.
Removes the blank separator, combines everything, sorts by date, rewrites sheet.
"""
import os, json, base64, time
from dotenv import load_dotenv
import gspread
from google.oauth2.service_account import Credentials
load_dotenv()

GOOGLE_SHEET_ID = "1LzkU7rH3OtrJckV5oMvFHyuLAnbRn9E74FO1uyfM65k"
SHEET_NAME = "parsed_picks_new"
HEADERS = ["date", "capper", "sport", "pick", "line", "game", "spread", "side", "result", "ocr_text"]

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

# Find the last blank separator row
blank_rows = [i for i, row in enumerate(all_values) if not any(cell.strip() for cell in row)]
print(f"Blank row indices: {blank_rows}")

# rows 0,1 = top metadata rows; row 2 = header
top_rows = all_values[:2]
header_row = all_values[2]

# Last blank row is the separator between clean and suspects
sep_row = blank_rows[-1]  # index 11978
print(f"Separator at index {sep_row} (sheet row {sep_row+1})")

# Clean data rows = rows 3..sep_row-1 (skip any blank rows at start)
data_rows = [r for r in all_values[3:sep_row] if any(cell.strip() for cell in r)]
suspect_rows = [r for r in all_values[sep_row+1:] if any(cell.strip() for cell in r)]

print(f"Clean data rows: {len(data_rows)}")
print(f"Suspect rows to merge back: {len(suspect_rows)}")

# Combine and sort by date
all_data = data_rows + suspect_rows
col = {h: i for i, h in enumerate(header_row)}
date_col = col.get("date", 0)
all_data.sort(key=lambda r: r[date_col] if len(r) > date_col else "")

print(f"Total data rows after merge: {len(all_data)}")

# Confirm before writing
answer = input(f"\nMerge {len(suspect_rows)} rows back and rewrite {len(all_data)} sorted rows? [y/N] ").strip().lower()
if answer != "y":
    print("Aborted.")
    exit()

# Build new sheet content
new_data = list(top_rows) + [header_row] + all_data
print(f"\nClearing and rewriting sheet ({len(new_data)} total rows)...")
ws.clear()

chunk_size = 500
col_letter = chr(ord("A") + len(header_row) - 1)
for i in range(0, len(new_data), chunk_size):
    chunk = new_data[i: i + chunk_size]
    start_row = i + 1
    end_row = start_row + len(chunk) - 1
    ws.update(f"A{start_row}:{col_letter}{end_row}", chunk, value_input_option="USER_ENTERED")
    print(f"  Wrote rows {start_row}-{end_row}")
    if i + chunk_size < len(new_data):
        time.sleep(1)

print(f"\nDone. Sheet now has {len(new_data)} rows ({len(all_data)} data rows sorted by date, no separator).")
