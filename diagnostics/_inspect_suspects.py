#!/usr/bin/env python3
"""Inspect sheet structure and find suspect rows."""
import os, json, base64
from dotenv import load_dotenv
import gspread
from google.oauth2.service_account import Credentials
load_dotenv()
creds_dict = json.loads(base64.b64decode(os.environ['GOOGLE_CREDENTIALS']).decode())
creds = Credentials.from_service_account_info(creds_dict, scopes=[
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive'
])
gc = gspread.authorize(creds)
ss = gc.open_by_key('1LzkU7rH3OtrJckV5oMvFHyuLAnbRn9E74FO1uyfM65k')
ws = ss.worksheet('parsed_picks_new')
all_values = ws.get_all_values()
print(f'Total rows: {len(all_values)}')
print(f'Row 1 (index 0): {all_values[0][:5]}')
print(f'Row 2 (index 1): {all_values[1][:5]}')
print(f'Row 3 (index 2): {all_values[2][:5]}')
print(f'Row 4 (index 3): {all_values[3][:5]}')
print()

# Find blank separator rows
print('Searching for blank separator rows...')
blank_rows = []
for i, row in enumerate(all_values):
    if not any(cell.strip() for cell in row):
        blank_rows.append(i)

print(f'Blank rows at indices: {blank_rows}')
if blank_rows:
    # Last blank row is probably the separator
    sep_row = blank_rows[-1]
    print(f'Last blank row (separator): index {sep_row} = sheet row {sep_row+1}')
    suspect_rows = all_values[sep_row+1:]
    print(f'Rows after last blank: {len(suspect_rows)}')
    header = all_values[2]
    col = {h: i for i, h in enumerate(header)}
    print()
    for i, row in enumerate(suspect_rows):
        pick = row[col.get('pick', 3)]
        ocr = row[col.get('ocr_text', 9)]
        date = row[col.get('date', 0)]
        capper = row[col.get('capper', 1)]
        sport = row[col.get('sport', 2)]
        line = row[col.get('line', 4)]
        print(f'[{i+1}] {date} | {capper} | {sport} | pick={repr(pick)} line={repr(line)}')
        print(f'     OCR: {ocr[:200]}')
        print()
