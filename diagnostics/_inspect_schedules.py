#!/usr/bin/env python3
"""Inspect schedule sheet formats."""
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

for sheet_name in ['nba_schedule', 'cbb_schedule', 'nhl_schedule']:
    ws = ss.worksheet(sheet_name)
    rows = ws.get_all_values()
    print(f"\n=== {sheet_name} ===")
    print(f"Total rows: {len(rows)}")
    if rows:
        print(f"Headers: {rows[0]}")
    # Show a few sample rows
    for row in rows[1:6]:
        print(f"  {row[:7]}")
