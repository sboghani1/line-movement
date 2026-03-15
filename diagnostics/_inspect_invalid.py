#!/usr/bin/env python3
"""Inspect wrong-sport and prop rows in detail."""
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
header_row = all_values[2]
col = {h: i for i, h in enumerate(header_row)}
date_col   = col.get("date", 0)
capper_col = col.get("capper", 1)
sport_col  = col.get("sport", 2)
pick_col   = col.get("pick", 3)
line_col   = col.get("line", 4)
ocr_col    = col.get("ocr_text", 9)

VALID_SPORTS = {"nba", "cbb", "nhl"}
data_rows = [r for r in all_values[3:] if any(cell.strip() for cell in r)]
for row in data_rows:
    while len(row) < 10:
        row.append("")

print("=== WRONG SPORT (all) ===")
for row in data_rows:
    sport = row[sport_col].strip()
    if sport.lower() not in VALID_SPORTS:
        print(f"  sport={repr(sport)} capper={repr(row[capper_col])} pick={repr(row[pick_col])} line={repr(row[line_col])}")
        print(f"  date={row[date_col]} OCR: {row[ocr_col][:120]}")

PROP_KEYWORDS = [
    "passing yard", "rushing yard", "receiving yard", "recv yard",
    "receptions", "completions", "touchdowns", "td scorer",
    "points scored", "rebounds", "assists", "blocks", "steals",
    "strikeouts", "home run", "hits", "rbi", "saves",
    "anytime td", "first td", "last td",
    "over .5", "over 1.5", "over 2.5", "over 3.5", "over 4.5",
    "rush attempts", "pass attempts",
]

print("\n=== PROPS (first 20) ===")
count = 0
for row in data_rows:
    sport = row[sport_col].strip()
    pick = row[pick_col].strip()
    line = row[line_col].strip()
    if sport.lower() not in VALID_SPORTS:
        continue
    text = (line + " " + pick).lower()
    matched = [kw for kw in PROP_KEYWORDS if kw in text]
    if matched:
        print(f"  [{row[date_col]}] {row[capper_col]} | {sport} | pick={repr(pick)} line={repr(line)}")
        print(f"  matched kw: {matched}")
        print(f"  OCR: {row[ocr_col][:120]}")
        print()
        count += 1
        if count >= 20:
            break
