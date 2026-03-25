#!/usr/bin/env python3
"""
Invalid row cleanup for parsed_picks_new.

Strategy per user request:
  - Duplicates: delete directly (move to bottom section 1)
  - Other invalid (wrong sport, props, parlays, totals): move to bottom section 2 for manual review
  - Clean rows: sorted by date at top

Sheet layout after run:
  rows 1-2:    top metadata
  row  3:      header
  rows 4+:     clean rows (sorted by date)
  [blank row]
  [duplicate rows]
  [blank row]
  [other invalid rows - for manual review]
"""
import os, json, base64, re, time
from dotenv import load_dotenv
import gspread
from google.oauth2.service_account import Credentials
load_dotenv()

GOOGLE_SHEET_ID = "1LzkU7rH3OtrJckV5oMvFHyuLAnbRn9E74FO1uyfM65k"
SHEET_NAME = "parsed_picks_new"
HEADERS = ["date", "capper", "sport", "pick", "line", "game", "spread", "result", "ocr_text"]
VALID_SPORTS = {"nba", "cbb", "nhl"}

# Use word-boundary aware matching for props
PROP_PATTERNS = [
    r'\bpassing yards?\b', r'\brushing yards?\b', r'\breceiving yards?\b', r'\brecv yards?\b',
    r'\breceptions\b', r'\bcompletions\b', r'\btouchdowns?\b', r'\btd scorer\b',
    r'\bpoints scored\b', r'\brebounds?\b', r'\bassists?\b', r'\bblocks?\b', r'\bsteals?\b',
    r'\bstrikeouts?\b', r'\bhome run\b', r'\b\d+ hits?\b', r'\brbi\b', r'\bsaves?\b',
    r'\banytime td\b', r'\bfirst td\b', r'\blast td\b',
    r'\bover [0-9]+\.5 recv\b', r'\brush attempts?\b', r'\bpass attempts?\b',
    r'\bover \d+\.\d+ (passing|rushing|receiving|recv|receptions|completions|touchdowns|strikeouts|rebounds|assists)\b',
]
PROP_RE = re.compile('|'.join(PROP_PATTERNS), re.IGNORECASE)

def is_total(line: str) -> bool:
    """Over/Under game totals: line is just O/U + number (not a spread)."""
    l = line.strip()
    return bool(re.match(r'^[OoUu]\s*\d', l))

def is_prop(line: str, pick: str) -> bool:
    text = line + " " + pick
    return bool(PROP_RE.search(text))

def is_parlay(pick: str) -> bool:
    if "/" not in pick:
        return False
    parts = [p.strip() for p in pick.split("/")]
    return sum(1 for p in parts if len(p) >= 2) >= 2

def is_wrong_sport(sport: str) -> bool:
    return sport.lower().strip() not in VALID_SPORTS

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

top_rows = all_values[:2]
header_row = all_values[2]
data_rows = [r for r in all_values[3:] if any(cell.strip() for cell in r)]
print(f"Data rows: {len(data_rows)}")

col = {h: i for i, h in enumerate(header_row)}
date_col   = col.get("date", 0)
capper_col = col.get("capper", 1)
sport_col  = col.get("sport", 2)
pick_col   = col.get("pick", 3)
line_col   = col.get("line", 4)

clean_rows    = []
duplicates    = []
other_invalid = []  # wrong sport, props, parlays, totals — manual review

seen = {}

for row in data_rows:
    while len(row) < len(HEADERS):
        row.append("")

    date   = row[date_col].strip()
    capper = row[capper_col].strip()
    sport  = row[sport_col].strip()
    pick   = row[pick_col].strip()
    line   = row[line_col].strip()

    reason = None

    if is_wrong_sport(sport):
        reason = f"wrong_sport:{sport!r}"
    elif is_parlay(pick):
        reason = "parlay"
    elif is_total(line):
        reason = "total"
    elif is_prop(line, pick):
        reason = "prop"

    if reason:
        other_invalid.append((reason, row))
        continue

    # Duplicate check (after filtering garbage)
    key = (date, capper.lower(), sport.lower(), pick.lower(), line.lower())
    if key in seen:
        duplicates.append(row)
        continue
    seen[key] = True

    clean_rows.append(row)

print(f"\nResults:")
print(f"  Clean:         {len(clean_rows)}")
print(f"  Duplicates:    {len(duplicates)}  (will be auto-deleted)")
print(f"  Other invalid: {len(other_invalid)}  (moved to bottom for manual review)")
print(f"    breakdown: ", end="")
from collections import Counter
reasons = Counter(r for r, _ in other_invalid)
print(", ".join(f"{v} {k}" for k, v in reasons.items()))

# Show samples
def show_sample(label, rows, n=5):
    if not rows:
        return
    print(f"\n--- {label} (showing up to {n}) ---")
    for row in rows[:n]:
        print(f"  [{row[date_col]}] {row[capper_col]} | {row[sport_col]} | pick={repr(row[pick_col])} line={repr(row[line_col])}")

show_sample("Duplicates", duplicates)
show_sample("Other invalid", [r for _, r in other_invalid])

total_removed = len(duplicates)
total_bottom  = len(other_invalid)
print(f"\nWill delete {total_removed} duplicates and move {total_bottom} rows to bottom for review.")

answer = input("\nProceed? [y/N] ").strip().lower()
if answer != "y":
    print("Aborted.")
    exit()

# Sort clean rows by date
clean_rows.sort(key=lambda r: r[date_col] if len(r) > date_col else "")

# Build new sheet: clean rows, blank, duplicates, blank, other invalid
EMPTY = [""] * len(header_row)
new_data = list(top_rows) + [header_row] + clean_rows
if duplicates:
    new_data.append(EMPTY)
    new_data += duplicates
if other_invalid:
    new_data.append(EMPTY)
    new_data += [r for _, r in other_invalid]

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

print(f"\nDone.")
print(f"  {len(clean_rows)} clean rows sorted by date at top")
print(f"  {len(duplicates)} duplicate rows after first blank separator (delete them)")
print(f"  {len(other_invalid)} other invalid rows after second blank separator (review manually)")
