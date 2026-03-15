#!/usr/bin/env python3
"""Diagnose the 77 misses in detail."""
import os, json, base64, re
from collections import defaultdict
from dotenv import load_dotenv
import gspread
from google.oauth2.service_account import Credentials
load_dotenv()

GOOGLE_SHEET_ID = "1LzkU7rH3OtrJckV5oMvFHyuLAnbRn9E74FO1uyfM65k"
SPORT_TO_SCHED = {"nba": "nba_schedule", "cbb": "cbb_schedule", "nhl": "nhl_schedule"}

_NOISE = re.compile(
    r'\b(blue devils?|crimson tide|golden eagles?|golden bears?|golden gophers?|'
    r'golden knights?|golden flashes?|golden panthers?|golden bulls?|'
    r'red hawks?|red raiders?|red storm|red foxes?|'
    r'fighting irish|fighting illini|fighting hawks?|'
    r'tar heels?|hoyas?|retrievers?|'
    r'wolverines?|buckeyes?|badgers?|hawkeyes?|cyclones?|'
    r'cornhuskers?|huskers?|longhorns?|sooners?|jayhawks?|wildcats?|'
    r'bulldogs?|tigers?|bears?|wolves?|timberwolves?|'
    r'cavaliers?|cavs?|celtics?|nets?|knicks?|bucks?|bulls?|'
    r'lakers?|clippers?|suns?|heat|magic|pistons?|pacers?|'
    r'hawks?|hornets?|wizards?|raptors?|grizzlies?|pelicans?|'
    r'spurs?|mavericks?|mavs?|rockets?|thunder|nuggets?|jazz|'
    r'timberwolves?|trail blazers?|blazers?|kings?|warriors?|'
    r'lightning|panthers?|rangers?|islanders?|devils?|flyers?|'
    r'penguins?|capitals?|caps?|hurricanes?|canes?|'
    r'blue jackets?|red wings?|bruins?|canadiens?|habs?|'
    r'senators?|maple leafs?|leafs?|sabres?|wild|blackhawks?|'
    r'blues?|avalanche?|avs?|stars?|predators?|preds?|jets?|'
    r'coyotes?|canucks?|flames?|oilers?|sharks?|ducks?|kraken|'
    r'trojans?|rebels?|miners?|roadrunners?|'
    r'ramblers?|racers?|chippewas?|broncos?|falcons?|'
    r'antelopes?|lions?|billikens?|midshipmen|black knights?|'
    r'tommies?|owls?|jaguars?|rams?|aztecs?|spartans?|'
    r'aggies?|roadrunners?|monarchs?|'
    r'hilltoppers?|eagles?|cardinals?|seminoles?|wolfpack|'
    r'mountaineers?|bearcats?|huskies?|terrapins?|terps?|'
    r'nittany lions?|boilermakers?|hoosiers?|illini|'
    r'commodores?|volunteers?|gators?|gamecocks?|razorbacks?|'
    r'wolverines?|buckeyes?|badgers?|hawkeyes?|cyclones?|'
    r'longhorns?|horned frogs?|cowboys?|red raiders?|'
    r'sun devils?|utes?|lobos?|pokes?|'
    r'lumberjacks?|thunderbirds?|greyhounds?|retrievers?)\b',
    re.IGNORECASE
)

def normalize(name):
    n = name.lower().strip()
    n = _NOISE.sub('', n)
    n = re.sub(r'\s+', ' ', n).strip()
    return n

def team_matches(pick_name, schedule_name):
    p = pick_name.lower().strip()
    s = schedule_name.lower().strip()
    if p == s: return True
    if p in s or s in p: return True
    pn = normalize(pick_name)
    sn = normalize(schedule_name)
    if pn and sn and (pn == sn or pn in sn or sn in pn): return True
    return False

creds_dict = json.loads(base64.b64decode(os.environ['GOOGLE_CREDENTIALS']).decode())
creds = Credentials.from_service_account_info(creds_dict, scopes=[
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive'
])
gc = gspread.authorize(creds)
ss = gc.open_by_key(GOOGLE_SHEET_ID)

print("Loading schedules...")
schedules = {}
for sport, sheet_name in SPORT_TO_SCHED.items():
    ws = ss.worksheet(sheet_name)
    rows = ws.get_all_values()
    headers = rows[0]
    hcol = {h: i for i, h in enumerate(headers)}
    by_date = defaultdict(list)
    for row in rows[1:]:
        while len(row) < len(headers): row.append("")
        game_date = row[hcol.get("game_date", 1)].strip()
        away = row[hcol.get("away_team", 2)].strip()
        home = row[hcol.get("home_team", 3)].strip()
        spread = row[hcol.get("spread", 5)].strip()
        if game_date and away and home:
            by_date[game_date].append((away, home, spread))
    schedules[sport] = by_date

print("Loading picks...")
ws = ss.worksheet('parsed_picks_new')
all_values = ws.get_all_values()
header_row = all_values[2]
col = {h: i for i, h in enumerate(header_row)}
date_col  = col.get("date", 0)
cap_col   = col.get("capper", 1)
sport_col = col.get("sport", 2)
pick_col  = col.get("pick", 3)
line_col  = col.get("line", 4)
game_col  = col.get("game", 5)

data_rows = [r for r in all_values[3:] if any(cell.strip() for cell in r)]

# Categorise misses
wrong_sport = []   # pick found in a different sport's schedule
no_schedule = []   # date has zero games in that sport
not_found   = []   # date has games but team not found
holiday_gap = []   # date has games in schedule but team not matched — check if any game exists at all

for row in data_rows:
    while len(row) < 10: row.append("")
    date  = row[date_col].strip()
    sport = row[sport_col].strip().lower()
    pick  = row[pick_col].strip()
    game  = row[game_col].strip()
    if game: continue  # already filled
    if sport not in SPORT_TO_SCHED: continue

    games_on_date = schedules[sport].get(date, [])

    # Try to find in correct sport
    found = any(team_matches(pick, away) or team_matches(pick, home) for away, home, _ in games_on_date)
    if found: continue  # matched — counted above

    # Check other sports
    other_sport_match = None
    for other_sport, sched in schedules.items():
        if other_sport == sport: continue
        for away, home, _ in sched.get(date, []):
            if team_matches(pick, away) or team_matches(pick, home):
                other_sport_match = other_sport
                break
        if other_sport_match:
            break

    if other_sport_match:
        wrong_sport.append((date, row[cap_col], row[sport_col], pick, row[line_col], other_sport_match))
    elif not games_on_date:
        no_schedule.append((date, row[cap_col], row[sport_col], pick, row[line_col]))
    else:
        # Games exist on that date but team not found — check if it's a naming issue
        candidates = [(away, home) for away, home, _ in games_on_date]
        not_found.append((date, row[cap_col], row[sport_col], pick, row[line_col], candidates[:3]))

print(f"\n{'='*60}")
print(f"WRONG SPORT TAG ({len(wrong_sport)} rows) — pick found in different sport schedule:")
print(f"  Format: [date] capper | tagged_as → actually_{sport} | pick line")
for date, cap, sport, pick, line, actual in wrong_sport:
    print(f"  [{date}] {cap} | {sport} → {actual} | {repr(pick)} {line}")

print(f"\n{'='*60}")
print(f"NO SCHEDULE FOR DATE ({len(no_schedule)} rows) — 0 games in that sport on that date:")
for date, cap, sport, pick, line in no_schedule:
    print(f"  [{date}] {cap} | {sport} | {repr(pick)} {line}")

print(f"\n{'='*60}")
print(f"TEAM NOT IN SCHEDULE ({len(not_found)} rows) — games exist but team name not matched:")
for date, cap, sport, pick, line, candidates in not_found:
    print(f"  [{date}] {cap} | {sport} | {repr(pick)} {line}")
    print(f"    schedule has: {[f'{a} @ {h}' for a,h in candidates]}")
