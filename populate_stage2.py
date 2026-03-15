#!/usr/bin/env python3
"""
Stage 2: Populate game, spread, and side columns in parsed_picks_new
by matching each pick against the ESPN schedule sheets.

Usage:
  .venv/bin/python3 populate_stage2.py --dry-run   # preview matches/misses, no write
  .venv/bin/python3 populate_stage2.py              # write to sheet

Sheet layout after run:
  rows 1-2:    top metadata
  row  3:      header
  rows 4+:     matched rows (game/spread/side filled, sorted by date)
  [blank row]
  [wrong-sport rows: sport tag corrected, moved here for your review]
  [blank row]
  [unmatched rows: game not in schedule, for manual review]
"""
import os, json, base64, re, time, argparse
from collections import defaultdict
from dotenv import load_dotenv
import gspread
from google.oauth2.service_account import Credentials
load_dotenv()

GOOGLE_SHEET_ID = "1LzkU7rH3OtrJckV5oMvFHyuLAnbRn9E74FO1uyfM65k"
PICKS_SHEET     = "parsed_picks_new"
SPORT_TO_SCHED  = {"nba": "nba_schedule", "cbb": "cbb_schedule", "nhl": "nhl_schedule"}
PICK_HEADERS    = ["date", "capper", "sport", "pick", "line", "game", "spread", "side", "result", "ocr_text"]

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
    r'trail blazers?|blazers?|kings?|warriors?|'
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
    r'aggies?|monarchs?|hilltoppers?|eagles?|cardinals?|'
    r'seminoles?|wolfpack|mountaineers?|bearcats?|huskies?|'
    r'terrapins?|terps?|nittany lions?|boilermakers?|hoosiers?|illini|'
    r'commodores?|volunteers?|gators?|gamecocks?|razorbacks?|'
    r'horned frogs?|cowboys?|sun devils?|utes?|lobos?|pokes?|'
    r'lumberjacks?|thunderbirds?|greyhounds?|retrievers?|'
    r'shockers?|spiders?|flyers?|musketeers?|friars?|hoyas?|'
    r'chanticleers?|penguins?|redhawks?|buffaloes?|buffs?|'
    r'hokies?|demon deacons?|yellow jackets?|scarlet knights?|'
    r'mean green|49ers?|aggies?|pride|ospreys?)\b',
    re.IGNORECASE
)

def normalize(name: str) -> str:
    n = name.lower().strip()
    n = _NOISE.sub('', n)
    n = re.sub(r'\s+', ' ', n).strip()
    return n

def team_matches(pick_name: str, schedule_name: str) -> bool:
    p = pick_name.lower().strip()
    s = schedule_name.lower().strip()
    if p == s: return True
    if p in s or s in p: return True
    pn = normalize(pick_name)
    sn = normalize(schedule_name)
    if pn and sn and (pn == sn or pn in sn or sn in pn): return True
    return False

def load_schedules(ss) -> dict:
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
            away      = row[hcol.get("away_team", 2)].strip()
            home      = row[hcol.get("home_team", 3)].strip()
            spread    = row[hcol.get("spread", 5)].strip()
            if game_date and away and home:
                by_date[game_date].append((away, home, spread))
        schedules[sport] = by_date
        print(f"  {sum(len(v) for v in by_date.values())} games from {sheet_name}")
    return schedules

def find_game(pick: str, date: str, sport: str, schedules: dict):
    """Return (away, home, spread) from sport schedule, or None."""
    for away, home, spread in schedules.get(sport, {}).get(date, []):
        if team_matches(pick, away) or team_matches(pick, home):
            return away, home, spread
    return None

def find_game_any_sport(pick: str, date: str, schedules: dict):
    """Search all sports for the pick. Returns (sport, away, home, spread) or None."""
    for sport, by_date in schedules.items():
        for away, home, spread in by_date.get(date, []):
            if team_matches(pick, away) or team_matches(pick, home):
                return sport, away, home, spread
    return None

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true",
                        help="Preview without writing to sheet")
    args = parser.parse_args()

    creds_dict = json.loads(base64.b64decode(os.environ['GOOGLE_CREDENTIALS']).decode())
    creds = Credentials.from_service_account_info(creds_dict, scopes=[
        'https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/drive'
    ])
    gc = gspread.authorize(creds)
    ss = gc.open_by_key(GOOGLE_SHEET_ID)

    print("Loading schedules...")
    schedules = load_schedules(ss)

    print(f"\nLoading {PICKS_SHEET}...")
    ws_picks = ss.worksheet(PICKS_SHEET)
    all_values = ws_picks.get_all_values()
    top_rows   = all_values[:2]
    header_row = all_values[2]
    data_rows  = [list(r) for r in all_values[3:] if any(cell.strip() for cell in r)]
    print(f"  {len(data_rows)} data rows")

    col        = {h: i for i, h in enumerate(header_row)}
    date_col   = col.get("date", 0)
    capper_col = col.get("capper", 1)
    sport_col  = col.get("sport", 2)
    pick_col   = col.get("pick", 3)
    line_col   = col.get("line", 4)
    game_col   = col.get("game", 5)
    spread_col = col.get("spread", 6)
    side_col   = col.get("side", 7)

    matched_rows     = []
    wrong_sport_rows = []   # sport tag fixed, moved to bottom section 1
    unmatched_rows   = []   # no game found anywhere, bottom section 2

    for row in data_rows:
        while len(row) < len(PICK_HEADERS): row.append("")

        date  = row[date_col].strip()
        sport = row[sport_col].strip().lower()
        pick  = row[pick_col].strip()

        if sport not in SPORT_TO_SCHED:
            unmatched_rows.append(row)
            continue

        # Try correct sport first
        result = find_game(pick, date, sport, schedules)
        if result:
            away, home, sched_spread = result
            row[game_col]   = f"{away} @ {home}"
            row[spread_col] = sched_spread
            row[side_col]   = pick
            matched_rows.append(row)
            continue

        # Try other sports — wrong sport tag
        other = find_game_any_sport(pick, date, schedules)
        if other:
            correct_sport, away, home, sched_spread = other
            row[sport_col]  = correct_sport.upper()   # fix the tag
            row[game_col]   = f"{away} @ {home}"
            row[spread_col] = sched_spread
            row[side_col]   = pick
            wrong_sport_rows.append(row)
            continue

        # Not found anywhere
        unmatched_rows.append(row)

    total = len(data_rows)
    print(f"\nResults:")
    print(f"  Matched (game filled):          {len(matched_rows)}")
    print(f"  Wrong sport tag (fixed):        {len(wrong_sport_rows)}")
    print(f"  No game found (unmatched):      {len(unmatched_rows)}")
    print(f"  Match rate:                     {len(matched_rows)/total*100:.1f}%")

    if wrong_sport_rows:
        print(f"\nWrong-sport rows (sport tag corrected — review at bottom of sheet):")
        for row in wrong_sport_rows:
            print(f"  [{row[date_col]}] {row[capper_col]} | sport fixed → {row[sport_col]} | {repr(row[pick_col])} {row[line_col]}")

    if unmatched_rows:
        print(f"\nUnmatched rows (no game in any schedule — review at bottom of sheet):")
        for row in unmatched_rows:
            print(f"  [{row[date_col]}] {row[capper_col]} | {row[sport_col]} | {repr(row[pick_col])} {row[line_col]}")

    if args.dry_run:
        print("\n[dry-run] Not writing to sheet.")
        return

    # Sort matched rows by date
    matched_rows.sort(key=lambda r: r[date_col] if len(r) > date_col else "")

    EMPTY = [""] * len(header_row)
    new_data = list(top_rows) + [header_row] + matched_rows
    if wrong_sport_rows:
        new_data.append(EMPTY)
        new_data += wrong_sport_rows
    if unmatched_rows:
        new_data.append(EMPTY)
        new_data += unmatched_rows

    print(f"\nClearing and rewriting sheet ({len(new_data)} total rows)...")
    ws_picks.clear()

    chunk_size = 500
    col_letter = chr(ord("A") + len(header_row) - 1)
    for i in range(0, len(new_data), chunk_size):
        chunk = new_data[i: i + chunk_size]
        start_row = i + 1
        end_row   = start_row + len(chunk) - 1
        ws_picks.update(range_name=f"A{start_row}:{col_letter}{end_row}", values=chunk, value_input_option="USER_ENTERED")
        print(f"  Wrote rows {start_row}-{end_row}")
        if i + chunk_size < len(new_data):
            time.sleep(1)

    print(f"\nDone.")
    print(f"  {len(matched_rows)} matched rows (game/spread/side filled) sorted by date")
    if wrong_sport_rows:
        print(f"  {len(wrong_sport_rows)} wrong-sport rows (sport tag fixed) after first separator — review and move up")
    if unmatched_rows:
        print(f"  {len(unmatched_rows)} unmatched rows after second separator — review manually")

if __name__ == "__main__":
    main()
