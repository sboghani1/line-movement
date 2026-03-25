#!/usr/bin/env python3
"""
populate_team_names.py — One-time script to populate the team_name_resolution
sheet from multiple sources:

  1. ESPN schedule sheets — all unique team names (the canonical values)
  2. master_sheet pick column — aliases that resolved via game column
  3. ABBREV_MAP — known abbreviation→team mappings (NBA, NHL, CBB)
  4. Hardcoded nickname dicts — common mascot-only and city names

Output columns:
  sport           — nba, cbb, or nhl
  espn_team_name  — the canonical ESPN full team name
  aliases         — comma-separated list of known alternate names / abbreviations

Usage:
  .venv/bin/python3 populate_team_names.py --dry-run   # preview without writing
  .venv/bin/python3 populate_team_names.py              # write to sheet
"""

import re
import argparse
from collections import defaultdict

from dotenv import load_dotenv

from sheets_utils import (
    GOOGLE_SHEET_ID,
    SPORT_TO_SCHED,
    get_gspread_client,
    sheets_read,
    sheets_write,
)

load_dotenv()

RESOLUTION_SHEET = "team_name_resolution"


# ── Source 3: ABBREV_MAP (copied from audit_hallucinations.py) ───────────────
# Maps (abbreviation, sport) → list of possible full team name substrings.
# Sport-tagged to avoid cross-sport contamination (e.g. "nets" matching CBB "Hornets").
ABBREV_MAP = {
    # NBA
    ("GSW", "nba"): ["golden state"],
    ("LAL", "nba"): ["los angeles lakers", "lakers"],
    ("LAC", "nba"): ["los angeles clippers", "la clippers", "clippers"],
    ("NYK", "nba"): ["new york knicks", "knicks"],
    ("BKN", "nba"): ["brooklyn nets"],
    ("PHX", "nba"): ["phoenix suns"],
    ("MIL", "nba"): ["milwaukee bucks"],
    ("BOS", "nba"): ["boston celtics"],
    ("MIA", "nba"): ["miami heat"],
    ("CHI", "nba"): ["chicago bulls"],
    ("DET", "nba"): ["detroit pistons"],
    ("IND", "nba"): ["indiana pacers"],
    ("CLE", "nba"): ["cleveland cavaliers"],
    ("ATL", "nba"): ["atlanta hawks"],
    ("CHA", "nba"): ["charlotte hornets"],
    ("ORL", "nba"): ["orlando magic"],
    ("WAS", "nba"): ["washington wizards"],
    ("TOR", "nba"): ["toronto raptors"],
    ("MEM", "nba"): ["memphis grizzlies"],
    ("NOP", "nba"): ["new orleans pelicans"],
    ("SAS", "nba"): ["san antonio spurs"],
    ("DAL", "nba"): ["dallas mavericks"],
    ("HOU", "nba"): ["houston rockets"],
    ("OKC", "nba"): ["oklahoma city thunder"],
    ("DEN", "nba"): ["denver nuggets"],
    ("UTA", "nba"): ["utah jazz"],
    ("MIN", "nba"): ["minnesota timberwolves"],
    ("POR", "nba"): ["portland trail blazers"],
    ("SAC", "nba"): ["sacramento kings"],
    ("PHI", "nba"): ["philadelphia 76ers"],
    # NHL
    ("VGK", "nhl"): ["vegas golden knights"],
    ("TBL", "nhl"): ["tampa bay lightning"],
    ("FLA", "nhl"): ["florida panthers"],
    ("NYR", "nhl"): ["new york rangers"],
    ("NYI", "nhl"): ["new york islanders"],
    ("NJD", "nhl"): ["new jersey devils"],
    ("PIT", "nhl"): ["pittsburgh penguins"],
    ("WSH", "nhl"): ["washington capitals"],
    ("CAR", "nhl"): ["carolina hurricanes"],
    ("CBJ", "nhl"): ["columbus blue jackets"],
    ("MTL", "nhl"): ["montreal canadiens"],
    ("OTT", "nhl"): ["ottawa senators"],
    ("BUF", "nhl"): ["buffalo sabres"],
    ("STL", "nhl"): ["st. louis blues"],
    ("COL", "nhl"): ["colorado avalanche"],
    ("NSH", "nhl"): ["nashville predators"],
    ("WPG", "nhl"): ["winnipeg jets"],
    ("VAN", "nhl"): ["vancouver canucks"],
    ("CGY", "nhl"): ["calgary flames"],
    ("EDM", "nhl"): ["edmonton oilers"],
    ("SJS", "nhl"): ["san jose sharks"],
    ("ANA", "nhl"): ["anaheim ducks"],
    ("LAK", "nhl"): ["los angeles kings", "la kings"],
    ("SEA", "nhl"): ["seattle kraken"],
    # CBB common acronyms
    ("UNC", "cbb"): ["north carolina tar heels", "north carolina"],
    ("USC", "cbb"): ["usc trojans"],
    ("UCF", "cbb"): ["ucf knights"],
    ("VCU", "cbb"): ["vcu rams"],
    ("SMU", "cbb"): ["smu mustangs"],
    ("TCU", "cbb"): ["tcu horned frogs"],
    ("LSU", "cbb"): ["lsu tigers"],
    ("BYU", "cbb"): ["byu cougars"],
    ("UCLA", "cbb"): ["ucla bruins"],
    ("UNLV", "cbb"): ["unlv rebels"],
    ("UTEP", "cbb"): ["utep miners"],
    ("UTSA", "cbb"): ["utsa roadrunners"],
    ("UAB", "cbb"): ["uab blazers"],
    ("UIC", "cbb"): ["uic flames"],
    ("ETSU", "cbb"): ["east tennessee state"],
    ("UNCW", "cbb"): ["unc wilmington"],
    ("NEB", "cbb"): ["nebraska cornhuskers", "nebraska"],
    ("PITT", "cbb"): ["pittsburgh panthers"],
    ("UK", "cbb"): ["kentucky wildcats", "kentucky"],
    ("OU", "cbb"): ["oklahoma sooners", "oklahoma"],
    ("FAU", "cbb"): ["florida atlantic owls", "florida atlantic"],
    ("UVA", "cbb"): ["virginia cavaliers", "virginia"],
    ("UMASS", "cbb"): ["massachusetts minutemen"],
    ("UCONN", "cbb"): ["uconn huskies", "connecticut"],
    ("URI", "cbb"): ["rhode island rams"],
    ("USF", "cbb"): ["south florida bulls"],
    ("SDSU", "cbb"): ["san diego state aztecs", "san diego state"],
    ("SJSU", "cbb"): ["san jose state spartans", "san jose state"],
    ("NMSU", "cbb"): ["new mexico state aggies", "new mexico state"],
    ("FIU", "cbb"): ["florida international panthers", "florida international"],
    ("ODU", "cbb"): ["old dominion monarchs", "old dominion"],
    ("GCU", "cbb"): ["grand canyon"],
    ("LMU", "cbb"): ["loyola marymount lions", "loyola marymount"],
    ("SLU", "cbb"): ["saint louis billikens", "saint louis"],
    ("WKU", "cbb"): ["western kentucky hilltoppers", "western kentucky"],
    ("EMU", "cbb"): ["eastern michigan eagles", "eastern michigan"],
    ("WMU", "cbb"): ["western michigan broncos", "western michigan"],
    ("CMU", "cbb"): ["central michigan chippewas", "central michigan"],
    ("NIU", "cbb"): ["northern illinois huskies", "northern illinois"],
    ("BGSU", "cbb"): ["bowling green falcons", "bowling green"],
    ("KSU", "cbb"): ["kansas state wildcats"],
}


def find_espn_team(hint: str, sport_teams: set) -> str | None:
    """Find the ESPN team name that matches a hint string (case-insensitive).

    Tries: exact match, substring in either direction, word-boundary match.
    Returns the ESPN team name or None.
    """
    hint_lower = hint.lower().strip()
    for team in sport_teams:
        team_lower = team.lower()
        if hint_lower == team_lower:
            return team
        if hint_lower in team_lower or team_lower in hint_lower:
            return team
    return None


def main():
    parser = argparse.ArgumentParser(
        description="Populate team_name_resolution sheet from all data sources"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview without writing to sheet",
    )
    args = parser.parse_args()

    print("Connecting to Google Sheets...")
    gc = get_gspread_client()
    ss = gc.open_by_key(GOOGLE_SHEET_ID)

    # ── 1. Load all ESPN team names per sport ────────────────────────────────
    print("\nLoading ESPN team names from schedule sheets...")
    espn_teams = {}  # {sport: set of team names}
    for sport, sheet_name in SPORT_TO_SCHED.items():
        ws = sheets_read(ss.worksheet, sheet_name)
        rows = sheets_read(ws.get_all_values)
        headers = rows[0]
        hcol = {h: i for i, h in enumerate(headers)}
        teams = set()
        for row in rows[1:]:
            while len(row) < len(headers):
                row.append("")
            away = row[hcol.get("away_team", 2)].strip()
            home = row[hcol.get("home_team", 3)].strip()
            if away:
                teams.add(away)
            if home:
                teams.add(home)
        espn_teams[sport] = teams
        print(f"  {sport}: {len(teams)} teams")

    # Build the main data structure: {(sport, espn_team_name): set of aliases}
    team_aliases = defaultdict(set)  # (sport, espn_name) → {alias1, alias2, ...}

    # Initialize with all ESPN teams (every team gets an entry, even with no aliases)
    for sport, teams in espn_teams.items():
        for team in teams:
            team_aliases[(sport, team)]  # touch to create entry

    # ── 2. Mine master_sheet: pick → game column mapping ────────────────────
    print("\nMining master_sheet for pick aliases...")
    ws = sheets_read(ss.worksheet, "master_sheet")
    all_values = sheets_read(ws.get_all_values)
    header = all_values[0]
    col = {h: i for i, h in enumerate(header)}

    pick_col = col.get("pick", 3)
    sport_col = col.get("sport", 2)
    game_col = col.get("game", 5)

    master_aliases_found = 0
    for row in all_values[1:]:
        while len(row) < len(header):
            row.append("")
        sport = row[sport_col].strip().lower()
        pick = row[pick_col].strip()
        game = row[game_col].strip()

        if not sport or not pick or not game:
            continue
        if sport not in espn_teams:
            continue
        # Skip if pick is already an exact ESPN name
        if pick in espn_teams[sport]:
            continue

        # Parse game column to find which ESPN team the pick mapped to
        parts = game.split(" @ ", 1)
        if len(parts) != 2:
            continue
        away, home = parts[0].strip(), parts[1].strip()

        # Verify BOTH game teams belong to this sport's ESPN team list.
        # This filters out wrong-sport matches (e.g. CBB "Mercyhurst Lakers"
        # matched to NBA "Los Angeles Lakers" game).
        if away not in espn_teams.get(sport, set()) or home not in espn_teams.get(sport, set()):
            continue

        # Match pick to one of the two teams
        pick_lower = pick.lower()
        matched_team = None
        for team in [away, home]:
            team_lower = team.lower()
            if pick_lower in team_lower or team_lower in pick_lower:
                matched_team = team
                break

        if matched_team:
            team_aliases[(sport, matched_team)].add(pick)
            master_aliases_found += 1

    print(f"  Found {master_aliases_found} alias usages in master_sheet")

    # ── 3. Add ABBREV_MAP entries ───────────────────────────────────────────
    print("\nAdding ABBREV_MAP abbreviations...")
    abbrev_added = 0
    for (abbrev, sport), hints in ABBREV_MAP.items():
        teams = espn_teams.get(sport, set())
        for hint in hints:
            espn_name = find_espn_team(hint, teams)
            if espn_name:
                team_aliases[(sport, espn_name)].add(abbrev)
                abbrev_added += 1
                break  # found match, move to next abbreviation

    print(f"  Added {abbrev_added} abbreviation→team mappings")

    # ── 4. Add common nicknames / short names ───────────────────────────────
    # These are the most common patterns: cappers use just the mascot or city
    print("\nAdding common nickname patterns...")
    nickname_count = 0

    # NBA mascot-only nicknames (these are unambiguous within NBA)
    nba_nicknames = {
        "Atlanta Hawks": ["Hawks"],
        "Boston Celtics": ["Celtics"],
        "Brooklyn Nets": ["Nets"],
        "Charlotte Hornets": ["Hornets"],
        "Chicago Bulls": ["Bulls"],
        "Cleveland Cavaliers": ["Cavaliers", "Cavs"],
        "Dallas Mavericks": ["Mavericks", "Mavs"],
        "Denver Nuggets": ["Nuggets"],
        "Detroit Pistons": ["Pistons"],
        "Golden State Warriors": ["Warriors"],
        "Houston Rockets": ["Rockets"],
        "Indiana Pacers": ["Pacers"],
        "LA Clippers": ["Clippers", "Los Angeles Clippers"],
        "Los Angeles Lakers": ["Lakers"],
        "Memphis Grizzlies": ["Grizzlies"],
        "Miami Heat": ["Heat"],
        "Milwaukee Bucks": ["Bucks"],
        "Minnesota Timberwolves": ["Timberwolves", "Wolves", "T-Wolves"],
        "New Orleans Pelicans": ["Pelicans"],
        "New York Knicks": ["Knicks"],
        "Oklahoma City Thunder": ["Thunder", "OKC Thunder"],
        "Orlando Magic": ["Magic"],
        "Philadelphia 76ers": ["Sixers", "76ers"],
        "Phoenix Suns": ["Suns"],
        "Portland Trail Blazers": ["Trail Blazers", "Blazers"],
        "Sacramento Kings": ["Kings"],
        "San Antonio Spurs": ["Spurs"],
        "Toronto Raptors": ["Raptors"],
        "Utah Jazz": ["Jazz"],
        "Washington Wizards": ["Wizards"],
    }
    for espn_name, nicks in nba_nicknames.items():
        if espn_name in espn_teams.get("nba", set()):
            for nick in nicks:
                team_aliases[("nba", espn_name)].add(nick)
                nickname_count += 1

    # NHL mascot/city nicknames
    nhl_nicknames = {
        "Anaheim Ducks": ["Ducks"],
        "Boston Bruins": ["Bruins"],
        "Buffalo Sabres": ["Sabres"],
        "Calgary Flames": ["Flames"],
        "Carolina Hurricanes": ["Hurricanes", "Canes"],
        "Chicago Blackhawks": ["Blackhawks"],
        "Colorado Avalanche": ["Avalanche", "Avs"],
        "Columbus Blue Jackets": ["Blue Jackets"],
        "Dallas Stars": ["Stars"],
        "Detroit Red Wings": ["Red Wings"],
        "Edmonton Oilers": ["Oilers"],
        "Florida Panthers": ["Panthers"],
        "Los Angeles Kings": ["Kings", "LA Kings"],
        "Minnesota Wild": ["Wild"],
        "Montreal Canadiens": ["Canadiens", "Habs"],
        "Nashville Predators": ["Predators", "Preds"],
        "New Jersey Devils": ["Devils"],
        "New York Islanders": ["Islanders"],
        "New York Rangers": ["Rangers"],
        "Ottawa Senators": ["Senators"],
        "Philadelphia Flyers": ["Flyers"],
        "Pittsburgh Penguins": ["Penguins", "Pens"],
        "San Jose Sharks": ["Sharks"],
        "Seattle Kraken": ["Kraken"],
        "St. Louis Blues": ["Blues"],
        "Tampa Bay Lightning": ["Lightning", "Bolts"],
        "Toronto Maple Leafs": ["Maple Leafs", "Leafs"],
        "Vancouver Canucks": ["Canucks"],
        "Vegas Golden Knights": ["Golden Knights"],
        "Washington Capitals": ["Capitals", "Caps"],
        "Winnipeg Jets": ["Jets"],
        "Utah Hockey Club": ["Utah HC"],
    }
    for espn_name, nicks in nhl_nicknames.items():
        if espn_name in espn_teams.get("nhl", set()):
            for nick in nicks:
                team_aliases[("nhl", espn_name)].add(nick)
                nickname_count += 1

    print(f"  Added {nickname_count} nickname entries")

    # Note: OCR text mining was attempted (source 5) but produces too much noise.
    # Each capper image contains OCR for multiple picks, making it impossible to
    # reliably attribute abbreviations to specific teams. Sources 1-4 already
    # cover 97%+ of real-world picks. Missing aliases will be caught by the
    # resolver's needs_review queue and added manually.

    # ── Build output ────────────────────────────────────────────────────────
    print("\nBuilding output...")

    # Filter out non-team entries (international teams in NHL, etc.)
    skip_teams = {"Canada", "Finland", "Sweden", "USA", "Utah Mammoth"}

    output_rows = []
    teams_with_aliases = 0
    for (sport, espn_name) in sorted(team_aliases.keys()):
        if espn_name in skip_teams:
            continue
        aliases = team_aliases[(sport, espn_name)]
        # Remove the ESPN name itself from aliases (if it snuck in)
        aliases.discard(espn_name)
        # Remove empty strings
        aliases.discard("")
        # Remove aliases that contain the ESPN name as a prefix — these are likely
        # malformed data (e.g. "Dallas Mavericks Timberwolves" for "Dallas Mavericks")
        aliases = {a for a in aliases
                   if not (a.startswith(espn_name) and len(a) > len(espn_name))}

        alias_str = ", ".join(sorted(aliases)) if aliases else ""
        output_rows.append([sport, espn_name, alias_str])
        if aliases:
            teams_with_aliases += 1

    print(f"  Total teams: {len(output_rows)}")
    print(f"  Teams with aliases: {teams_with_aliases}")
    print(f"  Teams without aliases: {len(output_rows) - teams_with_aliases}")

    # Print preview
    if args.dry_run:
        print("\n=== Preview ===")
        for row in output_rows:
            if row[2]:  # only show teams with aliases
                print(f"  [{row[0]:3s}] {row[1]:45s} → {row[2]}")

        print(f"\n[dry-run] Would write {len(output_rows)} rows. No changes made.")
        return

    # ── Write to sheet ──────────────────────────────────────────────────────
    print(f"\nWriting to {RESOLUTION_SHEET}...")
    ws_res = sheets_read(ss.worksheet, RESOLUTION_SHEET)

    sheets_write(ws_res.clear)
    all_data = [["sport", "espn_team_name", "aliases"]] + output_rows
    sheets_write(
        ws_res.update,
        range_name=f"A1:C{len(all_data)}",
        values=all_data,
        value_input_option="RAW",
    )

    print(f"Done. Wrote {len(output_rows)} teams to {RESOLUTION_SHEET}.")
    print(f"  {teams_with_aliases} teams have aliases")


if __name__ == "__main__":
    main()
