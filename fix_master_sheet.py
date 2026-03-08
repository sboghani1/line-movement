#!/usr/bin/env python3
"""
One-time script to fix master_sheet data:
1. Normalize team names to ESPN's full names
2. Clear 'side' column for total bets (O/U)
3. Track incomplete game entries ("X game")
"""

import csv
import re
from collections import defaultdict

# ESPN full team names mapping
NBA_TEAMS = {
    # Short names -> ESPN full name
    "76ers": "Philadelphia 76ers",
    "Sixers": "Philadelphia 76ers",
    "Bucks": "Milwaukee Bucks",
    "Bulls": "Chicago Bulls",
    "Cavaliers": "Cleveland Cavaliers",
    "Cavs": "Cleveland Cavaliers",
    "Celtics": "Boston Celtics",
    "Clippers": "LA Clippers",
    "LA Clippers": "LA Clippers",
    "Grizzlies": "Memphis Grizzlies",
    "Hawks": "Atlanta Hawks",
    "Heat": "Miami Heat",
    "Hornets": "Charlotte Hornets",
    "Jazz": "Utah Jazz",
    "Kings": "Sacramento Kings",
    "Knicks": "New York Knicks",
    "Lakers": "Los Angeles Lakers",
    "LA Lakers": "Los Angeles Lakers",
    "Magic": "Orlando Magic",
    "Mavericks": "Dallas Mavericks",
    "Mavs": "Dallas Mavericks",
    "Nets": "Brooklyn Nets",
    "Nuggets": "Denver Nuggets",
    "Pacers": "Indiana Pacers",
    "Pelicans": "New Orleans Pelicans",
    "Pistons": "Detroit Pistons",
    "Raptors": "Toronto Raptors",
    "Rockets": "Houston Rockets",
    "Spurs": "San Antonio Spurs",
    "Suns": "Phoenix Suns",
    "Thunder": "Oklahoma City Thunder",
    "OKC Thunder": "Oklahoma City Thunder",
    "OKC": "Oklahoma City Thunder",
    "Timberwolves": "Minnesota Timberwolves",
    "Wolves": "Minnesota Timberwolves",
    "Trail Blazers": "Portland Trail Blazers",
    "Blazers": "Portland Trail Blazers",
    "Warriors": "Golden State Warriors",
    "Wizards": "Washington Wizards",
    # Already full names
    "Philadelphia 76ers": "Philadelphia 76ers",
    "Milwaukee Bucks": "Milwaukee Bucks",
    "Chicago Bulls": "Chicago Bulls",
    "Cleveland Cavaliers": "Cleveland Cavaliers",
    "Boston Celtics": "Boston Celtics",
    "Memphis Grizzlies": "Memphis Grizzlies",
    "Atlanta Hawks": "Atlanta Hawks",
    "Miami Heat": "Miami Heat",
    "Charlotte Hornets": "Charlotte Hornets",
    "Utah Jazz": "Utah Jazz",
    "Sacramento Kings": "Sacramento Kings",
    "New York Knicks": "New York Knicks",
    "Los Angeles Lakers": "Los Angeles Lakers",
    "Orlando Magic": "Orlando Magic",
    "Dallas Mavericks": "Dallas Mavericks",
    "Brooklyn Nets": "Brooklyn Nets",
    "Denver Nuggets": "Denver Nuggets",
    "Indiana Pacers": "Indiana Pacers",
    "New Orleans Pelicans": "New Orleans Pelicans",
    "Detroit Pistons": "Detroit Pistons",
    "Toronto Raptors": "Toronto Raptors",
    "Houston Rockets": "Houston Rockets",
    "San Antonio Spurs": "San Antonio Spurs",
    "Phoenix Suns": "Phoenix Suns",
    "Oklahoma City Thunder": "Oklahoma City Thunder",
    "Minnesota Timberwolves": "Minnesota Timberwolves",
    "Portland Trail Blazers": "Portland Trail Blazers",
    "Golden State Warriors": "Golden State Warriors",
    "Washington Wizards": "Washington Wizards",
    # City-only
    "Utah": "Utah Jazz",
}

NHL_TEAMS = {
    "Avalanche": "Colorado Avalanche",
    "Blackhawks": "Chicago Blackhawks",
    "Blue Jackets": "Columbus Blue Jackets",
    "Blues": "St. Louis Blues",
    "Bruins": "Boston Bruins",
    "Canadiens": "Montreal Canadiens",
    "Canucks": "Vancouver Canucks",
    "Capitals": "Washington Capitals",
    "Coyotes": "Utah Hockey Club",  # Moved to Utah
    "Devils": "New Jersey Devils",
    "Ducks": "Anaheim Ducks",
    "Flames": "Calgary Flames",
    "Flyers": "Philadelphia Flyers",
    "Golden Knights": "Vegas Golden Knights",
    "Vegas": "Vegas Golden Knights",
    "Hurricanes": "Carolina Hurricanes",
    "Islanders": "New York Islanders",
    "Jets": "Winnipeg Jets",
    "Kings": "Los Angeles Kings",
    "Kraken": "Seattle Kraken",
    "Lightning": "Tampa Bay Lightning",
    "Maple Leafs": "Toronto Maple Leafs",
    "Leafs": "Toronto Maple Leafs",
    "Oilers": "Edmonton Oilers",
    "Panthers": "Florida Panthers",
    "Penguins": "Pittsburgh Penguins",
    "Predators": "Nashville Predators",
    "Preds": "Nashville Predators",
    "Rangers": "New York Rangers",
    "Red Wings": "Detroit Red Wings",
    "Sabres": "Buffalo Sabres",
    "Senators": "Ottawa Senators",
    "Sens": "Ottawa Senators",
    "Sharks": "San Jose Sharks",
    "Stars": "Dallas Stars",
    "Wild": "Minnesota Wild",
    # Already full
    "Colorado Avalanche": "Colorado Avalanche",
    "Chicago Blackhawks": "Chicago Blackhawks",
    "Columbus Blue Jackets": "Columbus Blue Jackets",
    "St. Louis Blues": "St. Louis Blues",
    "Boston Bruins": "Boston Bruins",
    "Montreal Canadiens": "Montreal Canadiens",
    "Vancouver Canucks": "Vancouver Canucks",
    "Washington Capitals": "Washington Capitals",
    "Utah Hockey Club": "Utah Hockey Club",
    "New Jersey Devils": "New Jersey Devils",
    "Anaheim Ducks": "Anaheim Ducks",
    "Calgary Flames": "Calgary Flames",
    "Philadelphia Flyers": "Philadelphia Flyers",
    "Vegas Golden Knights": "Vegas Golden Knights",
    "Carolina Hurricanes": "Carolina Hurricanes",
    "New York Islanders": "New York Islanders",
    "Winnipeg Jets": "Winnipeg Jets",
    "Los Angeles Kings": "Los Angeles Kings",
    "Seattle Kraken": "Seattle Kraken",
    "Tampa Bay Lightning": "Tampa Bay Lightning",
    "Toronto Maple Leafs": "Toronto Maple Leafs",
    "Edmonton Oilers": "Edmonton Oilers",
    "Florida Panthers": "Florida Panthers",
    "Pittsburgh Penguins": "Pittsburgh Penguins",
    "Nashville Predators": "Nashville Predators",
    "New York Rangers": "New York Rangers",
    "Detroit Red Wings": "Detroit Red Wings",
    "Buffalo Sabres": "Buffalo Sabres",
    "Ottawa Senators": "Ottawa Senators",
    "San Jose Sharks": "San Jose Sharks",
    "Dallas Stars": "Dallas Stars",
    "Minnesota Wild": "Minnesota Wild",
}

# CBB teams - just map common abbreviations
CBB_ABBREVS = {
    "UNC": "North Carolina",
    "UCLA": "UCLA",
    "USC": "USC",
    "UConn": "UConn",
    "ETSU": "East Tennessee State",
    "FAU": "Florida Atlantic",
    "SFA": "Stephen F. Austin",
    "SMU": "SMU",
    "TCU": "TCU",
    "BYU": "BYU",
    "Wichita St": "Wichita State",
    "Cleveland St": "Cleveland State",
    "Wisconsin Green Bay": "Green Bay",
}


def normalize_team_name(team: str, sport: str) -> str:
    """Normalize a team name to ESPN's full name."""
    team = team.strip()
    
    if sport == "NBA":
        if team in NBA_TEAMS:
            return NBA_TEAMS[team]
    elif sport == "NHL":
        if team in NHL_TEAMS:
            return NHL_TEAMS[team]
    elif sport == "CBB":
        if team in CBB_ABBREVS:
            return CBB_ABBREVS[team]
    
    return team  # Return as-is if not found


def is_total_bet(pick: str, line: str) -> bool:
    """Check if this is a total (O/U) bet."""
    pick_lower = pick.lower()
    line_lower = line.lower()
    
    # Check for over/under patterns
    if any(x in pick_lower for x in ["o ", "u ", "over", "under", "o/u", "/"]):
        return True
    if any(x in line_lower for x in ["o ", "u ", "over", "under", "o/u"]):
        return True
    
    return False


def normalize_game_teams(game: str, sport: str) -> str:
    """Normalize team names within a game string."""
    if not game or "game" in game.lower():
        return game  # Leave incomplete games as-is
    
    # Split by @ or vs
    if " @ " in game:
        parts = game.split(" @ ")
        sep = " @ "
    elif " vs " in game:
        parts = game.split(" vs ")
        sep = " vs "  # Keep vs as-is since we don't know home/away
    else:
        return game
    
    if len(parts) == 2:
        away = normalize_team_name(parts[0].strip(), sport)
        home = normalize_team_name(parts[1].strip(), sport)
        return f"{away}{sep}{home}"
    
    return game


def normalize_spread_teams(spread: str, sport: str) -> str:
    """Normalize team names in spread column."""
    if not spread:
        return spread
    
    # Match patterns like "Team -3.5" or "Team +3.5" or "O/U 151"
    match = re.match(r"^(.+?)\s*([+-]\d+\.?\d*|O/U.*)$", spread)
    if match:
        team = match.group(1).strip()
        rest = match.group(2)
        normalized = normalize_team_name(team, sport)
        return f"{normalized} {rest}"
    
    return spread


def process_csv(input_path: str, output_path: str):
    """Process the CSV file and apply fixes."""
    incomplete_games = defaultdict(list)  # date -> list of game descriptions
    
    with open(input_path, "r") as infile:
        reader = csv.DictReader(infile)
        fieldnames = reader.fieldnames
        rows = list(reader)
    
    fixed_rows = []
    for row in rows:
        date = row.get("date", "")
        sport = row.get("sport", "")
        pick = row.get("pick", "")
        line = row.get("line", "")
        game = row.get("game", "")
        spread = row.get("spread", "")
        side = row.get("side", "")
        
        # Track incomplete games
        if game and "game" in game.lower():
            incomplete_games[date].append(game)
        
        # Normalize team names
        new_pick = normalize_team_name(pick, sport) if pick else pick
        new_game = normalize_game_teams(game, sport)
        new_spread = normalize_spread_teams(spread, sport)
        new_side = normalize_team_name(side, sport) if side else side
        
        # Clear side for total bets
        if is_total_bet(pick, line):
            new_side = ""
        
        row["pick"] = new_pick
        row["game"] = new_game
        row["spread"] = new_spread
        row["side"] = new_side
        
        fixed_rows.append(row)
    
    # Write output
    with open(output_path, "w", newline="") as outfile:
        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(fixed_rows)
    
    # Report incomplete games
    print("\n=== INCOMPLETE GAMES REPORT ===")
    total_incomplete = 0
    for date in sorted(incomplete_games.keys()):
        games = incomplete_games[date]
        total_incomplete += len(games)
        unique_games = set(games)
        print(f"\n{date}: {len(games)} incomplete entries")
        for g in sorted(unique_games):
            count = games.count(g)
            print(f"  - {g} ({count}x)")
    
    print(f"\n=== TOTAL: {total_incomplete} incomplete game entries ===")
    print(f"\nFixed file written to: {output_path}")


if __name__ == "__main__":
    input_file = "/Users/boghani/Downloads/Line Movement - master_sheet (4).csv"
    output_file = "/Users/boghani/Downloads/Line Movement - master_sheet_fixed.csv"
    process_csv(input_file, output_file)
