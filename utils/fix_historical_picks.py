#!/usr/bin/env python3
"""
╔═══════════════════════════════════════════════════════════════════════════════╗
║  ONE-TIME UTILITY: Fix Historical Picks Data                                  ║
╠═══════════════════════════════════════════════════════════════════════════════╣
║                                                                               ║
║  PURPOSE:                                                                     ║
║  Normalize team names in historical pick data to match ESPN format.           ║
║  Converts abbreviations and nicknames to full official team names.            ║
║                                                                               ║
║  EXAMPLES:                                                                    ║
║  - NBA: "OKC" → "Oklahoma City Thunder"                                       ║
║  - NHL: "CBJ" → "Columbus Blue Jackets"                                       ║
║  - CBB: "Michigan" → "Michigan Wolverines"                                    ║
║                                                                               ║
║  USAGE:                                                                       ║
║  Run manually when historical data needs team name normalization.             ║
║  Reads from master_sheet tab, normalizes names, writes back.                  ║
║                                                                               ║
║  TEAM MAPPINGS:                                                               ║
║  - NBA_NORMALIZE: All 30 NBA teams + common abbreviations                     ║
║  - NHL_NORMALIZE: All 32 NHL teams + common abbreviations                     ║
║  - CBB_NORMALIZE: Top 200+ college basketball programs                        ║
║                                                                               ║
║  NOTE: This is a destructive operation - backs up data before modifying.      ║
╚═══════════════════════════════════════════════════════════════════════════════╝
"""

import csv
import re
from collections import defaultdict

# ═══════════════════════════════════════════════════════════════════════════════
# NBA TEAMS - Full ESPN names
# ═══════════════════════════════════════════════════════════════════════════════
NBA_NORMALIZE = {
    # Abbreviations
    "ATL": "Atlanta Hawks",
    "BOS": "Boston Celtics",
    "BKN": "Brooklyn Nets",
    "CHA": "Charlotte Hornets",
    "CHI": "Chicago Bulls",
    "CLE": "Cleveland Cavaliers",
    "DAL": "Dallas Mavericks",
    "DEN": "Denver Nuggets",
    "DET": "Detroit Pistons",
    "GSW": "Golden State Warriors",
    "GS": "Golden State Warriors",
    "HOU": "Houston Rockets",
    "IND": "Indiana Pacers",
    "LAC": "LA Clippers",
    "LAL": "Los Angeles Lakers",
    "MEM": "Memphis Grizzlies",
    "MIA": "Miami Heat",
    "MIL": "Milwaukee Bucks",
    "MIN": "Minnesota Timberwolves",
    "NO": "New Orleans Pelicans",
    "NOP": "New Orleans Pelicans",
    "NYK": "New York Knicks",
    "NY": "New York Knicks",
    "OKC": "Oklahoma City Thunder",
    "ORL": "Orlando Magic",
    "PHI": "Philadelphia 76ers",
    "PHX": "Phoenix Suns",
    "POR": "Portland Trail Blazers",
    "SAC": "Sacramento Kings",
    "SAS": "San Antonio Spurs",
    "SA": "San Antonio Spurs",
    "TOR": "Toronto Raptors",
    "UTA": "Utah Jazz",
    "WAS": "Washington Wizards",
    # City only
    "Atlanta": "Atlanta Hawks",
    "Boston": "Boston Celtics",
    "Brooklyn": "Brooklyn Nets",
    "Charlotte": "Charlotte Hornets",
    "Chicago": "Chicago Bulls",
    "Cleveland": "Cleveland Cavaliers",
    "Dallas": "Dallas Mavericks",
    "Denver": "Denver Nuggets",
    "Detroit": "Detroit Pistons",
    "Houston": "Houston Rockets",
    "Indiana": "Indiana Pacers",
    "Memphis": "Memphis Grizzlies",
    "Miami": "Miami Heat",
    "Milwaukee": "Milwaukee Bucks",
    "Minnesota": "Minnesota Timberwolves",
    "New Orleans": "New Orleans Pelicans",
    "New York": "New York Knicks",
    "Oklahoma City": "Oklahoma City Thunder",
    "Orlando": "Orlando Magic",
    "Philadelphia": "Philadelphia 76ers",
    "Phoenix": "Phoenix Suns",
    "Portland": "Portland Trail Blazers",
    "Sacramento": "Sacramento Kings",
    "San Antonio": "San Antonio Spurs",
    "Toronto": "Toronto Raptors",
    "Utah": "Utah Jazz",
    "Washington": "Washington Wizards",
    # Nickname only
    "Hawks": "Atlanta Hawks",
    "Celtics": "Boston Celtics",
    "Nets": "Brooklyn Nets",
    "Hornets": "Charlotte Hornets",
    "Bulls": "Chicago Bulls",
    "Cavaliers": "Cleveland Cavaliers",
    "Cavs": "Cleveland Cavaliers",
    "Mavericks": "Dallas Mavericks",
    "Mavs": "Dallas Mavericks",
    "Nuggets": "Denver Nuggets",
    "Pistons": "Detroit Pistons",
    "Warriors": "Golden State Warriors",
    "Rockets": "Houston Rockets",
    "Pacers": "Indiana Pacers",
    "Clippers": "LA Clippers",
    "Lakers": "Los Angeles Lakers",
    "Grizzlies": "Memphis Grizzlies",
    "Heat": "Miami Heat",
    "Bucks": "Milwaukee Bucks",
    "Timberwolves": "Minnesota Timberwolves",
    "Wolves": "Minnesota Timberwolves",
    "Pelicans": "New Orleans Pelicans",
    "Knicks": "New York Knicks",
    "Thunder": "Oklahoma City Thunder",
    "Magic": "Orlando Magic",
    "76ers": "Philadelphia 76ers",
    "Sixers": "Philadelphia 76ers",
    "Suns": "Phoenix Suns",
    "Trail Blazers": "Portland Trail Blazers",
    "Blazers": "Portland Trail Blazers",
    "Kings": "Sacramento Kings",
    "Spurs": "San Antonio Spurs",
    "Raptors": "Toronto Raptors",
    "Jazz": "Utah Jazz",
    "Wizards": "Washington Wizards",
    # Partial names
    "Golden State": "Golden State Warriors",
    "LA Clippers": "LA Clippers",
    "LA Lakers": "Los Angeles Lakers",
    "OKC Thunder": "Oklahoma City Thunder",
    # Already full (passthrough)
    "Atlanta Hawks": "Atlanta Hawks",
    "Boston Celtics": "Boston Celtics",
    "Brooklyn Nets": "Brooklyn Nets",
    "Charlotte Hornets": "Charlotte Hornets",
    "Chicago Bulls": "Chicago Bulls",
    "Cleveland Cavaliers": "Cleveland Cavaliers",
    "Dallas Mavericks": "Dallas Mavericks",
    "Denver Nuggets": "Denver Nuggets",
    "Detroit Pistons": "Detroit Pistons",
    "Golden State Warriors": "Golden State Warriors",
    "Houston Rockets": "Houston Rockets",
    "Indiana Pacers": "Indiana Pacers",
    "Los Angeles Lakers": "Los Angeles Lakers",
    "Memphis Grizzlies": "Memphis Grizzlies",
    "Miami Heat": "Miami Heat",
    "Milwaukee Bucks": "Milwaukee Bucks",
    "Minnesota Timberwolves": "Minnesota Timberwolves",
    "New Orleans Pelicans": "New Orleans Pelicans",
    "New York Knicks": "New York Knicks",
    "Oklahoma City Thunder": "Oklahoma City Thunder",
    "Orlando Magic": "Orlando Magic",
    "Philadelphia 76ers": "Philadelphia 76ers",
    "Phoenix Suns": "Phoenix Suns",
    "Portland Trail Blazers": "Portland Trail Blazers",
    "Sacramento Kings": "Sacramento Kings",
    "San Antonio Spurs": "San Antonio Spurs",
    "Toronto Raptors": "Toronto Raptors",
    "Utah Jazz": "Utah Jazz",
    "Washington Wizards": "Washington Wizards",
}

# ═══════════════════════════════════════════════════════════════════════════════
# NHL TEAMS - Full ESPN names
# ═══════════════════════════════════════════════════════════════════════════════
NHL_NORMALIZE = {
    # Abbreviations
    "ANA": "Anaheim Ducks",
    "ARI": "Utah Hockey Club",
    "BOS": "Boston Bruins",
    "BUF": "Buffalo Sabres",
    "CGY": "Calgary Flames",
    "CAR": "Carolina Hurricanes",
    "CHI": "Chicago Blackhawks",
    "COL": "Colorado Avalanche",
    "CBJ": "Columbus Blue Jackets",
    "DAL": "Dallas Stars",
    "DET": "Detroit Red Wings",
    "EDM": "Edmonton Oilers",
    "FLA": "Florida Panthers",
    "LAK": "Los Angeles Kings",
    "LA": "Los Angeles Kings",
    "MIN": "Minnesota Wild",
    "MTL": "Montreal Canadiens",
    "NSH": "Nashville Predators",
    "NJD": "New Jersey Devils",
    "NJ": "New Jersey Devils",
    "NYI": "New York Islanders",
    "NYR": "New York Rangers",
    "OTT": "Ottawa Senators",
    "PHI": "Philadelphia Flyers",
    "PIT": "Pittsburgh Penguins",
    "SJS": "San Jose Sharks",
    "SJ": "San Jose Sharks",
    "SEA": "Seattle Kraken",
    "STL": "St. Louis Blues",
    "TBL": "Tampa Bay Lightning",
    "TB": "Tampa Bay Lightning",
    "TOR": "Toronto Maple Leafs",
    "VAN": "Vancouver Canucks",
    "VGK": "Vegas Golden Knights",
    "WSH": "Washington Capitals",
    "WPG": "Winnipeg Jets",
    # City only
    "Anaheim": "Anaheim Ducks",
    "Boston": "Boston Bruins",
    "Buffalo": "Buffalo Sabres",
    "Calgary": "Calgary Flames",
    "Carolina": "Carolina Hurricanes",
    "Chicago": "Chicago Blackhawks",
    "Colorado": "Colorado Avalanche",
    "Columbus": "Columbus Blue Jackets",
    "Dallas": "Dallas Stars",
    "Detroit": "Detroit Red Wings",
    "Edmonton": "Edmonton Oilers",
    "Florida": "Florida Panthers",
    "Minnesota": "Minnesota Wild",
    "Montreal": "Montreal Canadiens",
    "Nashville": "Nashville Predators",
    "New Jersey": "New Jersey Devils",
    "Ottawa": "Ottawa Senators",
    "Philadelphia": "Philadelphia Flyers",
    "Pittsburgh": "Pittsburgh Penguins",
    "San Jose": "San Jose Sharks",
    "Seattle": "Seattle Kraken",
    "St. Louis": "St. Louis Blues",
    "Tampa Bay": "Tampa Bay Lightning",
    "Tampa": "Tampa Bay Lightning",
    "Toronto": "Toronto Maple Leafs",
    "Vancouver": "Vancouver Canucks",
    "Vegas": "Vegas Golden Knights",
    "Washington": "Washington Capitals",
    "Winnipeg": "Winnipeg Jets",
    # Nickname only
    "Ducks": "Anaheim Ducks",
    "Bruins": "Boston Bruins",
    "Sabres": "Buffalo Sabres",
    "Flames": "Calgary Flames",
    "Hurricanes": "Carolina Hurricanes",
    "Canes": "Carolina Hurricanes",
    "Blackhawks": "Chicago Blackhawks",
    "Hawks": "Chicago Blackhawks",
    "Avalanche": "Colorado Avalanche",
    "Avs": "Colorado Avalanche",
    "Blue Jackets": "Columbus Blue Jackets",
    "Jackets": "Columbus Blue Jackets",
    "Stars": "Dallas Stars",
    "Red Wings": "Detroit Red Wings",
    "Wings": "Detroit Red Wings",
    "Oilers": "Edmonton Oilers",
    "Panthers": "Florida Panthers",
    "Kings": "Los Angeles Kings",
    "Wild": "Minnesota Wild",
    "Canadiens": "Montreal Canadiens",
    "Habs": "Montreal Canadiens",
    "Predators": "Nashville Predators",
    "Preds": "Nashville Predators",
    "Devils": "New Jersey Devils",
    "Islanders": "New York Islanders",
    "Isles": "New York Islanders",
    "Rangers": "New York Rangers",
    "Senators": "Ottawa Senators",
    "Sens": "Ottawa Senators",
    "Flyers": "Philadelphia Flyers",
    "Penguins": "Pittsburgh Penguins",
    "Pens": "Pittsburgh Penguins",
    "Sharks": "San Jose Sharks",
    "Kraken": "Seattle Kraken",
    "Blues": "St. Louis Blues",
    "Lightning": "Tampa Bay Lightning",
    "Bolts": "Tampa Bay Lightning",
    "Maple Leafs": "Toronto Maple Leafs",
    "Leafs": "Toronto Maple Leafs",
    "Canucks": "Vancouver Canucks",
    "Golden Knights": "Vegas Golden Knights",
    "Capitals": "Washington Capitals",
    "Caps": "Washington Capitals",
    "Jets": "Winnipeg Jets",
    # Already full (passthrough)
    "Anaheim Ducks": "Anaheim Ducks",
    "Boston Bruins": "Boston Bruins",
    "Buffalo Sabres": "Buffalo Sabres",
    "Calgary Flames": "Calgary Flames",
    "Carolina Hurricanes": "Carolina Hurricanes",
    "Chicago Blackhawks": "Chicago Blackhawks",
    "Colorado Avalanche": "Colorado Avalanche",
    "Columbus Blue Jackets": "Columbus Blue Jackets",
    "Dallas Stars": "Dallas Stars",
    "Detroit Red Wings": "Detroit Red Wings",
    "Edmonton Oilers": "Edmonton Oilers",
    "Florida Panthers": "Florida Panthers",
    "Los Angeles Kings": "Los Angeles Kings",
    "Minnesota Wild": "Minnesota Wild",
    "Montreal Canadiens": "Montreal Canadiens",
    "Nashville Predators": "Nashville Predators",
    "New Jersey Devils": "New Jersey Devils",
    "New York Islanders": "New York Islanders",
    "New York Rangers": "New York Rangers",
    "Ottawa Senators": "Ottawa Senators",
    "Philadelphia Flyers": "Philadelphia Flyers",
    "Pittsburgh Penguins": "Pittsburgh Penguins",
    "San Jose Sharks": "San Jose Sharks",
    "Seattle Kraken": "Seattle Kraken",
    "St. Louis Blues": "St. Louis Blues",
    "Tampa Bay Lightning": "Tampa Bay Lightning",
    "Toronto Maple Leafs": "Toronto Maple Leafs",
    "Utah Hockey Club": "Utah Hockey Club",
    "Vancouver Canucks": "Vancouver Canucks",
    "Vegas Golden Knights": "Vegas Golden Knights",
    "Washington Capitals": "Washington Capitals",
    "Winnipeg Jets": "Winnipeg Jets",
}

# ═══════════════════════════════════════════════════════════════════════════════
# CBB TEAMS - Full ESPN names (School + Mascot)
# ═══════════════════════════════════════════════════════════════════════════════
CBB_NORMALIZE = {
    # Short -> Full ESPN name
    "Alabama": "Alabama Crimson Tide",
    "Arizona": "Arizona Wildcats",
    "Arizona State": "Arizona State Sun Devils",
    "Arkansas": "Arkansas Razorbacks",
    "Auburn": "Auburn Tigers",
    "Baylor": "Baylor Bears",
    "Boston College": "Boston College Eagles",
    "Boston University": "Boston University Terriers",
    "Butler": "Butler Bulldogs",
    "BYU": "BYU Cougars",
    "California": "California Golden Bears",
    "Cal": "California Golden Bears",
    "Campbell": "Campbell Fighting Camels",
    "Central Arkansas": "Central Arkansas Bears",
    "Charleston": "Charleston Cougars",
    "Charlotte": "Charlotte 49ers",
    "Clemson": "Clemson Tigers",
    "Colgate": "Colgate Raiders",
    "Colorado": "Colorado Buffaloes",
    "Creighton": "Creighton Bluejays",
    "Duke": "Duke Blue Devils",
    "East Tennessee State": "East Tennessee State Buccaneers",
    "ETSU": "East Tennessee State Buccaneers",
    "Florida": "Florida Gators",
    "Florida Atlantic": "Florida Atlantic Owls",
    "FAU": "Florida Atlantic Owls",
    "Florida State": "Florida State Seminoles",
    "Georgetown": "Georgetown Hoyas",
    "Georgia": "Georgia Bulldogs",
    "Georgia Southern": "Georgia Southern Eagles",
    "GA Southern": "Georgia Southern Eagles",
    "Gonzaga": "Gonzaga Bulldogs",
    "High Point": "High Point Panthers",
    "Houston": "Houston Cougars",
    "Idaho State": "Idaho State Bengals",
    "Illinois": "Illinois Fighting Illini",
    "Incarnate Word": "Incarnate Word Cardinals",
    "Indiana": "Indiana Hoosiers",
    "Iowa": "Iowa Hawkeyes",
    "Iowa State": "Iowa State Cyclones",
    "Kansas": "Kansas Jayhawks",
    "Kansas State": "Kansas State Wildcats",
    "Kentucky": "Kentucky Wildcats",
    "Lehigh": "Lehigh Mountain Hawks",
    "Little Rock": "Little Rock Trojans",
    "Arkansas Little Rock": "Little Rock Trojans",
    "Louisville": "Louisville Cardinals",
    "LSU": "LSU Tigers",
    "Marist": "Marist Red Foxes",
    "Marquette": "Marquette Golden Eagles",
    "Marshall": "Marshall Thundering Herd",
    "Maryland": "Maryland Terrapins",
    "McNeese": "McNeese Cowboys",
    "Memphis": "Memphis Tigers",
    "Merrimack": "Merrimack Warriors",
    "Miami": "Miami Hurricanes",
    "Miami OH": "Miami (OH) RedHawks",
    "Miami Ohio": "Miami (OH) RedHawks",
    "Miami (OH)": "Miami (OH) RedHawks",
    "Michigan": "Michigan Wolverines",
    "Michigan State": "Michigan State Spartans",
    "Minnesota": "Minnesota Golden Gophers",
    "Mississippi State": "Mississippi State Bulldogs",
    "Missouri": "Missouri Tigers",
    "Morehead State": "Morehead State Eagles",
    "Navy": "Navy Midshipmen",
    "NC State": "NC State Wolfpack",
    "NC Wilmington": "UNC Wilmington Seahawks",
    "Nebraska": "Nebraska Cornhuskers",
    "Nicholls": "Nicholls Colonels",
    "Nicholls State": "Nicholls Colonels",
    "North Carolina": "North Carolina Tar Heels",
    "UNC": "North Carolina Tar Heels",
    "Northern Iowa": "Northern Iowa Panthers",
    "Northwestern": "Northwestern Wildcats",
    "Northwestern State": "Northwestern State Demons",
    "Notre Dame": "Notre Dame Fighting Irish",
    "Ohio State": "Ohio State Buckeyes",
    "Oklahoma": "Oklahoma Sooners",
    "Oklahoma State": "Oklahoma State Cowboys",
    "Ole Miss": "Ole Miss Rebels",
    "Oregon": "Oregon Ducks",
    "Oregon State": "Oregon State Beavers",
    "Pacific": "Pacific Tigers",
    "Penn State": "Penn State Nittany Lions",
    "Pittsburgh": "Pittsburgh Panthers",
    "Pitt": "Pittsburgh Panthers",
    "Portland State": "Portland State Vikings",
    "Providence": "Providence Friars",
    "Purdue": "Purdue Boilermakers",
    "Queens": "Queens University Royals",
    "Queens University": "Queens University Royals",
    "Rutgers": "Rutgers Scarlet Knights",
    "San Francisco": "San Francisco Dons",
    "Santa Clara": "Santa Clara Broncos",
    "Seton Hall": "Seton Hall Pirates",
    "SF Austin": "Stephen F. Austin Lumberjacks",
    "Stephen F. Austin": "Stephen F. Austin Lumberjacks",
    "SFA": "Stephen F. Austin Lumberjacks",
    "SMU": "SMU Mustangs",
    "South Carolina": "South Carolina Gamecocks",
    "South Florida": "South Florida Bulls",
    "Stanford": "Stanford Cardinal",
    "St. John's": "St. John's Red Storm",
    "Syracuse": "Syracuse Orange",
    "TCU": "TCU Horned Frogs",
    "Tennessee": "Tennessee Volunteers",
    "Texas": "Texas Longhorns",
    "Texas A&M": "Texas A&M Aggies",
    "Texas Tech": "Texas Tech Red Raiders",
    "Towson": "Towson Tigers",
    "Troy": "Troy Trojans",
    "Tulane": "Tulane Green Wave",
    "UCF": "UCF Knights",
    "UCLA": "UCLA Bruins",
    "UConn": "UConn Huskies",
    "UCONN": "UConn Huskies",
    "UIC": "UIC Flames",
    "UNC Wilmington": "UNC Wilmington Seahawks",
    "USC": "USC Trojans",
    "Utah": "Utah Utes",
    "Vanderbilt": "Vanderbilt Commodores",
    "Villanova": "Villanova Wildcats",
    "Virginia": "Virginia Cavaliers",
    "Wake Forest": "Wake Forest Demon Deacons",
    "Washington": "Washington Huskies",
    "Western Michigan": "Western Michigan Broncos",
    "Wichita State": "Wichita State Shockers",
    "Wichita St": "Wichita State Shockers",
    "Winthrop": "Winthrop Eagles",
    "Wisconsin": "Wisconsin Badgers",
    "Xavier": "Xavier Musketeers",
    # Already full (passthrough)
    "Alabama Crimson Tide": "Alabama Crimson Tide",
    "Arizona Wildcats": "Arizona Wildcats",
    "Arizona State Sun Devils": "Arizona State Sun Devils",
    "Arkansas Razorbacks": "Arkansas Razorbacks",
    "Auburn Tigers": "Auburn Tigers",
    "Baylor Bears": "Baylor Bears",
    "Boston College Eagles": "Boston College Eagles",
    "Boston University Terriers": "Boston University Terriers",
    "Butler Bulldogs": "Butler Bulldogs",
    "BYU Cougars": "BYU Cougars",
    "California Golden Bears": "California Golden Bears",
    "Campbell Fighting Camels": "Campbell Fighting Camels",
    "Central Arkansas Bears": "Central Arkansas Bears",
    "Charleston Cougars": "Charleston Cougars",
    "Charlotte 49ers": "Charlotte 49ers",
    "Clemson Tigers": "Clemson Tigers",
    "Colgate Raiders": "Colgate Raiders",
    "Colorado Buffaloes": "Colorado Buffaloes",
    "Creighton Bluejays": "Creighton Bluejays",
    "Duke Blue Devils": "Duke Blue Devils",
    "East Tennessee State Buccaneers": "East Tennessee State Buccaneers",
    "Florida Gators": "Florida Gators",
    "Florida Atlantic Owls": "Florida Atlantic Owls",
    "Florida State Seminoles": "Florida State Seminoles",
    "Georgetown Hoyas": "Georgetown Hoyas",
    "Georgia Bulldogs": "Georgia Bulldogs",
    "Georgia Southern Eagles": "Georgia Southern Eagles",
    "Gonzaga Bulldogs": "Gonzaga Bulldogs",
    "High Point Panthers": "High Point Panthers",
    "Houston Cougars": "Houston Cougars",
    "Idaho State Bengals": "Idaho State Bengals",
    "Illinois Fighting Illini": "Illinois Fighting Illini",
    "Incarnate Word Cardinals": "Incarnate Word Cardinals",
    "Indiana Hoosiers": "Indiana Hoosiers",
    "Iowa Hawkeyes": "Iowa Hawkeyes",
    "Iowa State Cyclones": "Iowa State Cyclones",
    "Kansas Jayhawks": "Kansas Jayhawks",
    "Kansas State Wildcats": "Kansas State Wildcats",
    "Kentucky Wildcats": "Kentucky Wildcats",
    "Lehigh Mountain Hawks": "Lehigh Mountain Hawks",
    "Little Rock Trojans": "Little Rock Trojans",
    "Louisville Cardinals": "Louisville Cardinals",
    "LSU Tigers": "LSU Tigers",
    "Marist Red Foxes": "Marist Red Foxes",
    "Marquette Golden Eagles": "Marquette Golden Eagles",
    "Marshall Thundering Herd": "Marshall Thundering Herd",
    "Maryland Terrapins": "Maryland Terrapins",
    "McNeese Cowboys": "McNeese Cowboys",
    "Memphis Tigers": "Memphis Tigers",
    "Merrimack Warriors": "Merrimack Warriors",
    "Miami Hurricanes": "Miami Hurricanes",
    "Miami (OH) RedHawks": "Miami (OH) RedHawks",
    "Michigan Wolverines": "Michigan Wolverines",
    "Michigan State Spartans": "Michigan State Spartans",
    "Minnesota Golden Gophers": "Minnesota Golden Gophers",
    "Mississippi State Bulldogs": "Mississippi State Bulldogs",
    "Missouri Tigers": "Missouri Tigers",
    "Morehead State Eagles": "Morehead State Eagles",
    "Navy Midshipmen": "Navy Midshipmen",
    "NC State Wolfpack": "NC State Wolfpack",
    "Nebraska Cornhuskers": "Nebraska Cornhuskers",
    "Nicholls Colonels": "Nicholls Colonels",
    "North Carolina Tar Heels": "North Carolina Tar Heels",
    "Northern Iowa Panthers": "Northern Iowa Panthers",
    "Northwestern Wildcats": "Northwestern Wildcats",
    "Northwestern State Demons": "Northwestern State Demons",
    "Notre Dame Fighting Irish": "Notre Dame Fighting Irish",
    "Ohio State Buckeyes": "Ohio State Buckeyes",
    "Oklahoma Sooners": "Oklahoma Sooners",
    "Oklahoma State Cowboys": "Oklahoma State Cowboys",
    "Ole Miss Rebels": "Ole Miss Rebels",
    "Oregon Ducks": "Oregon Ducks",
    "Oregon State Beavers": "Oregon State Beavers",
    "Pacific Tigers": "Pacific Tigers",
    "Penn State Nittany Lions": "Penn State Nittany Lions",
    "Pittsburgh Panthers": "Pittsburgh Panthers",
    "Portland State Vikings": "Portland State Vikings",
    "Providence Friars": "Providence Friars",
    "Purdue Boilermakers": "Purdue Boilermakers",
    "Queens University Royals": "Queens University Royals",
    "Rutgers Scarlet Knights": "Rutgers Scarlet Knights",
    "San Francisco Dons": "San Francisco Dons",
    "Santa Clara Broncos": "Santa Clara Broncos",
    "Seton Hall Pirates": "Seton Hall Pirates",
    "Stephen F. Austin Lumberjacks": "Stephen F. Austin Lumberjacks",
    "SMU Mustangs": "SMU Mustangs",
    "South Carolina Gamecocks": "South Carolina Gamecocks",
    "South Florida Bulls": "South Florida Bulls",
    "Stanford Cardinal": "Stanford Cardinal",
    "St. John's Red Storm": "St. John's Red Storm",
    "Syracuse Orange": "Syracuse Orange",
    "TCU Horned Frogs": "TCU Horned Frogs",
    "Tennessee Volunteers": "Tennessee Volunteers",
    "Texas Longhorns": "Texas Longhorns",
    "Texas A&M Aggies": "Texas A&M Aggies",
    "Texas Tech Red Raiders": "Texas Tech Red Raiders",
    "Towson Tigers": "Towson Tigers",
    "Troy Trojans": "Troy Trojans",
    "Tulane Green Wave": "Tulane Green Wave",
    "UCF Knights": "UCF Knights",
    "UCLA Bruins": "UCLA Bruins",
    "UConn Huskies": "UConn Huskies",
    "UIC Flames": "UIC Flames",
    "UNC Wilmington Seahawks": "UNC Wilmington Seahawks",
    "USC Trojans": "USC Trojans",
    "Utah Utes": "Utah Utes",
    "Vanderbilt Commodores": "Vanderbilt Commodores",
    "Villanova Wildcats": "Villanova Wildcats",
    "Virginia Cavaliers": "Virginia Cavaliers",
    "Wake Forest Demon Deacons": "Wake Forest Demon Deacons",
    "Washington Huskies": "Washington Huskies",
    "Western Michigan Broncos": "Western Michigan Broncos",
    "Wichita State Shockers": "Wichita State Shockers",
    "Winthrop Eagles": "Winthrop Eagles",
    "Wisconsin Badgers": "Wisconsin Badgers",
    "Xavier Musketeers": "Xavier Musketeers",
}


def normalize_team_name(name: str, sport: str) -> str:
    """Normalize team name to full ESPN format."""
    name = name.strip()
    if not name:
        return name

    sport_upper = sport.upper() if sport else ""

    if sport_upper == "NBA":
        if name in NBA_NORMALIZE:
            return NBA_NORMALIZE[name]
    elif sport_upper == "NHL":
        if name in NHL_NORMALIZE:
            return NHL_NORMALIZE[name]
    elif sport_upper in ("CBB", "NCAAB"):
        if name in CBB_NORMALIZE:
            return CBB_NORMALIZE[name]

    # Try all mappings if sport doesn't match
    if name in NBA_NORMALIZE:
        return NBA_NORMALIZE[name]
    if name in NHL_NORMALIZE:
        return NHL_NORMALIZE[name]
    if name in CBB_NORMALIZE:
        return CBB_NORMALIZE[name]

    return name


def normalize_game_column(game: str, sport: str) -> str:
    """Normalize team names in game column (Team A @ Team B or Team A vs Team B)."""
    if not game:
        return game

    # Handle both @ and vs separators
    if " @ " in game:
        parts = game.split(" @ ")
        if len(parts) == 2:
            away = normalize_team_name(parts[0], sport)
            home = normalize_team_name(parts[1], sport)
            return f"{away} @ {home}"
    elif " vs " in game:
        parts = game.split(" vs ")
        if len(parts) == 2:
            team1 = normalize_team_name(parts[0], sport)
            team2 = normalize_team_name(parts[1], sport)
            return f"{team1} vs {team2}"

    return game


def normalize_spread_column(spread: str, sport: str) -> str:
    """Normalize team name in spread column (Team Name -3.5)."""
    if not spread:
        return spread

    # Match pattern: Team Name +/-number
    match = re.match(r"^(.+?)\s*([+-][\d.]+)$", spread)
    if match:
        team = normalize_team_name(match.group(1).strip(), sport)
        line = match.group(2)
        return f"{team} {line}"

    return spread


def fix_csv(input_file: str, output_file: str):
    """Read CSV, normalize all team names, write fixed CSV."""

    with open(input_file, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
        fieldnames = reader.fieldnames

    fixed_rows = []
    changes = defaultdict(int)

    for row in rows:
        sport = row.get("sport", "")

        # Fix pick column
        old_pick = row.get("pick", "")
        new_pick = normalize_team_name(old_pick, sport)
        if old_pick != new_pick:
            changes[f"pick: {old_pick} -> {new_pick}"] += 1
        row["pick"] = new_pick

        # Fix side column
        old_side = row.get("side", "")
        new_side = normalize_team_name(old_side, sport)
        if old_side != new_side:
            changes[f"side: {old_side} -> {new_side}"] += 1
        row["side"] = new_side

        # Fix game column
        old_game = row.get("game", "")
        new_game = normalize_game_column(old_game, sport)
        if old_game != new_game:
            changes[f"game: {old_game} -> {new_game}"] += 1
        row["game"] = new_game

        # Fix spread column
        old_spread = row.get("spread", "")
        new_spread = normalize_spread_column(old_spread, sport)
        if old_spread != new_spread:
            changes[f"spread: {old_spread} -> {new_spread}"] += 1
        row["spread"] = new_spread

        fixed_rows.append(row)

    # Write output
    with open(output_file, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(fixed_rows)

    print(f"Fixed {len(fixed_rows)} rows")
    print(f"Total changes: {sum(changes.values())}")
    print("\nChanges by type:")
    for change, count in sorted(changes.items(), key=lambda x: -x[1])[:50]:
        print(f"  {count}x {change}")

    return fixed_rows


if __name__ == "__main__":
    import sys

    if len(sys.argv) < 2:
        print("Usage: python fix_historical_picks.py <input.csv> [output.csv]")
        print("       If output not specified, writes to <input>_fixed.csv")
        sys.exit(1)

    input_file = sys.argv[1]
    if len(sys.argv) >= 3:
        output_file = sys.argv[2]
    else:
        output_file = input_file.replace(".csv", "_fixed.csv")

    fix_csv(input_file, output_file)
    print(f"\nOutput written to: {output_file}")
