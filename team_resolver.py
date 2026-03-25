#!/usr/bin/env python3
"""
team_resolver.py — Resolve raw pick team names to canonical ESPN team names.

Replaces Claude's Stage 2 abbreviation resolution with deterministic Python.

Resolution priority chain:
  1. Exact match against scheduled teams for that date/sport
  2. Alias lookup from team_name_resolution sheet
  3. Substring/fuzzy match (existing team_matches() logic)
  4. Try next-day schedule (cappers often post night-before)
  5. Try other sports (wrong-sport detection)
  6. None → needs_review queue

Usage:
    from team_resolver import TeamResolver

    resolver = TeamResolver(spreadsheet)
    result = resolver.resolve(pick="BKN", sport="nba", date="2026-03-24")
    # → ResolveResult(espn_name="Brooklyn Nets", game="... @ Brooklyn Nets",
    #                 spread="...", sport="nba", method="alias")
"""

import re
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Set, Tuple

from sheets_utils import (
    SPORT_TO_SCHED,
    sheets_read,
)


# ── Team-name matching (shared with populate_stage2.py / backfill_spread.py) ─
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
    r'mean green|49ers?|aggies?|pride|ospreys?)\\b',
    re.IGNORECASE,
)


def _normalize(name: str) -> str:
    """Strip mascot names for fuzzy comparison."""
    n = name.lower().strip()
    n = _NOISE.sub('', n)
    n = re.sub(r'\s+', ' ', n).strip()
    return n


def _team_matches(pick_name: str, schedule_name: str) -> bool:
    """Fuzzy team-name match: exact, substring, or normalized."""
    p = pick_name.lower().strip()
    s = schedule_name.lower().strip()
    if not p or not s:
        return False
    if p == s:
        return True
    if p in s or s in p:
        return True
    pn = _normalize(pick_name)
    sn = _normalize(schedule_name)
    if pn and sn and (pn == sn or pn in sn or sn in pn):
        return True
    return False


# ── Data types ────────────────────────────────────────────────────────────────

@dataclass
class ResolveResult:
    """Result of resolving a raw pick to a canonical ESPN team name."""
    espn_name: str         # Canonical ESPN team name
    game: str              # "Away Team @ Home Team" from schedule
    spread: str            # Consensus spread from schedule
    sport: str             # Sport code (may differ from input if wrong-sport)
    method: str            # How it was resolved: exact, alias, substring, next_day, wrong_sport


# Type alias for schedule data: {sport: {date: [(away, home, spread), ...]}}
ScheduleData = Dict[str, Dict[str, List[Tuple[str, str, str]]]]

# Type alias for alias data: {sport: {alias_lower: [espn_name, ...]}}
AliasData = Dict[str, Dict[str, List[str]]]


# ── Schedule loader ──────────────────────────────────────────────────────────

def load_schedules(spreadsheet) -> ScheduleData:
    """Load all schedule data from ESPN schedule sheets.

    Returns:
        {sport: {game_date: [(away_team, home_team, spread), ...]}}
    """
    import gspread
    schedules: ScheduleData = {}
    for sport, sheet_name in SPORT_TO_SCHED.items():
        try:
            ws = sheets_read(spreadsheet.worksheet, sheet_name)
            rows = sheets_read(ws.get_all_values)
        except gspread.exceptions.WorksheetNotFound:
            print(f"  {sheet_name}: not found, skipping")
            schedules[sport] = {}
            continue

        if not rows:
            schedules[sport] = {}
            continue

        headers = rows[0]
        hcol = {h: i for i, h in enumerate(headers)}
        by_date: Dict[str, List[Tuple[str, str, str]]] = defaultdict(list)

        for row in rows[1:]:
            while len(row) < len(headers):
                row.append("")
            game_date = row[hcol.get("game_date", 1)].strip()
            away = row[hcol.get("away_team", 2)].strip()
            home = row[hcol.get("home_team", 3)].strip()
            spread = row[hcol.get("spread", 5)].strip()
            if game_date and away and home:
                by_date[game_date].append((away, home, spread))

        schedules[sport] = dict(by_date)
        total = sum(len(v) for v in by_date.values())
        print(f"  {sheet_name}: {total} games loaded")

    return schedules


# ── Alias loader ─────────────────────────────────────────────────────────────

def load_aliases(spreadsheet) -> AliasData:
    """Load team alias data from team_name_resolution sheet.

    The sheet has columns: sport, espn_team_name, aliases (comma-separated).

    Returns:
        {sport: {alias_lower: [espn_name, ...]}}

    The value is a list because some aliases are ambiguous (e.g. "Iowa" could
    map to both "Iowa Hawkeyes" and "Iowa State Cyclones"). The resolver
    disambiguates by checking which team has a game on the pick's date.
    """
    RESOLUTION_SHEET = "team_name_resolution"
    try:
        ws = sheets_read(spreadsheet.worksheet, RESOLUTION_SHEET)
        rows = sheets_read(ws.get_all_values)
    except Exception as e:
        print(f"  Warning: could not load {RESOLUTION_SHEET}: {e}")
        return {}

    if len(rows) < 2:
        return {}

    headers = rows[0]
    hcol = {h: i for i, h in enumerate(headers)}
    sport_col = hcol.get("sport", 0)
    name_col = hcol.get("espn_team_name", 1)
    alias_col = hcol.get("aliases", 2)

    alias_data: AliasData = defaultdict(lambda: defaultdict(list))

    for row in rows[1:]:
        while len(row) <= alias_col:
            row.append("")
        sport = row[sport_col].strip().lower()
        espn_name = row[name_col].strip()
        aliases_str = row[alias_col].strip()

        if not sport or not espn_name:
            continue

        # The ESPN name itself is always a valid alias (for exact matching)
        alias_data[sport][espn_name.lower()].append(espn_name)

        # Add each comma-separated alias
        if aliases_str:
            for alias in aliases_str.split(","):
                alias = alias.strip()
                if alias:
                    alias_data[sport][alias.lower()].append(espn_name)

    # Convert nested defaultdicts to regular dicts
    return {sport: dict(aliases) for sport, aliases in alias_data.items()}


# ── Core resolver ─────────────────────────────────────────────────────────────

class TeamResolver:
    """Resolve raw pick team names to canonical ESPN team names.

    Loads schedule and alias data once, then resolves picks via a priority chain.
    """

    def __init__(self, spreadsheet, schedules: Optional[ScheduleData] = None,
                 aliases: Optional[AliasData] = None):
        """Initialize the resolver.

        Args:
            spreadsheet: gspread Spreadsheet object
            schedules: Pre-loaded schedule data (or None to load from sheets)
            aliases: Pre-loaded alias data (or None to load from sheets)
        """
        self.spreadsheet = spreadsheet

        print("Loading team resolver data...")
        if schedules is not None:
            self.schedules = schedules
            print("  Schedules: pre-loaded")
        else:
            print("  Loading schedules...")
            self.schedules = load_schedules(spreadsheet)

        if aliases is not None:
            self.aliases = aliases
            print("  Aliases: pre-loaded")
        else:
            print("  Loading aliases...")
            self.aliases = load_aliases(spreadsheet)

        # Build a set of all ESPN team names per sport for fast exact-match checks
        self._espn_teams: Dict[str, Set[str]] = {}
        for sport, by_date in self.schedules.items():
            teams = set()
            for games in by_date.values():
                for away, home, _ in games:
                    teams.add(away)
                    teams.add(home)
            self._espn_teams[sport] = teams

        # Summary
        for sport, teams in self._espn_teams.items():
            alias_count = len(self.aliases.get(sport, {}))
            print(f"  {sport}: {len(teams)} teams, {alias_count} alias keys")

    def _games_for_date(self, sport: str, date: str) -> List[Tuple[str, str, str]]:
        """Get scheduled games for a sport/date."""
        return self.schedules.get(sport, {}).get(date, [])

    def _find_game_for_team(
        self, team: str, sport: str, date: str
    ) -> Optional[Tuple[str, str, str]]:
        """Find a game by matching a team name against the schedule.

        Returns (away, home, spread) or None.
        """
        for away, home, spread in self._games_for_date(sport, date):
            if _team_matches(team, away) or _team_matches(team, home):
                return away, home, spread
        return None

    def _make_result(
        self, espn_name: str, away: str, home: str, spread: str,
        sport: str, method: str
    ) -> ResolveResult:
        """Construct a ResolveResult from matched game data."""
        return ResolveResult(
            espn_name=espn_name,
            game=f"{away} @ {home}",
            spread=spread,
            sport=sport,
            method=method,
        )

    def resolve(
        self, pick: str, sport: str, date: str
    ) -> Optional[ResolveResult]:
        """Resolve a raw pick string to a canonical ESPN team name.

        Args:
            pick: Raw team name from Stage 1 (e.g. "BKN", "Nets", "Brooklyn Nets")
            sport: Sport code (e.g. "nba", "cbb", "nhl")
            date: Game date in YYYY-MM-DD format

        Returns:
            ResolveResult with the canonical name, game, spread, and method,
            or None if the pick could not be resolved.
        """
        sport = sport.lower().strip()
        pick = pick.strip()
        if not pick:
            return None

        # ── 1. Exact match against ESPN team names for this sport ─────────
        if pick in self._espn_teams.get(sport, set()):
            game = self._find_game_for_team(pick, sport, date)
            if game:
                away, home, spread = game
                return self._make_result(pick, away, home, spread, sport, "exact")
            # ESPN name exists but no game on this date — try next day
            # (handled in step 4 below)

        # ── 2. Alias lookup ───────────────────────────────────────────────
        result = self._resolve_via_alias(pick, sport, date)
        if result:
            return result

        # ── 3. Substring/fuzzy match against scheduled teams for this date ─
        result = self._resolve_via_substring(pick, sport, date)
        if result:
            return result

        # ── 4. Try next-day schedule (cappers post picks night-before) ────
        try:
            next_date = (
                datetime.strptime(date, "%Y-%m-%d") + timedelta(days=1)
            ).strftime("%Y-%m-%d")
            result = self._resolve_with_date(pick, sport, next_date, "next_day")
            if result:
                return result
        except ValueError:
            pass  # Bad date format, skip next-day check

        # ── 5. Try other sports (wrong-sport detection) ───────────────────
        for other_sport in SPORT_TO_SCHED:
            if other_sport == sport:
                continue
            result = self._resolve_with_date(pick, other_sport, date, "wrong_sport")
            if result:
                return result
            # Also try next-day in other sports
            try:
                next_date = (
                    datetime.strptime(date, "%Y-%m-%d") + timedelta(days=1)
                ).strftime("%Y-%m-%d")
                result = self._resolve_with_date(
                    pick, other_sport, next_date, "wrong_sport"
                )
                if result:
                    return result
            except ValueError:
                pass

        # ── 6. Unresolvable → needs_review ────────────────────────────────
        return None

    def _resolve_with_date(
        self, pick: str, sport: str, date: str, method: str
    ) -> Optional[ResolveResult]:
        """Try all resolution methods (exact, alias, substring) for a specific sport+date.

        Used for next-day and wrong-sport fallbacks.
        """
        pick_stripped = pick.strip()

        # Exact ESPN name match
        if pick_stripped in self._espn_teams.get(sport, set()):
            game = self._find_game_for_team(pick_stripped, sport, date)
            if game:
                away, home, spread = game
                return self._make_result(pick_stripped, away, home, spread, sport, method)

        # Alias lookup
        result = self._resolve_via_alias(pick_stripped, sport, date, method)
        if result:
            return result

        # Substring match
        result = self._resolve_via_substring(pick_stripped, sport, date, method)
        if result:
            return result

        return None

    def _resolve_via_alias(
        self, pick: str, sport: str, date: str, method: str = "alias"
    ) -> Optional[ResolveResult]:
        """Resolve pick using the alias lookup table.

        Handles ambiguous aliases (e.g. "Iowa" → Iowa Hawkeyes or Iowa State
        Cyclones) by checking which candidate has a game on the given date.
        """
        sport_aliases = self.aliases.get(sport, {})
        pick_lower = pick.lower().strip()

        candidates = sport_aliases.get(pick_lower, [])
        if not candidates:
            return None

        # Single candidate — check if they have a game
        if len(candidates) == 1:
            espn_name = candidates[0]
            game = self._find_game_for_team(espn_name, sport, date)
            if game:
                away, home, spread = game
                return self._make_result(espn_name, away, home, spread, sport, method)
            return None

        # Multiple candidates (ambiguous alias) — disambiguate by schedule
        matches = []
        for espn_name in candidates:
            game = self._find_game_for_team(espn_name, sport, date)
            if game:
                matches.append((espn_name, game))

        if len(matches) == 1:
            espn_name, (away, home, spread) = matches[0]
            return self._make_result(espn_name, away, home, spread, sport, method)

        if len(matches) > 1:
            # Both ambiguous teams play on this date — can't disambiguate.
            # Return None and let it fall through to needs_review.
            return None

        # No candidates have a game on this date
        return None

    def _resolve_via_substring(
        self, pick: str, sport: str, date: str, method: str = "substring"
    ) -> Optional[ResolveResult]:
        """Resolve pick using substring/fuzzy matching against scheduled teams.

        Only matches teams that are actually playing on the given date.
        """
        for away, home, spread in self._games_for_date(sport, date):
            if _team_matches(pick, away):
                return self._make_result(away, away, home, spread, sport, method)
            if _team_matches(pick, home):
                return self._make_result(home, away, home, spread, sport, method)
        return None
