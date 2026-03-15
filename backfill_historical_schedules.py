#!/usr/bin/env python3
"""
One-time script to backfill historical schedule + score data for every
sport x date combo found in the live master_sheet.

Usage:
    python backfill_historical_schedules.py [--dry-run] [--sport nba|cbb|nhl|nfl|cfb] [--limit N]

    --dry-run   Print what would be fetched/written without touching the sheet.
    --sport     Only process one sport.
    --limit N   Stop after N dates.
"""

import os
import sys
import time
import argparse
from datetime import date, datetime, timedelta
from difflib import SequenceMatcher
from zoneinfo import ZoneInfo

import requests
from dotenv import load_dotenv

load_dotenv()

from espn_schedule_fetcher import (
    NBA_WORKSHEET_NAME,
    CBB_WORKSHEET_NAME,
    NHL_WORKSHEET_NAME,
    NFL_WORKSHEET_NAME,
    CFB_WORKSHEET_NAME,
    GOOGLE_SHEET_ID,
    get_gspread_client,
    get_or_create_worksheet,
    get_existing_games,
    fetch_and_parse_schedule_api,
    fetch_espn_results,
)

ODDS_API_KEY = os.environ.get("ODDS_API_KEY", "")
ODDS_API_BASE = "https://api.the-odds-api.com/v4/historical/sports"

SPORT_CONFIG = {
    "nba": NBA_WORKSHEET_NAME,
    "cbb": CBB_WORKSHEET_NAME,
    "nhl": NHL_WORKSHEET_NAME,
    "nfl": NFL_WORKSHEET_NAME,
    "cfb": CFB_WORKSHEET_NAME,
}

ODDS_API_SPORT_KEYS = {
    "nba": "basketball_nba",
    "cbb": "basketball_ncaab",
    "nhl": "icehockey_nhl",
    "nfl": "americanfootball_nfl",
    "cfb": "americanfootball_ncaaf",
}

PREFERRED_BOOKMAKERS = ["draftkings", "fanduel", "betmgm", "williamhill_us"]

# Start of each sport's previous season
SEASON_START_DATES = {
    "nba": date(2024, 10, 22),  # 2024-25 NBA season
    "cbb": date(2024, 11, 4),   # 2024-25 CBB season
    "nhl": date(2024, 10, 8),   # 2024-25 NHL season
    "nfl": date(2024, 9, 5),    # 2024 NFL season opener
    "cfb": date(2024, 8, 24),   # 2024 CFB early openers
}

# ESPN name → Odds API name for known mismatches
TEAM_NAME_ALIASES = {
    "LA Clippers": "Los Angeles Clippers",
    "LA Lakers": "Los Angeles Lakers",
    # CBB aliases
    "App State Mountaineers": "Appalachian St Mountaineers",
    "Long Beach State Beach": "Long Beach St 49ers",
    "East Texas A&M Lions": "Texas A&M-Commerce Lions",
}


def _fuzzy_match(name: str, candidates: list[str], threshold: float = 0.82) -> str | None:
    """Return the best fuzzy match from candidates, or None if below threshold."""
    best, best_score = None, 0.0
    for c in candidates:
        score = SequenceMatcher(None, name.lower(), c.lower()).ratio()
        if score > best_score:
            best, best_score = c, score
    return best if best_score >= threshold else None


def _normalize(name: str) -> str:
    """Return the last word of a team name as a nickname fallback (e.g. 'Clippers')."""
    return name.strip().split()[-1].lower()


ESPN_CBB_SCOREBOARD_URL = "https://site.api.espn.com/apis/site/v2/sports/basketball/mens-college-basketball/scoreboard"


def fetch_d1_cbb_games(date_str: str) -> list[dict]:
    """Fetch CBB games for a date (YYYYMMDD), filtering to D1 vs D1 matchups only.

    Uses conferenceId presence to determine D1 status — non-D1 teams have no conferenceId.
    Includes all D1 tournaments (NCAA, NIT, conference tourneys) automatically.
    """
    try:
        data = requests.get(
            ESPN_CBB_SCOREBOARD_URL,
            params={"dates": date_str, "groups": "50"},
            timeout=10,
        ).json()
    except Exception:
        return []

    game_date_fmt = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
    games = []

    for event in data.get("events", []):
        try:
            competition = event["competitions"][0]
            competitors = competition.get("competitors", [])
            if len(competitors) != 2:
                continue

            away_team, home_team = "", ""
            away_conf, home_conf = None, None

            for comp in competitors:
                team = comp.get("team", {})
                name = team.get("displayName", "")
                conf = team.get("conferenceId")
                if comp.get("homeAway") == "away":
                    away_team, away_conf = name, conf
                else:
                    home_team, home_conf = name, conf

            # Skip if either team is non-D1
            if not away_conf or not home_conf:
                continue

            status = event.get("status", {}).get("type", {}).get("name", "")
            date_obj = event.get("date", "")
            if status == "STATUS_SCHEDULED":
                try:
                    dt = datetime.fromisoformat(date_obj.replace("Z", "+00:00"))
                    game_time = dt.astimezone(ZoneInfo("America/New_York")).strftime("%I:%M %p").lstrip("0")
                except Exception:
                    game_time = "TBD"
            elif status in ("STATUS_IN_PROGRESS", "STATUS_HALFTIME"):
                game_time = "LIVE"
            else:
                game_time = "Final"

            venue_data = competition.get("venue", {})
            venue = venue_data.get("fullName", "")
            if venue:
                city = venue_data.get("address", {}).get("city", "")
                state = venue_data.get("address", {}).get("state", "")
                if city:
                    venue += f", {city}"
                    if state:
                        venue += f", {state}"

            broadcasts = competition.get("broadcasts", [])
            tv_network = broadcasts[0].get("names", [""])[0] if broadcasts else ""

            games.append({
                "game_date": game_date_fmt,
                "away_team": away_team,
                "home_team": home_team,
                "game_time": game_time,
                "spread": "",
                "over_under": "",
                "tv_network": tv_network,
                "venue": venue,
                "score": "",
            })
        except Exception:
            continue

    return games


def fetch_odds_api_lines(sport: str, game_date: str) -> tuple[dict, int | None]:
    """Fetch pre-game line + over/under from The Odds API for a sport/date.

    For NHL uses h2h (moneyline) as the primary market — hockey's main betting
    line is ML, not the puck line. NBA/CBB use spreads as usual.

    Queries a snapshot at 11 AM ET (16:00 UTC) on the game date, which is
    pre-game for all US sports.

    Returns (odds_lookup, credits_remaining) where odds_lookup maps
    (odds_away, odds_home) → (favored_odds_team, point_str, ou_str).
    The favored_odds_team is an Odds API team name — callers map it to ESPN names.
    """
    if not ODDS_API_KEY:
        return {}, None

    odds_sport = ODDS_API_SPORT_KEYS[sport]
    snapshot_time = f"{game_date}T16:00:00Z"
    # NHL: moneyline (h2h) is the primary line; NBA/CBB: point spread
    primary_market = "h2h" if sport == "nhl" else "spreads"

    try:
        resp = requests.get(
            f"{ODDS_API_BASE}/{odds_sport}/odds",
            params={
                "apiKey": ODDS_API_KEY,
                "regions": "us",
                "markets": f"{primary_market},totals",
                "date": snapshot_time,
                "oddsFormat": "american",
            },
            timeout=10,
        )
        resp.raise_for_status()
    except Exception as e:
        print(f"    ⚠️  Odds API request failed: {e}")
        return {}, None

    remaining = resp.headers.get("x-requests-remaining", "?")
    events = resp.json().get("data", [])

    odds_lookup: dict[tuple, tuple] = {}
    for event in events:
        away = event.get("away_team", "")
        home = event.get("home_team", "")
        favored_team, point_str, ou_str = "", "", ""

        bookmakers = event.get("bookmakers", [])
        chosen = None
        for pref in PREFERRED_BOOKMAKERS:
            chosen = next((b for b in bookmakers if b["key"] == pref), None)
            if chosen:
                break
        if not chosen and bookmakers:
            chosen = bookmakers[0]
        if not chosen:
            continue

        for market in chosen.get("markets", []):
            if market["key"] == "spreads":
                for outcome in market.get("outcomes", []):
                    point = outcome.get("point", "")
                    name = outcome.get("name", "")
                    if point != "" and float(point) <= 0:
                        point_val = float(point)
                        if point_val == 0:
                            favored_team = "PK"
                            point_str = "PK"
                        else:
                            favored_team = name
                            point_str = str(int(point_val)) if point_val == int(point_val) else str(point_val)
            elif market["key"] == "h2h":
                # Moneyline: favorite has the most negative (lowest) price
                outcomes = market.get("outcomes", [])
                if outcomes:
                    fav = min(outcomes, key=lambda o: o.get("price", 9999))
                    price = fav.get("price", "")
                    if price != "":
                        favored_team = fav["name"]
                        point_str = str(int(price))
            elif market["key"] == "totals":
                for outcome in market.get("outcomes", []):
                    if outcome.get("name") == "Over":
                        point = outcome.get("point", "")
                        if point != "":
                            ou_str = str(point)
                        break

        odds_lookup[(away, home)] = (favored_team, point_str, ou_str)

    return odds_lookup, int(remaining) if remaining != "?" else None


def match_odds_to_games(games: list[dict], odds_lookup: dict) -> None:
    """Merge spread + over_under from odds_lookup into games list in-place.

    Uses ESPN away_team/home_team names in the spread column — never Odds API names.
    Match priority: exact → alias → fuzzy → nickname (last word).
    """
    odds_keys = list(odds_lookup.keys())
    all_odds_teams = list({t for pair in odds_keys for t in pair})
    nickname_to_odds_team = {_normalize(t): t for t in all_odds_teams}

    def resolve(espn_name: str) -> str | None:
        """Map an ESPN team name to the corresponding Odds API team name."""
        if espn_name in all_odds_teams:
            return espn_name
        aliased = TEAM_NAME_ALIASES.get(espn_name)
        if aliased and aliased in all_odds_teams:
            return aliased
        fuzzy = _fuzzy_match(espn_name, all_odds_teams, threshold=0.78)
        if fuzzy:
            return fuzzy
        return nickname_to_odds_team.get(_normalize(espn_name))

    for game in games:
        espn_away = game["away_team"]
        espn_home = game["home_team"]
        odds_away = resolve(espn_away)
        odds_home = resolve(espn_home)
        if not odds_away or not odds_home:
            continue

        match = odds_lookup.get((odds_away, odds_home))
        if not match:
            # Try swapped — neutral site games sometimes have reversed home/away
            match = odds_lookup.get((odds_home, odds_away))
            if match:
                odds_away, odds_home = odds_home, odds_away  # align with what was found
        if not match:
            continue

        favored_odds_team, point_str, ou_str = match

        if point_str == "PK":
            game["spread"] = "PK"
        else:
            # Map favored Odds API team name back to ESPN name
            if favored_odds_team == odds_away:
                espn_favored = espn_away
            elif favored_odds_team == odds_home:
                espn_favored = espn_home
            else:
                continue  # can't determine favored team safely
            if point_str:
                game["spread"] = f"{espn_favored} {point_str}"
        if ou_str:
            game["over_under"] = ou_str


def build_date_combos(sports: list[str], start_date_override: date | None = None) -> dict[str, list[str]]:
    """Build {sport: [date, ...]} from each sport's season start date to yesterday."""
    yesterday = date.today() - timedelta(days=1)
    combos = {}
    for sport in sports:
        start = start_date_override or SEASON_START_DATES[sport]
        dates = []
        d = start
        while d <= yesterday:
            dates.append(d.isoformat())
            d += timedelta(days=1)
        combos[sport] = dates
        print(f"  {sport}: {len(dates)} dates  ({dates[0]} to {dates[-1]})")
    return combos


def backfill_sport_date(sport: str, game_date: str, worksheet, existing_games: dict, dry_run: bool, fetch_timestamp: str = "") -> tuple[int, int | None]:
    """Fetch schedule + scores + odds for one sport/date and write to sheet.

    Returns (rows_written, odds_api_credits_remaining).
    """
    date_str = game_date.replace("-", "")

    if sport == "cbb":
        games = fetch_d1_cbb_games(date_str)
    else:
        games = fetch_and_parse_schedule_api(sport, date_str)
        # Filter out non-competitive events (e.g. NBA All-Star celebrity games, NFL Pro Bowl)
        if sport in ("nba", "nfl"):
            games = [g for g in games if not (g["away_team"].startswith("Team ") or g["home_team"].startswith("Team "))]

    if not games:
        return 0, None

    # Fetch scores — reconstruct string using ESPN names from schedule so
    # away_team/home_team/score columns are always consistent.
    raw_scores = fetch_espn_results(sport, date_str)
    for game in games:
        result = raw_scores.get((game["away_team"], game["home_team"]))
        if result:
            raw, period_scores_str = result
            parts = raw.split(", ")
            if len(parts) == 2:
                away_score = parts[0].rsplit(" ", 1)[-1]
                home_score = parts[1].rsplit(" ", 1)[-1]
                game["score"] = f"{game['away_team']} {away_score}, {game['home_team']} {home_score}"
            else:
                game["score"] = raw
            game["period_scores"] = period_scores_str
        else:
            game["score"] = ""
            game["period_scores"] = ""

    # Fetch odds — spread label uses ESPN names via match_odds_to_games
    credits_remaining = None
    if ODDS_API_KEY:
        odds_lookup, credits_remaining = fetch_odds_api_lines(sport, game_date)
        if odds_lookup:
            match_odds_to_games(games, odds_lookup)

    if dry_run:
        new_count = sum(
            1 for g in games
            if f"{g['game_date']}|{g['away_team']}|{g['home_team']}" not in existing_games
        )
        odds_hits = sum(1 for g in games if g.get("spread"))
        print(
            f"    [dry-run] {sport} {game_date}: {len(games)} games fetched, "
            f"{new_count} would be written, {odds_hits} with odds"
        )
        return new_count, credits_remaining

    new_rows = []
    for game in games:
        key = f"{game['game_date']}|{game['away_team']}|{game['home_team']}"
        if key in existing_games:
            continue

        row = [
            fetch_timestamp,
            game["game_date"],
            game["away_team"],
            game["home_team"],
            game["game_time"],
            game.get("spread", ""),
            game.get("over_under", ""),
            game["tv_network"],
            game["venue"],
            game.get("score", ""),
            game.get("period_scores", ""),
        ]
        new_rows.append(row)
        existing_games[key] = {"row_idx": -1, "spread": game.get("spread", ""), "over_under": game.get("over_under", "")}

    if new_rows:
        worksheet.append_rows(new_rows, value_input_option="USER_ENTERED")

    return len(new_rows), credits_remaining


_BOX_INNER_WIDTH = 54  # width between the │ chars
_BOX_HEIGHT = 5


def _redraw_box(log_lines: int, box: list[str]) -> None:
    """Redraw the progress box in-place above the accumulated log lines."""
    sys.stdout.write(f"\033[{log_lines + _BOX_HEIGHT}A")
    for line in box:
        sys.stdout.write(f"\r\033[2K{line}\n")
    sys.stdout.write(f"\033[{log_lines}B")
    sys.stdout.flush()


def _render_progress_box(sport: str, rows_processed: int, total: int, elapsed: float, label: str = "backfill", unit: str = "rows") -> list[str]:
    pct = rows_processed / total if total else 1.0
    bar_width = _BOX_INNER_WIDTH - 12
    filled = int(bar_width * pct)
    bar = "█" * filled + "░" * (bar_width - filled)
    elapsed_str = f"{int(elapsed // 60)}m{int(elapsed % 60):02d}s"
    if rows_processed >= total:
        eta_str = "done"
    elif rows_processed > 0:
        eta_sec = elapsed / rows_processed * (total - rows_processed)
        eta_str = f"eta {int(eta_sec // 60)}m{int(eta_sec % 60):02d}s"
    else:
        eta_str = "eta --"
    title   = f"  {sport.upper()} — {label}"
    bar_ln  = f"  {bar}  {pct*100:.0f}%"
    stat_ln = f"  {rows_processed}/{total} {unit} · elapsed {elapsed_str} · {eta_str}"
    return [
        f"┌{'─' * _BOX_INNER_WIDTH}┐",
        f"│{title:<{_BOX_INNER_WIDTH}}│",
        f"│{bar_ln:<{_BOX_INNER_WIDTH}}│",
        f"│{stat_ln:<{_BOX_INNER_WIDTH}}│",
        f"└{'─' * _BOX_INNER_WIDTH}┘",
    ]


def update_period_scores(sport: str, worksheet, dry_run: bool, limit: int | None) -> int:
    """Fill in period_scores for existing rows that have a score but no period_scores.

    Groups qualifying rows by date and makes one ESPN API call per date.
    Writes to the sheet incrementally (one batch_update per date).
    Returns the number of rows updated.
    """
    import gspread.utils

    all_rows = worksheet.get_all_values()
    headers = all_rows[0]

    try:
        game_date_col = headers.index("game_date")
        away_team_col = headers.index("away_team")
        home_team_col = headers.index("home_team")
        score_col = headers.index("score")
        period_scores_col = headers.index("period_scores")
    except ValueError as e:
        print(f"  ❌ Missing column: {e}")
        return 0

    today = date.today()
    dates_needing_update: dict[str, list] = {}

    for i, row in enumerate(all_rows[1:], start=2):
        score = row[score_col] if len(row) > score_col else ""
        period_scores = row[period_scores_col] if len(row) > period_scores_col else ""
        if not score or score == "N/A" or period_scores:
            continue
        game_date_str = row[game_date_col]
        away_team = row[away_team_col]
        home_team = row[home_team_col]
        try:
            if datetime.strptime(game_date_str, "%Y-%m-%d").date() >= today:
                continue
        except ValueError:
            continue
        dates_needing_update.setdefault(game_date_str.replace("-", ""), []).append(
            (i, away_team, home_team)
        )

    total_qualifying = sum(len(v) for v in dates_needing_update.values())
    print(f"  {sport}: {total_qualifying} rows across {len(dates_needing_update)} date(s) need period_scores")

    if dry_run or not dates_needing_update:
        return 0

    rows_processed = 0
    total_updated = 0
    total_to_process = min(limit, total_qualifying) if limit is not None else total_qualifying
    start_time = time.monotonic()
    live = sys.stdout.isatty()

    if live:
        for line in _render_progress_box(sport, 0, total_to_process, 0, label="period_scores migration"):
            print(line)
    log_lines = 0

    for date_key, rows_for_date in sorted(dates_needing_update.items()):
        if limit is not None and rows_processed >= limit:
            break
        try:
            espn_results = fetch_espn_results(sport, date_key)
        except Exception as e:
            print(f"    {date_key}: skipped — {e}")
            rows_processed += len(rows_for_date)
            continue
        date_batch = []
        for row_idx, away_team, home_team in rows_for_date:
            if limit is not None and rows_processed >= limit:
                break
            rows_processed += 1
            result = espn_results.get((away_team, home_team))
            if result:
                _, period_scores_str = result
                if period_scores_str:
                    date_batch.append({
                        "range": gspread.utils.rowcol_to_a1(row_idx, period_scores_col + 1),
                        "values": [[period_scores_str]],
                    })
        if date_batch:
            worksheet.batch_update(date_batch)
            total_updated += len(date_batch)
            time.sleep(1)

        elapsed = time.monotonic() - start_time
        log_line = f"    {date_key}: updated {len(date_batch)} rows"

        if live:
            print(log_line)
            log_lines += 1
            box = _render_progress_box(sport, rows_processed, total_to_process, elapsed, label="period_scores migration")
            _redraw_box(log_lines, box)
        else:
            pct = rows_processed / total_to_process * 100 if total_to_process else 100
            eta_str = ""
            if rows_processed > 0 and rows_processed < total_to_process:
                eta_sec = elapsed / rows_processed * (total_to_process - rows_processed)
                eta_str = f"  eta {int(eta_sec // 60)}m{int(eta_sec % 60):02d}s"
            print(f"{log_line}  [{pct:.0f}%{eta_str}]")

    return total_updated


def main():
    parser = argparse.ArgumentParser(description="Backfill historical schedules from master_sheet")
    parser.add_argument("--dry-run", action="store_true", help="Print actions without writing to sheets")
    parser.add_argument("--sport", choices=["nba", "cbb", "nhl", "nfl", "cfb"], help="Only process this sport")
    parser.add_argument("--limit", type=int, help="Stop after processing this many dates/rows")
    parser.add_argument("--start-date", help="Override season start date (YYYY-MM-DD)")
    parser.add_argument("--update-period-scores", action="store_true",
                        help="Fill period_scores for existing rows that have a score but no period_scores")
    args = parser.parse_args()

    if args.dry_run:
        print("--- DRY RUN MODE — no writes will happen ---\n")

    if args.update_period_scores:
        sports = [args.sport] if args.sport else list(SPORT_CONFIG.keys())
        client = get_gspread_client()
        spreadsheet = client.open_by_key(GOOGLE_SHEET_ID)
        total = 0
        for sport in sports:
            ws_name = SPORT_CONFIG[sport]
            ws = get_or_create_worksheet(spreadsheet, ws_name)
            print(f"\n{sport.upper()}...")
            total += update_period_scores(sport, ws, args.dry_run, args.limit)
        print(f"\nDone. Total rows updated: {total}")
        return

    fetch_timestamp = datetime.now(ZoneInfo("America/New_York")).strftime("%Y-%m-%d %H:%M:%S")

    if not ODDS_API_KEY:
        print("⚠️  ODDS_API_KEY not set — spread/over_under will be empty\n")

    start_date_override = date.fromisoformat(args.start_date) if args.start_date else None
    sports = [args.sport] if args.sport else list(SPORT_CONFIG.keys())

    print("Building date ranges...")
    combos = build_date_combos(sports, start_date_override)

    client = get_gspread_client()
    spreadsheet = client.open_by_key(GOOGLE_SHEET_ID)

    # Pre-load worksheets and existing games once per sport
    worksheets = {}
    existing_games_by_sport = {}
    for sport, ws_name in SPORT_CONFIG.items():
        if sport not in combos:
            continue
        ws = get_or_create_worksheet(spreadsheet, ws_name)
        worksheets[sport] = ws
        existing_games_by_sport[sport] = get_existing_games(ws)
        print(f"  {sport}: {len(existing_games_by_sport[sport])} rows already in sheet")

    print()

    total_written = 0
    total_combos = sum(len(dates) for dates in combos.values())
    processed = 0
    last_credits = None
    live = sys.stdout.isatty()

    for sport, dates in sorted(combos.items()):
        sorted_dates = sorted(dates)
        remaining_limit = (args.limit - processed) if args.limit else None
        effective_total = min(len(sorted_dates), remaining_limit) if remaining_limit is not None else len(sorted_dates)

        sport_processed = 0
        sport_start = time.monotonic()
        log_lines = 0

        if live and effective_total > 0:
            for line in _render_progress_box(sport, 0, effective_total, 0, label="backfill", unit="dates"):
                print(line)

        for game_date in sorted_dates:
            if args.limit and processed >= args.limit:
                print(f"Reached --limit {args.limit}, stopping.")
                print(f"\nDone. Total rows written: {total_written}")
                if last_credits is not None:
                    print(f"Odds API credits remaining: {last_credits}")
                return

            processed += 1
            sport_processed += 1
            log_line = f"  [{processed}/{total_combos}] {sport} {game_date}..."

            try:
                written, credits = backfill_sport_date(
                    sport,
                    game_date,
                    worksheets.get(sport),
                    existing_games_by_sport.get(sport, {}),
                    args.dry_run,
                    fetch_timestamp,
                )
                log_line += f" {written} rows written"
                total_written += written
                if credits is not None:
                    last_credits = credits
            except Exception as e:
                log_line += f" ERROR: {e}"

            elapsed = time.monotonic() - sport_start

            if live:
                print(log_line)
                log_lines += 1
                box = _render_progress_box(sport, sport_processed, effective_total, elapsed, label="backfill", unit="dates")
                _redraw_box(log_lines, box)
            else:
                pct = sport_processed / effective_total * 100 if effective_total else 100
                eta_str = ""
                if sport_processed > 0 and sport_processed < effective_total:
                    eta_sec = elapsed / sport_processed * (effective_total - sport_processed)
                    eta_str = f"  eta {int(eta_sec // 60)}m{int(eta_sec % 60):02d}s"
                print(f"{log_line}  [{pct:.0f}%{eta_str}]")

    print(f"\nDone. Total rows written: {total_written}")
    if last_credits is not None:
        print(f"Odds API credits remaining: {last_credits}")


if __name__ == "__main__":
    main()
