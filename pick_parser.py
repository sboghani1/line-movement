"""
Pick parsing utilities: prompt construction, CSV response parsing, and
row assembly/deduplication for Stage 1 and Stage 2.

Extracted from capper_analyzer.py so that prompt-level changes are isolated
from the sheets/orchestration logic.
"""

import csv
import io
import os
import re
from typing import List, Optional, Tuple

import anthropic

# ── Claude usage tracking ────────────────────────────────────────────────────
# Sonnet 4.6 pricing: $3.00/M input, $15.00/M output
CLAUDE_USAGE = {"input_tokens": 0, "output_tokens": 0}
SONNET_INPUT_COST_PER_M = 3.00
SONNET_OUTPUT_COST_PER_M = 15.00


def get_claude_cost():
    """Calculate estimated cost from accumulated token usage."""
    input_cost = (CLAUDE_USAGE["input_tokens"] / 1_000_000) * SONNET_INPUT_COST_PER_M
    output_cost = (CLAUDE_USAGE["output_tokens"] / 1_000_000) * SONNET_OUTPUT_COST_PER_M
    return input_cost + output_cost


def log_claude_usage(message):
    """Accumulate token usage from a Claude API response."""
    if hasattr(message, "usage"):
        CLAUDE_USAGE["input_tokens"] += message.usage.input_tokens
        CLAUDE_USAGE["output_tokens"] += message.usage.output_tokens


# ── Picks schema ─────────────────────────────────────────────────────────────
PICKS_COLUMNS = [
    "date",
    "capper",
    "sport",
    "pick",
    "line",
    "game",
    "spread",
    "result",
    "ocr_text",
    "source",   # index 9 — "discord_all_in_one" or "telegram_cappers_free"
]

# ── Source identifiers ───────────────────────────────────────────────────────
DISCORD_SOURCE = "discord_all_in_one"
TELEGRAM_SOURCE = "telegram_cappers_free"

# ── Per-source prompt configuration ──────────────────────────────────────────
# New sports go to TELEGRAM first; enable for Discord only after its own testing.
DISCORD_VALID_SPORTS = {"NBA", "CBB", "NHL", "NCAAB"}
TELEGRAM_VALID_SPORTS = {"NBA", "CBB", "NHL", "NCAAB", "MLB"}

_DISCORD_SPORT_DEF = "NBA, CBB, or NHL only. Normalize NCAAB to CBB."
_TELEGRAM_SPORT_DEF = "NBA, CBB, NHL, or MLB only. Normalize NCAAB to CBB."

_DISCORD_FILTER_RULES = """- Sports: NBA, NHL, CBB (college basketball) ONLY. Skip ATP, NFL, soccer, etc.
- Bet types: Spread or Moneyline (ML) ONLY
- Skip: Totals (O/U), player props, team totals, first half bets, quarter bets, parlays, live bets"""

_TELEGRAM_FILTER_RULES = """- Sports: NBA, NHL, CBB (college basketball), MLB ONLY. Skip ATP, NFL, soccer, tennis, etc.
- Bet types: Spread or Moneyline (ML) ONLY. For MLB: ML and run line (-1.5/+1.5) are valid.
- Skip: Totals (O/U — including baseball totals like "over 7"), player props, team totals, first half bets, quarter bets, parlays, live bets
- For MLB: "Guardians/Mariners over 7" is a total — SKIP. "Guardians ML" is valid — INCLUDE."""

_DISCORD_PARSING_PATTERNS = """- "-8 O Texas A&M 6-UNITS" format: The "O" means "over" (against opponent), NOT an over/under total. Extract ONLY the spread: line="-8"
- "ML -130 v Capitals 5u POTD" format: Extra text after bet type. Extract ONLY: line="ML". Ignore odds (-130), opponent references (v Capitals), and unit sizes (5u).
- Any "v ", "v.", or "vs" followed by a team name is context to ignore, not part of the line.
- NHL "3-way", "3way", "3-way ML", "3way moneyline", "3 way ml" = regulation win bet — treat as line=ML. This is NOT a parlay.
- "MI" after a team name is a common OCR misread of "ML" — treat it as ML (e.g. "Kentucky MI (-140)" = Kentucky ML, "New Mexico St MI (-140)" = New Mexico St ML). "MI"/"ML" is the bet type, NOT a second team — "Team MI" on one line = one pick."""

_TELEGRAM_PARSING_PATTERNS = """- "@handle • Today 5:54 AM" style headers are message metadata — ignore them entirely, do not extract as picks.
- Emoji bullets (✓, •, ✅, 🔒, etc.) precede individual picks — use them to identify pick boundaries but do not include in output.
- Unit sizes like "2U", "3U", "0.5U" are stake sizes — ignore them, do not include in the line column.
- NHL "3-way", "3way", "3-way ML", "3way moneyline", "3 way ml" = regulation win bet — treat as line=ML. This is NOT a parlay.
- "MI" after a team name is a common OCR misread of "ML" — treat it as ML."""

# ── Prompt examples ──────────────────────────────────────────────────────────
# Example rows for prompts (spread and ML per sport - NO totals)
# Includes examples for tricky formats: Porter Picks "O opponent" and Analytics Capper "v opponent"
EXAMPLE_PICKS_ROWS_DISCORD = """2026-02-01,BEEZO WINS,CBB,Iowa State Cyclones,-11.5,Iowa State Cyclones vs Kansas State Wildcats,,
2026-02-01,DARTH FADER,NBA,LA Clippers,+2,LA Clippers @ Phoenix Suns,,
2026-02-01,A11 BETS,NBA,LA Clippers,ML,LA Clippers @ Phoenix Suns,,
2026-02-03,ANALYTICS CAPPER,NHL,Philadelphia Flyers,ML,Washington Capitals @ Philadelphia Flyers,,
2026-02-04,PORTER PICKS,CBB,Alabama Crimson Tide,-8,Alabama Crimson Tide vs Texas A&M Aggies,,
2026-02-03,HAMMERING HANK,NBA,Brooklyn Nets,+8.5,Los Angeles Lakers @ Brooklyn Nets,,
2026-02-01,HAMMERING HANK,CBB,Florida Gators,-8.5,Florida Gators vs Alabama Crimson Tide,,"""

# Telegram examples will be populated as real Telegram OCR cases emerge.
EXAMPLE_PICKS_ROWS_TELEGRAM = ""

# Stage 2 examples: input (capper,sport,pick,line) → output (capper,pick,game)
# Claude only resolves abbreviations, normalizes capper names, and fills game column.
STAGE2_EXAMPLE_INPUT = """BEEZO WINS,CBB,Iowa State,-11.5
DARTH FADER,NBA,LAC,+2
A11 BETS,NBA,LAC,ML
ANALYTICS CAPPER,NHL,PHI,ML
PORTER PICKS,CBB,Alabama,-8
HAMMERING HANK,NBA,BKN,+8.5
HAMMERING HANK,CBB,Florida,-8.5"""

STAGE2_EXAMPLE_OUTPUT = """BEEZO WINS,Iowa State Cyclones,Iowa State Cyclones vs Kansas State Wildcats
DARTH FADER,LA Clippers,LA Clippers @ Phoenix Suns
A11 BETS,LA Clippers,LA Clippers @ Phoenix Suns
ANALYTICS CAPPER,Philadelphia Flyers,Washington Capitals @ Philadelphia Flyers
PORTER PICKS,Alabama Crimson Tide,Alabama Crimson Tide vs Texas A&M Aggies
HAMMERING HANK,Brooklyn Nets,Los Angeles Lakers @ Brooklyn Nets
HAMMERING HANK,Florida Gators,Florida Gators vs Alabama Crimson Tide"""


# ── Claude API call ──────────────────────────────────────────────────────────
def call_sonnet_text(prompt: str, max_tokens: int = 8192) -> str:
    """Call Claude Sonnet with a text-only prompt."""
    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not api_key:
        raise ValueError("ANTHROPIC_API_KEY environment variable not set")

    client = anthropic.Anthropic(api_key=api_key)
    message = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=max_tokens,
        messages=[{"role": "user", "content": prompt}],
    )
    log_claude_usage(message)
    return message.content[0].text


# ── Prompt builders ──────────────────────────────────────────────────────────
def build_stage1_prompt(
    picks_to_parse: List[Tuple[str, str, str, int]],
    schedule_data: dict,
    source: str = DISCORD_SOURCE,
) -> str:
    """Build the Stage 1 parsing prompt.

    Args:
        picks_to_parse: List of (capper_name, message_date, ocr_text, row_id) tuples
        schedule_data: Dict with 'nba', 'cbb', 'nhl' (and optionally 'mlb') schedule strings
        source: DISCORD_SOURCE or TELEGRAM_SOURCE — selects source-specific prompt sections

    Returns:
        The full prompt string
    """
    picks_section = ""
    for capper, date, ocr_text, row_id in picks_to_parse:
        picks_section += f"\n[ROW:{row_id}] [Capper: {capper}, Date: {date}]\n{ocr_text}\n"

    if source == TELEGRAM_SOURCE:
        parsing_patterns = _TELEGRAM_PARSING_PATTERNS
        example_rows = EXAMPLE_PICKS_ROWS_TELEGRAM
        sport_def = _TELEGRAM_SPORT_DEF
        filter_rules = _TELEGRAM_FILTER_RULES
        mlb_schedule_line = f"\nMLB: {schedule_data.get('mlb', 'No schedule — use team names exactly as written')}"
    else:
        parsing_patterns = _DISCORD_PARSING_PATTERNS
        example_rows = EXAMPLE_PICKS_ROWS_DISCORD
        sport_def = _DISCORD_SPORT_DEF
        filter_rules = _DISCORD_FILTER_RULES
        mlb_schedule_line = ""

    examples_block = (
        f"\nEXAMPLE ROWS (note pick column is always a single FULL team name):\n{example_rows}"
        if example_rows else ""
    )

    prompt = f"""Parse the following betting picks from OCR text into CSV rows.

OUTPUT FORMAT (one row per pick, comma-separated):
date,capper,sport,pick,line,game,spread,result

COLUMN DEFINITIONS:
- date: YYYY-MM-DD format (use the message date provided with each pick)
- capper: Name of the person making the pick (provided with each pick)
- sport: {sport_def}
- pick: A SINGLE team name (the team being bet on). NEVER use "Team A @ Team B" format. Use the schedule to resolve abbreviations (e.g., "TROY -6.5" means bet on "Troy Trojans").
- line: ONLY the spread number or "ML". Strip all extra text (odds, units, opponent names).
- game: Leave empty for now
- spread: Leave empty for now
- result: Leave empty

ROW ATTRIBUTION (CRITICAL):
- Each OCR block is tagged [ROW:N] — ONLY parse picks that appear in that block
- NEVER invent picks, copy picks from other rows, or use the schedule to fabricate bets
- If an OCR block is unreadable or contains no valid picks, output nothing for that row
- Do NOT hallucinate picks that are not explicitly present in the OCR text

SOURCE-SPECIFIC PARSING PATTERNS:
{parsing_patterns}

ABBREVIATION RESOLUTION (MANDATORY):
- The pick column MUST contain the FULL official team name from the schedule (e.g., "Oklahoma City Thunder" not "OKC")
- ALWAYS resolve abbreviations using the schedule provided below
- Common examples: BKN=Brooklyn Nets, CHI=Chicago Bulls, NO/NOP=New Orleans Pelicans, MEM=Memphis Grizzlies, IND=Indiana Pacers, CBJ=Columbus Blue Jackets, EDM=Edmonton Oilers, OKC=Oklahoma City Thunder, PHI=Philadelphia 76ers/Flyers

TOTAL BET DETECTION (SKIP THESE ENTIRELY):
- O or U followed by a number with OR without space: O 220.5, O220.5, O5.5, U5.5, U 150
- These are total bets - do NOT include them in output

CRITICAL - PICK COLUMN MUST BE:
- A single FULL team name like "Troy Trojans" or "Oklahoma City Thunder"
- NEVER a game format like "Georgia Southern Eagles @ Troy Trojans"
- NEVER an abbreviation like "OKC" or "BKN"

NEVER INVERT PICKS (CRITICAL):
- The pick MUST be the EXACT team mentioned in the text - NEVER the opponent
- Under a "Fades:" header, if text says "Virginia +8", pick = Virginia Cavaliers, line = +8. Do NOT pick Duke (the fade target).
- Under a "Fades:" header, if text says "Houston +3", pick = Houston Cougars, line = +3. Do NOT pick Arizona (the fade target).
- Keep the line sign EXACTLY as written (+8 stays +8, -7 stays -7)
- Do NOT "correct" picks based on who is favored in the schedule
- Do NOT interpret "Fades", "Fade", or "Against" labels to mean bet the opponent
- ALWAYS record the team that is explicitly named in the OCR text

FILTERING RULES - ONLY INCLUDE:
{filter_rules}
{examples_block}
TODAY'S SCHEDULE (use to resolve team name abbreviations):
NBA: {schedule_data.get("nba", "No games")}
CBB: {schedule_data.get("cbb", "No games")}
NHL: {schedule_data.get("nhl", "No games")}{mlb_schedule_line}

PICKS TO PARSE:
{picks_section}

OUTPUT (one CSV row per line, no headers, no explanation, no blank lines between rows):"""

    return prompt


def build_stage2_prompt(
    rows_to_finalize: List[str],
    schedule_data: dict,
    known_cappers: Optional[List[str]] = None,
) -> str:
    """Build the Stage 2 finalization prompt.

    Claude receives only the columns it needs to act on (capper,sport,pick,line)
    and returns only the columns it changes (capper,pick,game). Python handles
    the rest: date, sport, line, result pass through from input; spread is
    looked up from the schedule.

    Args:
        rows_to_finalize: List of "capper,sport,pick,line" CSV strings
        schedule_data: Dict with 'nba', 'cbb', 'nhl' schedule strings
        known_cappers: Optional list of known capper names for normalization

    Returns:
        The full prompt string
    """
    rows_section = "\n".join(rows_to_finalize)

    cappers_section = ""
    if known_cappers:
        cappers_section = f"""
KNOWN CAPPERS (normalize capper names to these if similar):
{chr(10).join(f"- {c}" for c in known_cappers)}

CAPPER NORMALIZATION RULES:
- If input capper name is similar to a known capper (case-insensitive, partial match, or close spelling), use the EXACT known capper name
- Examples: "anthony walters" → "Anthony Walters", "A. Walters" → "Anthony Walters", "walters" → "Anthony Walters"
- If no match found, keep the original capper name (properly capitalized)
"""

    prompt = f"""Resolve team abbreviations and fill the game column for these betting picks.

INPUT FORMAT: capper,sport,pick,line
OUTPUT FORMAT: capper,pick,game

You must output EXACTLY one row per input row, in the same order.
{cappers_section}

CRITICAL RULES:
1. FIX pick column: If pick is an abbreviation, resolve to FULL official team name from the schedule:
   - "OKC" → "Oklahoma City Thunder"
   - "BKN" → "Brooklyn Nets"
   - "CBJ" → "Columbus Blue Jackets"
   - "TROY" → "Troy Trojans" (find in schedule)

2. game: "away_team @ home_team" using EXACT team names from the schedule

3. For ML bets: pick = full team name, game = matchup from schedule

ABBREVIATION RESOLUTION (MANDATORY):
- pick column MUST contain FULL official team names from the schedule
- NEVER leave abbreviations like OKC, BKN, CHI, CBJ, EDM, NO, MEM, IND in pick

NEVER INVERT PICKS (CRITICAL):
- The pick MUST match the original team from Stage 1 - NEVER switch to the opponent
- Under a "Fades:" header, if Stage 1 says pick="Virginia Cavaliers" line="+8", keep it as Virginia Cavaliers +8. Do NOT flip to Duke (the fade target).
- Do NOT "correct" the pick based on who is favored in the schedule
- Do NOT flip underdog/favorite - keep the exact team and line from input

NBA SCHEDULE:
{schedule_data.get("nba", "No games")}

NHL SCHEDULE:
{schedule_data.get("nhl", "No games")}

CBB SCHEDULE:
{schedule_data.get("cbb", "No games")}

EXAMPLE INPUT:
{STAGE2_EXAMPLE_INPUT}

EXAMPLE OUTPUT:
{STAGE2_EXAMPLE_OUTPUT}

ROWS TO FINALIZE:
{rows_section}

OUTPUT (one row per input, capper,pick,game — no headers, no explanation):"""

    return prompt


# ── Response parsers ─────────────────────────────────────────────────────────
def _is_valid_date(s: str) -> bool:
    return bool(re.match(r"^\d{4}-\d{2}-\d{2}$", s.strip()))


def _is_valid_sport(s: str, valid_sports: set | None = None) -> bool:
    allowed = valid_sports if valid_sports is not None else DISCORD_VALID_SPORTS
    return s.strip().upper() in allowed


def _is_valid_line(s: str) -> bool:
    val = s.strip().upper()
    if val == "ML":
        return True
    return bool(re.match(r"^[+-]?\d+(\.\d+)?$", val))


def parse_csv_response(response: str, valid_sports: set | None = None) -> List[List[str]]:
    """Parse Stage 1 CSV response into list of 8-column row lists.

    Validates date/sport/line to filter out reasoning text that leaks into output.

    Args:
        valid_sports: Accepted sport codes. Defaults to DISCORD_VALID_SPORTS.
    """
    rows = []
    for line in response.strip().split("\n"):
        line = line.strip()
        if not line or line.startswith("date,"):
            continue
        try:
            for row in csv.reader(io.StringIO(line)):
                if len(row) < 5:
                    continue
                # Handle packed rows: model sometimes writes multiple rows on one line.
                # Scan from position 5 for a date boundary and split there.
                split_at = next(
                    (i for i in range(5, len(row)) if _is_valid_date(row[i])), None
                )
                if split_at is not None:
                    i = 0
                    while i < len(row):
                        if not _is_valid_date(row[i]):
                            i += 1
                            continue
                        next_boundary = next(
                            (j for j in range(i + 5, len(row)) if _is_valid_date(row[j])),
                            len(row),
                        )
                        sub = [f.strip() for f in row[i:next_boundary]]
                        while len(sub) < 8:
                            sub.append("")
                        sub = sub[:8]
                        if _is_valid_sport(sub[2], valid_sports) and _is_valid_line(sub[4]):
                            rows.append(sub)
                        i = next_boundary
                    continue
                if not _is_valid_date(row[0]):
                    continue
                if not _is_valid_sport(row[2], valid_sports):
                    continue
                if not _is_valid_line(row[4]):
                    continue
                while len(row) < 8:
                    row.append("")
                rows.append(row[:8])
        except Exception:
            continue
    return rows


def parse_stage2_response(response: str) -> List[List[str]]:
    """Parse Stage 2 response: each line is capper,pick,game (3 columns)."""
    rows = []
    for line in response.strip().split("\n"):
        line = line.strip()
        if not line or line.lower().startswith("capper,"):
            continue
        try:
            for row in csv.reader(io.StringIO(line)):
                if len(row) >= 3:
                    rows.append(row[:3])
        except Exception:
            continue
    return rows


# ── Row assembly / validation / dedup ────────────────────────────────────────
def lookup_spread_from_schedule(
    pick_team: str, date: str, sport: str, schedule_games: List[dict]
) -> str:
    """Look up the consensus spread from the schedule for a pick."""
    pt = pick_team.lower().strip()
    for game in schedule_games:
        away = game.get("away_team", "")
        home = game.get("home_team", "")
        if (pt and away and home and
            (pt in away.lower() or away.lower() in pt or
             pt in home.lower() or home.lower() in pt)):
            return game.get("spread", "")
    return ""


def assemble_finalized_rows(
    input_rows: List[List[str]],
    stage2_rows: List[List[str]],
    schedule_games_by_sport: dict,
) -> List[List[str]]:
    """Stitch Stage 2 output back with original input rows.

    Takes passthrough columns (date, sport, line, result) from input,
    Claude's output (capper, pick, game) from stage2_rows, and looks up
    spread from the schedule.

    Args:
        input_rows: Original 8-column rows sent to Stage 2
        stage2_rows: Claude's 3-column output (capper, pick, game)
        schedule_games_by_sport: {sport: [game_dicts]} for spread lookup

    Returns:
        List of assembled 8-column rows: [date, capper, sport, pick, line, game, spread, result]
    """
    assembled = []
    for i, s2_row in enumerate(stage2_rows):
        if i >= len(input_rows):
            break

        orig = input_rows[i]
        capper = s2_row[0]
        pick = s2_row[1]
        game = s2_row[2] if len(s2_row) > 2 else ""

        date = orig[0] if len(orig) > 0 else ""
        sport = orig[2] if len(orig) > 2 else ""
        line = orig[4] if len(orig) > 4 else ""
        result = orig[7] if len(orig) > 7 else ""

        games = schedule_games_by_sport.get(sport.lower().strip(), [])
        spread = lookup_spread_from_schedule(pick, "", sport, games)

        assembled.append([date, capper, sport, pick, line, game, spread, result])

    return assembled


def validate_and_fix_pick_column(rows: List[List[str]]) -> List[List[str]]:
    """Fix rows where pick column incorrectly contains a game format (Team A @ Team B)."""
    fixed_rows = []
    for row in rows:
        pick = row[3] if len(row) > 3 else ""
        line = row[4] if len(row) > 4 else ""
        game = row[5] if len(row) > 5 else ""
        line_upper = line.upper().strip()

        if "@" in pick:
            fixed_team = None
            if line_upper != "ML":
                abbrev_match = re.match(r"^([A-Z]{2,5})\s*[+-]", line_upper)
                if abbrev_match:
                    abbrev = abbrev_match.group(1)
                    if game:
                        for team in game.split(" @ "):
                            for word in team.upper().split():
                                if abbrev in word or word.startswith(abbrev):
                                    fixed_team = team
                                    break
                            if fixed_team:
                                break
            if fixed_team:
                row[3] = fixed_team

        fixed_rows.append(row)
    return fixed_rows


def deduplicate_ml_vs_spread(
    new_rows: List[List[str]],
    existing_rows: Optional[List[List[str]]] = None,
    existing_row_offset: int = 0,
) -> Tuple[List[List[str]], List[int]]:
    """Drop ML picks when a spread pick exists for the same capper+game+date.

    Handles both orderings:
    - Spread is new, ML already exists in the sheet → returns sheet row indices to delete
    - ML is new, spread already exists in the sheet → filters ML from new_rows

    Args:
        new_rows: Rows about to be appended (each is a list of column values).
        existing_rows: All data rows already in finalized_picks (list of lists).
        existing_row_offset: The 1-based sheet row index of existing_rows[0].

    Returns:
        (filtered_new_rows, existing_indices_to_delete)
    """
    def is_spread(line: str) -> bool:
        l = line.strip().upper()
        if not l or l == "ML":
            return False
        if l.startswith("O ") or l.startswith("U "):
            return False
        return bool(re.match(r"^[+-]?\d", l))

    def row_key(row: List[str]) -> tuple:
        date = row[0].strip() if len(row) > 0 else ""
        capper = row[1].strip().upper() if len(row) > 1 else ""
        game = row[5].strip().upper() if len(row) > 5 else ""
        return (date, capper, game)

    existing_rows = existing_rows or []

    existing_spread_keys: set = set()
    for row in existing_rows:
        if len(row) > 4 and is_spread(row[4]):
            existing_spread_keys.add(row_key(row))

    new_spread_keys: set = set()
    for row in new_rows:
        if len(row) > 4 and is_spread(row[4]):
            new_spread_keys.add(row_key(row))

    all_spread_keys = existing_spread_keys | new_spread_keys
    filtered_new: List[List[str]] = []
    for row in new_rows:
        line = row[4].strip().upper() if len(row) > 4 else ""
        if line == "ML" and row_key(row) in all_spread_keys:
            print(
                f"  [dedup] Dropping ML pick for {row[1]} - {row[5]} "
                f"(spread exists for this game)"
            )
            continue
        filtered_new.append(row)

    existing_indices_to_delete: List[int] = []
    for i, row in enumerate(existing_rows):
        line = row[4].strip().upper() if len(row) > 4 else ""
        if line == "ML" and row_key(row) in new_spread_keys:
            sheet_row = existing_row_offset + i
            print(
                f"  [dedup] Marking existing ML row {sheet_row} for deletion: "
                f"{row[1]} - {row[5]} (spread arriving in new batch)"
            )
            existing_indices_to_delete.append(sheet_row)

    return filtered_new, existing_indices_to_delete
