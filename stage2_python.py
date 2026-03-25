#!/usr/bin/env python3
"""
stage2_python.py — Pure-Python replacement for Claude Stage 2.

Replaces the Claude API call (build_stage2_prompt → call_sonnet_text →
parse_stage2_response → assemble_finalized_rows) with deterministic Python
using TeamResolver and CapperResolver.

Input:  parsed_picks rows [date, capper, sport, pick, line, game, spread, result, ocr_text]
Output: finalized rows    [date, capper, sport, pick, line, game, spread, result]

Sentinel values when resolution fails:
  - pick   = original raw value (unchanged)
  - capper = "capper_not_found" if CapperResolver fails
  - game   = "team_not_found"   if TeamResolver fails
  - spread = ""                 if no game found

These sentinels are caught by the nightly audit (daily_audit.py) which flags
them for human review. Once a human provides the correct value, remediate.py
adds the mapping to the resolution sheet so the resolver handles it next time.

Usage:
    from stage2_python import finalize_picks_python

    rows = finalize_picks_python(spreadsheet, parsed_rows)
"""

from typing import List, Optional

from capper_resolver import CapperResolver
from team_resolver import TeamResolver, ResolveResult


# Sentinel values written when resolution fails
TEAM_NOT_FOUND = "team_not_found"
CAPPER_NOT_FOUND = "capper_not_found"


def finalize_picks_python(
    team_resolver: TeamResolver,
    capper_resolver: CapperResolver,
    parsed_rows: List[List[str]],
) -> List[List[str]]:
    """Finalize parsed picks using Python resolvers (no Claude API call).

    Takes raw parsed_picks rows and resolves:
      - capper → canonical unique_capper_name via CapperResolver
      - pick   → canonical ESPN team name via TeamResolver
      - game   → "Away @ Home" from schedule via TeamResolver
      - spread → consensus spread from schedule via TeamResolver

    Args:
        team_resolver:   Pre-initialized TeamResolver instance
        capper_resolver: Pre-initialized CapperResolver instance
        parsed_rows:     List of parsed_picks rows (9+ columns):
                         [date, capper, sport, pick, line, game, spread, result, ocr_text, ...]

    Returns:
        List of finalized rows (8 columns):
        [date, capper, sport, pick, line, game, spread, result]
    """
    finalized = []

    for row in parsed_rows:
        if not row:
            continue

        # Extract input columns
        date    = row[0].strip() if len(row) > 0 else ""
        capper  = row[1].strip() if len(row) > 1 else ""
        sport   = row[2].strip() if len(row) > 2 else ""
        pick    = row[3].strip() if len(row) > 3 else ""
        line    = row[4].strip() if len(row) > 4 else ""
        result  = row[7].strip() if len(row) > 7 else ""

        # ── Resolve capper ─────────────────────────────────────────────
        resolved_capper = capper_resolver.resolve(capper)

        # ── Resolve team → ESPN name + game + spread ───────────────────
        team_result: Optional[ResolveResult] = None
        if pick and sport and date:
            team_result = team_resolver.resolve(pick=pick, sport=sport, date=date)

        if team_result:
            resolved_pick   = team_result.espn_name
            resolved_game   = team_result.game
            resolved_spread = team_result.spread
            resolved_sport  = team_result.sport  # may differ if wrong-sport detected
        else:
            # Team not found — keep original pick, mark game as sentinel
            resolved_pick   = pick
            resolved_game   = TEAM_NOT_FOUND
            resolved_spread = ""
            resolved_sport  = sport

        finalized.append([
            date,
            resolved_capper,
            resolved_sport.upper(),
            resolved_pick,
            line,
            resolved_game,
            resolved_spread,
            result,
        ])

    return finalized
