#!/usr/bin/env python3
"""
Hallucination audit for parsed_picks_new (and any picks sheet).

Two-pass approach:
  Pass 1 (fast, free): substring/abbreviation check — flags obvious mismatches
  Pass 2 (Opus): sends suspects to Claude Opus in batches to confirm which are
                 genuine hallucinations vs. legitimate abbreviation/nickname usage

The Opus auditor is a standalone function `opus_audit_suspects()` that can be
imported and called from any workflow (e.g. capper_analyzer.py on a
scheduled basis to catch new hallucinations as they come in).

CLI usage:
  .venv/bin/python3 audit_hallucinations.py [--dry-run] [--skip-opus]

Flags:
  --dry-run     Print results without writing to the sheet
  --skip-opus   Only run pass 1 (substring check), skip Opus confirmation
"""

import os
import re
import json
import argparse
import time
from typing import List, Dict, Tuple

import anthropic
import gspread
from dotenv import load_dotenv

from sheets_utils import GOOGLE_SHEET_ID, get_gspread_client

load_dotenv()
SHEET_NAME = "parsed_picks_new"

HEADERS = ["date", "capper", "sport", "pick", "line", "game", "spread", "result", "ocr_text"]
COL = {h: i for i, h in enumerate(HEADERS)}

# Opus pricing: $15.00/M input, $75.00/M output
OPUS_INPUT_COST_PER_M  = 15.00
OPUS_OUTPUT_COST_PER_M = 75.00

# How many suspects to send Opus per API call (keeps prompts manageable)
OPUS_BATCH_SIZE = 30


# ── Abbreviation / nickname map ───────────────────────────────────────────────
# Keys: abbreviation that may appear in OCR
# Values: substrings of the full team name stored in the pick column
ABBREV_MAP = {
    # NBA
    "GSW": ["golden state"],
    "LAL": ["los angeles lakers", "lakers"],
    "LAC": ["los angeles clippers", "clippers"],
    "NYK": ["new york knicks", "knicks"],
    "BKN": ["brooklyn nets", "nets"],
    "PHX": ["phoenix suns", "suns"],
    "MIL": ["milwaukee bucks", "bucks"],
    "BOS": ["boston celtics", "celtics"],
    "MIA": ["miami heat", "heat"],
    "CHI": ["chicago bulls", "bulls"],
    "DET": ["detroit pistons", "pistons"],
    "IND": ["indiana pacers", "pacers"],
    "CLE": ["cleveland cavaliers", "cavaliers", "cavs"],
    "ATL": ["atlanta hawks", "hawks"],
    "CHA": ["charlotte hornets", "hornets"],
    "ORL": ["orlando magic", "magic"],
    "WAS": ["washington wizards", "wizards"],
    "TOR": ["toronto raptors", "raptors"],
    "MEM": ["memphis grizzlies", "grizzlies"],
    "NOP": ["new orleans pelicans", "pelicans"],
    "SAS": ["san antonio spurs", "spurs"],
    "DAL": ["dallas mavericks", "mavericks", "mavs"],
    "HOU": ["houston rockets", "rockets"],
    "OKC": ["oklahoma city thunder", "thunder"],
    "DEN": ["denver nuggets", "nuggets"],
    "UTA": ["utah jazz", "jazz"],
    "MIN": ["minnesota timberwolves", "timberwolves", "wolves"],
    "POR": ["portland trail blazers", "trail blazers", "blazers"],
    "SAC": ["sacramento kings", "kings"],
    # NHL
    "VGK": ["vegas golden knights", "golden knights"],
    "TBL": ["tampa bay lightning", "lightning"],
    "FLA": ["florida panthers", "panthers"],
    "NYR": ["new york rangers", "rangers"],
    "NYI": ["new york islanders", "islanders"],
    "NJD": ["new jersey devils", "devils"],
    "PHI": ["philadelphia flyers", "flyers"],
    "PIT": ["pittsburgh penguins", "penguins"],
    "WSH": ["washington capitals", "capitals", "caps"],
    "CAR": ["carolina hurricanes", "hurricanes", "canes"],
    "CBJ": ["columbus blue jackets", "blue jackets"],
    "DET": ["detroit red wings", "red wings"],
    "BOS": ["boston bruins", "bruins"],
    "MTL": ["montreal canadiens", "canadiens", "habs"],
    "OTT": ["ottawa senators", "senators"],
    "TOR": ["toronto maple leafs", "maple leafs", "leafs"],
    "BUF": ["buffalo sabres", "sabres"],
    "MIN": ["minnesota wild", "wild"],
    "CHI": ["chicago blackhawks", "blackhawks"],
    "STL": ["st. louis blues", "blues"],
    "COL": ["colorado avalanche", "avalanche", "avs"],
    "DAL": ["dallas stars", "stars"],
    "NSH": ["nashville predators", "predators", "preds"],
    "WPG": ["winnipeg jets", "jets"],
    "ARI": ["arizona coyotes", "coyotes"],
    "VAN": ["vancouver canucks", "canucks"],
    "CGY": ["calgary flames", "flames"],
    "EDM": ["edmonton oilers", "oilers"],
    "SJS": ["san jose sharks", "sharks"],
    "ANA": ["anaheim ducks", "ducks"],
    "LAK": ["los angeles kings"],
    "SEA": ["seattle kraken", "kraken"],
    # CBB common acronyms
    "UNC": ["north carolina tar heels", "north carolina"],
    "USC": ["usc trojans", "southern california"],
    "UCF": ["ucf knights", "central florida"],
    "VCU": ["vcu rams", "virginia commonwealth"],
    "SMU": ["smu mustangs", "southern methodist"],
    "TCU": ["tcu horned frogs", "texas christian"],
    "LSU": ["lsu tigers", "louisiana state"],
    "BYU": ["byu cougars", "brigham young"],
    "UCLA": ["ucla bruins"],
    "UNLV": ["unlv rebels", "nevada las vegas"],
    "UTEP": ["utep miners", "texas el paso"],
    "UTSA": ["utsa roadrunners", "texas san antonio"],
    "UAB": ["uab blazers", "alabama birmingham"],
    "UIC": ["uic flames", "illinois chicago"],
    "ETSU": ["east tennessee state"],
    "UNCW": ["unc wilmington", "wilmington seahawks"],
    "NEB": ["nebraska cornhuskers", "nebraska"],
    "PITT": ["pittsburgh panthers", "pittsburgh"],
    "IOWA": ["iowa hawkeyes", "iowa"],
    "UK": ["kentucky wildcats", "kentucky"],
    "OU": ["oklahoma sooners", "oklahoma"],
    "UT": ["texas longhorns", "tennessee volunteers"],
    "FAU": ["florida atlantic owls", "florida atlantic"],
    "UVA": ["virginia cavaliers", "virginia"],
    "UMASS": ["massachusetts minutemen", "massachusetts"],
    "UCONN": ["connecticut huskies", "connecticut"],
    "UMBC": ["umbc retrievers", "maryland baltimore county"],
    "URI": ["rhode island rams", "rhode island"],
    "USF": ["south florida bulls", "south florida"],
    "UNI": ["northern iowa panthers", "northern iowa"],
    "SDSU": ["san diego state aztecs", "san diego state"],
    "SJSU": ["san jose state spartans", "san jose state"],
    "NMSU": ["new mexico state aggies", "new mexico state"],
    "FAU": ["florida atlantic owls", "florida atlantic"],
    "FIU": ["florida international panthers", "florida international"],
    "ODU": ["old dominion monarchs", "old dominion"],
    "GCU": ["grand canyon antelopes", "grand canyon"],
    "LMU": ["loyola marymount lions", "loyola marymount"],
    "SLU": ["saint louis billikens", "saint louis"],
    "NAVY": ["navy midshipmen"],
    "ARMY": ["army black knights"],
    "YALE": ["yale bulldogs"],
    "WKU": ["western kentucky hilltoppers", "western kentucky"],
    "EMU": ["eastern michigan eagles", "eastern michigan"],
    "WMU": ["western michigan broncos", "western michigan"],
    "CMU": ["central michigan chippewas", "central michigan"],
    "NIU": ["northern illinois huskies", "northern illinois"],
    "BGSU": ["bowling green falcons", "bowling green"],
    "KSU": ["kansas state wildcats", "kent state golden flashes"],
}


# ── Pass 1: fast substring check ─────────────────────────────────────────────
def pick_in_ocr(pick: str, ocr: str) -> bool:
    """Return True if the pick team can be found in the OCR text via any heuristic."""
    pick_lower = pick.lower().strip()
    ocr_lower = ocr.lower()

    if not pick_lower or not ocr_lower:
        return False

    # Direct substring
    if pick_lower in ocr_lower:
        return True

    # Any word from the pick that is ≥5 chars
    words = [w for w in pick_lower.split() if len(w) >= 5]
    for word in words:
        if word in ocr_lower:
            return True

    # Abbreviation / nickname map
    for abbrev, full_names in ABBREV_MAP.items():
        for full in full_names:
            if full in pick_lower:
                pattern = r'\b' + re.escape(abbrev) + r'\b'
                if re.search(pattern, ocr.upper()):
                    return True

    return False


# ── Pass 2: Opus hallucination confirmation ───────────────────────────────────
def opus_audit_suspects(
    suspects: List[Dict],
    dry_run: bool = False,
) -> Tuple[List[Dict], List[Dict]]:
    """Use Claude Opus to determine which suspect rows are genuine hallucinations.

    This function is designed to be imported and reused in scheduled workflows.

    Args:
        suspects: list of dicts with keys: date, capper, sport, pick, line, ocr_text
        dry_run:  if True, skip the API call and return all suspects as unconfirmed

    Returns:
        (confirmed_hallucinations, false_positives)
        confirmed_hallucinations: rows Opus says are NOT traceable to OCR
        false_positives: rows Opus says ARE legitimate (abbreviation/nickname match)
    """
    if dry_run or not suspects:
        return suspects, []

    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not api_key:
        raise ValueError("ANTHROPIC_API_KEY not set")

    client = anthropic.Anthropic(api_key=api_key)

    confirmed = []
    false_positives = []
    total_input_tokens = 0
    total_output_tokens = 0

    for batch_start in range(0, len(suspects), OPUS_BATCH_SIZE):
        batch = suspects[batch_start: batch_start + OPUS_BATCH_SIZE]
        batch_end = batch_start + len(batch)
        print(f"  Opus audit: rows {batch_start+1}–{batch_end} of {len(suspects)}...")

        # Build the prompt
        rows_text = ""
        for i, s in enumerate(batch):
            rows_text += (
                f"[{i}] date={s['date']} capper={s['capper']} sport={s['sport']} "
                f"pick=\"{s['pick']}\" line={s['line']}\n"
                f"    OCR: {s['ocr_text'][:600]}\n\n"
            )

        prompt = f"""You are auditing betting pick data for hallucinations.

For each numbered row below, determine: does the PICK appear in the OCR text?
A pick is valid if the team name, a standard abbreviation, or a common nickname
for that team appears anywhere in the OCR text — even partially.

Examples of VALID matches (not hallucinations):
- pick="Duke Blue Devils" OCR="Duke -15.5" → VALID (Duke is in OCR)
- pick="BYU Cougars" OCR="BYU ML -120" → VALID (BYU abbreviation)
- pick="Cleveland Cavaliers" OCR="Cavs ML (-103)" → VALID (Cavs nickname)
- pick="USC Trojans" OCR="USC +6.5" → VALID (USC abbreviation)
- pick="Nebraska Cornhuskers" OCR="NEB -5.5" → VALID (NEB abbreviation)
- pick="East Tennessee State" OCR="ETSU -3.5" → VALID (ETSU acronym)

Examples of HALLUCINATIONS (pick NOT in OCR):
- pick="Orlando Magic" OCR="Detroit Pistons +4 / Utah Jazz ML / Memphis Grizzlies -3" → HALLUCINATION
- pick="Sacramento Kings" OCR="Utah Jazz -125 Detroit Pistons +4" → HALLUCINATION

Respond with a JSON array. Each element: {{"idx": <row index>, "verdict": "VALID" or "HALLUCINATION", "reason": "<brief>"}}
Output ONLY the JSON array, no other text.

ROWS TO AUDIT:
{rows_text}"""

        try:
            response = client.messages.create(
                model="claude-opus-4-6",
                max_tokens=2000,
                messages=[{"role": "user", "content": prompt}],
            )
            total_input_tokens  += response.usage.input_tokens
            total_output_tokens += response.usage.output_tokens

            text = response.content[0].text.strip()
            # Strip markdown code fences if present
            if text.startswith("```"):
                text = text.split("```")[1]
                if text.startswith("json"):
                    text = text[4:]
                text = text.strip()

            verdicts = json.loads(text)
            verdict_map = {v["idx"]: v["verdict"] for v in verdicts}

            for i, suspect in enumerate(batch):
                if verdict_map.get(i, "VALID") == "HALLUCINATION":
                    confirmed.append(suspect)
                    reason = next((v.get("reason","") for v in verdicts if v["idx"] == i), "")
                    print(f"    HALLUCINATION [{suspect['date']}] {suspect['capper']} | {suspect['pick']} — {reason}")
                else:
                    false_positives.append(suspect)

        except Exception as e:
            print(f"  Warning: Opus API error on batch {batch_start}–{batch_end}: {e}")
            # On error, treat the whole batch as unconfirmed (keep as suspect)
            confirmed.extend(batch)

        # Avoid hammering the API
        if batch_end < len(suspects):
            time.sleep(1)

    cost = (
        (total_input_tokens  / 1_000_000) * OPUS_INPUT_COST_PER_M +
        (total_output_tokens / 1_000_000) * OPUS_OUTPUT_COST_PER_M
    )
    print(f"  Opus tokens  input: {total_input_tokens:,}  output: {total_output_tokens:,}")
    print(f"  Estimated Opus cost: ${cost:.4f}")
    print(f"  Confirmed hallucinations: {len(confirmed)}  |  False positives cleared: {len(false_positives)}")

    return confirmed, false_positives


# ── Sheet rewrite ─────────────────────────────────────────────────────────────
def rewrite_sheet(ws, top_rows, header_row, clean_rows, suspect_rows):
    """Sort clean rows by date, append suspects at the bottom after a blank separator."""
    clean_rows.sort(key=lambda r: r[COL["date"]] if len(r) > COL["date"] else "")

    new_data = list(top_rows) + [header_row] + clean_rows
    if suspect_rows:
        new_data.append([""] * len(HEADERS))  # blank separator
        new_data += suspect_rows

    print(f"\nClearing and rewriting sheet ({len(new_data)} total rows)...")
    ws.clear()
    chunk_size = 500
    for i in range(0, len(new_data), chunk_size):
        chunk = new_data[i: i + chunk_size]
        start_row = i + 1
        end_row   = start_row + len(chunk) - 1
        col_letter = chr(ord("A") + len(HEADERS) - 1)
        ws.update(f"A{start_row}:{col_letter}{end_row}", chunk, value_input_option="USER_ENTERED")
        print(f"  Wrote rows {start_row}–{end_row}")
        if i + chunk_size < len(new_data):
            time.sleep(1)  # stay inside write quota


# ── CLI entry point ───────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run",    action="store_true", help="Print results without writing to sheet")
    parser.add_argument("--skip-opus",  action="store_true", help="Skip Opus pass (pass 1 only)")
    args = parser.parse_args()

    print("Connecting to Google Sheets...")
    client_gs = get_gspread_client()
    ss = client_gs.open_by_key(GOOGLE_SHEET_ID)
    ws = ss.worksheet(SHEET_NAME)

    print(f"Loading {SHEET_NAME}...")
    all_values = ws.get_all_values()

    if len(all_values) < 4:
        print("Sheet has fewer than 4 rows — nothing to process.")
        return

    top_rows   = all_values[:2]
    header_row = all_values[2]
    data_rows  = all_values[3:]
    print(f"  {len(data_rows)} data rows loaded")

    col = {h: i for i, h in enumerate(header_row)}
    date_col   = col.get("date",     COL["date"])
    pick_col   = col.get("pick",     COL["pick"])
    ocr_col    = col.get("ocr_text", COL["ocr_text"])
    capper_col = col.get("capper",   COL["capper"])
    sport_col  = col.get("sport",    COL["sport"])
    line_col   = col.get("line",     COL["line"])

    # ── Pass 1: fast substring filter ────────────────────────────────────────
    clean_rows   = []
    pass1_suspects = []
    no_ocr_rows  = []

    for row in data_rows:
        if not any(cell.strip() for cell in row):
            continue
        while len(row) < len(HEADERS):
            row.append("")

        pick  = row[pick_col].strip()
        ocr   = row[ocr_col].strip()

        if not ocr:
            no_ocr_rows.append(row)
        elif not pick or pick_in_ocr(pick, ocr):
            clean_rows.append(row)
        else:
            pass1_suspects.append(row)

    print(f"\nPass 1 (substring check):")
    print(f"  Clean:    {len(clean_rows)}")
    print(f"  Suspects: {len(pass1_suspects)}")
    print(f"  No-OCR:   {len(no_ocr_rows)}")

    # ── Pass 2: Opus confirmation ─────────────────────────────────────────────
    if args.skip_opus or not pass1_suspects:
        confirmed_hallucinations = pass1_suspects
        cleared = []
    else:
        print(f"\nPass 2 (Opus audit of {len(pass1_suspects)} suspects)...")
        suspect_dicts = [
            {
                "date":     row[date_col],
                "capper":   row[capper_col],
                "sport":    row[sport_col],
                "pick":     row[pick_col],
                "line":     row[line_col],
                "ocr_text": row[ocr_col],
                "_row":     row,  # keep original row for sheet rewrite
            }
            for row in pass1_suspects
        ]
        # dry_run only gates the sheet write — always run Opus so we can preview results
        confirmed_dicts, cleared_dicts = opus_audit_suspects(suspect_dicts, dry_run=False)
        confirmed_hallucinations = [d["_row"] for d in confirmed_dicts]
        cleared = [d["_row"] for d in cleared_dicts]
        # Cleared rows move back to clean
        clean_rows.extend(cleared)

    print(f"\nFinal counts:")
    print(f"  Clean rows (incl. cleared):    {len(clean_rows) + len(no_ocr_rows)}")
    print(f"  Confirmed hallucinations:      {len(confirmed_hallucinations)}")

    if confirmed_hallucinations:
        print(f"\nConfirmed hallucinated picks (will be moved to bottom of sheet):")
        for row in confirmed_hallucinations:
            print(f"  [{row[date_col]}] {row[capper_col]} | {row[sport_col]} | {row[pick_col]} {row[line_col]}")
            print(f"    OCR: {row[ocr_col][:120]}{'...' if len(row[ocr_col]) > 120 else ''}")

    if args.dry_run:
        print("\n[dry-run] Not writing to sheet.")
        return

    if not confirmed_hallucinations:
        print("\nNo hallucinations found — rewriting sheet (sort by date only).")

    answer = input("\nProceed with rewriting sheet? [y/N] ").strip().lower()
    if answer != "y":
        print("Aborted.")
        return

    rewrite_sheet(
        ws,
        top_rows,
        header_row,
        clean_rows + no_ocr_rows,
        confirmed_hallucinations,
    )

    print(f"\nDone.")
    print(f"  {len(clean_rows) + len(no_ocr_rows)} clean rows sorted by date")
    if confirmed_hallucinations:
        print(f"  {len(confirmed_hallucinations)} hallucinated rows at the bottom — review and delete")


if __name__ == "__main__":
    main()
