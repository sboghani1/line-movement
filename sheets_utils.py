#!/usr/bin/env python3
"""
Shared Google Sheets utilities.

Centralises authentication, the sheet-ID constant, rate-limit retry logic,
and the sport→schedule-sheet mapping so they aren't duplicated across
capper_analyzer, daily_audit, audit_hallucinations, and populate_results.
"""

import base64
import json
import os
import time
from typing import List

import gspread
from google.oauth2.service_account import Credentials

# ── Constants ────────────────────────────────────────────────────────────────
GOOGLE_SHEET_ID = "1LzkU7rH3OtrJckV5oMvFHyuLAnbRn9E74FO1uyfM65k"

SPORT_TO_SCHED = {
    "nba": "nba_schedule",
    "cbb": "cbb_schedule",
    "nhl": "nhl_schedule",
}

# ── Rate-limit cooldown ──────────────────────────────────────────────────────
# Google Sheets API enforces separate per-user quotas:
#   - Read requests:  60/min  (worksheet, get_all_values, col_values, etc.)
#   - Write requests: 60/min  (update, append_rows, update_cells, delete_rows, etc.)
# Reads and writes have independent buckets — a read doesn't count against the
# write quota and vice versa.  We track the last call time for each type
# separately so reads don't artificially slow down writes (and vice versa).
_READ_COOLDOWN = 1.5   # seconds between consecutive reads  (~40/min, well under 60)
_WRITE_COOLDOWN = 1.5  # seconds between consecutive writes (~40/min, well under 60)
_last_read_time = 0.0
_last_write_time = 0.0


# ── Auth ─────────────────────────────────────────────────────────────────────
def get_gspread_client():
    """Authenticate with Google Sheets using base64-encoded service-account JSON."""
    creds_b64 = os.environ.get("GOOGLE_CREDENTIALS", "")
    if not creds_b64:
        raise ValueError("GOOGLE_CREDENTIALS not set")
    creds_dict = json.loads(base64.b64decode(creds_b64).decode())
    creds = Credentials.from_service_account_info(creds_dict, scopes=[
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ])
    return gspread.authorize(creds)


# ── Retry helpers ────────────────────────────────────────────────────────────
def _sheets_call_with_cooldown(fn, args, kwargs, retries, last_time_ref, cooldown):
    """Internal: call fn with per-bucket cooldown and exponential backoff on 429."""
    delay = 15
    for attempt in range(retries):
        # Enforce cooldown between calls of the same type
        elapsed = time.time() - last_time_ref[0]
        if elapsed < cooldown:
            time.sleep(cooldown - elapsed)

        try:
            last_time_ref[0] = time.time()
            return fn(*args, **kwargs)
        except gspread.exceptions.APIError as e:
            if e.response.status_code == 429 and attempt < retries - 1:
                print(f"  [rate limit] waiting {delay}s before retry {attempt+2}/{retries}...")
                time.sleep(delay)
                delay = min(delay * 2, 120)
            else:
                raise


# Mutable containers so the inner helper can update them
_last_read_ref = [0.0]
_last_write_ref = [0.0]


def sheets_read(fn, *args, retries=6, **kwargs):
    """Call a Google Sheets READ operation with cooldown and 429 retry.

    Uses the read-specific cooldown bucket (separate from writes).
    Use for: worksheet(), get_all_values(), col_values(), row_values(),
             get_all_records(), get(), acell(), cell(), etc.
    """
    return _sheets_call_with_cooldown(fn, args, kwargs, retries, _last_read_ref, _READ_COOLDOWN)


def sheets_write(fn, *args, retries=6, **kwargs):
    """Call a Google Sheets WRITE operation with cooldown and 429 retry.

    Uses the write-specific cooldown bucket (separate from reads).
    Use for: update(), append_row(), append_rows(), update_acell(),
             update_cells(), batch_update(), delete_rows(), resize(), etc.
    """
    return _sheets_call_with_cooldown(fn, args, kwargs, retries, _last_write_ref, _WRITE_COOLDOWN)


def sheets_call(fn, *args, retries=6, **kwargs):
    """Backwards-compatible wrapper — routes to sheets_read().

    Existing callers (daily_audit, populate_results, espn_schedule_fetcher, etc.)
    that already use sheets_call() will keep working. New code should prefer
    sheets_read() or sheets_write() directly.
    """
    return sheets_read(fn, *args, retries=retries, **kwargs)


# ── Schedule helper ──────────────────────────────────────────────────────────
def get_schedule_for_date(spreadsheet, sheet_name: str, target_date: str) -> List[dict]:
    """Get games from a schedule sheet for a specific date.

    Uses sheets_call() for automatic rate-limit cooldown and 429 retry.

    Args:
        spreadsheet: The gspread spreadsheet object
        sheet_name: Name of the schedule sheet
        target_date: Date in YYYY-MM-DD format

    Returns:
        List of game dicts with away_team, home_team, spread, over_under, game_time
    """
    try:
        worksheet = sheets_read(spreadsheet.worksheet, sheet_name)
        all_values = sheets_read(worksheet.get_all_values)
        if len(all_values) < 2:
            return []

        headers = all_values[0]
        games = []

        for row in all_values[1:]:
            if len(row) < 4:
                continue
            row_dict = dict(zip(headers, row))
            game_date = row_dict.get("game_date", "")

            if game_date == target_date:
                games.append(
                    {
                        "away_team": row_dict.get("away_team", ""),
                        "home_team": row_dict.get("home_team", ""),
                        "spread": row_dict.get("spread", ""),
                        "over_under": row_dict.get("over_under", ""),
                        "game_time": row_dict.get("game_time", ""),
                    }
                )

        return games
    except gspread.WorksheetNotFound:
        return []
    except Exception as e:
        print(f"Error fetching schedule from {sheet_name}: {e}")
        return []
