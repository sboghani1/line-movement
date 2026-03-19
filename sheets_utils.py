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

import gspread
from google.oauth2.service_account import Credentials

# ── Constants ────────────────────────────────────────────────────────────────
GOOGLE_SHEET_ID = "1LzkU7rH3OtrJckV5oMvFHyuLAnbRn9E74FO1uyfM65k"

SPORT_TO_SCHED = {
    "nba": "nba_schedule",
    "cbb": "cbb_schedule",
    "nhl": "nhl_schedule",
}


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


# ── Retry helper ─────────────────────────────────────────────────────────────
def sheets_call(fn, *args, retries=6, **kwargs):
    """Call fn(*args, **kwargs) with exponential backoff on 429 rate-limit errors."""
    delay = 15
    for attempt in range(retries):
        try:
            return fn(*args, **kwargs)
        except gspread.exceptions.APIError as e:
            if e.response.status_code == 429 and attempt < retries - 1:
                print(f"  [rate limit] waiting {delay}s before retry {attempt+2}/{retries}...")
                time.sleep(delay)
                delay = min(delay * 2, 120)
            else:
                raise
