#!/usr/bin/env python3
"""
Telegram Stage 1 backtest tool.

Fetches real Telegram images locally, caches OCR, runs Stage 1 parsing, and
writes a human-readable results file for review and iteration.

Usage:
  python backtest_telegram.py            # fetch 5 + parse (default)
  python backtest_telegram.py --fetch 20 # fetch 20 images (overwrites cache)
  python backtest_telegram.py --parse    # parse from local cache only (free)
  python backtest_telegram.py --fresh-ocr # re-OCR even if cache exists

Cost notes:
  - Fetch:   free (Telethon, no API cost)
  - OCR:     ~$0.001/image (Haiku); skipped on re-runs if cached
  - Stage 1: ~$0.01-0.05 per run (Sonnet); always re-runs (prompt is what we iterate)
"""
import argparse
import asyncio
import io
import json
import os
import re
import sys
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from dotenv import load_dotenv
from PIL import Image

load_dotenv()

# Force UTF-8 output on Windows
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

# ── Local storage paths ──────────────────────────────────────────────────────
DATA_DIR = Path("backtest_data/telegram")
IMAGES_DIR = DATA_DIR / "images"
MESSAGES_FILE = DATA_DIR / "messages.json"
OCR_CACHE_FILE = DATA_DIR / "ocr_cache.json"
RESULTS_FILE = DATA_DIR / "results.txt"

# ── Telegram config ──────────────────────────────────────────────────────────
CHANNEL_ID_ENV = os.environ.get("TELEGRAM_CHANNEL_ID", "")
API_ID_ENV = os.environ.get("TELEGRAM_API_ID", "")
API_HASH = os.environ.get("TELEGRAM_API_HASH", "")
SESSION = os.environ.get("TELEGRAM_SESSION", "")


def _channel_id() -> int:
    raw = CHANNEL_ID_ENV.lstrip("-")
    return int(f"-100{raw}")


# ── Fetch ────────────────────────────────────────────────────────────────────
async def _fetch_async(n: int) -> list:
    from telethon import TelegramClient
    from telethon.sessions import StringSession

    client = TelegramClient(StringSession(SESSION), int(API_ID_ENV), API_HASH)
    await client.start()

    channel_id = _channel_id()
    eastern = ZoneInfo("America/New_York")
    results = []
    last_capper = "UNKNOWN"
    album_capper: dict = {}

    print(f"Fetching up to {n} images from channel {channel_id}...")

    async for msg in client.iter_messages(channel_id):
        if msg.text:
            first_line = re.sub(r"\*+", "", msg.text.strip().split("\n")[0]).strip()
            if first_line and len(first_line) <= 40:
                last_capper = first_line.upper()
                if msg.grouped_id:
                    album_capper[msg.grouped_id] = last_capper

        if not msg.photo:
            continue

        if msg.grouped_id and msg.grouped_id in album_capper:
            capper = album_capper[msg.grouped_id]
        else:
            capper = last_capper

        buf = io.BytesIO()
        await client.download_media(msg, file=buf)

        results.append({
            "msg_id": msg.id,
            "source_ref": f"telegram:{channel_id}:{msg.id}",
            "capper": capper,
            "sent_at": msg.date.astimezone(eastern).strftime("%Y-%m-%d %H:%M:%S"),
            "sent_date": msg.date.astimezone(eastern).strftime("%Y-%m-%d"),
            "raw_text": msg.text or "",
            "image_bytes_b64": __import__("base64").b64encode(buf.getvalue()).decode(),
        })
        print(f"  [{len(results)}/{n}] msg_id={msg.id} capper={capper} size={len(buf.getvalue())//1024}KB")

        if len(results) >= n:
            break

    await client.disconnect()
    # Oldest first
    return list(reversed(results))


def fetch(n: int):
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    IMAGES_DIR.mkdir(parents=True, exist_ok=True)

    messages = asyncio.run(_fetch_async(n))
    if not messages:
        print("No images fetched.")
        return

    # Save messages metadata + image bytes
    for m in messages:
        img_path = IMAGES_DIR / f"msg_{m['msg_id']}.jpg"
        raw = __import__("base64").b64decode(m["image_bytes_b64"])
        img_path.write_bytes(raw)

    # Save metadata (strip image bytes from JSON — images are on disk)
    metadata = [{k: v for k, v in m.items() if k != "image_bytes_b64"} for m in messages]
    MESSAGES_FILE.write_text(json.dumps(metadata, indent=2), encoding="utf-8")
    print(f"\nSaved {len(messages)} images to {DATA_DIR}/")


# ── OCR ──────────────────────────────────────────────────────────────────────
def run_ocr(messages: list, fresh: bool = False) -> dict:
    """OCR all images; returns {source_ref: ocr_text}. Uses cache unless fresh=True."""
    cache: dict = {}
    if OCR_CACHE_FILE.exists() and not fresh:
        cache = json.loads(OCR_CACHE_FILE.read_text(encoding="utf-8"))

    to_ocr = [m for m in messages if not cache.get(m["source_ref"], "").strip()]
    if not to_ocr:
        print(f"OCR: all {len(messages)} images cached, skipping API call.")
        return cache

    print(f"OCR: running on {len(to_ocr)} image(s) (cached: {len(messages) - len(to_ocr)})...")

    # Import OCR function from capper_analyzer
    from capper_analyzer import extract_text_from_bytes_batch, OCR_BATCH_SIZE

    items = []
    for m in to_ocr:
        img_path = IMAGES_DIR / f"msg_{m['msg_id']}.jpg"
        items.append((img_path.read_bytes(), "image/jpeg"))

    # Process in batches to avoid hitting max_tokens limits
    ocr_results = []
    for batch_start in range(0, len(items), OCR_BATCH_SIZE):
        batch = items[batch_start : batch_start + OCR_BATCH_SIZE]
        batch_num = batch_start // OCR_BATCH_SIZE + 1
        total_batches = (len(items) + OCR_BATCH_SIZE - 1) // OCR_BATCH_SIZE
        print(f"  OCR batch {batch_num}/{total_batches} ({len(batch)} images)...")
        ocr_results.extend(extract_text_from_bytes_batch(batch))

    for m, text in zip(to_ocr, ocr_results):
        cache[m["source_ref"]] = text
        print(f"  msg_{m['msg_id']}: {text[:80].replace(chr(10), ' ')}...")

    OCR_CACHE_FILE.write_text(json.dumps(cache, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"OCR cache saved to {OCR_CACHE_FILE}")
    return cache


# ── Stage 1 ──────────────────────────────────────────────────────────────────
def run_stage1(messages: list, ocr_cache: dict, debug: bool = False) -> list:
    """Run Stage 1 on all messages. Returns list of result dicts per message."""
    from capper_analyzer import (
        build_stage1_prompt, call_sonnet_text, parse_csv_response,
        TELEGRAM_SOURCE, TELEGRAM_VALID_SPORTS,
    )

    # No live schedule — abbreviation resolution will show raw pick names.
    # Flags will catch obvious abbreviations in review.
    schedule_data = {
        "nba": "No games (backtest mode)",
        "cbb": "No games (backtest mode)",
        "nhl": "No games (backtest mode)",
        "mlb": "No schedule — use team names exactly as written",
    }

    results = []
    for i, m in enumerate(messages):
        ocr_text = ocr_cache.get(m["source_ref"], "")
        if not ocr_text.strip():
            results.append({**m, "ocr_text": "", "picks": [], "flags": ["NO_OCR"]})
            continue

        # Build a single-row prompt (one image at a time for clarity)
        picks_to_parse = [(m["capper"], m["sent_date"], ocr_text, i + 1)]
        prompt = build_stage1_prompt(picks_to_parse, schedule_data, source=TELEGRAM_SOURCE)

        print(f"\n[{i+1}/{len(messages)}] Stage 1: msg_{m['msg_id']} capper={m['capper']}")
        try:
            response = call_sonnet_text(prompt)
            if debug:
                print(f"  RAW RESPONSE:\n{response}\n  ---")
            picks = parse_csv_response(response, valid_sports=TELEGRAM_VALID_SPORTS)
        except Exception as e:
            print(f"  Stage 1 failed: {e}")
            results.append({**m, "ocr_text": ocr_text, "picks": [], "flags": [f"STAGE1_ERROR: {e}"]})
            continue

        flags = _auto_flag(picks, m, ocr_text)
        print(f"  → {len(picks)} pick(s), flags: {flags or 'none'}")
        results.append({**m, "ocr_text": ocr_text, "picks": picks, "flags": flags})

    return results


_PARLAY_RE = re.compile(r'\bparlay\b', re.IGNORECASE)
_PARLAY_SLIP_RE = re.compile(r'parlay\s*\d+\s*legs?|^\s*\d+\s*leg\s*parlay', re.IGNORECASE | re.MULTILINE)
_GAME_TOTAL_RE = re.compile(r'\b(over|under|o\d|u\d)\s*\d+(\.\d+)?', re.IGNORECASE)
_PARTIAL_GAME_RE = re.compile(
    r'\b(1h|2h|1st\s*half|2nd\s*half|first\s*half|second\s*half'
    r'|5to\s*innings?|5th\s*innings?|[1-9]th\s*inn'
    r'|1st\s*quarter|2nd\s*quarter|3rd\s*quarter|4th\s*quarter|q[1-4]\b)',
    re.IGNORECASE,
)
_PLAYER_PROP_RE = re.compile(
    r'\b(rebounds?|assists?|three.pointers?|3-pointers?|blocks?|steals?'
    r'|strikeouts?|era|batting\s*avg|home\s*runs?'
    r'|yards?|touchdowns?|receptions?|tackles?'
    r'|saves?|shots?\s*on\s*goal|goals?\s*scored'
    r'|\d+\.\d+\s*(pts?|reb|ast|stl|blk))',
    re.IGNORECASE,
)
_SLIP_NO_GAME_RE = re.compile(
    r'(main\s*play|active\s*bets?|est\.\s*payout|total\s*payout|cashout)',
    re.IGNORECASE,
)
_PROMO_RE = re.compile(
    r'(price\s*:\s*\$\d|join\s*the\s*best|dm\s*@|subscribe|package|vip\s*pass'
    r'|\$\d+\.\d{2}\s*\n.*\$\d+\.\d{2})',
    re.IGNORECASE,
)
# Promo patterns detectable from the Telegram message text (raw_text), not OCR.
# These are subscription/advertisement posts — the image is a teaser, not a pick.
_RAW_TEXT_PROMO_RE = re.compile(
    r'(for\s+the\s+price\s+of'         # "2 days for the price of 1"
    r'|price\s*:\s*\$\d'               # "PRICE: $19.99"
    r'|your\s+pass\s+will\s+not\s+expire'
    r'|join\s+the\s+best\s+team'
    r'|cheapest\s+prices\s+in\s+the\s+industry)',
    re.IGNORECASE,
)
# Sportsbook advertisement patterns (welcome bonus, sign-up promotions)
_SPORTSBOOK_AD_RE = re.compile(
    r'welcome\s+bonus|sign[-\s]*up\s+here|\d{2,3}%\s+welcome',
    re.IGNORECASE,
)
# Capper name normalization: strip trailing win/loss record suffixes and leading emoji.
_CAPPER_RECORD_SUFFIX_RE = re.compile(r'\s*[-–]\s*[\u2705\u274c\u2b55\u26aa\s]+$')
_CAPPER_LEADING_EMOJI_RE = re.compile(r'^[^\x00-\x7F\w]+')
# Flag prefixes that are expected filter behavior — silently excluded from review digest.
_SILENT_FLAG_PREFIXES = ("PARLAY_SLIP", "GAME_TOTAL", "PARTIAL_GAME", "PLAYER_PROP", "PROMO_IMAGE")


def _norm_capper(s: str) -> str:
    """Normalize a capper name for comparison: strip record suffixes, leading emoji, uppercase."""
    s = _CAPPER_RECORD_SUFFIX_RE.sub('', s).strip()
    s = _CAPPER_LEADING_EMOJI_RE.sub('', s).strip()
    return s.upper()


def _classify_no_picks(ocr_text: str, raw_text: str = "") -> str:
    """Return a human-readable reason string when Stage 1 produced no picks."""
    # Subscription/promo post — detectable from the Telegram message text
    if _RAW_TEXT_PROMO_RE.search(raw_text):
        return "PROMO_IMAGE — subscription advertisement, not a pick"
    # Explicit parlay bet slip (e.g. "Parlay 2 Legs", "2 LEG PARLAY")
    if _PARLAY_SLIP_RE.search(ocr_text) or (
        _PARLAY_RE.search(ocr_text) and "Legs" in ocr_text
    ):
        return "PARLAY_SLIP"
    # Game total (Over/Under on a full game)
    if _GAME_TOTAL_RE.search(ocr_text):
        # Could still be a partial-game total — check that first
        if _PARTIAL_GAME_RE.search(ocr_text):
            return "PARTIAL_GAME — half/inning/quarter bet (not a full-game side)"
        return "GAME_TOTAL — Over/Under total bet (filter skips all O/U)"
    # Partial-game bet without an obvious total marker
    if _PARTIAL_GAME_RE.search(ocr_text):
        return "PARTIAL_GAME — half/inning/quarter bet (not a full-game side)"
    # Player prop
    if _PLAYER_PROP_RE.search(ocr_text):
        return "PLAYER_PROP — individual player statistic bet (filter skips props)"
    # Bet slip with no identifiable game
    if _SLIP_NO_GAME_RE.search(ocr_text):
        return "UNIDENTIFIABLE_SLIP — bet slip image shows odds/stake but no game name"
    # Very short / no real content
    if len(ocr_text.strip()) < 60:
        return "NO_CONTENT — image contains no pick information (notification/header)"
    # Subscription promo
    if _PROMO_RE.search(ocr_text):
        return "PROMO_IMAGE — subscription advertisement, not a pick"
    # NRFI probability table (No Run First Inning stats tool, not a betting pick)
    if re.search(r'nrfi\s+probability', ocr_text, re.IGNORECASE):
        return "PARTIAL_GAME — half/inning/quarter bet (not a full-game side)"
    # Sportsbook advertisement (welcome bonus, sign-up promotions)
    if _SPORTSBOOK_AD_RE.search(ocr_text) or _SPORTSBOOK_AD_RE.search(raw_text):
        return "PROMO_IMAGE — subscription advertisement, not a pick"
    return "NO_PICKS — Stage 1 found no valid spread/ML picks; review OCR above"


def _auto_flag(picks: list, m: dict, ocr_text: str = "") -> list:
    """Return list of review flag strings for a set of parsed picks."""
    flags = []
    if not picks:
        reason = _classify_no_picks(ocr_text, raw_text=m.get("raw_text", ""))
        flags.append(reason)
        return flags

    # Patterns that suggest an abbreviated team name
    abbrev_re = re.compile(r"^[A-Z]{2,4}$")
    valid_sports = {"NBA", "CBB", "NHL", "MLB"}
    line_re = re.compile(r"^([+-]?\d+(\.\d+)?|ML)$", re.IGNORECASE)

    for j, row in enumerate(picks):
        if len(row) < 5:
            flags.append(f"ROW{j+1}_SHORT")
            continue
        date_, capper_, sport_, pick_, line_ = row[0], row[1], row[2], row[3], row[4]

        if sport_.upper() not in valid_sports:
            flags.append(f"ROW{j+1}_UNKNOWN_SPORT:{sport_!r}")

        if abbrev_re.match(pick_.strip()):
            flags.append(f"ROW{j+1}_ABBREV_PICK:{pick_!r}")
        elif len(pick_.strip()) < 4:
            flags.append(f"ROW{j+1}_SHORT_PICK:{pick_!r}")

        if line_.strip() and not line_re.match(line_.strip()):
            flags.append(f"ROW{j+1}_ODD_LINE:{line_!r}")

        if _norm_capper(capper_.strip()) != _norm_capper(m["capper"]):
            flags.append(f"ROW{j+1}_CAPPER_MISMATCH:expected={m['capper']!r} got={capper_!r}")

    return flags


# ── Write results ─────────────────────────────────────────────────────────────
def write_results(results: list):
    lines = []
    run_ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    lines.append(f"Telegram Stage 1 Backtest — {run_ts}")
    lines.append(f"Images: {len(results)}  |  Source: telegram_cappers_free")
    lines.append("=" * 80)

    review_count = 0
    for r in results:
        sep = "=" * 80
        lines.append(sep)
        status_flags = r.get("flags", [])
        is_silent = any(f.startswith(p) for p in _SILENT_FLAG_PREFIXES for f in status_flags)
        needs_review = bool(status_flags) and not is_silent
        if needs_review:
            review_count += 1
        if is_silent:
            status = "○  SKIP"
        elif needs_review:
            status = "⚠  REVIEW"
        else:
            status = "✓  OK"

        lines.append(
            f"[{status}]  msg_{r['msg_id']}  |  {r['sent_at']}  |  CAPPER: {r['capper']}"
        )
        lines.append("")

        lines.append("--- RAW TEXT (from Telegram message) ---")
        lines.append(r.get("raw_text", "").strip() or "(none)")
        lines.append("")

        lines.append("--- OCR TEXT ---")
        lines.append(r.get("ocr_text", "").strip() or "(empty)")
        lines.append("")

        lines.append("--- STAGE 1 PICKS ---")
        picks = r.get("picks", [])
        if picks:
            lines.append("date,capper,sport,pick,line,game,spread,result")
            for row in picks:
                lines.append(",".join(row))
        else:
            lines.append("(no picks parsed)")
        lines.append("")

        if status_flags:
            lines.append("--- FLAGS ---")
            for f in status_flags:
                lines.append(f"  ⚠  {f}")
            lines.append("")

    lines.append("=" * 80)
    skip_count = sum(
        1 for r in results
        if any(f.startswith(p) for p in _SILENT_FLAG_PREFIXES for f in r.get("flags", []))
    )
    ok_count = len(results) - review_count - skip_count
    lines.append(
        f"SUMMARY: {len(results)} images | {review_count} need review"
        f" | {skip_count} expected skips (totals/parlays/props) | {ok_count} OK"
    )
    lines.append("=" * 80)

    # ── Review digest ────────────────────────────────────────────────────────
    review_items = [
        r for r in results
        if r.get("flags") and not any(
            any(f.startswith(p) for p in _SILENT_FLAG_PREFIXES) for f in r["flags"]
        )
    ]
    parlay_count = sum(
        1 for r in results
        if r.get("flags") and any(
            any(f.startswith(p) for p in _SILENT_FLAG_PREFIXES) for f in r["flags"]
        )
    )
    if review_items or parlay_count:
        lines.append("")
        lines.append("=" * 80)
        digest_note = f"REVIEW DIGEST — {len(review_items)} items needing attention"
        if parlay_count:
            digest_note += f"  (+{parlay_count} parlay slips silently skipped)"
        lines.append(digest_note)
        lines.append("=" * 80)
        for r in review_items:
            capper = r["capper"]
            sent_at = r.get("sent_at", "")
            flags = r.get("flags", [])
            reason = flags[0] if flags else "?"

            # Header: easy-to-find info
            lines.append(f"┌─ CAPPER: {capper}  │  {sent_at}")
            lines.append(f"│  REASON: {reason}")

            # Raw Telegram message text (what the capper posted as text)
            raw = r.get("raw_text", "").strip()
            raw_first = raw.split("\n")[0].strip() if raw else ""
            if raw_first and raw_first not in (capper, f"**{capper}"):
                lines.append(f"│  MSG   : {raw[:200].replace(chr(10), ' ↵ ')}")

            # OCR — show enough to understand what the image contains
            ocr = r.get("ocr_text", "").strip()
            if ocr:
                # Show up to 5 meaningful lines of OCR
                ocr_lines = [l.strip() for l in ocr.splitlines() if l.strip()]
                preview = "  ↵  ".join(ocr_lines[:6])
                if len(preview) > 300:
                    preview = preview[:300] + "…"
                lines.append(f"│  OCR   : {preview}")
            else:
                lines.append("│  OCR   : (empty)")

            lines.append("└" + "─" * 79)
            lines.append("")

    text = "\n".join(lines)
    RESULTS_FILE.write_text(text, encoding="utf-8")
    parlay_slip_count_print = sum(
        1 for r in results if any(f.startswith("PARLAY_SLIP") for f in r.get("flags", []))
    )
    ok_count_print = len(results) - review_count - parlay_slip_count_print
    print(f"\nResults written to {RESULTS_FILE}")
    print(
        f"SUMMARY: {len(results)} images | {review_count} need review"
        f" | {parlay_slip_count_print} parlay slips | {ok_count_print} OK"
    )


# ── Main ─────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="Telegram Stage 1 backtest")
    parser.add_argument("--fetch", type=int, metavar="N", help="Fetch N images from Telegram")
    parser.add_argument("--parse", action="store_true", help="Parse from local cache only")
    parser.add_argument("--fresh-ocr", action="store_true", help="Re-OCR even if cached")
    parser.add_argument("--debug", action="store_true", help="Print raw Stage 1 responses")
    args = parser.parse_args()

    # Determine whether to fetch
    do_fetch = args.fetch is not None
    do_parse = args.parse or not do_fetch  # default: parse after fetching

    if not do_fetch and not MESSAGES_FILE.exists():
        print(f"No local cache found at {MESSAGES_FILE}. Use --fetch N to download images first.")
        sys.exit(1)

    if do_fetch:
        fetch(args.fetch)

    if do_parse:
        if not MESSAGES_FILE.exists():
            print(f"No messages file at {MESSAGES_FILE}. Run with --fetch N first.")
            sys.exit(1)
        messages = json.loads(MESSAGES_FILE.read_text(encoding="utf-8"))
        print(f"\nLoaded {len(messages)} messages from cache.")
        ocr_cache = run_ocr(messages, fresh=args.fresh_ocr)
        results = run_stage1(messages, ocr_cache, debug=args.debug)
        write_results(results)


if __name__ == "__main__":
    main()
