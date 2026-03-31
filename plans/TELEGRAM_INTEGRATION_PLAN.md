# Telegram Integration Plan

## Goal

Add CAPPERS FREE Telegram channel as a second ingestion source alongside Discord.
Picks from both sources flow through the same OCR → Stage 1/2 → master_sheet pipeline.
A new `source` column distinguishes origin in the final output.

---

## Architecture

```
discord_fetcher.py      ← extracted from capper_analyzer.py (Discord logic)
telegram_fetcher.py     ← new file (Telethon, async)
backtest_telegram.py    ← local backtest tool (fetch → OCR cache → Stage 1 → results.txt)
capper_analyzer.py      ← orchestrator: calls both fetchers, merges, runs existing pipeline
```

**Discord flow is untouched end-to-end.** Telegram rows enter the pipeline at the same
point Discord rows do. Everything downstream (Stage 1/2, sheets, CSV, git push) is shared.

### Key interface decisions

- **Discord**: keeps URL-based OCR (CDN URL stored in `image_pull`, backfill still works)
- **Telegram**: bytes-based OCR (image downloaded at fetch time, no public URL). `backfill_ocr`
  skips `telegram:` refs (guard in place); full Telethon re-download backfill is a future TODO.
- **`image_pull` source ref**: Discord rows keep CDN URL; Telegram rows store `telegram:{channel_id}:{msg_id}`
- **`source` column**: last column in `master_sheet` and `parsed_picks_new`; value is
  `"discord_all_in_one"` or `"telegram_cappers_free"` (constants `DISCORD_SOURCE` / `TELEGRAM_SOURCE`)

### Source-isolation principle

New sports and prompt features are enabled for **Telegram first**, Discord separately after its
own testing. The code is structured to make enabling a feature for Discord a one-line change:

| Concern | Discord constant | Telegram constant |
|---|---|---|
| Valid sports (prompt + CSV filter) | `DISCORD_VALID_SPORTS` | `TELEGRAM_VALID_SPORTS` |
| Sport definition line in prompt | `_DISCORD_SPORT_DEF` | `_TELEGRAM_SPORT_DEF` |
| Filtering rules in prompt | `_DISCORD_FILTER_RULES` | `_TELEGRAM_FILTER_RULES` |
| Parsing pattern notes | `_DISCORD_PARSING_PATTERNS` | `_TELEGRAM_PARSING_PATTERNS` |
| Example pick rows | `EXAMPLE_PICKS_ROWS_DISCORD` | `EXAMPLE_PICKS_ROWS_TELEGRAM` |

**Currently Telegram-only (not yet enabled for Discord):** MLB

---

## Files Changed

| File | Change type | Notes |
|---|---|---|
| `discord_fetcher.py` | New | Extracted from `capper_analyzer.py`, no logic change |
| `telegram_fetcher.py` | New | Telethon, async wrapped in `asyncio.run()` |
| `backtest_telegram.py` | New | Local backtest tool; see Backtest section below |
| `capper_analyzer.py` | Modified | See details below |
| `requirements.txt` | Modified | Added `telethon>=1.36.0` |
| `.github/workflows/capper_analyzer.yml` | TODO (Step 4) | Add Telegram secrets |
| `populate_results.py` | None | Uses header-based lookup — safe automatically |
| `daily_audit.py` | None | Uses header-based lookup — safe automatically |
| `gh-pages/index.html` | None | Reads CSV dynamically — new column is additive |

### `capper_analyzer.py` changes summary

- `DISCORD_SOURCE` / `TELEGRAM_SOURCE` string constants
- `DISCORD_VALID_SPORTS` / `TELEGRAM_VALID_SPORTS` sets
- `_DISCORD_SPORT_DEF` / `_TELEGRAM_SPORT_DEF` — sport line in prompt
- `_DISCORD_FILTER_RULES` / `_TELEGRAM_FILTER_RULES` — filtering section in prompt
- `_DISCORD_PARSING_PATTERNS` / `_TELEGRAM_PARSING_PATTERNS` — source-specific pattern notes
- `EXAMPLE_PICKS_ROWS_DISCORD` (renamed) / `EXAMPLE_PICKS_ROWS_TELEGRAM` (empty, fill in Step 5)
- `build_stage1_prompt(picks, schedule, source)` — branches on source for all of the above
- `extract_text_from_bytes_batch(items)` — OCR from raw bytes (Telegram path)
- `get_rows_needing_stage1` — returns 5-tuple including source (derived from col 3 prefix)
- `run_stage1` — groups rows by source, runs separate source-specific prompt per group;
  attaches `source` as col 9 to parsed rows so Stage 2 tags correctly
- `run_stage2` — reads source from col 9 instead of hardcoding; guards against
  manual-queue rows that put ocr_text in col 9 instead of col 8 (pre-existing discrepancy)
- `parse_csv_response(response, valid_sports)` — `valid_sports` param, defaults to
  `DISCORD_VALID_SPORTS`; packed-row splitter handles model cramming multiple rows on one line
- `_is_valid_sport(s, valid_sports)` — accepts set override
- Telegram fetch block added to `run()` after Discord block

---

## Steps

### ✅ Step 0 — Add `source` column (done)

`source` column added to `master_sheet` and `parsed_picks_new`. `PICKS_COLUMNS` updated.
All existing rows backfilled with `"discord"`.

### ✅ Step 1 — Extract `discord_fetcher.py` (done)

Pure refactor. `capper_analyzer.py` imports `get_messages_with_images_since` from it.

### ✅ Step 2 — Guard `backfill_ocr` against non-URL rows (done)

`backfill_ocr` skips rows where source ref does not start with `"http"`.
TODO comment in place for future Telethon re-download backfill.

### ✅ Step 3 — `telegram_fetcher.py` + wire into `capper_analyzer.py` (done)

All code written and locally tested via `backtest_telegram.py`.
**Not yet deployed to GitHub Actions** — waiting on Step 5 backtest sign-off.

### ✅ Step 3 addendum — Source-specific Stage 1 prompts (done)

All source isolation constants and branching in place. See source-isolation table above.

### ⬜ Step 4 — GitHub Actions secrets + workflow update

Add to repo secrets: `TELEGRAM_SESSION`, `TELEGRAM_API_ID`, `TELEGRAM_API_HASH`, `TELEGRAM_CHANNEL_ID`

Update `capper_analyzer.yml`:
```yaml
env:
  DISCORD_USER_TOKEN: ${{ secrets.DISCORD_USER_TOKEN }}
  GOOGLE_CREDENTIALS: ${{ secrets.GOOGLE_CREDENTIALS }}
  ANTHROPIC_API_KEY: ${{ secrets.ANTHROPIC_API_KEY }}
  TELEGRAM_SESSION: ${{ secrets.TELEGRAM_SESSION }}
  TELEGRAM_API_ID: ${{ secrets.TELEGRAM_API_ID }}
  TELEGRAM_API_HASH: ${{ secrets.TELEGRAM_API_HASH }}
  TELEGRAM_CHANNEL_ID: ${{ secrets.TELEGRAM_CHANNEL_ID }}
```

**Verify:** First live CI run fetches Telegram images. Check Actions logs for errors.

### 🔄 Step 5 — Backtest and iterate (in progress)

**Backtest tool:** `backtest_telegram.py`

```
python backtest_telegram.py --fetch 50   # download 50 images (one-time)
python backtest_telegram.py --parse      # re-parse from cache (free, iterate on prompt)
python backtest_telegram.py --fresh-ocr  # re-OCR if images changed
python backtest_telegram.py --debug      # print raw Stage 1 responses
```

Local cache: `backtest_data/telegram/` — images, messages.json, ocr_cache.json, results.txt

**Results from 5-image test (all correct):**
- Multi-pick images (Knicks +8.5, Hornets ML, Guardians ML from one image): ✓
- Betting slip screenshots (Pelicans +6): ✓
- Duke ML from minimal pick card: ✓
- Baseball total (over 7) correctly skipped: ✓
- Same-game parlay correctly skipped: ✓

**Bugs found and fixed during backtest:**
1. `_is_valid_sport` didn't include MLB → rows silently dropped; fixed with `TELEGRAM_VALID_SPORTS`
2. Model sometimes packs multiple CSV rows on one line → `parse_csv_response` packed-row
   splitter: finds date boundaries starting at position 5, splits cleanly
3. MLB enabled for Telegram only (not Discord) per source-isolation principle

**Known areas to watch in larger batches:**
- Capper name mismatches (e.g. `BEEZOWINS` vs `BEEZO WINS`)
- Album grouping: capper name carried forward via `grouped_id`
- False-positive images (promos, announcements) — should produce 0 picks
- Multi-pick cards with many picks per image
- `EXAMPLE_PICKS_ROWS_TELEGRAM` is empty — add real examples as patterns emerge

**Sign-off criteria before Step 4 deployment:**
- ≥ 50 images tested
- No false positives (hallucinated picks from non-pick images)
- No missed picks on clearly readable pick cards
- Capper names reasonable (fixable via capper_name_resolution sheet post-deploy)

---

## What Is Explicitly Not Changed

- `backfill_ocr` logic (except the `startswith("http")` guard)
- `extract_text_from_images_batch` — Discord OCR path, unchanged
- Stage 2 (`stage2_python.py`) — pure Python, source-agnostic
- Sheets write logic
- CSV sync and git push
- GitHub Actions schedule (15 min cadence)
- `populate_results.py`
- `daily_audit.py`
- Frontend (`gh-pages/index.html`)
