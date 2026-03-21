# Telegram Integration Plan

## Goal

Add CAPPERS FREE Telegram channel as a second ingestion source alongside Discord.
Picks from both sources flow through the same OCR → Stage 1/2 → master_sheet pipeline.
A new `source` column distinguishes origin in the final output.

---

## Architecture

```
discord_fetcher.py      ← new file, extracted from capper_analyzer.py (Discord logic)
telegram_fetcher.py     ← new file (Telethon, async)
capper_analyzer.py      ← orchestrator: calls both fetchers, merges, runs existing pipeline
```

**Discord flow is untouched end-to-end.** Telegram rows enter the pipeline at the same
point Discord rows do. Everything downstream (Stage 1/2, sheets, CSV, git push) is shared.

### Key interface decisions

- **Discord**: keeps URL-based OCR (CDN URL stored in `image_pull`, backfill still works)
- **Telegram**: bytes-based OCR (image downloaded at fetch time, no public URL)
- **`image_pull` source ref**: Discord rows keep CDN URL; Telegram rows store `telegram:{channel_id}:{msg_id}`
- **`source` column**: added as the last column in `master_sheet` and `parsed_picks_new`; value is `"discord"` or `"telegram"`

---

## Files Changed

| File | Change type | Notes |
|---|---|---|
| `discord_fetcher.py` | New | Extracted from `capper_analyzer.py`, no logic change |
| `telegram_fetcher.py` | New | Telethon, async wrapped in `asyncio.run()` |
| `capper_analyzer.py` | Modified | Imports both fetchers; adds `source` tagging; `backfill_ocr` guard |
| `populate_results.py` | None | Uses header-based lookup — safe automatically |
| `daily_audit.py` | None | Uses header-based lookup — safe automatically |
| `gh-pages/index.html` | None | Reads CSV dynamically — new column is additive |
| `requirements.txt` | Modified | Add `telethon` |
| `.github/workflows/capper_analyzer.yml` | Modified | Add Telegram secrets |

---

## Steps

Each step is independently landable and verifiable before moving to the next.

---

### Step 0 — Add `source` column (schema migration, no code yet)

**Why first:** The column needs to exist in the sheet before any code tries to write to it.
Do this manually in Google Sheets + via a one-off backfill script.

**Changes:**
- In `master_sheet`: add `source` as the last column header, backfill all existing rows with `"discord"`
- In `parsed_picks_new`: same — add `source` header, backfill with `"discord"`
- In `capper_analyzer.py`: add `"source"` to `PICKS_COLUMNS` list (last position)

**Verify:** Open both sheets, confirm `source` column exists and all existing rows say `"discord"`.
Confirm the next scheduled run writes `"discord"` to new Discord picks.

---

### Step 1 — Extract `discord_fetcher.py` (pure refactor)

**Why:** Creates symmetry so both fetchers are equal citizens.
Zero behavior change — this is purely moving code.

**Functions to move out of `capper_analyzer.py`:**
- `fetch_recent_messages`
- `fetch_all_messages_since`
- `get_messages_with_images_since`
- `parse_discord_timestamp`

`capper_analyzer.py` imports them from `discord_fetcher`.

**Verify:** Run the next scheduled cycle. `image_pull` and `master_sheet` output
is identical to before. No errors in Actions logs.

---

### Step 2 — Guard `backfill_ocr` against non-URL rows

**Why:** `backfill_ocr` runs every 15 minutes and passes every source-ref from
`image_pull` into `requests.get()`. A `telegram:...` string would throw an error.
This guard must land before any Telegram rows appear in the sheet.

**Change:** In `backfill_ocr`, skip rows where the source ref does not start with `"http"`:

```python
# existing:
urls = [url for _, url in batch]

# becomes:
urls = [url for _, url in batch if url.startswith("http")]
```

**Verify:** Run a cycle, confirm no errors, Discord backfill still works.

---

### Step 3 — Add `telegram_fetcher.py` + wire into `capper_analyzer.py`

**`telegram_fetcher.py` responsibilities:**
- Connect via `StringSession` from env
- Accept a `since_timestamp` (same pattern as Discord)
- Iterate messages newest-first, stop at cutoff
- Handle album grouping: carry capper name forward across `text=None` messages
  using `msg.grouped_id` to associate images in the same album
- Extract capper name: first line of `msg.text`, strip `**` markdown, uppercase
- Download image bytes immediately via `client.download_media()`
- Return a list of dicts (same shape Discord produces) with:
  - `source_ref`: `"telegram:{channel_id}:{msg_id}"`
  - `image_bytes`: raw bytes
  - `media_type`: `"image/jpeg"` (all Telegram photos are JPEG)
  - `sent_at`: Eastern-formatted timestamp
  - `capper_name`: uppercased
  - `message_content`: raw text for context
  - `message_dt`: UTC datetime for ordering
  - `source`: `"telegram"`

**`capper_analyzer.py` changes:**
- Call `discord_fetcher.get_messages_with_images_since(last_run)` → tag each result `source="discord"`
- Call `telegram_fetcher.fetch_images_since(last_run)` → already tagged `source="telegram"`
- Merge into one list, sorted by `message_dt`
- Discord items: OCR via existing `extract_text_from_images_batch(urls)` (unchanged)
- Telegram items: OCR via `ocr_from_bytes(images)` (bytes-based, validated in scratch tests)
- Both write to `image_pull` — Discord with CDN URL, Telegram with `telegram:` ref
- Both write `source` value when appending to `parsed_picks_new` and `master_sheet`

**Verify:**
- Confirm Telegram rows appear in `image_pull` with `telegram:` source ref
- Confirm `source` column is populated correctly in `master_sheet`
- Confirm Discord rows are unaffected
- Inspect Stage 1/2 output for Telegram picks — expect iteration needed (see Step 5)

---

### Step 4 — GitHub Actions secrets + workflow update

**Add to repo secrets:**
- `TELEGRAM_SESSION`
- `TELEGRAM_API_ID`
- `TELEGRAM_API_HASH`

**Update `capper_analyzer.yml`:**
```yaml
env:
  DISCORD_USER_TOKEN: ${{ secrets.DISCORD_USER_TOKEN }}
  GOOGLE_CREDENTIALS: ${{ secrets.GOOGLE_CREDENTIALS }}
  ANTHROPIC_API_KEY: ${{ secrets.ANTHROPIC_API_KEY }}
  TELEGRAM_SESSION: ${{ secrets.TELEGRAM_SESSION }}
  TELEGRAM_API_ID: ${{ secrets.TELEGRAM_API_ID }}
  TELEGRAM_API_HASH: ${{ secrets.TELEGRAM_API_HASH }}
```

**Update `requirements.txt`:** add `telethon>=1.36.0`

**Verify:** First live CI run fetches Telegram images. Check Actions logs for errors.

---

### Step 5 — Validate and iterate (expected bugs)

This step is ongoing. Known areas likely to need adjustment:

**Capper name mismatches:**
Telegram may produce `BEEZOWINS` where Discord has `BEEZO WINS` for the same person.
Monitor `master_sheet` for duplicate capper identities and normalize as needed.

**Stage 1 prompt tuning:**
The Stage 1 prompt was written with Discord pick screenshot formats in mind.
Telegram OCR will surface different layouts (e.g. `@handle • Today 5:54 AM` headers,
emoji bullets, unit sizes like `2U`). Add Telegram-style examples to `EXAMPLE_PICKS_ROWS`
as real cases emerge.

**Multi-pick cards:**
Some Telegram images contain many picks per image. Verify Stage 1 correctly
extracts multiple rows from a single OCR block.

**Album grouping edge cases:**
If the admin posts a capper's name in one message and the images in a follow-up
message with no text, verify the `grouped_id` carry-forward logic catches it.

**False-positive images:**
Telegram channel may contain non-pick images (promo graphics, announcements).
Verify Stage 1 correctly returns no rows for these rather than hallucinating picks.

---

## What Is Explicitly Not Changed

- `backfill_ocr` logic (except the one `startswith("http")` guard)
- `extract_text_from_images_batch` — Discord uses this unchanged
- Stage 1 / Stage 2 parsing functions (until tuning in Step 5)
- Sheets write logic
- CSV sync and git push
- GitHub Actions schedule (15 min cadence)
- `populate_results.py`
- `daily_audit.py`
- Frontend (`gh-pages/index.html`)
