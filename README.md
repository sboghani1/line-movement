# Line Movement - Fade Finder

Automated betting pick tracker with Discord integration, ESPN schedules, and odds polling.

## 🎯 What This Does

1. **Discord picks** → OCR/parse pick images → Google Sheets + GitHub Pages
2. **ESPN schedules** → Daily NBA/CBB/NHL game schedules for pick validation
3. **Odds polling** → Track line movement from The Odds API
4. **Static website** → Fade Finder React app showing capper consensus

## 📁 Folder Structure

### Core Scripts (GitHub Actions automation)
| File | Workflow | Description |
|------|----------|-------------|
| `capper_analyzer.py` | Every 15 min | Main script: fetch Discord images, OCR with Claude, parse picks, finalize with schedule matching, append to Google Sheets + local CSV. Also runs `daily_audit.py` at the end of each nightly invocation. |
| `espn_schedule_fetcher.py` | Daily 10am ET | Fetch NBA/CBB/NHL schedules from ESPN for pick validation |
| `nba_odds_poller.py` | Every 3 hours | Poll betting odds from The Odds API |
| `activity_logger.py` | N/A (imported) | Log activity to Google Sheets activity_log tab |
| `daily_audit.py` | Nightly (via capper_analyzer) | Check-based audit of yesterday's picks. Auto-fixes what it can, flags ambiguous cases for Opus review. Writes findings to `audit_results` sheet, upgrades any existing `needs_review` rows when a fix is applied, syncs CSV after any auto-fixes, and logs to `activity_log`. See § "Audit Results Review Process". |
| `audit_hallucinations.py` | Manual / imported by daily_audit | Standalone two-pass hallucination audit for any picks sheet. Contains reusable `pick_in_ocr()`, `ABBREV_MAP`, and `opus_audit_suspects()` used by `daily_audit.py`. |
| `remediate.py` | Manual (after audit review) | Apply a remediation file to master_sheet: delete or patch bad rows, update audit_results status, sync CSV. See § "Audit Results Review Process". |

### Backfill Scripts (one-time historical import)
| File | Description |
|------|-------------|
| `backfill_stage1.py` | Batch OCR → parsed_picks_new (Stage 1 only) |
| `backfill_orchestrate.py` | Orchestrate full Stage 1+2 backfill in batches of 20 |
| `populate_stage2.py` | Fill game/spread by matching picks against schedule sheets |
| `cleanup_invalid_rows.py` | Remove duplicates, props, parlays, totals, and wrong-sport rows |
| `finalize_picks.py` | Copy parsed_picks_new → master_sheet_new (backfill final step) |
| `reprocess_cbb_picks.py` | Re-parse CBB picks that were tagged with the wrong sport |

### Diagnostics (`diagnostics/`)
One-time inspection and repair scripts used during the historical backfill.
Kept for reference in case similar issues arise with future data.

| File | Description |
|------|-------------|
| `_diagnose_misses.py` | Categorise unmatched picks: wrong sport tag, no schedule for date, team name mismatch |
| `_inspect_invalid.py` | Inspect wrong-sport and prop rows in detail before cleanup |
| `_inspect_schedules.py` | Print schedule sheet headers and sample rows to verify format |
| `_inspect_suspects.py` | Inspect sheet structure and find suspect rows (e.g. blank game column) |
| `_merge_all_back.py` | Merge all rows after blank separators back into the sorted clean section |
| `_merge_suspects_back.py` | Merge only suspect rows (after separator) back into sorted section |
| `dump_backfill_batch.py` | Dump image_pull_new rows in batches for in-conversation parsing |
| `insert_backfill_rows.py` | Insert pre-parsed CSV rows into parsed_picks_new |
| File | Usage | Description |
|------|-------|-------------|
| `fix_historical_picks.py` | One-time | Normalize team names in historical data to ESPN format |
| `ncaa_schedule_fetcher.py` | One-time | Backfill NCAA schedules for a date range |

### Manual/Legacy Scripts
| File | Description |
|------|-------------|
| `local_picks_parser.py` | Parse pick images from local folder (manual use) |
| `discord_picks_parser.py` | DEPRECATED our fallen friend - original bot-based Discord parser |
| `fix_master_sheet.py` | One-time fix for master_sheet team names |
| `analyze_filter.py` | Analyze odds data for changes |
| `generate_filtered.py` | Generate filtered odds CSV |

### GitHub Pages (`gh-pages/`)
| File | Description |
|------|-------------|
| `index.html` | Fade Finder React app (converted from Claude TSX artifact) |
| `data/master_sheet.csv` | Pick data loaded by the app |

### Shell Scripts
| File | Description |
|------|-------------|
| `deploy.sh` | Git add/commit/push gh-pages changes |
| `update_csv.sh` | Copy new CSV to gh-pages/data/ |
| `update_tsx.sh` | Convert Claude TSX artifact to index.html |

## 🔧 Environment Variables

Required secrets in GitHub Actions:
- `DISCORD_USER_TOKEN` - Discord user token for image fetching
- `GOOGLE_CREDENTIALS` - Base64-encoded service account JSON
- `ANTHROPIC_API_KEY` - Claude API key for OCR/parsing
- `ODDS_API_KEY` - The Odds API key (optional)

## 🚀 GitHub Actions Workflows

| Workflow | Schedule | Trigger |
|----------|----------|---------|
| `capper_analyzer.yml` | Every 15 min | Manual |
| `espn_schedule.yml` | Daily 10am ET | Manual |
| `poll_odds.yml` | Every 3 hours | Manual |
| `deploy-pages.yml` | On push to gh-pages/ | Manual |

## 📊 Google Sheets Structure

Sheet: `1LzkU7rH3OtrJckV5oMvFHyuLAnbRn9E74FO1uyfM65k`

| Tab | Description |
|-----|-------------|
| `image_pull` | Raw Discord image URLs and OCR text |
| `parsed_picks` | Stage 1: OCR → structured picks (cleared after Stage 2) |
| `finalized_picks` | Stage 2: Validated picks (staging area) |
| `master_sheet` | All finalized picks — permanent history, no `ocr_text` |
| `parsed_picks_new` | Append-only mirror of master_sheet with `ocr_text` (col 9); source for daily audit |
| `audit_results` | Audit findings from nightly checks — status dropdown tracks review workflow |
| `nba_schedule` | ESPN NBA schedules |
| `cbb_schedule` | ESPN CBB schedules |
| `nhl_schedule` | ESPN NHL schedules |
| `nba_odds` | Historical odds data |
| `activity_log` | Script activity log (includes `daily_audit` Opus cost entries) |

## 🩺 Audit Results Review Process

When rows appear in `audit_results`, group by pattern using `check_failed` + `details`, then write a new check in `daily_audit.py`:

```bash
python daily_audit.py --dry-run --date YYYY-MM-DD   # verify against past affected dates
python daily_audit.py --date YYYY-MM-DD              # apply — fixes master_sheet, upgrades
                                                     # needs_review rows to auto_fixed, syncs CSV
```

### Fix log

| Date | Issue | PR |
|------|-------|----|
| 2026-03-21 | Advance pick dates — cappers post picks the night before; Stage 2 searched wrong date's schedule, leaving `game` empty | #14 |

---

## 🛠 TODO

- [ ] **Populate `result` column**: Build a script that looks up game outcomes and fills the `result` column (W/L/Push) in `master_sheet` and `parsed_picks_new`.

- [ ] **Audit report**: After the nightly Opus audit runs, generate a next-day manual review report (written to a `audit_report` sheet or Discord message) containing:
  - `num_rows_audit_pass` — picks that passed all 4 passes cleanly
  - `num_rows_audit_warn` — Pass 3 suspects cleared by Opus (false positives)
  - `num_rows_audit_fail` — confirmed hallucinations written to `audit_results`
  - Full details for warns and failures: date, capper, pick, line, OCR snippet, Opus verdict

- [ ] **Opus predictor**: Before picks come in each day, use Opus to analyze the day's schedule (NBA/CBB/NHL games) and identify high-value spots to watch — e.g. public fade candidates, line value based on historical capper tendencies, noteworthy matchups. Output posted somewhere visible (Discord, `predictions` sheet, or GitHub Pages) so you have context before reviewing the day's picks.

---

## 🔬 Refactoring Plan: Fix Spread & Shorten Prompts

### The Big Idea

Claude Stage 1 (OCR parsing) genuinely needs LLM intelligence — it reads messy screenshot text. But Stage 2 does nothing that requires an LLM:

- **Abbreviation resolution**: Finite set of team aliases → static lookup table
- **Capper normalization**: String matching against a known list
- **Game matching**: Schedule lookup by team name + date (already working in `populate_stage2.py`)
- **Spread lookup**: Python reads it directly from the schedule sheet
- **Side**: Removed entirely — it was just a copy of `pick`

The plan: keep Claude for Stage 1 OCR, replace Stage 2 entirely with Python.

### Phase 1: Remove `side` column ✅ Done

Remove the `side` column entirely from code, Google Sheets, and CSV. It is just a copy of `pick` — the dashboard already falls back to `pick`, and `populate_results.py` doesn't use it.

- Remove `side` from all column headers/constants across every file
- Shift column indices (result moves from index 8 → 7, etc.)
- Update dashboard (`index.html`) to use `pick` directly instead of `side` with fallback
- Remove `side` from `daily_audit.py`: `check_next_day_game` updates, `side_consistency` check spec, `REQUIRED_COLUMNS`
- Remove `side` from `validate_and_fix_pick_column()`
- Remove `side` from `populate_stage2.py`
- Delete `side` column from Google Sheets and CSV

### Phase 2: Move spread to Python schedule lookup ✅ Done

Spread is now filled by Python from ESPN schedule sheets, not Claude. Stage 2 prompt reduced to only send `capper,sport,pick,line` (4 cols) and receive `capper,pick,game` (3 cols) — Python handles all passthrough columns and spread lookup.

- `capper_analyzer.py`: Added `lookup_spread_from_schedule()`, `parse_stage2_response()`, `assemble_finalized_rows()`
- Both Stage 2 call sites (main flow + manual queue) use the reduced-format pattern
- `daily_audit.py`: `spread_consistency` check uses schedule spread instead of `"{pick} {line}"`
- Example constants split into `STAGE2_EXAMPLE_INPUT` / `STAGE2_EXAMPLE_OUTPUT`

### Phase 3: Backfill existing data

- [x] Fix spread values in `master_sheet` for all rows where spread ≠ schedule spread (3,472 fixed, 9,798 already correct, 30 no schedule match)
- [ ] Deduplicate the 18 known duplicate composite key pairs
- [ ] Validate match rate of Python resolver against all historical `parsed_picks_new` data before going live

### Phase 4: Eliminate Claude from Stage 2

Claude Stage 2 currently does 3 things — all replaceable with Python:

1. **Abbreviation resolution** (e.g. "BKN" → "Brooklyn Nets") — Same Sonnet model already ran in Stage 1 with the same schedule data. If it didn't resolve there, running it again won't help. A static alias map is more reliable.
2. **Capper normalization** (e.g. "beezo wins" → "BEEZO WINS") — Pure string matching against a known list. No AI needed.
3. **Game column** (e.g. "Brooklyn Nets" + date → "Los Angeles Lakers @ Brooklyn Nets") — Schedule lookup by team name + date. Already proven in `populate_stage2.py` via `find_game()`.

#### Implementation plan

**A. Team alias resolution (replaces Claude abbreviation handling)**
- Create `team_aliases` Google Sheet tab with columns: `abbreviation`, `full_name`, `sport`
- Pre-populate from existing prompt examples + `ABBREV_MAP` in `audit_hallucinations.py`
- Build `resolve_team_name(pick, sport, date, schedule, aliases)`:
  1. Exact match against scheduled teams for that date/sport
  2. Substring match (existing `team_matches()` logic from `populate_stage2.py`)
  3. Alias lookup from sheet
  4. Try other sports (existing wrong-sport detection)
  5. `None` if truly unresolvable (flag for manual review)

**B. Capper normalization (replaces Claude capper matching)**
- Use existing `known_cappers` sheet/list already loaded by `get_known_cappers()`
- Build `normalize_capper(raw_name, known_cappers)`:
  - Case-insensitive exact match
  - Fuzzy match (Levenshtein or substring)
  - If no match, keep original (properly capitalized) and log for review

**C. Game column lookup (replaces Claude schedule matching)**
- Reuse `find_game()` logic from `populate_stage2.py`
- Given resolved team name + date + sport → look up `away @ home` from schedule sheet
- Already handles wrong-sport fallback via `find_game_any_sport()`

**D. Wire it together**
- New `run_stage2_python()` function replacing `build_stage2_prompt()` + Claude API call:
  - For each parsed pick row: resolve abbreviation → normalize capper → find game → lookup spread
  - No API call, no token cost, no latency, no hallucination risk
- Keep Claude Stage 1 as-is (OCR parsing genuinely needs LLM intelligence)
- Picks that fail resolution go to a `needs_review` queue instead of a Claude fallback
- Update both Stage 2 call sites (main flow + manual queue) and `daily_audit.py`
