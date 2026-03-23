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
| `populate_stage2.py` | Fill game/spread/side by matching picks against schedule sheets |
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
| `parsed_picks_new` | Append-only mirror of master_sheet with `ocr_text` (col 10); source for daily audit |
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

Claude's only truly irreplaceable job in Stage 2 is **abbreviation resolution** — mapping OCR shorthand like "OKC", "BKN", "CBJ" to full team names. Everything else can be pure Python:

- **Game matching**: Given a full team name + date + sport, Python already does this perfectly in `populate_stage2.py` via `team_matches()`
- **Spread lookup**: Python reads it directly from the schedule sheet (already working in `populate_stage2.py` and `finalize_picks.py`)
- **Side**: Being removed entirely — it's just a copy of `pick` and nothing depends on it

The abbreviation space is finite (30 NBA + 32 NHL + ~362 CBB teams, each with a small set of common aliases). On any given day only ~15-30 teams are playing, so the search space is tiny. A maintained alias mapping in a Google Sheet can replace Claude for this task.

### Phase 1: Remove `side` column

Remove the `side` column entirely from code, Google Sheets, and CSV. It is just a copy of `pick` — the dashboard already falls back to `pick`, and `populate_results.py` doesn't use it.

- Remove `side` from all column headers/constants across every file
- Shift column indices (result moves from index 8 → 7, etc.)
- Update dashboard (`index.html`) to use `pick` directly instead of `side` with fallback
- Remove `side` from `daily_audit.py`: `check_next_day_game` updates, `side_consistency` check spec, `REQUIRED_COLUMNS`
- Remove `side` from `validate_and_fix_pick_column()`
- Remove `side` from `populate_stage2.py`
- Delete `side` column from Google Sheets and CSV

### Phase 2: Move spread to Python schedule lookup

Stop Claude from guessing `spread`. Instead, look it up from the schedule sheet via Python after Claude returns.

- In `capper_analyzer.py` `run_stage2()`: after Claude returns rows, do a Python post-pass that looks up spread from schedule (same pattern as `populate_stage2.py`)
- Update `build_stage2_prompt()`: remove spread instruction from Claude's output
- In `daily_audit.py` `check_next_day_game()`: change `new_spread = f"{matched_team} {line}"` to schedule lookup
- Update `spread_consistency` check spec: spread should match schedule, not `"{pick} {line}"`
- Simplify `format_schedule_for_prompt()` — Claude no longer needs spread data at all

### Phase 3: Backfill existing data

- Fix spread values in `master_sheet` for all non-ML rows where spread ≠ schedule spread
- Deduplicate the 18 known duplicate composite key pairs
- Validate match rate of Python resolver against all historical `parsed_picks_new` data before going live

### Phase 4: Eliminate Claude from Stage 2

Replace Claude Stage 2 entirely with Python regex + abbreviation mapping.

- Create `team_aliases` Google Sheet with columns: `abbreviation`, `full_name`, `sport`
- Pre-populate with known abbreviations from existing prompts + `_NOISE` regex in `populate_stage2.py`
- Build `resolve_team_name(pick_text, sport, date, schedules, aliases)`:
  - Try exact match against scheduled teams for that date/sport
  - Try substring match (existing `team_matches()` logic)
  - Try alias lookup from Google Sheet
  - If still no match, try other sports (existing wrong-sport detection)
  - Return `None` only if truly unresolvable
- Replace `build_stage2_prompt()` + Claude API call with this Python function
- Keep Claude Stage 1 as-is (OCR parsing genuinely needs LLM intelligence)
- For unresolvable picks: leave for manual review or fall back to lightweight Claude call
