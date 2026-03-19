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
| `daily_audit.py` | Nightly (via capper_analyzer) | Two-pass hallucination audit of yesterday's picks. Pass 1: free Python substring check. Pass 2: Claude Opus confirmation — only fires within 15 min after midnight PST. Appends failures to `audit_data` sheet and logs Opus cost to `activity_log`. |
| `audit_hallucinations.py` | Manual / imported by daily_audit | Standalone two-pass hallucination audit for any picks sheet. Contains reusable `pick_in_ocr()`, `ABBREV_MAP`, and `opus_audit_suspects()` used by `daily_audit.py`. |

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
| `audit_data` | Confirmed hallucinations flagged by the nightly Opus audit |
| `nba_schedule` | ESPN NBA schedules |
| `cbb_schedule` | ESPN CBB schedules |
| `nhl_schedule` | ESPN NHL schedules |
| `nba_odds` | Historical odds data |
| `activity_log` | Script activity log (includes `daily_audit` Opus cost entries) |

## 🛠 TODO

- [ ] **Populate `result` column**: Build a script that looks up game outcomes and fills the `result` column (W/L/Push) in `master_sheet` and `parsed_picks_new`.

- [ ] **Audit report**: After the nightly Opus audit runs, generate a next-day manual review report (written to a `audit_report` sheet or Discord message) containing:
  - `num_rows_audit_pass` — picks that passed all 4 passes cleanly
  - `num_rows_audit_warn` — Pass 3 suspects cleared by Opus (false positives)
  - `num_rows_audit_fail` — confirmed hallucinations written to `audit_data`
  - Full details for warns and failures: date, capper, pick, line, OCR snippet, Opus verdict

- [ ] **Opus predictor**: Before picks come in each day, use Opus to analyze the day's schedule (NBA/CBB/NHL games) and identify high-value spots to watch — e.g. public fade candidates, line value based on historical capper tendencies, noteworthy matchups. Output posted somewhere visible (Discord, `predictions` sheet, or GitHub Pages) so you have context before reviewing the day's picks.
