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
| `discord_image_fetcher.py` | Every 15 min | Main script: fetch Discord images, OCR with Claude, parse picks, finalize with schedule matching, append to Google Sheets + local CSV |
| `espn_schedule_fetcher.py` | Daily 10am ET | Fetch NBA/CBB/NHL schedules from ESPN for pick validation |
| `nba_odds_poller.py` | Every 3 hours | Poll betting odds from The Odds API |
| `activity_logger.py` | N/A (imported) | Log activity to Google Sheets activity_log tab |

### Utility Scripts (`utils/`)
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
| `process_discord_images.yml` | Every 15 min | Manual |
| `espn_schedule.yml` | Daily 10am ET | Manual |
| `poll_odds.yml` | Every 3 hours | Manual |
| `deploy-pages.yml` | On push to gh-pages/ | Manual |

## 📊 Google Sheets Structure

Sheet: `1LzkU7rH3OtrJckV5oMvFHyuLAnbRn9E74FO1uyfM65k`

| Tab | Description |
|-----|-------------|
| `image_pull` | Raw Discord image URLs and OCR text |
| `parsed_picks` | Stage 1: OCR → structured picks |
| `finalized_picks` | Stage 2: Validated picks |
| `master_sheet` | All finalized picks (source of truth) |
| `nba_schedule` | ESPN NBA schedules |
| `cbb_schedule` | ESPN CBB schedules |
| `nhl_schedule` | ESPN NHL schedules |
| `nba_odds` | Historical odds data |
| `activity_log` | Script activity log |
