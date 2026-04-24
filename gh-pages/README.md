# Fade Finder - GitHub Pages Deployment

Static website for the Capper Consensus Tracker.

## Structure

```
gh-pages/
├── index.html           # React app (compiled from TSX artifact)
├── data/
│   └── master_sheet.csv # Pick data (auto-loaded on page load)
└── README.md
```

## Live Site

Once deployed: `https://<username>.github.io/line-movement/`

---

## Updating Data (CSV)

### Option 1: Script
```bash
./update_csv.sh "/path/to/Line Movement - master_sheet (XX).csv"
```

### Option 2: Manual
```bash
cp "/path/to/new_master_sheet.csv" gh-pages/data/master_sheet.csv
./deploy.sh "Update data YYYY-MM-DD"
```

---

## Updating the App (TSX Artifact)

When you have a new Claude artifact TSX file:

### Option 1: Ask Copilot
> "Update gh-pages/index.html with this new artifact" (attach the .tsx file)

Copilot will:
1. Read the new TSX
2. Convert it to standalone HTML with React CDN
3. Ensure it fetches `data/master_sheet.csv` on load
4. Update `gh-pages/index.html`

### Option 2: Script (may need manual fixes)
```bash
./update_tsx.sh /path/to/artifact.tsx
```

**Note:** The TSX→HTML conversion requires removing `import` statements, replacing `window.storage` API with `fetch()`, and adding React CDN scripts. Copilot handles this better than the shell script.

---

## Deploying Changes

```bash
./deploy.sh "Your commit message"
```

Or manually:
```bash
git add gh-pages/
git commit -m "Update"
git push
```

---

## Initial GitHub Pages Setup

1. Push to GitHub
2. Go to **Settings → Pages**
3. Source: **Deploy from a branch**
4. Branch: `main`, Folder: `/gh-pages`
5. Save

The site will be live within ~1 minute.

---

## Key Differences from Claude Artifact

| Claude Artifact | GitHub Pages Version |
|-----------------|---------------------|
| `window.storage` API | `fetch('data/master_sheet.csv')` |
| `import React...` | React loaded via CDN |
| `export default` | Direct render to `#root` |
| User uploads CSV | Auto-loads CSV on page load |

---

## Local Development

The HTML pages fetch data from Google Sheets via CORS requests, so opening them directly as `file://` URLs won't work. Start a local server instead:

```bash
cd gh-pages
python3 -m http.server 8080
```

Then open http://localhost:8080/boxscore.html (or any other page).

---

## Troubleshooting

**CSV not loading?**
- Check browser console for fetch errors
- Verify `data/master_sheet.csv` exists and has content
- Add cache buster: the app fetches with `?t=timestamp`

**App not rendering?**
- Check browser console for JS errors
- Babel compiles JSX in-browser; syntax errors will show there

**Changes not showing?**
- GitHub Pages can cache for ~1 min
- Hard refresh: Cmd+Shift+R
