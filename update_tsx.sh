#!/bin/bash
# Update Fade Finder from a new Claude TSX artifact
# Usage: ./update_tsx.sh /path/to/new_artifact.tsx

set -e
cd "$(dirname "$0")"

if [ -z "$1" ]; then
    echo "Usage: ./update_tsx.sh /path/to/artifact.tsx"
    echo ""
    echo "This converts a Claude TSX artifact to the GitHub Pages HTML format."
    echo "The TSX should export a default React component."
    exit 1
fi

SOURCE="$1"

if [ ! -f "$SOURCE" ]; then
    echo "❌ File not found: $SOURCE"
    exit 1
fi

# Read the TSX content (skip the import line)
TSX_CONTENT=$(grep -v "^import React" "$SOURCE" | grep -v "^export default")

# Get the component name (last export default function line)
COMPONENT_NAME=$(grep -E "^(export default )?function \w+" "$SOURCE" | tail -1 | sed -E 's/.*(function )(\w+).*/\2/')

if [ -z "$COMPONENT_NAME" ]; then
    COMPONENT_NAME="CapperConsensusTracker"
fi

echo "🔧 Converting TSX to HTML..."
echo "   Component: $COMPONENT_NAME"

# Create the HTML file
cat > gh-pages/index.html << 'HTMLHEAD'
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Fade Finder - Capper Consensus Tracker</title>
  <script crossorigin src="https://unpkg.com/react@18/umd/react.production.min.js"></script>
  <script crossorigin src="https://unpkg.com/react-dom@18/umd/react-dom.production.min.js"></script>
  <script src="https://unpkg.com/@babel/standalone/babel.min.js"></script>
  <style>
    * { box-sizing: border-box; margin: 0; padding: 0; }
    body { font-family: 'Segoe UI', system-ui, sans-serif; }
  </style>
</head>
<body>
  <div id="root"></div>
  <script type="text/babel">
const { useState, useMemo, useEffect, useCallback, memo } = React;

HTMLHEAD

# Process the TSX: 
# 1. Remove import statements
# 2. Remove "export default" 
# 3. Replace window.storage API calls with fetch for CSV
sed -E \
    -e '/^import /d' \
    -e 's/export default //' \
    -e 's/const STORAGE_KEY.*$//' \
    "$SOURCE" >> gh-pages/index.html

# Add the fetch-based loading and render
cat >> gh-pages/index.html << HTMLTAIL

// Override to fetch from CSV
const originalComponent = $COMPONENT_NAME;

const root = ReactDOM.createRoot(document.getElementById('root'));
root.render(<$COMPONENT_NAME />);
  </script>
</body>
</html>
HTMLTAIL

echo "✅ Updated gh-pages/index.html"
echo ""
echo "⚠️  NOTE: You may need to manually verify the useEffect that loads the CSV:"
echo "    It should fetch('data/master_sheet.csv') instead of using window.storage"
echo ""

read -p "Deploy to GitHub Pages now? [y/N] " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    ./deploy.sh "Update app from TSX $(date +%Y-%m-%d)"
fi
