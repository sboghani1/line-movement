#!/bin/bash
# Update the CSV data for Fade Finder
# Usage: ./update_csv.sh /path/to/new_master_sheet.csv

set -e
cd "$(dirname "$0")"

if [ -z "$1" ]; then
    echo "Usage: ./update_csv.sh /path/to/new_master_sheet.csv"
    echo ""
    echo "This will copy the CSV to gh-pages/data/master_sheet.csv"
    echo "and optionally deploy it to GitHub Pages."
    exit 1
fi

SOURCE="$1"

if [ ! -f "$SOURCE" ]; then
    echo "❌ File not found: $SOURCE"
    exit 1
fi

# Copy the CSV
cp "$SOURCE" gh-pages/data/master_sheet.csv
echo "✅ Updated gh-pages/data/master_sheet.csv"

# Show stats
LINES=$(wc -l < gh-pages/data/master_sheet.csv)
echo "   📊 $LINES lines"

# Ask to deploy
read -p "Deploy to GitHub Pages now? [y/N] " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    ./deploy.sh "Update CSV data $(date +%Y-%m-%d)"
fi
