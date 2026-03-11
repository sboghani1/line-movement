#!/bin/bash
# Deploy Fade Finder to GitHub Pages
# Usage: ./deploy.sh [commit message]

set -e
cd "$(dirname "$0")"

# Commit message
MSG="${1:-Update Fade Finder data}"

# Check if gh-pages directory exists
if [ ! -d "gh-pages" ]; then
    echo "❌ gh-pages directory not found"
    exit 1
fi

# Add and commit changes
git add gh-pages/
git commit -m "$MSG" || echo "Nothing to commit"
git push

echo ""
echo "✅ Deployed! Your site will be live at:"
echo "   https://<your-username>.github.io/line-movement/"
echo ""
echo "📝 To enable GitHub Pages:"
echo "   1. Go to Settings → Pages"
echo "   2. Source: Deploy from a branch"
echo "   3. Branch: main, folder: /gh-pages"
echo "   4. Save"
