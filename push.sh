#!/bin/bash
# Run this after a sync to publish the latest dashboard to GitHub Pages
set -e

DASHBOARD="/Users/liam/Library/Mobile Documents/com~apple~CloudDocs/Personal/Fitness/Running/Dashboard/cologne-marathon-dashboard.html"
REPO="$HOME/RunningJournal"

cp "$DASHBOARD" "$REPO/index.html"
cd "$REPO"
git add index.html
git diff --cached --quiet && echo "No changes." && exit 0
git commit -m "Update dashboard $(date '+%Y-%m-%d')"
git push origin main
echo "✅ Dashboard published to GitHub Pages"
