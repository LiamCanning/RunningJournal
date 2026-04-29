#!/bin/bash
# Publish the latest dashboard + data to GitHub Pages.
# Run after a local sync so GitHub Actions always has fresh base data.
set -e

ICLOUD="/Users/liam/Library/Mobile Documents/com~apple~CloudDocs/Personal/Fitness/Running"
REPO="$HOME/RunningJournal"

cd "$REPO"
git pull --rebase origin main

cp "$ICLOUD/Dashboard/cologne-marathon-dashboard.html" "$REPO/index.html"
cp "$ICLOUD/_automation/data/strava_activity_cache.json" "$REPO/strava_activity_cache.json"

git add index.html strava_activity_cache.json
git diff --cached --quiet && echo "No changes." && exit 0
git commit -m "Update dashboard $(date '+%Y-%m-%d')"
git push origin main
echo "✅ Dashboard published to GitHub Pages"
