#!/bin/bash
# Manual sync trigger — fetches Strava activities and runs the full pipeline.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
AUTOMATION_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

echo "🔄 Running manual sync..."
echo "   Automation dir: $AUTOMATION_DIR"
echo ""

echo "── Step 1: Fetch Strava activities ──"
python3 "$SCRIPT_DIR/fetch_strava_activities.py"
echo ""

echo "── Step 2: Merge and classify ──"
python3 "$SCRIPT_DIR/merge_and_classify.py"
echo ""

echo "✅ Sync complete"
