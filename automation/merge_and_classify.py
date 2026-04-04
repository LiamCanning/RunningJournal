#!/usr/bin/env python3
"""
Orchestrator: merge data sources and re-classify runs.

1. Check for new Garmin CSV imports
2. Convert Strava activity cache to a CSV that classify_runs.py can read
3. Run classify_runs.py
4. If classified_runs.csv changed, update the dashboard HTML
"""

import csv
import hashlib
import json
import os
import subprocess
import sys
import tempfile
import logging
from datetime import datetime

sys.path.insert(0, os.path.dirname(__file__))
import config

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(os.path.join(config.LOG_DIR, "classify.log")),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger(__name__)


def file_hash(filepath):
    """Compute MD5 hash of a file."""
    if not os.path.exists(filepath):
        return None
    h = hashlib.md5()
    with open(filepath, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def check_garmin_imports():
    """Run the Garmin folder monitor to pick up new files."""
    log.info("Checking for new Garmin imports...")
    try:
        from monitor_garmin_folder import process_new_files
        count = process_new_files()
        log.info("Garmin monitor: %d new file(s) processed.", count)
        return count
    except Exception as e:
        log.error("Garmin monitor failed: %s", e)
        return 0


def generate_strava_csv():
    """
    Convert the Strava activity cache into a CSV file compatible with
    classify_runs.py's expected Strava export format.

    classify_runs.py expects columns:
      Activity Type, Activity Date, Activity Name, Activity Description, Distance, ...
    """
    if not os.path.exists(config.STRAVA_CACHE_FILE):
        log.info("No Strava cache file found. Skipping Strava CSV generation.")
        return None

    try:
        with open(config.STRAVA_CACHE_FILE, "r") as f:
            cache = json.load(f)
    except (json.JSONDecodeError, OSError) as e:
        log.error("Could not read Strava cache: %s", e)
        return None

    if not cache:
        log.info("Strava cache is empty.")
        return None

    # Build a CSV in the Strava bulk-export format that classify_runs.py reads
    strava_csv_path = os.path.join(config.AUTOMATION_DIR, "data", "strava_api_activities.csv")

    header = [
        "Activity ID", "Activity Date", "Activity Name", "Activity Type",
        "Activity Description", "Elapsed Time", "Distance",
        "Max Heart Rate", "Average Heart Rate", "Average Speed",
        "Workout Type",
    ]

    rows = []
    for aid, act in cache.items():
        # classify_runs.py parses Activity Date as "Mon DD, YYYY, HH:MM:SS AM/PM"
        # Convert ISO date to that format
        date_str = act.get("start_date_local", act.get("start_date", ""))
        try:
            dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
            formatted_date = dt.strftime("%b %d, %Y, %I:%M:%S %p")
        except (ValueError, AttributeError):
            formatted_date = date_str

        rows.append([
            act.get("id", ""),
            formatted_date,
            act.get("name", ""),
            act.get("type", "Run"),
            act.get("description", ""),
            act.get("elapsed_time", ""),
            act.get("distance_m", 0),  # classify_runs.py divides by 1000
            act.get("max_heartrate", ""),
            act.get("average_heartrate", ""),
            act.get("average_speed", ""),
            act.get("workout_type", ""),
        ])

    with open(strava_csv_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(rows)

    log.info("Generated Strava CSV with %d activities: %s", len(rows), strava_csv_path)
    return strava_csv_path


def run_classify():
    """
    Run classify_runs.py via subprocess.
    Returns True if it completed successfully.
    """
    if not os.path.exists(config.CLASSIFY_SCRIPT):
        log.error("classify_runs.py not found at: %s", config.CLASSIFY_SCRIPT)
        return False

    log.info("Running classify_runs.py ...")
    try:
        result = subprocess.run(
            [sys.executable, config.CLASSIFY_SCRIPT],
            capture_output=True,
            text=True,
            timeout=120,
            cwd=config.DASHBOARD_DIR,
        )
        if result.returncode != 0:
            log.error("classify_runs.py failed (exit %d):\nstdout: %s\nstderr: %s",
                      result.returncode, result.stdout[-500:], result.stderr[-500:])
            return False
        log.info("classify_runs.py completed successfully.")
        if result.stdout.strip():
            log.info("classify output: %s", result.stdout.strip()[:300])
        return True
    except subprocess.TimeoutExpired:
        log.error("classify_runs.py timed out after 120s.")
        return False
    except Exception as e:
        log.error("Failed to run classify_runs.py: %s", e)
        return False


def run_dashboard_update():
    """Run update_dashboard.py to inject CSV into HTML."""
    update_script = os.path.join(os.path.dirname(__file__), "update_dashboard.py")
    if not os.path.exists(update_script):
        log.error("update_dashboard.py not found at: %s", update_script)
        return False

    try:
        result = subprocess.run(
            [sys.executable, update_script],
            capture_output=True,
            text=True,
            timeout=60,
            cwd=config.AUTOMATION_DIR,
        )
        if result.returncode != 0:
            log.error("update_dashboard.py failed (exit %d):\nstdout: %s\nstderr: %s",
                      result.returncode, result.stdout[-500:], result.stderr[-500:])
            return False
        log.info("Dashboard updated successfully.")
        return True
    except Exception as e:
        log.error("Failed to run update_dashboard.py: %s", e)
        return False


def main():
    log.info("=" * 60)
    log.info("Starting merge-and-classify pipeline...")

    # Step 1: Check for new Garmin imports
    check_garmin_imports()

    # Step 2: Generate Strava activities CSV from cache
    generate_strava_csv()

    # Step 3: Hash the current classified CSV before running
    hash_before = file_hash(config.CLASSIFIED_CSV)

    # Step 4: Run the classifier
    if not run_classify():
        log.error("Classification failed. Pipeline aborted.")
        return

    # Step 5: Always update dashboard (CSV may be unchanged but LAPS_DATA may have new data)
    hash_after = file_hash(config.CLASSIFIED_CSV)
    if hash_before == hash_after:
        log.info("classified_runs.csv unchanged. Updating dashboard for LAPS_DATA.")
    else:
        log.info("classified_runs.csv changed (before=%s, after=%s). Updating dashboard...",
                 hash_before[:8] if hash_before else "none",
                 hash_after[:8] if hash_after else "none")

    # Step 6: Update the dashboard HTML
    run_dashboard_update()

    log.info("Pipeline complete.")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        log.exception("Unhandled error in merge_and_classify: %s", e)
