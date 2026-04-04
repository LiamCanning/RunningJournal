#!/usr/bin/env python3
"""
Update the dashboard HTML by injecting fresh CSV data.

Reads classified_runs.csv and injects it into the
`var CSV_DATA = \`...\`;` block in the dashboard HTML file.
"""

import json
import os
import re
import sys
import logging

sys.path.insert(0, os.path.dirname(__file__))
import config

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(os.path.join(config.LOG_DIR, "dashboard_update.log")),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger(__name__)

# Regex to match the CSV_DATA block:
#   var CSV_DATA = `...`;
# The backtick content can span many lines.
CSV_DATA_PATTERN = re.compile(
    r'(var\s+CSV_DATA\s*=\s*`)(.*?)(`;)',
    re.DOTALL,
)
LAPS_DATA_PATTERN = re.compile(
    r'(var\s+LAPS_DATA\s*=\s*)(.*?)(;)',
    re.DOTALL,
)


def build_laps_data():
    """
    Read the Strava cache and build a LAPS_DATA dict keyed by date string (YYYY-MM-DD).
    Each entry is a list of {dist_km, laps} objects (multiple runs possible per day).
    Lap pace is computed from avg_speed (m/s).
    """
    if not os.path.exists(config.STRAVA_CACHE_FILE):
        return {}

    with open(config.STRAVA_CACHE_FILE, "r", encoding="utf-8") as f:
        cache = json.load(f)

    result = {}
    for act in cache.values():
        raw_laps = act.get("laps")
        if not raw_laps:
            continue
        date_str = (act.get("start_date_local") or act.get("start_date") or "")[:10]
        if not date_str:
            continue
        dist_km = act.get("distance_km", 0)

        processed_laps = []
        for lap in raw_laps:
            speed = lap.get("avg_speed")  # m/s
            if speed and speed > 0:
                pace_secs = round(1000.0 / speed)
                pace_str = f"{pace_secs // 60}:{pace_secs % 60:02d}"
            else:
                pace_str = None
            processed_laps.append({
                "n": lap.get("n", 0),
                "dist": round(lap.get("dist_m", 0) / 1000.0, 2),
                "time": lap.get("moving_time", 0),
                "pace": pace_str,
                "hr": lap.get("avg_hr"),
                "cad": lap.get("avg_cad"),
                "elev": lap.get("elevation_gain"),
            })

        entry = {"dist_km": dist_km, "laps": processed_laps}
        result.setdefault(date_str, []).append(entry)

    return result


def main():
    log.info("=" * 60)
    log.info("Updating dashboard HTML...")

    # Read the classified CSV
    if not os.path.exists(config.CLASSIFIED_CSV):
        log.error("classified_runs.csv not found: %s", config.CLASSIFIED_CSV)
        return False

    with open(config.CLASSIFIED_CSV, "r", encoding="utf-8") as f:
        csv_content = f.read()

    if not csv_content.strip():
        log.error("classified_runs.csv is empty.")
        return False

    log.info("Read %d bytes from classified_runs.csv (%d lines).",
             len(csv_content), csv_content.count("\n"))

    # Read the dashboard HTML
    if not os.path.exists(config.DASHBOARD_HTML):
        log.error("Dashboard HTML not found: %s", config.DASHBOARD_HTML)
        return False

    with open(config.DASHBOARD_HTML, "r", encoding="utf-8") as f:
        html = f.read()

    # Find and replace the CSV_DATA block
    match = CSV_DATA_PATTERN.search(html)
    if not match:
        log.error("Could not find 'var CSV_DATA = `...`;' block in dashboard HTML.")
        return False

    old_csv = match.group(2)
    log.info("Found CSV_DATA block: %d bytes of existing CSV data.", len(old_csv))

    # Ensure the CSV content has a leading newline for readability
    if not csv_content.startswith("\n"):
        csv_content = "\n" + csv_content
    if not csv_content.endswith("\n"):
        csv_content = csv_content + "\n"

    new_html = html[:match.start()] + match.group(1) + csv_content + match.group(3) + html[match.end():]

    # Inject LAPS_DATA
    laps_data = build_laps_data()
    laps_json = json.dumps(laps_data, ensure_ascii=False, separators=(',', ':'))
    laps_match = LAPS_DATA_PATTERN.search(new_html)
    if laps_match:
        new_html = (new_html[:laps_match.start()] +
                    laps_match.group(1) + laps_json + laps_match.group(3) +
                    new_html[laps_match.end():])
        log.info("LAPS_DATA injected: %d date entries.", len(laps_data))
    else:
        log.warning("Could not find 'var LAPS_DATA = ...;' placeholder in dashboard HTML.")

    # Write the updated HTML
    with open(config.DASHBOARD_HTML, "w", encoding="utf-8") as f:
        f.write(new_html)

    log.info("Dashboard HTML updated successfully: %s", config.DASHBOARD_HTML)
    return True


if __name__ == "__main__":
    try:
        success = main()
        sys.exit(0 if success else 1)
    except Exception as e:
        log.exception("Unhandled error in update_dashboard: %s", e)
        sys.exit(1)
