#!/usr/bin/env python3
"""
Fetch new Strava activities since last sync.

Runs as an hourly poller. Loads the stored OAuth token (refreshing if
expired), fetches activities after the last sync timestamp, filters for
runs, and updates the local cache.
"""

import json
import os
import sys
import time
import logging
from datetime import datetime

sys.path.insert(0, os.path.dirname(__file__))
import config
from strava_auth import load_token, refresh_token_if_needed, save_token

try:
    import requests
except ImportError:
    print("ERROR: 'requests' package is required. Install with: pip3 install requests")
    sys.exit(1)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(os.path.join(config.LOG_DIR, "strava_poll.log")),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger(__name__)

STRAVA_API_BASE = "https://www.strava.com/api/v3"


def get_last_sync_timestamp():
    """Read the epoch timestamp of the last successful sync."""
    if os.path.exists(config.LAST_SYNC_FILE):
        try:
            with open(config.LAST_SYNC_FILE, "r") as f:
                return int(f.read().strip())
        except (ValueError, OSError) as e:
            log.warning("Could not read last sync timestamp: %s", e)
    return 0


def set_last_sync_timestamp(ts):
    """Write the epoch timestamp of the last successful sync."""
    with open(config.LAST_SYNC_FILE, "w") as f:
        f.write(str(int(ts)))


def load_cache():
    """Load the activity cache (dict keyed by activity ID string)."""
    if os.path.exists(config.STRAVA_CACHE_FILE):
        try:
            with open(config.STRAVA_CACHE_FILE, "r") as f:
                return json.load(f)
        except (json.JSONDecodeError, OSError) as e:
            log.warning("Could not load cache, starting fresh: %s", e)
    return {}


def save_cache(cache):
    """Write the activity cache to disk."""
    with open(config.STRAVA_CACHE_FILE, "w") as f:
        json.dump(cache, f, indent=2, ensure_ascii=False)


def append_sync_log(entry):
    """Append a line to the sync JSONL log."""
    with open(config.SYNC_LOG_FILE, "a") as f:
        f.write(json.dumps(entry, ensure_ascii=False) + "\n")


def extract_activity_fields(activity):
    """Extract and normalize the fields we care about from a Strava activity."""
    distance_m = activity.get("distance", 0)
    distance_km = round(distance_m / 1000.0, 2)
    moving_time = activity.get("moving_time", 0)
    elapsed_time = activity.get("elapsed_time", 0)

    return {
        "id": activity["id"],
        "name": activity.get("name", ""),
        "description": activity.get("description", ""),
        "distance_km": distance_km,
        "distance_m": distance_m,
        "moving_time": moving_time,
        "elapsed_time": elapsed_time,
        "start_date": activity.get("start_date", ""),
        "start_date_local": activity.get("start_date_local", ""),
        "type": activity.get("type", ""),
        "sport_type": activity.get("sport_type", ""),
        "average_heartrate": activity.get("average_heartrate"),
        "max_heartrate": activity.get("max_heartrate"),
        "average_speed": activity.get("average_speed"),
        "workout_type": activity.get("workout_type"),
        "fetched_at": datetime.now(tz=__import__('datetime').timezone.utc).isoformat(),
    }


def fetch_laps(access_token, activity_id):
    """Fetch lap data for a single activity. Returns list of lap dicts or None on error."""
    headers = {"Authorization": f"Bearer {access_token}"}
    url = f"{STRAVA_API_BASE}/activities/{activity_id}/laps"
    try:
        resp = requests.get(url, headers=headers, timeout=30)
        if resp.status_code == 200:
            return [
                {
                    "n": lap.get("lap_index", i + 1),
                    "dist_m": round(lap.get("distance", 0), 1),
                    "moving_time": lap.get("moving_time", 0),
                    "avg_speed": lap.get("average_speed"),
                    "avg_hr": lap.get("average_heartrate"),
                    "avg_cad": lap.get("average_cadence"),
                    "elevation_gain": lap.get("total_elevation_gain"),
                }
                for i, lap in enumerate(resp.json())
            ]
        elif resp.status_code == 429:
            log.warning("Rate limited fetching laps for activity %s", activity_id)
        else:
            log.warning("Laps fetch failed for %s: HTTP %s", activity_id, resp.status_code)
    except requests.RequestException as e:
        log.warning("Laps fetch error for %s: %s", activity_id, e)
    return None


def fetch_activities(access_token, after_timestamp):
    """
    Fetch activities from the Strava API.
    Returns a list of raw activity dicts.
    """
    headers = {"Authorization": f"Bearer {access_token}"}
    params = {"per_page": 30}
    if after_timestamp > 0:
        params["after"] = after_timestamp

    url = f"{STRAVA_API_BASE}/athlete/activities"
    log.info("Fetching activities: after=%s per_page=%s", after_timestamp, params["per_page"])

    resp = requests.get(url, headers=headers, params=params, timeout=30)

    if resp.status_code == 401:
        log.error("Strava API returned 401 Unauthorized. Token may be invalid.")
        return None
    if resp.status_code == 429:
        log.warning("Strava API rate limited (429). Will retry next cycle.")
        return None

    resp.raise_for_status()
    activities = resp.json()
    log.info("Received %d activities from Strava API.", len(activities))
    return activities


def main():
    log.info("=" * 60)
    log.info("Starting Strava activity fetch...")

    # Load and refresh token
    token_data = refresh_token_if_needed()
    if token_data is None:
        log.error("No valid token available. Run strava_auth.py first.")
        return

    access_token = token_data["access_token"]
    after_ts = get_last_sync_timestamp()
    log.info("Last sync timestamp: %s (%s)",
             after_ts,
             datetime.fromtimestamp(after_ts, tz=__import__('datetime').timezone.utc).isoformat() if after_ts > 0 else "never")

    try:
        activities = fetch_activities(access_token, after_ts)
    except requests.RequestException as e:
        log.error("API request failed: %s", e)
        append_sync_log({
            "timestamp": datetime.now(tz=__import__('datetime').timezone.utc).isoformat(),
            "status": "error",
            "error": str(e),
        })
        return

    if activities is None:
        return

    # Filter for runs only
    runs = [a for a in activities if a.get("type") == "Run"]
    log.info("Found %d runs out of %d total activities.", len(runs), len(activities))

    # Update cache (fetch laps for new activities)
    cache = load_cache()
    new_count = 0
    updated_count = 0
    for activity in runs:
        aid = str(activity["id"])
        fields = extract_activity_fields(activity)
        is_new = aid not in cache
        if is_new:
            new_count += 1
        else:
            updated_count += 1
        # Preserve existing laps if already fetched; fetch for new activities
        existing_laps = cache.get(aid, {}).get("laps")
        if existing_laps is not None:
            fields["laps"] = existing_laps
        elif is_new:
            laps = fetch_laps(access_token, activity["id"])
            if laps is not None:
                fields["laps"] = laps
                log.info("  Fetched %d laps for activity %s", len(laps), aid)
        cache[aid] = fields

    # Backfill laps for cached activities that don't have them yet
    backfill_count = 0
    for aid, act in cache.items():
        if "laps" not in act:
            laps = fetch_laps(access_token, int(aid))
            if laps is not None:
                act["laps"] = laps
                backfill_count += 1
            time.sleep(0.25)  # gentle rate limiting: ~4 req/sec
    if backfill_count:
        log.info("Backfilled laps for %d cached activities.", backfill_count)

    save_cache(cache)
    log.info("Cache updated: %d new, %d updated, %d total.", new_count, updated_count, len(cache))

    # Update last sync timestamp to now
    now_ts = int(time.time())
    set_last_sync_timestamp(now_ts)

    # Append to sync log
    append_sync_log({
        "timestamp": datetime.now(tz=__import__('datetime').timezone.utc).isoformat(),
        "status": "ok",
        "activities_fetched": len(activities),
        "runs_found": len(runs),
        "new_cached": new_count,
        "updated_cached": updated_count,
        "total_cached": len(cache),
    })

    log.info("Fetch complete. Next sync after timestamp: %s", now_ts)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        log.exception("Unhandled error in fetch_strava_activities: %s", e)
