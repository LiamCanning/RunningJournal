#!/usr/bin/env python3
"""
GitHub Actions sync — runs in the cloud every hour.
Fetches new runs from intervals.icu (fed by Garmin's official partner push),
classifies them, appends to classified_runs.csv, rebuilds index.html.
No Garmin CSV or iCloud dependency.
"""

import csv
import json
import os
import re
import sys
from datetime import datetime, timedelta, timezone

import time

import requests

# ── Credentials from GitHub secrets ──────────────────────────────────────────
# intervals.icu: static API key over HTTP Basic auth (username literally
# 'API_KEY'). No OAuth, no token refresh, no rotation - nothing to expire.
ATHLETE_ID = os.environ['INTERVALS_ATHLETE_ID']
API_KEY    = os.environ['INTERVALS_API_KEY']
API_BASE   = 'https://intervals.icu/api/v1'
AUTH       = ('API_KEY', API_KEY)
UA         = 'RunningJournal-intervals-sync/1.0 (+https://github.com/LiamCanning/RunningJournal)'

# ── Repo-relative paths ───────────────────────────────────────────────────────
REPO_DIR       = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CACHE_JSON     = os.path.join(REPO_DIR, 'strava_activity_cache.json')
CLASSIFIED_CSV = os.path.join(REPO_DIR, 'classified_runs.csv')
DASHBOARD_HTML = os.path.join(REPO_DIR, 'index.html')

# ── 1. HTTP layer ─────────────────────────────────────────────────────────────
def _get_with_retry(url, params=None, retries=3, backoff=15):
    """GET from intervals.icu with retry on 5xx. Exits gracefully on rate limit."""
    last_429 = None
    for attempt in range(retries):
        try:
            resp = requests.get(url, auth=AUTH, headers={'User-Agent': UA}, params=params, timeout=30)
            if resp.status_code == 429:
                last_429 = resp
                print(f"[rate-limit] 429 (attempt {attempt+1}/{retries})")
                if attempt < retries - 1:
                    wait = 15 * (2 ** attempt)
                    print(f"[rate-limit] Waiting {wait}s...")
                    time.sleep(wait)
                continue
            if resp.status_code in (401, 403):
                # Auth errors on a static API key never clear on their own
                # (revoked/regenerated key). Fail loudly -> workflow fails ->
                # Slack alert. Never exit 0 looking healthy while syncing nothing.
                print(f"[forbidden] intervals.icu {resp.status_code} (needs manual fix, "
                      f"not transient): {resp.text[:300]}", file=sys.stderr)
                resp.raise_for_status()
            if resp.status_code < 500:
                return resp
            print(f"[retry] intervals.icu GET {resp.status_code} (attempt {attempt+1}/{retries})", file=sys.stderr)
        except requests.exceptions.RequestException as e:
            print(f"[retry] network error: {e} (attempt {attempt+1}/{retries})", file=sys.stderr)
            if attempt == retries - 1:
                raise
        if attempt < retries - 1:
            time.sleep(backoff * (attempt + 1))
    # All retries exhausted on 429 — exit cleanly; next hourly run retries
    if last_429 is not None:
        print("[rate-limit] All retries exhausted on 429. Exiting cleanly.")
        sys.exit(0)
    resp.raise_for_status()
    return resp


# ── 2. Fetch recent intervals.icu activities ─────────────────────────────────
RUN_TYPES = {'Run', 'TrailRun', 'VirtualRun'}


def fetch_recent(after_ts):
    """Fetch all activities after after_ts (unix timestamp) from intervals.icu."""
    oldest = datetime.fromtimestamp(after_ts, tz=timezone.utc).strftime('%Y-%m-%dT%H:%M:%S')
    resp = _get_with_retry(
        f'{API_BASE}/athlete/{ATHLETE_ID}/activities',
        params={'oldest': oldest},
    )
    resp.raise_for_status()
    activities = resp.json()
    # Diagnostic: log every raw activity in the window so a "missing run" can
    # be traced to type-filtering vs the API not returning it at all.
    print(f"[fetch] intervals.icu returned {len(activities)} raw activity(ies) in window:")
    for a in activities:
        print(f"  [raw] {str(a.get('start_date_local',''))[:10]} "
              f"id={a.get('id')} type={a.get('type')!r} name={a.get('name','')!r}")
    return [a for a in activities if a.get('type') in RUN_TYPES]


# ── 3. Load / update Strava cache ─────────────────────────────────────────────
def load_cache():
    if os.path.exists(CACHE_JSON):
        with open(CACHE_JSON) as f:
            return json.load(f)
    return {}


def save_cache(cache):
    with open(CACHE_JSON, 'w') as f:
        json.dump(cache, f, ensure_ascii=False)


def _compute_km_splits(activity_id):
    """Build Strava-style per-km splits_metric from intervals.icu streams.
    Returns [] on any failure - splits are enrichment, not critical."""
    try:
        resp = _get_with_retry(
            f'{API_BASE}/activity/{activity_id}/streams',
            params={'types': 'time,distance,heartrate,altitude'},
        )
        if resp.status_code != 200:
            return []
        streams = {s.get('type'): s.get('data') or [] for s in resp.json()}
        dist = streams.get('distance') or []
        t    = streams.get('time') or []
        hr   = streams.get('heartrate') or []
        alt  = streams.get('altitude') or []
        if len(dist) < 2 or len(t) != len(dist):
            return []
        splits, seg_start, n = [], 0, 1
        for i in range(1, len(dist)):
            is_last = (i == len(dist) - 1)
            if (dist[i] is not None and dist[i] >= n * 1000) or is_last:
                seg_d = (dist[i] or 0) - (dist[seg_start] or 0)
                seg_t = (t[i] or 0) - (t[seg_start] or 0)
                if seg_d <= 0 or seg_t <= 0:
                    seg_start = i; n += 1; continue
                seg_hr = [h for h in hr[seg_start:i+1] if h] if hr else []
                elev = None
                if len(alt) == len(dist) and alt[i] is not None and alt[seg_start] is not None:
                    elev = round(alt[i] - alt[seg_start], 1)
                splits.append({
                    'split': n,
                    'distance': round(seg_d, 1),
                    'moving_time': int(seg_t),
                    'elapsed_time': int(seg_t),
                    'average_speed': round(seg_d / seg_t, 3),
                    'average_heartrate': round(sum(seg_hr) / len(seg_hr), 1) if seg_hr else None,
                    'elevation_difference': elev,
                })
                seg_start = i
                n += 1
        return splits
    except Exception as e:
        print(f"  [splits] failed for {activity_id}: {e}", file=sys.stderr)
        return []


def update_cache(cache, activities):
    """Merge new activities into cache. Returns list of newly added.
    Cache entries keep the exact schema the Strava sync used, so everything
    downstream (classify, CSV_DATA, LAPS_DATA) is unchanged."""
    new = []
    for act in activities:
        aid = str(act['id'])
        if aid not in cache:
            try:
                dist_m = act.get('distance') or 0
                moving = act.get('moving_time') or 0
                avg_speed = act.get('average_speed')
                if not avg_speed and dist_m and moving:
                    avg_speed = dist_m / moving
                cache[aid] = {
                    'id': aid,
                    'name': act.get('name', ''),
                    'type': 'Run',
                    'start_date_local': (act.get('start_date_local') or '')[:19],
                    'distance_km': dist_m / 1000.0,
                    'distance_m': dist_m,
                    'elapsed_time': act.get('elapsed_time', 0),
                    'moving_time': moving,
                    'average_speed': avg_speed,
                    'average_heartrate': act.get('average_heartrate'),
                    'max_heartrate': act.get('max_heartrate'),
                    'description': act.get('description') or '',
                    'splits_metric': _compute_km_splits(aid),
                    'laps': [],
                }
                print(f"  [cache] +{aid} {cache[aid]['start_date_local'][:10]} {dist_m/1000:.2f}km — {cache[aid]['name']}")
                new.append(cache[aid])
            except Exception as e:
                print(f"  [cache] failed to process {aid}: {e}", file=sys.stderr)
    return new


# ── 4. Read existing classified_runs.csv ──────────────────────────────────────
def load_classified():
    if not os.path.exists(CLASSIFIED_CSV):
        return [], []
    with open(CLASSIFIED_CSV, newline='', encoding='utf-8') as f:
        reader = csv.reader(f)
        headers = next(reader)
        rows = list(reader)
    return headers, rows


def existing_dates_dists(rows):
    """Return a set of (date_str, round_dist) already in classified_runs.csv."""
    result = set()
    for r in rows:
        if len(r) > 4:
            try:
                date_str = r[1][:10]  # YYYY-MM-DD
                dist = round(float(r[4].replace(',', '.')), 1)
                result.add((date_str, dist))
            except (ValueError, IndexError):
                pass
    return result


def last_classified_date(rows):
    """Return datetime of most recent run in classified_runs.csv."""
    latest = datetime(2020, 1, 1, tzinfo=timezone.utc)
    for r in rows:
        if len(r) > 1:
            try:
                dt = datetime.strptime(r[1], '%Y-%m-%d %H:%M:%S').replace(tzinfo=timezone.utc)
                if dt > latest:
                    latest = dt
            except ValueError:
                pass
    return latest


# ── 5. Classify (Strava-only signals) ────────────────────────────────────────
def normalize(s):
    return s.replace('\u2018', "'").replace('\u2019', "'").replace('\u201c', '"').replace('\u201d', '"')


KNOWN_RACES = [
    r'Bedford\s*5K', r'Marsh\s*Gibbon', r'MRR\s*5k', r'Manchester.*10K',
    r'Valencia\s*Nocturna', r'Pas\s*Ras', r'Valencia\s*10K',
    r'Gal[aá]pagos', r'Carrera\s*(de\s*)?Empresas', r'Ponle\s*Freno',
    r'Solidaridad', r'Marca\s*Personal', r'10K.*Marca', r'Marca.*10K',
    r'Jos[eé]\s*Antonio\s*Redolat', r'Abierta\s*Al\s*Mar', r'Volta\s*a\s*Peu',
    r'Sant\s*Isi[dr]+e', r'Contra\s*Cancer', r'New\s*Balance\s*XXL',
    r'MediaCity\s*10k', r'Barris\s*de\s*Sant', r'Nocturna\s*Val',
    r'Mitja\s*Marat', r'K.ln\s*Marathon', r'Koeln\s*Marathon', r'Cologne\s*Marathon',
]

# Confirmed race calendar: date → (display name, expected distance in km).
# Used to classify generic-named activities (e.g. "Evening Run") that fall on a race day.
RACE_CALENDAR = {
    '2026-04-25': ('Volta a Peu',          5),
    '2026-05-10': ('Redolat 5K',           5),
    '2026-05-24': ('VII Marta Fernandez',  5),
    '2026-06-14': ('Runners Ciutat 5K',    5),
    '2026-06-21': ('Ponle Freno 10K',     10),
    '2026-09-20': ('Barris de Sant Marti', 5),
    '2026-09-26': ('Nocturna Valencia',   15),
    '2026-10-04': ('Köln Marathon',       42),
    '2026-11-08': ('Mitja Marató Gandia', 21),
}
PARKRUN_RE = re.compile(r'[Pp]ark\s*[Rr]un')

INTERVAL_RE = re.compile(
    r'[Ii]nterval|[Ii]ntervalo|\d+\s*x\s*\d+|[Ff]artlek|[Hh]ill\s*[Rr]ep|'
    r'[Cc]uestas|\bx\s*400\b|\bx\s*800\b|\bx\s*1\s*km\b|[Ss]prints?'
)
TEMPO_RE    = re.compile(r'[Tt]empo|[Tt]hreshold|[Pp]rogres|progresiv', re.IGNORECASE)
RECOVERY_RE = re.compile(r'[Rr]ecovery|[Rr]ecupera|regenera', re.IGNORECASE)
LONG_RE     = re.compile(r'[Ll]ong\s*[Rr]un|[Ll]argo|tirón largo', re.IGNORECASE)
EASY_RE     = re.compile(r'relax|easy|suave|tranquil|aerobic|jog', re.IGNORECASE)


def extract_interval_label(name, desc):
    combined = f"{normalize(name)} {normalize(desc)}"
    # NxN' minutes
    m = re.findall(r'(\d+)\s*x\s*(\d+)\'', combined)
    if m:
        best = max(m, key=lambda x: int(x[0]))
        return f"{best[0]}x{best[1]}'"
    # NxNkm
    m = re.search(r'(\d+)\s*x\s*(\d+)\s*km', combined, re.IGNORECASE)
    if m: return f"{m.group(1)}x{m.group(2)}km"
    # NxNm
    m = re.search(r'(\d+)\s*x\s*(\d+)\s*m\b', combined, re.IGNORECASE)
    if m: return f"{m.group(1)}x{m.group(2)}m"
    # Generic NxN
    m = re.search(r'(\d+)\s*x\s*(\d+)', combined)
    if m: return f"{m.group(1)}x{m.group(2)}"
    if re.search(r'[Hh]ill|[Cc]uesta', combined): return "Hill Reps"
    if re.search(r'[Ff]artlek', combined): return "Fartlek"
    return None


def classify_strava_run(name, desc, dist_km):
    name_n = normalize(name)
    desc_n = normalize(desc or '')
    combined = f"{name_n} {desc_n}"
    clean = re.sub(r'[\U00010000-\U0010ffff]', '', name).strip()

    # Race?
    if PARKRUN_RE.search(combined) and dist_km >= 4:
        m = re.match(r'(.+?)\s*[Pp]ark\s*[Rr]un', name)
        loc = m.group(1).strip() if m else ''
        loc = re.sub(r'^(First|2nd|3rd|New|Last|Icy)\s+', '', loc).strip()
        return f"ParkRun – {loc}" if loc else "ParkRun"

    for pat in KNOWN_RACES:
        if re.search(pat, combined, re.IGNORECASE) and dist_km >= 3:
            time_m = re.search(r'\b(\d{1,2}:\d{2}(?::\d{2})?)\b', desc or '')
            suffix = f" | {time_m.group(1)}" if time_m else ''
            return f"Race – {clean}{suffix}"

    if re.match(r'.*\b(5K|10K)\b.*', name, re.IGNORECASE) and dist_km >= 3.5:
        return f"Race – {clean}"

    # Interval?
    if INTERVAL_RE.search(combined):
        label = extract_interval_label(name, desc or '')
        return f"Intervals – {label}" if label else "Intervals"

    # Tempo?
    if TEMPO_RE.search(combined):
        if re.search(r'progresiv', combined, re.IGNORECASE):
            return "Progressive Run"
        return "Tempo Run"

    # Long run?
    if dist_km >= 18 or LONG_RE.search(combined):
        return f"Long Run – {dist_km:.0f}km"

    # Recovery?
    if RECOVERY_RE.search(combined):
        return "Recovery Run"

    # Easy / relaxed?
    if EASY_RE.search(name):
        return "Easy Run"

    # Fallback: descriptive name
    if clean and not re.match(
        r'^(morning|afternoon|evening|lunch|night)?\s*(mon|tue|wed|thu|fri|sat|sun)?\s*run\s*$',
        clean, re.IGNORECASE
    ):
        return f"Easy Run – {clean}"
    return "Easy Run"


def pace_str(avg_speed):
    if not avg_speed or avg_speed <= 0:
        return '--'
    secs = 1000.0 / avg_speed
    return f"{int(secs // 60)}:{int(secs % 60):02d}"


def elapsed_str(secs):
    s = int(secs or 0)
    h, r = divmod(s, 3600)
    m, s = divmod(r, 60)
    return f"{h:02d}:{m:02d}:{s:02d}"


# ── 6. Rebuild dashboard HTML ─────────────────────────────────────────────────
def build_laps_data(cache):
    """Build LAPS_DATA dict from Strava cache (same logic as update_dashboard.py)."""
    result = {}
    for act in cache.values():
        splits = act.get('splits_metric', [])
        raw_laps = act.get('laps', [])
        desc = (act.get('description') or '').strip()
        if not splits and not raw_laps and not desc:
            continue
        date_str = (act.get('start_date_local') or '')[:10]
        if not date_str:
            continue
        dist_km = act.get('distance_km', 0)

        if splits and len(raw_laps or []) <= 1 and len(splits) > 1:
            processed = []
            for s in splits:
                speed = s.get('average_speed')
                pace = None
                if speed and speed > 0:
                    ps = round(1000.0 / speed)
                    pace = f"{ps // 60}:{ps % 60:02d}"
                processed.append({
                    'n': s.get('split', 0), 'dist': round(s.get('distance', 0) / 1000, 2),
                    'time': s.get('moving_time', 0), 'pace': pace,
                    'hr': s.get('average_heartrate'), 'cad': None,
                    'elev': s.get('elevation_difference'),
                })
        else:
            processed = []
            for lap in (raw_laps or []):
                speed = lap.get('avg_speed')
                pace = None
                if speed and speed > 0:
                    ps = round(1000.0 / speed)
                    pace = f"{ps // 60}:{ps % 60:02d}"
                processed.append({
                    'n': lap.get('n', 0), 'dist': round(lap.get('dist_m', 0) / 1000, 2),
                    'time': lap.get('moving_time', 0), 'pace': pace,
                    'hr': lap.get('avg_hr'), 'cad': lap.get('avg_cad'),
                    'elev': lap.get('elevation_gain'),
                })

        result.setdefault(date_str, []).append({'dist_km': dist_km, 'laps': processed, 'desc': desc})
    return result


def rebuild_dashboard(headers, rows, cache):
    if not os.path.exists(DASHBOARD_HTML):
        print("[dashboard] index.html not found — skipping rebuild.", file=sys.stderr)
        return

    with open(DASHBOARD_HTML, encoding='utf-8') as f:
        html = f.read()

    # Inject CSV_DATA
    import io
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(headers)
    writer.writerows(rows)
    csv_content = '\n' + buf.getvalue()

    import re as _re
    csv_pat = _re.compile(r'(var\s+CSV_DATA\s*=\s*`)(.*?)(`;\s*)', _re.DOTALL)
    m = csv_pat.search(html)
    if m:
        html = html[:m.start()] + m.group(1) + csv_content + m.group(3) + html[m.end():]
        print(f"[dashboard] CSV_DATA injected ({len(rows)} rows).")
    else:
        print("[dashboard] WARNING: CSV_DATA block not found.", file=sys.stderr)

    # Inject LAPS_DATA - MERGE with what's already in the HTML rather than
    # replace. The local Garmin-import pipeline adds lap entries the activity
    # cache doesn't have; replacing wholesale would silently drop those dates.
    laps_data = build_laps_data(cache)
    START = 'var LAPS_DATA = '
    END   = '/*__LAPS_DATA_END__*/;'
    si, ei = html.find(START), html.find(END)
    if si >= 0 and ei > si:
        try:
            existing = json.loads(html[si + len(START):ei].rstrip().rstrip(';'))
        except json.JSONDecodeError:
            existing = {}
        merged = {**existing, **laps_data}   # cache wins on shared dates
        laps_json = json.dumps(merged, ensure_ascii=False, separators=(',', ':'))
        html = html[:si + len(START)] + laps_json + html[ei:]
        print(f"[dashboard] LAPS_DATA merged ({len(laps_data)} from cache, "
              f"{len(merged)} total date entries).")
    else:
        print("[dashboard] WARNING: LAPS_DATA sentinel not found.", file=sys.stderr)

    with open(DASHBOARD_HTML, 'w', encoding='utf-8') as f:
        f.write(html)
    print("[dashboard] index.html written.")


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    print("=== intervals.icu CI Sync ===")

    # 1. Load cache + CSV
    cache = load_cache()
    headers, rows = load_classified()
    if not headers:
        print("[csv] classified_runs.csv missing or empty — nothing to base off.", file=sys.stderr)
        sys.exit(1)

    # 2. Determine fetch window — last classified date minus 24h, capped at 14 days
    last_dt = last_classified_date(rows)
    after_ts = int(last_dt.timestamp()) - 86400
    min_ts   = int((datetime.now(timezone.utc) - timedelta(days=14)).timestamp())
    after_ts = max(after_ts, min_ts)
    print(f"[fetch] Fetching intervals.icu activities after {datetime.fromtimestamp(after_ts, tz=timezone.utc).date()}...")

    # 3. Fetch new activities
    activities = fetch_recent(after_ts)
    print(f"[fetch] Got {len(activities)} run(s) from intervals.icu.")

    # 4. Update cache
    new_cached = update_cache(cache, activities)
    save_cache(cache)
    print(f"[cache] {len(new_cached)} new activities added to cache.")

    # 6. Find runs not yet in classified CSV
    col_map = {col: i for i, col in enumerate(headers)}
    already = existing_dates_dists(rows)

    new_rows = []
    for act in sorted(cache.values(), key=lambda x: x.get('start_date_local', '')):
        date_str = act.get('start_date_local', '')[:10]
        dist_km = round(act.get('distance_km', 0), 1)
        if (date_str, dist_km) in already:
            continue

        # Only include runs after the last Garmin entry
        try:
            act_dt = datetime.strptime(act['start_date_local'][:19], '%Y-%m-%dT%H:%M:%S')
        except (ValueError, KeyError):
            continue
        if act_dt.replace(tzinfo=timezone.utc) <= last_dt:
            continue

        name = act.get('name', '')
        desc = act.get('description', '')
        dist_full = act.get('distance_km', 0)

        # Race-calendar override: if this date is a confirmed race AND distance
        # roughly matches (±3km), use the race name regardless of Strava title.
        calendar_title = None
        if date_str in RACE_CALENDAR:
            race_name, race_dist = RACE_CALENDAR[date_str]
            if abs(dist_full - race_dist) <= 3:
                calendar_title = f"Race – {race_name}"
                print(f"  [calendar] {date_str}: matched race day → {calendar_title}")

        title = calendar_title or classify_strava_run(name, desc, dist_full)

        new_row = [''] * len(headers)
        new_row[col_map.get('Tipo de actividad', 0)] = 'Carrera'
        new_row[col_map.get('Fecha', 1)]              = act['start_date_local'][:19].replace('T', ' ')
        new_row[col_map.get('Favorito', 2)]           = 'false'
        new_row[col_map.get('Título', 3)]             = title
        new_row[col_map.get('Distancia', 4)]          = f"{dist_full:.2f}"
        new_row[col_map.get('Tiempo', 6)]             = elapsed_str(act.get('elapsed_time', 0))
        if act.get('average_heartrate'):
            new_row[col_map.get('Frecuencia cardiaca media', 7)] = str(int(act['average_heartrate']))
        if act.get('max_heartrate'):
            new_row[col_map.get('FC máxima', 8)] = str(int(act['max_heartrate']))
        new_row[col_map.get('Ritmo medio', 12)] = pace_str(act.get('average_speed'))

        new_rows.append(new_row)
        already.add((date_str, dist_km))
        print(f"  + {date_str} | {dist_full:.2f}km | {title}")

    if not new_rows:
        print("[csv] No new runs to add.")
    else:
        rows.extend(new_rows)
        # Sort by date descending
        rows.sort(key=lambda r: r[1] if len(r) > 1 else '', reverse=True)
        with open(CLASSIFIED_CSV, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(headers)
            writer.writerows(rows)
        print(f"[csv] classified_runs.csv updated ({len(new_rows)} new rows, {len(rows)} total).")

    # 7. Rebuild dashboard
    rebuild_dashboard(headers, rows, cache)
    print("=== Done ===")


if __name__ == '__main__':
    main()
