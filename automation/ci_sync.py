#!/usr/bin/env python3
"""
GitHub Actions sync — runs in the cloud every hour.
Fetches new Strava runs, classifies them, appends to classified_runs.csv,
rebuilds index.html. No Garmin CSV or iCloud dependency.
"""

import csv
import json
import os
import re
import sys
from datetime import datetime, timezone

import requests

# ── Credentials from GitHub secrets ──────────────────────────────────────────
CLIENT_ID     = os.environ['STRAVA_CLIENT_ID']
CLIENT_SECRET = os.environ['STRAVA_CLIENT_SECRET']
REFRESH_TOKEN = os.environ['STRAVA_REFRESH_TOKEN']
GH_PAT        = os.environ.get('GH_PAT', '')
GH_REPO       = os.environ.get('GH_REPO', '')

# ── Repo-relative paths ───────────────────────────────────────────────────────
REPO_DIR       = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CACHE_JSON     = os.path.join(REPO_DIR, 'strava_activity_cache.json')
CLASSIFIED_CSV = os.path.join(REPO_DIR, 'classified_runs.csv')
DASHBOARD_HTML = os.path.join(REPO_DIR, 'index.html')

# ── 1. Token refresh ──────────────────────────────────────────────────────────
def get_access_token():
    resp = requests.post('https://www.strava.com/oauth/token', data={
        'client_id':     CLIENT_ID,
        'client_secret': CLIENT_SECRET,
        'refresh_token': REFRESH_TOKEN,
        'grant_type':    'refresh_token',
    })
    resp.raise_for_status()
    data = resp.json()
    new_refresh = data['refresh_token']
    access_token = data['access_token']

    # If Strava rotated our refresh token, update the GitHub secret so next
    # run still works. Requires GH_PAT with repo secrets:write scope.
    if new_refresh != REFRESH_TOKEN and GH_PAT and GH_REPO:
        _update_github_secret('STRAVA_REFRESH_TOKEN', new_refresh)

    print(f"[token] Access token obtained. Expires: {data.get('expires_at')}")
    return access_token


def _update_github_secret(secret_name, secret_value):
    """Update a GitHub Actions secret via REST API using a PAT."""
    try:
        from base64 import b64encode
        from nacl import encoding, public  # pip install PyNaCl

        headers = {
            'Authorization': f'token {GH_PAT}',
            'Accept': 'application/vnd.github+json',
        }
        # Get repo public key
        key_resp = requests.get(
            f'https://api.github.com/repos/{GH_REPO}/actions/secrets/public-key',
            headers=headers
        )
        key_resp.raise_for_status()
        pub_key_data = key_resp.json()
        pub_key = public.PublicKey(pub_key_data['key'].encode(), encoding.Base64Encoder())
        sealed = public.SealedBox(pub_key).encrypt(secret_value.encode())
        encrypted = b64encode(sealed).decode()

        put_resp = requests.put(
            f'https://api.github.com/repos/{GH_REPO}/actions/secrets/{secret_name}',
            headers=headers,
            json={'encrypted_value': encrypted, 'key_id': pub_key_data['key_id']},
        )
        put_resp.raise_for_status()
        print(f"[token] GitHub secret {secret_name} updated (refresh token rotated).")
    except Exception as e:
        print(f"[token] WARNING: could not update GitHub secret: {e}", file=sys.stderr)


# ── 2. Fetch recent Strava activities ─────────────────────────────────────────
def fetch_recent(access_token, after_ts):
    """Fetch all activities after after_ts (unix timestamp)."""
    activities = []
    page = 1
    while True:
        resp = requests.get('https://www.strava.com/api/v3/athlete/activities', headers={
            'Authorization': f'Bearer {access_token}'
        }, params={'after': after_ts, 'per_page': 100, 'page': page})
        resp.raise_for_status()
        batch = resp.json()
        if not batch:
            break
        activities.extend(batch)
        page += 1
        if len(batch) < 100:
            break
    return [a for a in activities if a.get('type') == 'Run']


# ── 3. Load / update Strava cache ─────────────────────────────────────────────
def load_cache():
    if os.path.exists(CACHE_JSON):
        with open(CACHE_JSON) as f:
            return json.load(f)
    return {}


def save_cache(cache):
    with open(CACHE_JSON, 'w') as f:
        json.dump(cache, f, ensure_ascii=False)


def update_cache(cache, activities, access_token):
    """Merge new activities into cache. Returns list of newly added."""
    new = []
    for act in activities:
        aid = str(act['id'])
        if aid not in cache:
            # Fetch detail (for description)
            try:
                detail = requests.get(
                    f'https://www.strava.com/api/v3/activities/{aid}',
                    headers={'Authorization': f'Bearer {access_token}'}
                ).json()
                dist_m = detail.get('distance', act.get('distance', 0))
                cache[aid] = {
                    'id': aid,
                    'name': detail.get('name', act.get('name', '')),
                    'type': 'Run',
                    'start_date_local': detail.get('start_date_local', ''),
                    'distance_km': dist_m / 1000.0,
                    'distance_m': dist_m,
                    'elapsed_time': detail.get('elapsed_time', 0),
                    'moving_time': detail.get('moving_time', 0),
                    'average_speed': detail.get('average_speed'),
                    'average_heartrate': detail.get('average_heartrate'),
                    'max_heartrate': detail.get('max_heartrate'),
                    'description': detail.get('description', ''),
                    'splits_metric': detail.get('splits_metric', []),
                    'laps': [],
                }
                print(f"  [cache] +{aid} {cache[aid]['start_date_local'][:10]} {dist_m/1000:.2f}km — {cache[aid]['name']}")
                new.append(cache[aid])
            except Exception as e:
                print(f"  [cache] failed to fetch detail for {aid}: {e}", file=sys.stderr)
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

    # Inject LAPS_DATA
    laps_data = build_laps_data(cache)
    laps_json = json.dumps(laps_data, ensure_ascii=False, separators=(',', ':'))
    START = 'var LAPS_DATA = '
    END   = '/*__LAPS_DATA_END__*/;'
    si, ei = html.find(START), html.find(END)
    if si >= 0 and ei > si:
        html = html[:si + len(START)] + laps_json + html[ei:]
        print(f"[dashboard] LAPS_DATA injected ({len(laps_data)} date entries).")
    else:
        print("[dashboard] WARNING: LAPS_DATA sentinel not found.", file=sys.stderr)

    with open(DASHBOARD_HTML, 'w', encoding='utf-8') as f:
        f.write(html)
    print("[dashboard] index.html written.")


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    print("=== Strava CI Sync ===")

    # 1. Token
    access_token = get_access_token()

    # 2. Load cache + CSV
    cache = load_cache()
    headers, rows = load_classified()
    if not headers:
        print("[csv] classified_runs.csv missing or empty — nothing to base off.", file=sys.stderr)
        sys.exit(1)

    # 3. Determine fetch window (last classified date, minus 24h buffer)
    last_dt = last_classified_date(rows)
    after_ts = int(last_dt.timestamp()) - 86400
    print(f"[fetch] Fetching Strava activities after {last_dt.date()} (with 24h buffer)...")

    # 4. Fetch new activities
    activities = fetch_recent(access_token, after_ts)
    print(f"[fetch] Got {len(activities)} run(s) from Strava.")

    # 5. Update cache
    new_cached = update_cache(cache, activities, access_token)
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
        title = classify_strava_run(name, desc, dist_full)

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
