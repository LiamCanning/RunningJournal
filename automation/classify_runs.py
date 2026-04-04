#!/usr/bin/env python3
"""
Classify all 254 Garmin runs using Strava descriptions + data signals.
"""

import csv
import os
import re
from datetime import datetime, timedelta


def normalize_quotes(s):
    """Replace curly/smart quotes with straight ones for regex matching."""
    s = s.replace('\u2018', "'").replace('\u2019', "'")
    s = s.replace('\u201C', '"').replace('\u201D', '"')
    return s

GARMIN_PATH = "/Users/liam/Library/Mobile Documents/com~apple~CloudDocs/Personal/Fitness/Running/Data/Garmin/Garmin Connect Activities (29:3:26).csv"
STRAVA_PATH = "/Users/liam/Library/Mobile Documents/com~apple~CloudDocs/Personal/Fitness/Running/Data/Strava/activities.csv"
API_STRAVA_PATH = "/Users/liam/Library/Mobile Documents/com~apple~CloudDocs/Personal/Fitness/Running/_automation/data/strava_api_activities.csv"
OUTPUT_PATH = "/Users/liam/Library/Mobile Documents/com~apple~CloudDocs/Personal/Fitness/Running/Dashboard/classified_runs.csv"


def parse_garmin_date(s):
    return datetime.strptime(s.strip(), "%Y-%m-%d %H:%M:%S")


def parse_strava_date(s):
    s = s.strip().strip('"')
    for fmt in ("%b %d, %Y, %I:%M:%S %p", "%b %d, %Y, %H:%M:%S"):
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            continue
    raise ValueError(f"Cannot parse Strava date: {s}")


def parse_pace(pace_str):
    """Parse pace string like '5:22' into total seconds per km."""
    pace_str = pace_str.strip().strip('"')
    if not pace_str or pace_str == '--':
        return None
    m = re.match(r'(\d+):(\d+)', pace_str)
    if m:
        return int(m.group(1)) * 60 + int(m.group(2))
    return None


def safe_float(s):
    """Parse float, handling comma as thousands separator (Garmin uses '6,930')."""
    try:
        val = s.strip().strip('"')
        # Garmin uses comma as thousands separator in some locales
        # e.g. "6,930" means 6.930 or "8,000" means 8.000
        # But also "10.01" is normal decimal
        if ',' in val and '.' not in val:
            # Could be thousands separator or decimal separator
            # Garmin Spanish CSV: "6,930" likely means 6.930 km (not 6930)
            # Check context: if the part after comma has exactly 3 digits, it's likely a decimal
            parts = val.split(',')
            if len(parts) == 2 and len(parts[1]) == 3:
                # Treat comma as decimal separator: 6,930 -> 6.930
                return float(val.replace(',', '.'))
            elif len(parts) == 2 and len(parts[1]) <= 2:
                return float(val.replace(',', '.'))
            else:
                return float(val.replace(',', ''))
        return float(val)
    except (ValueError, AttributeError):
        return None


def safe_int(s):
    try:
        return int(s.strip().strip('"').replace(',', ''))
    except (ValueError, AttributeError):
        return None


# ── Load Strava runs ──
strava_runs = []
with open(STRAVA_PATH, 'r', encoding='utf-8') as f:
    reader = csv.DictReader(f)
    for row in reader:
        if row['Activity Type'] != 'Run':
            continue
        try:
            dt = parse_strava_date(row['Activity Date'])
        except ValueError:
            continue
        dist_m = float(row.get('Distance', '0') or '0')
        dist_km = dist_m / 1000.0
        strava_runs.append({
            'date': dt,
            'date_str': dt.strftime('%Y-%m-%d'),
            'name': row['Activity Name'].strip(),
            'name_norm': normalize_quotes(row['Activity Name'].strip()),
            'desc': (row.get('Activity Description') or '').strip(),
            'desc_norm': normalize_quotes((row.get('Activity Description') or '').strip()),
            'dist_km': dist_km,
        })

# ── Load API-fetched Strava runs (for recent runs not in bulk export) ──
api_strava_runs = []
if os.path.exists(API_STRAVA_PATH):
    with open(API_STRAVA_PATH, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row.get('Activity Type') != 'Run':
                continue
            try:
                dt = parse_strava_date(row['Activity Date'])
            except ValueError:
                continue
            dist_m = float(row.get('Distance', '0') or '0')
            elapsed = float(row.get('Elapsed Time', '0') or '0')
            avg_hr = float(row.get('Average Heart Rate', '0') or '0') or None
            max_hr = float(row.get('Max Heart Rate', '0') or '0') or None
            avg_speed = float(row.get('Average Speed', '0') or '0') or None
            api_strava_runs.append({
                'date': dt,
                'date_str': dt.strftime('%Y-%m-%d'),
                'name': row['Activity Name'].strip(),
                'name_norm': normalize_quotes(row['Activity Name'].strip()),
                'dist_km': dist_m / 1000.0,
                'elapsed_secs': elapsed,
                'avg_hr': avg_hr,
                'max_hr': max_hr,
                'avg_speed': avg_speed,
            })

# ── Load Garmin runs ──
with open(GARMIN_PATH, 'r', encoding='utf-8') as f:
    reader = csv.reader(f)
    garmin_headers = next(reader)
    garmin_rows = list(reader)

print(f"Loaded {len(garmin_rows)} Garmin runs, {len(strava_runs)} Strava runs")

# ── Build Strava lookup by date ──
strava_by_date = {}
for sr in strava_runs:
    strava_by_date.setdefault(sr['date_str'], []).append(sr)


def match_strava(garmin_dt, garmin_dist):
    """Find matching Strava activity by date, then closest time + similar distance."""
    date_str = garmin_dt.strftime('%Y-%m-%d')
    candidates = strava_by_date.get(date_str, [])
    if not candidates:
        return None
    if len(candidates) == 1:
        return candidates[0]
    # Multiple candidates: score by time difference + distance similarity
    best = None
    best_score = float('inf')
    for c in candidates:
        time_diff = abs((c['date'] - garmin_dt).total_seconds())
        dist_diff = abs(c['dist_km'] - garmin_dist) if garmin_dist else 0
        score = time_diff + dist_diff * 600  # weight distance difference
        if score < best_score:
            best_score = score
            best = c
    return best


# ── Known race patterns ──
KNOWN_RACES = [
    "Bedford 5K", "Marsh Gibbon 5K", "Manchester Road Runners 10K",
    "MRR 5k", "Valencia Nocturna 15k", "Pas Ras 10k", "Valencia 10K",
    r"Gal[aá]pagos", "Carrera Empresas", "Ponle Freno", "Solidaridad",
    r"Jos[eé] Antonio Redolat", "Abierta Al Mar", "Volta a Peu",
    r"Sant Isi[dr]+e", r"Saint Isi[dr]+e", "Contra Cancer",
    "New Balance XXL", "Pas Ras 10km", "MediaCity 10k",
]

PARKRUN_LOCATIONS = {
    'Stretford': 'Stretford',
    'Longford': 'Longford',
    'Alexandra': 'Alexandra',
    'Bucks': 'Bucks',
    'Buckingham': 'Bucks',
    'Rheinpark': 'Rheinpark',
    'Aachener Weiher': 'Aachener Weiher',
    'Aachner Weier': 'Aachener Weiher',
    'Bushy': 'Bushy',
    'An England': 'An England',
}

GENERIC_NAMES = {
    'morning run', 'afternoon run', 'evening run', 'lunch run',
    'monday run', 'tuesday run', 'wednesday run', 'thursday run',
    'friday run', 'saturday run', 'sunday run',
}

INTERVAL_NAME_PATTERNS = [
    r'[Ii]nterval', r'[Ii]ntervalo', r'\bx\s*400', r'\bx\s*800', r'\bx\s*1km', r'\bx\s*1\'',
    r'[Ss]prints?', r'[Pp]istas', r'[Hh]ill\s*[Rr]eps', r'[Ff]artlek',
    r'\bx\s*200', r'[Ll]adder', r'[Dd]own [Ll]adder',
]

INTERVAL_DESC_PATTERNS = [
    r'\d+\s*x\s*\d+\s*(?:km|m\b|\'|"|min)', r'\d+\s*x\s*\d+\'',
    r'[Hh]ill\s*[Rr]eps', r'[Cc]uestas', r'[Ss]prints?', r'[Ff]artlek',
    r'\d+x\d+', r'\d+\s*x\s*\d+\s*(?:km|m\b)',
    r'\d+\s*x\s*\d+\'\s*@', r'\d+\s*x\s*\d+\s*min',
    r'\b[34]00\s*@\s*\d+:\d+',  # 400 @ pace or 300 @ pace reps
]


def is_generic_name(name):
    """Check if a Strava name is generic (day + run type)."""
    n = name.lower().strip()
    # Remove emojis and special chars for comparison
    n_clean = re.sub(r'[^\w\s]', '', n).strip()
    if n_clean in GENERIC_NAMES:
        return True
    # Pattern: "Day Morning/Afternoon/Evening/Lunch Run"
    if re.match(r'^(monday|tuesday|wednesday|thursday|friday|saturday|sunday)?\s*(morning|afternoon|evening|lunch|night)?\s*run\s*$', n_clean, re.IGNORECASE):
        return True
    # Numbered runs: "Run #N", "1st Run", etc
    if re.match(r'^(first|1st|2nd|3rd|\d+th)?\s*run\s*(#?\d+)?\s*$', n_clean, re.IGNORECASE):
        return True
    return False


def extract_interval_desc(name, desc):
    """Try to extract a concise interval description from name+desc."""
    combined = normalize_quotes(f"{name} {desc}")

    # Hill reps / cuestas - check early as they're distinctive
    if re.search(r'[Hh]ill\s*[Rr]eps|[Cc]uestas', combined):
        # Pattern: "Nx Hill Reps" or "Nx Cuestas" or "NxNm Hill Reps"
        m = re.search(r'(\d+)\s*x\s*(?:\d+m?\s+)?(?:[Hh]ill\s*[Rr]eps|[Cc]uestas)', combined)
        if m:
            return f"{m.group(1)}x Hill Reps"
        m = re.search(r'(\d+)\s*x\s*[Cc]uestas', combined, re.IGNORECASE)
        if m:
            return f"{m.group(1)}x Hill Reps"
        return "Hill Reps"

    # NxN' pattern (minutes) - check before km as this is very common
    # Find the pattern with the LARGEST first number (main set)
    all_min_matches = re.findall(r'(\d+)\s*x\s*(\d+)\'', combined)
    if all_min_matches:
        # Pick the one with the most reps (largest first number)
        best = max(all_min_matches, key=lambda m: int(m[0]))
        return f"{best[0]}x{best[1]}'"

    # NxNkm pattern
    m = re.search(r'(\d+)\s*x\s*(\d+)\s*km', combined, re.IGNORECASE)
    if m:
        return f"{m.group(1)}x{m.group(2)}km"

    # NxN mile
    m = re.search(r'(\d+)\s*x\s*(\d+)\s*mile', combined, re.IGNORECASE)
    if m:
        return f"{m.group(1)}x{m.group(2)}mile"

    # NxN" pattern (seconds)
    m = re.search(r'(\d+)\s*x\s*(\d+)"', combined)
    if m:
        return f"{m.group(1)}x{m.group(2)}\""

    # NxNm pattern
    m = re.search(r'(\d+)\s*x\s*(\d+)\s*m\b', combined, re.IGNORECASE)
    if m:
        return f"{m.group(1)}x{m.group(2)}m"

    # NxN min
    m = re.search(r'(\d+)\s*x\s*(\d+)\s*min', combined, re.IGNORECASE)
    if m:
        return f"{m.group(1)}x{m.group(2)}min"

    # "10x1' - desc" style from name like "Intervalos" with desc "10x1' - 4:21 a 3:29"
    m = re.search(r'(\d+)\s*x\s*(\d+)\s*[\'"]', combined)
    if m:
        return f"{m.group(1)}x{m.group(2)}'"

    # Mixed 400/300m reps: "400 @ pace, 300 @ pace, 400 @ pace..."
    reps_400 = len(re.findall(r'\b400\s*@', combined))
    reps_300 = len(re.findall(r'\b300\s*@', combined))
    if reps_400 + reps_300 >= 3:
        return f"{reps_400 + reps_300}x 400/300m"

    # Ladder pattern
    if re.search(r'[Ll]adder', combined):
        m = re.search(r'([\d\s\-–]+min\s*[Ll]adder)', combined)
        if m:
            return m.group(1).strip()
        return "Ladder"

    # Sprints + escaleras
    if re.search(r'[Ss]prints?|escalera', combined):
        return "Sprints"

    # Fartlek
    if re.search(r'[Ff]artlek', combined):
        return "Fartlek"

    # "N mins on, N min off" pattern
    m = re.search(r'(\d+)\s*min(?:ute)?s?\s*on.*?(\d+)\s*min(?:ute)?s?\s*off', combined, re.IGNORECASE)
    if m:
        return f"{m.group(1)}' on / {m.group(2)}' off"

    # "N' on / N' off"
    m = re.search(r"(\d+)\s*['']\s*on.*?(\d+)\s*['']\s*off", combined)
    if m:
        return f"{m.group(1)}' on / {m.group(2)}' off"

    # Cooper test
    if re.search(r'[Cc]ooper', combined):
        return "Cooper Test"

    # 1km Down Ladder style
    if re.search(r'[Dd]own\s*[Ll]adder', combined):
        return "Down Ladder"

    # Generic NxN from desc split times
    m = re.search(r'(\d+)\s*(?:x|X)\s*(\d+)\s*(?:km|k)\b', combined, re.IGNORECASE)
    if m:
        return f"{m.group(1)}x{m.group(2)}km"

    return None


def detect_race(strava, garmin_row):
    """Detect if this is a race. Returns race title or None."""
    name = strava['name'] if strava else ''
    desc = strava['desc_norm'] if strava else ''
    combined = f"{normalize_quotes(name)} {desc}"
    favorito = garmin_row[2].strip().lower() == 'true'
    dist = safe_float(garmin_row[4]) or 0

    # "Bucks Run" with ~5km and PB mention = ParkRun at Bucks
    if re.search(r'Bucks\s*Run', name, re.IGNORECASE) and 4.5 <= dist <= 6:
        desc_lower = desc.lower()
        if 'pb' in desc_lower or 'parkrun' in desc_lower or re.search(r'\b2[012]:\d{2}\b', desc):
            return "ParkRun – Bucks"

    # ParkRun detection
    if re.search(r'[Pp]ark\s*[Rr]un', combined):
        # Only classify as ParkRun if dist is roughly 5k (4-6km)
        if dist < 3.5:
            return None  # Too short - probably warm-up for parkrun
        location = None
        for key, loc in PARKRUN_LOCATIONS.items():
            if key.lower() in combined.lower():
                location = loc
                break
        if location:
            return f"ParkRun – {location}"
        # Try to extract location from name
        m = re.match(r'(.+?)\s*[Pp]ark\s*[Rr]un', name)
        if m:
            loc = m.group(1).strip()
            # Clean up common patterns
            loc = re.sub(r'^(First|2nd|3rd|Icy|Post holiday|Last|New)\s+', '', loc, flags=re.IGNORECASE).strip()
            # Remove emojis
            loc = re.sub(r'[\U00010000-\U0010ffff]', '', loc).strip()
            if loc and len(loc) > 1 and not re.match(r'^(First|2nd|3rd|New|My|The|A|Last)\b', loc, re.IGNORECASE):
                return f"ParkRun – {loc}"
        # Check if it's "Representando a Runners Ciutat fuera de casa" with ParkRun in desc
        if re.search(r'[Pp]ark[Rr]un', desc) and not re.search(r'[Pp]ark[Rr]un', name):
            return "ParkRun"
        return "ParkRun"

    # Known race names - but only if distance is appropriate (not a warm-up)
    for race_pat in KNOWN_RACES:
        if re.search(race_pat, combined, re.IGNORECASE):
            # Skip if this is clearly a warm-up (short distance, another longer run same day)
            if dist < 3:
                return None  # Let warm-up detection handle it
            race_name = name
            # Clean up race name - remove emojis for cleaner output
            clean_name = re.sub(r'[\U00010000-\U0010ffff]', '', race_name).strip()
            clean_name = re.sub(r'\s+', ' ', clean_name).strip()
            # Remove trailing " |" artifacts
            clean_name = re.sub(r'\s*\|\s*$', '', clean_name).strip()

            if dist >= 25:
                return f"Race – 30K {clean_name}"
            elif dist >= 14:
                return f"Race – 15K {clean_name}"
            elif dist >= 9:
                return f"Race – 10K {clean_name}"
            elif dist >= 5.5:
                return f"Race – {clean_name}"
            elif dist >= 4.5:
                return f"Race – 5K {clean_name}"
            else:
                return f"Race – {clean_name}"

    # "5K" or "10K" as event name in strava name (not just distance mention)
    if re.match(r'.*\b(?:5K|10K|10k|5k)\b.*', name) and not re.search(r'x\s*(?:5K|10K|5k|10k)', name):
        if dist >= 3.5:  # Not a warm-up
            if (re.search(r'\b5K\b', name, re.IGNORECASE) and 4.5 <= dist <= 6) or \
               (re.search(r'\b10K\b', name, re.IGNORECASE) and 9 <= dist <= 11):
                if not re.search(r'tempo|threshold|warm|interval', name, re.IGNORECASE):
                    clean_name = re.sub(r'[\U00010000-\U0010ffff]', '', name).strip()
                    clean_name = re.sub(r'\s*\|\s*$', '', clean_name).strip()
                    return f"Race – {clean_name}"

    # Favorito + high effort could be a race
    if favorito and dist >= 4.5:
        avg_pace = parse_pace(garmin_row[12])
        hr = safe_int(garmin_row[7])
        if avg_pace and hr and avg_pace < 280 and hr > 160:
            if not is_generic_name(name) and name:
                clean_name = re.sub(r'[\U00010000-\U0010ffff]', '', name).strip()
                return f"Race – {clean_name}"

    return None


def detect_intervals(strava, garmin_row):
    """Detect interval/speed sessions. Returns title or None."""
    name = strava['name_norm'] if strava else ''
    desc = strava['desc_norm'] if strava else ''
    combined = f"{name} {desc}"
    dist = safe_float(garmin_row[4]) or 0

    # Check name patterns first
    for pat in INTERVAL_NAME_PATTERNS:
        if re.search(pat, name):
            detail = extract_interval_desc(name, desc)
            if detail:
                return f"Intervals – {detail}"
            # Use the name itself if descriptive
            clean_name = re.sub(r'[\U00010000-\U0010ffff]', '', name).strip()
            return f"Intervals – {clean_name}"

    # "Runners Ciutat" with interval structure in description - CHECK BEFORE general patterns
    # so we can defer long-rep Runners Ciutat sessions to tempo
    if re.search(r'Runners Ciutat', name, re.IGNORECASE):
        if desc:
            # Skip to tempo if this has long continuous efforts (>=8 min) without short reps
            # Check for NxN' where N>=8 → those are tempo, handled in detect_tempo
            m_long = re.search(r"(\d+)\s*x\s*(\d+)['']\s*@", desc)
            if m_long and int(m_long.group(2)) >= 8:
                return None  # Let detect_tempo handle it
            # Multiple @-blocks without "x" → tempo, let detect_tempo handle
            at_blocks = re.findall(r"(\d+)[''']?\s*@", desc)
            if len(at_blocks) >= 2 and not re.search(r'\d+\s*x', desc):
                return None  # Let detect_tempo handle it

            # Cuestas/escaleras/sprints in desc
            if re.search(r'[Cc]uesta|escalera|sprint', desc, re.IGNORECASE):
                detail = extract_interval_desc(name, desc)
                if detail:
                    return f"Intervals – {detail}"
                return "Intervals – Runners Ciutat"
            # Check for NxN interval patterns
            if re.search(r'\d+\s*x\s*\d+|x\s*cuestas|\d+x\d+', desc, re.IGNORECASE):
                detail = extract_interval_desc(name, desc)
                if detail:
                    return f"Intervals – {detail}"
                return "Intervals – Runners Ciutat"
            # Short efforts with @ pace: e.g. "5x3' @ 3:58-4:10"
            m = re.search(r"(\d+)\s*x\s*(\d+)['']\s*@", desc)
            if m and int(m.group(2)) < 8:
                return f"Intervals – {m.group(1)}x{m.group(2)}'"
            # NxN' in name (e.g. "Runners Ciutat | 5x4'")
            m = re.search(r"(\d+)\s*x\s*(\d+)['''\"]", name)
            if m and int(m.group(2)) < 8:
                return f"Intervals – {m.group(1)}x{m.group(2)}'"
            # NxN'' (seconds) patterns
            m = re.search(r"(\d+)\s*x\s*(\d+)['']{2}", desc)
            if m:
                return f"Intervals – {m.group(1)}x{m.group(2)}\""
        # Check name itself for structure like "Runners Ciutat - 20', 6x3'"
        m = re.search(r'(\d+)\s*x\s*(\d+)', name)
        if m:
            rep_dur = int(m.group(2))
            if rep_dur < 8:  # Short reps = intervals
                detail = extract_interval_desc(name, '')
                if detail:
                    return f"Intervals – {detail}"
                return "Intervals – Runners Ciutat"
            # >=8 min reps, let tempo handle it

    # "NxN" patterns in the name (e.g. "10x 400", "4x1km") - but NOT Runners Ciutat (handled above)
    if not re.search(r'Runners Ciutat', name, re.IGNORECASE):
        m = re.search(r'(\d+)\s*x\s*(\d+)', name)
        if m:
            detail = extract_interval_desc(name, desc)
            if detail:
                return f"Intervals – {detail}"

    # Check description patterns
    for pat in INTERVAL_DESC_PATTERNS:
        if re.search(pat, desc):
            detail = extract_interval_desc(name, desc)
            if detail:
                return f"Intervals – {detail}"
            return "Intervals"

    # Description has split times (multiple pace entries) with rest indicators
    pace_entries = re.findall(r'\b[34]:\d{2}\b', desc)
    if len(pace_entries) >= 3 and re.search(r'rest|descanso|suave|90s|60s|2\s*min', desc, re.IGNORECASE):
        detail = extract_interval_desc(name, desc)
        if detail:
            return f"Intervals – {detail}"
        return "Intervals"

    # "Bucks Run" with interval structure in desc (e.g., "3x 1 mile @ 4:14")
    if desc and re.search(r'\d+\s*x\s*\d+\s*mile', desc, re.IGNORECASE):
        detail = extract_interval_desc(name, desc)
        if detail:
            return f"Intervals – {detail}"
        return "Intervals"

    # Data signal: best pace much faster than avg AND many laps relative to distance
    avg_pace = parse_pace(garmin_row[12])
    best_pace = parse_pace(garmin_row[13])
    laps = safe_int(garmin_row[30])
    if avg_pace and best_pace and laps and dist > 0:
        ratio = avg_pace / best_pace if best_pace > 0 else 0
        if ratio > 1.25 and laps > dist * 1.5:
            detail = extract_interval_desc(name, desc)
            if detail:
                return f"Intervals – {detail}"
            if name and not is_generic_name(name):
                clean_name = re.sub(r'[\U00010000-\U0010ffff]', '', name).strip()
                return f"Intervals – {clean_name}"
            return "Intervals"

    # "on/off" patterns in desc
    if re.search(r'\d+\s*min(?:ute)?s?\s*on.*\d+\s*min(?:ute)?s?\s*off', desc, re.IGNORECASE):
        detail = extract_interval_desc(name, desc)
        if detail:
            return f"Intervals – {detail}"
        return "Intervals"

    # "Ns on, Ns off" patterns
    if re.search(r'\d+s\s*on.*\d+s\s*off', desc, re.IGNORECASE):
        detail = extract_interval_desc(name, desc)
        return f"Intervals – {detail}" if detail else "Intervals"

    # Cooper test
    if re.search(r'[Cc]ooper', combined):
        return "Intervals – Cooper Test"

    # Description has split times that look like reps (short numbers like "75; 77; 78")
    if re.findall(r'\b\d{2}s?\b', desc) and re.search(r'400|x\d+', combined):
        detail = extract_interval_desc(name, desc)
        if detail:
            return f"Intervals – {detail}"

    return None


def detect_tempo(strava, garmin_row):
    """Detect tempo/threshold/progressive runs. Returns title or None."""
    name = strava['name_norm'] if strava else ''
    desc = strava['desc_norm'] if strava else ''
    combined = f"{name} {desc}"
    dist = safe_float(garmin_row[4]) or 0

    # Name contains tempo/threshold/progresivo/progreso
    if re.search(r'\b[Tt]empo+\b', name):
        return "Tempo Run"
    if re.search(r'[Pp]rogres[io]v[oa]|[Pp]rogreso', name):
        return "Progressive Run"
    if re.search(r'[Tt]hreshold', combined):
        return "Tempo Run"

    # Structured tempo blocks: "20' @ 4:20", "10' @ 4:14", "5' @ 4:04"
    if desc and re.search(r"\d+['']\s*@\s*\d+:\d+", desc):
        blocks = re.findall(r"(\d+)['']\s*@", desc)
        if blocks and not re.search(r'\d+\s*x', desc):
            return "Tempo Run"

    # Description mentions structured tempo effort: "Nk @ pace"
    if re.search(r'\d+k\s*@\s*\d+:\d+', desc, re.IGNORECASE) and not re.search(r'\d+\s*x', desc):
        return "Tempo Run"

    # "progresivo" in description
    if re.search(r'progresiv[oa]', desc, re.IGNORECASE):
        return "Progressive Run"

    # Description has "-> " pattern suggesting progressive pace
    if re.search(r'\d+:\d+\s*->\s*\d+:\d+', desc):
        return "Progressive Run"

    # "Runners Ciutat" with longer continuous efforts
    if re.search(r'Runners Ciutat', name, re.IGNORECASE):
        # Check name for NxN' where N>=8 (e.g., "Runners Ciutat | 3x10'")
        m_name = re.search(r'(\d+)\s*x\s*(\d+)', name)
        if m_name and int(m_name.group(2)) >= 8:
            return f"Tempo Run – Runners Ciutat"
        if desc:
            # Longer efforts: "20' @", "30' @"
            m = re.search(r"(\d+)['']\s*@", desc)
            if m and int(m.group(1)) >= 8 and not re.search(r'\d+\s*x', desc):
                return "Tempo Run – Runners Ciutat"
            # "3x10'" etc - longer intervals (>=8min) = tempo
            m = re.search(r"(\d+)\s*x\s*(\d+)['']\s", desc)
            if m and int(m.group(2)) >= 8:
                return "Tempo Run – Runners Ciutat"
            m = re.search(r'(\d+)\s*x\s*(\d+)\s*min', desc, re.IGNORECASE)
            if m and int(m.group(2)) >= 8:
                return "Tempo Run – Runners Ciutat"
            # "Nk progresivo"
            if re.search(r'progresiv', desc, re.IGNORECASE):
                return "Progressive Run – Runners Ciutat"
            # "20' tempo"
            if re.search(r'tempo', desc, re.IGNORECASE):
                return "Tempo Run – Runners Ciutat"
            # Structured block runs: "30' @ 4:57 / 10' @ 4:23 / 10' @ 4:45"
            at_blocks = re.findall(r"(\d+)[''']?\s*@", desc)
            if len(at_blocks) >= 2 and not re.search(r'\d+\s*x', desc):
                return "Tempo Run – Runners Ciutat"

    # "benchmark" in name
    if re.search(r'[Bb]enchmark', name):
        return "Tempo Run – Benchmark"

    # "a little tempo"
    if re.search(r'tempo', name, re.IGNORECASE):
        return "Tempo Run"

    # "tempo" in desc
    if re.search(r'\btempo\b', desc, re.IGNORECASE):
        return "Tempo Run"

    # "threshold" in desc
    if re.search(r'threshold', desc, re.IGNORECASE):
        return "Tempo Run"

    # Data signal: avg pace 4:10-4:40 with HR > 160 and distance 8-15km
    avg_pace = parse_pace(garmin_row[12])
    hr = safe_int(garmin_row[7])
    if avg_pace and hr:
        if 250 <= avg_pace <= 280 and hr > 160 and 8 <= dist <= 15:
            return "Tempo Run"

    return None


def classify_run(garmin_row, all_garmin_rows, row_idx):
    """Classify a single Garmin run. Returns the new title."""
    garmin_dt = parse_garmin_date(garmin_row[1])
    dist = safe_float(garmin_row[4]) or 0
    avg_pace = parse_pace(garmin_row[12])

    # Match to Strava
    strava = match_strava(garmin_dt, dist)
    strava_name = strava['name'] if strava else ''
    strava_name_norm = strava['name_norm'] if strava else ''
    strava_desc = strava['desc'] if strava else ''
    strava_desc_norm = strava['desc_norm'] if strava else ''

    # ── D. Warm-up / Cool-down Detection (check BEFORE race to avoid false positives) ──
    date_str = garmin_dt.strftime('%Y-%m-%d')
    if dist < 3:
        same_day_runs = []
        for i, r in enumerate(all_garmin_rows):
            if i == row_idx:
                continue
            r_dt = parse_garmin_date(r[1])
            if r_dt.strftime('%Y-%m-%d') == date_str:
                r_dist = safe_float(r[4]) or 0
                same_day_runs.append((r_dt, r_dist, i))
        for r_dt, r_dist, _ in same_day_runs:
            if r_dist > dist * 1.5:
                if garmin_dt < r_dt:
                    return "Warm-Up"
                else:
                    return "Cool-Down"

    # ── A. Race Detection ──
    race = detect_race(strava, garmin_row)
    if race:
        return race

    # ── B. Interval/Speed Detection ──
    intervals = detect_intervals(strava, garmin_row)
    if intervals:
        return intervals

    # ── C. Tempo/Threshold Detection ──
    tempo = detect_tempo(strava, garmin_row)
    if tempo:
        return tempo

    # ── E. Long Run Detection ──
    if dist >= 20:
        return f"Long Run – {dist:.0f}km"
    if dist >= 15:
        return f"Long Run – {dist:.0f}km"

    # ── F. Easy / Recovery / Moderate / Steady / Fast ──
    context = ""
    if strava_name and not is_generic_name(strava_name):
        context = strava_name

    if avg_pace is None:
        category = "Easy Run"
    elif avg_pace > 330:  # > 5:30
        category = "Recovery Run"
    elif avg_pace > 300:  # 5:00-5:30
        category = "Easy Run"
    elif avg_pace > 280:  # 4:40-5:00
        category = "Moderate Run"
    elif avg_pace > 260:  # 4:20-4:40
        category = "Steady Run"
    elif dist < 8:  # < 4:20 and short
        category = "Fast Run"
    else:
        category = "Steady Run"

    # Add context from Strava name if descriptive
    if context:
        ctx_lower = context.lower()
        # Don't duplicate if context already matches category words
        if any(w in ctx_lower for w in ['recovery', 'easy', 'moderate', 'steady', 'fast', 'relaxed', 'relajado', 'tranquilo']):
            return category
        # Clean up context
        clean_ctx = re.sub(r'[\U00010000-\U0010ffff]', '', context).strip()
        clean_ctx = re.sub(r'\s+', ' ', clean_ctx).strip()
        # Remove trailing pipes and strava links
        clean_ctx = re.sub(r'\s*\|.*$', '', clean_ctx).strip()
        clean_ctx = re.sub(r'\[strava://.*?\]', '', clean_ctx).strip()
        if clean_ctx and len(clean_ctx) > 2:
            return f"{category} – {clean_ctx}"

    return category


# ── Classify all runs ──
results = []
for idx, row in enumerate(garmin_rows):
    title = classify_run(row, garmin_rows, idx)
    new_row = list(row)
    new_row[3] = title
    results.append(new_row)
    dt = row[1]
    dist = row[4]
    old_title = row[3]
    print(f"{idx+1:3d}. {dt} | {dist:>6s}km | {title}")

# ── Append Strava-only runs not present in Garmin data ──
if garmin_rows:
    last_garmin_dt = max(parse_garmin_date(r[1]) for r in garmin_rows)
else:
    last_garmin_dt = datetime(2020, 1, 1)

garmin_dates = {parse_garmin_date(r[1]).strftime('%Y-%m-%d') for r in garmin_rows}
col_map = {col: i for i, col in enumerate(garmin_headers)}

strava_only = [ar for ar in api_strava_runs
               if ar['date'] > last_garmin_dt and ar['date_str'] not in garmin_dates]
strava_only.sort(key=lambda x: x['date'])

for ar in strava_only:
    name = ar['name_norm']
    dist = ar['dist_km']

    # Simple classification from Strava name
    if re.search(r'[Ii]nterval|[Ii]ntervalo|\bx\s*\d+km\b|\bx\s*400\b|\bx\s*800\b|\bx\s*1km\b|[Ff]artlek|[Hh]ill\s*[Rr]ep', name):
        title = "Intervals"
    elif re.search(r'[Tt]empo|[Tt]hreshold|[Pp]rogres', name):
        title = "Tempo Run"
    elif dist >= 18 or re.search(r'[Ll]ong\s*[Rr]un|[Ll]argo', name):
        title = f"Long Run – {dist:.0f}km"
    elif re.search(r'[Rr]ecovery|[Rr]ecupera', name):
        title = "Recovery Run"
    elif re.search(r'\b[Rr]ace\b|[Cc]arrera\s+[A-Z]', name):
        title = f"Race – {ar['name']}"
    else:
        clean = re.sub(r'[\U00010000-\U0010ffff]', '', ar['name']).strip()
        title = f"Easy Run – {clean}" if clean else "Easy Run"

    # Convert speed (m/s) to pace string mm:ss
    if ar['avg_speed'] and ar['avg_speed'] > 0:
        pace_secs = 1000.0 / ar['avg_speed']
        pace_str = f"{int(pace_secs // 60)}:{int(pace_secs % 60):02d}"
    else:
        pace_str = '--'

    # Convert elapsed time to hh:mm:ss
    t = int(ar['elapsed_secs'] or 0)
    h, rem = divmod(t, 3600)
    m, s = divmod(rem, 60)
    time_str = f"{h:02d}:{m:02d}:{s:02d}"

    new_row = [''] * len(garmin_headers)
    new_row[col_map.get('Tipo de actividad', 0)] = 'Carrera'
    new_row[col_map.get('Fecha', 1)] = ar['date'].strftime('%Y-%m-%d %H:%M:%S')
    new_row[col_map.get('Favorito', 2)] = 'false'
    new_row[col_map.get('Título', 3)] = title
    new_row[col_map.get('Distancia', 4)] = f"{dist:.2f}"
    new_row[col_map.get('Tiempo', 6)] = time_str
    if ar['avg_hr']:
        new_row[col_map.get('Frecuencia cardiaca media', 7)] = str(int(ar['avg_hr']))
    if ar['max_hr']:
        new_row[col_map.get('FC máxima', 8)] = str(int(ar['max_hr']))
    new_row[col_map.get('Ritmo medio', 12)] = pace_str
    results.append(new_row)
    print(f"  + Strava-only: {ar['date_str']} | {dist:.2f}km | {title}")

# ── Write output ──
with open(OUTPUT_PATH, 'w', newline='', encoding='utf-8') as f:
    writer = csv.writer(f)
    writer.writerow(garmin_headers)
    writer.writerows(results)

print(f"\nWritten {len(results)} classified runs to {OUTPUT_PATH}")
