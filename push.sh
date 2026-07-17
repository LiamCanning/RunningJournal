#!/bin/bash
# Publish the latest dashboard to GitHub Pages.
# Merges CI-injected Strava data (CSV_DATA, LAPS_DATA) into the local HTML
# before pushing, so new activities from the hourly CI sync are never lost.
set -e

ICLOUD="$HOME/Library/Mobile Documents/com~apple~CloudDocs/Personal/Fitness/Running"
REPO="$HOME/RunningJournal"
LOCAL_HTML="$ICLOUD/Dashboard/cologne-marathon-dashboard.html"

cd "$REPO"
git pull --rebase origin main || {
    echo "[sync] pull --rebase failed — aborting rebase and resetting to origin/main"
    git rebase --abort 2>/dev/null || true
    git fetch origin main
    git reset --hard origin/main
}

# Merge CI-injected data from repo's index.html into local dashboard HTML
LOCAL_HTML="$LOCAL_HTML" python3 - << 'PYEOF'
import re, json, sys, os

repo_path   = 'index.html'
local_path  = os.environ['LOCAL_HTML']

try:
    repo_html  = open(repo_path,  encoding='utf-8').read()
    local_html = open(local_path, encoding='utf-8').read()
except FileNotFoundError as e:
    print(f"[sync] Skipping merge: {e}")
    sys.exit(0)

changed = False

# ── CSV_DATA: union-merge rows by (date, distance) key (local wins on shared
# keys, CI-only rows added) — a row-count comparison can silently drop runs ──
csv_pat = re.compile(r'(var CSV_DATA = `)(.*?)(`;\s*)', re.DOTALL)
m_repo  = csv_pat.search(repo_html)
m_local = csv_pat.search(local_html)
if not m_repo or not m_local:
    print(f"[sync] ERROR: CSV_DATA block not found (repo={bool(m_repo)}, local={bool(m_local)}) — aborting to avoid publishing stale data", file=sys.stderr)
    sys.exit(1)

import csv as _csv, io as _io

def _csv_rows(block):
    lines = [l for l in block.strip().splitlines() if l.strip()]
    parsed = list(_csv.reader(lines))
    return (parsed[0], parsed[1:]) if parsed else ([], [])

def _row_key(r):
    try:
        return (r[1][:10], round(float(r[4].replace(',', '.')), 1))
    except (IndexError, ValueError):
        return ('raw', ','.join(r))

header_l, rows_l = _csv_rows(m_local.group(2))
header_r, rows_r = _csv_rows(m_repo.group(2))
local_keys = {_row_key(r) for r in rows_l}
new_rows   = [r for r in rows_r if _row_key(r) not in local_keys]
# Always emit rows newest-first, matching ci_sync.py (reverse=True) and the order
# classified_runs.csv is stored in. The two writers used to disagree on direction
# (CI descending, push.sh ascending), so each rewrote the whole block in its own
# order: a ~310-line no-op diff, a commit and a Pages deploy on every single run.
# Sort unconditionally (not only when new_rows) so a local file already in the
# wrong order gets normalised. Safe: the client re-sorts allRuns itself (line ~1974).
merged = sorted(rows_l + new_rows, key=lambda r: r[1] if len(r) > 1 else '', reverse=True)
buf = _io.StringIO()
w = _csv.writer(buf)
w.writerow(header_l or header_r)
w.writerows(merged)
_new_block = m_local.group(1) + '\n' + buf.getvalue() + m_local.group(3)
if _new_block != m_local.group(0):
    local_html = local_html[:m_local.start()] + _new_block + local_html[m_local.end():]
    changed = True
    print(f"[sync] CSV_DATA: rewritten in canonical order, {len(new_rows)} new CI row(s) (local {len(rows_l)} -> {len(merged)})")
else:
    print(f"[sync] CSV_DATA: local is current ({len(rows_l)} rows, repo {len(rows_r)})")

# ── LAPS_DATA: merge by date key (local overrides, new CI dates added) ──────
START = 'var LAPS_DATA = '
END   = '/*__LAPS_DATA_END__*/'
si_r, ei_r = repo_html.find(START),  repo_html.find(END)
si_l, ei_l = local_html.find(START), local_html.find(END)
if si_r >= 0 and ei_r > si_r and si_l >= 0 and ei_l > si_l:
    repo_laps_str  = repo_html[si_r  + len(START):ei_r].rstrip(';').strip()
    local_laps_str = local_html[si_l + len(START):ei_l].rstrip(';').strip()
    try:
        repo_obj  = json.loads(repo_laps_str)
        local_obj = json.loads(local_laps_str)
        # Merge: repo provides base, local overrides (preserves manual edits)
        merged = {**repo_obj, **local_obj}
        new_dates = [k for k in repo_obj if k not in local_obj]
        if new_dates:
            merged_json = json.dumps(merged, ensure_ascii=False, separators=(',', ':'))
            local_html = local_html[:si_l + len(START)] + merged_json + local_html[ei_l:]
            print(f"[sync] LAPS_DATA: added {len(new_dates)} new date(s) from CI: {', '.join(sorted(new_dates)[-3:])}")
            changed = True
        else:
            print(f"[sync] LAPS_DATA: local already has all CI dates ({len(local_obj)} entries)")
    except json.JSONDecodeError as e:
        print(f"[sync] LAPS_DATA: merge skipped (parse error: {e})")

# ── Wellness blocks: CI-owned (intervals.icu wellness -> VO2max + predictions).
# The repo copy is always the freshest; the local pipeline no longer writes
# these, so repo wins wholesale. Single-line vars - no semicolons inside. ──
for _var in ('GARMIN_VO2MAX', 'GARMIN_RACE_PRED', 'GARMIN_LATEST_PRED'):
    _pat = re.compile(r'var ' + _var + r' = .*?;')
    _mr, _ml = _pat.search(repo_html), _pat.search(local_html)
    if _mr and _ml and _mr.group(0) != _ml.group(0):
        local_html = local_html[:_ml.start()] + _mr.group(0) + local_html[_ml.end():]
        print(f"[sync] {_var}: refreshed from CI")
        changed = True

# ── CLUB_SESSIONS: merge by week key (repo/routine wins, preserves injected club sessions) ──
CSTART = 'var CLUB_SESSIONS = '
CEND   = '/*__CLUB_SESSIONS_END__*/'
cr_s, cr_e = repo_html.find(CSTART), repo_html.find(CEND)
cl_s, cl_e = local_html.find(CSTART), local_html.find(CEND)
if cr_s >= 0 and cr_e > cr_s and cl_s >= 0 and cl_e > cl_s:
    repo_club_str  = repo_html[cr_s  + len(CSTART):cr_e].rstrip().rstrip(';').strip()
    local_club_str = local_html[cl_s + len(CSTART):cl_e].rstrip().rstrip(';').strip()
    try:
        repo_club  = json.loads(repo_club_str)  if repo_club_str  else {}
        local_club = json.loads(local_club_str) if local_club_str else {}
        merged_club = {**repo_club, **local_club}  # local (manually maintained) wins; repo adds only new keys
        if merged_club != local_club:
            merged_json = json.dumps(merged_club, ensure_ascii=False, separators=(',', ':'))
            local_html = local_html[:cl_s + len(CSTART)] + merged_json + '; ' + local_html[cl_e:]
            new_keys = [k for k in repo_club if k not in local_club]
            print(f"[sync] CLUB_SESSIONS: merged {len(new_keys)} new week(s) from routine: {', '.join(sorted(new_keys)[-3:])}")
            changed = True
        else:
            print(f"[sync] CLUB_SESSIONS: local already current ({len(local_club)} week(s))")
    except json.JSONDecodeError as e:
        print(f"[sync] CLUB_SESSIONS: merge skipped (parse error: {e})")

if changed:
    open(local_path, 'w', encoding='utf-8').write(local_html)
    print("[sync] Local dashboard updated.")
else:
    print("[sync] No changes needed.")
PYEOF

# Copy updated local HTML to repo (strava_activity_cache.json is CI-maintained — don't touch it)
cp "$LOCAL_HTML" "$REPO/index.html"

git add index.html
git diff --cached --quiet && echo "No changes to push." && exit 0
git commit -m "Update dashboard $(date '+%Y-%m-%d')"
git push origin main
echo "✅ Dashboard published to GitHub Pages"
