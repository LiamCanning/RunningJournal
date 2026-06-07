#!/bin/bash
# Publish the latest dashboard to GitHub Pages.
# Merges CI-injected Strava data (CSV_DATA, LAPS_DATA) into the local HTML
# before pushing, so new activities from the hourly CI sync are never lost.
set -e

ICLOUD="/Users/liam/Library/Mobile Documents/com~apple~CloudDocs/Personal/Fitness/Running"
REPO="$HOME/RunningJournal"
LOCAL_HTML="$ICLOUD/Dashboard/cologne-marathon-dashboard.html"

cd "$REPO"
git pull --rebase origin main

# Merge CI-injected data from repo's index.html into local dashboard HTML
python3 - << 'PYEOF'
import re, json, sys

repo_path   = 'index.html'
local_path  = "/Users/liam/Library/Mobile Documents/com~apple~CloudDocs/Personal/Fitness/Running/Dashboard/cologne-marathon-dashboard.html"

try:
    repo_html  = open(repo_path,  encoding='utf-8').read()
    local_html = open(local_path, encoding='utf-8').read()
except FileNotFoundError as e:
    print(f"[sync] Skipping merge: {e}")
    sys.exit(0)

changed = False

# ── CSV_DATA: use whichever version has more data rows ──────────────────────
csv_pat = re.compile(r'(var CSV_DATA = `)(.*?)(`;\s*)', re.DOTALL)
m_repo  = csv_pat.search(repo_html)
m_local = csv_pat.search(local_html)
if m_repo and m_local:
    repo_csv   = m_repo.group(2)
    local_csv  = m_local.group(2)
    repo_rows  = len([l for l in repo_csv.strip().splitlines() if l.strip()])
    local_rows = len([l for l in local_csv.strip().splitlines() if l.strip()])
    if repo_rows > local_rows:
        local_html = local_html[:m_local.start()] + m_local.group(1) + repo_csv + m_local.group(3) + local_html[m_local.end():]
        print(f"[sync] CSV_DATA: repo has more rows ({repo_rows} vs {local_rows}) — merged in")
        changed = True
    else:
        print(f"[sync] CSV_DATA: local is current ({local_rows} rows)")

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
        merged_club = {**local_club, **repo_club}  # routine-injected (repo) wins
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
