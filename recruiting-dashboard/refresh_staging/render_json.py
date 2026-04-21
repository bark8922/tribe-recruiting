"""render_json.py — Assemble dashboard_data.json from Snowflake CSV outputs.

Reads 4 Snowflake query outputs + the current dashboard_data.json (for static
fields) and emits `rendered_dashboard_data.json` in the exact shape App.jsx
consumes.

Inputs (under refresh_staging/):
  snowflake_wbr.csv              raw per-week WBR grid (2026-01..2026-w15)
  snowflake_wbr_jobs.csv         per-week # Jobs grid (wbr_jobs_weekly.sql)
  snowflake_ts.csv               raw per-week TS grid
  snowflake_ts_conversion.csv    per-TS Active Pipelines + funnel (w15 snapshot)
  snowflake_aux_12w.csv          long-format 12w + 60d rollups (TA + TS roles)

Output:
  rendered_dashboard_data.json   merged result, ready to swap into the dashboard

Static fields preserved verbatim from the current dashboard_data.json:
  targets, ts_weekly, ta_weekly_notes, mbr_ta_targets, mbr_window,
  mbr_active_excludes, mbr_source_note, mbr_active_clients, roles, jobs,
  ts_jobs

Computed fields:
  wbr_actuals           (client|ta → per-week funnel; Wolt routed to sub-BU)
  weekly_trend          (per-week aggregate totals across all clients)
  ts_actuals            (ts → per-week funnel)
  ts_conversion         (list of per-TS AP + funnel rows)
  ts_positive_responses (ts → int)
  hires_12w             (raw client|ta → int)
  ta_ats_12w, ta_screens_12w, ta_ttf_12w, ta_jobs_60d (raw keys)
  ts_hires_12w, ts_ats_12w, ts_screens_12w (plain TS name, roster-scoped)
  ta_jobs_weekly        (wNN → {raw_client|raw_ta: # jobs}; event.who_event_created_for
                         attribution matching PBI DAX for WBR Client Summary # Jobs)
  mbr_ta_actuals        (display client|ta → 4w + 12w + 60d rollup)
  mbr_ts_actuals        (ts → 4w + 12w rollup)
  mbr_client_totals     (display client → 4w + 12w_hires rollup)

Key-format conventions (empirically aligned with the current dashboard_data):
  hires_12w / ta_*_12w / ta_jobs_60d / roles  — RAW Keboola client (keeps
    trailing spaces "AVIV ", "Nexi "; no case fix; Wolt without sub-BU)
  wbr_actuals                                 — CLIENT_RENAME (AVIV→Aviv,
    Doordash→DoorDash, Nexi stripped) + Wolt-raw routed to sub-BU via WBR
    roster (long-form labels: "Wolt Central & South", "Wolt North, Baltics &
    Benelux"). DoorDash/SevenRooms kept as separate clients.
  mbr_ta_actuals / mbr_client_totals          — ABBREV rollup (Wolt long labels
    abbreviated to "Wolt C&S"/"Wolt NBB"; Doordash/SevenRooms merged to
    "Wolt HQ"; AVIV→Aviv; etc.)

Run:
  python3 refresh_staging/render_json.py
"""
from __future__ import annotations

import csv
import datetime
import json
import os
import re
import unicodedata
from collections import defaultdict
from pathlib import Path

# Upper ISO-week bound for the WBR CSV loaders. Computed dynamically so the
# render doesn't silently drop current-week TA data when the ISO week rolls.
# (Previous hardcoded `> 16` cap made TA surfaces miss w17 while TS surfaces
# kept showing it — "TS works, TA doesn't" — on 2026-04-21.) +2 buffer handles
# year-end rollover and any clock skew between the SQL run and render.
MAX_ISO_WEEK = datetime.date.today().isocalendar().week + 2

HERE = Path(__file__).resolve().parent
# ROOT = the workspace folder (Recruiting Dashboard). When this script lives at
# <workspace>/refresh_staging/render_json.py, HERE.parent == <workspace>.
# Portable across Cowork sessions (session IDs change; mount folder doesn't).
ROOT = HERE.parent
LIVE_JSON = ROOT / "dashboard_data.json"
OUT_JSON = HERE / "rendered_dashboard_data.json"

SNOW_WBR = HERE / "snowflake_wbr.csv"
SNOW_WBR_JOBS = HERE / "snowflake_wbr_jobs.csv"
SNOW_TS = HERE / "snowflake_ts.csv"
SNOW_TS_CONV = HERE / "snowflake_ts_conversion.csv"
SNOW_TS_JOBS = HERE / "snowflake_ts_jobs.csv"
SNOW_AUX = HERE / "snowflake_aux_12w.csv"
SNOW_PROJECT_DASHBOARD = HERE / "snowflake_project_dashboard.csv"
SNOW_PROJECT_HIRES = HERE / "snowflake_project_dashboard_hires.csv"

# WBR target sheet CSVs — synced by n8n workflow j5QsaTUpk4Nk1xhn.
# These are the SINGLE SOURCE OF TRUTH for who appears in the dashboard:
#   wbr_ta_target.csv      → TA roster + Wolt sub-BU mapping (monthly)
#   wbr_ts_weekly.csv      → TS roster (per-week)
#   wbr_ta_weekly_note.csv → Per-week (Client, TA) active roster
WBR_TA_TARGET_CSV = ROOT / "wbr_static" / "wbr_ta_target.csv"
WBR_TS_WEEKLY_CSV = ROOT / "wbr_static" / "wbr_ts_weekly.csv"
WBR_TA_WEEKLY_NOTE_CSV = ROOT / "wbr_static" / "wbr_ta_weekly_note.csv"

# ─────────────────────────────────────────────────────────────────────────────
# Normalization helpers
# ─────────────────────────────────────────────────────────────────────────────

def norm_name(s: str) -> str:
    """Collapse whitespace + trim — matches App.jsx normalizeTa."""
    return re.sub(r"\s+", " ", s or "").strip()


def fold_name(s: str) -> str:
    """Diacritics-folded, lowered version of norm_name for fuzzy roster matching.
    'Dora Vrbanić' and 'Dora Vrbanic' both fold to 'dora vrbanic'."""
    n = norm_name(s)
    nfkd = unicodedata.normalize("NFKD", n)
    return "".join(c for c in nfkd if not unicodedata.combining(c)).lower()


# WBR-style client rename (minimal): used for wbr_actuals + weekly_trend.
#   AVIV → Aviv   Doordash → DoorDash   "Nexi " → "Nexi"
# Wolt-raw is routed to sub-BU via the roster (see wbr_display_client below).
def wbr_rename(c: str) -> str:
    t = (c or "").strip()
    lc = t.lower()
    if lc == "aviv":
        return "Aviv"
    if lc == "doordash":
        return "DoorDash"
    if lc == "nexi":
        return "Nexi"
    return t


# MBR-style client normalize (ABBREV): used for mbr_* rollups.
#   Doordash / SevenRooms → "Wolt HQ"
#   "Wolt Central & South" → "Wolt C&S"
#   "Wolt North, Baltics & Benelux" → "Wolt NBB"
#   AVIV → Aviv, Nexi → Nexi
ABBREV = {
    "Wolt Central & South": "Wolt C&S",
    "Wolt North, Baltics & Benelux": "Wolt NBB",
}


def mbr_normalize_client(c: str) -> str:
    t = (c or "").strip()
    lc = t.lower()
    if lc == "aviv":
        return "Aviv"
    if lc == "doordash":
        return "Wolt HQ"
    if lc == "sevenrooms":
        return "Wolt HQ"
    if lc == "nexi":
        return "Nexi"
    return ABBREV.get(t, t)


INTERNAL_CLIENTS = {
    "Tribe.xyz", "Tribe.xyz (IR)", "BD - Tribe",
    "Tribe - Marketing", "Kamila AI - TEST", "Bubble test",
}

# Ghost TAs: appear in WBR target sheet but must be excluded from the dashboard.
# Key = "fold_client|fold_ta" so matching is diacritics- and case-insensitive.
# Maintained per Blake's directive (MBR v13).
GHOST_TAS: set[str] = {
    "fever|andrea akovic",
    "grover|rodrigo gomes",
    "grover|rodrigo gomez",       # CSV spelling variant
    "grover|eduardo moral",
    # NOTE: Eucalyptus|Dusan Spica was originally listed as "ASCII dup" ghost
    # in MBR v13, but Dušan Špica is a real TA with real data. The ASCII dup
    # issue doesn't apply in the Snowflake pipeline (DISTINCT COUNT dedupes).
}


def is_ghost_ta(client: str, ta: str) -> bool:
    """True if this (client, TA) pair is on the ghost-TA exclusion list."""
    return f"{fold_name(client)}|{fold_name(ta)}" in GHOST_TAS


# ─────────────────────────────────────────────────────────────────────────────
# Load current dashboard_data.json (for static fields + rosters)
# ─────────────────────────────────────────────────────────────────────────────

def load_live():
    with LIVE_JSON.open() as f:
        return json.load(f)


def build_wolt_roster(target_list, abbrev: bool) -> dict[str, str]:
    """norm_ta → sub-BU display label. Handles both WBR (long form) and MBR
    (ABBREV form) targets. Includes DoorDash/SevenRooms targets only for MBR
    (they merge to Wolt HQ there; WBR keeps them as separate clients)."""
    m = {}
    for t in target_list:
        raw_c = (t.get("client") or "").strip()
        lc = raw_c.lower()
        ta = norm_name(t.get("ta") or "")
        if not ta:
            continue
        # Wolt sub-BU rostering
        if raw_c.startswith("Wolt"):
            label = ABBREV.get(raw_c, raw_c) if abbrev else raw_c
            m[ta] = label
        elif lc in ("doordash", "sevenrooms"):
            if abbrev:
                m[ta] = "Wolt HQ"  # MBR merges
            # WBR: DoorDash/SevenRooms tracked separately — no Wolt roster entry
    return m


def load_mbr_wolt_roster_from_history() -> dict[str, str]:
    """norm_ta → ABBREV Wolt sub-BU label, sourced from wbr_static/wbr_ta_target.csv.

    Takes the MOST RECENT (Year, Month) row per TA whose normalize-client is a
    Wolt-group label (Wolt HQ / Wolt C&S / Wolt NBB / Wolt Tech / …). This
    matches rollup_mbr_ta.py::load_wolt_sub_bu_roster and PBI's "WBR TA Target
    most-recent-per-TA" lookup. TAs who have 2026 Wolt activity but are not in
    the current month's mbr_ta_targets still rollup to the right Wolt sub-BU.

    Falls back to the current mbr_ta_targets-derived roster if the history CSV
    is missing (shouldn't happen in production, but keeps render_json.py usable
    for spot-check runs that only have the dashboard JSON)."""
    newest: dict[str, tuple[int, int, str]] = {}
    try:
        f = WBR_TA_TARGET_CSV.open()
    except FileNotFoundError:
        return {}
    with f:
        for row in csv.DictReader(f):
            try:
                y = int(row["Year"])
                m_ = int(row["Month"])
            except (ValueError, KeyError, TypeError):
                continue
            disp = mbr_normalize_client(row.get("Client") or "")
            if not disp.startswith("Wolt"):
                continue
            ta = norm_name(row.get("TA") or "")
            if not ta:
                continue
            cur = newest.get(ta)
            if cur is None or (y, m_) > (cur[0], cur[1]):
                newest[ta] = (y, m_, disp)
    return {ta: v[2] for ta, v in newest.items()}


def load_ta_roster_from_csv() -> tuple[set[str], dict[str, str]]:
    """Load TA roster + WBR Wolt sub-BU mapping from wbr_ta_target.csv + weekly note.

    Returns (ta_roster, wbr_wolt_roster):
      ta_roster: set of fold_name(TA) for all 2026 rows from BOTH the monthly
        target sheet AND the weekly note. This ensures former employees who
        appear in earlier weekly notes still have their Snowflake data included.
      wbr_wolt_roster: {fold_ta: Wolt sub-BU long label} from most-recent 2026
        month per TA. Used for WBR Wolt routing."""
    ta_roster: set[str] = set()
    wolt_newest: dict[str, tuple[int, str]] = {}  # fold_ta → (month, label)
    try:
        f = WBR_TA_TARGET_CSV.open()
    except FileNotFoundError:
        return set(), {}
    with f:
        for row in csv.DictReader(f):
            try:
                y = int(row["Year"])
            except (ValueError, KeyError):
                continue
            if y != 2026:
                continue
            ta = fold_name(row.get("TA") or "")
            if not ta:
                continue
            ta_roster.add(ta)
            # Wolt sub-BU routing (long-form labels for WBR)
            raw_c = (row.get("Client") or "").strip()
            if raw_c.startswith("Wolt"):
                try:
                    m_ = int(row["Month"])
                except (ValueError, KeyError):
                    continue
                cur = wolt_newest.get(ta)
                if cur is None or m_ > cur[0]:
                    wolt_newest[ta] = (m_, raw_c)
    # Also include TAs from the weekly note — catches people who appear in
    # per-week rosters but aren't in the monthly target sheet (e.g. mid-month
    # additions, former employees still in earlier weeks' rosters).
    try:
        f2 = WBR_TA_WEEKLY_NOTE_CSV.open()
    except FileNotFoundError:
        pass
    else:
        with f2:
            for row in csv.DictReader(f2):
                wk = row.get("Week", "")
                if not wk.startswith("2026"):
                    continue
                ta = fold_name(row.get("TA") or "")
                client = (row.get("Client") or "").strip()
                if ta and client and not is_ghost_ta(client, row.get("TA", "")):
                    ta_roster.add(ta)
    wbr_wolt = {ta: v[1] for ta, v in wolt_newest.items()}
    return ta_roster, wbr_wolt


def load_ta_weekly_roster() -> dict[str, list[str]]:
    """Parse wbr_ta_weekly_note.csv → {wN: ["Client|TA", ...]} per week.
    Uses RAW client/TA names from the sheet (frontend normalizes for matching).
    Only 2026 weeks included."""
    out: dict[str, set[str]] = {}
    try:
        f = WBR_TA_WEEKLY_NOTE_CSV.open()
    except FileNotFoundError:
        return {}
    with f:
        for row in csv.DictReader(f):
            wk_str = row.get("Week", "")
            m = re.match(r"(\d{4})W(\d+)", wk_str)
            if not m or m.group(1) != "2026":
                continue
            wk = f"w{m.group(2)}"
            client = (row.get("Client") or "").strip()
            ta = (row.get("TA") or "").strip()
            if client and ta and not is_ghost_ta(client, ta):
                out.setdefault(wk, set()).add(f"{client}|{ta}")
    return {wk: sorted(v) for wk, v in out.items()}


def load_ta_targets_from_csv(preserve_from_live: list[dict] | None = None) -> list[dict]:
    """Build the `targets` top-level list from wbr_ta_target.csv for the most
    recent 2026 month with a non-zero row per (Client, TA). Preserves team_group
    from the live JSON (the Bubble/n8n pipeline carries that labelling) when
    available; defaults to the client-level Dolphins/Whales or Ponies/Unicorns
    mapping otherwise.

    Without this the `targets` field was being carried forward verbatim from
    live dashboard_data.json via dict(live), which meant Aiven/new-TAs had
    contacted=0 / actual_screens=0 / moved_to_ats=0 (their old values) even
    after Andy set positive targets in the sheet — the WBR Client Summary
    color thresholds silently skipped those cells because the denominator
    was 0.
    """
    try:
        f = WBR_TA_TARGET_CSV.open()
    except FileNotFoundError:
        return list(preserve_from_live or [])

    live_lookup: dict[tuple[str, str], dict] = {}
    for row in (preserve_from_live or []):
        key = (fold_name(row.get("client", "")), fold_name(row.get("ta", "")))
        live_lookup.setdefault(key, row)

    # Group by (client, TA); pick the newest month's values.
    latest: dict[tuple[str, str], tuple[int, dict]] = {}
    with f:
        for row in csv.DictReader(f):
            try:
                y = int(row["Year"])
                m_ = int(row["Month"])
            except (ValueError, KeyError):
                continue
            if y != 2026:
                continue
            client = (row.get("Client") or "").strip()
            ta = (row.get("TA") or "").strip()
            if not client or not ta:
                continue

            def _num(v: str) -> float:
                try:
                    return float(v) if v not in (None, "", " ") else 0.0
                except ValueError:
                    return 0.0

            entry = {
                "client": client,
                "ta": ta,
                "contacted":      _num(row.get("Contacted") or ""),
                "actual_screens": _num(row.get("Actual Screens") or ""),
                "moved_to_ats":   _num(row.get("Moved to ATS") or ""),
                "hires":          _num(row.get("Hires") or ""),
            }
            key = (fold_name(client), fold_name(ta))
            # Preserve team_group from live if present; default client-based mapping
            live_row = live_lookup.get(key)
            tg = (live_row.get("team_group") or "").strip() if live_row else ""
            entry["team_group"] = tg  # empty string = roster-only; keep live's label

            cur = latest.get(key)
            if cur is None or m_ > cur[0]:
                latest[key] = (m_, entry)

    out: list[dict] = [v[1] for v in latest.values()]
    # Add any live rows not in CSV (e.g. synthesized weekly-roster-only rows)
    # so we don't drop existing behaviour downstream.
    csv_keys = set(latest.keys())
    for row in (preserve_from_live or []):
        key = (fold_name(row.get("client", "")), fold_name(row.get("ta", "")))
        if key not in csv_keys:
            out.append(row)
    return out


def load_ta_weekly_notes() -> list[dict]:
    """Parse wbr_ta_weekly_note.csv → list of {client, ta, year, week, reasoning, comment}
    rows, one per Client/TA/Week. Matches the shape App.jsx's taDetail lookup
    uses: it searches for n.ta === t.ta && n.week === selectedWeek (selectedWeek
    is the int week number). Only 2026 rows included.

    This REPLACES the stale ta_weekly_notes carried forward from the live JSON
    — without this, new weeks (w16, w17, ...) show no comments in TA Detail
    until the separate Bubble/n8n pipeline catches up to refresh the PBI
    dashboard_data.json. With this, the Snowflake-side JSON reflects the
    current state of Andy's Google Sheet immediately."""
    out: list[dict] = []
    try:
        f = WBR_TA_WEEKLY_NOTE_CSV.open()
    except FileNotFoundError:
        return out
    with f:
        for row in csv.DictReader(f):
            wk_str = row.get("Week", "")
            m = re.match(r"(\d{4})W(\d+)", wk_str)
            if not m or m.group(1) != "2026":
                continue
            out.append({
                "client": (row.get("Client") or "").strip(),
                "ta": (row.get("TA") or "").strip(),
                "year": 2026,
                "week": int(m.group(2)),
                "reasoning": (row.get("Reasoning") or "").strip() or None,
                "comment": (row.get("Comment") or "").strip() or None,
            })
    return out


def load_ts_weekly_roster() -> dict[str, list[str]]:
    """Parse wbr_ts_weekly.csv → {wN: ["TS1", "TS2", ...]} per week.
    Uses RAW TS names from the sheet. Only 2026 weeks included."""
    out: dict[str, set[str]] = {}
    try:
        f = WBR_TS_WEEKLY_CSV.open()
    except FileNotFoundError:
        return {}
    with f:
        for row in csv.DictReader(f):
            wk_str = row.get("Week", "")
            m = re.match(r"(\d{4})W(\d+)", wk_str)
            if not m or m.group(1) != "2026":
                continue
            wk = f"w{m.group(2)}"
            ts = (row.get("TS") or "").strip()
            if ts:
                out.setdefault(wk, set()).add(ts)
    return {wk: sorted(v) for wk, v in out.items()}


def build_ts_weekly_from_csv() -> list[dict]:
    """Rebuild ts_weekly from wbr_ts_weekly.csv — replaces stale live JSON version.
    Returns list of {ts, year, week, contacted_target, reasoning, comment} dicts.
    This ensures former TSes from earlier weeks are included."""
    rows = []
    try:
        f = WBR_TS_WEEKLY_CSV.open()
    except FileNotFoundError:
        return []
    with f:
        for row in csv.DictReader(f):
            wk_str = row.get("Week", "")
            m = re.match(r"(\d{4})W(\d+)", wk_str)
            if not m or m.group(1) != "2026":
                continue
            ts = (row.get("TS") or "").strip()
            if not ts:
                continue
            rows.append({
                "ts": ts,
                "year": 2026,
                "week": int(m.group(2)),
                "contacted_target": _safe_int(row.get("Contacted Target")),
                "reasoning": (row.get("Reasoning") or "").strip() or None,
                "comment": (row.get("Comment") or "").strip() or None,
            })
    return rows


def _safe_int(v):
    """Parse to int, return None if empty/invalid."""
    try:
        return int(float(v))
    except (TypeError, ValueError):
        return None


def load_ts_roster_from_csv() -> set[str]:
    """Load TS roster from wbr_ts_weekly.csv. Returns set of fold_name(TS) for 2026.
    Uses diacritics-folded names for fuzzy matching."""
    roster: set[str] = set()
    try:
        f = WBR_TS_WEEKLY_CSV.open()
    except FileNotFoundError:
        return set()
    with f:
        for row in csv.DictReader(f):
            try:
                y = int(row["Year"])
            except (ValueError, KeyError):
                continue
            if y != 2026:
                continue
            ts = fold_name(row.get("TS") or "")
            if ts:
                roster.add(ts)
    return roster


# ─────────────────────────────────────────────────────────────────────────────
# Snowflake loaders
# ─────────────────────────────────────────────────────────────────────────────

def load_wbr():
    """Return raw[(raw_client, raw_ta)][f"w{n}"][metric] = int, ISO 2026 only."""
    raw = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))
    with SNOW_WBR.open() as f:
        for row in csv.DictReader(f):
            if int(row["ISO_YEAR"]) != 2026:
                continue
            wn = int(row["ISO_WEEK"])
            if wn < 1 or wn > MAX_ISO_WEEK:
                continue
            wk = f"w{wn}"
            c = row["CLIENT"]
            t = row["TA"]  # keep raw (preserves double-space, trailing space)
            for m in ("contacted", "screened", "actual_screens", "ats", "offers", "hires"):
                raw[(c, t)][wk][m] += int(row[m.upper()])
    return raw


def load_ts_jobs_weekly():
    """Return {f"w{n}": {ts: {num_jobs, num_tas, ta_names}}}, ISO 2026 only.

    Sourced from snowflake_ts_jobs.csv (produced by wbr_ts_jobs_weekly.sql).
    Drives the TS Weekly tab's `# Jobs`, `# TA`, and `TA Names` columns —
    replaces the stale static `ts_jobs` dict that was carried forward from
    live JSON and had wrong per-week counts (Andrea shown as 1/1/'Chené
    Elliot' but PBI shows 7/4 with 4 names).

    Returns empty dict if the file is missing (UI falls back to static ts_jobs)."""
    out: dict[str, dict[str, dict]] = {}
    if not SNOW_TS_JOBS.exists():
        return out
    with SNOW_TS_JOBS.open() as f:
        for row in csv.DictReader(f):
            try:
                y = int(row["ISO_YEAR"])
                wn = int(row["ISO_WEEK"])
            except (ValueError, KeyError):
                continue
            if y != 2026 or wn < 1 or wn > 20:
                continue
            wk = f"w{wn}"
            ts = (row.get("TS") or "").strip()
            if not ts:
                continue
            out.setdefault(wk, {})[ts] = {
                "num_jobs": int(row.get("NUM_JOBS") or 0),
                "num_tas":  int(row.get("NUM_TAS")  or 0),
                "ta_names": (row.get("TA_NAMES") or "").strip(),
            }
    return out



def load_project_dashboard():
    """Return {"rows": [...]} from snowflake_project_dashboard.csv.

    Produced by project_dashboard.sql — per-(client, job, TA, TS, category,
    source, external, day) funnel counts with 6 metrics (contacted, PR,
    actual_screens, ats, offered, hired). Powers the Project Dashboard tab.

    Attribution validated 2026-04-20 against PBIX Overview (Apr 13-19):
    24/24 per-client metrics within 1-3 units.

    File is opt-in. Returns empty list if missing so legacy runs still work."""
    if not SNOW_PROJECT_DASHBOARD.exists():
        return {"rows": []}
    rows = []
    with SNOW_PROJECT_DASHBOARD.open() as f:
        for row in csv.DictReader(f):
            client = (row.get("CLIENT") or "").strip()
            if client in INTERNAL_CLIENTS:
                continue
            rows.append({
                "client":                 client,
                "job_id":                 (row.get("JOB_ID") or "").strip(),
                "job_title":              (row.get("JOB_TITLE") or "").strip(),
                "job_category":           (row.get("JOB_CATEGORY") or "").strip(),
                "ta":                     (row.get("TA") or "").strip(),
                "ts":                     (row.get("TS") or "").strip(),
                "candidate_source":       (row.get("CANDIDATE_SOURCE") or "").strip(),
                "is_external_recruiter":  (row.get("IS_EXTERNAL_RECRUITER") or "").strip().lower() == "true",
                "iso_year":               int(row.get("ISO_YEAR") or 0),
                "iso_week":               int(row.get("ISO_WEEK") or 0),
                "viewed":                 int(row.get("VIEWED") or 0),
                "contacted":              int(row.get("CONTACTED") or 0),
                "positive_response":      int(row.get("POSITIVE_RESPONSE") or 0),
                "screens":                int(row.get("SCREENS") or 0),
                "actual_screens":         int(row.get("ACTUAL_SCREENS") or 0),
                "ats":                    int(row.get("ATS") or 0),
                "offered":                int(row.get("OFFERED") or 0),
                "hired":                  int(row.get("HIRED") or 0),
            })
    return {"rows": rows}


def load_project_hires():
    """Return list of per-hire rows from snowflake_project_dashboard_hires.csv.

    Produced by project_dashboard_hires.sql — one row per hired candidate
    since 2025-01-01. Powers the collapsed "Hires in period" drill-down.

    File is opt-in. Returns empty list if missing."""
    if not SNOW_PROJECT_HIRES.exists():
        return []
    out = []
    with SNOW_PROJECT_HIRES.open() as f:
        for row in csv.DictReader(f):
            client = (row.get("CLIENT") or "").strip()
            if client in INTERNAL_CLIENTS:
                continue
            out.append({
                "candidate_id":           (row.get("CANDIDATE_ID") or "").strip(),
                "client":                 client,
                "job_id":                 (row.get("JOB_ID") or "").strip(),
                "job_title":              (row.get("JOB_TITLE") or "").strip(),
                "ta":                     (row.get("TA") or "").strip(),
                "ts":                     (row.get("TS") or "").strip(),
                "candidate_source":       (row.get("CANDIDATE_SOURCE") or "").strip(),
                "is_external_recruiter":  (row.get("IS_EXTERNAL_RECRUITER") or "").strip().lower() == "true",
                "date_contacted":         (row.get("DATE_CONTACTED") or "").strip() or None,
                "date_screen_actual":     (row.get("DATE_SCREEN_ACTUAL") or "").strip() or None,
                "date_offer":             (row.get("DATE_OFFER") or "").strip() or None,
                "date_hired":             (row.get("DATE_HIRED") or "").strip() or None,
            })
    return out


def load_wbr_jobs():
    """Return jobs[f"w{n}"][f"{raw_client}|{raw_ta}"] = int, ISO 2026 only.

    Sourced from snowflake_wbr_jobs.csv (produced by wbr_jobs_weekly.sql),
    which attributes DISTINCTCOUNT(event.job_id) to event.who_event_created_for
    per (client, TA, iso_year, iso_week). This is the PBI-DAX-compatible
    attribution for the Client Summary # Jobs column — different from the
    job.job_recruiter attribution used for other WBR metrics.

    Returns empty dict if the file is missing (allows render to proceed with
    older snapshots; the UI will fall back to 0 in that case)."""
    out: dict[str, dict[str, int]] = {}
    if not SNOW_WBR_JOBS.exists():
        return out
    with SNOW_WBR_JOBS.open() as f:
        for row in csv.DictReader(f):
            if int(row["ISO_YEAR"]) != 2026:
                continue
            wn = int(row["ISO_WEEK"])
            if wn < 1 or wn > MAX_ISO_WEEK:
                continue
            wk = f"w{wn}"
            c = row["CLIENT"]  # preserve raw spacing
            t = row["TA"]
            key = f"{c}|{t}"
            out.setdefault(wk, {})[key] = int(row.get("JOBS", 0) or 0)
    return out


def load_ts():
    """Return raw[ts][f"w{n}"][metric] = int, ISO 2026 only."""
    raw = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))
    with SNOW_TS.open() as f:
        for row in csv.DictReader(f):
            if int(row["ISO_YEAR"]) != 2026:
                continue
            wn = int(row["ISO_WEEK"])
            wk = f"w{wn}"
            ts = row["TS"]
            for m in ("contacted", "recruiter_screens", "actual_screens",
                      "ats", "offers", "hires"):
                raw[ts][wk][m] += int(row[m.upper()])
    return raw


def _ci(row, key):
    """Case-insensitive CSV column lookup."""
    if key in row:
        return row[key]
    for k in row:
        if k.lower() == key.lower():
            return row[k]
    raise KeyError(key)


def load_ts_conversion():
    """Return list of per-TS rows from snowflake_ts_conversion.csv."""
    rows = []
    with SNOW_TS_CONV.open() as f:
        for row in csv.DictReader(f):
            rows.append({
                "ts": _ci(row, "ts"),
                "active_pipelines": int(_ci(row, "active_pipelines")),
                "contacted": int(_ci(row, "contacted")),
                "positive_response": int(_ci(row, "positive_response")),
                "recruiter_screens": int(_ci(row, "recruiter_screens")),
                "actual_screens": int(_ci(row, "actual_screens")),
                "ats": int(_ci(row, "ats")),
            })
    return rows


def load_aux():
    """Return {(role, metric, client, who): val} from snowflake_aux_12w.csv.
    Keeps raw whitespace in client/who (matches live hires_12w keys)."""
    out = {}
    with SNOW_AUX.open() as f:
        for row in csv.DictReader(f):
            key = (_ci(row, "role"), _ci(row, "metric"),
                   _ci(row, "client"), _ci(row, "who"))
            out[key] = int(float(_ci(row, "val")))
    return out


# ─────────────────────────────────────────────────────────────────────────────
# Computed surfaces
# ─────────────────────────────────────────────────────────────────────────────

def wbr_display_client(raw_client: str, raw_ta: str, wbr_wolt_roster: dict):
    """Route raw Snowflake (client, TA) to the wbr_actuals display-client key.
    Returns None to skip (e.g., raw Wolt for a TA not in the WBR Wolt roster).
    wbr_wolt_roster is keyed by fold_name."""
    raw_c = (raw_client or "").strip()
    lc = raw_c.lower()
    if raw_c == "Wolt" or lc == "wolt":
        return wbr_wolt_roster.get(fold_name(raw_ta))
    return wbr_rename(raw_client)


def build_wbr_actuals(raw_wbr, wbr_wolt_roster, ta_roster=None):
    """key = 'DisplayClient|RawTA' → {wN: {contacted, screened, actual_screens, ats, offers, hires}}
    If ta_roster is given, only include TAs in that set (WBR target sheet = source of truth)."""
    out = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))
    for (rc, rt), weeks in raw_wbr.items():
        # drop internal/test clients
        if rc.strip() in INTERNAL_CLIENTS:
            continue
        if ta_roster is not None and fold_name(rt) not in ta_roster:
            continue
        if is_ghost_ta(rc, rt):
            continue
        disp = wbr_display_client(rc, rt, wbr_wolt_roster)
        if disp is None:
            continue
        key = f"{disp}|{rt}"
        for wk, mv in weeks.items():
            for m, v in mv.items():
                out[key][wk][m] += v
    # Convert nested defaultdicts to plain dicts for JSON
    return {k: {wk: dict(mv) for wk, mv in weeks.items()} for k, weeks in out.items()}


def build_weekly_trend(raw_wbr, min_week: int = 2, max_week: int = MAX_ISO_WEEK):
    """Sum all (client, TA) cells per ISO week → list of {week, contacted, screened, ats, offers, hires}.
    screened here follows App.jsx fallback: actual_screens || screened.
    Week range defaults to w2..MAX_ISO_WEEK (dynamic current-ISO-week ceiling)
    to match the live dashboard: drops near-empty w1, keeps current partial
    week. The upper bound auto-rolls with the calendar — no manual bump."""
    per_week = defaultdict(lambda: defaultdict(int))
    for (rc, rt), weeks in raw_wbr.items():
        if rc.strip() in INTERNAL_CLIENTS:
            continue
        for wk, mv in weeks.items():
            n = int(wk[1:])
            if n < min_week or n > max_week:
                continue
            per_week[wk]["contacted"] += mv.get("contacted", 0)
            per_week[wk]["screened"] += mv.get("actual_screens", 0) or mv.get("screened", 0)
            per_week[wk]["ats"] += mv.get("ats", 0)
            per_week[wk]["offers"] += mv.get("offers", 0)
            per_week[wk]["hires"] += mv.get("hires", 0)
    rows = []
    for wk in sorted(per_week, key=lambda k: int(k[1:])):
        d = per_week[wk]
        rows.append({
            "week": int(wk[1:]),
            "contacted": d["contacted"],
            "screened": d["screened"],
            "ats": d["ats"],
            "offers": d["offers"],
            "hires": d["hires"],
        })
    return rows


def build_ts_actuals(raw_ts, ts_roster=None):
    """key = TS name → {wN: {contacted, recruiter_screens, actual_screens, ats, offers, hires}}
    If ts_roster is given, only include TSes in that set (WBR target sheet = source of truth)."""
    return {ts: {wk: dict(mv) for wk, mv in weeks.items()}
            for ts, weeks in raw_ts.items()
            if ts_roster is None or fold_name(ts) in ts_roster}


def build_ts_positive_responses(ts_conv_rows):
    return {row["ts"]: row["positive_response"] for row in ts_conv_rows}


# ── Raw 12w + 60d surfaces (straight from aux_12w) ──────────────────────────

def build_raw_ta_dict(aux, metric):
    """Sum TA rows by (raw_client, raw_who) for given metric → {'client|ta': val}."""
    out = defaultdict(int)
    for (role, m, c, w), v in aux.items():
        if role != "TA" or m != metric:
            continue
        if is_ghost_ta(c, w):
            continue
        out[f"{c}|{w}"] += v
    return {k: v for k, v in out.items() if v != 0}


def build_ta_ttf(aux):
    """Average TTF days per (raw_client, raw_ta) = round(ttf_sum / ttf_cnt)."""
    sums = defaultdict(int)
    cnts = defaultdict(int)
    for (role, m, c, w), v in aux.items():
        if role != "TA":
            continue
        if is_ghost_ta(c, w):
            continue
        key = f"{c}|{w}"
        if m == "ttf_sum":
            sums[key] += v
        elif m == "ttf_cnt":
            cnts[key] += v
    out = {}
    for k, cnt in cnts.items():
        if cnt > 0:
            out[k] = round(sums[k] / cnt)
    return out


def build_raw_ts_dict(aux, metric, roster: set | None = None):
    """Sum TS rows by who for given metric. If roster is given (fold_name set), only include TSes in roster."""
    out = defaultdict(int)
    for (role, m, c, w), v in aux.items():
        if role != "TS" or m != metric:
            continue
        ts = norm_name(w)
        if roster is not None and fold_name(w) not in roster:
            continue
        out[ts] += v
    return {k: v for k, v in out.items() if v != 0}


# ── MBR rollups (ABBREV keys) ────────────────────────────────────────────────

def mbr_display_for(raw_client: str, raw_ta_norm: str, mbr_wolt_roster: dict) -> str:
    """Matches rollup_mbr_ta.py::display_for.
       - Wolt-raw → roster lookup (fallback: "Wolt")
       - Doordash/SevenRooms → mbr_normalize_client → "Wolt HQ"
       - Else → mbr_normalize_client"""
    lc = (raw_client or "").strip().lower()
    if lc == "wolt":
        return mbr_wolt_roster.get(raw_ta_norm, "Wolt")
    return mbr_normalize_client(raw_client)


def build_mbr_ta_actuals(raw_wbr, aux, mbr_wolt_roster, mbr_weeks: list[str], ta_roster=None):
    """Produce {display|TA: {contacted, actual_screens, ats, offers, hires,
                             hires_12w, screens_12w, ats_12w, jobs_60d}}.
    4w fields sourced from snowflake_wbr per mbr_weeks (w12-w15 default).
    12w + 60d fields sourced from snowflake_aux_12w TA rows.
    If ta_roster is given, only include TAs in that set."""
    out = defaultdict(lambda: dict(
        contacted=0, actual_screens=0, ats=0, offers=0, hires=0,
        hires_12w=0, screens_12w=0, ats_12w=0, jobs_60d=0,
    ))
    # 4-week MBR actuals from snowflake_wbr
    for (rc, rt), weeks in raw_wbr.items():
        if rc.strip() in INTERNAL_CLIENTS:
            continue
        nt = norm_name(rt)
        if ta_roster is not None and fold_name(rt) not in ta_roster:
            continue
        if is_ghost_ta(rc, rt):
            continue
        disp = mbr_display_for(rc, nt, mbr_wolt_roster)
        key = f"{disp}|{nt}"
        for wk in mbr_weeks:
            mv = weeks.get(wk, {})
            out[key]["contacted"] += mv.get("contacted", 0)
            out[key]["actual_screens"] += mv.get("actual_screens", 0)
            out[key]["ats"] += mv.get("ats", 0)
            out[key]["offers"] += mv.get("offers", 0)
            out[key]["hires"] += mv.get("hires", 0)
    # 12w + 60d from aux
    metric_map = {"hires": "hires_12w", "screens": "screens_12w",
                  "ats": "ats_12w", "jobs_60d": "jobs_60d"}
    for (role, m, c, w), v in aux.items():
        if role != "TA":
            continue
        if m not in metric_map:
            continue
        if c.strip() in INTERNAL_CLIENTS:
            continue
        nt = norm_name(w)
        if ta_roster is not None and fold_name(w) not in ta_roster:
            continue
        if is_ghost_ta(c, w):
            continue
        disp = mbr_display_for(c, nt, mbr_wolt_roster)
        key = f"{disp}|{nt}"
        out[key][metric_map[m]] += v
    # Drop empty rows
    return {
        k: v for k, v in out.items()
        if any(v[f] for f in ("contacted", "actual_screens", "ats", "offers", "hires",
                              "hires_12w", "screens_12w", "ats_12w", "jobs_60d"))
    }


def build_mbr_ts_actuals(raw_ts, aux, mbr_weeks: list[str], ts_roster=None):
    """Produce {ts: {contacted_4w, recruiter_screens_4w, actual_screens_4w, ats_4w,
                     hires_12w, screens_12w, ats_12w}}.
    If ts_roster is given, only include TSes in that set."""
    out = {}
    # 4w from raw_ts
    for ts, weeks in raw_ts.items():
        if ts_roster is not None and fold_name(ts) not in ts_roster:
            continue
        row = dict(contacted_4w=0, recruiter_screens_4w=0, actual_screens_4w=0, ats_4w=0,
                   hires_12w=0, screens_12w=0, ats_12w=0)
        for wk in mbr_weeks:
            mv = weeks.get(wk, {})
            row["contacted_4w"] += mv.get("contacted", 0)
            row["recruiter_screens_4w"] += mv.get("recruiter_screens", 0)
            row["actual_screens_4w"] += mv.get("actual_screens", 0)
            row["ats_4w"] += mv.get("ats", 0)
        out[ts] = row
    # 12w from aux (TS role)
    metric_map = {"hires": "hires_12w", "screens": "screens_12w", "ats": "ats_12w"}
    for (role, m, c, w), v in aux.items():
        if role != "TS" or m not in metric_map:
            continue
        ts = norm_name(w)
        if ts_roster is not None and fold_name(w) not in ts_roster:
            continue
        if ts not in out:
            out[ts] = dict(contacted_4w=0, recruiter_screens_4w=0, actual_screens_4w=0, ats_4w=0,
                           hires_12w=0, screens_12w=0, ats_12w=0)
        out[ts][metric_map[m]] += v
    # drop empty
    return {k: v for k, v in out.items()
            if any(v[f] for f in v)}


def build_mbr_client_totals(mbr_ta_actuals):
    """Aggregate mbr_ta_actuals up to display-client level. Matches the original
    dashboard_data shape {client: {contacted, actual_screens, ats, offers, hires, hires_12w}}."""
    out = defaultdict(lambda: dict(contacted=0, actual_screens=0, ats=0, offers=0, hires=0, hires_12w=0))
    for key, row in mbr_ta_actuals.items():
        client = key.split("|", 1)[0]
        for f in ("contacted", "actual_screens", "ats", "offers", "hires", "hires_12w"):
            out[client][f] += row.get(f, 0)
    return {k: v for k, v in out.items()
            if any(v[f] for f in v)}


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def main():
    live = load_live()
    raw_wbr = load_wbr()
    raw_wbr_jobs = load_wbr_jobs()
    raw_ts = load_ts()
    ts_conv_rows = load_ts_conversion()
    aux = load_aux()

    # ── Rosters from WBR target sheet (single source of truth) ──
    # TA roster: who appears in TA sections. Wolt sub-BU mapping from Client col.
    ta_roster, wbr_wolt = load_ta_roster_from_csv()
    if not ta_roster:
        # Fallback: if CSV missing, derive from live JSON (legacy path)
        print("WARNING: wbr_ta_target.csv not found — falling back to live JSON roster")
        wbr_wolt = build_wolt_roster(live.get("targets", []), abbrev=False)
        ta_roster = None  # disable filtering

    # TS roster: who appears in TS sections.
    ts_roster = load_ts_roster_from_csv()
    if not ts_roster:
        print("WARNING: wbr_ts_weekly.csv not found — falling back to live JSON roster")
        ts_roster = {norm_name(t["ts"]) for t in (live.get("ts_weekly") or [])
                     if (t.get("ts"))}

    # MBR Wolt roster: historical CSV (all months) + current targets overlay.
    mbr_wolt_hist = load_mbr_wolt_roster_from_history()
    mbr_wolt_targets = build_wolt_roster(live.get("mbr_ta_targets", []), abbrev=True)
    mbr_wolt = {**mbr_wolt_hist, **mbr_wolt_targets}

    # Per-week rosters from TA Weekly Note / TS Weekly Note
    ta_weekly_roster = load_ta_weekly_roster()
    ts_weekly_roster = load_ts_weekly_roster()
    ta_weekly_notes_from_csv = load_ta_weekly_notes()

    # Rebuild ts_weekly from CSV — the live JSON's version may be stale and
    # missing former TSes from earlier weeks (e.g. Ejla Suljcic w1 only).
    ts_weekly_from_csv = build_ts_weekly_from_csv()
    if ts_weekly_from_csv:
        print(f"  ts_weekly rebuilt from CSV: {len(ts_weekly_from_csv)} entries "
              f"({len(set(r['ts'] for r in ts_weekly_from_csv))} unique TSes)")

    print(f"  TA roster: {len(ta_roster) if ta_roster else 'UNFILTERED'} TAs from CSV")
    print(f"  TS roster: {len(ts_roster)} TSes from CSV")
    print(f"  WBR Wolt sub-BUs: {len(wbr_wolt)} TAs mapped")
    print(f"  TA weekly roster: {len(ta_weekly_roster)} weeks")
    print(f"  TS weekly roster: {len(ts_weekly_roster)} weeks")

    # MBR window = last 4 weeks present in the TA weekly roster (which comes
    # from Andy's Google Sheet). Auto-advances as new weeks are added. Falls
    # back to the live JSON's mbr_window (if present) and finally to a
    # hardcoded default so we never render an empty MBR.
    # Only include weeks that have completed (Sunday in the past). The current
    # ISO week is always partial at any time before end-of-Sunday, so drop it.
    _current_iso_week = datetime.date.today().isocalendar().week
    _ta_week_nums = sorted({
        int(k[1:]) for k in ta_weekly_roster.keys()
        if isinstance(k, str) and k.startswith("w") and k[1:].isdigit()
        and int(k[1:]) < _current_iso_week
    })
    if len(_ta_week_nums) >= 1:
        _last4 = _ta_week_nums[-4:]
        mbr_weeks = [f"w{n}" for n in _last4]
    else:
        mbr_weeks = (live.get("mbr_window") or {}).get("weeks", ["w12", "w13", "w14", "w15"])

    # Computed surfaces (roster-filtered)
    wbr_actuals = build_wbr_actuals(raw_wbr, wbr_wolt, ta_roster=ta_roster)
    weekly_trend = build_weekly_trend(raw_wbr)
    ts_actuals = build_ts_actuals(raw_ts, ts_roster=ts_roster)
    # NOTE: ts_positive_responses in live dashboard comes from an UNSCOPED
    # credit-only PR query (not limited to Active Pipelines), so values are
    # much larger than ts_conversion.positive_response. Preserve from live
    # until we build an unscoped PR SQL. (Ours is scoped; kept for reference
    # inside ts_conversion.positive_response but NOT written to the top-level
    # ts_positive_responses key.)
    ts_positive_responses = build_ts_positive_responses(ts_conv_rows)  # unused in output

    hires_12w = build_raw_ta_dict(aux, "hires")
    ta_ats_12w = build_raw_ta_dict(aux, "ats")
    ta_screens_12w = build_raw_ta_dict(aux, "screens")
    ta_ttf_12w = build_ta_ttf(aux)
    ta_jobs_60d = build_raw_ta_dict(aux, "jobs_60d")

    # TS 12w surfaces — roster-scoped (matches live shape: ts_hires_12w has only rostered TS)
    ts_hires_12w = build_raw_ts_dict(aux, "hires", roster=ts_roster)
    ts_ats_12w = build_raw_ts_dict(aux, "ats", roster=ts_roster)
    ts_screens_12w = build_raw_ts_dict(aux, "screens", roster=ts_roster)

    mbr_ta_actuals = build_mbr_ta_actuals(raw_wbr, aux, mbr_wolt, mbr_weeks, ta_roster=ta_roster)
    mbr_ts_actuals = build_mbr_ts_actuals(raw_ts, aux, mbr_weeks, ts_roster=ts_roster)
    mbr_client_totals = build_mbr_client_totals(mbr_ta_actuals)

    # ts_conversion — static snapshot (lifetime scoped to Active Pipelines).
    ts_conversion = [
        {
            "ts": row["ts"],
            "active_pipelines": row["active_pipelines"],
            "contacted": row["contacted"],
            "positive_response": row["positive_response"],
            "recruiter_screens": row["recruiter_screens"],
            "actual_screens": row["actual_screens"],
            "ats": row["ats"],
        }
        for row in ts_conv_rows
        if ts_roster is None or fold_name(row["ts"]) in ts_roster
    ]

    # ts_conversion_weekly: per-week cumulative conversion data from ts_actuals.
    # For week N, sums w1..wN. Gives per-week-snapshot view of conversion progress.
    # active_pipelines kept from the static snapshot (can't compute per-week).
    ap_lookup = {row["ts"]: row["active_pipelines"] for row in ts_conv_rows}
    pr_lookup = {row["ts"]: row["positive_response"] for row in ts_conv_rows}
    max_week = max((int(wk[1:]) for ts_weeks in ts_actuals.values()
                    for wk in ts_weeks), default=15)
    ts_conversion_weekly = {}
    for wn in range(1, max_week + 1):
        wk_key = f"w{wn}"
        week_rows = []
        for ts_name, weeks in ts_actuals.items():
            # Cumulative sums w1..wN
            cum = defaultdict(int)
            for w in range(1, wn + 1):
                wd = weeks.get(f"w{w}", {})
                cum["contacted"] += wd.get("contacted", 0)
                cum["recruiter_screens"] += wd.get("recruiter_screens", 0) or wd.get("screened", 0)
                cum["actual_screens"] += wd.get("actual_screens", 0)
                cum["ats"] += wd.get("ats", 0)
            if any(cum.values()):
                week_rows.append({
                    "ts": ts_name,
                    "active_pipelines": ap_lookup.get(ts_name, 0),
                    "contacted": cum["contacted"],
                    "positive_response": pr_lookup.get(ts_name, 0),
                    "recruiter_screens": cum["recruiter_screens"],
                    "actual_screens": cum["actual_screens"],
                    "ats": cum["ats"],
                })
        ts_conversion_weekly[wk_key] = week_rows

    # Build targets from wbr_ta_target.csv (current-month values) instead of
    # carrying forward stale numbers from live. Preserves team_group labels
    # from live where available.
    csv_targets = load_ta_targets_from_csv(preserve_from_live=live.get("targets", []))
    print(f"  targets rebuilt from CSV: {len(csv_targets)} entries")

    # Synthesize target entries for TAs in weekly roster but not in targets.
    # This ensures former/mid-month TAs show up in the TA detail table with
    # their actuals (targets = 0) when viewing earlier weeks.
    existing_targets = list(csv_targets)
    existing_pairs = {
        f"{fold_name(t.get('client',''))}|{fold_name(t.get('ta',''))}"
        for t in existing_targets
    }
    added_targets = 0
    for wk_entries in ta_weekly_roster.values():
        for pair in wk_entries:
            parts = pair.split("|", 1)
            if len(parts) != 2:
                continue
            client_raw, ta_raw = parts
            key = f"{fold_name(client_raw)}|{fold_name(ta_raw)}"
            if key not in existing_pairs:
                existing_pairs.add(key)
                existing_targets.append({
                    "client": client_raw,
                    "ta": ta_raw,
                    "contacted": 0, "actual_screens": 0,
                    "moved_to_ats": 0, "hires": 0,
                    "team_group": "",
                })
                added_targets += 1
    if added_targets:
        print(f"  Synthesized {added_targets} target rows for weekly-note-only TAs")

    # Assemble output. Start from a COPY of live so any static field we don't
    # explicitly touch is preserved verbatim (roles, jobs, ts_jobs, targets,
    # ts_weekly, ta_weekly_notes, mbr_ta_targets, mbr_window, etc.)
    out = dict(live)  # shallow copy
    out["targets"] = existing_targets  # includes synthesized entries
    if ts_weekly_from_csv:
        out["ts_weekly"] = ts_weekly_from_csv  # rebuilt from CSV — includes all former TSes
    if ta_weekly_notes_from_csv:
        # Rebuilt from the Google-Sheet-synced CSV so new-week comments
        # (e.g. w16 rolled in by Andy) appear in TA Detail immediately.
        # Without this, ta_weekly_notes came from the live JSON which lags
        # behind Andy's sheet by however long the Bubble/n8n PBI refresh takes.
        print(f"  ta_weekly_notes rebuilt from CSV: {len(ta_weekly_notes_from_csv)} entries")
        out["ta_weekly_notes"] = ta_weekly_notes_from_csv

    out["wbr_actuals"] = wbr_actuals
    out["weekly_trend"] = weekly_trend
    out["ts_actuals"] = ts_actuals
    out["ts_conversion"] = ts_conversion
    out["ts_conversion_weekly"] = ts_conversion_weekly
    # ts_positive_responses intentionally NOT overwritten — live has unscoped PR
    # values (credit-only, no AP filter) that our scoped SQL can't reproduce.
    # Preserved from live via the dict(live) shallow copy. See build_ts_positive_responses docstring.
    out["hires_12w"] = hires_12w
    out["ta_ats_12w"] = ta_ats_12w
    out["ta_screens_12w"] = ta_screens_12w
    out["ta_ttf_12w"] = ta_ttf_12w
    out["ta_jobs_60d"] = ta_jobs_60d
    # Per-week # Jobs for the WBR Client Summary. Keyed by w{N}/{raw_client|raw_ta}
    # (TA = event.who_event_created_for). App.jsx normalizes client, splits Wolt
    # sub-BUs via recruiter map, and filters to (client, TA) pairs present in
    # targets with non-empty team_group. Validated 2026-04-20 vs PBI w16: 129
    # vs 130 (99.2%), 14/15 clients exact.
    out["ta_jobs_weekly"] = raw_wbr_jobs
    # Per-week TS Jobs / TAs / TA names from wbr_ts_jobs_weekly.sql.
    # Replaces the stale static `ts_jobs` for the TS Weekly tab.
    out["ts_jobs_weekly"] = load_ts_jobs_weekly()
    # Project Dashboard — per-day per-(client, job, TA, TS, source, external) funnel
    # counts + line-level hires. Both opt-in (load_project_* gracefully return
    # empty when the CSV is missing). Frontend filters/aggregates client-side.
    out["project_dashboard"] = load_project_dashboard()
    out["project_dashboard_hires"] = load_project_hires()
    if out["project_dashboard"]["rows"]:
        print(f"  project_dashboard: {len(out['project_dashboard']['rows'])} rows")
    if out["project_dashboard_hires"]:
        print(f"  project_dashboard_hires: {len(out['project_dashboard_hires'])} hires")
    out["ts_hires_12w"] = ts_hires_12w
    out["ts_ats_12w"] = ts_ats_12w
    out["ts_screens_12w"] = ts_screens_12w
    out["mbr_ta_actuals"] = mbr_ta_actuals
    out["mbr_ts_actuals"] = mbr_ts_actuals
    out["mbr_client_totals"] = mbr_client_totals
    # Overwrite mbr_window so the frontend sees the same weeks the MBR tables
    # were actually computed from. Dates are ISO Mon-Sun for each week in 2026.
    from datetime import date, timedelta
    def _iso_monday(year: int, week: int) -> date:
        jan4 = date(year, 1, 4)
        jan4_monday = jan4 - timedelta(days=jan4.isoweekday() - 1)
        return jan4_monday + timedelta(weeks=week - 1)
    if mbr_weeks:
        _first_n = int(mbr_weeks[0][1:])
        _last_n = int(mbr_weeks[-1][1:])
        _start = _iso_monday(2026, _first_n).isoformat()
        _end = (_iso_monday(2026, _last_n) + timedelta(days=6)).isoformat()
        out["mbr_window"] = {"start": _start, "end": _end, "weeks": list(mbr_weeks)}
    # Per-week rosters — used by App.jsx to filter client summary and TA/TS
    # detail tables to only show entries active in the selected week.
    out["wbr_ta_weekly_roster"] = ta_weekly_roster
    out["wbr_ts_weekly_roster"] = ts_weekly_roster

    with OUT_JSON.open("w") as f:
        json.dump(out, f, indent=2, ensure_ascii=False, sort_keys=False)

    # Summary log
    print(f"Wrote {OUT_JSON}")
    print(f"  wbr_actuals: {len(wbr_actuals)} keys")
    print(f"  weekly_trend: {len(weekly_trend)} weeks")
    print(f"  ts_actuals: {len(ts_actuals)} TSes")
    print(f"  ts_conversion: {len(ts_conversion)} rows")
    print(f"  hires_12w: {len(hires_12w)} keys  ta_ats_12w: {len(ta_ats_12w)}  "
          f"ta_screens_12w: {len(ta_screens_12w)}  ta_ttf_12w: {len(ta_ttf_12w)}  "
          f"ta_jobs_60d: {len(ta_jobs_60d)}")
    print(f"  ts_hires_12w: {len(ts_hires_12w)} (roster-scoped; {len(ts_roster)} in roster)")
    print(f"  mbr_ta_actuals: {len(mbr_ta_actuals)}  mbr_ts_actuals: {len(mbr_ts_actuals)}  "
          f"mbr_client_totals: {len(mbr_client_totals)}")


if __name__ == "__main__":
    main()
