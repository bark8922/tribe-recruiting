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
SNOW_TS_SUMMARY = HERE / "snowflake_ts_summary.csv"  # per-sourcer x per-week (KPI-TS Summary tab)
SNOW_PROJECT_DASHBOARD = HERE / "snowflake_project_dashboard.csv"
SNOW_PROJECT_DASHBOARD_EVENTATTR = HERE / "snowflake_project_dashboard_eventattr.csv"  # parallel event-based attribution
SNOW_PROJECT_HIRES = HERE / "snowflake_project_dashboard_hires.csv"
SNOW_MBR_CONTACTED_EV = HERE / "snowflake_mbr_contacted_ev.csv"

# Internal Recruiting tab (Tribe.xyz (IR) only) — Phase 2a Bubble-only port
# of Andy's Internal Recruitment PBI page. Phase 2b will add Ashby data.
SNOW_IR_FUNNEL_JOBWEEK     = HERE / "snowflake_ir_funnel_jobweek.csv"
SNOW_IR_SOURCED_JOBWEEK    = HERE / "snowflake_ir_sourced_jobweek.csv"
SNOW_IR_INTERVIEWED_JOBWEEK= HERE / "snowflake_ir_interviewed_jobweek.csv"
SNOW_IR_DQ_BY_STAGE        = HERE / "snowflake_ir_dq_by_stage.csv"
SNOW_IR_JOBS_ACTIVE        = HERE / "snowflake_ir_jobs_active.csv"
SNOW_IR_DQ_BYJOB_REASON    = HERE / "snowflake_ir_dq_byjob_reason.csv"
SNOW_TTH_JOBS = HERE / "snowflake_tth_jobs.csv"
SNOW_WEEKLY_SUMMARY = HERE / "snowflake_weekly_summary.csv"  # PBI Weekly Progress port (dim_type x dim_value x week)
SNOW_WEEKLY_SUMMARY_BYJOB = HERE / "snowflake_weekly_summary_byjob.csv"  # person x job drill

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
    "BD - Tribe", "Tribe - Marketing",
    "Kamila AI - TEST", "Bubble test",
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

            # Keboola's Google Drive extractor sanitizes column names (spaces
            # → underscores); sync_google_sheet.py preserves the originals.
            # Accept both so render_json works under either pipeline.
            entry = {
                "client": client,
                "ta": ta,
                "contacted":      _num(row.get("Contacted") or ""),
                "actual_screens": _num(row.get("Actual Screens") or row.get("Actual_Screens") or ""),
                "moved_to_ats":   _num(row.get("Moved to ATS") or row.get("Moved_to_ATS") or ""),
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
                # Accept "Contacted Target" (sync_google_sheet) or "Contacted_Target" (Keboola extractor)
                "contacted_target": _safe_int(row.get("Contacted Target") or row.get("Contacted_Target")),
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



def load_mbr_contacted_ev():
    """Return list of dicts from snowflake_mbr_contacted_ev.csv.

    Keeps RAW client name (so we can route Wolt / Doordash / SevenRooms via
    the mbr_wolt_roster at MBR-build time, matching mbr_display_for). Returns
    empty list if the CSV is missing (render_json.py falls back to the
    candidate_stage-based Contacted)."""
    out = []
    if not SNOW_MBR_CONTACTED_EV.exists():
        return out
    with SNOW_MBR_CONTACTED_EV.open() as f:
        for row in csv.DictReader(f):
            try:
                client = (row.get("CLIENT") or "").strip()
                ta = (row.get("TA") or "").strip()
                y = int(row.get("ISO_YEAR") or 0)
                w = int(row.get("ISO_WEEK") or 0)
                n = int(row.get("CONTACTED_EV") or 0)
            except (ValueError, KeyError):
                continue
            if not client or not ta:
                continue
            out.append((client, ta, y, w, n))
    return out


def load_project_dashboard(source_path=SNOW_PROJECT_DASHBOARD):
    """Return {"rows": [...]} from snowflake_project_dashboard.csv.

    Produced by project_dashboard.sql — per-(client, job, TA, TS, category,
    source, external, day) funnel counts with 6 metrics (contacted, PR,
    actual_screens, ats, offered, hired). Powers the Project Dashboard tab.

    Attribution validated 2026-04-20 against PBIX Overview (Apr 13-19):
    24/24 per-client metrics within 1-3 units.

    File is opt-in. Returns empty list if missing so legacy runs still work."""
    if not source_path.exists():
        return {"rows": []}
    rows = []
    with source_path.open() as f:
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
                "reacted":                int(row.get("REACTED") or 0),
                "positive_response":      int(row.get("POSITIVE_RESPONSE") or 0),
                "screens":                int(row.get("SCREENS") or 0),
                "actual_screens":         int(row.get("ACTUAL_SCREENS") or 0),
                "ats":                    int(row.get("ATS") or 0),
                "offered":                int(row.get("OFFERED") or 0),
                "hired":                  int(row.get("HIRED") or 0),
            })
    return {"rows": rows}


def load_project_dashboard_eventattr():
    """Return {"rows": [...]} from snowflake_project_dashboard_eventattr.csv.

    Parallel to load_project_dashboard() but event-based attribution
    (TA = event.who_event_created_for). Produced by
    project_dashboard_eventattr.sql. Same schema; React offers an attribution
    toggle (default = job_recruiter). Empty if file missing."""
    return load_project_dashboard(source_path=SNOW_PROJECT_DASHBOARD_EVENTATTR)


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


def load_tth_jobs():
    """Return {"tth_jobs": [...], "tth_monthly": [...]} from snowflake_tth_jobs.csv.

    Produced by the TTH transformation (01kpztmw7d7911kbmyrdf7gcq5) - one row
    per job that has >=1 candidate_stage.date_created >= 2023-01-01. Each row
    carries per-year has_t2f_YYYY / has_t2fi_YYYY inclusion flags plus two
    comma-separated month-lists (t2f_months, t2fi_months) for PBI-accurate
    month-level filtering.

    Powers the Time to Hire dashboard tab. Returns empty lists if the CSV is
    missing so legacy runs without TTH inputs still render the rest of the
    dashboard."""
    if not SNOW_TTH_JOBS.exists():
        return {"tth_jobs": [], "tth_monthly": [], "tth_summary": {"jobs_total": 0, "note": "snowflake_tth_jobs.csv not staged"}}

    jobs = []
    with SNOW_TTH_JOBS.open() as f:
        for row in csv.DictReader(f):
            months_all = [m.strip() for m in (row.get("cand_months") or row.get("CAND_MONTHS") or "").split(",") if m.strip()]
            t2f_months = [m.strip() for m in (row.get("t2f_months") or row.get("T2F_MONTHS") or "").split(",") if m.strip()]
            t2fi_months = [m.strip() for m in (row.get("t2fi_months") or row.get("T2FI_MONTHS") or "").split(",") if m.strip()]
            def _ival(k):
                v = row.get(k) or row.get(k.upper()) or "0"
                try: return int(v)
                except: return 0
            jobs.append({
                "job_id":                     (row.get("job_id") or row.get("JOB_ID") or "").strip(),
                "client":                     (row.get("client_name") or row.get("CLIENT_NAME") or "").strip(),
                "job_title":                  (row.get("job_title") or row.get("JOB_TITLE") or ""),
                "job_category":               (row.get("job_category") or row.get("JOB_CATEGORY") or "Other"),
                "job_subcategory":            (row.get("job_subcategory") or row.get("JOB_SUBCATEGORY") or ""),
                "ta":                         (row.get("ta") or row.get("TA") or ""),
                "date_created":               (row.get("date_created") or row.get("DATE_CREATED") or ""),
                "date_first_hired":           (row.get("date_first_hired") or row.get("DATE_FIRST_HIRED") or ""),
                "date_first_hired_contacted": (row.get("date_first_hired_contacted") or row.get("DATE_FIRST_HIRED_CONTACTED") or ""),
                "tth":           _ival("tth"),
                "t2find":        _ival("t2find"),
                "t2fill":        _ival("t2fill"),
                "has_t2f_2023":  _ival("has_t2f_2023"),
                "has_t2fi_2023": _ival("has_t2fi_2023"),
                "has_t2f_2024":  _ival("has_t2f_2024"),
                "has_t2fi_2024": _ival("has_t2fi_2024"),
                "has_t2f_2025":  _ival("has_t2f_2025"),
                "has_t2fi_2025": _ival("has_t2fi_2025"),
                "has_t2f_2026":  _ival("has_t2f_2026"),
                "has_t2fi_2026": _ival("has_t2fi_2026"),
                "has_t2f":       _ival("has_t2f"),
                "has_t2fi":      _ival("has_t2fi"),
                "tech_role":     (row.get("tech_role") or row.get("TECH_ROLE") or "No"),
                "hire_months":   months_all,
                "t2f_months":    t2f_months,
                "t2fi_months":   t2fi_months,
            })

    # Monthly aggregation: for each month found in any job's hire_months,
    # compute #Jobs, avg TTH (tth > 0), avg T2F (month in t2f_months),
    # avg T2Fi (month in t2fi_months). Matches the dashboard's Month Trends chart.
    from collections import defaultdict
    by_month = defaultdict(list)
    for j in jobs:
        for m in j["hire_months"]:
            by_month[m].append(j)
    monthly = []
    for m in sorted(by_month.keys()):
        js = by_month[m]
        tth_vals = [x["tth"] for x in js if x["tth"] > 0]
        t2f_vals = [x["t2find"] for x in js if m in x["t2f_months"]]
        t2fi_vals = [x["t2fill"] for x in js if m in x["t2fi_months"]]
        monthly.append({
            "month":  m,
            "jobs":   len(js),
            "tth":    round(sum(tth_vals)/len(tth_vals), 2) if tth_vals else None,
            "t2find": round(sum(t2f_vals)/len(t2f_vals), 2) if t2f_vals else None,
            "t2fill": round(sum(t2fi_vals)/len(t2fi_vals), 2) if t2fi_vals else None,
        })

    return {
        "tth_jobs":    jobs,
        "tth_monthly": monthly,
        "tth_summary": {
            "jobs_total": len(jobs),
            "source": "out.c-TTH---tth-jobs.tth_jobs via Keboola Flow",
        },
    }



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


def load_ir_ashby_active_pipeline():
    """Per-(ashby_job_id, stage_title) count of currently-active apps. Empty if Ashby fetch was skipped."""
    p = HERE / "ashby_applications.json"
    if not p.exists(): return []
    import json as _json
    apps = _json.loads(p.read_text())
    by_job_stage = {}
    job_titles = {}
    for a in apps:
        if a.get("status") != "Active": continue
        jid = (a.get("job") or {}).get("id") or ""
        jtl = (a.get("job") or {}).get("title") or ""
        job_titles[jid] = jtl
        st = (a.get("currentInterviewStage") or {}).get("title") or "(none)"
        k = (jid, st)
        by_job_stage[k] = by_job_stage.get(k, 0) + 1
    return [{"ashby_job_id": jid, "ashby_job_title": job_titles.get(jid, ""),
             "stage": st, "count": n}
            for (jid, st), n in sorted(by_job_stage.items(), key=lambda x: (-x[1], x[0]))]


def load_ir_ashby_dq_reasons():
    """Aggregated archive_reason counts across all Tribe Ashby applications."""
    p = HERE / "ashby_applications.json"
    if not p.exists(): return []
    import json as _json
    apps = _json.loads(p.read_text())
    reasons = {}
    for a in apps:
        if a.get("status") != "Archived": continue
        r = (a.get("archiveReason") or {}).get("text") or "(none)"
        reasons[r] = reasons.get(r, 0) + 1
    return [{"reason": r, "count": n} for r, n in sorted(reasons.items(), key=lambda x: -x[1])]


def load_ir_ashby_funnel_jobweek():
    """Per-(ashby_job_id, year, week, stage_title) count of stage entries.
    Reads ashby_application_histories.json — a list of {applicationId, applicationHistory: [...], candidate, job}
    written by ashby_extract.fetch_late_stage_histories(). Each applicationHistory entry has
    {enteredStageAt, leftStageAt, title, stageNumber}."""
    hist_p = HERE / "ashby_application_histories.json"
    if not hist_p.exists(): return []
    import json as _json
    from datetime import datetime
    histories = _json.loads(hist_p.read_text())
    by = {}
    for app_h in histories:
        jid = (app_h.get("job") or {}).get("id","")
        jtl = (app_h.get("job") or {}).get("title","")
        for h in (app_h.get("applicationHistory") or []):
            ent = h.get("enteredStageAt")
            if not ent: continue
            try:
                d = datetime.fromisoformat(ent.replace("Z","+00:00"))
                iso = d.isocalendar()
            except Exception:
                continue
            stage = h.get("title") or ""
            k = (jid, jtl, iso.year, iso.week, stage)
            by[k] = by.get(k, 0) + 1
    return [{"ashby_job_id": jid, "ashby_job_title": jtl,
             "iso_year": y, "iso_week": w, "stage": st, "count": n}
            for (jid, jtl, y, w, st), n in sorted(by.items())]


def load_ir_ashby_jobs_all():
    """All Ashby Tribe-brand jobs (any status) with openedAt/closedAt
    timestamps. The dashboard frontend uses these to determine, per ISO
    week, which jobs were active during that week:
      active_in_week(W) =
        openedAt <= sunday_of_W AND
        (closedAt is null OR closedAt >= monday_of_W)
    Replaces the older ir_ashby_jobs_open which only emitted Open+Draft.
    Now emits all 79 jobs so date-windowed filters can resolve historical
    activity correctly."""
    p = HERE / "ashby_jobs.json"
    if not p.exists(): return []
    import json as _json
    jobs = _json.loads(p.read_text())
    return [{
        "ashby_job_id":     j["id"],
        "ashby_job_title":  j.get("title"),
        "status":           j.get("status"),
        "openedAt":         j.get("openedAt") or j.get("createdAt") or "",
        "closedAt":         j.get("closedAt") or "",
        "createdAt":        j.get("createdAt") or "",
    } for j in jobs]


def load_ir_ashby_hires():
    """Per-(ashby_job_id, year, week) hire count from applications with status=Hired."""
    p = HERE / "ashby_applications.json"
    if not p.exists(): return []
    import json as _json
    from datetime import datetime
    apps = _json.loads(p.read_text())
    by = {}
    for a in apps:
        if a.get("status") != "Hired": continue
        # Use updatedAt as proxy for hire date (Ashby doesn't return a hire-specific timestamp on the app object)
        ts = a.get("updatedAt") or a.get("archivedAt")
        if not ts: continue
        try:
            d = datetime.fromisoformat(ts.replace("Z","+00:00"))
            iso = d.isocalendar()
        except Exception:
            continue
        jid = (a.get("job") or {}).get("id","")
        jtl = (a.get("job") or {}).get("title","")
        k = (jid, jtl, iso.year, iso.week)
        by[k] = by.get(k, 0) + 1
    return [{"ashby_job_id": jid, "ashby_job_title": jtl,
             "iso_year": y, "iso_week": w, "count": n}
            for (jid, jtl, y, w), n in sorted(by.items())]


def load_ir_funnel_jobweek():
    """Per-(job_id, ISO year, ISO week) full funnel for Tribe.xyz (IR).
    Frontend filters by job_id and aggregates across weeks."""
    if not SNOW_IR_FUNNEL_JOBWEEK.exists():
        return []
    rows = []
    with SNOW_IR_FUNNEL_JOBWEEK.open() as f:
        for row in csv.DictReader(f):
            rows.append({
                "job_id":         (row.get("JOB_ID") or "").strip(),
                "iso_year":       int(row.get("ISO_YEAR") or 0),
                "iso_week":       int(row.get("ISO_WEEK") or 0),
                "contacted":      int(row.get("CONTACTED") or 0),
                "pos_response":   int(row.get("POS_RESPONSE") or 0),
                "rec_screens":    int(row.get("REC_SCREENS") or 0),
                "actual_screens": int(row.get("ACTUAL_SCREENS") or 0),
                "ats":            int(row.get("ATS") or 0),
                "onsite":         int(row.get("ONSITE") or 0),
                "culture":        int(row.get("CULTURE") or 0),
                "call_w_client":  int(row.get("CALL_W_CLIENT") or 0),
                "offered":        int(row.get("OFFERED") or 0),
                "hired":          int(row.get("HIRED") or 0),
            })
    return rows


def load_ir_sourced_jobweek():
    """Per-(job_id, sourcer, ISO week) Contacted/Pos/Hired."""
    if not SNOW_IR_SOURCED_JOBWEEK.exists():
        return []
    rows = []
    with SNOW_IR_SOURCED_JOBWEEK.open() as f:
        for row in csv.DictReader(f):
            rows.append({
                "job_id":       (row.get("JOB_ID") or "").strip(),
                "sourcer":      (row.get("SOURCER") or "").strip(),
                "iso_year":     int(row.get("ISO_YEAR") or 0),
                "iso_week":     int(row.get("ISO_WEEK") or 0),
                "contacted":    int(row.get("CONTACTED") or 0),
                "pos_response": int(row.get("POS_RESPONSE") or 0),
                "hired":        int(row.get("HIRED") or 0),
            })
    return rows


def load_ir_interviewed_jobweek():
    """Per-(job_id, TA, ISO week) Actual Screens."""
    if not SNOW_IR_INTERVIEWED_JOBWEEK.exists():
        return []
    rows = []
    with SNOW_IR_INTERVIEWED_JOBWEEK.open() as f:
        for row in csv.DictReader(f):
            rows.append({
                "job_id":         (row.get("JOB_ID") or "").strip(),
                "ta":             (row.get("TA") or "").strip(),
                "iso_year":       int(row.get("ISO_YEAR") or 0),
                "iso_week":       int(row.get("ISO_WEEK") or 0),
                "actual_screens": int(row.get("ACTUAL_SCREENS") or 0),
            })
    return rows


def load_ir_dq_by_stage():
    """Per-job DQ counts at each stage. (job-grain, no week)."""
    if not SNOW_IR_DQ_BY_STAGE.exists():
        return []
    rows = []
    with SNOW_IR_DQ_BY_STAGE.open() as f:
        for row in csv.DictReader(f):
            rows.append({
                "job_title":           (row.get("JOB_TITLE") or "").strip(),
                "job_id":              (row.get("JOB_ID") or "").strip(),
                "stage_contacted":     int(row.get("STAGE_CONTACTED") or 0),
                "stage_rec_screen":    int(row.get("STAGE_REC_SCREEN") or 0),
                "stage_actual_screen": int(row.get("STAGE_ACTUAL_SCREEN") or 0),
                "stage_move_to_ats":   int(row.get("STAGE_MOVE_TO_ATS") or 0),
                "stage_onsite":        int(row.get("STAGE_ONSITE") or 0),
                "stage_offer":         int(row.get("STAGE_OFFER") or 0),
                "total":               int(row.get("TOTAL") or 0),
            })
    return rows


def load_ir_dq_byjob_reason():
    """Per-(job_id, reason) DQ counts. Frontend aggregates per job filter."""
    if not SNOW_IR_DQ_BYJOB_REASON.exists():
        return []
    rows = []
    with SNOW_IR_DQ_BYJOB_REASON.open() as f:
        for row in csv.DictReader(f):
            rows.append({
                "job_id": (row.get("JOB_ID") or "").strip(),
                "reason": (row.get("REASON") or "").strip(),
                "count":  int(row.get("COUNT") or 0),
            })
    return rows


def load_ir_jobs_active():
    """Active IR jobs with days open + hires count."""
    if not SNOW_IR_JOBS_ACTIVE.exists():
        return []
    rows = []
    with SNOW_IR_JOBS_ACTIVE.open() as f:
        for row in csv.DictReader(f):
            rows.append({
                "job_id":        (row.get("JOB_ID") or "").strip(),
                "job_title":     (row.get("JOB_TITLE") or "").strip(),
                "date_created":  (row.get("DATE_CREATED") or "").strip(),
                "job_recruiter": (row.get("JOB_RECRUITER") or "").strip(),
                "job_sourcer":   (row.get("JOB_SOURCER") or "").strip(),
                "days_open":     int(row.get("DAYS_OPEN") or 0),
                "hires_total":   int(row.get("HIRES_TOTAL") or 0),
            })
    return rows


def load_ts_summary():
    """Return [{ts, iso_year, iso_week, viewed, contacted, reacted,
    positive_response, screens, actual_screens, ats, offers, hires,
    hires_tech, jobs}] from snowflake_ts_summary.csv.

    KPI-TS Summary tab data source. Replicates Andy's PBI page filters:
    is_job_archived=False, test=False, who_created_event_first IN Current_TS,
    client_name NOT IN test_clients. Validated 2026-04-27: 11/11 PBI sourcers
    within 10% drift vs snapshot data (2).xlsx.

    Columns viewed, reacted, hires_tech were added 2026-04-27 to support the
    funnel + KPI cards. Older CSVs without these columns get 0 (.get default).

    Returns [] if CSV missing (Flow may not have run yet for the new transform).
    """
    if not SNOW_TS_SUMMARY.exists():
        return []
    rows = []
    def _g(row, key):  # forgiving int reader
        v = _ci(row, key)
        if v is None or v == "":
            return 0
        try:
            return int(float(v))
        except (TypeError, ValueError):
            return 0
    with SNOW_TS_SUMMARY.open() as f:
        for row in csv.DictReader(f):
            rows.append({
                "ts": _ci(row, "ts"),
                "iso_year": int(_ci(row, "iso_year")),
                "iso_week": int(_ci(row, "iso_week")),
                "viewed": _g(row, "viewed"),
                "contacted": _g(row, "contacted"),
                "reacted": _g(row, "reacted"),
                "positive_response": _g(row, "positive_response"),
                "screens": _g(row, "screens"),
                "actual_screens": _g(row, "actual_screens"),
                "ats": _g(row, "ats"),
                "offers": _g(row, "offers"),
                "hires": _g(row, "hires"),
                "hires_tech": _g(row, "hires_tech"),
                "jobs": _g(row, "jobs"),
            })
    return rows


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
    """Route a raw (client, TA) pair to an MBR display client.

    - Wolt / DoorDash / SevenRooms → TA's Wolt sub-BU per the roster.
      (Previously DoorDash/SevenRooms were hardcoded to Wolt HQ regardless
      of the TA, which caused e.g. Adelya Khakimova's DoorDash activity to
      land under Wolt HQ while her target sheet puts her at Wolt NBB. This
      is also the +22/-20 HQ/NBB drift root cause.)
    - Fallback when the TA has no roster entry: Wolt HQ (safe default since
      Wolt HQ is the largest bucket).
    - Else → mbr_normalize_client (AVIV→Aviv, Nexi→Nexi, etc.)
    """
    lc = (raw_client or "").strip().lower()
    if lc in ("wolt", "doordash", "sevenrooms"):
        return mbr_wolt_roster.get(raw_ta_norm, "Wolt HQ")
    return mbr_normalize_client(raw_client)


def build_mbr_ta_actuals(raw_wbr, aux, mbr_wolt_roster, mbr_weeks: list[str], ta_roster=None,
                          active_target_pairs=None, weekly_active_pairs=None):
    """Produce {display|TA: {contacted, actual_screens, ats, offers, hires,
                             hires_12w, screens_12w, ats_12w, jobs_60d}}.
    4w fields sourced from snowflake_wbr per mbr_weeks (w12-w15 default).
    12w + 60d fields sourced from snowflake_aux_12w TA rows.
    If ta_roster is given, only include TAs in that set.
    If active_target_pairs is given (set of (display_client, fold_ta)), only
    include (client, TA) combos present in the target list — this matches
    PBI's MBR scoping. Without it, TAs with activity on clients they're NOT
    targeted for (e.g. Alisa Liddell helping on Wolt Volume jobs while
    targeted only at Eucalyptus) would incorrectly bubble up under that
    non-target client. That's the root of Wolt Volume showing 9 hires that
    PBI shows as 0.
    """
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
        ta_fold = fold_name(rt)
        key = f"{disp}|{nt}"
        for wk in mbr_weeks:
            # Weekly-roster scoping: only count this (disp, TA) for weeks where
            # the TA is on Andy's weekly roster for that client. This matches
            # PBI — e.g. Mark Kandaurov is on Scorewarrior w13 only (dropped
            # w14-w16), so his w13 contacts count but w14-w16 don't.
            if weekly_active_pairs is not None:
                wk_num = int(wk[1:]) if isinstance(wk, str) and wk.startswith("w") else None
                roster = weekly_active_pairs.get(wk_num, set()) if wk_num is not None else set()
                if (disp, ta_fold) not in roster:
                    continue
            elif active_target_pairs is not None and (disp, ta_fold) not in active_target_pairs:
                continue
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
        if active_target_pairs is not None and (disp, fold_name(w)) not in active_target_pairs:
            continue
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

def load_weekly_summary(source_path=SNOW_WEEKLY_SUMMARY):
    """Return list of weekly_summary rows — the PBI 'Weekly Progress' port.
    Grain: dim_type (company/ta/ts/client) x dim_value x iso_year x iso_week,
    full funnel incl. viewed + reacted. Produced by weekly_summary.sql
    (transformation 01ksm8rz0qfrhgzekke65bkd28). Opt-in: returns [] if the CSV
    has not been staged yet so legacy/partial runs still work."""
    if not source_path.exists():
        return []
    rows = []
    with source_path.open() as f:
        for row in csv.DictReader(f):
            rows.append({
                "dim_type":          (row.get("DIM_TYPE") or "").strip(),
                "dim_value":         (row.get("DIM_VALUE") or "").strip(),
                "iso_year":          int(row.get("ISO_YEAR") or 0),
                "iso_week":          int(row.get("ISO_WEEK") or 0),
                "viewed":            int(row.get("VIEWED") or 0),
                "contacted":         int(row.get("CONTACTED") or 0),
                "reacted":           int(row.get("REACTED") or 0),
                "positive_response": int(row.get("POSITIVE_RESPONSE") or 0),
                "screens":           int(row.get("REC_SCREENS") or 0),
                "actual_screens":    int(row.get("ACTUAL_SCREENS") or 0),
                "ats":               int(row.get("ATS") or 0),
                "offered":           int(row.get("OFFERED") or 0),
                "hired":             int(row.get("HIRED") or 0),
            })
    return rows


def load_weekly_summary_byjob(source_path=SNOW_WEEKLY_SUMMARY_BYJOB):
    """Person x job drill companion to weekly_summary. Grain (dim_type ta/ts,
    person, job_id, client, job_title, iso week) + full funnel. Produced by the
    weekly_summary_byjob block (transformation 01ksm8rz0qfrhgzekke65bkd28).
    Opt-in: returns [] if not staged yet."""
    if not source_path.exists():
        return []
    rows = []
    with source_path.open() as f:
        for row in csv.DictReader(f):
            rows.append({
                "dim_type":          (row.get("DIM_TYPE") or "").strip(),
                "person":            (row.get("PERSON") or "").strip(),
                "job_id":            (row.get("JOB_ID") or "").strip(),
                "client":            (row.get("CLIENT") or "").strip(),
                "job_title":         (row.get("JOB_TITLE") or "").strip(),
                "iso_year":          int(row.get("ISO_YEAR") or 0),
                "iso_week":          int(row.get("ISO_WEEK") or 0),
                "viewed":            int(row.get("VIEWED") or 0),
                "contacted":         int(row.get("CONTACTED") or 0),
                "reacted":           int(row.get("REACTED") or 0),
                "positive_response": int(row.get("POSITIVE_RESPONSE") or 0),
                "screens":           int(row.get("REC_SCREENS") or 0),
                "actual_screens":    int(row.get("ACTUAL_SCREENS") or 0),
                "ats":               int(row.get("ATS") or 0),
                "offered":           int(row.get("OFFERED") or 0),
                "hired":             int(row.get("HIRED") or 0),
            })
    return rows


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

    # Build (display_client, fold_ta) set from target rows — scopes MBR to only
    # the (Client, TA) combinations Andy has in the target sheet, matching PBI.
    active_target_pairs = set()
    for t in (live.get("mbr_ta_targets") or []):
        tc = (t.get("client") or "").strip()
        disp_c = ABBREV.get(tc, tc)
        ta_f = fold_name(t.get("ta") or "")
        if disp_c and ta_f:
            active_target_pairs.add((disp_c, ta_f))

    # Weekly roster per-week: Andy's wbr_ta_weekly_note.csv → {wk_num: set((disp_c, fold_ta))}.
    # PBI scopes each MBR week by this list (not by the monthly target list), so
    # e.g. a TA who was active on Scorewarrior w13 but dropped off in w14-w16
    # gets counted for w13 only. Using ta_weekly_roster (already loaded above).
    weekly_active_pairs = {}
    for wk_key, pairs in (ta_weekly_roster or {}).items():
        if not (isinstance(wk_key, str) and wk_key.startswith("w")):
            continue
        try:
            wk_num = int(wk_key[1:])
        except ValueError:
            continue
        s_set = set()
        for pair in pairs:
            parts = pair.split("|", 1)
            if len(parts) != 2:
                continue
            client_raw = parts[0].strip()
            disp_c = mbr_normalize_client(client_raw)
            ta_f = fold_name(parts[1])
            if disp_c and ta_f:
                s_set.add((disp_c, ta_f))
        weekly_active_pairs[wk_num] = s_set

    mbr_ta_actuals = build_mbr_ta_actuals(raw_wbr, aux, mbr_wolt, mbr_weeks,
                                          ta_roster=ta_roster,
                                          active_target_pairs=active_target_pairs,
                                          weekly_active_pairs=weekly_active_pairs)

    # Override MBR Contacted with event-based counts (who_event_created_for)
    # Matches PBI DAX. Iterates (disp, fold_ta, year, week) entries and sums
    # only those within mbr_weeks and in the weekly roster for that week.
    ev_contacted = load_mbr_contacted_ev()
    if ev_contacted:
        mbr_week_nums = {int(w[1:]) for w in mbr_weeks if isinstance(w, str) and w.startswith("w")}
        # Zero out existing Contacted counts; we'll rebuild from event-based
        for key in list(mbr_ta_actuals.keys()):
            mbr_ta_actuals[key]["contacted"] = 0
        for raw_c, raw_ta, y, wk, n in ev_contacted:
            if y != 2026 or wk not in mbr_week_nums:
                continue
            norm_ta = norm_name(raw_ta)
            # Route Wolt/DoorDash/SevenRooms via the TA's sub-BU roster —
            # same logic as mbr_display_for used when building the table.
            disp = mbr_display_for(raw_c, norm_ta, mbr_wolt)
            fold_ta = fold_name(raw_ta)
            if weekly_active_pairs is not None:
                roster = weekly_active_pairs.get(wk, set())
                if (disp, fold_ta) not in roster:
                    continue
            key = f"{disp}|{norm_ta}"
            if key in mbr_ta_actuals:
                mbr_ta_actuals[key]["contacted"] += n
        print(f"  mbr Contacted overridden with event-based attribution")
    mbr_ts_actuals = build_mbr_ts_actuals(raw_ts, aux, mbr_weeks, ts_roster=ts_roster)
    mbr_client_totals = build_mbr_client_totals(mbr_ta_actuals)

    # Filter MBR output to clients Andy marks active. Without this, non-MBR
    # clients like DualEntry (which has some candidate activity) and Fever
    # (which has a dangling target row) leak into the MBR tables. Uses
    # live["mbr_active_clients"] as the authoritative list.
    active_clients = set(live.get("mbr_active_clients") or [])
    if active_clients:
        # Allow the long-form Wolt client names in targets to resolve against
        # the ABBREV-form active list (target rows arrive as e.g.
        # "Wolt North, Baltics & Benelux" but the active list is "Wolt NBB").
        def _client_is_active(c):
            if c in active_clients:
                return True
            abbrev = ABBREV.get((c or "").strip())
            return abbrev in active_clients if abbrev else False
        mbr_ta_actuals = {k: v for k, v in mbr_ta_actuals.items()
                          if k.split("|", 1)[0] in active_clients}
        mbr_client_totals = {k: v for k, v in mbr_client_totals.items()
                             if k in active_clients}
        # Scope the target list too — otherwise Fever/Grover dangling target
        # rows show up as empty-data rows in the MBR TA table.
        live_targets = live.get("mbr_ta_targets") or []
        filtered_targets = [t for t in live_targets if _client_is_active(t.get("client"))]
        live["mbr_ta_targets"] = filtered_targets
        print(f"  mbr filtered to {len(active_clients)} active clients "
              f"(targets {len(live_targets)} -> {len(filtered_targets)})")

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
    out["project_dashboard_eventattr"] = load_project_dashboard_eventattr()
    out["project_dashboard_hires"] = load_project_hires()
    out["weekly_summary"] = load_weekly_summary()
    out["weekly_summary_byjob"] = load_weekly_summary_byjob()
    if out["weekly_summary_byjob"]:
        print(f"  weekly_summary_byjob: {len(out['weekly_summary_byjob'])} rows")
    if out["weekly_summary"]:
        print(f"  weekly_summary: {len(out['weekly_summary'])} rows")
    if out["project_dashboard"]["rows"]:
        print(f"  project_dashboard: {len(out['project_dashboard']['rows'])} rows")
    if out["project_dashboard_eventattr"]["rows"]:
        print(f"  project_dashboard_eventattr: {len(out['project_dashboard_eventattr']['rows'])} rows")
    if out["project_dashboard_hires"]:
        print(f"  project_dashboard_hires: {len(out['project_dashboard_hires'])} hires")
    # Time to Hire tab — tth_jobs + tth_monthly from snowflake_tth_jobs.csv.
    # Opt-in: returns empty lists if the CSV is missing.
    _tth = load_tth_jobs()
    out["tth_jobs"] = _tth["tth_jobs"]
    out["tth_monthly"] = _tth["tth_monthly"]
    out["tth_summary"] = _tth["tth_summary"]
    if _tth["tth_jobs"]:
        print(f"  tth_jobs: {len(_tth['tth_jobs'])} jobs, {len(_tth['tth_monthly'])} monthly buckets")
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

    # ts_summary - KPI-TS Summary tab, per-sourcer x per-week (2026-04-27).
    # Loads gracefully if CSV missing (Flow may not have run yet).
    out["ts_summary"] = load_ts_summary()
    if out["ts_summary"]:
        print(f"  ts_summary: {len(out['ts_summary'])} (sourcer, year, week) rows")

    # Internal Recruiting tab (Phase 2a, 2026-05-01) — Bubble-only port of
    # Andy's PBI Internal Recruitment page. Tribe.xyz (IR) jobs only.
    # Phase 2b will overlay Ashby for the right side of the funnel.
    # Internal Recruiting tab data — load CSVs, but PRESERVE existing live
    # values if the CSV is missing/empty. Without this preservation, n8n's
    # nightly refresh wipes the tab to all-empty whenever Keboola doesn't
    # have the ir_* tables (the IR transformations were never wired up; see
    # legacy-pbix/.../project_ir_phase2a_shipped.md). Fixed 2026-05-04.
    def _ir_load(loader_fn, key):
        loaded = loader_fn()
        if loaded:
            return loaded
        existing = (live or {}).get(key) or []
        if existing:
            print(f"  WARN: {key} CSV missing/empty — preserving {len(existing)} entries from live JSON")
        return existing

    out["ir_funnel_jobweek"]      = _ir_load(load_ir_funnel_jobweek,      "ir_funnel_jobweek")
    out["ir_sourced_jobweek"]     = _ir_load(load_ir_sourced_jobweek,     "ir_sourced_jobweek")
    out["ir_interviewed_jobweek"] = _ir_load(load_ir_interviewed_jobweek, "ir_interviewed_jobweek")
    out["ir_dq_by_stage"]         = _ir_load(load_ir_dq_by_stage,         "ir_dq_by_stage")
    out["ir_jobs_active"]         = _ir_load(load_ir_jobs_active,         "ir_jobs_active")
    out["ir_dq_byjob_reason"]     = _ir_load(load_ir_dq_byjob_reason,     "ir_dq_byjob_reason")
    # Ashby-derived right side of the IR funnel (Phase 2b). Empty if extractor was skipped.
    out["ir_ashby_active_pipeline"] = _ir_load(load_ir_ashby_active_pipeline, "ir_ashby_active_pipeline")
    out["ir_ashby_dq_reasons"]      = _ir_load(load_ir_ashby_dq_reasons,      "ir_ashby_dq_reasons")
    out["ir_ashby_funnel_jobweek"]  = _ir_load(load_ir_ashby_funnel_jobweek,  "ir_ashby_funnel_jobweek")
    out["ir_ashby_hires"]           = _ir_load(load_ir_ashby_hires,           "ir_ashby_hires")
    out["ir_ashby_jobs_all"]        = _ir_load(load_ir_ashby_jobs_all,        "ir_ashby_jobs_all")
    if out["ir_funnel_jobweek"]:
        print(f"  ir_funnel_jobweek: {len(out['ir_funnel_jobweek'])} (job,week) rows  "
              f"sourced_jobweek: {len(out['ir_sourced_jobweek'])}  "
              f"interviewed_jobweek: {len(out['ir_interviewed_jobweek'])}  "
              f"dq_by_stage: {len(out['ir_dq_by_stage'])}  "
              f"jobs: {len(out['ir_jobs_active'])}  "
              f"dq_byjob_reason: {len(out['ir_dq_byjob_reason'])}")

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
