"""render_json.py — Assemble dashboard_data.json from Snowflake CSV outputs.

Reads 4 Snowflake query outputs + the current dashboard_data.json (for static
fields) and emits `rendered_dashboard_data.json` in the exact shape App.jsx
consumes.

Inputs (under refresh_staging/):
  snowflake_wbr.csv              raw per-week WBR grid (2026-01..2026-w15)
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
import json
import os
import re
import unicodedata
from collections import defaultdict
from pathlib import Path

HERE = Path(__file__).resolve().parent
# ROOT = the workspace folder (Recruiting Dashboard). When this script lives at
# <workspace>/refresh_staging/render_json.py, HERE.parent == <workspace>.
# Portable across Cowork sessions (session IDs change; mount folder doesn't).
ROOT = HERE.parent
LIVE_JSON = ROOT / "dashboard_data.json"
OUT_JSON = HERE / "rendered_dashboard_data.json"

SNOW_WBR = HERE / "snowflake_wbr.csv"
SNOW_TS = HERE / "snowflake_ts.csv"
SNOW_TS_CONV = HERE / "snowflake_ts_conversion.csv"
SNOW_AUX = HERE / "snowflake_aux_12w.csv"

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
    """Load TA roster + WBR Wolt sub-BU mapping from wbr_ta_target.csv.

    Returns (ta_roster, wbr_wolt_roster):
      ta_roster: set of fold_name(TA) for all 2026 rows. Uses diacritics-folded
        names so 'Dora Vrbanić' (Snowflake) matches 'Dora Vrbanic' (CSV).
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
            if wn < 1 or wn > 16:
                continue
            wk = f"w{wn}"
            c = row["CLIENT"]
            t = row["TA"]  # keep raw (preserves double-space, trailing space)
            for m in ("contacted", "screened", "actual_screens", "ats", "offers", "hires"):
                raw[(c, t)][wk][m] += int(row[m.upper()])
    return raw


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


def load_ts_conversion():
    """Return list of per-TS rows from snowflake_ts_conversion.csv."""
    rows = []
    with SNOW_TS_CONV.open() as f:
        for row in csv.DictReader(f):
            rows.append({
                "ts": row["ts"],
                "active_pipelines": int(row["active_pipelines"]),
                "contacted": int(row["contacted"]),
                "positive_response": int(row["positive_response"]),
                "recruiter_screens": int(row["recruiter_screens"]),
                "actual_screens": int(row["actual_screens"]),
                "ats": int(row["ats"]),
            })
    return rows


def load_aux():
    """Return {(role, metric, client, who): val} from snowflake_aux_12w.csv.
    Keeps raw whitespace in client/who (matches live hires_12w keys)."""
    out = {}
    with SNOW_AUX.open() as f:
        for row in csv.DictReader(f):
            key = (row["role"], row["metric"], row["client"], row["who"])
            out[key] = int(float(row["val"]))
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


def build_weekly_trend(raw_wbr, min_week: int = 2, max_week: int = 16):
    """Sum all (client, TA) cells per ISO week → list of {week, contacted, screened, ats, offers, hires}.
    screened here follows App.jsx fallback: actual_screens || screened.
    Week range: w2..w16 to match live dashboard (live drops near-empty w1,
    keeps current partial w16). Adjust if cadence changes."""
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

    print(f"  TA roster: {len(ta_roster) if ta_roster else 'UNFILTERED'} TAs from CSV")
    print(f"  TS roster: {len(ts_roster)} TSes from CSV")
    print(f"  WBR Wolt sub-BUs: {len(wbr_wolt)} TAs mapped")
    print(f"  TA weekly roster: {len(ta_weekly_roster)} weeks")
    print(f"  TS weekly roster: {len(ts_weekly_roster)} weeks")

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

    # ts_conversion — emit in the same shape App.jsx consumes (list of dicts,
    # with contacted/recruiter_screens kept as real values now that SQL is
    # scoped to Active Pipelines per Andy's rule).
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

    # Assemble output. Start from a COPY of live so any static field we don't
    # explicitly touch is preserved verbatim (roles, jobs, ts_jobs, targets,
    # ts_weekly, ta_weekly_notes, mbr_ta_targets, mbr_window, etc.)
    out = dict(live)  # shallow copy

    out["wbr_actuals"] = wbr_actuals
    out["weekly_trend"] = weekly_trend
    out["ts_actuals"] = ts_actuals
    out["ts_conversion"] = ts_conversion
    # ts_positive_responses intentionally NOT overwritten — live has unscoped PR
    # values (credit-only, no AP filter) that our scoped SQL can't reproduce.
    # Preserved from live via the dict(live) shallow copy. See build_ts_positive_responses docstring.
    out["hires_12w"] = hires_12w
    out["ta_ats_12w"] = ta_ats_12w
    out["ta_screens_12w"] = ta_screens_12w
    out["ta_ttf_12w"] = ta_ttf_12w
    out["ta_jobs_60d"] = ta_jobs_60d
    out["ts_hires_12w"] = ts_hires_12w
    out["ts_ats_12w"] = ts_ats_12w
    out["ts_screens_12w"] = ts_screens_12w
    out["mbr_ta_actuals"] = mbr_ta_actuals
    out["mbr_ts_actuals"] = mbr_ts_actuals
    out["mbr_client_totals"] = mbr_client_totals
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
