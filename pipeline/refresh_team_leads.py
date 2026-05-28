"""
refresh_team_leads.py — derive the WBR team-lead filter map from BambooHR.

What it does
------------
1. Pulls the current BambooHR employee directory (active employees + their
   supervisor field).
2. Reads `recruiting-dashboard/src/dashboard_data_snowflake.json` to discover
   which TA / TS names actually appear in the WBR funnel.
3. Builds a mapping: { team_lead_name -> [direct_report_dashboard_names] }
   - "Direct reports" = strict, current Bamboo supervisor only (no transitive).
   - A name in the funnel only counts if it can be matched to a Bamboo active
     employee via exact-after-diacritic-fold OR full last+first fold.
     The fragile "first-name-only / last-name-only unique" fallback is
     intentionally NOT applied — it produced false matches in the audit
     (e.g. Aleksandra Markovic -> Aleksandra Vistac).
4. Writes `recruiting-dashboard/src/team_leads.json` for the dashboard to
   import at build time.

Output shape (team_leads.json)
------------------------------
    {
        "generated_at": "2026-05-28T12:34:56Z",
        "source": "bamboohr_directory",
        "leads": [
            {"name": "Chené Elliot", "reports": ["Adelya Khakimova", ...]},
            ...
        ],
        "unmatched_funnel_names": ["Adis Prepoljac", ...],
        "name_overrides_applied": {"Lejla Silva": "Lejla Dizdarevic", ...}
    }

The dashboard renders the dropdown from `leads` (sorted), and uses each
`reports` list to filter the TA Weekly Detail, TS Weekly, and TS Conversion
Rate sections. `unmatched_funnel_names` are intentionally excluded from any
team filter — they only show in "All teams".

Manual overrides
----------------
NAME_OVERRIDES below pins fuzzy matches that need human judgment (married
names, etc.). Add a new entry whenever a dashboard name should be force-mapped
to a specific Bamboo displayName.

Environment variables
---------------------
    BAMBOOHR_SUBDOMAIN   e.g. "tribexyz" (required)
    BAMBOOHR_API_KEY     the API key      (required)
    REPO_ROOT            optional override; defaults to script's repo root

CLI
---
    python -m pipeline.refresh_team_leads
"""

from __future__ import annotations

import json
import os
import sys
import unicodedata
from datetime import datetime, timezone
from pathlib import Path

# Import the shared client (same one used by the finance dashboard).
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from pipeline.clients.bamboohr import BambooHRClient  # noqa: E402


# Manual name overrides for cases the matcher can't resolve safely.
# Key = name as it appears in dashboard_data_snowflake.json
# Value = exact Bamboo displayName to map to.
NAME_OVERRIDES: dict[str, str] = {
    # Confirmed by Blake during 2026-05-28 audit:
    "Lejla Silva": "Lejla Dizdarevic",
    "Tina Aramouni": "Tina Abdul-Karim",
    "Maria Desiree Gerbore": "Maria Gerbore",
    "Valeriia Yurykova": "Valeriia Yurikova",
}


def norm(s: str) -> str:
    """Normalize a name: trim, collapse whitespace, NFKD-fold diacritics, lowercase."""
    if not s:
        return ""
    s = " ".join(s.strip().split())
    s = unicodedata.normalize("NFKD", s)
    s = "".join(c for c in s if not unicodedata.combining(c))
    return s.lower()


def extract_funnel_names(dashboard_data: dict) -> tuple[set[str], set[str]]:
    """Return (ta_names, ts_names) appearing in WBR funnel data."""
    ta_names: set[str] = set()
    for key in dashboard_data.get("wbr_actuals", {}).keys():
        parts = key.split("|", 1)
        if len(parts) == 2:
            ta_names.add(parts[1].strip())
    ts_names = {n.strip() for n in dashboard_data.get("ts_actuals", {}).keys()}
    return ta_names, ts_names


def build_bamboo_indexes(employees: list[dict]) -> tuple[dict, dict, dict]:
    """Build lookup tables: by normalized full name, by last name, by first name."""
    by_norm: dict[str, dict] = {}
    by_last: dict[str, list[dict]] = {}
    by_first: dict[str, list[dict]] = {}
    for e in employees:
        name = e.get("displayName", "")
        n = norm(name)
        if not n:
            continue
        by_norm[n] = e
        parts = n.split()
        if len(parts) >= 2:
            by_last.setdefault(parts[-1], []).append(e)
            by_first.setdefault(parts[0], []).append(e)
    return by_norm, by_last, by_first


def match_name(
    dashboard_name: str,
    by_norm: dict,
    by_last: dict,
    by_first: dict,
) -> dict | None:
    """Return the Bamboo employee dict, or None if no safe match.

    Match order (strict only — no first-name-only or last-name-only fallback):
        1. NAME_OVERRIDES manual mapping
        2. Exact match after diacritic + whitespace fold
        3. Both first + last name match after fold (handles middle-name drops
           like "Maria Desiree Gerbore" -> "Maria Gerbore")
    """
    if dashboard_name in NAME_OVERRIDES:
        target = norm(NAME_OVERRIDES[dashboard_name])
        return by_norm.get(target)

    n = norm(dashboard_name)
    if n in by_norm:
        return by_norm[n]

    parts = n.split()
    if len(parts) >= 2:
        first, last = parts[0], parts[-1]
        for cand in by_last.get(last, []):
            cand_parts = norm(cand["displayName"]).split()
            if cand_parts and cand_parts[0] == first:
                return cand
        # Also check first-name bucket in case the matched-first record's
        # last name differs only by middle-name drop. We require last name
        # to be a substring match either way.
        for cand in by_first.get(first, []):
            cand_parts = norm(cand["displayName"]).split()
            if cand_parts and (cand_parts[-1] == last or last in cand_parts):
                return cand

    return None


def derive_team_leads(
    funnel_ta: set[str],
    funnel_ts: set[str],
    employees: list[dict],
) -> tuple[dict[str, list[str]], list[str]]:
    """Build {supervisor_name -> [dashboard_names_of_direct_reports]}.

    Only includes direct reports that appear in the WBR funnel data (so leads
    with no IC reports in the funnel are automatically excluded — e.g. the
    list naturally limits to people whose team is actually visible on WBR).

    Returns (leads_map, unmatched_names).
    """
    by_norm, by_last, by_first = build_bamboo_indexes(employees)

    leads_map: dict[str, list[str]] = {}
    unmatched: list[str] = []

    for dashboard_name in sorted(funnel_ta | funnel_ts):
        emp = match_name(dashboard_name, by_norm, by_last, by_first)
        if emp is None:
            unmatched.append(dashboard_name)
            continue
        supervisor = (emp.get("supervisor") or "").strip()
        if not supervisor:
            # No supervisor on file — drop from any team view.
            continue
        leads_map.setdefault(supervisor, []).append(dashboard_name)

    # Sort each report list for stable output.
    for lead in leads_map:
        leads_map[lead].sort()

    return leads_map, unmatched


def main() -> int:
    repo_root = Path(os.environ.get("REPO_ROOT") or Path(__file__).resolve().parents[1])
    data_path = repo_root / "recruiting-dashboard" / "src" / "dashboard_data_snowflake.json"
    out_path = repo_root / "recruiting-dashboard" / "src" / "team_leads.json"

    if not data_path.exists():
        print(f"FAIL: dashboard data not found at {data_path}", file=sys.stderr)
        return 1

    print(f"Reading dashboard data from {data_path}")
    with data_path.open() as f:
        dashboard_data = json.load(f)
    funnel_ta, funnel_ts = extract_funnel_names(dashboard_data)
    print(f"  funnel TAs: {len(funnel_ta)}, TSs: {len(funnel_ts)} "
          f"({len(funnel_ta | funnel_ts)} unique)")

    print("Pulling BambooHR employee directory...")
    client = BambooHRClient()  # reads env vars
    dir_data = client.get_employee_directory()
    employees = dir_data.get("employees", [])
    print(f"  pulled {len(employees)} active employees from Bamboo")

    if len(employees) < 30:
        print(f"FAIL: only {len(employees)} employees returned (expected >=30) — "
              f"refusing to write team_leads.json", file=sys.stderr)
        return 2

    leads_map, unmatched = derive_team_leads(funnel_ta, funnel_ts, employees)

    leads_list = [
        {"name": lead, "reports": reports}
        for lead, reports in sorted(leads_map.items(), key=lambda kv: kv[0].lower())
    ]

    output = {
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "source": "bamboohr_directory",
        "leads": leads_list,
        "unmatched_funnel_names": sorted(unmatched),
        "name_overrides_applied": {
            k: v for k, v in NAME_OVERRIDES.items()
            if k in (funnel_ta | funnel_ts)
        },
    }

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(output, indent=2, ensure_ascii=False),
                        encoding="utf-8")

    print(f"Wrote {out_path}")
    print(f"  leads: {len(leads_list)}")
    for entry in leads_list:
        print(f"    {entry['name']:<32} ({len(entry['reports'])} reports)")
    print(f"  unmatched (dropped from all team views): {len(unmatched)}")
    for u in unmatched:
        print(f"    - {u}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
