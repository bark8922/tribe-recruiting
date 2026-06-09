"""pull_project_dashboard.py — download Snowflake-derived tables from Keboola Storage API.

Originally pulled the 2 Project Dashboard tables. As of 2026-04-21 (Phase 2 step 1)
it also pulls the 6 WBR/MBR weekly aggregation tables that previously came from
the scheduled-task prompt's MCP `query_data` calls — so the MCP-query step in the
prompt can be retired once this pull is proven stable.

Outputs (written alongside refresh_daily.py):
  - snowflake_project_dashboard.csv        (weekly funnel)
  - snowflake_project_dashboard_hires.csv  (hires drill-down)
  - snowflake_mbr_contacted_ev.csv         (event-based Contacted attribution)
  - snowflake_wbr.csv                      (wbr_weekly)
  - snowflake_wbr_jobs.csv                 (wbr_jobs_weekly)
  - snowflake_ts_jobs.csv                  (wbr_ts_jobs_weekly)
  - snowflake_ts.csv                       (ts_weekly)
  - snowflake_ts_conversion.csv            (ts_conversion)
  - snowflake_aux_12w.csv                  (aux_12w)

Why Storage API (not MCP query_data):
  Original reason (Project Dashboard only): project_dashboard.sql output is
  ~600KB of CSV. The Keboola MCP's query_data response is capped at ~56KB of
  tokens, so we couldn't fetch it directly. The Storage API has no such cap
  and streams the full table via a signed S3 URL.
  After Phase 2: even for the smaller WBR/MBR tables we prefer this path
  because the MCP-query step only runs when Cowork is open; the Keboola
  transformations run on their own Flow schedule, so the Storage API pull
  sees fresh data 3x/day regardless of laptop state.

Authentication:
  Reads the Keboola Storage API token from environment variable
  KEBOOLA_READONLY_TOKEN. The token needs READ access to these output buckets:
    - out.c-Project-Dashboard---weekly-funnel
    - out.c-Project-Dashboard---hires-drill-down
    - out.c-MBR-Contacted---event-based-attribution
    - out.c-WBRMBR-weekly-aggregations

  Generate a token in Keboola UI → Settings → API Tokens → "+ New Token".

Keboola transformations that produce these tables:
  - Project Dashboard - weekly funnel        (config 01kpqh9r7g2z66c8vvdr5d87xd)
  - Project Dashboard - hires drill-down     (config 01kpqharhz3seww52sms915216)
  - MBR Contacted event-based attribution    (config 01kpqxgczrvb92e95y6dh7zxmh)
  - WBR/MBR weekly aggregations              (config 01kpr0tr0dt5ryf96a5zk85bx7)

Data freshness:
  This script curls whatever is CURRENTLY in the Keboola output tables. The
  transformations run on the Keboola Flow's cron (40 14,20,8 Prague, 3x/day).
  If the transformation hasn't been re-run recently, this pull returns stale
  data. Accepted trade-off; Flow guarantees freshness within 6 hours.

Usage:
  export KEBOOLA_READONLY_TOKEN=855-NNNNN-...
  python3 pull_project_dashboard.py
"""
from __future__ import annotations

import gzip
import json
import logging
import os
import sys
import time
import urllib.parse
import urllib.request
from pathlib import Path

try:
    import boto3
except ImportError:
    print("ERROR: boto3 not installed. Run: pip install boto3 --break-system-packages")
    sys.exit(2)

BASE = "https://connection.eu-central-1.keboola.com/v2/storage"
HERE = Path(__file__).resolve().parent

TABLES = [
    ("out.c-Project-Dashboard---weekly-funnel.project_dashboard",
     HERE / "snowflake_project_dashboard.csv"),
    ("out.c-Project-Dashboard---event-attr.project_dashboard_eventattr",
     HERE / "snowflake_project_dashboard_eventattr.csv"),
    ("out.c-Project-Dashboard---hires-drill-down.project_dashboard_hires",
     HERE / "snowflake_project_dashboard_hires.csv"),
    ("out.c-MBR-Contacted---event-based-attribution.mbr_contacted_ev",
     HERE / "snowflake_mbr_contacted_ev.csv"),
    # WBR/MBR weekly aggregations (Phase 2, config 01kpr0tr0dt5ryf96a5zk85bx7)
    # Replaces the MCP query_data x6 step that previously ran in the Cowork
    # scheduled-task prompt. Keep output filenames identical to the CSV names
    # refresh_daily.py already expects — drop-in swap.
    ("out.c-WBRMBR-weekly-aggregations.wbr_weekly",
     HERE / "snowflake_wbr.csv"),
    ("out.c-WBRMBR-weekly-aggregations.wbr_jobs_weekly",
     HERE / "snowflake_wbr_jobs.csv"),
    ("out.c-WBRMBR-weekly-aggregations.wbr_ts_jobs_weekly",
     HERE / "snowflake_ts_jobs.csv"),
    ("out.c-WBRMBR-weekly-aggregations.ts_weekly",
     HERE / "snowflake_ts.csv"),
    ("out.c-WBRMBR-weekly-aggregations.ts_conversion",
     HERE / "snowflake_ts_conversion.csv"),
    ("out.c-WBRMBR-weekly-aggregations.aux_12w",
     HERE / "snowflake_aux_12w.csv"),
    # ts_summary_per_sourcer: KPI - TS Summary tab data source (added 2026-04-27, b0.c6).
    # Per-sourcer x per-week aggregate replicating Andy's PBI "KPI - TS Summary" page
    # filters (year, is_job_archived=False, test=False, who_created_event_first IN
    # Current_TS, client_name NOT IN test_clients). Validated 2026-04-27: 11/11
    # PBI sourcers within 10% drift vs snapshot data (2).xlsx.
    ("out.c-WBRMBR-weekly-aggregations.ts_summary_per_sourcer",
     HERE / "snowflake_ts_summary.csv"),
    # Internal Recruiting tab — Phase 2a + 2a.1 (2026-05-01). Bubble-only port of
    # Andy's PBI Internal Recruitment page with per-(job, week) granularity so
    # the frontend can filter by Job and Date window. SQL in refresh_staging/ir_*.sql.
    # Filter: client_name = 'Tribe.xyz (IR)' (inverts WBR/MBR exclusion).
    # Phase 2b will add Ashby data via separate ashby_extract.py + Keboola.
    ("out.c-WBRMBR-weekly-aggregations.ir_funnel_jobweek",
     HERE / "snowflake_ir_funnel_jobweek.csv"),
    ("out.c-WBRMBR-weekly-aggregations.ir_sourced_jobweek",
     HERE / "snowflake_ir_sourced_jobweek.csv"),
    ("out.c-WBRMBR-weekly-aggregations.ir_interviewed_jobweek",
     HERE / "snowflake_ir_interviewed_jobweek.csv"),
    ("out.c-WBRMBR-weekly-aggregations.ir_dq_by_stage",
     HERE / "snowflake_ir_dq_by_stage.csv"),
    ("out.c-WBRMBR-weekly-aggregations.ir_jobs_active",
     HERE / "snowflake_ir_jobs_active.csv"),
    ("out.c-WBRMBR-weekly-aggregations.ir_dq_byjob_reason",
     HERE / "snowflake_ir_dq_byjob_reason.csv"),
    # New Project Health tab (2026-06-09) — per-role KR2/KR3 for roles opened
    # <=45d. SQL in refresh_staging/new_project_health.sql.
    ("out.c-WBRMBR-weekly-aggregations.new_project_health",
     HERE / "snowflake_new_project_health.csv"),
    # Note: the Google Drive extractor (config 01kpr3tek8ezs48pg02e60jdpe) also
    # writes 5 sheet tabs to in.c-wbr-sheet.wbr_{ta_target,ta_weekly_note,
    # ts_weekly,ir,reasoning_guidance}. We intentionally do NOT pull those
    # here yet — sync_google_sheet.py (Step 1a in the scheduled task) still
    # runs and writes the same CSVs directly from the live sheet with no
    # Flow-schedule lag. Pulling from Keboola would overwrite fresh CSVs
    # with up-to-6h stale ones. Retire sync_google_sheet.py (and uncomment
    # these entries) when the Custom Python render component ships and the
    # Cowork task goes away.
]

log = logging.getLogger("pull_project_dashboard")

# Tables whose absence must NOT abort the whole pull. new_project_health is
# served from a committed snapshot CSV + render preserves it; the Keboola
# source table may not be wired on every environment yet (2026-06-09).
OPTIONAL_TABLES = {"out.c-WBRMBR-weekly-aggregations.new_project_health"}


def _get_token() -> str:
    tok = os.environ.get("KEBOOLA_READONLY_TOKEN")
    if not tok:
        raise RuntimeError(
            "KEBOOLA_READONLY_TOKEN env var not set. "
            "Generate a read-only token in Keboola UI → Settings → API Tokens, "
            "scoped to buckets: out.c-Project-Dashboard---weekly-funnel, "
            "out.c-Project-Dashboard---hires-drill-down. Then export it before "
            "running: export KEBOOLA_READONLY_TOKEN=855-..."
        )
    return tok


def _req(method: str, url: str, token: str) -> dict:
    req = urllib.request.Request(url, method=method, headers={"X-StorageApi-Token": token})
    with urllib.request.urlopen(req, timeout=60) as resp:
        return json.loads(resp.read())


def _export_one(table_id: str, out_path: Path, token: str) -> None:
    log.info("export %s", table_id)
    job = _req("POST", f"{BASE}/tables/{table_id}/export-async", token)
    job_id = job["id"]

    # Poll async export job
    for _ in range(120):
        s = _req("GET", f"{BASE}/jobs/{job_id}", token)
        if s["status"] == "success":
            file_id = s["results"]["file"]["id"]
            break
        if s["status"] == "error":
            raise RuntimeError(f"export job {job_id} errored: {s}")
        time.sleep(1)
    else:
        raise TimeoutError(f"export job {job_id} did not finish within 120s")

    # Fetch file metadata with federation token (gives temporary AWS creds for S3)
    finfo = _req("GET", f"{BASE}/files/{file_id}?federationToken=1", token)
    creds = finfo["credentials"]
    s3 = boto3.client(
        "s3",
        aws_access_key_id=creds["AccessKeyId"],
        aws_secret_access_key=creds["SecretAccessKey"],
        aws_session_token=creds["SessionToken"],
        region_name=finfo["region"],
    )

    # Sliced exports: download the manifest, then each slice
    with urllib.request.urlopen(finfo["url"], timeout=60) as r:
        manifest = json.loads(r.read())
    slices = manifest.get("entries", [])

    # Build CSV (sliced exports omit the header; fetch column names from table metadata)
    tinfo = _req("GET", f"{BASE}/tables/{table_id}", token)
    header = ",".join(tinfo["columns"])
    parts = [header.encode() + b"\n"]

    for entry in slices:
        s3_url = entry["url"]
        assert s3_url.startswith("s3://"), s3_url
        bucket, _, key = s3_url[5:].partition("/")
        obj = s3.get_object(Bucket=bucket, Key=key)
        data = obj["Body"].read()
        if data[:2] == b"\x1f\x8b":
            data = gzip.decompress(data)
        parts.append(data)

    out_path.write_bytes(b"".join(parts))
    lines = sum(1 for _ in out_path.open())
    log.info("  wrote %s: %d lines (%d KB)",
             out_path.name, lines, out_path.stat().st_size // 1024)


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    try:
        token = _get_token()
    except RuntimeError as e:
        log.error("%s", e)
        return 1

    t0 = time.time()
    for table_id, out_path in TABLES:
        try:
            _export_one(table_id, out_path, token)
        except Exception as e:
            if table_id in OPTIONAL_TABLES:
                log.warning("optional table %s unavailable (%s); keeping existing CSV", table_id, e)
                continue
            raise
    log.info("Pull complete in %.1fs", time.time() - t0)
    return 0


if __name__ == "__main__":
    sys.exit(main())
