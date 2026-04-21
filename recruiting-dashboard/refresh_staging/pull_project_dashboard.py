"""pull_project_dashboard.py — download Project Dashboard tables from Keboola Storage API.

Fetches the two Project Dashboard output tables that the Snowflake transformations
produce and saves them as CSVs in refresh_staging/. Invoked by refresh_daily.py
at the top of the orchestration so the CSVs exist before render_json.py runs.

Why Storage API (not MCP query_data):
  The Project Dashboard SQL output is ~600KB of CSV. The Keboola MCP's query_data
  response is capped at ~56KB of tokens, so we can't fetch it directly. The
  Storage API has no such cap and streams the full table via a signed S3 URL.

Authentication:
  Reads the Keboola Storage API token from environment variable
  KEBOOLA_READONLY_TOKEN. The token only needs READ access to the two project
  dashboard output buckets:
    - out.c-Project-Dashboard---weekly-funnel
    - out.c-Project-Dashboard---hires-drill-down

  Generate a token in Keboola UI → Settings → API Tokens → "+ New Token".

Keboola transformations that produce these tables:
  - Project Dashboard - weekly funnel     (config 01kpqh9r7g2z66c8vvdr5d87xd)
  - Project Dashboard - hires drill-down  (config 01kpqharhz3seww52sms915216)

Data freshness:
  This script curls whatever is CURRENTLY in the Keboola output tables. The
  transformations do NOT run on this script's schedule — they run when someone
  clicks Run in Keboola OR when the transformations are added to the Keboola
  Flow's schedule (post-MVP migration). If the transformation hasn't been
  re-run recently, this pull returns the stale data. Accepted trade-off for v1.

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
    ("out.c-Project-Dashboard---hires-drill-down.project_dashboard_hires",
     HERE / "snowflake_project_dashboard_hires.csv"),
    ("out.c-MBR-Contacted---event-based-attribution.mbr_contacted_ev",
     HERE / "snowflake_mbr_contacted_ev.csv"),
]

log = logging.getLogger("pull_project_dashboard")


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
        _export_one(table_id, out_path, token)
    log.info("Pull complete in %.1fs", time.time() - t0)
    return 0


if __name__ == "__main__":
    sys.exit(main())
