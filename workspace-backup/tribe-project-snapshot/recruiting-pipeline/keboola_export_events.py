#!/usr/bin/env python3
"""
Export Events from Keboola Storage API → JSON files for DuckDB transform.
=========================================================================
Instead of making ~27K Bubble API calls (expensive + slow), this script
downloads the Events table directly from Keboola's Snowflake backend.

The Keboola Bubble connector already syncs all 14.6M events. We just need
the 2025+ subset (~2.7M rows) for the recruiting dashboard.

Usage:
    # Set your Keboola Storage API token (find it in Keboola → Settings → API Tokens)
    export KEBOOLA_TOKEN='your-token-here'

    # Run the export
    python3 keboola_export_events.py

    # Then run the transform as usual
    python3 transform.py

The script exports month-by-month to bubble_Events_YYYYMM.json files,
matching the format produced by bubble_extract.py's windowed extraction.
Already-downloaded months are skipped (safe to re-run).
"""

import csv
import io
import json
import logging
import os
import sys
import time
from datetime import date, timedelta
from pathlib import Path

import requests

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

KEBOOLA_URL = "https://connection.eu-central-1.keboola.com"
KEBOOLA_TOKEN = os.environ.get("KEBOOLA_TOKEN", "")
TABLE_ID = "in.c-kds-team-ex-bubble-io-122527414.Events"
DATA_DIR = Path(os.environ.get("RECRUIT_DATA_DIR", Path(__file__).parent / "data"))

# Columns needed by transform.py (must match bubble_extract.py field list)
COLUMNS = [
    "bubbleinternal_id", "Candidate", "talent", "job", "event_type",
    "moved_to_stage", "who_event_created_for", "who_created_event",
    "replied", "recruiterScreen", "AI", "not_fit", "not_fit_reason",
    "Automation_flow", "Automation_step", "duxsoupMessage",
    "Nylas_email", "Content", "archived", "external_recruiter",
    "ats_creation_time", "new_role", "new_sub_role",
    "Ai_Search", "Created_Date", "Modified_Date",
]

START_DATE = "2025-01-01"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("keboola_export")


# ---------------------------------------------------------------------------
# Keboola Storage API helpers
# ---------------------------------------------------------------------------

def keboola_export_table(table_id: str, columns: list, where_col: str = None,
                         where_op: str = None, where_values: list = None,
                         limit: int = None) -> str:
    """
    Export a Keboola table as CSV using the synchronous export endpoint.
    Returns CSV string.
    """
    headers = {"X-StorageApi-Token": KEBOOLA_TOKEN}
    params = {
        "columns": ",".join(columns),
        "format": "rfc",
    }
    if where_col:
        params["whereColumn"] = where_col
    if where_op:
        params["whereOperator"] = where_op
    if where_values:
        for i, v in enumerate(where_values):
            params[f"whereValues[{i}]"] = v
    if limit:
        params["limit"] = limit

    url = f"{KEBOOLA_URL}/v2/storage/tables/{table_id}/export-async"
    log.info(f"  Requesting async export...")
    resp = requests.post(url, headers=headers, data=params)
    resp.raise_for_status()
    job = resp.json()
    job_id = job.get("id")

    # Poll for completion
    while True:
        resp = requests.get(f"{KEBOOLA_URL}/v2/storage/jobs/{job_id}", headers=headers)
        resp.raise_for_status()
        status = resp.json()
        if status.get("status") == "success":
            break
        elif status.get("status") == "error":
            raise RuntimeError(f"Export job failed: {status.get('error', {}).get('message', 'unknown')}")
        time.sleep(2)

    # Download the result
    file_info = status.get("results", {}).get("file", {})
    file_id = file_info.get("id")
    if not file_id:
        raise RuntimeError("No file ID in export results")

    resp = requests.get(f"{KEBOOLA_URL}/v2/storage/files/{file_id}", headers=headers)
    resp.raise_for_status()
    file_meta = resp.json()
    download_url = file_meta.get("url")

    log.info(f"  Downloading export file...")
    resp = requests.get(download_url)
    resp.raise_for_status()

    # Handle gzipped content
    if file_meta.get("isSliced"):
        raise RuntimeError("Sliced files not yet supported — try reducing export size")

    return resp.text


def keboola_sql_export(sql: str) -> str:
    """
    Run a SQL query via Keboola's SQL workspace (Snowflake) and return CSV.
    Uses the direct query endpoint.
    """
    headers = {
        "X-StorageApi-Token": KEBOOLA_TOKEN,
        "Content-Type": "application/json",
    }
    # Use Keboola's SQL query endpoint
    url = f"{KEBOOLA_URL}/v2/storage/branch/default/sql/query"
    payload = {"query": sql}

    resp = requests.post(url, headers=headers, json=payload)
    resp.raise_for_status()
    data = resp.json()

    # Response format: {"columns": [...], "rows": [[...], ...]}
    return data


# ---------------------------------------------------------------------------
# Main export logic
# ---------------------------------------------------------------------------

def csv_to_records(csv_text: str) -> list[dict]:
    """Convert CSV text to list of dicts matching bubble_extract.py format."""
    reader = csv.DictReader(io.StringIO(csv_text))
    records = []
    for row in reader:
        # Normalize empty strings to None (matching Bubble API behavior)
        normalized = {}
        for k, v in row.items():
            normalized[k] = v if v != "" else None
        records.append(normalized)
    return records


def export_events_monthly():
    """Export Events from Keboola month-by-month."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    # Build list of monthly windows
    start = date.fromisoformat(START_DATE)
    today = date.today()
    windows = []
    current = start.replace(day=1)
    while current <= today:
        next_month = (current.replace(day=28) + timedelta(days=4)).replace(day=1)
        windows.append((current.isoformat(), next_month.isoformat()))
        current = next_month

    total_records = 0
    log.info(f"Exporting Events from Keboola in {len(windows)} monthly windows...")

    for window_start, window_end in windows:
        label = window_start[:7]
        suffix = label.replace("-", "")
        out_path = DATA_DIR / f"bubble_Events_{suffix}.json"

        # Skip months already on disk (allows resuming)
        if out_path.exists():
            try:
                with open(out_path) as f:
                    existing = json.load(f)
                count = len(existing)
                total_records += count
                log.info(f"  Events [{label}]: skipping, {count:,} records already on disk")
                continue
            except (json.JSONDecodeError, Exception):
                pass

        log.info(f"  Events [{label}]: querying Keboola...")
        start_time = time.time()

        try:
            # Use SQL query endpoint for precise date filtering
            cols_quoted = ", ".join(f'"{c}"' for c in COLUMNS)
            sql = f"""
                SELECT {cols_quoted}
                FROM "KEBOOLA_855"."in.c-kds-team-ex-bubble-io-122527414"."Events"
                WHERE "Created_Date" >= '{window_start}T00:00:00.000Z'
                  AND "Created_Date" < '{window_end}T00:00:00.000Z'
            """

            result = keboola_sql_export(sql)
            columns = result.get("columns", [])
            rows = result.get("rows", [])

            # Convert to list of dicts
            records = []
            for row in rows:
                rec = {}
                for col_name, val in zip(columns, row):
                    rec[col_name] = val if val != "" else None
                records.append(rec)

        except Exception as e:
            log.warning(f"  SQL query failed ({e}), falling back to table export...")
            try:
                csv_text = keboola_export_table(
                    TABLE_ID,
                    columns=COLUMNS,
                    where_col="Created_Date",
                    where_op="ge",
                    where_values=[f"{window_start}T00:00:00.000Z"],
                )
                records = csv_to_records(csv_text)
                # Filter to window end
                records = [r for r in records
                          if r.get("Created_Date", "") < f"{window_end}T00:00:00.000Z"]
            except Exception as e2:
                log.error(f"  Events [{label}]: FAILED - {e2}")
                continue

        elapsed = time.time() - start_time
        total_records += len(records)
        log.info(f"  Events [{label}]: {len(records):,} records in {elapsed:.1f}s")

        # Save as JSON (matching bubble_extract.py format)
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(records, f, ensure_ascii=False, default=str)
        log.info(f"  Saved → {out_path.name}")

        # Free memory between windows
        del records

    log.info(f"Events TOTAL: {total_records:,} records across {len(windows)} months")
    return total_records


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    if not KEBOOLA_TOKEN:
        print("ERROR: Set KEBOOLA_TOKEN environment variable first!")
        print("  Find it in Keboola → Settings → API Tokens")
        print("  export KEBOOLA_TOKEN='your-token-here'")
        sys.exit(1)

    log.info("=" * 60)
    log.info("Keboola Events Export")
    log.info(f"  Table: {TABLE_ID}")
    log.info(f"  Date range: {START_DATE} → today")
    log.info(f"  Output: {DATA_DIR}")
    log.info("=" * 60)

    total = export_events_monthly()

    log.info("=" * 60)
    log.info(f"Done! {total:,} events exported to {DATA_DIR}")
    log.info("Next step: python3 transform.py")
