#!/usr/bin/env python3
"""
Bubble.io Data Extractor for Tribe Recruiting Dashboard
=========================================================
Pulls data from Bubble's Data API, handles cursor-based pagination,
and saves to Parquet files for DuckDB transformation.

API: https://overview.tribe.xyz/api/1.1/obj/<type>
Auth: Bearer token
Pagination: cursor-based (remaining + cursor fields)

Usage:
    python bubble_extract.py --full          # Full extraction (all endpoints)
    python bubble_extract.py --incremental   # Incremental (only recent changes)
    python bubble_extract.py --endpoint Jobs  # Single endpoint
"""

import os
import sys
import json
import time
import asyncio
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import aiohttp

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

API_BASE = "https://overview.tribe.xyz/api/1.1/obj"
API_TOKEN = os.environ.get("BUBBLE_API_TOKEN", "")
DATA_DIR = Path(os.environ.get("RECRUIT_DATA_DIR", Path(__file__).parent / "data"))
PAGE_LIMIT = 100  # Bubble max per request
MAX_CONCURRENT = 3  # Be nice to Bubble's API
RATE_DELAY = 0.3  # seconds between requests per endpoint
DATE_FILTER_2025 = "2025-01-01T00:00:00.000Z"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("bubble_extract")

# ---------------------------------------------------------------------------
# Endpoint definitions
# ---------------------------------------------------------------------------
# Each endpoint specifies:
#   - name: Bubble type name
#   - key: field used as primary key (for dedup)
#   - date_field: field to filter on for incremental loads
#   - mode: "full" or "incremental"
#   - fields: list of fields to extract (None = all fields)
#   - date_filter: whether to apply 2025+ filter on full loads

# CORE tables (large, incremental-capable)
INCREMENTAL_ENDPOINTS = [
    {
        "name": "Candidate",
        "key": "_id",
        "date_field": "Modified Date",
        "fields": [
            "_id", "Stage", "Talent", "Job", "sourcer", "disqualified",
            "first_name", "last_name", "atsID", "reason_not_interested",
            "Sourcedsource", "hired_salary_euro", "hired_salary",
            "hired_currency", "archived", "linkedin", "Created Date",
            "Modified Date",
        ],
    },
    {
        "name": "Talent",
        "key": "_id",
        "date_field": "Modified Date",
        "fields": [
            "_id", "Full name", "companyName", "current_company_name",
            "currentTitle", "Email", "linkedin", "location_address",
            "LinkedinMainID", "linkedin_nick", "Created Date", "Modified Date",
        ],
    },
    {
        "name": "Events",
        "key": "_id",
        "date_field": "Modified Date",
        "fields": [
            "_id", "Candidate", "talent", "job", "event_type",
            "moved_to_stage", "who_event_created_for", "who_created_event",
            "replied", "recruiterScreen", "AI", "not_fit", "not_fit_reason",
            "Automation_flow", "Automation_step", "duxsoupMessage",
            "Nylas_email", "Content", "archived", "external_recruiter",
            "ats_creation_time", "new_role", "new_sub_role",
            "Ai_Search", "Created Date", "Modified Date",
        ],
    },
    {
        "name": "Emails",
        "key": "_id",
        "date_field": "Modified Date",
        "fields": ["_id", "email", "talent", "count", "Created Date", "Modified Date"],
    },
    {
        "name": "Company",
        "key": "_id",
        "date_field": "Modified Date",
        "fields": [
            "_id", "Name", "CompanyWebsite", "client", "jobs", "users",
            "archived", "test", "ats", "Created Date", "Modified Date",
        ],
    },
    {
        "name": "Position",
        "key": "_id",
        "date_field": "Modified Date",
        "fields": [
            "_id", "Job_title", "Talent", "Company", "Worked_from",
            "Worked_to", "Created Date", "Modified Date",
        ],
    },
    {
        "name": "Nylas_Email_message",
        "key": "_id",
        "date_field": "Modified Date",
        "fields": ["_id", "A_B_Id", "version", "Read", "Created Date", "Modified Date"],
    },
    {
        "name": "duxsoup_messages",
        "key": "_id",
        "date_field": "Modified Date",
        "fields": ["_id", "A_B_id", "version", "Created Date", "Modified Date"],
    },
    {
        "name": "Analytic",
        "key": "_id",
        "date_field": "Modified Date",
        "fields": ["_id", "page", "user", "Created Date", "Modified Date"],
    },
    {
        "name": "stages",
        "key": "_id",
        "date_field": "Modified Date",
        "fields": [
            "_id", "Company", "Point_of_process", "showDashboard",
            "stageName", "stagesType", "atsID", "clientID",
            "Created Date", "Modified Date",
        ],
    },
    {
        "name": "recruiter_screeen_notes",
        "key": "_id",
        "date_field": "Modified Date",
        "fields": [
            "_id", "candidate", "Current_salary", "Current_salary_currency",
            "desired_salary", "Desired_salary_currency", "job", "Languages",
            "rating", "tech_stack", "visa_dropdown", "Location", "recruiter",
            "relocation_dropdown", "Salary_type", "start_date_dropdown",
            "Created Date", "Modified Date",
        ],
    },
]

# LOOKUP tables (small, always full load)
FULL_LOAD_ENDPOINTS = [
    {"name": "Jobs", "key": "_id", "fields": [
        "_id", "Company", "Title", "HiringManager", "Location_address",
        "recruiter_responsible", "sourcer_responsible", "stages", "priority",
        "archived", "atsID", "executive_search", "external_recruiter",
        "test", "campaign_inREPLY", "sub_category",
        "Created Date", "Modified Date",
    ]},
    {"name": "HiringManager", "key": "_id", "fields": [
        "_id", "Name", "Title", "Company", "Email", "archived",
    ]},
    {"name": "User", "key": "_id", "fields": [
        "_id", "First Name", "Last Name", "full_name", "Recruiter",
        "Role within platform", "Employee number", "archived",
        "authentication_email_email",
    ]},
    {"name": "stagesType", "key": "_id", "fields": [
        "_id", "point_of_process", "showInDashboard", "stage_type_name",
    ]},
    {"name": "atsOptions", "key": "_id", "fields": ["_id", "name"]},
    {"name": "RoleWithinPlatform", "key": "_id", "fields": ["_id", "Name", "Number", "Internal"]},
    {"name": "EventType", "key": "_id", "fields": ["_id", "slug", "name", "classification"]},
    {"name": "ReasonNotInterested", "key": "_id", "fields": ["_id", "name", "number"]},
    {"name": "Salary_currency", "key": "_id", "fields": ["_id", "Name"]},
    {"name": "Language_talent", "key": "_id", "fields": ["_id", "Language_level", "Language_name"]},
    {"name": "Languages", "key": "_id", "fields": ["_id", "Name"]},
    {"name": "Languages_levels", "key": "_id", "fields": ["_id", "Name"]},
    {"name": "recruiter_screen_relocation_dropdown", "key": "_id", "fields": ["_id", "name"]},
    {"name": "SalaryType", "key": "_id", "fields": ["_id", "Name"]},
    {"name": "recruiter_screen_notice_period_dropdown", "key": "_id", "fields": ["_id", "name"]},
    {"name": "TechStack", "key": "_id", "fields": ["_id", "Name", "TechStackType"]},
    {"name": "TechStackType", "key": "_id", "fields": ["_id", "Name"]},
    {"name": "recruiter_screen_visa_dropdown", "key": "_id", "fields": ["_id", "name"]},
    {"name": "Sub_conditional", "key": "_id", "fields": ["_id", "Name", "parent_conditional", "Conditional"]},
    {"name": "Automationflow", "key": "_id", "fields": [
        "_id", "Steps", "Schedule", "Name", "Job", "Enabled",
        "Conditional", "company",
    ]},
    {"name": "Automationstep", "key": "_id", "fields": [
        "_id", "Type", "Delay_days", "Delay_hours", "order_number",
        "automation_flow", "Sub_conditional",
    ]},
    {"name": "job_category", "key": "_id", "fields": ["_id", "name"]},
    {"name": "Job_sub_category", "key": "_id", "fields": ["_id", "name", "Job_category"]},
    {"name": "Sourced_source", "key": "_id", "fields": ["_id", "Name", "order_number"]},
    {"name": "Goals", "key": "_id", "fields": [
        "_id", "Job", "date_range", "Goal_number", "type",
        "Created Date", "Modified Date",
    ]},
    {"name": "Conditional", "key": "_id", "fields": [
        "_id", "main", "delay", "automation_flow", "type",
        "sub_conditionals", "archived",
    ]},
    {"name": "Roles", "key": "_id", "fields": ["_id", "name"]},
    {"name": "sub_roles", "key": "_id", "fields": ["_id", "name", "order"]},
    {"name": "bd_crunchbase", "key": "_id", "fields": [
        "_id", "company", "Company_Type", "Organization_Name",
        "Estimated_Revenue_Range", "Funding_Status",
        "Last_Equity_Funding_Amount_Currency_in_USD",
        "Last_Funding_Amount_Currency_in_USD",
        "Total_Funding_Amount_Currency_in_USD",
        "Last_Funding_Date", "Number_of_Funding_Rounds",
        "Website", "LinkedIn", "Organization_Name_URL",
        "Created Date",
    ]},
]


# ---------------------------------------------------------------------------
# Bubble API Client
# ---------------------------------------------------------------------------

class BubbleClient:
    """Async client for Bubble.io Data API with cursor-based pagination."""

    def __init__(self, token: str, base_url: str = API_BASE):
        self.token = token
        self.base_url = base_url
        self._session: Optional[aiohttp.ClientSession] = None
        self._semaphore = asyncio.Semaphore(MAX_CONCURRENT)

    async def __aenter__(self):
        self._session = aiohttp.ClientSession(
            headers={"Authorization": f"Bearer {self.token}"},
            timeout=aiohttp.ClientTimeout(total=120),
        )
        return self

    async def __aexit__(self, *args):
        if self._session:
            await self._session.close()

    async def fetch_all(
        self,
        type_name: str,
        constraints: list | None = None,
        sort_field: str = "Created Date",
        sort_descending: bool = False,
    ) -> list[dict]:
        """Fetch all records for a Bubble type with cursor pagination."""
        all_records = []
        cursor = 0

        while True:
            async with self._semaphore:
                params = {
                    "limit": PAGE_LIMIT,
                    "cursor": cursor,
                    "sort_field": sort_field,
                    "descending": str(sort_descending).lower(),
                }
                if constraints:
                    params["constraints"] = json.dumps(constraints)

                url = f"{self.base_url}/{type_name}"
                try:
                    async with self._session.get(url, params=params) as resp:
                        if resp.status == 429:
                            # Rate limited — wait and retry
                            retry_after = int(resp.headers.get("Retry-After", 5))
                            log.warning(f"Rate limited on {type_name}, waiting {retry_after}s")
                            await asyncio.sleep(retry_after)
                            continue
                        resp.raise_for_status()
                        data = await resp.json()
                except aiohttp.ClientError as e:
                    log.error(f"Error fetching {type_name} at cursor={cursor}: {e}")
                    raise

                response = data.get("response", {})
                results = response.get("results", [])
                remaining = response.get("remaining", 0)

                if not results:
                    break

                # Normalize Bubble field names: replace spaces with underscores
                for rec in results:
                    normalized = {}
                    for k, v in rec.items():
                        # Bubble uses "_id" internally but "Created Date" with spaces
                        key = k.replace(" ", "_") if " " in k else k
                        # Also map "bubbleinternal_id" from _id
                        normalized[key] = v
                    # Ensure bubbleinternal_id is set (Bubble's _id)
                    if "_id" in rec:
                        normalized["bubbleinternal_id"] = rec["_id"]
                    all_records.append(normalized)

                log.info(
                    f"  {type_name}: fetched {len(all_records)} "
                    f"(+{len(results)}, {remaining} remaining)"
                )

                if remaining <= 0:
                    break

                cursor += len(results)
                await asyncio.sleep(RATE_DELAY)

        return all_records


# ---------------------------------------------------------------------------
# Extraction Logic
# ---------------------------------------------------------------------------

def save_records(records: list[dict], type_name: str, data_dir: Path):
    """Save records as JSON (line-delimited for easy loading into DuckDB)."""
    data_dir.mkdir(parents=True, exist_ok=True)
    out_path = data_dir / f"bubble_{type_name}.json"

    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, default=str)

    log.info(f"  Saved {len(records)} records → {out_path.name}")
    return out_path


def load_existing(type_name: str, data_dir: Path) -> list[dict]:
    """Load previously extracted records for merge."""
    path = data_dir / f"bubble_{type_name}.json"
    if path.exists():
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    return []


def merge_records(existing: list[dict], new: list[dict], key: str = "bubbleinternal_id") -> list[dict]:
    """Merge new records into existing, replacing by key."""
    by_key = {r.get(key): r for r in existing}
    for r in new:
        by_key[r.get(key)] = r
    return list(by_key.values())


async def extract_endpoint(
    client: BubbleClient,
    endpoint: dict,
    mode: str,  # "full" or "incremental"
    data_dir: Path,
    since: Optional[str] = None,
):
    """Extract a single endpoint."""
    type_name = endpoint["name"]
    constraints = []

    if mode == "incremental" and since:
        # Only fetch records modified since last run
        constraints.append({
            "key": endpoint.get("date_field", "Modified Date"),
            "constraint_type": "greater than",
            "value": since,
        })
    elif mode == "full":
        # For large tables, filter to 2025+ on full loads.
        # Company (444K+), Nylas_Email_message (551K+), duxsoup_messages
        # are also huge — must be date-filtered.
        #
        # Use "Modified Date" for Candidate/Talent/recruiter_screeen_notes
        # so we capture older records still active in the pipeline.
        # Use "Created Date" for Events/Emails/etc where we only want
        # recent activity data.
        if type_name in ("Candidate", "Talent", "recruiter_screeen_notes"):
            constraints.append({
                "key": "Modified Date",
                "constraint_type": "greater than",
                "value": DATE_FILTER_2025,
            })
        elif type_name in ("Events", "Emails", "Position", "Analytic",
                           "Company", "Nylas_Email_message", "duxsoup_messages",
                           "stages"):
            constraints.append({
                "key": "Created Date",
                "constraint_type": "greater than",
                "value": DATE_FILTER_2025,
            })

    log.info(f"Extracting {type_name} ({mode})...")
    start = time.time()

    records = await client.fetch_all(type_name, constraints=constraints if constraints else None)

    if mode == "incremental" and records:
        # Merge with existing data
        existing = load_existing(type_name, data_dir)
        records = merge_records(existing, records, key=endpoint.get("key", "bubbleinternal_id"))

    elapsed = time.time() - start
    log.info(f"  {type_name}: {len(records)} total records in {elapsed:.1f}s")

    save_records(records, type_name, data_dir)
    return type_name, len(records)


async def run_extraction(mode: str = "full", endpoint_name: Optional[str] = None):
    """Run the full extraction pipeline."""
    if not API_TOKEN:
        log.error("BUBBLE_API_TOKEN environment variable not set!")
        sys.exit(1)

    DATA_DIR.mkdir(parents=True, exist_ok=True)

    # Determine since date for incremental
    since = None
    state_file = DATA_DIR / "_extraction_state.json"
    if mode == "incremental" and state_file.exists():
        with open(state_file) as f:
            state = json.load(f)
            since = state.get("last_run")
    elif mode == "incremental":
        # Fall back to 1 day ago
        since = (datetime.utcnow() - timedelta(days=1)).strftime("%Y-%m-%dT%H:%M:%S.000Z")

    # Select endpoints
    if endpoint_name:
        all_eps = INCREMENTAL_ENDPOINTS + FULL_LOAD_ENDPOINTS
        endpoints = [e for e in all_eps if e["name"].lower() == endpoint_name.lower()]
        if not endpoints:
            log.error(f"Unknown endpoint: {endpoint_name}")
            sys.exit(1)
    else:
        if mode == "full":
            endpoints = FULL_LOAD_ENDPOINTS + INCREMENTAL_ENDPOINTS
        else:
            # Incremental: only pull incremental endpoints + lookup tables
            endpoints = FULL_LOAD_ENDPOINTS + INCREMENTAL_ENDPOINTS

    log.info(f"Starting {mode} extraction of {len(endpoints)} endpoints...")
    log.info(f"Data directory: {DATA_DIR}")
    if since:
        log.info(f"Incremental since: {since}")

    results = {}
    async with BubbleClient(API_TOKEN) as client:
        # Extract lookup tables first (small, always full)
        for ep in [e for e in endpoints if e in FULL_LOAD_ENDPOINTS]:
            try:
                name, count = await extract_endpoint(client, ep, "full", DATA_DIR)
                results[name] = count
            except Exception as exc:
                log.warning(f"Skipping {ep['name']}: {exc}")
                continue

        # Then extract large tables (potentially filtered)
        for ep in [e for e in endpoints if e in INCREMENTAL_ENDPOINTS]:
            try:
                ep_mode = mode if mode == "incremental" else "full"
                name, count = await extract_endpoint(client, ep, ep_mode, DATA_DIR, since=since)
                results[name] = count
            except Exception as exc:
                log.warning(f"Skipping {ep['name']}: {exc}")
                continue

    # Save extraction state
    with open(state_file, "w") as f:
        json.dump({
            "last_run": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.000Z"),
            "mode": mode,
            "results": results,
        }, f, indent=2)

    log.info("=" * 60)
    log.info("Extraction complete!")
    total = sum(results.values())
    log.info(f"Total records: {total:,} across {len(results)} tables")
    for name, count in sorted(results.items(), key=lambda x: -x[1]):
        log.info(f"  {name}: {count:,}")

    return results


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Bubble.io Data Extractor")
    parser.add_argument("--full", action="store_true", help="Full extraction")
    parser.add_argument("--incremental", action="store_true", help="Incremental extraction")
    parser.add_argument("--endpoint", type=str, help="Single endpoint name")
    args = parser.parse_args()

    if args.incremental:
        mode = "incremental"
    else:
        mode = "full"

    asyncio.run(run_extraction(mode=mode, endpoint_name=args.endpoint))
