"""ashby_extract.py — Ashby REST API extractor for the Internal Recruiting tab.

Phase 2b extractor that fetches the right side of the IR funnel from Ashby
(stages where Bubble is sparse: Onsite, Culture Interview, Call with Client,
Offer, Hired) plus structured archive reasons and active pipeline depth.

Architecture:
    Ashby REST API  →  ashby_extract.py (this file)  →  ashby_*.json files
                                                              ↓
                                          render_json.py reads + aggregates
                                                              ↓
                                           ir_ashby_* sections in output JSON
                                                              ↓
                                                  IR tab merges Bubble + Ashby

Why direct REST instead of the Ashby MCP:
    The MCP is for interactive AI use; it dropped 3x during scoping. n8n's
    refresh job needs Python code it can call directly, on a 2-hour cycle.
    Same pattern as Bubble→Snowflake (Keboola does the ETL there; here we
    own it because Keboola has no native Ashby extractor).

Auth:
    Ashby uses HTTP Basic with the API key as username, empty password.
    Get a key from Ashby: Settings → Integrations → API Keys → New Key.
    Required scopes: candidates:read, applications:read, jobs:read,
    interviews:read, offers:read.
    Pass via env var ASHBY_API_KEY or as a Keboola component parameter.

Scope filter:
    Tribe.xyz brand id = 4380b3f0-8d17-4c9a-9a78-9d943c68a404
    All Tribe internal hiring is under this brand.

Endpoints used (all POST with JSON body, per Ashby convention):
    /job.list                 — all jobs in the brand (Open, Draft, Closed, Archived)
    /application.list         — applications per job (filter by jobId)
    /applicationHistory.list  — stage transitions per application (entered_at, stage_id)
    /interview.list           — interview templates (for stage names)
    /interviewStageGroup.list — stage groups for stage classification
    /offer.list               — offer records per application

Output files (written to OUTPUT_DIR):
    ashby_jobs.json                   — flat list of jobs
    ashby_applications.json           — flat list of applications with current stage + archive reason
    ashby_application_history.json    — flat list of stage transitions
    ashby_interview_stages.json       — stage_id → stage_name + group lookup
    ashby_offers.json                 — offer records

Pagination:
    Ashby returns {results, moreDataAvailable, nextCursor, syncToken}.
    Pass nextCursor on subsequent calls to paginate. Persist syncToken for
    incremental sync on the next run (only fetches changes since last token).

Rate limit:
    Ashby allows ~100 req/sec; we cap at 5 concurrent + 0.2s per-call delay
    to stay safely below.

Retry:
    Exponential backoff on 429/500/502/503/504. Max 3 retries.

USAGE — local dev:
    export ASHBY_API_KEY='your_key_here'
    python ashby_extract.py --output-dir /tmp/ashby --since 2026-01-01

USAGE — Keboola Python component:
    Set #ashby_api_key in component parameters. keboola_entry.py imports
    extract_all() and writes outputs to the staging dir before render_json.

NOT YET WIRED:
    keboola_entry.py integration — TODO
    n8n schedule — TODO (will piggyback on existing 2-hour flow)
    render_json.py ir_ashby_* sections — TODO (Phase 2b step 2)
    App.jsx funnel merge — TODO (Phase 2b step 3)
"""
from __future__ import annotations

import argparse
import asyncio
import base64
import json
import logging
import os
import sys
from pathlib import Path
from typing import Any

try:
    import aiohttp
except ImportError:
    aiohttp = None  # imported lazily; keboola_entry can install it

ASHBY_API = "https://api.ashbyhq.com"
TRIBE_BRAND_ID = "4380b3f0-8d17-4c9a-9a78-9d943c68a404"

# Endpoints. All are POST with JSON body. The /list endpoints return
# {results, moreDataAvailable, nextCursor, syncToken}.
ENDPOINTS = {
    "jobs":                 "/job.list",
    "applications":         "/application.list",
    "application_history":  "/applicationHistory.list",
    "interview_stages":     "/interviewStage.list",
    "interview_stage_groups": "/interviewStageGroup.list",
    "offers":               "/offer.list",
}

log = logging.getLogger("ashby_extract")


def _basic_auth_header(api_key: str) -> str:
    """Ashby uses HTTP Basic with API key as username, empty password."""
    raw = f"{api_key}:".encode("utf-8")
    return "Basic " + base64.b64encode(raw).decode("ascii")


async def _request(
    session: "aiohttp.ClientSession",
    path: str,
    body: dict | None = None,
    api_key: str = "",
    max_retries: int = 3,
) -> dict:
    """POST to Ashby with retries on 429/5xx."""
    body = body or {}
    headers = {
        "Authorization": _basic_auth_header(api_key),
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    url = ASHBY_API + path
    for attempt in range(max_retries + 1):
        try:
            async with session.post(url, headers=headers, json=body, timeout=aiohttp.ClientTimeout(total=60)) as r:
                if r.status == 200:
                    data = await r.json()
                    if not data.get("success", True):
                        # Ashby returns {success: false, errors: [...]} on logical errors
                        raise RuntimeError(f"Ashby API error: {data.get('errors')}")
                    return data
                if r.status in (429, 500, 502, 503, 504) and attempt < max_retries:
                    wait = 2 ** attempt
                    log.warning("Ashby %s %d on %s, retry in %ds", r.status, r.status, path, wait)
                    await asyncio.sleep(wait)
                    continue
                text = await r.text()
                raise RuntimeError(f"Ashby HTTP {r.status} on {path}: {text[:300]}")
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            if attempt < max_retries:
                wait = 2 ** attempt
                log.warning("Ashby network error on %s (%s), retry in %ds", path, e, wait)
                await asyncio.sleep(wait)
                continue
            raise


async def _paginate(
    session: "aiohttp.ClientSession",
    path: str,
    base_body: dict,
    api_key: str,
    page_delay: float = 0.2,
) -> tuple[list[dict], str | None]:
    """Paginate through an Ashby /list endpoint. Returns (all_results, last_sync_token)."""
    results: list[dict] = []
    cursor: str | None = None
    sync_token: str | None = None
    page = 0
    while True:
        body = dict(base_body)
        if cursor:
            body["cursor"] = cursor
        data = await _request(session, path, body, api_key)
        page += 1
        page_results = data.get("results", []) or []
        results.extend(page_results)
        sync_token = data.get("syncToken") or sync_token
        if not data.get("moreDataAvailable") or not data.get("nextCursor"):
            break
        cursor = data["nextCursor"]
        await asyncio.sleep(page_delay)
        if page > 200:  # safety cap
            log.warning("Pagination cap hit on %s after %d pages", path, page)
            break
    return results, sync_token


async def fetch_jobs(session, api_key: str) -> list[dict]:
    """All jobs across all statuses, filtered to Tribe.xyz brand."""
    all_jobs = []
    for status in ("Open", "Draft", "Closed", "Archived"):
        body = {"status": [status]}
        results, _ = await _paginate(session, ENDPOINTS["jobs"], body, api_key)
        # Filter to Tribe brand if Ashby returns jobs from multiple brands
        for j in results:
            if j.get("brandId") == TRIBE_BRAND_ID or j.get("brand", {}).get("id") == TRIBE_BRAND_ID:
                all_jobs.append(j)
    log.info("Fetched %d Tribe-brand jobs across all statuses", len(all_jobs))
    return all_jobs


async def fetch_applications_for_job(session, job_id: str, api_key: str) -> list[dict]:
    """All applications for a single job (Active + Archived + Hired + Lead)."""
    body = {"jobId": job_id}
    results, _ = await _paginate(session, ENDPOINTS["applications"], body, api_key)
    return results


async def fetch_application_history(session, application_id: str, api_key: str) -> list[dict]:
    """Stage transition timeline for a single application."""
    body = {"applicationId": application_id}
    results, _ = await _paginate(session, ENDPOINTS["application_history"], body, api_key)
    return results


async def fetch_interview_stages(session, api_key: str) -> list[dict]:
    """All interview stages across all interview plans (for stage_id → name mapping)."""
    # interviewStage.list requires interviewPlanId per call. Get plans first.
    plans_body: dict = {}
    plans, _ = await _paginate(session, "/interviewPlan.list", plans_body, api_key)
    all_stages = []
    for plan in plans:
        body = {"interviewPlanId": plan["id"]}
        try:
            stages, _ = await _paginate(session, ENDPOINTS["interview_stages"], body, api_key)
        except Exception as e:
            log.warning("Stage fetch failed for plan %s: %s", plan["id"], e)
            stages = []
        for s in stages:
            s["interviewPlanId"] = plan["id"]
            s["interviewPlanTitle"] = plan.get("title")
        all_stages.extend(stages)
    return all_stages


async def fetch_interview_stage_groups(session, api_key: str) -> list[dict]:
    """Stage group lookup (Active/Archived/Hired classification)."""
    results, _ = await _paginate(session, ENDPOINTS["interview_stage_groups"], {}, api_key)
    return results


async def fetch_offers(session, api_key: str) -> list[dict]:
    """All offers (filter to Tribe brand applications client-side)."""
    results, _ = await _paginate(session, ENDPOINTS["offers"], {}, api_key)
    return results


async def extract_all(api_key: str, output_dir: Path, fetch_history: bool = True) -> dict[str, int]:
    """Run the full extract. Writes 5 JSON files into output_dir.

    Returns counts per file for logging.
    """
    if aiohttp is None:
        raise RuntimeError("aiohttp not installed. pip install aiohttp")
    output_dir.mkdir(parents=True, exist_ok=True)
    counts: dict[str, int] = {}

    connector = aiohttp.TCPConnector(limit=5)
    async with aiohttp.ClientSession(connector=connector) as session:
        jobs = await fetch_jobs(session, api_key)
        (output_dir / "ashby_jobs.json").write_text(json.dumps(jobs, indent=2, ensure_ascii=False))
        counts["jobs"] = len(jobs)

        all_apps: list[dict] = []
        for job in jobs:
            apps = await fetch_applications_for_job(session, job["id"], api_key)
            all_apps.extend(apps)
            await asyncio.sleep(0.05)
        (output_dir / "ashby_applications.json").write_text(json.dumps(all_apps, indent=2, ensure_ascii=False))
        counts["applications"] = len(all_apps)

        if fetch_history:
            histories: list[dict] = []
            # Only fetch history for non-Lead applications to keep the volume manageable.
            history_apps = [a for a in all_apps if a.get("status") != "Lead"]
            log.info("Fetching application history for %d apps (skipping %d Leads)",
                     len(history_apps), len(all_apps) - len(history_apps))
            for app in history_apps:
                try:
                    hist = await fetch_application_history(session, app["id"], api_key)
                    for h in hist:
                        h["applicationId"] = app["id"]
                    histories.extend(hist)
                except Exception as e:
                    log.warning("History fetch failed for app %s: %s", app["id"], e)
                await asyncio.sleep(0.05)
            (output_dir / "ashby_application_history.json").write_text(json.dumps(histories, indent=2, ensure_ascii=False))
            counts["application_history"] = len(histories)

        stages = await fetch_interview_stages(session, api_key)
        (output_dir / "ashby_interview_stages.json").write_text(json.dumps(stages, indent=2, ensure_ascii=False))
        counts["interview_stages"] = len(stages)

        offers = await fetch_offers(session, api_key)
        (output_dir / "ashby_offers.json").write_text(json.dumps(offers, indent=2, ensure_ascii=False))
        counts["offers"] = len(offers)

    return counts


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--output-dir", required=True, type=Path,
                        help="Directory to write ashby_*.json files into.")
    parser.add_argument("--api-key", default=os.environ.get("ASHBY_API_KEY"),
                        help="Ashby API key. Defaults to $ASHBY_API_KEY.")
    parser.add_argument("--skip-history", action="store_true",
                        help="Skip the per-application history fetch (faster, but loses stage timestamps).")
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper()),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    if not args.api_key:
        log.error("ASHBY_API_KEY not set and --api-key not provided")
        return 2

    counts = asyncio.run(extract_all(args.api_key, args.output_dir, fetch_history=not args.skip_history))
    log.info("Extract complete: %s", counts)
    print(json.dumps({"status": "ok", "counts": counts, "output_dir": str(args.output_dir)}))
    return 0


if __name__ == "__main__":
    sys.exit(main())
