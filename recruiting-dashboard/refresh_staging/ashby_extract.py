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

Stdlib-only implementation:
    Uses urllib + concurrent.futures.ThreadPoolExecutor. No third-party deps,
    so it runs on the bare Keboola Custom Python runtime without needing
    pip-installed packages. ~1500 calls × 100ms ≈ 30s with 8 worker threads.

Auth:
    Ashby uses HTTP Basic with the API key as username, empty password.
    Get a key from Ashby: Settings → Integrations → API Keys → New Key.
    Required scopes: candidates:read, applications:read, jobs:read,
    interviews:read, offers:read.
    Pass via env var ASHBY_API_KEY or Keboola component parameter
    `#ashby_api_key`.

Cloudflare WAF:
    api.ashbyhq.com is fronted by Cloudflare which 403s requests with the
    default urllib User-Agent (error 1010). We always send a custom UA.

Scope filter:
    Tribe.xyz brand id = 4380b3f0-8d17-4c9a-9a78-9d943c68a404
    All Tribe internal hiring is under this brand.

Endpoints used (all POST with JSON body):
    /job.list           — all jobs in the brand (Open, Draft, Closed, Archived)
    /application.list   — applications per job (filter by jobId)
    /application.info   — full application incl. embedded applicationHistory
                          (no separate /applicationHistory.list endpoint exists)
    /interviewPlan.list — interview plans
    /interviewStage.list — stages per interview plan
    /offer.list         — offer records

Output files (written to OUTPUT_DIR):
    ashby_jobs.json                   — flat list of jobs (Tribe brand only)
    ashby_applications.json           — flat list of applications
    ashby_application_history.json    — flat list of stage transitions
                                        (one row per stage entered, with applicationId)
    ashby_interview_stages.json       — stage_id → stage_name lookup
    ashby_offers.json                 — offer records

Pagination:
    Ashby returns {results, moreDataAvailable, nextCursor, syncToken}.
    Pass nextCursor on subsequent calls to paginate.

Rate limit / retry:
    8 worker threads, exponential backoff on 429/5xx, max 3 retries per call.
"""
from __future__ import annotations

import argparse
import base64
import json
import logging
import os
import sys
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any

ASHBY_API = "https://api.ashbyhq.com"
TRIBE_BRAND_ID = "4380b3f0-8d17-4c9a-9a78-9d943c68a404"

USER_AGENT = "tribe-recruiting-dashboard/1.0 (ashby_extract.py)"

ENDPOINTS = {
    "jobs":                 "/job.list",
    "applications":         "/application.list",
    "application_info":     "/application.info",
    "interview_plans":      "/interviewPlan.list",
    "interview_stages":     "/interviewStage.list",
    "offers":               "/offer.list",
}

log = logging.getLogger("ashby_extract")


def _auth_header(api_key: str) -> str:
    raw = f"{api_key}:".encode("utf-8")
    return "Basic " + base64.b64encode(raw).decode("ascii")


def _post(api_key: str, path: str, body: dict | None = None, max_retries: int = 3) -> dict:
    """POST to Ashby with retries on 429/5xx. Returns parsed JSON dict."""
    body = body or {}
    headers = {
        "Authorization": _auth_header(api_key),
        "Content-Type": "application/json",
        "Accept": "application/json",
        "User-Agent": USER_AGENT,
    }
    url = ASHBY_API + path
    payload = json.dumps(body).encode("utf-8")
    last_err: Exception | None = None
    for attempt in range(max_retries + 1):
        req = urllib.request.Request(url, data=payload, headers=headers, method="POST")
        try:
            with urllib.request.urlopen(req, timeout=60) as r:
                data = json.loads(r.read())
                if not data.get("success", True):
                    raise RuntimeError(f"Ashby API error on {path}: {data.get('errors')}")
                return data
        except urllib.error.HTTPError as e:
            last_err = e
            if e.code in (429, 500, 502, 503, 504) and attempt < max_retries:
                wait = 2 ** attempt
                log.warning("Ashby HTTP %d on %s, retry in %ds", e.code, path, wait)
                time.sleep(wait)
                continue
            try:
                body_text = e.read().decode()[:300]
            except Exception:
                body_text = "(unreadable body)"
            raise RuntimeError(f"Ashby HTTP {e.code} on {path}: {body_text}") from e
        except urllib.error.URLError as e:
            last_err = e
            if attempt < max_retries:
                wait = 2 ** attempt
                log.warning("Ashby network error on %s (%s), retry in %ds", path, e, wait)
                time.sleep(wait)
                continue
            raise
    if last_err:
        raise last_err
    raise RuntimeError(f"Unexpected fall-through on {path}")


def _paginate(api_key: str, path: str, base_body: dict, page_delay: float = 0.1) -> list[dict]:
    """Paginate through an Ashby /list endpoint."""
    results: list[dict] = []
    cursor: str | None = None
    page = 0
    while True:
        body = dict(base_body)
        if cursor:
            body["cursor"] = cursor
        data = _post(api_key, path, body)
        page += 1
        results.extend(data.get("results", []) or [])
        if not data.get("moreDataAvailable") or not data.get("nextCursor"):
            break
        cursor = data["nextCursor"]
        time.sleep(page_delay)
        if page > 200:
            log.warning("Pagination cap hit on %s after %d pages", path, page)
            break
    return results


def fetch_jobs(api_key: str) -> list[dict]:
    """All jobs across all statuses, filtered to Tribe.xyz brand client-side."""
    all_jobs = []
    for status in ("Open", "Draft", "Closed", "Archived"):
        results = _paginate(api_key, ENDPOINTS["jobs"], {"status": [status]})
        for j in results:
            brand_id = j.get("brandId") or (j.get("brand") or {}).get("id")
            if brand_id == TRIBE_BRAND_ID:
                all_jobs.append(j)
    log.info("Fetched %d Tribe-brand jobs across all statuses", len(all_jobs))
    return all_jobs


def fetch_applications_for_job(api_key: str, job_id: str) -> list[dict]:
    """All applications for a single job (Active + Archived + Hired + Lead)."""
    return _paginate(api_key, ENDPOINTS["applications"], {"jobId": job_id})


def fetch_application_info(api_key: str, application_id: str) -> dict:
    """Full application info including embedded applicationHistory.

    Ashby has no /applicationHistory.list endpoint (verified 404 on 2026-05-04).
    The history is EMBEDDED in /application.info as `applicationHistory`,
    where each entry is {id, stageId, title, stageNumber, enteredStageAt,
    leftStageAt (omitted on current stage), actorId}.
    """
    data = _post(api_key, ENDPOINTS["application_info"], {"applicationId": application_id})
    return data.get("results") or {}


def fetch_interview_stages(api_key: str) -> list[dict]:
    """All interview stages across all plans (for stage_id → name + plan title)."""
    plans = _paginate(api_key, ENDPOINTS["interview_plans"], {})
    all_stages = []
    for plan in plans:
        try:
            stages = _paginate(api_key, ENDPOINTS["interview_stages"], {"interviewPlanId": plan["id"]})
        except Exception as e:
            log.warning("Stage fetch failed for plan %s: %s", plan["id"], e)
            stages = []
        for s in stages:
            s["interviewPlanId"] = plan["id"]
            s["interviewPlanTitle"] = plan.get("title")
        all_stages.extend(stages)
    return all_stages


def fetch_offers(api_key: str) -> list[dict]:
    """All offers (filter to Tribe brand applications client-side)."""
    return _paginate(api_key, ENDPOINTS["offers"], {})


def extract_all(api_key: str, output_dir: Path | str, fetch_history: bool = True,
                max_workers: int = 8) -> dict[str, int]:
    """Run the full extract. Writes 5 JSON files into output_dir.

    Returns counts per file for logging.
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    counts: dict[str, int] = {}

    jobs = fetch_jobs(api_key)
    (output_dir / "ashby_jobs.json").write_text(json.dumps(jobs, indent=2, ensure_ascii=False))
    counts["jobs"] = len(jobs)

    # Applications per job — parallel
    all_apps: list[dict] = []
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        for apps in ex.map(lambda j: fetch_applications_for_job(api_key, j["id"]), jobs):
            all_apps.extend(apps)
    (output_dir / "ashby_applications.json").write_text(json.dumps(all_apps, indent=2, ensure_ascii=False))
    counts["applications"] = len(all_apps)

    if fetch_history:
        history_apps = [a for a in all_apps if a.get("status") != "Lead"]
        log.info("Fetching application history for %d apps (skipping %d Leads)",
                 len(history_apps), len(all_apps) - len(history_apps))
        histories: list[dict] = []

        def _hist_for(app):
            try:
                info = fetch_application_info(api_key, app["id"])
                hist = info.get("applicationHistory") or []
                for h in hist:
                    h["applicationId"] = app["id"]
                return hist
            except Exception as e:
                log.warning("History fetch failed for app %s: %s", app["id"], e)
                return []

        with ThreadPoolExecutor(max_workers=max_workers) as ex:
            for hist in ex.map(_hist_for, history_apps):
                histories.extend(hist)
        (output_dir / "ashby_application_history.json").write_text(json.dumps(histories, indent=2, ensure_ascii=False))
        counts["application_history"] = len(histories)

    stages = fetch_interview_stages(api_key)
    (output_dir / "ashby_interview_stages.json").write_text(json.dumps(stages, indent=2, ensure_ascii=False))
    counts["interview_stages"] = len(stages)

    offers = fetch_offers(api_key)
    (output_dir / "ashby_offers.json").write_text(json.dumps(offers, indent=2, ensure_ascii=False))
    counts["offers"] = len(offers)

    return counts


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Extract Ashby data for the IR tab.")
    parser.add_argument("--output-dir", required=True, type=Path,
                        help="Directory to write ashby_*.json files into.")
    parser.add_argument("--api-key", default=os.environ.get("ASHBY_API_KEY"),
                        help="Ashby API key. Defaults to $ASHBY_API_KEY.")
    parser.add_argument("--skip-history", action="store_true",
                        help="Skip per-application history fetch (faster, but loses stage timestamps).")
    parser.add_argument("--max-workers", type=int, default=8)
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

    counts = extract_all(args.api_key, args.output_dir,
                         fetch_history=not args.skip_history,
                         max_workers=args.max_workers)
    log.info("Extract complete: %s", counts)
    print(json.dumps({"status": "ok", "counts": counts, "output_dir": str(args.output_dir)}))
    return 0


if __name__ == "__main__":
    sys.exit(main())
