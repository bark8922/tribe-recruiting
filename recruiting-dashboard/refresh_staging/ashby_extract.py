"""ashby_extract.py — Ashby REST extractor for the IR tab. v2 lean+incremental.

Phase 2b v2 architecture (post-rollback redesign 2026-05-05):
    Step 1 — ONE-TIME BASELINE (~2 min, slow):
        Run with --mode=baseline once to fetch every Tribe app via
        /job.list + /application.list across all 4 statuses. Output committed
        to repo as ashby_jobs.json + ashby_applications.json + sync_tokens.json.

    Step 2 — INCREMENTAL (default, ~10s every Keboola run):
        Reads baseline + sync_tokens.json from --baseline-dir.
        For each job whose syncToken is known, calls /application.list with
        that token to get ONLY the apps that changed. Merges into the baseline
        (replacing apps with the same id) and writes the updated files.
        Saves the new sync_tokens.json so the next run picks up from there.

What we DELIBERATELY don't fetch:
    /application.info (per-app full history). It's 1 call per app × ~15k apps
    = 15-30 min. The previous "v1" version fetched this and broke the flow's
    3-4 min baseline. Without it we lose stage-entry timestamps but keep:
      - currentInterviewStage (where each app is now)
      - status (Active/Archived/Hired/Lead)
      - archiveReason (clean DQ taxonomy)
      - createdAt / updatedAt / archivedAt timestamps
      - candidate + job + creditedToUser metadata
    That's enough for the IR tab v1 (right-side funnel current depth, hire
    list, DQ pie, archive timing).

Auth: HTTP Basic, API key as username, empty password. Cloudflare WAF in
front of api.ashbyhq.com requires a non-default User-Agent (verified
2026-05-04: error 1010 on default urllib UA).

Scope filter: brand_id = 4380b3f0-8d17-4c9a-9a78-9d943c68a404 (Tribe.xyz).

Hard time budget: extract_all() respects max_seconds (default 60s) and
returns partial results with status=timeout if it can't finish — never
blocks the caller indefinitely. Critical for the Keboola flow which has
its own runtime budget.
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
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any

ASHBY_API = "https://api.ashbyhq.com"
TRIBE_BRAND_ID = "4380b3f0-8d17-4c9a-9a78-9d943c68a404"
USER_AGENT = "tribe-recruiting-dashboard/1.0 (ashby_extract.py)"
DEFAULT_MAX_SECONDS = 60
DEFAULT_MAX_WORKERS = 4

# Field whitelist for trimmed output (full /application.list rows are ~2KB
# each; trimming to needed fields gets us to ~500B avg. 15k apps fit in 8MB
# instead of 28MB, which matters for the git-cloned baseline.)
def _trim_application(a: dict) -> dict:
    return {
        "id": a["id"],
        "status": a.get("status"),
        "createdAt": a.get("createdAt"),
        "updatedAt": a.get("updatedAt"),
        "archivedAt": a.get("archivedAt"),
        "currentInterviewStage": (
            {"title": (a.get("currentInterviewStage") or {}).get("title"),
             "type":  (a.get("currentInterviewStage") or {}).get("type")}
            if a.get("currentInterviewStage") else None
        ),
        "archiveReason": (
            {"text": (a.get("archiveReason") or {}).get("text")}
            if a.get("archiveReason") else None
        ),
        "job": (
            {"id":    (a.get("job") or {}).get("id"),
             "title": (a.get("job") or {}).get("title")}
            if a.get("job") else None
        ),
        "candidate": (
            {"id":   (a.get("candidate") or {}).get("id"),
             "name": (a.get("candidate") or {}).get("name")}
            if a.get("candidate") else None
        ),
        "creditedToUser": (
            {"id":        (a.get("creditedToUser") or {}).get("id"),
             "firstName": (a.get("creditedToUser") or {}).get("firstName"),
             "lastName":  (a.get("creditedToUser") or {}).get("lastName")}
            if a.get("creditedToUser") else None
        ),
    }


def _trim_job(j: dict) -> dict:
    return {
        "id": j["id"],
        "title": j.get("title"),
        "status": j.get("status"),
        "departmentId": j.get("departmentId"),
        "locationId": j.get("locationId"),
        "createdAt": j.get("createdAt"),
        "updatedAt": j.get("updatedAt"),
        "openedAt": j.get("openedAt"),
        "closedAt": j.get("closedAt"),
        "brandId": j.get("brandId"),
        "hiringTeam": j.get("hiringTeam") or [],
    }


log = logging.getLogger("ashby_extract")


def _auth(api_key: str) -> str:
    return "Basic " + base64.b64encode(f"{api_key}:".encode()).decode()


def _post(api_key: str, path: str, body: dict | None = None, max_retries: int = 3) -> dict:
    body = body or {}
    headers = {
        "Authorization": _auth(api_key),
        "Content-Type": "application/json",
        "Accept": "application/json",
        "User-Agent": USER_AGENT,
    }
    payload = json.dumps(body).encode()
    for attempt in range(max_retries + 1):
        req = urllib.request.Request(ASHBY_API + path, data=payload,
                                     headers=headers, method="POST")
        try:
            with urllib.request.urlopen(req, timeout=30) as r:
                data = json.loads(r.read())
                if not data.get("success", True):
                    raise RuntimeError(f"Ashby API error on {path}: {data.get('errors')}")
                return data
        except urllib.error.HTTPError as e:
            if e.code in (429, 500, 502, 503, 504) and attempt < max_retries:
                time.sleep(1.0 + attempt)
                continue
            raise RuntimeError(f"Ashby HTTP {e.code} on {path}: {e.read().decode()[:200]}") from e
        except urllib.error.URLError as e:
            if attempt < max_retries:
                time.sleep(1.0 + attempt)
                continue
            raise


def _paginate(api_key: str, path: str, body: dict) -> tuple[list[dict], str | None]:
    """Returns (results, last_sync_token)."""
    out: list[dict] = []
    cursor: str | None = None
    sync_token: str | None = None
    while True:
        b = dict(body)
        if cursor:
            b["cursor"] = cursor
        d = _post(api_key, path, b)
        out.extend(d.get("results", []) or [])
        sync_token = d.get("syncToken") or sync_token
        if not d.get("moreDataAvailable") or not d.get("nextCursor"):
            break
        cursor = d["nextCursor"]
    return out, sync_token


def fetch_jobs(api_key: str, statuses: tuple = ("Open", "Draft", "Closed", "Archived")) -> list[dict]:
    """Tribe-brand jobs in given statuses. Default = all statuses for baseline."""
    out = []
    for status in statuses:
        results, _ = _paginate(api_key, "/job.list", {"status": [status]})
        for j in results:
            if (j.get("brandId") or (j.get("brand") or {}).get("id")) == TRIBE_BRAND_ID:
                out.append(_trim_job(j))
    log.info("Fetched %d Tribe-brand jobs (statuses=%s)", len(out), statuses)
    return out


def fetch_applications_for_job(api_key: str, job_id: str,
                                sync_token: str | None = None) -> tuple[list[dict], str | None]:
    """Apps for a job. With sync_token, returns ONLY apps changed since."""
    body = {"jobId": job_id}
    if sync_token:
        body["syncToken"] = sync_token
    apps, new_token = _paginate(api_key, "/application.list", body)
    return [_trim_application(a) for a in apps], new_token



def _is_late_stage_app(a: dict) -> bool:
    """Did this application reach Onsite / Final / Offer / Hired stages?
    Used to decide whether the per-app history is worth fetching. Most apps
    (especially the ~5k "Disqualify after application" rejections) never
    progressed past stage 1, so fetching their history is wasted work.
    """
    if a.get("status") == "Hired":
        return True
    stage = a.get("currentInterviewStage") or {}
    stage_type = (stage.get("type") or "").lower()
    if stage_type in ("final interview", "offer", "hired"):
        return True
    title = (stage.get("title") or "").lower()
    return any(k in title for k in ("onsite", "culture", "call with client",
                                     "client prep", "offer", "final"))


def fetch_application_info(api_key: str, application_id: str) -> dict:
    """Full /application.info — used to extract embedded applicationHistory.
    Stage transitions live in info.applicationHistory[] with enteredStageAt
    and leftStageAt timestamps. There is no /applicationHistory.list endpoint
    (verified 404 on 2026-05-04)."""
    data = _post(api_key, "/application.info", {"applicationId": application_id})
    return data.get("results") or {}


def fetch_late_stage_histories(api_key: str, apps: list[dict],
                                 already_have: set | None = None,
                                 max_workers: int = 4,
                                 deadline: float | None = None) -> list[dict]:
    """Fetch applicationHistory for late-stage apps. Returns list of
    {applicationId, applicationHistory: [...]} dicts. Skips apps in
    already_have (for incremental — avoid re-fetching unchanged apps)."""
    already_have = already_have or set()
    targets = [a for a in apps if _is_late_stage_app(a) and a["id"] not in already_have]
    if not targets:
        return []
    log.info("Fetching applicationHistory for %d late-stage apps", len(targets))

    def _hist(a):
        if deadline and time.time() > deadline:
            return None
        try:
            info = fetch_application_info(api_key, a["id"])
            return {"applicationId": a["id"],
                    "applicationHistory": info.get("applicationHistory") or [],
                    "candidate": (info.get("candidate") or {"name": (a.get("candidate") or {}).get("name")}),
                    "job": {"id": (a.get("job") or {}).get("id"),
                            "title": (a.get("job") or {}).get("title")}}
        except Exception as e:
            log.warning("history fetch failed for app %s: %s", a["id"][:8], e)
            return None

    out = []
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        for r in ex.map(_hist, targets):
            if r:
                out.append(r)
    return out


def extract_all(
    api_key: str,
    output_dir: Path | str,
    mode: str = "incremental",
    baseline_dir: Path | str | None = None,
    max_seconds: int = DEFAULT_MAX_SECONDS,
    max_workers: int = DEFAULT_MAX_WORKERS,
) -> dict[str, Any]:
    """Run the extractor.

    mode='baseline'    — full pull across all statuses. Use ONCE to seed.
                         Output: ashby_jobs.json + ashby_applications.json
                                 + sync_tokens.json.
    mode='incremental' — read baseline_dir, fetch only changes via syncTokens,
                         merge, write to output_dir. Use this for every
                         scheduled refresh.

    Returns: {mode, jobs_count, apps_count, changed_count, elapsed_s, status}
    where status = 'ok' | 'timeout' | 'no_baseline'.
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    baseline_dir = Path(baseline_dir) if baseline_dir else output_dir
    t0 = time.time()
    deadline = t0 + max_seconds

    if mode == "baseline":
        return _run_baseline(api_key, output_dir, deadline, max_workers, t0)
    elif mode == "incremental":
        return _run_incremental(api_key, output_dir, baseline_dir, deadline, max_workers, t0)
    else:
        raise ValueError(f"Unknown mode: {mode!r} (expected 'baseline' or 'incremental')")


def _run_baseline(api_key, output_dir, deadline, max_workers, t0):
    log.info("=== BASELINE mode (one-time full pull) ===")
    jobs = fetch_jobs(api_key)
    if time.time() > deadline:
        return {"mode": "baseline", "jobs_count": len(jobs), "apps_count": 0,
                "elapsed_s": time.time() - t0, "status": "timeout"}
    (output_dir / "ashby_jobs.json").write_text(json.dumps(jobs, indent=2, ensure_ascii=False))

    all_apps: list[dict] = []
    tokens: dict[str, str] = {}

    def fetch_one(j):
        if time.time() > deadline:
            return j["id"], [], None, "timeout"
        try:
            apps, tok = fetch_applications_for_job(api_key, j["id"])
            return j["id"], apps, tok, None
        except Exception as e:
            return j["id"], [], None, str(e)[:120]

    errors = []
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        for jid, apps, tok, err in ex.map(fetch_one, jobs):
            if err:
                errors.append({"job_id": jid, "error": err})
            else:
                all_apps.extend(apps)
                if tok:
                    tokens[jid] = tok

    (output_dir / "ashby_applications.json").write_text(json.dumps(all_apps, indent=2, ensure_ascii=False))
    (output_dir / "sync_tokens.json").write_text(json.dumps(tokens, indent=2))

    # Fetch applicationHistory for late-stage apps only (~200 of 15k)
    histories = []
    if time.time() < deadline:
        histories = fetch_late_stage_histories(api_key, all_apps, max_workers=max_workers, deadline=deadline)
    (output_dir / "ashby_application_histories.json").write_text(
        json.dumps(histories, indent=2, ensure_ascii=False))

    elapsed = time.time() - t0
    status = "timeout" if time.time() > deadline else "ok"
    log.info("baseline complete: %d jobs, %d apps, %d tokens, %d histories, %d errors, %.1fs",
             len(jobs), len(all_apps), len(tokens), len(histories), len(errors), elapsed)
    return {"mode": "baseline", "jobs_count": len(jobs), "apps_count": len(all_apps),
            "tokens_count": len(tokens), "histories_count": len(histories),
            "errors": errors, "elapsed_s": elapsed, "status": status}


def _run_incremental(api_key, output_dir, baseline_dir, deadline, max_workers, t0):
    log.info("=== INCREMENTAL mode ===")
    bjobs_p = baseline_dir / "ashby_jobs.json"
    bapps_p = baseline_dir / "ashby_applications.json"
    btoks_p = baseline_dir / "sync_tokens.json"
    if not (bjobs_p.exists() and bapps_p.exists() and btoks_p.exists()):
        log.error("Baseline files missing in %s — run --mode=baseline once first", baseline_dir)
        return {"mode": "incremental", "elapsed_s": time.time() - t0, "status": "no_baseline"}

    jobs = json.loads(bjobs_p.read_text())
    apps_by_id: dict[str, dict] = {a["id"]: a for a in json.loads(bapps_p.read_text())}
    tokens: dict[str, str] = json.loads(btoks_p.read_text())

    # Refresh job list — only Open+Draft (the ones taking new apps). Closed/
    # Archived jobs are already in the baseline and won't change. Cuts the
    # incremental fetch from 79 jobs to ~12, keeping wall time under 30s.
    fresh_jobs = fetch_jobs(api_key, statuses=("Open", "Draft"))
    # Merge with baseline jobs so we don't lose closed/archived in the output
    baseline_job_ids = {j["id"] for j in fresh_jobs}
    for j in jobs:
        if j["id"] not in baseline_job_ids:
            fresh_jobs.append(j)
    job_ids_now = {j["id"] for j in fresh_jobs}
    # Detect new jobs (no token yet) so they get a full pull, not incremental
    new_job_ids = job_ids_now - set(tokens.keys())
    if new_job_ids:
        log.info("Detected %d new jobs since baseline (no token, will full-pull)", len(new_job_ids))

    def fetch_one(j):
        if time.time() > deadline:
            return j["id"], [], None, "timeout"
        try:
            tok = tokens.get(j["id"])  # None for new jobs → full pull
            apps, new_tok = fetch_applications_for_job(api_key, j["id"], tok)
            return j["id"], apps, new_tok, None
        except Exception as e:
            return j["id"], [], None, str(e)[:120]

    changed_count = 0
    errors = []
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        for jid, apps, new_tok, err in ex.map(fetch_one, fresh_jobs):
            if err:
                errors.append({"job_id": jid, "error": err})
                continue
            if new_tok:
                tokens[jid] = new_tok
            for a in apps:
                apps_by_id[a["id"]] = a  # upsert
                changed_count += 1
            if time.time() > deadline:
                log.warning("incremental sync hit time budget; partial results saved")
                break

    # Refresh histories for apps that touch late stages (only fetch ones we
    # don't already have — the existing history file is loaded as the seed).
    hist_p = baseline_dir / "ashby_application_histories.json"
    existing_histories = json.loads(hist_p.read_text()) if hist_p.exists() else []
    have_ids = {h["applicationId"] for h in existing_histories}
    if time.time() < deadline:
        new_histories = fetch_late_stage_histories(
            api_key, list(apps_by_id.values()), already_have=have_ids,
            max_workers=max_workers, deadline=deadline)
        # Also re-fetch histories for apps where status / current stage CHANGED
        # so we capture e.g. Onsite -> Offer transitions on existing apps
        changed_late = [a for a in apps_by_id.values()
                        if _is_late_stage_app(a) and a["id"] in have_ids]
        # For changed apps with existing history, re-fetch to overwrite
        if changed_count > 0 and changed_late:
            refresh = fetch_late_stage_histories(
                api_key, changed_late, already_have=set(),
                max_workers=max_workers, deadline=deadline)
            # Merge: refresh overrides existing
            refresh_ids = {h["applicationId"] for h in refresh}
            existing_histories = [h for h in existing_histories if h["applicationId"] not in refresh_ids]
            existing_histories.extend(refresh)
        existing_histories.extend(new_histories)
    histories_count = len(existing_histories)

    # Persist
    (output_dir / "ashby_jobs.json").write_text(json.dumps(fresh_jobs, indent=2, ensure_ascii=False))
    (output_dir / "ashby_applications.json").write_text(
        json.dumps(list(apps_by_id.values()), indent=2, ensure_ascii=False))
    (output_dir / "ashby_application_histories.json").write_text(
        json.dumps(existing_histories, indent=2, ensure_ascii=False))
    (output_dir / "sync_tokens.json").write_text(json.dumps(tokens, indent=2))

    elapsed = time.time() - t0
    status = "timeout" if time.time() > deadline else "ok"
    log.info("incremental complete: %d jobs, %d apps total (%d changed), %d errors, %.1fs",
             len(fresh_jobs), len(apps_by_id), changed_count, len(errors), elapsed)
    return {"mode": "incremental", "jobs_count": len(fresh_jobs),
            "apps_count": len(apps_by_id), "changed_count": changed_count,
            "histories_count": histories_count,
            "errors": errors, "elapsed_s": elapsed, "status": status}


def main(argv=None):
    parser = argparse.ArgumentParser(description="Extract Ashby data for the IR tab.")
    parser.add_argument("--mode", choices=("baseline", "incremental"), default="incremental")
    parser.add_argument("--output-dir", required=True, type=Path)
    parser.add_argument("--baseline-dir", type=Path)
    parser.add_argument("--api-key", default=os.environ.get("ASHBY_API_KEY"))
    parser.add_argument("--max-seconds", type=int, default=DEFAULT_MAX_SECONDS)
    parser.add_argument("--max-workers", type=int, default=DEFAULT_MAX_WORKERS)
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args(argv)
    logging.basicConfig(level=getattr(logging, args.log_level.upper()),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
    if not args.api_key:
        log.error("ASHBY_API_KEY not set and --api-key not provided")
        return 2
    result = extract_all(args.api_key, args.output_dir, mode=args.mode,
                         baseline_dir=args.baseline_dir,
                         max_seconds=args.max_seconds,
                         max_workers=args.max_workers)
    log.info("done: %s", result)
    print(json.dumps(result))
    return 0 if result.get("status") == "ok" else 1


if __name__ == "__main__":
    sys.exit(main())
