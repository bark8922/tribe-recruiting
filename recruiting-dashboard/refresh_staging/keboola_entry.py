"""keboola_entry.py - Custom Python component entrypoint for the recruiting dashboard refresh.

Short version: stage 14 input CSVs + PBI JSON into a flat layout, call
render_json.main(), PUT output to recruiting-dashboard/src/dashboard_data_snowflake.json.

No `from __future__ import annotations` - Python 3.13 defaults handle it and the
component is strict about __future__ import ordering.
"""
import sys
print("=== keboola_entry.py loaded ===", flush=True)

import base64
import gzip
import json
import logging
import os
import shutil
import subprocess
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

print("=== stdlib imports done ===", flush=True)

from keboola.component import CommonInterface

print("=== keboola.component imported ===", flush=True)

REPO = "bark8922/tribe-recruiting"
TARGET_FILE = "recruiting-dashboard/public/dashboard_data_snowflake.json.gz"

# Finance actual_spend export (contains the 'BU Client' tab: client -> BU lead
# per month). Written by the n8n "Actual Spend Export" workflow each finance
# refresh. render_json.load_bu_groups() reads it from staging to derive the
# Dolphins/Ponies BU group. Best-effort — see fetch_spend_csv().
FINANCE_REPO = "bark8922/tribe-dashboard"
FINANCE_SPEND_PATH = "data-next/spend/actual_spend_all_tabs.csv"
DQ_TARGET_FILE = "recruiting-dashboard/public/dq_reasons.json"
FINDER_TARGET_FILE = "recruiting-dashboard/public/finder_data.json.gz"


def stage_inputs(ci, flat):
    repo_root = Path(__file__).resolve().parent.parent.parent
    src_dir = repo_root / "recruiting-dashboard" / "refresh_staging"
    src_json = repo_root / "recruiting-dashboard" / "src" / "dashboard_data.json"
    print("[stage_inputs] repo_root=" + str(repo_root), flush=True)

    (flat / "refresh_staging").mkdir(parents=True, exist_ok=True)
    (flat / "wbr_static").mkdir(parents=True, exist_ok=True)

    copied = 0
    for p in src_dir.iterdir():
        # .py / .sql / .md = source code render_json reads at runtime.
        # ashby_*.json = baseline files seeded into staging by run_ashby()
        # (other .json files in src_dir are not relevant to render_json).
        if p.suffix in (".py", ".sql", ".md") or (
            p.suffix == ".json" and p.name.startswith("ashby_")
        ) or p.name in ("snowflake_new_project_health.csv", "ir_crosswalk.csv"):
            # snowflake_new_project_health.csv is a committed SEED for the New
            # Project Health tab. Copied here so render_json finds it even before
            # a Keboola input table exists. If/when that table is mapped as an
            # input, the input-tables loop below overwrites this seed with live data.
            shutil.copy(p, flat / "refresh_staging" / p.name)
            copied += 1
    print("[stage_inputs] copied " + str(copied) + " repo files", flush=True)

    shutil.copy(src_json, flat / "dashboard_data.json")
    print("[stage_inputs] copied PBI dashboard_data.json", flush=True)

    staged = 0
    for tbl in ci.get_input_tables_definitions():
        name = Path(tbl.full_path).name
        if name.startswith("snowflake_"):
            dst = flat / "refresh_staging" / name
        elif name.startswith("wbr_"):
            dst = flat / "wbr_static" / name
        else:
            print("[stage_inputs] WARN unmapped " + name, flush=True)
            dst = flat / "refresh_staging" / name
        shutil.copy(tbl.full_path, dst)
        staged += 1
    print("[stage_inputs] staged " + str(staged) + " input tables", flush=True)


def run_ashby(flat, ashby_key):
    """Fetch Ashby data via incremental sync. Reads baseline JSONs from the
    cloned repo and writes updated ones into the flat staging dir for
    render_json.py to consume.

    v2 design (2026-05-05): NEVER blocks the flow.
    - Hard 180s deadline; partial results OK (incremental persists progress).
    - Wraps everything in try/except — failures log WARN and skip Ashby
      step entirely. render_json.py's _ir_load() falls back to live JSON.
    - Uses /application.list only (NOT /application.info, which exploded the
      previous v1 to 15+ min). Loses stage-entry timestamps but keeps current
      stage, status, archive reason — enough for the IR tab v1.
    - Reads ashby_*_baseline.json from the cloned repo as the seed; writes
      ashby_jobs.json + ashby_applications.json + sync_tokens.json into
      staging for downstream consumption.
    """
    if not ashby_key:
        print("[run_ashby] no #ashby_api_key, skipping", flush=True)
        return
    staging = flat / "refresh_staging"
    sys.path.insert(0, str(staging))
    try:
        import ashby_extract
        import shutil
        # Seed staging with baseline files (the cloned repo has ashby_*_baseline.json)
        for src_name, dst_name in [
            ("ashby_jobs_baseline.json",                  "ashby_jobs.json"),
            ("ashby_applications_baseline.json",          "ashby_applications.json"),
            ("ashby_application_histories_baseline.json", "ashby_application_histories.json"),
            ("ashby_sync_tokens_baseline.json",           "sync_tokens.json"),
        ]:
            src_p = staging / src_name
            if src_p.exists():
                shutil.copy(src_p, staging / dst_name)
        result = ashby_extract.extract_all(
            api_key=ashby_key,
            output_dir=staging,
            mode="incremental",
            baseline_dir=staging,
            max_seconds=180,
            max_workers=4,
        )
        print("[run_ashby] " + str(result), flush=True)
    except Exception as e:
        print("[run_ashby] WARN failed: " + type(e).__name__ + ": " + str(e), flush=True)


def run_render(flat):
    staging = flat / "refresh_staging"
    sys.path.insert(0, str(staging))
    import render_json
    print("[run_render] render_json imported, calling main()", flush=True)
    render_json.main()
    out = staging / "rendered_dashboard_data.json"
    if not out.exists():
        raise RuntimeError("render_json did not produce rendered_dashboard_data.json")
    print("[run_render] done: " + str(out.stat().st_size // 1024) + " KB", flush=True)
    return out


def get_dq_artifact(flat):
    """Optional second artifact: rendered_dq_reasons.json (PD tab DQ section).
    Returns Path or None — absence must never fail the flow."""
    out = flat / "refresh_staging" / "rendered_dq_reasons.json"
    if out.exists():
        print("[get_dq_artifact] found (" + str(out.stat().st_size // 1024) + " KB)", flush=True)
        return out
    print("[get_dq_artifact] not produced - skipping", flush=True)
    return None


def build_finder(flat):
    """Render the candidate_finder table (staged as snowflake_candidate_finder.csv)
    into a lean JSON array for the Candidate Finder tab. Best-effort: if the CSV
    is not staged (input mapping missing or transform skipped), returns None and
    the finder file is simply not updated this run."""
    import csv as _csv
    src = flat / "refresh_staging" / "snowflake_candidate_finder.csv"
    if not src.exists():
        print("[build_finder] snowflake_candidate_finder.csv not staged - skipping", flush=True)
        return None
    cols = ["name", "current_title", "company", "location", "country", "function",
            "role_type", "client", "sourced_role", "stage", "reason", "linkedin"]
    rows = []
    _csv.field_size_limit(10 * 1024 * 1024)
    with open(src, newline="", encoding="utf-8") as fh:
        for row in _csv.DictReader(fh):
            rows.append({k: (row.get(k) or "") for k in cols})
    print("[build_finder] " + str(len(rows)) + " finder rows", flush=True)
    return json.dumps({"generated_at": datetime.now(timezone.utc).isoformat(), "candidates": rows}, ensure_ascii=False)


def push_to_github(token, content, dq_content=None, finder_content=None):
    """Push content to GitHub via git CLI (not Contents API).

    The container clones bark8922/tribe-recruiting at /code/repo_clone before
    running this script. We write the new JSON to that working copy, then
    `git add / commit / push` via the same PAT used for the clone.

    Why not the Contents API: GitHub's REST Contents endpoint returns HTTP 500
    on PUTs above ~45 MB base64 (the JSON is ~30 MB → ~40 MB base64). git's
    smart-HTTP transport packs and streams, so the size limit is effectively
    GitHub's per-push 2 GB ceiling — well above what we'll ever send here.
    Hit consistently on 2026-06-03 after the viewed-attribution change split
    the row count up; retry-on-5xx didn't help because every PUT 500'd.

    Returns the new commit SHA (short, 10 chars).
    """
    repo_dir = Path("/code/repo_clone")
    if not repo_dir.exists():
        raise RuntimeError("Expected git clone at /code/repo_clone — component not configured?")

    target = repo_dir / TARGET_FILE
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(gzip.compress(content.encode("utf-8"), 9))
    size_kb = target.stat().st_size // 1024
    print("[push_to_github] wrote " + TARGET_FILE + " (" + str(size_kb) + " KB gzipped)", flush=True)

    tracked = [TARGET_FILE]
    if dq_content is not None:
        dq_target = repo_dir / DQ_TARGET_FILE
        dq_target.parent.mkdir(parents=True, exist_ok=True)
        dq_target.write_text(dq_content, encoding="utf-8")
        tracked.append(DQ_TARGET_FILE)
        print("[push_to_github] wrote " + DQ_TARGET_FILE + " (" + str(dq_target.stat().st_size // 1024) + " KB)", flush=True)

    if finder_content is not None:
        finder_target = repo_dir / FINDER_TARGET_FILE
        finder_target.parent.mkdir(parents=True, exist_ok=True)
        finder_target.write_bytes(gzip.compress(finder_content.encode("utf-8"), 9))
        tracked.append(FINDER_TARGET_FILE)
        print("[push_to_github] wrote " + FINDER_TARGET_FILE + " (" + str(finder_target.stat().st_size // 1024) + " KB gzipped)", flush=True)

    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    env = os.environ.copy()
    env["GIT_AUTHOR_NAME"] = "Keboola Flow"
    env["GIT_AUTHOR_EMAIL"] = "blake@tribe.xyz"
    env["GIT_COMMITTER_NAME"] = "Keboola Flow"
    env["GIT_COMMITTER_EMAIL"] = "blake@tribe.xyz"

    def run(cmd):
        print("[push_to_github] $ " + " ".join(cmd), flush=True)
        result = subprocess.run(cmd, cwd=str(repo_dir), env=env, capture_output=True, text=True, timeout=120)
        if result.stdout:
            print(result.stdout, flush=True)
        if result.returncode != 0:
            print("STDERR: " + (result.stderr or ""), flush=True)
            raise RuntimeError("git " + cmd[1] + " failed (rc=" + str(result.returncode) + ")")
        return result.stdout.strip()

    run(["git", "add"] + tracked)
    status = subprocess.run(["git", "status", "--porcelain"] + tracked, cwd=str(repo_dir), capture_output=True, text=True)
    if not status.stdout.strip():
        print("[push_to_github] no changes vs main — skipping commit", flush=True)
        head = run(["git", "rev-parse", "HEAD"])
        return head

    # Authenticated push URL (embed PAT). Avoid logging the URL.
    push_url = "https://x-access-token:" + token + "@github.com/" + REPO + ".git"
    subprocess.run(["git", "remote", "set-url", "origin", push_url], cwd=str(repo_dir), check=True)

    run(["git", "commit", "-m", "refresh: Keboola-driven rebuild (" + now + ")"])
    run(["git", "push", "origin", "HEAD:main"])
    sha = run(["git", "rev-parse", "HEAD"])
    print("[push_to_github] pushed " + sha[:10], flush=True)
    return sha



def notify_circle(token):
    """Fire a repository_dispatch at bark8922/tribe-circle so its refresh
    workflow rebuilds circle_data.json immediately after our push, instead of
    waiting on GitHub's best-effort cron (which silently dropped both morning
    slots on 2026-06-10 and left Circle stale all morning).

    Best-effort by design: a failed dispatch must never fail the Flow. The
    cron in tribe-circle stays as a backstop.
    """
    url = "https://api.github.com/repos/bark8922/tribe-circle/dispatches"
    body = json.dumps({"event_type": "recruiting-data-updated"}).encode("utf-8")
    req = urllib.request.Request(url, data=body, method="POST", headers={
        "Authorization": "Bearer " + token,
        "Accept": "application/vnd.github+json",
        "User-Agent": "keboola-recruiting-refresh",
    })
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            print("[notify_circle] dispatch sent, HTTP " + str(resp.status), flush=True)
    except Exception as exc:
        print("[notify_circle] WARNING: dispatch failed (non-fatal): " + repr(exc), flush=True)


def fetch_spend_csv(token, flat):
    """Fetch the finance actual_spend export (contains the 'BU Client' tab) from
    tribe-dashboard and drop it into staging so render_json.load_bu_groups() can
    derive the Dolphins/Ponies BU group from Leadership's single source of truth.

    Best-effort: any failure logs WARN and is swallowed. render_json then bakes an
    empty bu_group_by_key and App.jsx falls back to its legacy client-name rule,
    so the flow never breaks. Requires the token to have read access to
    bark8922/tribe-dashboard (same org as the push target)."""
    dst = flat / "refresh_staging" / "actual_spend_all_tabs.csv"
    tmp = Path("/tmp/_spend_fetch")
    shutil.rmtree(tmp, ignore_errors=True)
    # Use the same git transport as push_to_github (known to work from the Keboola
    # runner) rather than the REST Contents API, which the runner may not reach.
    clone_url = "https://x-access-token:" + token + "@github.com/" + FINANCE_REPO + ".git"
    try:
        # Plain shallow clone (repo is ~6 MB; the runner's git is too old for
        # --sparse / partial-clone flags — verified 2026-07-13 job log).
        subprocess.run(
            ["git", "clone", "--depth", "1", clone_url, str(tmp)],
            check=True, capture_output=True, text=True, timeout=120)
        src = tmp / FINANCE_SPEND_PATH
        shutil.copy(src, dst)
        print("[fetch_spend_csv] fetched " + str(src.stat().st_size // 1024)
              + " KB via git from " + FINANCE_SPEND_PATH, flush=True)
    except subprocess.CalledProcessError as e:
        print("[fetch_spend_csv] WARN git failed: " + ((e.stderr or str(e))[:300]), flush=True)
    except Exception as e:
        print("[fetch_spend_csv] WARN failed: " + type(e).__name__ + ": " + str(e), flush=True)
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


def main():
    print("=== main() called ===", flush=True)
    ci = CommonInterface()
    params = ci.configuration.parameters
    print("=== CommonInterface ready, params keys: " + str(list(params.keys())) + " ===", flush=True)

    github_token = params.get("#github_token")
    if not github_token:
        github_token = params.get("user_properties", {}).get("#github_token")
    if not github_token:
        raise RuntimeError("Missing #github_token in configuration parameters.")
    print("=== github_token loaded (len=" + str(len(github_token)) + ") ===", flush=True)

    ashby_key = params.get("#ashby_api_key")
    if not ashby_key:
        ashby_key = params.get("user_properties", {}).get("#ashby_api_key")
    if ashby_key:
        print("=== ashby_api_key loaded (len=" + str(len(ashby_key)) + ") ===", flush=True)

    flat = Path("/tmp/flat")
    shutil.rmtree(flat, ignore_errors=True)
    flat.mkdir(parents=True)

    stage_inputs(ci, flat)
    fetch_spend_csv(github_token, flat)  # finance BU Client tab → staging (best-effort)
    run_ashby(flat, ashby_key)  # v2 lean+incremental, 180s deadline, best-effort
    out_path = run_render(flat)
    content = out_path.read_text(encoding="utf-8")
    dq_path = get_dq_artifact(flat)
    dq_content = dq_path.read_text(encoding="utf-8") if dq_path else None
    try:
        finder_content = build_finder(flat)
    except Exception as e:
        print("[build_finder] WARN failed: " + type(e).__name__ + ": " + str(e), flush=True)
        finder_content = None

    sha = push_to_github(github_token, content, dq_content, finder_content)
    notify_circle(github_token)
    print("=== done: commit=" + sha[:10] + " size=" + str(len(content) // 1024) + "KB ===", flush=True)
    return 0


print("=== about to call main() ===", flush=True)
_rc = main()
print("=== main() returned " + str(_rc) + " ===", flush=True)
sys.exit(_rc)
