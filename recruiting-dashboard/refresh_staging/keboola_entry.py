"""keboola_entry.py - Custom Python component entrypoint for the recruiting dashboard refresh.

Short version: stage 14 input CSVs + PBI JSON into a flat layout, call
render_json.main(), PUT output to recruiting-dashboard/src/dashboard_data_snowflake.json.

No `from __future__ import annotations` - Python 3.13 defaults handle it and the
component is strict about __future__ import ordering.
"""
import sys
print("=== keboola_entry.py loaded ===", flush=True)

import base64
import json
import logging
import shutil
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

print("=== stdlib imports done ===", flush=True)

from keboola.component import CommonInterface

print("=== keboola.component imported ===", flush=True)

REPO = "bark8922/tribe-recruiting"
TARGET_FILE = "recruiting-dashboard/src/dashboard_data_snowflake.json"


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
        ):
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


def push_to_github(token, content):
    """Push content to GitHub Contents API with retry on transient 5xx / 409.

    GitHub's Contents API occasionally returns 500 Internal Server Error on large
    PUTs (the JSON is ~27 MB; ~37 MB base64-encoded). Pre-2026-06-03 behaviour was
    to fail the whole Flow on any 5xx. Now we retry up to 3 times with exponential
    backoff and re-fetch the SHA each attempt (so a 409 from a race with a manual
    commit also recovers).
    """
    url = "https://api.github.com/repos/" + REPO + "/contents/" + TARGET_FILE
    headers = {
        "Authorization": "token " + token,
        "Accept": "application/vnd.github.v3+json",
        "User-Agent": "keboola-custom-python-tribe-recruiting",
    }
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    encoded = base64.b64encode(content.encode("utf-8")).decode("ascii")

    last_err = None
    for attempt in range(1, 4):
        try:
            req = urllib.request.Request(url + "?ref=main", headers=headers)
            with urllib.request.urlopen(req, timeout=30) as r:
                current = json.loads(r.read())
            sha = current["sha"]
            print("[push_to_github] attempt " + str(attempt) + " current sha: " + sha[:10], flush=True)

            body = json.dumps({
                "message": "refresh: Keboola-driven rebuild (" + now + ")",
                "content": encoded,
                "sha": sha,
                "branch": "main",
            }).encode("utf-8")
            req = urllib.request.Request(
                url, data=body, method="PUT",
                headers={**headers, "Content-Type": "application/json"},
            )
            with urllib.request.urlopen(req, timeout=120) as r:
                resp = json.loads(r.read())
            commit = resp["commit"]
            print("[push_to_github] pushed " + commit["sha"][:10] + ": " + commit["html_url"], flush=True)
            return commit["sha"]
        except urllib.error.HTTPError as e:
            last_err = e
            if e.code >= 500 or e.code == 409:
                backoff = 5 * (2 ** (attempt - 1))  # 5s, 10s, 20s
                print("[push_to_github] attempt " + str(attempt) + " failed: HTTP " + str(e.code) + " - retrying in " + str(backoff) + "s", flush=True)
                time.sleep(backoff)
                continue
            raise
    raise last_err


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
    run_ashby(flat, ashby_key)  # v2 lean+incremental, 180s deadline, best-effort
    out_path = run_render(flat)
    content = out_path.read_text(encoding="utf-8")

    sha = push_to_github(github_token, content)
    print("=== done: commit=" + sha[:10] + " size=" + str(len(content) // 1024) + "KB ===", flush=True)
    return 0


print("=== about to call main() ===", flush=True)
_rc = main()
print("=== main() returned " + str(_rc) + " ===", flush=True)
sys.exit(_rc)
