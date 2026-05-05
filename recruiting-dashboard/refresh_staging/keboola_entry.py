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
        if p.suffix in (".py", ".sql", ".md"):
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
    """Fetch Ashby data into flat/refresh_staging/ashby_*.json. Best-effort:
    if the key is missing or the fetch fails, log and skip. render_json.py
    handles missing files gracefully."""
    if not ashby_key:
        print("[run_ashby] no #ashby_api_key set, skipping (Phase 2b not yet plumbed in)", flush=True)
        return
    staging = flat / "refresh_staging"
    sys.path.insert(0, str(staging))
    try:
        import asyncio
        import ashby_extract
        counts = asyncio.run(ashby_extract.extract_all(ashby_key, staging, fetch_history=True))
        print("[run_ashby] extracted: " + str(counts), flush=True)
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
    url = "https://api.github.com/repos/" + REPO + "/contents/" + TARGET_FILE
    headers = {
        "Authorization": "token " + token,
        "Accept": "application/vnd.github.v3+json",
        "User-Agent": "keboola-custom-python-tribe-recruiting",
    }
    req = urllib.request.Request(url + "?ref=main", headers=headers)
    with urllib.request.urlopen(req, timeout=30) as r:
        current = json.loads(r.read())
    sha = current["sha"]
    print("[push_to_github] current sha: " + sha[:10], flush=True)

    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    body = json.dumps({
        "message": "refresh: Keboola-driven rebuild (" + now + ")",
        "content": base64.b64encode(content.encode("utf-8")).decode("ascii"),
        "sha": sha,
        "branch": "main",
    }).encode("utf-8")
    req = urllib.request.Request(
        url, data=body, method="PUT",
        headers={**headers, "Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=60) as r:
        resp = json.loads(r.read())
    commit = resp["commit"]
    print("[push_to_github] pushed " + commit["sha"][:10] + ": " + commit["html_url"], flush=True)
    return commit["sha"]


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
    run_ashby(flat, ashby_key)
    out_path = run_render(flat)
    content = out_path.read_text(encoding="utf-8")

    sha = push_to_github(github_token, content)
    print("=== done: commit=" + sha[:10] + " size=" + str(len(content) // 1024) + "KB ===", flush=True)
    return 0


print("=== about to call main() ===", flush=True)
_rc = main()
print("=== main() returned " + str(_rc) + " ===", flush=True)
sys.exit(_rc)
