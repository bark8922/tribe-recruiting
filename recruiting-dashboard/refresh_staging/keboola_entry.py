import sys as _sys
print("=== keboola_entry.py loaded ===", flush=True)
_sys.stderr.write("=== keboola_entry.py stderr ===\n")
_sys.stderr.flush()

"""keboola_entry.py — Custom Python component entrypoint for the recruiting dashboard refresh.

See commit history for full design notes. Short version:
  1. Keboola Flow runs Google Sheet + 4 Snowflake transformations first
  2. Keboola clones bark8922/tribe-recruiting + runs this file
  3. We stage 14 input CSVs + PBI JSON into a flat layout render_json.py expects
  4. Call render_json.main()
  5. PUT output to recruiting-dashboard/src/dashboard_data_snowflake.json
"""
from __future__ import annotations

import base64
import json
import logging
import shutil
import sys
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

print("=== imports done ===", flush=True)

from keboola.component import CommonInterface

print("=== keboola.component imported ===", flush=True)

REPO = "bark8922/tribe-recruiting"
TARGET_FILE = "recruiting-dashboard/src/dashboard_data_snowflake.json"


def stage_inputs(ci, flat):
    repo_root = Path(__file__).resolve().parent.parent.parent
    src_dir = repo_root / "recruiting-dashboard" / "refresh_staging"
    src_json = repo_root / "recruiting-dashboard" / "src" / "dashboard_data.json"
    print(f"[stage_inputs] repo_root={repo_root}", flush=True)

    (flat / "refresh_staging").mkdir(parents=True, exist_ok=True)
    (flat / "wbr_static").mkdir(parents=True, exist_ok=True)

    copied = 0
    for p in src_dir.iterdir():
        if p.suffix in {".py", ".sql", ".md"}:
            shutil.copy(p, flat / "refresh_staging" / p.name)
            copied += 1
    print(f"[stage_inputs] copied {copied} repo files", flush=True)

    shutil.copy(src_json, flat / "dashboard_data.json")
    print(f"[stage_inputs] copied PBI dashboard_data.json", flush=True)

    staged = 0
    for tbl in ci.get_input_tables_definitions():
        name = Path(tbl.full_path).name
        if name.startswith("snowflake_"):
            dst = flat / "refresh_staging" / name
        elif name.startswith("wbr_"):
            dst = flat / "wbr_static" / name
        else:
            print(f"[stage_inputs] WARN unmapped {name}", flush=True)
            dst = flat / "refresh_staging" / name
        shutil.copy(tbl.full_path, dst)
        staged += 1
    print(f"[stage_inputs] staged {staged} input tables", flush=True)


def run_render(flat):
    staging = flat / "refresh_staging"
    sys.path.insert(0, str(staging))
    import render_json  # type: ignore
    print("[run_render] render_json imported, calling main()", flush=True)
    render_json.main()
    out = staging / "rendered_dashboard_data.json"
    if not out.exists():
        raise RuntimeError("render_json did not produce rendered_dashboard_data.json")
    print(f"[run_render] done: {out.stat().st_size // 1024} KB", flush=True)
    return out


def push_to_github(token, content):
    url = f"https://api.github.com/repos/{REPO}/contents/{TARGET_FILE}"
    headers = {
        "Authorization": f"token {token}",
        "Accept": "application/vnd.github.v3+json",
        "User-Agent": "keboola-custom-python-tribe-recruiting",
    }
    req = urllib.request.Request(f"{url}?ref=main", headers=headers)
    with urllib.request.urlopen(req, timeout=30) as r:
        current = json.loads(r.read())
    sha = current["sha"]
    print(f"[push_to_github] current sha: {sha[:10]}", flush=True)

    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    body = json.dumps({
        "message": f"refresh: Keboola-driven rebuild ({now})",
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
    print(f"[push_to_github] pushed {commit['sha'][:10]}: {commit['html_url']}", flush=True)
    return commit["sha"]


def main():
    print("=== main() called ===", flush=True)
    ci = CommonInterface()
    params = ci.configuration.parameters
    print(f"=== CommonInterface ready, params keys: {list(params.keys())} ===", flush=True)

    github_token = params.get("#github_token")
    if not github_token:
        github_token = params.get("user_properties", {}).get("#github_token")
    if not github_token:
        raise RuntimeError(
            "Missing #github_token in configuration parameters."
        )
    print(f"=== github_token loaded (len={len(github_token)}) ===", flush=True)

    flat = Path("/tmp/flat")
    shutil.rmtree(flat, ignore_errors=True)
    flat.mkdir(parents=True)

    stage_inputs(ci, flat)
    out_path = run_render(flat)
    content = out_path.read_text(encoding="utf-8")

    sha = push_to_github(github_token, content)
    print(f"=== done: commit={sha[:10]} size={len(content) // 1024}KB ===", flush=True)
    return 0


print("=== about to call main() ===", flush=True)
_rc = main()
print(f"=== main() returned {_rc} ===", flush=True)
sys.exit(_rc)
