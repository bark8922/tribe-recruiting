"""keboola_entry.py — Custom Python component entrypoint for the recruiting dashboard refresh.

Runs inside the Keboola `kds-team.app-custom-python` component, replacing the
Cowork scheduled task. End-to-end:

  1. Keboola's Flow has already refreshed the Google Sheet (in.c-wbr-sheet.*)
     and run the 4 Snowflake transformations (out.c-*) before this component
     executes. 14 input tables are wired into this component's input mapping.

  2. Keboola clones bark8922/tribe-recruiting into the working directory via
     the `source: git` + `auth: pat` config and runs THIS file.

  3. We stage the input CSVs + the PBI-side dashboard_data.json into a flat
     layout that render_json.py expects (wbr_static/ + refresh_staging/ under
     a common ROOT).

  4. We call render_json.main() to produce rendered_dashboard_data.json.

  5. We PUT the rendered JSON to
     recruiting-dashboard/src/dashboard_data_snowflake.json via the GitHub
     Contents API — the only touched file, just like the scheduled task did.

Required user_properties:
  - #github_token — GitHub PAT with contents:write on bark8922/tribe-recruiting.
                    (Same PAT as the scheduled task; could in theory be scoped
                    tighter to just this one file but not worth the effort.)

Input mapping (destination filenames are what render_json.py expects):
  Snowflake outputs → refresh_staging/*.csv
    snowflake_wbr.csv, snowflake_wbr_jobs.csv, snowflake_ts_jobs.csv,
    snowflake_ts.csv, snowflake_ts_conversion.csv, snowflake_aux_12w.csv,
    snowflake_project_dashboard.csv, snowflake_project_dashboard_hires.csv,
    snowflake_mbr_contacted_ev.csv
  Google Sheet tabs → wbr_static/*.csv
    wbr_ta_target.csv, wbr_ta_weekly_note.csv, wbr_ts_weekly.csv,
    wbr_ir.csv, wbr_reasoning_guidance.csv
"""
from __future__ import annotations

import base64
import json
import logging
import os
import shutil
import sys
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

from keboola.component import CommonInterface

REPO = "bark8922/tribe-recruiting"
TARGET_FILE = "recruiting-dashboard/src/dashboard_data_snowflake.json"


def stage_inputs(ci: CommonInterface, flat: Path) -> None:
    """Copy Keboola input CSVs + render_json.py + PBI dashboard_data.json into
    the flat layout render_json.py expects.

    render_json.py computes ROOT = Path(__file__).parent.parent, so if it
    lives at <flat>/refresh_staging/render_json.py then ROOT = <flat> and:
      - WBR_TA_TARGET_CSV   = <flat>/wbr_static/wbr_ta_target.csv
      - LIVE_JSON           = <flat>/dashboard_data.json
      - OUT_JSON            = <flat>/refresh_staging/rendered_dashboard_data.json
    """
    repo_root = Path.cwd()
    src_dir = repo_root / "recruiting-dashboard" / "refresh_staging"
    src_json = repo_root / "recruiting-dashboard" / "src" / "dashboard_data.json"

    (flat / "refresh_staging").mkdir(parents=True, exist_ok=True)
    (flat / "wbr_static").mkdir(parents=True, exist_ok=True)

    # Copy render_json.py (and any helpers) from the cloned repo
    for p in src_dir.iterdir():
        if p.suffix in {".py", ".sql", ".md"}:
            shutil.copy(p, flat / "refresh_staging" / p.name)

    # Copy the PBI-side JSON (render_json.py's static-fields source)
    shutil.copy(src_json, flat / "dashboard_data.json")

    # Route each Keboola input table to the right folder based on filename
    # convention. destination is the filename set in the input mapping.
    for tbl in ci.get_input_tables_definitions():
        name = Path(tbl.full_path).name
        if name.startswith("snowflake_"):
            dst = flat / "refresh_staging" / name
        elif name.startswith("wbr_"):
            dst = flat / "wbr_static" / name
        else:
            logging.warning("Unmapped input table %s — dropping into refresh_staging/", name)
            dst = flat / "refresh_staging" / name
        shutil.copy(tbl.full_path, dst)
        logging.info("  staged %s (%d KB)", dst.relative_to(flat), dst.stat().st_size // 1024)


def run_render(flat: Path) -> Path:
    """Run render_json.main() from the staged flat layout. Returns the output path."""
    staging = flat / "refresh_staging"
    sys.path.insert(0, str(staging))
    # Import render_json after sys.path is set so it resolves ROOT correctly
    import render_json  # type: ignore
    logging.info("Running render_json.main()")
    render_json.main()
    out = staging / "rendered_dashboard_data.json"
    if not out.exists():
        raise RuntimeError("render_json did not produce rendered_dashboard_data.json")
    logging.info("  render complete: %d KB", out.stat().st_size // 1024)
    return out


def push_to_github(token: str, content: str) -> str:
    """PUT the rendered JSON to GitHub via Contents API. Returns the commit SHA."""
    url = f"https://api.github.com/repos/{REPO}/contents/{TARGET_FILE}"
    headers = {
        "Authorization": f"token {token}",
        "Accept": "application/vnd.github.v3+json",
        "User-Agent": "keboola-custom-python-tribe-recruiting",
    }

    # Fetch current file SHA (required by the PUT API for updates)
    req = urllib.request.Request(f"{url}?ref=main", headers=headers)
    with urllib.request.urlopen(req, timeout=30) as r:
        current = json.loads(r.read())
    sha = current["sha"]
    logging.info("Current file sha: %s", sha[:10])

    # PUT new content. GitHub's Contents API expects base64-encoded body
    # with a 76-col line length — the api accepts any valid base64, so
    # stdlib b64encode works.
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    body = json.dumps(
        {
            "message": f"refresh: Keboola-driven rebuild ({now})",
            "content": base64.b64encode(content.encode("utf-8")).decode("ascii"),
            "sha": sha,
            "branch": "main",
        }
    ).encode("utf-8")
    req = urllib.request.Request(
        url, data=body, method="PUT",
        headers={**headers, "Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=60) as r:
        resp = json.loads(r.read())
    commit = resp["commit"]
    logging.info("Pushed commit %s: %s", commit["sha"][:10], commit["html_url"])
    return commit["sha"]


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    ci = CommonInterface()
    params = ci.configuration.parameters

    token_key = "#github_token"
    if token_key not in params:
        # user_properties is the canonical place for secrets in this component
        token_key_up = params.get("user_properties", {}).get("#github_token")
        if not token_key_up:
            raise RuntimeError(
                "Missing #github_token in configuration parameters (expected either "
                "at top-level params or under user_properties.#github_token)."
            )
        github_token = token_key_up
    else:
        github_token = params[token_key]

    flat = Path("/tmp/flat")
    shutil.rmtree(flat, ignore_errors=True)
    flat.mkdir(parents=True)

    stage_inputs(ci, flat)
    out_path = run_render(flat)
    content = out_path.read_text(encoding="utf-8")

    sha = push_to_github(github_token, content)
    logging.info("Done. commit=%s size=%dKB", sha[:10], len(content) // 1024)
    return 0


if __name__ == "__main__":
    sys.exit(main())
