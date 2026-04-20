"""refresh_daily.py — Render + local-mirror half of the daily dashboard refresh.

Replaces the old Keboola → PBI pipeline. This script runs INSIDE a Cowork
scheduled task. The task prompt handles the Snowflake query step via the
Keboola MCP `query_data` tool (so there are no direct Snowflake creds in
this repo). After the 4 CSVs are dropped next to this script, the prompt
invokes this script to render the JSON and mirror it to the workspace
folder. The git push step is also handled by the scheduled-task prompt via
the `github` skill — NOT by this script.

Pipeline (this script's scope):
  1. check_inputs()      fail fast if any of the 4 CSVs is missing
  2. render_json.main()  read 4 CSVs + live dashboard_data.json → rendered JSON
  3. mirror_local()      copy rendered JSON → <workspace>/dashboard_data.json

Expected CSVs (produced by the scheduled-task prompt via Keboola MCP):
  wbr_weekly.sql       → snowflake_wbr.csv
  wbr_jobs_weekly.sql  → snowflake_wbr_jobs.csv
  ts_weekly.sql        → snowflake_ts.csv
  ts_conversion.sql    → snowflake_ts_conversion.csv
  aux_12w.sql          → snowflake_aux_12w.csv

This script is intentionally small: git push happens OUTSIDE this process,
driven by the scheduled-task prompt + the github skill. Keeping the concerns
separated makes both the script and the prompt easier to debug in isolation.

Run:
  python3 refresh_daily.py                # render + mirror to workspace
  python3 refresh_daily.py --render-only  # render only, no mirror (dev loop)
  python3 refresh_daily.py --dry-run      # render + log the mirror target
"""
from __future__ import annotations

import argparse
import logging
import shutil
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
# WORKSPACE is the parent folder (the mounted "Recruiting Dashboard" folder).
# Portable across Cowork sessions (session IDs change; mount folder doesn't).
WORKSPACE = HERE.parent
WORKSPACE_JSON = WORKSPACE / "dashboard_data.json"

REQUIRED_CSVS = [
    "snowflake_wbr.csv",
    "snowflake_wbr_jobs.csv",
    "snowflake_ts.csv",
    "snowflake_ts_conversion.csv",
    "snowflake_ts_jobs.csv",
    "snowflake_aux_12w.csv",
]

log = logging.getLogger("refresh_daily")


def _setup_logging(level: str = "INFO") -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def check_inputs() -> None:
    """Fail early if the scheduled-task upstream step didn't drop all 4 CSVs."""
    missing = [c for c in REQUIRED_CSVS if not (HERE / c).exists()]
    if missing:
        raise FileNotFoundError(
            f"Missing input CSVs: {missing}. "
            "The scheduled-task prompt should run query_data for each .sql "
            "file and write the response to the matching CSV before invoking "
            "refresh_daily.py."
        )
    for c in REQUIRED_CSVS:
        p = HERE / c
        age_h = (time.time() - p.stat().st_mtime) / 3600
        log.info("  %s: %d KB (age %.1fh)", c, p.stat().st_size // 1024, age_h)


def render_dashboard() -> Path:
    """Call render_json.main() to produce rendered_dashboard_data.json."""
    sys.path.insert(0, str(HERE))
    import importlib
    import render_json
    importlib.reload(render_json)
    log.info("Rendering dashboard_data.json")
    render_json.main()
    out = HERE / "rendered_dashboard_data.json"
    if not out.exists():
        raise RuntimeError("render_json did not produce rendered_dashboard_data.json")
    log.info("  wrote %s (%d KB)", out.name, out.stat().st_size // 1024)
    return out


def mirror_local(rendered: Path, dry_run: bool = False) -> None:
    """Copy rendered JSON to <workspace>/dashboard_data.json so local preview matches prod."""
    if dry_run:
        log.info("[dry-run] would copy %s → %s", rendered.name, WORKSPACE_JSON)
        return
    shutil.copy2(rendered, WORKSPACE_JSON)
    log.info("Copied → %s", WORKSPACE_JSON)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--render-only", action="store_true", help="Render only; no local mirror")
    ap.add_argument("--dry-run",     action="store_true", help="Render + log mirror target, no copy")
    ap.add_argument("--log-level",   default="INFO")
    args = ap.parse_args()
    _setup_logging(args.log_level)

    t0 = time.time()
    check_inputs()
    rendered = render_dashboard()

    if not args.render_only:
        mirror_local(rendered, dry_run=args.dry_run)

    log.info("Total refresh time: %.1fs", time.time() - t0)
    return 0


if __name__ == "__main__":
    sys.exit(main())
