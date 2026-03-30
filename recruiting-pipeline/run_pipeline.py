#!/usr/bin/env python3
"""
Tribe Recruiting Dashboard — Full Pipeline Orchestrator
==========================================================
Runs the complete pipeline: Bubble.io extraction → DuckDB transform → JSON output.

This is what n8n calls on schedule (9am, 3pm, 9pm CET).

Usage:
    # Full rebuild (first run, or weekly refresh)
    python run_pipeline.py --full

    # Incremental update (3x daily)
    python run_pipeline.py --incremental

    # Transform only (skip extraction, reprocess existing data)
    python run_pipeline.py --transform-only

Environment variables:
    BUBBLE_API_TOKEN     - Bubble.io API bearer token (required for extraction)
    RECRUIT_DATA_DIR     - Where to store raw Bubble JSON files (default: ./data)
    RECRUIT_OUTPUT_DIR   - Where to write dashboard JSON (default: ./output)
"""

import asyncio
import logging
import os
import sys
import shutil
import time
from pathlib import Path

# Add this directory to path so we can import siblings
sys.path.insert(0, str(Path(__file__).parent))

from bubble_extract import run_extraction
from transform import main as run_transform

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("pipeline")

# Paths
PIPELINE_DIR = Path(__file__).parent
DATA_DIR = Path(os.environ.get("RECRUIT_DATA_DIR", PIPELINE_DIR / "data"))
OUTPUT_DIR = Path(os.environ.get("RECRUIT_OUTPUT_DIR", PIPELINE_DIR / "output"))
DASHBOARD_SRC = PIPELINE_DIR.parent / "recruiting-dashboard" / "src"


def run_full_pipeline(mode: str = "incremental", skip_extract: bool = False):
    """Run the complete pipeline."""
    start = time.time()

    log.info("=" * 70)
    log.info(f"TRIBE RECRUITING DASHBOARD PIPELINE — {mode.upper()}")
    log.info("=" * 70)

    # Step 1: Extract from Bubble.io
    if not skip_extract:
        token = os.environ.get("BUBBLE_API_TOKEN", "")
        if not token:
            log.error("BUBBLE_API_TOKEN not set! Set it and retry.")
            log.error("  export BUBBLE_API_TOKEN='your-token-here'")
            sys.exit(1)

        log.info("")
        log.info("STEP 1: Extracting data from Bubble.io API...")
        log.info("-" * 50)
        asyncio.run(run_extraction(mode=mode))
    else:
        log.info("")
        log.info("STEP 1: SKIPPED (--transform-only)")

    # Step 2: Transform with DuckDB
    log.info("")
    log.info("STEP 2: Running DuckDB transformations...")
    log.info("-" * 50)
    run_transform()

    # Step 3: Copy output to dashboard src (if it exists)
    json_path = OUTPUT_DIR / "recruiting_data.json"
    if json_path.exists() and DASHBOARD_SRC.exists():
        dst = DASHBOARD_SRC / "data.json"
        shutil.copy2(json_path, dst)
        log.info(f"Copied to dashboard: {dst}")

    elapsed = time.time() - start
    log.info("")
    log.info("=" * 70)
    log.info(f"PIPELINE COMPLETE in {elapsed:.0f}s ({elapsed/60:.1f} min)")
    log.info(f"Output: {json_path}")
    log.info("=" * 70)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Tribe Recruiting Pipeline")
    parser.add_argument("--full", action="store_true", help="Full rebuild")
    parser.add_argument("--incremental", action="store_true", help="Incremental update (default)")
    parser.add_argument("--transform-only", action="store_true", help="Skip extraction, just transform")
    args = parser.parse_args()

    if args.transform_only:
        mode = "incremental"
        skip = True
    elif args.full:
        mode = "full"
        skip = False
    else:
        mode = "incremental"
        skip = False

    run_full_pipeline(mode=mode, skip_extract=skip)
