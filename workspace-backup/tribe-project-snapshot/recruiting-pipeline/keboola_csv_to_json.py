#!/usr/bin/env python3
"""
Convert Keboola GZIP CSV exports → JSON files for DuckDB transform.
====================================================================
After downloading table exports from Keboola UI (More actions → Export table),
this script converts them into the bubble_*.json format expected by transform.py.

For Events (the largest table), it splits into monthly files
(bubble_Events_YYYYMM.json) to avoid OOM during transform.

Usage:
    # Place downloaded .csv.gz files in the data/ directory, named like:
    #   Events.csv.gz, Candidate.csv.gz, Talent.csv.gz, etc.
    #
    # Or specify a directory containing the downloads:
    python3 keboola_csv_to_json.py --input-dir ~/Downloads
    python3 keboola_csv_to_json.py --input-dir ./data

    # Then run the transform as usual:
    python3 transform.py
"""

import csv
import gzip
import json
import logging
import os
import sys
from collections import defaultdict
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("csv_to_json")

DATA_DIR = Path(os.environ.get("RECRUIT_DATA_DIR", Path(__file__).parent / "data"))

# Tables that should be split into monthly files (too large for single JSON)
MONTHLY_SPLIT_TABLES = {"Events"}

# Column name mapping: Keboola uses underscores already, but some fields
# in bubble_extract.py output use different names. This mapping ensures
# consistency with what transform.py expects.
# Keboola column → JSON key (only list differences; most are identical)
COLUMN_MAP = {
    # Keboola columns match bubble_extract.py output, no remapping needed
    # for most fields. The key difference is that Keboola includes extra
    # columns (Creator, Created_By, Feedback, etc.) that bubble_extract.py
    # doesn't pull — these are harmless, DuckDB just ignores unused columns.
}


def convert_csv_to_json(csv_path: Path, table_name: str, output_dir: Path):
    """Convert a single CSV/GZIP-CSV file to JSON format."""
    output_dir.mkdir(parents=True, exist_ok=True)

    # Detect if gzipped
    is_gzip = csv_path.suffix == '.gz' or csv_path.suffixes == ['.csv', '.gz']
    opener = gzip.open if is_gzip else open

    log.info(f"Converting {csv_path.name} → bubble_{table_name}*.json")

    if table_name in MONTHLY_SPLIT_TABLES:
        _convert_monthly_split(csv_path, table_name, output_dir, opener)
    else:
        _convert_single(csv_path, table_name, output_dir, opener)


def _convert_single(csv_path: Path, table_name: str, output_dir: Path, opener):
    """Convert to a single bubble_<table>.json file."""
    records = []
    with opener(csv_path, 'rt', encoding='utf-8', errors='replace') as f:
        reader = csv.DictReader(f)
        for row in reader:
            rec = {}
            for k, v in row.items():
                key = COLUMN_MAP.get(k, k)
                rec[key] = v if v != "" else None
            records.append(rec)

    out_path = output_dir / f"bubble_{table_name}.json"
    with open(out_path, 'w', encoding='utf-8') as f:
        json.dump(records, f, ensure_ascii=False, default=str)

    log.info(f"  → {out_path.name}: {len(records):,} records")


def _convert_monthly_split(csv_path: Path, table_name: str, output_dir: Path, opener):
    """Convert to monthly files: bubble_<table>_YYYYMM.json."""
    # Group records by month based on Created_Date
    monthly_buckets = defaultdict(list)
    total = 0

    with opener(csv_path, 'rt', encoding='utf-8', errors='replace') as f:
        reader = csv.DictReader(f)
        for row in reader:
            rec = {}
            for k, v in row.items():
                key = COLUMN_MAP.get(k, k)
                rec[key] = v if v != "" else None

            # Extract month from Created_Date (format: 2025-01-14T15:16:17.913Z)
            created = rec.get("Created_Date", "") or ""
            if len(created) >= 7:
                month_key = created[:7].replace("-", "")  # "202501"
            else:
                month_key = "unknown"

            monthly_buckets[month_key].append(rec)
            total += 1

            # Log progress every 1M records
            if total % 1_000_000 == 0:
                log.info(f"  Read {total:,} records so far...")

    log.info(f"  Read {total:,} total records across {len(monthly_buckets)} months")

    # Write each month's file
    for month_key in sorted(monthly_buckets.keys()):
        records = monthly_buckets[month_key]
        out_path = output_dir / f"bubble_{table_name}_{month_key}.json"
        with open(out_path, 'w', encoding='utf-8') as f:
            json.dump(records, f, ensure_ascii=False, default=str)
        log.info(f"  → {out_path.name}: {len(records):,} records")

    # Remove old monolithic file if it exists
    old_path = output_dir / f"bubble_{table_name}.json"
    if old_path.exists():
        old_path.unlink()
        log.info(f"  Removed old {old_path.name} (replaced by monthly files)")


def find_exports(input_dir: Path) -> list[tuple[Path, str]]:
    """Find Keboola export files and determine their table names."""
    found = []

    # Look for files matching patterns:
    #   Events.csv.gz, Events.csv, Candidate.csv.gz, etc.
    #   Also handle Keboola's default naming: sapi-export-*.csv.gz
    for ext_pattern in ['*.csv.gz', '*.csv']:
        for f in sorted(input_dir.glob(ext_pattern)):
            # Skip files that are already in bubble_* format
            if f.name.startswith('bubble_'):
                continue

            # Try to determine table name from filename
            name = f.stem
            if name.endswith('.csv'):
                name = name[:-4]  # Remove .csv from .csv.gz case

            # Known table names from Keboola
            known_tables = [
                "Events", "Candidate", "Talent", "Position", "Company",
                "Emails", "Nylas_Email_message", "duxsoup_messages",
                "Analytic", "stages", "recruiter_screeen_notes",
            ]

            # Match by name (case-insensitive)
            matched = None
            for t in known_tables:
                if name.lower() == t.lower():
                    matched = t
                    break

            if matched:
                found.append((f, matched))
            else:
                log.warning(f"  Skipping {f.name} — unknown table name '{name}'")

    return found


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Convert Keboola CSV exports to JSON")
    parser.add_argument("--input-dir", type=str, default=None,
                        help="Directory containing downloaded CSV/GZIP files")
    parser.add_argument("--file", type=str, default=None,
                        help="Single CSV/GZIP file to convert")
    parser.add_argument("--table", type=str, default=None,
                        help="Table name (required with --file)")
    args = parser.parse_args()

    if args.file:
        if not args.table:
            print("ERROR: --table is required when using --file")
            sys.exit(1)
        convert_csv_to_json(Path(args.file), args.table, DATA_DIR)
    else:
        input_dir = Path(args.input_dir) if args.input_dir else DATA_DIR
        exports = find_exports(input_dir)
        if not exports:
            log.error(f"No CSV export files found in {input_dir}")
            log.info("Expected files like: Events.csv.gz, Candidate.csv.gz, etc.")
            sys.exit(1)

        log.info(f"Found {len(exports)} export files in {input_dir}")
        for path, table in exports:
            convert_csv_to_json(path, table, DATA_DIR)

    log.info("Done! Run 'python3 transform.py' next.")
