#!/usr/bin/env python3
"""
Streaming Events CSV → monthly JSON converter.
Single-pass: writes each record to a per-month temp file (JSONL),
then converts each JSONL → JSON array. Keeps memory minimal.
"""
import csv
import gzip
import json
import logging
import os
import sys
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("fix_events")

DATA_DIR = Path(os.environ.get("RECRUIT_DATA_DIR", Path(__file__).parent / "data"))
CSV_PATH = DATA_DIR / "Events.csv.gz"

def main():
    if not CSV_PATH.exists():
        log.error(f"Events CSV not found at {CSV_PATH}")
        sys.exit(1)

    log.info(f"Streaming {CSV_PATH.name} → per-month JSONL files...")

    file_handles = {}
    month_counts = {}
    total = 0

    try:
        with gzip.open(CSV_PATH, 'rt', encoding='utf-8', errors='replace') as f:
            reader = csv.DictReader(f)
            for row in reader:
                rec = {k: (v if v != "" else None) for k, v in row.items()}
                created = rec.get("Created_Date", "") or ""
                month_key = created[:7].replace("-", "") if len(created) >= 7 else "unknown"

                if month_key not in file_handles:
                    tmp_path = DATA_DIR / f".tmp_events_{month_key}.jsonl"
                    file_handles[month_key] = open(tmp_path, 'w', encoding='utf-8')
                    month_counts[month_key] = 0

                file_handles[month_key].write(json.dumps(rec, ensure_ascii=False, default=str) + "\n")
                month_counts[month_key] += 1
                total += 1

                if total % 2_000_000 == 0:
                    log.info(f"  Processed {total:,} rows...")
    finally:
        for fh in file_handles.values():
            fh.close()

    log.info(f"  Processed {total:,} total rows across {len(month_counts)} months")

    for month_key in sorted(month_counts.keys()):
        tmp_path = DATA_DIR / f".tmp_events_{month_key}.jsonl"
        out_path = DATA_DIR / f"bubble_Events_{month_key}.json"
        count = month_counts[month_key]

        log.info(f"  Converting {month_key}: {count:,} records → {out_path.name}")

        with open(out_path, 'w', encoding='utf-8') as out_f:
            out_f.write("[\n")
            with open(tmp_path, 'r', encoding='utf-8') as in_f:
                first = True
                for line in in_f:
                    if not first:
                        out_f.write(",\n")
                    out_f.write(line.rstrip("\n"))
                    first = False
            out_f.write("\n]")

        tmp_path.unlink()

    old = DATA_DIR / "bubble_Events.json"
    if old.exists():
        old.unlink()
        log.info(f"Removed old bubble_Events.json")

    log.info("Events conversion done!")
    log.info(f"Monthly breakdown: {json.dumps({k: f'{v:,}' for k, v in sorted(month_counts.items())}, indent=2)}")

if __name__ == "__main__":
    main()
