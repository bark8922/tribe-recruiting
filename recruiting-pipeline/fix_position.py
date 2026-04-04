#!/usr/bin/env python3
"""Streaming Position CSV → JSON. Writes JSONL then converts to array."""
import csv, gzip, json, logging, os, sys
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("fix_position")

DATA_DIR = Path(os.environ.get("RECRUIT_DATA_DIR", Path(__file__).parent / "data"))
CSV_PATH = DATA_DIR / "Position.csv.gz"
TMP_PATH = DATA_DIR / ".tmp_position.jsonl"
OUT_PATH = DATA_DIR / "bubble_Position.json"

def main():
    log.info(f"Streaming {CSV_PATH.name} → JSONL...")
    total = 0
    with gzip.open(CSV_PATH, 'rt', encoding='utf-8', errors='replace') as f_in, \
         open(TMP_PATH, 'w', encoding='utf-8') as f_out:
        reader = csv.DictReader(f_in)
        for row in reader:
            rec = {k: (v if v != "" else None) for k, v in row.items()}
            f_out.write(json.dumps(rec, ensure_ascii=False, default=str) + "\n")
            total += 1
            if total % 1_000_000 == 0:
                log.info(f"  Processed {total:,} rows...")

    log.info(f"  {total:,} total rows. Converting JSONL → JSON array...")
    with open(OUT_PATH, 'w', encoding='utf-8') as out_f:
        out_f.write("[\n")
        first = True
        with open(TMP_PATH, 'r', encoding='utf-8') as in_f:
            for line in in_f:
                if not first:
                    out_f.write(",\n")
                out_f.write(line.rstrip("\n"))
                first = False
        out_f.write("\n]")

    TMP_PATH.unlink()
    log.info(f"  → {OUT_PATH.name}: {total:,} records. Done!")

if __name__ == "__main__":
    main()
