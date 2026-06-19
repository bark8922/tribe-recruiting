#!/usr/bin/env python3
"""
sync_google_sheet.py — Pull all tabs from Andy's WBR Target Google Sheet
and save them as CSVs in wbr_static/

Usage:
  1. One-time setup (see SETUP below)
  2. python sync_google_sheet.py                    # exports all tabs
  3. python sync_google_sheet.py --tab "TA Weekly Note"  # export one tab

The script uses a Google service account to authenticate. The service account
email must be added as a viewer/editor on the Google Sheet.

SETUP:
  1. Go to https://console.cloud.google.com/
  2. Create a project (or use existing) -> Enable "Google Sheets API"
  3. Go to "Service Accounts" -> Create one -> Download JSON key
  4. Save the JSON key as: wbr_static/google_service_account.json
  5. Copy the service account email (looks like: xxx@project.iam.gserviceaccount.com)
  6. Open the WBR Target Google Sheet -> Share -> Add the service account email as Viewer
  7. Run this script!

For n8n automation: add a Python node that runs this script before the main pipeline.
"""

import argparse
import csv
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

try:
    import gspread
    from google.oauth2.service_account import Credentials
except ImportError:
    print("ERROR: Missing dependencies. Run: pip install gspread google-auth")
    sys.exit(1)

# ─── Config ───────────────────────────────────────────────────────────────────
SHEET_ID = "1Hyb5M244bkh9ygxnssq_wppWxrT0KtGeVFEYvHVoouc"
STATIC_DIR = Path(__file__).parent / "wbr_static"
CREDS_FILE = STATIC_DIR / "google_service_account.json"

# Map sheet tab names -> output CSV filenames
TAB_MAP = {
    "TA Target":       "wbr_ta_target.csv",
    "TA Weekly Note":  "wbr_ta_weekly_note.csv",
    "TS Weekly Note":  "wbr_ts_weekly.csv",
    "IR":              "wbr_ir.csv",
    "Reasoning Guidance": "wbr_reasoning_guidance.csv",
    "Instruction":     None,   # skip — just instructions, not data
    "Data cleanup":    None,   # skip — internal cleanup notes
}

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets.readonly",
    "https://www.googleapis.com/auth/drive.readonly",
]


def get_client() -> gspread.Client:
    """Authenticate with Google using service account credentials."""
    if not CREDS_FILE.exists():
        print(f"ERROR: Credentials file not found at {CREDS_FILE}")
        print("See SETUP instructions at top of this script.")
        sys.exit(1)

    creds = Credentials.from_service_account_file(str(CREDS_FILE), scopes=SCOPES)
    return gspread.authorize(creds)


def export_tab(worksheet: gspread.Worksheet, output_path: Path) -> int:
    """Export a single worksheet tab to CSV. Returns row count."""
    data = worksheet.get_all_values()
    if not data:
        print(f"  WARNING: Empty tab, skipping")
        return 0

    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        for row in data:
            writer.writerow(row)

    return len(data) - 1  # subtract header row


def main():
    parser = argparse.ArgumentParser(description="Sync WBR Target Google Sheet -> CSVs")
    parser.add_argument("--tab", help="Export only this tab (by name)")
    parser.add_argument("--list-tabs", action="store_true", help="Just list tab names")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be exported")
    args = parser.parse_args()

    print(f"[{datetime.now(timezone.utc).isoformat()}] Connecting to Google Sheets...")
    client = get_client()
    spreadsheet = client.open_by_key(SHEET_ID)

    # List all tabs
    worksheets = spreadsheet.worksheets()
    tab_names = [ws.title for ws in worksheets]

    if args.list_tabs:
        print(f"Sheet: {spreadsheet.title}")
        print(f"Tabs ({len(tab_names)}):")
        for name in tab_names:
            mapped = TAB_MAP.get(name, "(unmapped - would be skipped)")
            if mapped is None:
                mapped = "(skipped)"
            print(f"  - {name} -> {mapped}")
        return

    # Determine which tabs to export
    if args.tab:
        tabs_to_export = {args.tab: TAB_MAP.get(args.tab, f"wbr_{args.tab.lower().replace(' ', '_')}.csv")}
    else:
        tabs_to_export = {name: filename for name, filename in TAB_MAP.items() if filename is not None}

    STATIC_DIR.mkdir(parents=True, exist_ok=True)

    total_rows = 0
    for tab_name, csv_filename in tabs_to_export.items():
        print(f"\n  [export] {tab_name} -> {csv_filename}")

        if args.dry_run:
            print(f"     (dry run — would export to {STATIC_DIR / csv_filename})")
            continue

        try:
            ws = spreadsheet.worksheet(tab_name)
        except gspread.WorksheetNotFound:
            print(f"  WARNING: Tab '{tab_name}' not found in sheet. Available: {tab_names}")
            continue

        output_path = STATIC_DIR / csv_filename
        rows = export_tab(ws, output_path)
        total_rows += rows
        print(f"     OK: {rows} data rows exported")

    if not args.dry_run:
        # Write sync metadata
        meta = {
            "synced_at": datetime.now(timezone.utc).isoformat(),
            "sheet_id": SHEET_ID,
            "sheet_title": spreadsheet.title,
            "tabs_exported": list(tabs_to_export.keys()),
            "total_rows": total_rows,
        }
        meta_path = STATIC_DIR / "google_sheet_sync.json"
        with open(meta_path, "w") as f:
            json.dump(meta, f, indent=2)

        print(f"\nDONE: Done. {total_rows} total rows exported to {STATIC_DIR}/")
        print(f"   Sync metadata saved to {meta_path}")


if __name__ == "__main__":
    main()
