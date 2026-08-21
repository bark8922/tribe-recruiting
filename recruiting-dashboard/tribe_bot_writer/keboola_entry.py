"""keboola_entry.py -- Tribe Bot candidate disqualification data writer.

Reads one pre-aggregated table (mapped as an input CSV) and PUTs
recruiting-dashboard/public/tribe-bot/candidate_dq.json.gz to
bark8922/tribe-recruiting via the GitHub Contents API:

  candidate_dq_by_stage -> "rows"   one row per disqualified candidate from
                                    2026 onwards, carrying BOTH the stage the
                                    candidate reached and the disqualification
                                    reason.

WHY THIS EXISTS
Existing recruiting outputs hold disqualification reasons with no stage
dimension (drops_by_sourcer, drops_by_recruiter, dq_reasons) or stage counts
with no reasons (project_dashboard funnel). Neither can answer "how many
candidates were rejected at the Actual Screen stage, and why".

The stage ladder keeps "Screen Booked" (recruiter screen scheduled, no
evaluation ever logged) separate from "Actual Screen" (evaluation logged, then
rejected). Those two behave very differently: booked-only drops are dominated
by Unresponsive / No Show / Dropped out, while Actual Screen drops are
dominated by Skills / Salary / Culture Fit. Collapsing them hides the signal.

CONSUMER
Not read by the recruiting dashboard. Fetched by the Onyx "Tribe Bot" loader on
the DigitalOcean droplet (/root/load_candidates_dq.py), which loads it into
Supabase for the bot's read-only SQL tool.

Output is gzipped: ~19k rows/year, same approach as finder_data.json.gz.

Sibling of the role tracker, sourcing and calls writers. Runs once daily in
flow 01m0hqmdvtt98sseg7jnb1n547 at 09:00 Europe/Prague, isolated so a failure
here can never affect the main dashboard push.
"""
import base64
import csv
import gzip
import json
import os
import urllib.error
import urllib.request
from datetime import datetime, timezone

from keboola.component import CommonInterface

REPO = "bark8922/tribe-recruiting"
TARGET_PATH = "recruiting-dashboard/public/tribe-bot/candidate_dq.json.gz"
SOURCE_CSV = "candidate_dq_by_stage.csv"

STR_FIELDS = ["candidate_id", "job_id", "job_title", "client_name",
              "job_recruiter", "job_sourcer", "candidate_sourcer",
              "job_category", "job_subcategory",
              "stage", "stage_detail", "reason"]
DATE_FIELDS = ["dq_date", "date_contacted", "date_screen", "date_screen_actual",
               "date_ats", "date_interview"]


def gh(url, token, method="GET", data=None):
    req = urllib.request.Request(url, data=data, method=method, headers={
        "Authorization": "Bearer " + token,
        "Accept": "application/vnd.github+json",
        "User-Agent": "keboola-tribe-bot",
    })
    return urllib.request.urlopen(req, timeout=60)


def put_file(token, path, content_bytes):
    api = "https://api.github.com/repos/" + REPO + "/contents/" + path
    sha = None
    try:
        with gh(api + "?ref=main", token) as resp:
            sha = json.loads(resp.read().decode("utf-8")).get("sha")
    except urllib.error.HTTPError as e:
        if e.code != 404:
            raise
    b64 = base64.b64encode(content_bytes).decode("ascii")
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    payload = {"message": "refresh: tribe bot candidate DQ (" + now + ")",
               "content": b64, "branch": "main"}
    if sha:
        payload["sha"] = sha
    data = json.dumps(payload).encode("utf-8")
    with gh(api, token, method="PUT", data=data) as resp:
        print("[tribe-bot] PUT HTTP " + str(resp.status), flush=True)


def main():
    ci = CommonInterface()
    params = ci.configuration.parameters
    token = params.get("#github_token") or params.get("user_properties", {}).get("#github_token")
    if not token:
        raise RuntimeError("Missing #github_token in configuration parameters.")

    src = os.path.join(ci.tables_in_path, SOURCE_CSV)
    if not os.path.exists(src):
        raise RuntimeError("Input CSV not mapped: " + SOURCE_CSV)

    rows = []
    with open(src, newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            obj = {k: (r.get(k) or "") or None for k in STR_FIELDS}
            for k in DATE_FIELDS:
                v = (r.get(k) or "")[:10]
                obj[k] = v or None
            ext = (r.get("is_external_recruiter") or "").strip().lower()
            obj["is_external_recruiter"] = ext in ("true", "1", "yes")
            rows.append(obj)

    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "rows": rows,
    }
    raw = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    gz = gzip.compress(raw, compresslevel=9)
    put_file(token, TARGET_PATH, gz)
    print("[tribe-bot] wrote " + str(len(rows)) + " rows, "
          + str(len(gz) // 1024) + " KB gzipped", flush=True)


main()
