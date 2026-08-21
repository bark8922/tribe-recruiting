"""keboola_entry.py -- Role Pipeline Tracker data writer.

Reads two pre-aggregated tables (mapped as input CSVs) and PUTs
recruiting-dashboard/public/role-tracker/data.json to bark8922/tribe-recruiting
via the GitHub Contents API:

  role_tracker_summary    -> "rows"        one row per role ON the new
                                           Interview 1/2/3 pipeline. Powers the
                                           Roles and Owners tabs.
  role_tracker_open_roles -> "open_roles"  one row per NON-ARCHIVED role whether
                                           or not it uses the new pipeline.
                                           Powers the Coverage tab. Carries
                                           job_id, which the page never renders
                                           but does include in its CSV export.

The coverage table is OPTIONAL. If its CSV is not mapped or fails to parse we
log a warning and still write "rows", so the Roles/Owners tabs can never be
broken by a problem in the newer dataset.

Sibling of the sourcing and calls dashboard writers. Runs in the 4x/day
recruiting Flow after both transformations. Data is tiny so the Contents API is
used (same approach as the sourcing writer).
"""
import base64
import csv
import json
import os
import urllib.error
import urllib.request
from datetime import datetime, timezone

from keboola.component import CommonInterface

REPO = "bark8922/tribe-recruiting"
TARGET_PATH = "recruiting-dashboard/public/role-tracker/data.json"
INT_FIELDS = ["total", "contacted", "pos_resp", "screen", "ats",
              "int1", "int2", "int3", "offer", "hired", "archived"]
COVERAGE_STR_FIELDS = ["job_id", "role", "client", "owner", "opened",
                       "role_type", "last_activity"]
COVERAGE_INT_FIELDS = ["days_open", "on_new_pipeline", "candidates",
                       "days_since_activity", "quiet_days"]


def gh(url, token, method="GET", data=None):
    req = urllib.request.Request(url, data=data, method=method, headers={
        "Authorization": "Bearer " + token,
        "Accept": "application/vnd.github+json",
        "User-Agent": "keboola-role-tracker",
    })
    return urllib.request.urlopen(req, timeout=30)


def put_file(token, path, content_str):
    api = "https://api.github.com/repos/" + REPO + "/contents/" + path
    sha = None
    try:
        with gh(api + "?ref=main", token) as resp:
            sha = json.loads(resp.read().decode("utf-8")).get("sha")
    except urllib.error.HTTPError as e:
        if e.code != 404:
            raise
    b64 = base64.b64encode(content_str.encode("utf-8")).decode("ascii")
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    payload = {"message": "refresh: role tracker data (" + now + ")",
               "content": b64, "branch": "main"}
    if sha:
        payload["sha"] = sha
    data = json.dumps(payload).encode("utf-8")
    with gh(api, token, method="PUT", data=data) as resp:
        print("[role-tracker] PUT HTTP " + str(resp.status), flush=True)


def read_coverage(tables_in_path):
    """Read role_tracker_open_roles.csv. Best-effort: never raises."""
    src = os.path.join(tables_in_path, "role_tracker_open_roles.csv")
    if not os.path.exists(src):
        print("[role-tracker] coverage CSV not mapped, skipping open_roles",
              flush=True)
        return []
    rows = []
    try:
        with open(src, newline="", encoding="utf-8") as f:
            for r in csv.DictReader(f):
                obj = {k: (r.get(k) or "") for k in COVERAGE_STR_FIELDS}
                obj["opened"] = obj["opened"][:10]
                obj["last_activity"] = obj["last_activity"][:10]
                for k in COVERAGE_INT_FIELDS:
                    raw = r.get(k)
                    if raw is None or str(raw).strip() == "":
                        obj[k] = None if k in ("days_since_activity", "quiet_days") else 0
                        continue
                    try:
                        obj[k] = int(float(raw))
                    except (TypeError, ValueError):
                        obj[k] = None if k in ("days_since_activity", "quiet_days") else 0
                rows.append(obj)
    except Exception as exc:
        print("[role-tracker] WARNING coverage read failed: " + str(exc),
              flush=True)
        return []
    rows.sort(key=lambda x: (x["role_type"] != "Client", -(x["days_open"] or 0)))
    return rows


def main():
    ci = CommonInterface()
    params = ci.configuration.parameters
    token = params.get("#github_token") or params.get("user_properties", {}).get("#github_token")
    if not token:
        raise RuntimeError("Missing #github_token in configuration parameters.")

    src = os.path.join(ci.tables_in_path, "role_tracker_summary.csv")
    rows = []
    with open(src, newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            obj = {
                "role": r.get("role", "") or "",
                "client": r.get("client", "") or "",
                "owner": r.get("owner", "") or "",
                "opened": (r.get("opened", "") or "")[:10],
            }
            for k in INT_FIELDS:
                try:
                    obj[k] = int(float(r.get(k) or 0))
                except (TypeError, ValueError):
                    obj[k] = 0
            rows.append(obj)

    rows.sort(key=lambda x: x["total"], reverse=True)
    open_roles = read_coverage(ci.tables_in_path)
    payload = {
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        "rows": rows,
        "open_roles": open_roles,
    }
    put_file(token, TARGET_PATH, json.dumps(payload, ensure_ascii=False))
    print("[role-tracker] wrote " + str(len(rows)) + " roles, "
          + str(len(open_roles)) + " open roles", flush=True)


main()
