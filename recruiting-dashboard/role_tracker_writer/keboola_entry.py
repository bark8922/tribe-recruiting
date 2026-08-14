"""keboola_entry.py -- Role Pipeline Tracker data writer.

Reads the pre-aggregated `role_tracker_summary` table (mapped as an input CSV)
and PUTs recruiting-dashboard/public/role-tracker/data.json to
bark8922/tribe-recruiting via the GitHub Contents API. Sibling of the sourcing
and calls dashboard writers. Runs in the 4x/day recruiting Flow after the
`role_tracker_summary` transformation. Data is tiny so the Contents API is used
(same approach as the sourcing writer).
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
    payload = {
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        "rows": rows,
    }
    put_file(token, TARGET_PATH, json.dumps(payload, ensure_ascii=False))
    print("[role-tracker] wrote " + str(len(rows)) + " roles", flush=True)


main()
