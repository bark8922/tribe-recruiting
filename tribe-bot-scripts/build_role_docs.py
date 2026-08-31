#!/usr/bin/env python3
"""
build_role_docs.py -- push "role story" documents into Onyx.

One document per role that has weekly sourcing updates: a minimal factual
header (including job_id), the intake brief if one is linked, then every
weekly update in date order.

Deliberately does NOT copy any metrics (rejection reasons, time to hire,
funnel counts) into the document. Those live in Supabase and are queried
live by the bot's SQL tool. Duplicating them here would go stale between
refreshes and give two different answers to the same question.

The header carries job_id precisely so the bot can run those queries against
an unambiguous key rather than matching on job title, which is not unique
across clients.

Also pushes standalone documents for intake records that have real text but
no job link, so they are at least searchable.

Env required (put in /root/.dash_env):
    SUPABASE_SECRET_KEY   sb_secret_...
    ONYX_API_KEY          on_...
"""
import json, os, re, sys, time, urllib.request, urllib.error
from collections import defaultdict

SUPABASE_URL = "https://garopkilxgpcmlkiqvbg.supabase.co"
ONYX_INGEST = "http://localhost/api/onyx-api/ingestion"
SB_KEY = os.environ.get("SUPABASE_SECRET_KEY", "")
ONYX_KEY = os.environ.get("ONYX_API_KEY", "")
MAX_INTAKE_CHARS = 8000


def sb(path):
    url = f"{SUPABASE_URL}/rest/v1/{path}"
    h = {"apikey": SB_KEY, "Accept": "application/json"}
    if not SB_KEY.startswith("sb_"):
        h["Authorization"] = "Bearer " + SB_KEY
    req = urllib.request.Request(url, headers=h)
    with urllib.request.urlopen(req, timeout=120) as r:
        return json.loads(r.read())


def push(doc):
    body = json.dumps({"document": doc}).encode()
    req = urllib.request.Request(ONYX_INGEST, data=body, method="POST", headers={
        "Authorization": "Bearer " + ONYX_KEY,
        "Content-Type": "application/json",
    })
    try:
        with urllib.request.urlopen(req, timeout=120) as r:
            return r.status
    except urllib.error.HTTPError as e:
        print(f"  FAILED {doc['id']}: HTTP {e.code} {e.read().decode()[:200]}")
        return None


def clean(v):
    """Flatten a jsonb/text field into readable prose."""
    if v is None:
        return ""
    if isinstance(v, (dict, list)):
        v = json.dumps(v, ensure_ascii=False)
    s = str(v).strip()
    if s in ("", "null", "{}", "[]"):
        return ""
    # strip hiring manager contact details wherever they appear
    s = re.sub(r"[\w\.\-\+]+@[\w\.\-]+\.\w+", "[email removed]", s)
    s = re.sub(r"https?://(www\.)?linkedin\.com/\S+", "[linkedin removed]", s)
    return s


def main():
    if not SB_KEY or not ONYX_KEY:
        raise SystemExit("Set SUPABASE_SECRET_KEY and ONYX_API_KEY first.")

    print("Fetching...")
    jobs = {j["job_id"]: j for j in sb(
        "dash_jobs?select=job_id,client_name,job_title,job_recruiter,job_sourcer,"
        "job_category,job_location,is_job_archived,date_created&limit=5000")}
    updates = sb("weekly_updates?select=*&is_submitted=eq.true&order=period_end.asc&limit=2000")
    intakes = sb("intake_records?select=*&limit=1000")
    print(f"  {len(jobs)} jobs, {len(updates)} updates, {len(intakes)} intake records")

    intake_by_job = {}
    for i in intakes:
        jid = i.get("job_id") or i.get("bubble_job_id")
        if jid and clean(i.get("doc_text")):
            intake_by_job.setdefault(jid, i)

    by_job = defaultdict(list)
    for u in updates:
        if u.get("job_id"):
            by_job[u["job_id"]].append(u)

    pushed = 0

    # ---------- one document per role ----------
    for jid, ups in sorted(by_job.items()):
        j = jobs.get(jid, {})
        title = j.get("job_title") or "Unknown role"
        client = j.get("client_name") or "Unknown client"

        L = []
        L.append(f"# {title} at {client} - sourcing history")
        L.append("")
        L.append("This document is the written history of how this role was sourced: the intake")
        L.append("brief and the weekly updates the sourcer wrote. It contains no pipeline metrics.")
        L.append("For counts, rejection reasons, time to hire or funnel numbers, query the")
        L.append("recruiting database using the job_id below. Do NOT match on job title:")
        L.append("titles repeat across clients and matching on them mixes different roles together.")
        L.append("")
        L.append("## Role")
        L.append(f"- job_id: {jid}")
        L.append(f"- Client: {client}")
        L.append(f"- Title: {title}")
        if j.get("job_recruiter"):
            L.append(f"- Recruiter: {j['job_recruiter']}")
        if j.get("job_sourcer"):
            L.append(f"- Dedicated sourcer: {j['job_sourcer']}")
        if j.get("job_location"):
            L.append(f"- Location: {j['job_location']}")
        if j.get("job_category"):
            L.append(f"- Category: {j['job_category']}")
        L.append(f"- Status: {'archived / closed' if j.get('is_job_archived') else 'live'}")
        L.append("")

        intake = intake_by_job.get(jid)
        if intake:
            txt = clean(intake.get("doc_text"))[:MAX_INTAKE_CHARS]
            L.append("## Intake brief")
            if intake.get("call_date"):
                L.append(f"Intake call: {str(intake['call_date'])[:10]}")
            L.append("")
            L.append(txt)
            if intake.get("google_doc_url"):
                L.append("")
                L.append(f"Full intake doc: {intake['google_doc_url']}")
            L.append("")

        L.append("## Weekly sourcing updates")
        L.append("")
        for u in ups:
            wk = u.get("period_end") or f"{u.get('iso_year')} week {u.get('iso_week')}"
            L.append(f"### Week ending {wk}")
            for label, key in (("Pipeline", "pipeline_snapshot"),
                               ("Blockers and drop-off", "blocker_and_dropoff"),
                               ("What was tried", "experiments_this_week"),
                               ("Risks and recommendations", "risks_and_recommendations")):
                val = clean(u.get(key))
                if val:
                    L.append(f"**{label}:** {val}")
            L.append("")

        doc = {
            "id": f"tribe-role-story-{jid}",
            "semantic_identifier": f"{title} at {client} - sourcing history",
            "title": f"{title} at {client} - sourcing history",
            "sections": [{"text": "\n".join(L)}],
            "source": "file",
            "metadata": {"category": "sourcing", "client": client,
                         "tags": ["sourcing history", "weekly update", "intake"]},
            "doc_updated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        }
        if push(doc):
            pushed += 1

    # ---------- unlinked intake records ----------
    for i in intakes:
        jid = i.get("job_id") or i.get("bubble_job_id")
        if jid and jid in by_job:
            continue
        txt = clean(i.get("doc_text"))
        if len(txt) < 200:
            continue
        when = str(i.get("call_date") or "")[:10]
        fields = clean(i.get("extracted_fields"))
        L = [f"# Intake call {when}", "",
             "Intake notes not yet linked to a role in the recruiting database.", ""]
        if fields:
            L += ["## Extracted details", fields, ""]
        L += ["## Notes", txt[:MAX_INTAKE_CHARS]]
        if i.get("google_doc_url"):
            L += ["", f"Full doc: {i['google_doc_url']}"]
        doc = {
            "id": f"tribe-intake-{i['id']}",
            "semantic_identifier": f"Intake call {when}",
            "title": f"Intake call {when}",
            "sections": [{"text": "\n".join(L)}],
            "source": "file",
            "metadata": {"category": "intake", "tags": ["intake", "role briefing"]},
            "doc_updated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        }
        if push(doc):
            pushed += 1

    print(f"\nPushed {pushed} documents to Onyx.")


if __name__ == "__main__":
    main()
