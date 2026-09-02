#!/usr/bin/env python3
"""Update the Tribe Bot OpenAPI spec for the new interview funnel (v2 anchors)."""
import json, subprocess, sys

PSQL = ["docker", "exec", "-i", "onyx-relational_db-1", "psql", "-U", "postgres"]

def sql(q, quiet=False):
    r = subprocess.run(PSQL + (["-t", "-A"] if quiet else []) + ["-c", q],
                       capture_output=True, text=True)
    if r.returncode:
        sys.exit("psql failed:\n" + r.stderr[:500])
    return r.stdout

spec = json.loads(sql("select openapi_schema::text from tool where id=9;", quiet=True).strip())
d = spec["paths"]["/query"]["post"]["description"]
before = len(d)
applied, skipped = 0, []

def sub(old, new, label, required=True):
    global d, applied
    if old not in d:
        if required:
            sys.exit(f"REQUIRED ANCHOR NOT FOUND: {label}\n{old[:160]}")
        skipped.append(label); print(f"  --  {label} (not found, skipped)"); return
    d = d.replace(old, new, 1); applied += 1
    print(f"  ok  {label}")

# 1. stage ladder
sub("| Moved to ATS | Interview | Offer | Hired",
    "| Moved to ATS | Interview (old funnel) | Interview 1 | Interview 2 | Interview 3 | Offer | Hired",
    "stage ladder")

# 2. the normalisation paragraph
start = d.find("stage_detail holds the raw label at the Interview rung")
if start == -1:
    sys.exit("REQUIRED ANCHOR NOT FOUND: stage_detail paragraph")
end = d.find("\n\n", start)
sub(d[start:end],
"""TWO FUNNEL GENERATIONS, KEPT SEPARATE ON PURPOSE. Tribe moved to a new interview funnel in
mid-July 2026 and both shapes are live while roles migrate across.
  OLD funnel: Moved to ATS -> Onsite / Culture Interview / Call with Client -> Offer -> Hired
              these land on stage = 'Interview (old funnel)'
  NEW funnel: Moved to ATS -> Interview 1 / Interview 2 / Interview 3 -> Offer -> Hired
              these land on stage = 'Interview 1', 'Interview 2' or 'Interview 3'
NEVER merge the two into a single interview rung. To compare interview drop-off across the
generations, compare 'Interview (old funnel)' against the SUM of Interview 1/2/3, or scope the
question with on_new_pipeline. There is zero crossover: every old-funnel interview row sits on an
old-funnel role and every Interview 1/2/3 row sits on a new-funnel role.
The new-funnel counts are small (roughly Interview 1: 13, Interview 2: 4, Interview 3: 1) only
because that funnel is weeks old. They grow from here while the old-funnel rung shrinks. Do not
describe the new funnel as unused or broken.
on_new_pipeline (boolean) marks a role as running the new funnel. date_int1 / date_int2 / date_int3
hold the date of that specific round and are NULL on old-funnel roles. date_interview is unchanged
and still holds the earliest interview event of either generation.
stage_detail still holds the raw label ('Onsite', 'Culture Interview', 'Interview 1'). Use it only
when someone asks about a specific named round. It is NULL below the interview rungs.""",
    "funnel generations paragraph")

# 3/4. column lists - optional, wording may vary
sub("date_interview date, is_external_recruiter boolean)",
    "date_interview date, date_int1 date, date_int2 date, date_int3 date, on_new_pipeline boolean, is_external_recruiter boolean)",
    "dq column list", required=False)
sub("actual_screens, ats, offered, hired",
    "actual_screens, ats, offered, hired, int1, int2, int3, on_new_pipeline",
    "funnel column list", required=False)

# 5. denominators rule, appended so it does not depend on fragile anchors
d += """

=== INTERVIEW STAGES AND THE TWO FUNNELS ===
dash_funnel gained int1, int2, int3 (candidates reaching Interview 1/2/3 that week) and
on_new_pipeline (true when the job runs the new funnel).

ALWAYS scope interview conversion denominators to on_new_pipeline = true. Dividing by total ATS
includes old-funnel roles that can never reach an interview column, which makes the rate look far
worse than it is:
  SELECT sum(int1)::numeric / nullif(sum(ats),0) FROM dash_funnel WHERE on_new_pipeline

A 0 in an interview column on an OLD-funnel role does NOT mean no interviews happened. The stage
does not exist on that role. Say "not applicable", never "zero". The dashboard renders a dash
rather than a 0 for exactly this reason.

Interviews only ever appear on rows where on_new_pipeline is true. If you see one elsewhere,
something is wrong with the data, so say so rather than reporting it."""
applied += 1
print("  ok  interview denominators section")

spec["paths"]["/query"]["post"]["description"] = d
spec["info"]["version"] = "2.4.0"
payload = json.dumps(spec).replace("'", "''")
sql(f"update tool set openapi_schema = '{payload}'::jsonb where id = 9;")
print(f"\n{applied} edits applied, {len(skipped)} skipped. {before} -> {len(d)} chars. Now 2.4.0.")
if skipped:
    print("Skipped (tell Blake): " + ", ".join(skipped))
