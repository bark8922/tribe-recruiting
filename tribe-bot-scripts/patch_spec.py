#!/usr/bin/env python3
"""Update the Tribe Bot OpenAPI spec for the new interview funnel.

Reads the live spec out of Onyx's Postgres, applies targeted edits, writes it
back. Every edit is asserted, so if the source text has drifted the script
stops without writing anything rather than half-applying.
"""
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
edits = 0

def sub(old, new, label):
    global d, edits
    if old not in d:
        sys.exit(f"ANCHOR NOT FOUND: {label}\nLooked for:\n{old[:200]}")
    d = d.replace(old, new, 1)
    edits += 1
    print(f"  ok  {label}")

# 1. stage ladder: split the collapsed Interview rung
sub("""    'Interview'     - reached a client interview""",
    """    'Interview (old funnel)' - reached an interview on an OLD-funnel role (Onsite, Culture Interview, Call with Client)
    'Interview 1' / 'Interview 2' / 'Interview 3' - reached that specific round on a NEW-funnel role""",
    "stage ladder split")

# 2. replace the normalisation paragraph
old_norm_start = "  TWO FUNNEL GENERATIONS."
i = d.find(old_norm_start)
if i == -1:
    sys.exit("ANCHOR NOT FOUND: two funnel generations paragraph")
j = d.find("\n\n", i)
old_norm = d[i:j]
new_norm = """  TWO FUNNEL GENERATIONS, KEPT SEPARATE ON PURPOSE. Tribe moved to a new interview funnel in
  mid-July 2026 and both shapes are live while roles migrate over.
    OLD funnel: Moved to ATS -> Onsite / Culture Interview / Call with Client -> Offer -> Hired
                all land on stage = 'Interview (old funnel)'
    NEW funnel: Moved to ATS -> Interview 1 / Interview 2 / Interview 3 -> Offer -> Hired
                land on stage = 'Interview 1', 'Interview 2', 'Interview 3'
  NEVER merge these into one interview rung. To compare interview drop-off across the two,
  compare 'Interview (old funnel)' against the SUM of Interview 1/2/3, or scope with
  on_new_pipeline. There is zero crossover: every old-funnel interview row sits on an
  old-funnel role and every Interview 1/2/3 row sits on a new-funnel role.
  The new-funnel counts are small (Interview 1: 13, Interview 2: 4, Interview 3: 1) only because
  that funnel is weeks old. They grow from here and the old-funnel rung shrinks. Do not describe
  the new funnel as unused.
  on_new_pipeline (boolean) marks a role as running the new funnel. date_int1 / date_int2 /
  date_int3 hold the date of that round and are NULL on old-funnel roles.
  date_interview is unchanged and still holds the earliest interview event of either generation."""
d = d.replace(old_norm, new_norm, 1); edits += 1
print("  ok  funnel generations paragraph")

# 3. dash_candidate_dq column list
sub("date_interview date, is_external_recruiter boolean)",
    "date_interview date, date_int1 date, date_int2 date, date_int3 date, on_new_pipeline boolean, is_external_recruiter boolean)",
    "dq column list")

# 4. dash_funnel column list
sub("viewed, contacted, reacted, positive_response, screens, actual_screens, ats, offered, hired",
    "viewed, contacted, reacted, positive_response, screens, actual_screens, ats, offered, hired, int1, int2, int3, on_new_pipeline",
    "funnel column list")

# 5. funnel guidance
sub("""  Contacts per hire:      sum(contacted)::numeric / nullif(sum(hired),0)""",
    """  int1 / int2 / int3 are candidates reaching Interview 1/2/3 that week, on NEW-funnel roles only.
  on_new_pipeline is true when the job runs the new funnel.
  A 0 in an interview column on an OLD-funnel role does NOT mean no interviews happened: the stage
  does not exist there. Say "not applicable", never "zero". The dashboard shows a dash for this reason.
  ALWAYS scope interview conversion denominators to on_new_pipeline = true. Dividing by total ATS
  includes old-funnel roles that can never reach an interview column and makes the rate look far
  worse than it is:
    SELECT sum(int1)::numeric / nullif(sum(ats),0) FROM dash_funnel WHERE on_new_pipeline

  Contacts per hire:      sum(contacted)::numeric / nullif(sum(hired),0)""",
    "funnel interview guidance")

spec["paths"]["/query"]["post"]["description"] = d
spec["info"]["version"] = "2.4.0"

payload = json.dumps(spec).replace("'", "''")
sql(f"update tool set openapi_schema = '{payload}'::jsonb where id = 9;")
print(f"\n{edits} edits applied. {before} -> {len(d)} chars. Spec now 2.4.0.")
