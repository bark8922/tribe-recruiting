# Handover to the Onyx / Tribe Bot session

**Date:** 2026-09-02
**Supabase project:** `garopkilxgpcmlkiqvbg` (tribe-job-intel)
**What I need from you:** one change to the Onyx loader on the DigitalOcean droplet. The Supabase
schema is already done. Nothing upstream needs to change.

---

## TL;DR

The recruiting funnel gained three new stages, **Interview 1, Interview 2, Interview 3**, sitting
between Move to ATS and Offer. They are live everywhere in the dashboard as of today.

Two Tribe Bot tables need to carry them. **I have already added all eight columns to Supabase.**
They are nullable with no defaults, so nothing is broken and today's load is unaffected. They will
simply stay NULL until the loader sends values.

**One of the two improvements needs nothing from you and lands automatically tomorrow.** See
"Already working" below before you plan any work.

---

## Background: two funnel generations coexist

Tribe migrated to a new interview funnel in **mid-July 2026**. Both shapes are live right now:

- **OLD funnel:** ... Move to ATS, then Onsite / Culture Interview / Call with Client, then Offer, Hired
- **NEW funnel:** ... Move to ATS, then Interview 1, Interview 2, Interview 3, then Offer, Hired

Roles migrate over time, so this is transitional and will resolve itself over the next few months.
Until then, **the two generations are deliberately kept separate everywhere.** They are not merged
into a single "Interview" rung, because the whole point is to be able to see the difference.

The flag that tells them apart is **`on_new_pipeline`** (boolean, per job). It comes from the role
tracker's own definition, so the bot and the dashboard agree on which roles are new-funnel.

---

## Already working, no action needed

`dash_candidate_dq.stage` changed today. It previously had a single collapsed rung called
`Interview`. It now has:

| stage value | meaning |
|---|---|
| `Interview (old funnel)` | reached an old-funnel interview (Onsite, Culture Interview, Call with Client) |
| `Interview 1` / `Interview 2` / `Interview 3` | reached that specific round on a new-funnel role |

**These are new VALUES in the existing `stage` text column, not new columns**, so they flow through
your current loader with zero changes. They land in Supabase on the next daily cycle
(transform 09:00, your load 10:00 Europe/Prague).

Expected distribution once loaded, out of 92,397 rows:

| stage | rows |
|---|---|
| Contacted | 63,048 |
| Actual Screen | 17,124 |
| Moved to ATS | 5,934 |
| Screen Booked | 4,054 |
| **Interview (old funnel)** | **1,394** |
| Sourced | 398 |
| Offer | 368 |
| Hired | 59 |
| **Interview 1** | **13** |
| **Interview 2** | **4** |
| **Interview 3** | **1** |

The new-funnel numbers are small only because the funnel is six weeks old. They grow from here and
`Interview (old funnel)` shrinks as roles migrate. **Row count is unchanged at 92,397 and no other
rung moved**, so if you see a different total after the load, something else is wrong.

---

## What I need you to change

Add these fields to the two loaders' insert. Both source files are in `bark8922/tribe-recruiting`
on `main`.

### 1. `dash_funnel`

Source: `recruiting-dashboard/public/dashboard_data_snowflake.json.gz`
JSON path: `project_dashboard.rows[]`

| Supabase column | JSON key | type | notes |
|---|---|---|---|
| `int1` | `int1` | integer | candidates moved to Interview 1 that week |
| `int2` | `int2` | integer | |
| `int3` | `int3` | integer | |
| `on_new_pipeline` | `on_new_pipeline` | boolean | true if the job runs the new funnel |

All four keys are **already present in the published file** as of today. Sanity figures in the
current file: 29,175 rows, **500 rows flagged `on_new_pipeline = true`**, and **zero interviews on
rows where the flag is false** (a useful invariant to assert after loading).

### 2. `dash_candidate_dq`

Source: `recruiting-dashboard/public/tribe-bot/candidate_dq.json.gz`
JSON path: `rows[]`

| Supabase column | JSON key | type |
|---|---|---|
| `date_int1` | `date_int1` | date |
| `date_int2` | `date_int2` | date |
| `date_int3` | `date_int3` | date |
| `on_new_pipeline` | `on_new_pipeline` | boolean |

These four keys appear in the file from **tomorrow's 09:00 build onward**, not in today's.
`date_interview` is unchanged and still holds the earliest interview-band event of either
generation, kept for backward compatibility.

---

## The one risk, please check this first

I added the columns as **nullable with no defaults**, which is safe for a loader that inserts **by
name** (dicts, or an explicit column list). It would break a loader that inserts **positionally**,
because the row tuples would now be shorter than the table.

I could prove `dash_candidate_dq`'s loader is name-based: it has a serial `id` column with a
`nextval` default that the loader cannot be supplying. **I could not prove it for `dash_funnel`**,
which has no such column.

**So before the next nightly run, please confirm the `dash_funnel` insert is name-based.** If it is
positional, tell Blake and I will drop the four columns immediately. Note the load path truncates
first via the `dash_truncate_all()` helper, so a failure means an empty table, not a stale one.

---

## Semantics the bot should know

1. **Never merge the two generations into one interview rung.** Compare
   `Interview (old funnel)` against the SUM of `Interview 1/2/3`, or scope with `on_new_pipeline`.
2. **There is zero crossover**, verified: every old-funnel interview row sits on an old-funnel role
   and every Interview 1/2/3 row sits on a new-funnel role.
3. **Interview conversion denominators must be scoped to `on_new_pipeline = true`.** Dividing by
   total ATS includes old-funnel roles that can never reach an interview column, which makes the
   rate look far worse than it is.
4. **A 0 in an interview column on an old-funnel role is not "no interviews happened".** The stage
   does not exist there. The dashboard renders a dash rather than a 0 for exactly this reason. If
   the bot phrases these numbers, it should say "not applicable" rather than "zero".
5. `dash_candidate_dq.stage` is **where the candidate stopped**, not how many reached it. The
   `dash_funnel` counts are cumulative. To reconcile, sum a rung and everything above it.

---

## How to verify after your change

```sql
-- dash_funnel: should return 500-ish flagged rows and interviews only on flagged rows
select count(*) filter (where on_new_pipeline) as new_pipeline_rows,
       sum(coalesce(int1,0)+coalesce(int2,0)+coalesce(int3,0)) as interviews,
       sum(coalesce(int1,0)+coalesce(int2,0)+coalesce(int3,0))
         filter (where on_new_pipeline is not true) as must_be_zero
from public.dash_funnel;

-- dash_candidate_dq: 92,397 rows, and the interview rungs split cleanly by generation
select stage,
       count(*) filter (where on_new_pipeline) as new_funnel,
       count(*) filter (where on_new_pipeline is not true) as old_funnel,
       count(*) as total
from public.dash_candidate_dq
group by 1 order by 4 desc;
```

`must_be_zero` must be 0. In the DQ query, `Interview (old funnel)` must have 0 in the new_funnel
column and `Interview 1/2/3` must have 0 in the old_funnel column.

---

## Separate issue, flagging not fixing

**RLS is disabled on 17 tables in this Supabase**, including `dash_candidates` (95k rows) and
`dash_candidate_dq` (92k). Anyone with the anon key can read or modify every row. Do not enable it
without writing policies first, since that would block the bot's own access. Raised with Blake, not
actioned.
