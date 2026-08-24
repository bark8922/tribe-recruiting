# ROLLBACK RECORD — Contacted week-anchor fix, 2026-08-24

**If anyone says the Contacted numbers are broken, this file is how you put them back.**

Owner: Blake. Prepared before the change was applied. Sibling to `ROLLBACK_ATS_FIX_2026-08-24.md`.

## STATUS: APPLIED AND VERIFIED, 2026-08-24 11:26 UTC

| Transformation | Config ID | Version | Restore to | Job |
|---|---|---|---|---|
| WBR/MBR weekly aggregations | `01kpr0tr0dt5ryf96a5zk85bx7` | 50 → **51** | **50** | `1014894145` (292s) |
| Project Dashboard - weekly funnel | `01kpqh9r7g2z66c8vvdr5d87xd` | 11 → **12** | **11** | `1014894180` (83s) |
| Project Dashboard - event-attr | `01ks4qf6zate4m7f0cxng2hnyy` | 2 → **3** | **2** | `1014894223` (65s) |

Every predicted figure hit exactly:

| Table | Year | Metric | Predicted | Actual |
|---|---|---|---|---|
| wbr_weekly | 2026 | CONTACTED | 75,473 | **75,473** |
| weekly funnel | 2025 | CONTACTED | 138,349 | **138,349** |
| weekly funnel | 2025 | REACTED | 53,603 | **53,603** |
| weekly funnel | 2026 | CONTACTED | 72,973 | **72,973** |
| weekly funnel | 2026 | REACTED | 27,943 | **27,943** |
| event-attr | 2025 | CONTACTED | 138,267 | **138,267** |
| event-attr | 2026 | CONTACTED | 72,944 | **72,944** |

Untouched as required: ATS 14,031 / 7,078, SCREENS 28,037 / 14,067, HIRED 2,646 / 1,319.

Jelena Lacmanovic in `wbr_weekly`, 2026: wk32 **185**, wk33 **250**, wk34 **260**, wk35 **0**,
total **2,136** unchanged. Exactly her pre-bulk-move readings.

---

## 1. One-line summary of what changed

Contacted used to take its week from `candidate_stage.date_contacted`, which we rebuild every run as
`max()` of the candidate's Contacted events. It now takes its week from the candidate's **first**
Contacted event. Candidates with **no** contact event at all (inbound applicants) keep the old
behaviour exactly.

---

## 2. How to roll back

### Option A — Keboola version restore (preferred)

| Transformation | Config ID | Restore to version |
|---|---|---|
| WBR/MBR weekly aggregations | `01kpr0tr0dt5ryf96a5zk85bx7` | *(recorded below after apply)* |
| Project Dashboard - weekly funnel | `01kpqh9r7g2z66c8vvdr5d87xd` | **11** |
| Project Dashboard - event-attr | `01ks4qf6zate4m7f0cxng2hnyy` | **2** |

Note: versions 11 and 2 are the post-ATS-fix versions from earlier today. Restoring to those undoes
the Contacted change but KEEPS the ATS fix, which is what you want. Do not go below 11 / 2 unless you
also intend to undo the ATS fix (see `ROLLBACK_ATS_FIX_2026-08-24.md`).

### Option B — paste the original SQL back

Section 4 holds the exact original SQL for every changed section.

---

## 3. The numbers BEFORE the change (captured 2026-08-24, after the ATS fix)

| Table | ISO_YEAR | CONTACTED | REACTED | Rows |
|---|---|---|---|---|
| `wbr_weekly` | 2026 | **75,533** | n/a | 1,257 |
| `project_dashboard` (weekly funnel) | 2025 | **138,283** | **53,604** | 19,466 |
| `project_dashboard` (weekly funnel) | 2026 | **73,033** | **27,939** | 9,420 |
| `project_dashboard_eventattr` | 2025 | **138,201** | n/a | 21,263 |
| `project_dashboard_eventattr` | 2026 | **73,004** | n/a | 10,118 |

### Expected numbers AFTER the change

| Table | ISO_YEAR | CONTACTED | Change | REACTED | Change |
|---|---|---|---|---|---|
| `wbr_weekly` | 2026 | **75,473** | -60 | n/a | |
| weekly funnel | 2025 | **138,349** | +66 | **53,603** | -1 |
| weekly funnel | 2026 | **72,973** | -60 | **27,943** | +4 |
| event-attr | 2025 | **138,267** | +66 | n/a | |
| event-attr | 2026 | **72,944** | -60 | n/a | |

ATS, SCREENS, ACTUAL_SCREENS, OFFERED, HIRED, VIEWED and POSITIVE_RESPONSE must be **unchanged**.
REACTED moves only in the weekly funnel, by 1 and 4, because it is deliberately bucketed on the same
date as Contacted. If anything else moves, roll back.

### Named spot-checks after the change (Jelena Lacmanovic, `wbr_weekly`)

| Week | Before | After | Note |
|---|---|---|---|
| 2026 wk32 | 121 | **185** | matches Blake's pre-bulk-move reading |
| 2026 wk33 | 164 | **250** | matches |
| 2026 wk34 | 181 | **260** | matches, this is the reported bug |
| 2026 wk35 | 229 | **0** | the phantom week clears |
| 2026 total | 2,136 | **2,136** | unchanged, as expected |

---

## 4. The ORIGINAL SQL, verbatim, as it ran before this change

### 4a. `WBR/MBR weekly aggregations` (`01kpr0tr0dt5ryf96a5zk85bx7`), block b0, code b0.c0

```sql
stage AS (
  SELECT
    cs."candidate_id",
    TRY_TO_DATE(cs."date_contacted")     AS dc,
```

### 4b. `Project Dashboard - weekly funnel` (`01kpqh9r7g2z66c8vvdr5d87xd`)

```sql
stage AS (
  SELECT
    cs."candidate_id",
    TRY_TO_DATE(cs."date_contacted") AS dc,
```

### 4c. `Project Dashboard - event-attr` (`01ks4qf6zate4m7f0cxng2hnyy`)

```sql
    MAX(CASE WHEN e."moved_to_stageType"='Contacted' AND e."moved_to_stage" <> 'Responded' THEN 1 ELSE 0 END) AS did_contacted,
```

```sql
contacted AS (SELECT client, job_id, job_title, job_category, ta, ts, candidate_source, is_external_recruiter, YEAROFWEEKISO(dc) AS iso_year, WEEKISO(dc) AS iso_week, COUNT(DISTINCT "candidate_id") AS contacted FROM base WHERE dc IS NOT NULL AND did_contacted = 1 AND YEAROFWEEKISO(dc) >= 2025 GROUP BY 1,2,3,4,5,6,7,8,9,10),
```

Full original scripts are preserved in Keboola version history.

---

## 5. Why the change was made

`candidate_stage.date_contacted` is not a Bubble field. We rebuild it every run in config
`375145203 [PROD] Data preparation V2` as `max(date_created)` of the candidate's Contacted-type
events. Because it is a `max()`, any **new** Contacted event moves the candidate out of the week they
were already counted in.

Trigger, 2026-08-24: Mikhail Kuzmin bulk-moved 229 of Jelena Lacmanovic's candidates into Contacted
on two No Isolation roles at 07:39 and 07:42 UTC. All 229 re-dated to 24 Aug, draining weeks 32, 33
and 34 by 64 / 86 / 79 and creating a phantom 229 in week 35. Her total never changed, which is why
it looked like a refresh had "lost" numbers.

Same root cause as the ATS fix applied earlier today, one statement up in the same script.

**The applicant fallback is essential.** Inbound applicants have no Contacted event by definition
(nobody contacted them, they applied). Their `date_contacted` is backfilled by a cascade in the same
script. Without `COALESCE(first_contact_event, date_contacted)` they would vanish: modelling showed
Simon Siew losing 52 DualEntry applicants, plus Dolores Palotas and Tinatini Karaulashvili.

**Only one person's yearly total moves materially:** Elena Petrovska, -63. Those 63 are on one job
(Wolt Payroll Specialist), first contacted early December 2025, then all re-moved on a single day,
13 January 2026. They are the same bug, and they belong in December 2025.

---

## 6. What was NOT changed

- `candidate_stage.date_contacted` itself. Config `375145203` was **not** modified.
- The other 8 transformations that read `date_contacted` (sourcing dashboards, TTH, weekly summary,
  hires drill-down, Candidate DQ by Stage, the Supabase/Tribe-Bot push). Those still use the old
  field. If Contacted numbers need to reconcile across those too, that is a separate follow-up.
- ATS, screens, offers, hires, and every other metric.
