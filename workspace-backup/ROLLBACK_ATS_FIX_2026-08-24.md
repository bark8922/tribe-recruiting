# ROLLBACK RECORD — ATS week-anchor fix, 2026-08-24

**If anyone says the ATS numbers are broken, this file is how you put them back.**

Owner: Blake. Prepared before the change was applied.

## STATUS: APPLIED AND VERIFIED, 2026-08-24 10:45 UTC

| | |
|---|---|
| weekly funnel `01kpqh9r7g2z66c8vvdr5d87xd` | version 10 → **11**, job `1014888499`, success, 70s, 28,887 rows |
| event-attr `01ks4qf6zate4m7f0cxng2hnyy` | version 1 → **2**, job `1014888556`, success, 46s, 31,382 rows |

Post-change verification, both tables, every figure matching the prediction exactly:

| Check | Predicted | Actual |
|---|---|---|
| ATS 2025 (both tables) | 14,031 | **14,031** |
| ATS 2026 (both tables) | 7,078 | **7,078** |
| No Isolation UK (South) 2026 wk33 | 2 | **2** |
| No Isolation UK (South) 2026 wk34 | 6 | **6** |
| No Isolation all roles 2026 wk34 | 8 | **8** |
| No Isolation all roles 2026 wk33 | 6 | **6** |
| Circula 2025 full year | 698 | **698** |

Mikhail's reported case is resolved: UK (South) week 34 now reads 6, not 8, and the 2 that were
wrongly pulled forward are back in week 33.

---

## 1. One-line summary of what changed

The ATS column used to take its week from `candidate_stage.date_interview`. It now takes its week
from the date of the candidate's first `Moved to ATS` event. Nothing else was touched.

---

## 2. How to roll back (fastest route first)

### Option A — Keboola version restore (preferred, 2 minutes)

Both transformations are versioned in Keboola. Restore the version listed below and re-run the Flow.

| Transformation | Config ID | Version BEFORE the change | Restore to |
|---|---|---|---|
| Project Dashboard - weekly funnel | `01kpqh9r7g2z66c8vvdr5d87xd` | **10** | version 10 |
| Project Dashboard - event-attr | `01ks4qf6zate4m7f0cxng2hnyy` | **1** | version 1 |

In Keboola: open the transformation → Versions tab → find the version number above → Restore.

Direct links:
- https://connection.eu-central-1.keboola.com/admin/projects/855/transformations-v2/keboola.snowflake-transformation/01kpqh9r7g2z66c8vvdr5d87xd
- https://connection.eu-central-1.keboola.com/admin/projects/855/transformations-v2/keboola.snowflake-transformation/01ks4qf6zate4m7f0cxng2hnyy

Then re-run the render/push so the dashboard picks it up. See the WBR force-refresh runbook.

### Option B — paste the original SQL back

The exact original SQL for the changed sections is in section 4 below. Paste it over the new
version and re-run. This produces byte-identical output to Option A.

---

## 3. The numbers BEFORE the change (captured 2026-08-24)

Live table totals at the moment of the change. If a rollback is done correctly, these come back exactly.

| Table | ISO_YEAR | ATS total | Rows | Clients |
|---|---|---|---|---|
| `project_dashboard` (weekly funnel) | 2025 | **13,407** | 19,468 | 33 |
| `project_dashboard` (weekly funnel) | 2026 | **7,069** | 9,422 | 31 |
| `project_dashboard_eventattr` | 2025 | **13,407** | 21,264 | 33 |
| `project_dashboard_eventattr` | 2026 | **7,069** | 10,120 | 31 |

(There is also one junk row with `ISO_YEAR = 2202`, Wolt "Product Lead, Merchant", all metrics zero.
Pre-existing, unrelated, ignore it.)

### Expected numbers AFTER the change

| Table | ISO_YEAR | Expected ATS total | Change |
|---|---|---|---|
| both | 2025 | **14,031** | +624 |
| both | 2026 | **7,078** | +9 |

Every other column (VIEWED, CONTACTED, REACTED, POSITIVE_RESPONSE, SCREENS, ACTUAL_SCREENS,
OFFERED, HIRED) must show **exactly zero change**. This was verified by running the patched script
end to end before applying. If any other column moves, something is wrong — roll back.

### Named cases to spot-check after the change

| Client | Period | Before | After |
|---|---|---|---|
| No Isolation, Account Manager UK (South) | 2026 wk 33 | 0 | 2 |
| No Isolation, Account Manager UK (South) | 2026 wk 34 | 8 | 6 |
| No Isolation (all roles) | 2026 wk 34 | 11 | 8 |
| Circula | 2025 full year | 85 | 698 |

---

## 4. The ORIGINAL SQL, verbatim, as it ran before 2026-08-24

### 4a. `Project Dashboard - weekly funnel` (`01kpqh9r7g2z66c8vvdr5d87xd`) — two CTEs

Original `ats_ev`:

```sql
ats_ev AS (
  SELECT DISTINCT "candidate_id" FROM "KEBOOLA_855"."out.c-reporting-v2"."event"
  WHERE "moved_to_stage" = 'Moved to ATS'
),
```

Original `ats_`:

```sql
ats_ AS (
  SELECT client, job_id, job_title, job_category, ta, ts, candidate_source, is_external_recruiter,
         YEAROFWEEKISO(di) AS iso_year, WEEKISO(di) AS iso_week,
         COUNT(DISTINCT "candidate_id") AS ats
  FROM joined WHERE di IS NOT NULL AND YEAROFWEEKISO(di) >= 2025
    AND "candidate_id" IN (SELECT "candidate_id" FROM ats_ev)
  GROUP BY 1,2,3,4,5,6,7,8,9,10
),
```

### 4b. `Project Dashboard - event-attr` (`01ks4qf6zate4m7f0cxng2hnyy`) — three edits

Original line inside `ev_attr`:

```sql
    MAX(CASE WHEN e."moved_to_stage"='Moved to ATS' THEN 1 ELSE 0 END) AS did_ats,
```

Original final line of the `base` SELECT list:

```sql
    ea.did_contacted, ea.did_screen, ea.did_eval, ea.did_ats, ea.did_offer, ea.did_hired
```

Original `ats_`:

```sql
ats_ AS (SELECT client, job_id, job_title, job_category, ta, ts, candidate_source, is_external_recruiter, YEAROFWEEKISO(di) AS iso_year, WEEKISO(di) AS iso_week, COUNT(DISTINCT "candidate_id") AS ats FROM base WHERE di IS NOT NULL AND did_ats = 1 AND YEAROFWEEKISO(di) >= 2025 GROUP BY 1,2,3,4,5,6,7,8,9,10),
```

Full original scripts are also preserved in the Keboola version history (section 2, Option A).

---

## 5. Why the change was made (the one-paragraph defence)

`date_interview` is not a Bubble field. It is computed by us in Keboola config `375145203
[PROD] Data preparation V2` as `max(date_created)` of any event whose stage type is
`Offsite` or `Interview`, gated on the candidate's *current* stage. `Moved to ATS` is type
`Offsite`, so the ATS move legitimately sets it — but a later `Interview 1` move overwrites it,
and moving a candidate backwards afterwards erases it entirely. Result: ATS counts landed in the
wrong week, and candidates who never reached an interview vanished from the column.

Reported by Mikhail Kuzmin 2026-08-21: No Isolation UK (South) showed 8 ATS in week 34 when Bubble
had 6 that week and 2 the week before. Confirmed exactly.

The +624 on 2025 is almost entirely Circula (85 → 698): 592 candidates moved to ATS by Rodrigo
Gomes between 27 Jan and 20 May 2025 who currently sit in a stage called "Sequence", which is below
the gate, so their ATS was suppressed. Those are real ATS moves that the old logic could not see.

---

## 6. What was NOT changed

- `candidate_stage.date_interview` itself is untouched. Config `375145203` was not modified.
- No other metric, table, or dashboard tab.
- The Contacted / WBR fix is a separate, later piece of work and was not applied here.
