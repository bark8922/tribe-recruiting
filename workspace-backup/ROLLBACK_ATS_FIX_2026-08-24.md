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

## PART 2 IS COMPLETE AND VERIFIED — 2026-08-25 12:39 UTC

All three transformations now anchor ATS on the ATS event date AND match `moved_to_stageType = 'Offsite'`
(catching Aiven's renamed "Move to ATS stage" and Tribe.xyz IR's "Language Check").

| Transformation | Config ID | Version | Rollback to |
|---|---|---|---|
| WBR/MBR weekly agg | `01kpr0tr0dt5ryf96a5zk85bx7` | **52** | 51 |
| PD - weekly funnel | `01kpqh9r7g2z66c8vvdr5d87xd` | **13** | 12 |
| PD - event-attr | `01ks4qf6zate4m7f0cxng2hnyy` | **4** | 3 |

Final state, all three tables in agreement:

| Table | 2025 ATS | 2026 ATS |
|---|---|---|
| weekly funnel | 14,031 | **7,127** |
| event-attr | 14,031 | **7,127** |
| wbr_weekly | n/a (2026-only) | **7,127** |

Predicted 7,123 at 09:15; actual 7,127 at 12:39. The +4 is ordinary new recruiting activity over
those hours, not a discrepancy. 2025 is exactly as predicted and unchanged.

Spot checks, both dashboards agreeing on the same candidates:

| Check | Value |
|---|---|
| WBR Jelena wk33 / wk34 ATS | **6 / 8** |
| PD No Isolation wk33 / wk34 ATS | **6 / 8** |
| WBR Jelena CONTACTED wk32/33/34 | **185 / 250 / 260** |

Mikhail's original report (8 in wk34, should be 6) and Blake's Contacted report (260 dropping to 181)
are both resolved, and WBR and the Project Dashboard no longer disagree.

Note: the two PD edits were applied by Blake in the Keboola UI on 2026-08-25 because the original
Keboola MCP connector was returning 401 on all write endpoints. A second connector
(token `11797764` "API Claude -token", non-expiring) was later added and works for both reads and
writes; that is what ran the final jobs.

---

## Superseded: PART 2 was incomplete at 09:25 UTC. Kept for history.

State as of 2026-08-25 09:25 UTC, when the Keboola connector dropped mid-change.

| Transformation | Config ID | Version | ATS anchor | Stage match | Done? |
|---|---|---|---|---|---|
| WBR/MBR weekly agg | `01kpr0tr0dt5ryf96a5zk85bx7` | **52** | event date | stageType `Offsite` | **DONE AND VERIFIED** — 2026 ATS = **7,123** as predicted, Jelena wk33 **6**, wk34 **8**, CONTACTED unaffected |
| PD - weekly funnel | `01kpqh9r7g2z66c8vvdr5d87xd` | 12 | event date | literal `'Moved to ATS'` | **NOT changed** |
| PD - event-attr | `01ks4qf6zate4m7f0cxng2hnyy` | 3 | event date | literal `'Moved to ATS'` | **NOT changed** |

**Consequence while incomplete:** WBR and the Project Dashboard will differ by ~11 candidates in
2026 (Aiven's renamed stage plus one Tribe.xyz IR). WBR is the *more* correct of the two. This is an
incomplete improvement, not a regression.

### TO FINISH (once the Keboola connector is reconnected)

1. Verify job `1015144312` succeeded, and that `wbr_weekly` 2026 reads ATS **7,123**,
   Jelena wk33 **6**, wk34 **8**, and CONTACTED still **75,473**.
2. Apply to `01kpqh9r7g2z66c8vvdr5d87xd`, block b0, code b0.c0, one replacement:
   `WHERE "moved_to_stage" = 'Moved to ATS'` → `WHERE "moved_to_stageType" = 'Offsite'`
3. Apply to `01ks4qf6zate4m7f0cxng2hnyy`, block b0, code b0.c0, one replacement:
   `MIN(CASE WHEN e."moved_to_stage"='Moved to ATS' THEN TRY_TO_DATE(e."date_created") END) AS ats_date,`
   → `MIN(CASE WHEN e."moved_to_stageType"='Offsite' THEN TRY_TO_DATE(e."date_created") END) AS ats_date,`
4. Run both, then confirm all three tables read 2026 ATS **7,123** and 2025 ATS **14,031**.

---

## PART 2, 2026-08-25 — WBR's own ATS column, and the stage-name-vs-type rule

Two gaps found the next day. Part 1 (below) only covered the two Project Dashboard transforms.

**Gap A: `wbr_weekly` has its OWN ATS calculation** which Part 1 did not touch, so WBR still bucketed
on `date_interview` and disagreed with the Project Dashboard. Jelena Lacmanovic 2026: WBR read wk33=2
/ wk34=11, Project Dashboard read wk33=6 / wk34=8. Verified she has only No Isolation roles in those
weeks, so the scopes really are comparable and the two genuinely disagreed.

**Gap B: matching the stage NAME missed a client's renamed stage.** Part 1 tested
`moved_to_stage = 'Moved to ATS'`. Three stage names share stageType `Offsite` and all mean the same
thing:

| Stage name | Type | Candidates 2026 | Clients |
|---|---|---|---|
| Moved to ATS | Offsite | 7,528 | 25 |
| Move to ATS stage | Offsite | 20 | Aiven only |
| Language Check | Offsite | 1 | Tribe.xyz (IR) |

So the fix now tests `moved_to_stageType = 'Offsite'`. WBR's looser rule was accidentally catching
these; it was not a bug on WBR's part, which is why the two disagreed by more than the date issue.

### Versions BEFORE Part 2

| Transformation | Config ID | Restore to |
|---|---|---|
| WBR/MBR weekly aggregations | `01kpr0tr0dt5ryf96a5zk85bx7` | **51** |
| Project Dashboard - weekly funnel | `01kpqh9r7g2z66c8vvdr5d87xd` | **12** |
| Project Dashboard - event-attr | `01ks4qf6zate4m7f0cxng2hnyy` | **3** |

### Numbers BEFORE Part 2 (2026-08-25)

| Table | ISO_YEAR | ATS now | Predicted after |
|---|---|---|---|
| `project_dashboard` | 2025 | 14,031 | **14,031** (no change) |
| `project_dashboard` | 2026 | 7,112 | **7,123** (+11) |
| `project_dashboard_eventattr` | 2026 | 7,112 | **7,123** (+11) |
| `wbr_weekly` | 2026 | **7,093** | **7,123** (+30) |

After Part 2 all three tables should read **7,123** for 2026 and agree with each other.

Spot-check, Jelena Lacmanovic 2026 in `wbr_weekly`: wk33 2 → **6**, wk34 11 → **8**, matching the
Project Dashboard.

### Original SQL before Part 2

`wbr_weekly` (`01kpr0tr0dt5ryf96a5zk85bx7`):
```sql
ats_ AS (
  SELECT client, ta, YEAROFWEEKISO(di) AS iso_year, WEEKISO(di) AS iso_week,
         COUNT(DISTINCT "candidate_id") AS ats
  FROM joined WHERE di IS NOT NULL AND YEAROFWEEKISO(di) = 2026
  GROUP BY 1,2,3,4
),
```

`weekly funnel` (`01kpqh9r7g2z66c8vvdr5d87xd`), inside `ats_ev`:
```sql
  WHERE "moved_to_stage" = 'Moved to ATS'
    AND TRY_TO_DATE("date_created") IS NOT NULL
```

`event-attr` (`01ks4qf6zate4m7f0cxng2hnyy`), inside `ev_attr`:
```sql
    MIN(CASE WHEN e."moved_to_stage"='Moved to ATS' THEN TRY_TO_DATE(e."date_created") END) AS ats_date,
```

> Note on the 24 Aug figures below: they were captured before the scheduled Flow ran again. ATS 2026
> read 7,078 at 11:26 on the 24th and 7,112 on the 25th. That drift is ordinary new recruiting
> activity, not the fix. The Flow ran successfully four times in between.

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
