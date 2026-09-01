# SCOPE — Step 1: anchor ATS on the ATS date

2026-08-31. **Nothing has been changed. Read-only analysis.**
Every number verified against live prod today, with the phantom events still in the data.

---

## What it does, in one sentence

Stop deciding which week an ATS move belongs to by looking at the candidate's most recent interview,
and start using the date they actually moved to ATS.

## What changes technically

Add one column, `date_ats`, to `candidate_stage` in PROD V2. It stores the **first** `Moved to ATS`
event date, gated exactly like every other stage date (`stage_current_num >= 3`). Then three
transforms bucket ATS on `date_ats` instead of `date_interview`.

**The population does not change.** The same candidates are counted. Only the week they land in
changes. `date_interview` is untouched, so screens, interviews, offers and hires are unaffected.

---

## Evidence it is safe

### The three checks that matter most

| Check | Current | Proposed |
|---|---|---|
| **ORG total ATS 2026** | **7,404** | **7,404** |
| **Rodrigo's worst single week** | **32** | **32** |
| Sourcer-weeks that change at all | — | **36 of 1,193 (3.0%)** |

**Nothing is created or destroyed. Only re-dated.** And critically, **Rodrigo's worst week is
identical**, which means the 143 does not come back. That was the whole reason for the 26 Aug
rollback, and it is now directly disproven with the phantom events still present in the data.

### It does not depend on Mikhail's archive
I deliberately ran every test **before** the historical phantoms are archived. The gate excludes
burst candidates on its own. So this can ship now; the archive is independent cleanup.

### Four years of history barely moves
99.6% to 100% of candidates land in the identical ISO week, every year 2023-2026.
**Not one candidate changes year.** About 84 move week across four years.

### It fixes the two live complaints
| | Current | Proposed | Truth |
|---|---|---|---|
| Simon Siew, W36 | **13** | **0** | 0 (none moved to ATS this week) |
| No Isolation UK South, W34 | **8** | **6** (+2 in W33) | 6 and 2, per Bubble |

### The biggest movers, all of them
Simon W36 -13, Mateja W34 -7, Simon W30 +5, Chené W32 +4, Mateja W33 +4, Mateja W32 +4,
Chené W34 -3, Valeriia W36 -3, then everything else is ±1 or ±2. Most are the same person's credit
moving backwards to the week the ATS move really happened.

---

## Risks

1. **Individual weeks move, and some people look worse.** Simon loses 13 this week, Mateja loses 7 in
   W34. They did not do anything wrong; the old number was crediting them for interview activity.
   **Tell them before they see it.**
2. **Targets were set against the old behaviour.** WBR/MBR attainment shifts for the 36 affected
   sourcer-weeks.
3. **PROD V2 is the widest-reaching config in the project.** It feeds Andy's PBI pipeline too. The
   change is additive (one nullable column, one UPDATE) but the blast radius is real.
4. **Three transforms must move together** or the tables disagree, which is exactly the 143-vs-3
   split from 26 Aug.
5. **Not fixed by this, deliberately out of scope:** `ts_weekly` screens still ungated
   (Rodrigo 184 vs 7); positive response ungated everywhere; the backfill inventing screens
   (Jonaed 97 vs 18); your lost stage-TYPE edit.

---

## Sequence. Nothing runs without a yes to that specific step.

**1.** Add `date_ats` to PROD V2 and run it. **Change no report.** The column sits unused, every
dashboard number stays byte-identical. Verify `date_ats` against raw events for named candidates.

**2.** Switch weekly-funnel only. Run. It should now disagree with WBR and event-attr by exactly the
amounts in this document, week for week. If it disagrees by anything else, stop and roll back.

**3.** Switch event-attr and WBR. Run. All three agree again.

**4.** Re-verify Simon W36 = 0, No Isolation W34 = 6, Rodrigo max week = 32, org total = 7,404.

**5.** Tell the affected recruiters.

## Rollback — CORRECTED 2026-08-31 22:00 after Wave 2 shipped

**The versions in the first draft of this doc were already stale and rolling back to them would have
destroyed Wave 2 and the 27 Aug spend optimisation.** Same class of mistake as 26 Aug. Re-verified
live:

| Config | First draft said | **ACTUAL live now** | What changed |
|---|---|---|---|
| PROD V2 `375145203` | 240 | **241** | 27 Aug spend optimisation, cascade flattened |
| weekly-funnel `01kpqh9r…` | 19 | **19** ✓ | Wave 1, 16:12 today |
| event-attr `01ks4qf6…` | 8 | **8** ✓ | unchanged |
| WBR `01kpr0tr…` | 53 | **60** | **Wave 2, 21:08-21:09 today** |
| weekly-summary `01ksm8rz…` | not listed | **11** | **Wave 2, 21:25-21:26 today** |

**Re-verify every version immediately before acting. Do not trust a number written earlier in a
session.**

---

## WAVE 2 INTERACTION — read before approving

Wave 2 added INT1/2/3 columns to `wbr_weekly`, `ts_weekly`, `ts_summary_per_sourcer`,
`weekly_summary` and `weekly_summary_byjob`.

**Those interview counts are computed inside the `ats_` CTE and bucketed on the ATS week**, by
design, so that phantom bursts cannot inflate them (the Wave 1 gating rule).

**Consequence: they are coupled to the ATS date.** Changing what the ATS week means will move the
INT1/2/3 numbers with it. Simon's W36 goes 13 ATS to 0, and his W36 interview counts move too.

This is arguably correct behaviour, since the columns mean "of the candidates moved to ATS in week X,
how many reached Interview N". But it is a side effect the first draft did not account for, because
Wave 2 did not exist when it was written. **Wave 2's numbers will change.**

### Second change: PROD V2 no longer has six cascade UPDATEs
v241 collapsed the six sequential date-cascade UPDATEs into one, verified row-for-row on a parallel
copy. The plan to "add one UPDATE" must be re-fitted to that structure, and `date_ats` should sit
**outside** the coalesce cascade so it can never be backfilled from a later stage. Not doing so would
reproduce the Jonaed defect (dates invented with no event behind them). **The v241 SQL must be
re-read before writing anything.**

---

## EXACT IMPLEMENTATION, after reading the live v241 SQL

It is **four edit points, not one**. The first draft was wrong about this.

**1. Column placeholder** in `create or replace table "final_candidate_stage_bubble"` (~line 543):
add `NULL::DATE as "date_ats",` alongside the other date placeholders.

**2. One new UPDATE**, placed with the other per-stage updates, BEFORE the cascade:
```sql
update "final_candidate_stage_bubble" as c
set c."date_ats" = (select min(t."date_created")::DATE
                      from "final_event" as t
                     where 1=1
                      and c."candidate_id"=t."candidate_id"
                      and t."moved_to_stage"='Moved to ATS'
                      and c."stage_current_num">=3
                   );
```
`>=3` is correct: `Moved to ATS` is stage type `Offsite`, which the CASE maps to 3. Burst candidates
sit in Sequence (Contacted, =1) and are excluded, which is the entire defence.

**3. BOTH SIDES of the UNION** in `final_candidate_stage_tmp` (~line 928). This is the trap. That
UNION has an explicit column list on each side, so adding the column upstream alone **breaks the
transform**. Bubble side gets `, "date_ats"`; the Recruitee side gets `, NULL::DATE as "date_ats"`.

**4. Nothing else.** `final_candidate_stage_all` uses `cs.*` and the output mapping has no column
list, so it flows through to `out.c-reporting-v2.candidate_stage` automatically. Verified.

### DO NOT TOUCH the flattened cascade (~line 682)
```sql
c."date_screen" = coalesce(c."date_screen", c."date_screen_actual", c."date_interview", ...)
```
`date_ats` must stay OUT of this chain. This chain is exactly what invents Jonaed's 79 phantom
screens. `date_ats` stays NULL when there is no ATS event, which is correct and wanted.

### RESOLVED — the gate was verified BEFORE editing, not assumed

The earlier draft flagged that the simulation used `date_interview IS NOT NULL` as a proxy while the
real change gates on `stage_current_num >= 3`. Blake asked for that to be checked first. It was, by
recomputing the exact production CASE expression against live rows:

| | Candidates |
|---|---|
| Have a `Moved to ATS` event | 51,053 |
| Pass the **exact** gate (`stage_current_num >= 3`) | **49,239** |
| Pass the **proxy** gate (`date_interview` set) | **49,239** |
| In exact but not proxy | **0** |
| In proxy but not exact | **0** |

**The two populations are identical. Every number in this document is confirmed against the real
production logic.** Re-derived with the exact gate:

| Check | Current | Proposed |
|---|---|---|
| Org total ATS 2026 | 7,405 | **7,405** |
| Rodrigo worst single week | 32 | **32** |
| Simon Siew W36 | 13 | **0** |

No assumption is now carried into the edit.

---

## What I will not do
Touch `ts_weekly` screens, positive response, the stage-TYPE edit, or Wave 2 in this change.
One thing at a time.
