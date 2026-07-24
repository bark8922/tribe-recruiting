/* =====================================================================
   RECRUITING FUNNEL - TRANSFORM CHANGES (DRAFT, DO NOT APPLY TO PROD)
   =====================================================================
   Target: Keboola transformation "[PROD] Data preparation V2"
           component keboola.snowflake-transformation, config 375145203
           Block "code" > codes "part 1 - bubble data" and "part 3 - final tables"
   Purpose: surface the new Interview 1/2/3 stage rungs between Move to ATS
            and Offer, without disturbing any existing metric.
   Author: Blake (drafted 2026-07-24). Build on a Keboola DEV BRANCH,
           run, validate against the known candidate (Section E), then merge.

   ---------------------------------------------------------------------
   PROOF RESULTS (read-only validation on live data, 2026-07-24)
   ---------------------------------------------------------------------
   - Jobs."stages" explode + rung flags: WORKS. 24 jobs currently define the
     new rungs; all 24 have all three (Interview 1/2/3) because that is
     Mikhail's default template. No job has trimmed to 1-2 rungs yet, so the
     pipeline-aware (has_int1/2/3) logic is correct but currently a no-op.
     It stays in for when recruiters start deleting rungs they don't use.
   - Snowflake gotcha found + fixed: a LATERAL FLATTEN cannot sit on the left
     of a JOIN. The flatten must live in its own CTE (see B1 below).
   - Per-rung date population from real events: WORKS. The one real test
     candidate lights up Interview 1/2/3 = 2026-07-22 correctly.
   - BACKFILL RULE (refined): an UNguarded backfill is wrong - across all
     rung-configured jobs, 564 candidates reached offer/hire but only 1 has a
     real rung event, and the other 563 were hired BEFORE the rungs existed.
     Backfilling them would fabricate history. BUT going forward a hire that
     skipped the rung clicks SHOULD be backfilled (a hire genuinely interviewed),
     same as every other stage today. The fix is a GUARDED backfill (Section B3):
     only for candidates who reached offer/hire, only when that offer/hire is
     on/after the 2026-07-14 go-live, and only for rungs the job has. This keeps
     consistency with the legacy cascade, excludes the 563, and does NOT hide the
     drop-off - mid-funnel drop-offs have no offer/hire date, so nothing backfills
     for them and they stay visible where they really stalled.
   ---------------------------------------------------------------------

   DESIGN PRINCIPLES (from STAGE_FUNNEL_BUILD_PLAN.md)
   - Continuity: the existing date_interview logic is left UNTOUCHED. It stays
     the "reached the ATS / interview band" anchor, so every current WBR/MBR/
     TTH number is byte-identical after this change. We only ADD signal.
   - Move to ATS is defined by the backfill rule (reached if any later stage
     reached). date_interview already behaves this way, so it remains the ATS
     anchor. We are NOT renaming or repointing it.
   - Positional rungs: date_interview_1/2/3 come straight from the stage TYPE
     ('Interview 1/2/3'), never the free-text stage name.
   - Pipeline-aware: a rung a job does not define must never be treated as a
     drop-off. Which rungs a job has comes from Jobs."stages" (see Section B1).
   ===================================================================== */


/* =====================================================================
   SECTION A  -  part 1 - bubble data
   ===================================================================== */

/* ---------------------------------------------------------------------
   A1. Declare the three new date columns.
   Add these three lines to the SELECT list of
   `create or replace table "final_candidate_stage_bubble" as select ...`,
   right after the existing `NULL::DATE as "date_interview",` line:
   --------------------------------------------------------------------- */
   -- NULL::DATE as "date_interview",          <-- existing line, keep as is
      NULL::DATE as "date_interview_1",      -- NEW
      NULL::DATE as "date_interview_2",      -- NEW
      NULL::DATE as "date_interview_3",      -- NEW
   -- NULL::DATE as "date_offer",              <-- existing line continues


/* ---------------------------------------------------------------------
   A2. Raw per-rung population.
   Add these three UPDATEs immediately AFTER the existing
   `set c."date_interview" = (... Offsite/Interview ...)` update and
   BEFORE the `set c."date_offer"` update. Candidate-level only, no job
   join needed here - a candidate either has an event of that type or not.
   The num>=3 gate mirrors the existing date_interview gate.
   --------------------------------------------------------------------- */
update "final_candidate_stage_bubble" as c
set c."date_interview_1" = (select max(t."date_created")
                            from "final_event" as t
                           where 1=1
                             and c."candidate_id"=t."candidate_id"
                             and t."moved_to_stageType"='Interview 1'
                             and c."stage_current_num">=3
                           );

update "final_candidate_stage_bubble" as c
set c."date_interview_2" = (select max(t."date_created")
                            from "final_event" as t
                           where 1=1
                             and c."candidate_id"=t."candidate_id"
                             and t."moved_to_stageType"='Interview 2'
                             and c."stage_current_num">=3
                           );

update "final_candidate_stage_bubble" as c
set c."date_interview_3" = (select max(t."date_created")
                            from "final_event" as t
                           where 1=1
                             and c."candidate_id"=t."candidate_id"
                             and t."moved_to_stageType"='Interview 3'
                             and c."stage_current_num">=3
                           );

/* NOTE on stage_current_num: the existing CASE already maps 'Interview 1/2/3'
   to num=3 via `lower("stage_current_type") like '%interview%'`, so no change
   is required there. The new rungs sit inside the existing num=3 band, which
   is what we want (they are all "in interviews"). Leave the CASE as is. */


/* =====================================================================
   SECTION B  -  part 3 - final tables
   ===================================================================== */

/* ---------------------------------------------------------------------
   B1. Job -> rung lookup (which interview rungs a job actually defines).
   Source of truth is Jobs."stages", a stringified list of stage ids, e.g.
     ['1784...x854...', '1784...x363...', ...]
   Explode it, join to the stage + stage type, and flag the rungs present.
   Add this near the top of part 3, before final_candidate_stage_tmp.
   --------------------------------------------------------------------- */
/* NOTE: LATERAL FLATTEN cannot be on the left side of a JOIN in Snowflake,
   so the flatten lives in its own CTE, then we join. This exact form was
   validated read-only on live data. */
create or replace table "job_rungs" as
with "exploded" as (
    select
        j."bubbleinternal_id" as "job_id",
        replace(replace(trim(f.value::string), '''', ''), ' ', '') as "stage_id"
    from "bubble_Jobs" as j,
         lateral flatten(input => split(trim(j."stages", '[]'), ',')) f
)
select
    e."job_id",
    max(iff(st."stage_type_name"='Interview 1', 1, 0)) as "has_int1",
    max(iff(st."stage_type_name"='Interview 2', 1, 0)) as "has_int2",
    max(iff(st."stage_type_name"='Interview 3', 1, 0)) as "has_int3"
from "exploded" as e
left join "bubble_Stages"     as s  on e."stage_id" = s."bubbleinternal_id"
left join "bubble_stagesType" as st on s."stagesType" = st."bubbleinternal_id"
group by 1;
/* CONFIRM: exact source table names for Jobs / Stages inside this transform
   (the part-1 code aliases them as "bubble_Jobs", "bubble_Stages",
   "bubble_stagesType"). Reuse whatever part 1 already references.
   job_rungs is consumed by the DASHBOARD/AGGREGATION layer for denominators
   (only count a rung across jobs that define it). It is NOT used to backfill
   dates - see B3. */


/* ---------------------------------------------------------------------
   B2. Carry the new columns through final_candidate_stage_tmp.
   Add the three columns to BOTH halves of the UNION. Bubble half selects
   them straight; recruitee half has no rungs, so NULL::DATE.
   --------------------------------------------------------------------- */
-- Bubble half: add after `, "date_interview"`
   , "date_interview_1"
   , "date_interview_2"
   , "date_interview_3"
-- Recruitee half: add after `, TRY_TO_DATE("date_interview") as "date_interview"`
   , NULL::DATE as "date_interview_1"
   , NULL::DATE as "date_interview_2"
   , NULL::DATE as "date_interview_3"

/* final_candidate_stage_all already does `cs.*`, so the new columns flow
   through automatically. It also still has "job_id" at this point (it is
   only dropped by the final `alter table ... drop "job_id"`), which B4 uses. */


/* ---------------------------------------------------------------------
   B3. GUARDED backfill for the rung columns.
   Insert in part 3 AFTER final_candidate_stage_all is built and BEFORE
   `alter table "final_candidate_stage_all" drop "job_id";`.

   Three guards (all required):
     (1) reached offer or hire  -> only successes get the middle filled in;
         mid-funnel drop-offs have no offer/hire date so they are untouched
         and stay visible where they really stalled.
     (2) that offer/hire is on/after the rung go-live (2026-07-14) -> excludes
         the 563 candidates hired before the rungs existed (no fabricated history).
     (3) has_intN = 1 -> never invent a rung a job does not define.

   Run int3, then int2, then int1 so earlier rungs can cascade from later ones.
   The go-live constant should match the actual stagesType creation date; make
   it a variable if you prefer.
   --------------------------------------------------------------------- */
update "final_candidate_stage_all" as cs
set cs."date_interview_3" = coalesce(cs."date_hired", cs."date_offer")
from "job_rungs" as r
where r."job_id" = cs."job_id"
  and r."has_int3" = 1
  and cs."date_interview_3" is NULL
  and coalesce(cs."date_hired", cs."date_offer") is not NULL
  and coalesce(cs."date_hired", cs."date_offer") >= '2026-07-14';

update "final_candidate_stage_all" as cs
set cs."date_interview_2" = coalesce(cs."date_interview_3", cs."date_hired", cs."date_offer")
from "job_rungs" as r
where r."job_id" = cs."job_id"
  and r."has_int2" = 1
  and cs."date_interview_2" is NULL
  and coalesce(cs."date_interview_3", cs."date_hired", cs."date_offer") is not NULL
  and coalesce(cs."date_interview_3", cs."date_hired", cs."date_offer") >= '2026-07-14';

update "final_candidate_stage_all" as cs
set cs."date_interview_1" = coalesce(cs."date_interview_2", cs."date_interview_3", cs."date_hired", cs."date_offer")
from "job_rungs" as r
where r."job_id" = cs."job_id"
  and r."has_int1" = 1
  and cs."date_interview_1" is NULL
  and coalesce(cs."date_interview_2", cs."date_interview_3", cs."date_hired", cs."date_offer") is not NULL
  and coalesce(cs."date_interview_2", cs."date_interview_3", cs."date_hired", cs."date_offer") >= '2026-07-14';

/* Rungs a job HAS but the candidate never reached (dropped mid-funnel, no
   offer/hire) correctly stay NULL. The "don't penalize a 2-round job" fairness
   rule is handled at the denominator level using job_rungs (Section C). */

/* Optional: carry has_int1/2/3 onto the output row so the dashboard layer can
   build correct denominators (Section C) without re-exploding Jobs."stages".
   If you want that, add three columns to final_candidate_stage_all and set
   them from job_rungs here, before the job_id drop. */


/* =====================================================================
   SECTION C  -  aggregation / dashboard layer  (NEXT PHASE, not in this file)
   ===================================================================== */
/* The rung DATE columns above answer "did this candidate reach rung N and when".
   The "don't penalize a 2-round job for missing rung 3" rule is a DENOMINATOR
   rule and lives in the WBR/MBR aggregations + React funnels, not here:
     - Interview N reached count  = candidates with date_interview_N not null
     - Interview N denominator    = candidates on jobs where has_intN = 1
   So conversion into/out of rung N is computed only across jobs that define
   rung N. Build this once real volume exists and we can validate the rates. */


/* =====================================================================
   SECTION D  -  OPEN CONFIRMATIONS before merge
   ===================================================================== */
/* 1. Source table names inside the transform for Jobs / Stages (B1) - reuse
      exactly what part 1 references (bubble_Jobs / bubble_Stages / bubble_stagesType).
   2. Jobs."stages" format: confirmed as a stringified list ['id', 'id', ...].
      The split/flatten/trim in B1 handles that; sanity-check one job after first run.
   3. Do we also want an explicit "date_ats" column split cleanly out of
      date_interview? Not needed for continuity (date_interview already serves
      as the ATS anchor). Add later only if the dashboard wants ATS and
      "reached interview band" as visibly separate fields.
   4. Recruitee side has no per-round rungs; left as NULL. Confirm that is fine
      (recruitee volume is small and legacy). */


/* =====================================================================
   SECTION E  -  VALIDATION (run on the dev branch after the transform runs)
   ===================================================================== */
/* Known case: candidate 1784707784121x651390804868988900 on job
   1784706592955x508945920112984060 (Sales Executive Brands) was moved through
   Interview 1, 2, 3 on 2026-07-22. After the change, that candidate's row in
   out.c-reporting-v2.candidate_stage should show date_interview_1/2/3 populated. */
select "candidate_id", "date_screen", "date_interview",
       "date_interview_1", "date_interview_2", "date_interview_3",
       "date_offer", "date_hired"
from "final_candidate_stage_all"
where "candidate_id" = '1784707784121x651390804868988900';

/* Also confirm job_rungs looks right for that job (expect has_int1/2/3 = 1): */
select * from "job_rungs" where "job_id" = '1784706592955x508945920112984060';
