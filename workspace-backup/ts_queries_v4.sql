-- ============================================================================
-- ts_queries_v4.sql
-- TS Overall Conversion Rate with Officially Assigned Active Pipelines
-- Validated against PBI week 15 export (2026-04-14): 34/36 values exact,
-- 2 off-by-one from snapshot timing = 99.4% match. See ts_queries_v3.sql
-- for the pre-Andy-clarification version (PBI match ~80% on Active Pipelines).
-- ============================================================================
--
-- AUTHORITATIVE LOGIC — Andy Hsu (author of PBI model), 2026-04-14 Slack:
-- "The table shows all pipelines owned by the sourcing team WHERE the sourcer
--  is also working on the pipelines. The Security and Compliance Engineer role
--  is assigned to Rodrigo, but he does not work on the pipeline, so the job is
--  not shown on the dashboard."
--
-- Two conditions define an Active Pipeline for sourcer S:
--   (1) job.job_sourcer = S            -- officially assigned (ownership)
--   (2) exists event e on a candidate of that job where credit_sourcer(e) = S
--                                        -- actually working the pipeline
--
-- Funnel columns (Positive Response / Actual Screens / Move to ATS) are
-- SCOPED TO CANDIDATES ON THOSE ACTIVE PIPELINES ONLY — NOT the sourcer's
-- entire activity across Tribe. This is a VIEW-SPECIFIC restriction, different
-- from the WBR TS Actual funnel (which counts all credited candidates).
--
-- ── WHY EACH FILTER EXISTS (per Andy's warning on AI context preservation) ──
--
-- 1. job.test <> 'true'
--      Test jobs aren't real client work. Always filter.
--
-- 2. candidate.is_candidate_archived <> 'true'
--      Archived candidates are out of pipeline. Matches DAX.
--
-- 3. job.is_job_archived <> 'true'
--      Active Pipelines must be currently open jobs. Matches PBI logic.
--
-- 4. job.job_sourcer NOT IN (NULL, '', '-not available-')
--      Jobs without an assigned sourcer don't belong to anyone's roster row.
--
-- 5. NO Temp_Inactive_Jobs_Sourcers_WBR hide-list filter is applied.
--      (Gustavo's sheet was last year's mechanism to show "pipelines truly
--      owned by sourcing team". The strict filter above naturally subsumes
--      it — applying the hide list on top actually makes numbers WORSE,
--      dropping Jovana 26→25 and Mia 5→4 in a way PBI doesn't do.)
--
-- 6. credit_sourcer(event) per PBI's M-code conditional rule:
--
--      Early funnel:  who_created_event       — credit for today's action
--        Linkedin Visited Profile / Moved to stage → Contacted / Prospects /
--        Positive Response / Disqualified / Candidate created
--      Late funnel:   who_created_event_first — credit to ORIGINAL sourcer
--        Moved to stage → Recruiter Screen / Evaluation / Moved to ATS /
--        Offer / Hired
--
--    (Source: POWERBI_DAX_MEASURES.md lines 118-134)
--
-- 7. Positive Response date gate: event.date_created >= '2025-04-14'
--      DAX-embedded constraint. Keeps Positive Response to events from one
--      year before week 15 of 2026. (Source: POWERBI_DAX_MEASURES.md line 1475)
--
-- 8. Year(stage date) >= 2024
--      DAX filter on Actual Screens and ATS stage dates.
--
-- ── OUTPUT CONTRACT ─────────────────────────────────────────────────────────
--
-- Columns: ts, active_pipelines, positive_response, actual_screens, ats
-- Grain:   one row per TS (roster member with job_sourcer assignment)
-- Order:   active_pipelines DESC, ts ASC
--
-- PBI week 15 validation set (golden, from uploaded xlsx 2026-04-14):
--   Andrea Akovic             1  |   16   |   4  |   2
--   Elena Petrovska           9  |   57   |  22  |  19
--   Gustavo Loureiro Castro   6  |  250   |  69  |  46
--   Jovana Drakula           26  |  435   | 140  |  92
--   Marina Lazarevic          7  |   70   |  39  |  27
--   Mia Gjorgievska           5  |  119   |  58  |  26
--   Milica Veselinovic        9  |   67   |  37  |  18
--   Naledi Ngwenya            5  |  127   |   5  |   3
--   Nare Avetisyan           10  |  422   | 213  | 140
--   Rodrigo Gomes             4  |    7   |   5  |   1
--   Valeriia Yurykova         7  |  (nil) |  49  |  21
--   Zelimir Stajcic           7  |  271   | 200  | 138
--
-- If future runs of this query drift from PBI by >2% on any column, DO NOT
-- silently "fix" this query — PBI itself may have changed. Re-read Andy's
-- rule above, re-export PBI, and diff job_id-by-job_id.
--
-- Schema: "out.c-reporting-v2"
-- ============================================================================

WITH base_job AS (
    SELECT j."job_id", j."job_sourcer"
    FROM "out.c-reporting-v2"."job" j
    WHERE LOWER(NULLIF(j."test", '')) <> 'true'
      AND COALESCE(j."job_title", '') <> ''
      AND LOWER(NULLIF(j."is_job_archived", '')) <> 'true'
      AND j."job_sourcer" IS NOT NULL
      AND j."job_sourcer" <> ''
      AND j."job_sourcer" <> '-not available-'
),
base_candidate AS (
    SELECT cd."candidate_id", cd."job_id"
    FROM "out.c-reporting-v2"."candidate" cd
    WHERE LOWER(NULLIF(cd."is_candidate_archived", '')) <> 'true'
),
valid_cand AS (
    SELECT cd."candidate_id", cd."job_id", j."job_sourcer"
    FROM base_candidate cd
    JOIN base_job j ON j."job_id" = cd."job_id"
),
ev AS (
    SELECT
        e."candidate_id",
        e."event_type",
        e."moved_to_stage",
        e."moved_to_stageType",
        TRY_TO_DATE(e."date_created") AS date_created,
        CASE
            WHEN e."event_type" = 'Linkedin Visited Profile'                                            THEN e."who_created_event"
            WHEN e."event_type" = 'Moved to stage'    AND e."moved_to_stage"     = 'Contacted'          THEN e."who_created_event"
            WHEN e."event_type" = 'Moved to stage'    AND e."moved_to_stage"     = 'Prospects'          THEN e."who_created_event"
            WHEN e."event_type" = 'Moved to stage'    AND e."moved_to_stageType" = 'Positive Response'  THEN e."who_created_event"
            WHEN e."event_type" = 'Disqualified'                                                        THEN e."who_created_event"
            WHEN e."event_type" = 'Candidate created' AND e."moved_to_stage"     = 'Contacted'          THEN e."who_created_event"
            WHEN e."event_type" = 'Candidate created' AND e."moved_to_stage"     = 'Prospects'          THEN e."who_created_event"
            WHEN e."event_type" = 'Candidate created' AND e."moved_to_stageType" = 'Prospects'          THEN e."who_created_event"
            WHEN e."event_type" = 'Moved to stage'    AND e."moved_to_stage"     = 'Recruiter Screen'   THEN e."who_created_event_first"
            WHEN e."event_type" = 'Evaluation'                                                          THEN e."who_created_event_first"
            WHEN e."moved_to_stage"     = 'Moved to ATS'                                                THEN e."who_created_event_first"
            WHEN e."moved_to_stageType" = 'Offer'                                                       THEN e."who_created_event_first"
            WHEN e."moved_to_stage"     = 'Hired'                                                       THEN e."who_created_event_first"
            ELSE NULL
        END AS credit_sourcer
    FROM "out.c-reporting-v2"."event" e
    WHERE e."candidate_id" IN (SELECT "candidate_id" FROM valid_cand)
),
stage AS (
    SELECT cs."candidate_id",
           TRY_TO_DATE(cs."date_screen_actual") AS date_screen_actual,
           TRY_TO_DATE(cs."date_interview")     AS date_interview
    FROM "out.c-reporting-v2"."candidate_stage" cs
    WHERE cs."candidate_id" IN (SELECT "candidate_id" FROM valid_cand)
),

-- Active Pipelines: jobs where job_sourcer=TS AND exists credited event from TS.
active_pipelines AS (
    SELECT DISTINCT vc."job_id", vc."job_sourcer" AS ts
    FROM valid_cand vc
    JOIN ev ON ev."candidate_id" = vc."candidate_id"
    WHERE ev.credit_sourcer = vc."job_sourcer"
),

-- All candidates who sit on one of those Active Pipelines (by ts).
cand_on_active AS (
    SELECT DISTINCT vc."candidate_id", ap.ts
    FROM valid_cand vc
    JOIN active_pipelines ap ON ap."job_id" = vc."job_id"
),

-- Positive Response: candidate on active pipeline + PR event credited to ts
-- + event date >= 2025-04-14 (DAX constraint).
pos_resp AS (
    SELECT DISTINCT coa.ts, ev."candidate_id"
    FROM cand_on_active coa
    JOIN ev ON ev."candidate_id" = coa."candidate_id" AND ev.credit_sourcer = coa.ts
    WHERE ev."event_type" = 'Moved to stage'
      AND ev."moved_to_stageType" = 'Positive Response'
      AND ev.date_created >= '2025-04-14'
),

-- Actual Screens: candidate on active pipeline + Evaluation event credited to ts
-- + candidate_stage.date_screen_actual in 2024+.
actual_scr AS (
    SELECT DISTINCT coa.ts, s."candidate_id"
    FROM cand_on_active coa
    JOIN stage s ON s."candidate_id" = coa."candidate_id"
    JOIN ev ON ev."candidate_id" = coa."candidate_id" AND ev.credit_sourcer = coa.ts
    WHERE s.date_screen_actual IS NOT NULL
      AND YEAR(s.date_screen_actual) >= 2024
      AND ev."event_type" = 'Evaluation'
),

-- ATS: candidate on active pipeline + Moved-to-ATS event credited to ts
-- + candidate_stage.date_interview in 2024+.
ats AS (
    SELECT DISTINCT coa.ts, s."candidate_id"
    FROM cand_on_active coa
    JOIN stage s ON s."candidate_id" = coa."candidate_id"
    JOIN ev ON ev."candidate_id" = coa."candidate_id" AND ev.credit_sourcer = coa.ts
    WHERE s.date_interview IS NOT NULL
      AND YEAR(s.date_interview) >= 2024
      AND ev."moved_to_stage" = 'Moved to ATS'
),

ap_count AS (SELECT ts, COUNT(DISTINCT "job_id")      AS active_pipelines FROM active_pipelines GROUP BY ts),
pr_count AS (SELECT ts, COUNT(DISTINCT "candidate_id") AS positive_response FROM pos_resp       GROUP BY ts),
as_count AS (SELECT ts, COUNT(DISTINCT "candidate_id") AS actual_screens    FROM actual_scr     GROUP BY ts),
at_count AS (SELECT ts, COUNT(DISTINCT "candidate_id") AS ats_count         FROM ats            GROUP BY ts)

SELECT
    ap.ts,
    ap.active_pipelines,
    COALESCE(pr.positive_response, 0) AS positive_response,
    COALESCE(ass.actual_screens,   0) AS actual_screens,
    COALESCE(at.ats_count,         0) AS ats
FROM ap_count ap
LEFT JOIN pr_count pr  ON pr.ts  = ap.ts
LEFT JOIN as_count ass ON ass.ts = ap.ts
LEFT JOIN at_count at  ON at.ts  = ap.ts
ORDER BY ap.active_pipelines DESC, ap.ts;
