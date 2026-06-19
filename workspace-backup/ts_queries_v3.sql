-- ============================================================================
-- ts_queries_v3.sql — TS (Sourcer) Keboola extracts, PBI-aligned
-- ============================================================================
--
-- WHAT CHANGED FROM v2:
-- v2 grouped by job.job_sourcer (official sourcer on the job).
-- PBI's WBR TS Actual DAX groups by event.who_created_event_first (credit
-- sourcer derived per-event via the Power Query conditional rule in the
-- `event` M-code, lines 118-134 of POWERBI_DAX_MEASURES.md).
--
-- v3 replicates that exactly:
--
--   credit_sourcer(event) =
--       CASE
--         -- Early funnel: credit goes to who did the action TODAY
--         WHEN event_type='Linkedin Visited Profile'                           THEN who_created_event
--         WHEN event_type='Moved to stage'  AND moved_to_stage='Contacted'      THEN who_created_event
--         WHEN event_type='Moved to stage'  AND moved_to_stage='Prospects'      THEN who_created_event
--         WHEN event_type='Moved to stage'  AND moved_to_stageType='Positive Response' THEN who_created_event
--         WHEN event_type='Disqualified'                                        THEN who_created_event
--         WHEN event_type='Candidate created' AND moved_to_stage='Contacted'    THEN who_created_event
--         WHEN event_type='Candidate created' AND moved_to_stage='Prospects'    THEN who_created_event
--         WHEN event_type='Candidate created' AND moved_to_stageType='Prospects' THEN who_created_event
--         -- Late funnel: credit goes to ORIGINAL sourcer
--         WHEN event_type='Moved to stage'  AND moved_to_stage='Recruiter Screen' THEN who_created_event_first
--         WHEN event_type='Evaluation'                                          THEN who_created_event_first
--         WHEN moved_to_stage='Moved to ATS'                                    THEN who_created_event_first
--         WHEN moved_to_stageType='Offer'                                       THEN who_created_event_first
--         WHEN moved_to_stage='Hired'                                           THEN who_created_event_first
--         ELSE NULL
--       END
--
-- Each metric then follows the PBI measure logic: count DISTINCT candidates
-- where candidate_stage.date_<metric> is in week W AND there exists an event
-- matching the metric's event condition AND the event's credit_sourcer = S.
--
-- Metric -> event gate (from DAX definitions):
--   contacted       -> NO event gate (just date_contacted <> BLANK)
--                      -> credited via any event on the candidate? See §A below.
--   screen          -> event.moved_to_stage = 'Recruiter Screen'
--   actual_screen   -> event.event_type = 'Evaluation'
--   ats             -> event.moved_to_stage = 'Moved to ATS'
--   offer           -> event.moved_to_stageType = 'Offer'
--   hired           -> event.moved_to_stage = 'Hired'
--
-- §A — Contacted attribution nuance:
--   The DAX measure has no event filter, BUT the grouping column
--   `event[who_created_event_first]` STILL filters candidates via relationship.
--   Practically: for Contacted we attribute the candidate to credit_sourcer
--   of the "Contacted" event (Moved to stage = Contacted) when one exists.
--   Falls back to the candidate's first credit_sourcer otherwise.
--
-- Filters (match DAX — WBR TS Actual):
--   job.test <> true, candidate.is_candidate_archived = false,
--   job.job_title <> BLANK, credit_sourcer <> BLANK, Year >= 2024
--   NO client-level test filter, NO client_name exclusion — PBI's DAX filters
--   only on job[test], NOT on client.test. (Taxfix has client.test=true but
--   103/104 of its jobs are legit test=false, so a client-level filter wrongly
--   drops ~32 candidates per week for sourcers like Valeriia on Taxfix.)
--
-- Schema: "out.c-reporting-v2"
-- ============================================================================


-- ============================================================================
-- QUERY 1: ts_actuals.csv
-- Weekly TS metrics per credit sourcer. One row per (ts, week).
-- Columns: ts, week, contacted, recruiter_screens, actual_screens, ats,
--          offers, hires
-- ============================================================================

WITH base_job AS (
    SELECT j."job_id", j."client_id"
    FROM "job" j
    WHERE LOWER(NULLIF(j."test", '')) <> 'true'
      AND COALESCE(j."job_title", '') <> ''
),
base_candidate AS (
    SELECT cd."candidate_id", cd."job_id"
    FROM "candidate" cd
    WHERE LOWER(NULLIF(cd."is_candidate_archived", '')) <> 'true'
),
-- valid candidate universe: passes job + candidate filters only
-- (PBI's WBR TS Actual DAX does NOT apply a client-level filter)
valid_cand AS (
    SELECT cd."candidate_id"
    FROM base_candidate cd
    JOIN base_job j ON j."job_id" = cd."job_id"
),
-- Apply PBI's conditional rule to derive credit_sourcer per event.
ev AS (
    SELECT
        e."event_id",
        e."candidate_id",
        e."event_type",
        e."moved_to_stage",
        e."moved_to_stageType",
        TRY_TO_DATE(e."date_created") AS date_created,
        CASE
            WHEN e."event_type" = 'Linkedin Visited Profile'                                                       THEN e."who_created_event"
            WHEN e."event_type" = 'Moved to stage'    AND e."moved_to_stage"     = 'Contacted'                     THEN e."who_created_event"
            WHEN e."event_type" = 'Moved to stage'    AND e."moved_to_stage"     = 'Prospects'                     THEN e."who_created_event"
            WHEN e."event_type" = 'Moved to stage'    AND e."moved_to_stageType" = 'Positive Response'             THEN e."who_created_event"
            WHEN e."event_type" = 'Disqualified'                                                                   THEN e."who_created_event"
            WHEN e."event_type" = 'Candidate created' AND e."moved_to_stage"     = 'Contacted'                     THEN e."who_created_event"
            WHEN e."event_type" = 'Candidate created' AND e."moved_to_stage"     = 'Prospects'                     THEN e."who_created_event"
            WHEN e."event_type" = 'Candidate created' AND e."moved_to_stageType" = 'Prospects'                     THEN e."who_created_event"
            WHEN e."event_type" = 'Moved to stage'    AND e."moved_to_stage"     = 'Recruiter Screen'              THEN e."who_created_event_first"
            WHEN e."event_type" = 'Evaluation'                                                                     THEN e."who_created_event_first"
            WHEN e."moved_to_stage"     = 'Moved to ATS'                                                           THEN e."who_created_event_first"
            WHEN e."moved_to_stageType" = 'Offer'                                                                  THEN e."who_created_event_first"
            WHEN e."moved_to_stage"     = 'Hired'                                                                  THEN e."who_created_event_first"
            ELSE NULL
        END AS credit_sourcer
    FROM "event" e
    WHERE e."candidate_id" IN (SELECT "candidate_id" FROM valid_cand)
),
-- Stage dates
stage AS (
    SELECT cs."candidate_id",
           TRY_TO_DATE(cs."date_contacted")      AS date_contacted,
           TRY_TO_DATE(cs."date_screen")         AS date_screen,
           TRY_TO_DATE(cs."date_screen_actual")  AS date_screen_actual,
           TRY_TO_DATE(cs."date_interview")      AS date_interview,
           TRY_TO_DATE(cs."date_offer")          AS date_offer,
           TRY_TO_DATE(cs."date_hired")          AS date_hired
    FROM "candidate_stage" cs
    WHERE cs."candidate_id" IN (SELECT "candidate_id" FROM valid_cand)
),

-- ---- Per-metric pairs (candidate, sourcer, week) ----
-- CONTACTED: event where Moved to stage = Contacted
contacted_pairs AS (
    SELECT DISTINCT
        ev.credit_sourcer AS ts,
        WEEKISO(s.date_contacted) AS week,
        s."candidate_id"
    FROM stage s
    JOIN ev ON ev."candidate_id" = s."candidate_id"
    WHERE s.date_contacted IS NOT NULL
      AND YEAR(s.date_contacted) >= 2024
      AND ev.credit_sourcer IS NOT NULL
      AND ev.credit_sourcer <> ''
      AND ev."moved_to_stage" = 'Contacted'
),
-- RECRUITER SCREENS: event where Moved to stage = Recruiter Screen
screen_pairs AS (
    SELECT DISTINCT
        ev.credit_sourcer AS ts,
        WEEKISO(s.date_screen) AS week,
        s."candidate_id"
    FROM stage s
    JOIN ev ON ev."candidate_id" = s."candidate_id"
    WHERE s.date_screen IS NOT NULL
      AND YEAR(s.date_screen) >= 2024
      AND ev.credit_sourcer IS NOT NULL
      AND ev.credit_sourcer <> ''
      AND ev."moved_to_stage" = 'Recruiter Screen'
),
-- ACTUAL SCREENS: event where event_type = Evaluation
actual_screen_pairs AS (
    SELECT DISTINCT
        ev.credit_sourcer AS ts,
        WEEKISO(s.date_screen_actual) AS week,
        s."candidate_id"
    FROM stage s
    JOIN ev ON ev."candidate_id" = s."candidate_id"
    WHERE s.date_screen_actual IS NOT NULL
      AND YEAR(s.date_screen_actual) >= 2024
      AND ev.credit_sourcer IS NOT NULL
      AND ev.credit_sourcer <> ''
      AND ev."event_type" = 'Evaluation'
),
-- ATS: event where Moved to stage = Moved to ATS
ats_pairs AS (
    SELECT DISTINCT
        ev.credit_sourcer AS ts,
        WEEKISO(s.date_interview) AS week,
        s."candidate_id"
    FROM stage s
    JOIN ev ON ev."candidate_id" = s."candidate_id"
    WHERE s.date_interview IS NOT NULL
      AND YEAR(s.date_interview) >= 2024
      AND ev.credit_sourcer IS NOT NULL
      AND ev.credit_sourcer <> ''
      AND ev."moved_to_stage" = 'Moved to ATS'
),
-- OFFERS: event where moved_to_stageType = Offer
offer_pairs AS (
    SELECT DISTINCT
        ev.credit_sourcer AS ts,
        WEEKISO(s.date_offer) AS week,
        s."candidate_id"
    FROM stage s
    JOIN ev ON ev."candidate_id" = s."candidate_id"
    WHERE s.date_offer IS NOT NULL
      AND YEAR(s.date_offer) >= 2024
      AND ev.credit_sourcer IS NOT NULL
      AND ev.credit_sourcer <> ''
      AND ev."moved_to_stageType" = 'Offer'
),
-- HIRES: event where Moved to stage = Hired
hire_pairs AS (
    SELECT DISTINCT
        ev.credit_sourcer AS ts,
        WEEKISO(s.date_hired) AS week,
        s."candidate_id"
    FROM stage s
    JOIN ev ON ev."candidate_id" = s."candidate_id"
    WHERE s.date_hired IS NOT NULL
      AND YEAR(s.date_hired) >= 2024
      AND ev.credit_sourcer IS NOT NULL
      AND ev.credit_sourcer <> ''
      AND ev."moved_to_stage" = 'Hired'
),
all_metrics AS (
    SELECT ts, week, 'contacted'         AS metric, COUNT(DISTINCT "candidate_id") AS val FROM contacted_pairs     GROUP BY 1,2
    UNION ALL
    SELECT ts, week, 'recruiter_screens' AS metric, COUNT(DISTINCT "candidate_id") AS val FROM screen_pairs        GROUP BY 1,2
    UNION ALL
    SELECT ts, week, 'actual_screens'    AS metric, COUNT(DISTINCT "candidate_id") AS val FROM actual_screen_pairs GROUP BY 1,2
    UNION ALL
    SELECT ts, week, 'ats'               AS metric, COUNT(DISTINCT "candidate_id") AS val FROM ats_pairs           GROUP BY 1,2
    UNION ALL
    SELECT ts, week, 'offers'            AS metric, COUNT(DISTINCT "candidate_id") AS val FROM offer_pairs         GROUP BY 1,2
    UNION ALL
    SELECT ts, week, 'hires'             AS metric, COUNT(DISTINCT "candidate_id") AS val FROM hire_pairs          GROUP BY 1,2
)
SELECT
    ts, week,
    MAX(CASE WHEN metric = 'contacted'         THEN val ELSE 0 END) AS contacted,
    MAX(CASE WHEN metric = 'recruiter_screens' THEN val ELSE 0 END) AS recruiter_screens,
    MAX(CASE WHEN metric = 'actual_screens'    THEN val ELSE 0 END) AS actual_screens,
    MAX(CASE WHEN metric = 'ats'               THEN val ELSE 0 END) AS ats,
    MAX(CASE WHEN metric = 'offers'            THEN val ELSE 0 END) AS offers,
    MAX(CASE WHEN metric = 'hires'             THEN val ELSE 0 END) AS hires
FROM all_metrics
GROUP BY ts, week
ORDER BY ts, week;


-- ============================================================================
-- QUERY 2: ts_jobs.csv
-- Active jobs per credit sourcer + TA names they sourced FOR.
-- Mirrors PBI's `TA` column: CONCATENATEX of event.who_event_created_for
-- excluding the sourcer themselves and blanks.
-- Columns: ts, jobs, num_tas, ta_names
-- ============================================================================

WITH base_job AS (
    SELECT j."job_id", j."client_id"
    FROM "job" j
    WHERE LOWER(NULLIF(j."test", '')) <> 'true'
      AND LOWER(NULLIF(j."is_job_archived", '')) <> 'true'
      AND COALESCE(j."job_title", '') <> ''
),
base_candidate AS (
    SELECT cd."candidate_id", cd."job_id"
    FROM "candidate" cd
    WHERE LOWER(NULLIF(cd."is_candidate_archived", '')) <> 'true'
),
valid_cand AS (
    SELECT cd."candidate_id", cd."job_id"
    FROM base_candidate cd
    JOIN base_job j ON j."job_id" = cd."job_id"
),
ev AS (
    SELECT
        e."candidate_id",
        e."who_event_created_for",
        CASE
            WHEN e."event_type" = 'Linkedin Visited Profile'                                                       THEN e."who_created_event"
            WHEN e."event_type" = 'Moved to stage'    AND e."moved_to_stage"     = 'Contacted'                     THEN e."who_created_event"
            WHEN e."event_type" = 'Moved to stage'    AND e."moved_to_stage"     = 'Prospects'                     THEN e."who_created_event"
            WHEN e."event_type" = 'Moved to stage'    AND e."moved_to_stageType" = 'Positive Response'             THEN e."who_created_event"
            WHEN e."event_type" = 'Disqualified'                                                                   THEN e."who_created_event"
            WHEN e."event_type" = 'Candidate created' AND e."moved_to_stage"     = 'Contacted'                     THEN e."who_created_event"
            WHEN e."event_type" = 'Candidate created' AND e."moved_to_stage"     = 'Prospects'                     THEN e."who_created_event"
            WHEN e."event_type" = 'Candidate created' AND e."moved_to_stageType" = 'Prospects'                     THEN e."who_created_event"
            WHEN e."event_type" = 'Moved to stage'    AND e."moved_to_stage"     = 'Recruiter Screen'              THEN e."who_created_event_first"
            WHEN e."event_type" = 'Evaluation'                                                                     THEN e."who_created_event_first"
            WHEN e."moved_to_stage"     = 'Moved to ATS'                                                           THEN e."who_created_event_first"
            WHEN e."moved_to_stageType" = 'Offer'                                                                  THEN e."who_created_event_first"
            WHEN e."moved_to_stage"     = 'Hired'                                                                  THEN e."who_created_event_first"
            ELSE NULL
        END AS credit_sourcer
    FROM "event" e
    WHERE e."candidate_id" IN (SELECT "candidate_id" FROM valid_cand)
),
-- Jobs a sourcer touched = jobs where any of their credited events landed
ts_jobs AS (
    SELECT DISTINCT ev.credit_sourcer AS ts, vc."job_id"
    FROM ev
    JOIN valid_cand vc ON vc."candidate_id" = ev."candidate_id"
    WHERE ev.credit_sourcer IS NOT NULL
      AND ev.credit_sourcer <> ''
),
-- TAs a sourcer sourced FOR (who_event_created_for != credit_sourcer)
ts_tas AS (
    SELECT DISTINCT ev.credit_sourcer AS ts, ev."who_event_created_for" AS ta
    FROM ev
    WHERE ev.credit_sourcer IS NOT NULL
      AND ev.credit_sourcer <> ''
      AND ev."who_event_created_for" IS NOT NULL
      AND ev."who_event_created_for" <> ''
      AND ev."who_event_created_for" <> ev.credit_sourcer
)
SELECT
    j.ts,
    COUNT(DISTINCT j."job_id") AS jobs,
    (SELECT COUNT(DISTINCT t.ta)        FROM ts_tas t WHERE t.ts = j.ts) AS num_tas,
    (SELECT LISTAGG(DISTINCT t.ta, ', ') WITHIN GROUP (ORDER BY t.ta)
        FROM ts_tas t WHERE t.ts = j.ts) AS ta_names
FROM ts_jobs j
GROUP BY j.ts
ORDER BY j.ts;


-- ============================================================================
-- QUERY 3: ts_hires_12w.csv
-- Hires in the last 12 ISO weeks per credit sourcer.
-- Columns: ts, hires_12w
-- ============================================================================

WITH base_job AS (
    SELECT j."job_id", j."client_id"
    FROM "job" j
    WHERE LOWER(NULLIF(j."test", '')) <> 'true'
      AND COALESCE(j."job_title", '') <> ''
),
base_candidate AS (
    SELECT cd."candidate_id", cd."job_id"
    FROM "candidate" cd
    WHERE LOWER(NULLIF(cd."is_candidate_archived", '')) <> 'true'
),
valid_cand AS (
    SELECT cd."candidate_id"
    FROM base_candidate cd
    JOIN base_job j ON j."job_id" = cd."job_id"
),
ev AS (
    SELECT
        e."candidate_id",
        CASE
            WHEN e."moved_to_stage" = 'Hired' THEN e."who_created_event_first"
            ELSE NULL
        END AS credit_sourcer
    FROM "event" e
    WHERE e."moved_to_stage" = 'Hired'
      AND e."candidate_id" IN (SELECT "candidate_id" FROM valid_cand)
),
stage AS (
    SELECT cs."candidate_id", TRY_TO_DATE(cs."date_hired") AS date_hired
    FROM "candidate_stage" cs
    WHERE cs."candidate_id" IN (SELECT "candidate_id" FROM valid_cand)
      AND TRY_TO_DATE(cs."date_hired") IS NOT NULL
      AND TRY_TO_DATE(cs."date_hired") >= DATEADD('week', -12, DATE_TRUNC('week', CURRENT_DATE()))
)
SELECT
    ev.credit_sourcer AS ts,
    COUNT(DISTINCT s."candidate_id") AS hires_12w
FROM stage s
JOIN ev ON ev."candidate_id" = s."candidate_id"
WHERE ev.credit_sourcer IS NOT NULL
  AND ev.credit_sourcer <> ''
GROUP BY 1
ORDER BY 1;
