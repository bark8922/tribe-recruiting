-- ============================================================================
-- ts_queries_v2.sql — Corrected TS (Sourcer) Keboola extracts
-- ============================================================================
--
-- FIX: Uses job.job_sourcer (official sourcer on the job) instead of
--      who_created_event_first (credit sourcer). This matches Power BI's
--      "WBR TS Weekly Target" view.
--
-- Andy confirmed (Apr 13):
--   1. Client grouping is mandatory (TA can work multiple clients)
--   2. Wolt + DoorDash + SevenRooms grouped into one
--   3. Actual Screens = date_screen_actual verified by evaluation event
--   4. Recruiter Screens = date_screen (moved to stage)
--
-- Run each query separately in Keboola and export as CSV.
-- Schema: "out.c-reporting-v2"
-- ============================================================================


-- ============================================================================
-- QUERY 1: ts_actuals.csv
-- Weekly metrics per sourcer. Uses independent week counting (each metric
-- counted by its own date's WEEKISO).
-- Columns: ts, week, contacted, recruiter_screens, actual_screens, ats, offers, hires
-- ============================================================================

WITH base_job AS (
    SELECT j."job_id", j."client_id", j."job_recruiter", j."job_sourcer",
           LOWER(NULLIF(j."is_job_archived", '')) = 'true' AS is_archived
    FROM "job" j
    WHERE LOWER(NULLIF(j."test", '')) <> 'true'
      AND COALESCE(j."job_title", '') <> ''
),
base_client AS (
    SELECT c."client_id", c."client_name"
    FROM "client" c
    WHERE LOWER(NULLIF(c."test", '')) <> 'true'
      AND COALESCE(c."client_name", '') <> ''
      AND c."client_name" NOT IN ('Tribe.xyz', 'Kamila AI - TEST')
),
base_candidate AS (
    SELECT cd."candidate_id", cd."job_id"
    FROM "candidate" cd
    WHERE LOWER(NULLIF(cd."is_candidate_archived", '')) <> 'true'
),
-- Join spine: candidate → job → client, carrying job_sourcer
cjc AS (
    SELECT cd."candidate_id", j."job_id", j."job_sourcer", j."job_recruiter",
           c."client_name", j.is_archived
    FROM base_candidate cd
    JOIN base_job j ON j."job_id" = cd."job_id"
    JOIN base_client c ON c."client_id" = j."client_id"
    WHERE COALESCE(j."job_sourcer", '') <> ''
      AND j."job_sourcer" <> '-not available-'
),
-- Authoritative stage dates from candidate_stage
stage AS (
    SELECT cs."candidate_id",
           TRY_TO_DATE(cs."date_contacted")      AS date_contacted,
           TRY_TO_DATE(cs."date_screen")          AS date_screen,
           TRY_TO_DATE(cs."date_screen_actual")   AS date_screen_actual,
           TRY_TO_DATE(cs."date_interview")       AS date_interview,
           TRY_TO_DATE(cs."date_offer")           AS date_offer,
           TRY_TO_DATE(cs."date_hired")           AS date_hired
    FROM "candidate_stage" cs
    JOIN cjc ON cjc."candidate_id" = cs."candidate_id"
),
-- Independent week counting: each metric uses its OWN date's week
-- (same pattern as the validated TA actuals query)
ts_metrics AS (
    SELECT cjc."job_sourcer" AS ts, WEEKISO(s.date_contacted) AS week,
           COUNT(DISTINCT cjc."candidate_id") AS val, 'contacted' AS metric
    FROM cjc JOIN stage s ON s."candidate_id" = cjc."candidate_id"
    WHERE s.date_contacted IS NOT NULL AND YEAR(s.date_contacted) = 2026
    GROUP BY 1, 2

    UNION ALL

    SELECT cjc."job_sourcer", WEEKISO(s.date_screen),
           COUNT(DISTINCT cjc."candidate_id"), 'recruiter_screens'
    FROM cjc JOIN stage s ON s."candidate_id" = cjc."candidate_id"
    WHERE s.date_screen IS NOT NULL AND YEAR(s.date_screen) = 2026
    GROUP BY 1, 2

    UNION ALL

    SELECT cjc."job_sourcer", WEEKISO(s.date_screen_actual),
           COUNT(DISTINCT cjc."candidate_id"), 'actual_screens'
    FROM cjc JOIN stage s ON s."candidate_id" = cjc."candidate_id"
    WHERE s.date_screen_actual IS NOT NULL AND YEAR(s.date_screen_actual) = 2026
    GROUP BY 1, 2

    UNION ALL

    SELECT cjc."job_sourcer", WEEKISO(s.date_interview),
           COUNT(DISTINCT cjc."candidate_id"), 'ats'
    FROM cjc JOIN stage s ON s."candidate_id" = cjc."candidate_id"
    WHERE s.date_interview IS NOT NULL AND YEAR(s.date_interview) = 2026
    GROUP BY 1, 2

    UNION ALL

    SELECT cjc."job_sourcer", WEEKISO(s.date_offer),
           COUNT(DISTINCT cjc."candidate_id"), 'offers'
    FROM cjc JOIN stage s ON s."candidate_id" = cjc."candidate_id"
    WHERE s.date_offer IS NOT NULL AND YEAR(s.date_offer) = 2026
    GROUP BY 1, 2

    UNION ALL

    SELECT cjc."job_sourcer", WEEKISO(s.date_hired),
           COUNT(DISTINCT cjc."candidate_id"), 'hires'
    FROM cjc JOIN stage s ON s."candidate_id" = cjc."candidate_id"
    WHERE s.date_hired IS NOT NULL AND YEAR(s.date_hired) = 2026
    GROUP BY 1, 2
)
SELECT
    ts, week,
    MAX(CASE WHEN metric = 'contacted' THEN val ELSE 0 END)          AS contacted,
    MAX(CASE WHEN metric = 'recruiter_screens' THEN val ELSE 0 END)  AS recruiter_screens,
    MAX(CASE WHEN metric = 'actual_screens' THEN val ELSE 0 END)     AS actual_screens,
    MAX(CASE WHEN metric = 'ats' THEN val ELSE 0 END)                AS ats,
    MAX(CASE WHEN metric = 'offers' THEN val ELSE 0 END)             AS offers,
    MAX(CASE WHEN metric = 'hires' THEN val ELSE 0 END)              AS hires
FROM ts_metrics
GROUP BY ts, week
ORDER BY ts, week;


-- ============================================================================
-- QUERY 2: ts_jobs.csv
-- Active jobs per sourcer with TA names (excludes sourcer from TA list)
-- Only counts NON-ARCHIVED jobs.
-- Columns: ts, jobs, num_tas, ta_names
-- ============================================================================

WITH base_job AS (
    SELECT j."job_id", j."client_id", j."job_recruiter", j."job_sourcer"
    FROM "job" j
    WHERE LOWER(NULLIF(j."test", '')) <> 'true'
      AND LOWER(NULLIF(j."is_job_archived", '')) <> 'true'
      AND COALESCE(j."job_title", '') <> ''
      AND COALESCE(j."job_sourcer", '') <> ''
      AND j."job_sourcer" <> '-not available-'
),
base_client AS (
    SELECT c."client_id", c."client_name"
    FROM "client" c
    WHERE LOWER(NULLIF(c."test", '')) <> 'true'
      AND COALESCE(c."client_name", '') <> ''
      AND c."client_name" NOT IN ('Tribe.xyz', 'Kamila AI - TEST')
),
jobs_with_client AS (
    SELECT j."job_id", j."job_sourcer", j."job_recruiter", c."client_name"
    FROM base_job j
    JOIN base_client c ON c."client_id" = j."client_id"
),
-- Count jobs and collect distinct TAs (excluding the sourcer themselves)
ts_summary AS (
    SELECT
        j."job_sourcer" AS ts,
        COUNT(DISTINCT j."job_id") AS jobs,
        COUNT(DISTINCT CASE
            WHEN j."job_recruiter" <> j."job_sourcer"
             AND j."job_recruiter" <> '-not available-'
             AND COALESCE(j."job_recruiter", '') <> ''
            THEN j."job_recruiter"
        END) AS num_tas,
        LISTAGG(DISTINCT CASE
            WHEN j."job_recruiter" <> j."job_sourcer"
             AND j."job_recruiter" <> '-not available-'
             AND COALESCE(j."job_recruiter", '') <> ''
            THEN j."job_recruiter"
        END, ', ') WITHIN GROUP (ORDER BY j."job_recruiter") AS ta_names
    FROM jobs_with_client j
    GROUP BY 1
)
SELECT ts, jobs, num_tas, ta_names
FROM ts_summary
ORDER BY ts;


-- ============================================================================
-- QUERY 3: ts_hires_12w.csv
-- Hires in the last 12 ISO weeks, per sourcer (using job_sourcer).
-- Columns: ts, hires_12w
-- ============================================================================

WITH base_job AS (
    SELECT j."job_id", j."client_id", j."job_sourcer"
    FROM "job" j
    WHERE LOWER(NULLIF(j."test", '')) <> 'true'
      AND COALESCE(j."job_title", '') <> ''
      AND COALESCE(j."job_sourcer", '') <> ''
      AND j."job_sourcer" <> '-not available-'
),
base_client AS (
    SELECT c."client_id"
    FROM "client" c
    WHERE LOWER(NULLIF(c."test", '')) <> 'true'
      AND COALESCE(c."client_name", '') <> ''
      AND c."client_name" NOT IN ('Tribe.xyz', 'Kamila AI - TEST')
),
base_candidate AS (
    SELECT cd."candidate_id", cd."job_id"
    FROM "candidate" cd
    WHERE LOWER(NULLIF(cd."is_candidate_archived", '')) <> 'true'
),
cjc AS (
    SELECT cd."candidate_id", j."job_sourcer"
    FROM base_candidate cd
    JOIN base_job j ON j."job_id" = cd."job_id"
    JOIN base_client c ON c."client_id" = j."client_id"
)
SELECT
    cjc."job_sourcer" AS ts,
    COUNT(DISTINCT cjc."candidate_id") AS hires_12w
FROM cjc
JOIN "candidate_stage" cs ON cs."candidate_id" = cjc."candidate_id"
WHERE TRY_TO_DATE(cs."date_hired") IS NOT NULL
  AND TRY_TO_DATE(cs."date_hired") >= DATEADD('week', -12, DATE_TRUNC('week', CURRENT_DATE()))
GROUP BY 1
ORDER BY 1;
