-- project_dashboard_hires.sql — line-level hires for Project Dashboard drill-down
--
-- One row per hired candidate from 2025-01-01 onwards. Powers the collapsed
-- "Hires in this period" panel in the Project Dashboard tab.
--
-- Output schema (matches snowflake_project_dashboard_hires.csv):
--   CANDIDATE_ID, CLIENT, JOB_ID, JOB_TITLE, TA, TS, CANDIDATE_SOURCE,
--   IS_EXTERNAL_RECRUITER,
--   DATE_CONTACTED, DATE_SCREEN_ACTUAL, DATE_OFFER, DATE_HIRED
--
-- Same attribution + filter rules as project_dashboard.sql. Row is emitted when
-- candidate_stage.date_hired is non-null and >= 2025-01-01.
--
-- TALENT_ID / TALENT_NAME / LINKEDIN_URL deferred to v2 (talent table lookup
-- requires additional join and isn't strictly needed for the first pass —
-- candidate_id is sufficient to identify each hire uniquely).

WITH cand AS (
  SELECT
    c."candidate_id",
    c."job_id",
    TRIM(c."candidate_sourcer") AS ts,
    c."source"                  AS candidate_source
  FROM "KEBOOLA_855"."out.c-reporting-v2"."candidate" c
  WHERE LOWER(NULLIF(c."is_candidate_archived",'')) <> 'true'
),
job_meta AS (
  SELECT
    j."job_id",
    TRIM(cl."client_name")          AS client,
    j."job_title",
    TRIM(j."job_recruiter")         AS ta,
    j."is_external_recruiter"       AS is_external_recruiter
  FROM "KEBOOLA_855"."out.c-reporting-v2"."job"    j
  JOIN "KEBOOLA_855"."out.c-reporting-v2"."client" cl ON cl."client_id" = j."client_id"
  WHERE LOWER(NULLIF(j."test",'')) <> 'true'
    AND cl."client_name" IS NOT NULL
    AND cl."client_name" <> ''
    AND cl."client_name" NOT IN ('Tribe.xyz','Tribe.xyz (IR)','BD - Tribe','Tribe - Marketing','Kamila AI - TEST','Bubble test')
),
stage AS (
  SELECT
    cs."candidate_id",
    TRY_TO_DATE(cs."date_contacted")     AS date_contacted,
    TRY_TO_DATE(cs."date_screen_actual") AS date_screen_actual,
    TRY_TO_DATE(cs."date_offer")         AS date_offer,
    TRY_TO_DATE(cs."date_hired")         AS date_hired
  FROM "KEBOOLA_855"."out.c-reporting-v2"."candidate_stage" cs
  WHERE TRY_TO_DATE(cs."date_hired") IS NOT NULL
    AND TRY_TO_DATE(cs."date_hired") >= DATE '2025-01-01'
)
SELECT
  c."candidate_id"                  AS "CANDIDATE_ID",
  jm.client                         AS "CLIENT",
  jm."job_id"                       AS "JOB_ID",
  jm."job_title"                    AS "JOB_TITLE",
  jm.ta                             AS "TA",
  COALESCE(c.ts,'')                 AS "TS",
  COALESCE(c.candidate_source,'')   AS "CANDIDATE_SOURCE",
  jm.is_external_recruiter          AS "IS_EXTERNAL_RECRUITER",
  TO_VARCHAR(s.date_contacted,     'YYYY-MM-DD') AS "DATE_CONTACTED",
  TO_VARCHAR(s.date_screen_actual, 'YYYY-MM-DD') AS "DATE_SCREEN_ACTUAL",
  TO_VARCHAR(s.date_offer,         'YYYY-MM-DD') AS "DATE_OFFER",
  TO_VARCHAR(s.date_hired,         'YYYY-MM-DD') AS "DATE_HIRED"
FROM stage    s
JOIN cand     c  ON c."candidate_id" = s."candidate_id"
JOIN job_meta jm ON jm."job_id"      = c."job_id"
ORDER BY s.date_hired DESC, jm.client, jm."job_title"
