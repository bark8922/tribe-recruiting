-- ir_jobs_active.sql — active Tribe.xyz (IR) jobs with days open + hires
--
-- Output schema:
--   JOB_ID, JOB_TITLE, DATE_CREATED, JOB_RECRUITER, JOB_SOURCER,
--   DAYS_OPEN, HIRES_TOTAL
--
-- Andy's snapshot data (1).xlsx Active Jobs (2026-04-24):
--   TAP Internal 84, DH ExecSearch 74, TS German 67, Generalist TAP 30,
--   Tech TAP APAC 28, Tech TS 9 — all with 0 hired

WITH ir_job AS (
  SELECT
    j."job_id",
    j."job_title",
    TRY_TO_DATE(j."date_created") AS date_created,
    NULLIF(TRIM(j."job_recruiter"),'') AS job_recruiter,
    NULLIF(TRIM(j."job_sourcer"),'')   AS job_sourcer
  FROM "KEBOOLA_855"."out.c-reporting-v2"."job"    j
  JOIN "KEBOOLA_855"."out.c-reporting-v2"."client" cl ON cl."client_id" = j."client_id"
  WHERE cl."client_name" = 'Tribe.xyz (IR)'
    AND LOWER(NULLIF(j."is_job_archived",'')) <> 'true'
    AND LOWER(NULLIF(j."test",''))            <> 'true'
    AND NULLIF(j."job_title",'') IS NOT NULL
),
hires AS (
  SELECT c."job_id", COUNT(DISTINCT cs."candidate_id") AS n
  FROM "KEBOOLA_855"."out.c-reporting-v2"."candidate_stage" cs
  JOIN "KEBOOLA_855"."out.c-reporting-v2"."candidate"       c ON c."candidate_id" = cs."candidate_id"
  WHERE TRY_TO_DATE(cs."date_hired") IS NOT NULL
    AND c."job_id" IN (SELECT "job_id" FROM ir_job)
  GROUP BY c."job_id"
)
SELECT
  ij."job_id"        AS "JOB_ID",
  ij."job_title"     AS "JOB_TITLE",
  ij.date_created    AS "DATE_CREATED",
  ij.job_recruiter   AS "JOB_RECRUITER",
  ij.job_sourcer     AS "JOB_SOURCER",
  DATEDIFF('day', ij.date_created, CURRENT_DATE())   AS "DAYS_OPEN",
  COALESCE(h.n, 0)   AS "HIRES_TOTAL"
FROM ir_job ij
LEFT JOIN hires h ON h."job_id" = ij."job_id"
ORDER BY "DAYS_OPEN" DESC
