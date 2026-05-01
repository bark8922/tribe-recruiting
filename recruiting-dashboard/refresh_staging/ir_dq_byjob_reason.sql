-- ir_dq_byjob_reason.sql — per-(job_id, reason) DQ count for Tribe.xyz (IR)
--
-- Output: JOB_ID, REASON, COUNT
--
-- One row per distinct (job, reason_not_interested). Frontend aggregates by
-- selected job filter or shows All.

WITH ir_job AS (
  SELECT j."job_id" FROM "KEBOOLA_855"."out.c-reporting-v2"."job" j
  JOIN "KEBOOLA_855"."out.c-reporting-v2"."client" cl ON cl."client_id" = j."client_id"
  WHERE cl."client_name" = 'Tribe.xyz (IR)'
    AND LOWER(NULLIF(j."test",'')) <> 'true'
    AND NULLIF(j."job_title",'') IS NOT NULL
)
SELECT
  c."job_id" "JOB_ID",
  COALESCE(NULLIF(TRIM(c."reason_not_interested"),''), '(blank)') "REASON",
  COUNT(DISTINCT c."candidate_id") "COUNT"
FROM "KEBOOLA_855"."out.c-reporting-v2"."candidate" c
WHERE c."job_id" IN (SELECT "job_id" FROM ir_job)
  AND NULLIF(c."reason_not_interested",'') IS NOT NULL
GROUP BY 1,2
ORDER BY "JOB_ID","COUNT" DESC
