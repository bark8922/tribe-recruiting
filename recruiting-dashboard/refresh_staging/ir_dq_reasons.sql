-- ir_dq_reasons.sql — DQ reason breakdown for Tribe.xyz (IR)
--
-- Output schema:
--   REASON, COUNT
--
-- One row per distinct reason_not_interested across all IR candidates,
-- ordered descending by count. Powers the "Disqualified Reasons" horizontal
-- bar list on the IR tab.

WITH ir_job AS (
  SELECT j."job_id"
  FROM "KEBOOLA_855"."out.c-reporting-v2"."job"    j
  JOIN "KEBOOLA_855"."out.c-reporting-v2"."client" cl ON cl."client_id" = j."client_id"
  WHERE cl."client_name" = 'Tribe.xyz (IR)'
    AND LOWER(NULLIF(j."is_job_archived",'')) <> 'true'
    AND LOWER(NULLIF(j."test",''))            <> 'true'
    AND NULLIF(j."job_title",'') IS NOT NULL
)
SELECT
  COALESCE(NULLIF(TRIM(c."reason_not_interested"),''), '(blank)') AS "REASON",
  COUNT(DISTINCT c."candidate_id")                                AS "COUNT"
FROM "KEBOOLA_855"."out.c-reporting-v2"."candidate" c
WHERE c."job_id" IN (SELECT "job_id" FROM ir_job)
  AND NULLIF(c."reason_not_interested",'') IS NOT NULL
GROUP BY 1
ORDER BY "COUNT" DESC
