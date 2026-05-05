-- ir_interviewed_jobweek.sql — per-(job_id, TA, ISO week) Actual Screens for Tribe.xyz (IR)
--
-- Output: JOB_ID, TA, ISO_YEAR, ISO_WEEK, ACTUAL_SCREENS

WITH ir_job AS (
  SELECT j."job_id" FROM "KEBOOLA_855"."out.c-reporting-v2"."job" j
  JOIN "KEBOOLA_855"."out.c-reporting-v2"."client" cl ON cl."client_id" = j."client_id"
  WHERE (cl."client_name" = 'Tribe.xyz (IR)' OR j."job_id" IN ('1761826848687x384161750920724500'))
    AND LOWER(NULLIF(j."test",'')) <> 'true'
    AND NULLIF(j."job_title",'') IS NOT NULL
),
cand_job AS (SELECT c."candidate_id", c."job_id" FROM "KEBOOLA_855"."out.c-reporting-v2"."candidate" c WHERE c."job_id" IN (SELECT "job_id" FROM ir_job)),
eval AS (
  SELECT e."candidate_id", NULLIF(TRIM(e."who_created_event"),'') AS ta
  FROM "KEBOOLA_855"."out.c-reporting-v2"."event" e
  WHERE e."job_id" IN (SELECT "job_id" FROM ir_job) AND e."event_type" = 'Evaluation'
),
ir_actual AS (
  SELECT cj."job_id", cs."candidate_id", TRY_TO_DATE(cs."date_screen_actual") dsa
  FROM "KEBOOLA_855"."out.c-reporting-v2"."candidate_stage" cs
  JOIN cand_job cj ON cj."candidate_id" = cs."candidate_id"
  WHERE TRY_TO_DATE(cs."date_screen_actual") IS NOT NULL
)
SELECT
  s."job_id" "JOB_ID",
  COALESCE(e.ta, '(unattributed)') "TA",
  YEAROFWEEKISO(s.dsa) "ISO_YEAR",
  WEEKISO(s.dsa) "ISO_WEEK",
  COUNT(DISTINCT s."candidate_id") "ACTUAL_SCREENS"
FROM ir_actual s LEFT JOIN eval e ON e."candidate_id" = s."candidate_id"
WHERE YEAROFWEEKISO(s.dsa) = 2026
GROUP BY 1,2,3,4
ORDER BY 1,2,3,4
