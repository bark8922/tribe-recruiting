-- ir_interviewed_by.sql — per-TA Actual Screens conducted for Tribe.xyz (IR)
--
-- Output schema:
--   TA, ACTUAL_SCREENS
--
-- TA attribution (per Andy's PBI Interviewed By visual):
--   TA = event.who_created_event (the person who created the Evaluation event)
--   Actual Screens = COUNT(DISTINCT candidate_stage.candidate_id)
--     where date_screen_actual <> blank
--     AND candidate has an Evaluation event on an IR job
--
-- Andy's snapshot (2026-04-24, full window):
--   Chené Elliot 13, Anna Tyulpanova 5, Jovana Drakula 4, Jelena Lacmanovic 3,
--   Samantha Nel 3, Iryna Dyda 2, Sanja Pavlovikj 1, (unattributed) 18, Total 49

WITH ir_job AS (
  SELECT j."job_id"
  FROM "KEBOOLA_855"."out.c-reporting-v2"."job"    j
  JOIN "KEBOOLA_855"."out.c-reporting-v2"."client" cl ON cl."client_id" = j."client_id"
  WHERE cl."client_name" = 'Tribe.xyz (IR)'
    AND LOWER(NULLIF(j."is_job_archived",'')) <> 'true'
    AND LOWER(NULLIF(j."test",''))            <> 'true'
    AND NULLIF(j."job_title",'') IS NOT NULL
),
eval_events AS (
  SELECT
    e."candidate_id",
    NULLIF(TRIM(e."who_created_event"),'') AS ta
  FROM "KEBOOLA_855"."out.c-reporting-v2"."event" e
  WHERE e."job_id" IN (SELECT "job_id" FROM ir_job)
    AND e."event_type" = 'Evaluation'
),
ir_actual_screens AS (
  SELECT cs."candidate_id"
  FROM "KEBOOLA_855"."out.c-reporting-v2"."candidate_stage" cs
  JOIN "KEBOOLA_855"."out.c-reporting-v2"."candidate" c ON c."candidate_id" = cs."candidate_id"
  WHERE c."job_id" IN (SELECT "job_id" FROM ir_job)
    AND TRY_TO_DATE(cs."date_screen_actual") IS NOT NULL
)
SELECT
  COALESCE(e.ta, '(unattributed)') AS "TA",
  COUNT(DISTINCT s."candidate_id") AS "ACTUAL_SCREENS"
FROM ir_actual_screens s
LEFT JOIN eval_events e ON e."candidate_id" = s."candidate_id"
GROUP BY 1
ORDER BY "ACTUAL_SCREENS" DESC, "TA"
