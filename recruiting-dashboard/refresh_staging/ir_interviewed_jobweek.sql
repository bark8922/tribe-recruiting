-- ir_interviewed_jobweek.sql — per-(job_id, TA, ISO week) Actual Screens for Tribe.xyz (IR)
--
-- Output: JOB_ID, TA, ISO_YEAR, ISO_WEEK, ACTUAL_SCREENS
--
-- Attribution: Evaluation event's `who_event_created_for` (canonical TA field
-- used by mbr_contacted_ev / aux_12w / ts_summary). Dedup by latest event.
-- If no Evaluation event with `who_event_created_for` exists for a candidate
-- whose date_screen_actual is populated, fall back to `job.job_recruiter`.
-- Previously this query used `who_created_event` (wrong field — the user who
-- CREATED the event, not the user the event was FOR), which produced a long
-- tail of "(unattributed)" rows for the Tech Talent Sourcer job.

WITH ir_job AS (
  SELECT j."job_id", TRIM(j."job_recruiter") AS job_recruiter
  FROM "KEBOOLA_855"."out.c-reporting-v2"."job" j
  JOIN "KEBOOLA_855"."out.c-reporting-v2"."client" cl ON cl."client_id" = j."client_id"
  WHERE (cl."client_name" = 'Tribe.xyz (IR)' OR j."job_id" IN ('1761826848687x384161750920724500'))
    AND LOWER(NULLIF(j."test",'')) <> 'true'
    AND NULLIF(j."job_title",'') IS NOT NULL
),
cand_job AS (
  SELECT c."candidate_id", c."job_id"
  FROM "KEBOOLA_855"."out.c-reporting-v2"."candidate" c
  WHERE c."job_id" IN (SELECT "job_id" FROM ir_job)
),
ir_actual AS (
  SELECT cj."job_id", cs."candidate_id", TRY_TO_DATE(cs."date_screen_actual") dsa
  FROM "KEBOOLA_855"."out.c-reporting-v2"."candidate_stage" cs
  JOIN cand_job cj ON cj."candidate_id" = cs."candidate_id"
  WHERE TRY_TO_DATE(cs."date_screen_actual") IS NOT NULL
),
eval_ranked AS (
  -- Latest Evaluation event per candidate with non-empty who_event_created_for.
  -- Matches aux_12w.sql / mbr_contacted_ev attribution rule.
  SELECT
    e."candidate_id",
    TRIM(e."who_event_created_for") AS ta,
    ROW_NUMBER() OVER (PARTITION BY e."candidate_id" ORDER BY e."datetime_created" DESC) AS rn
  FROM "KEBOOLA_855"."out.c-reporting-v2"."event" e
  WHERE e."job_id" IN (SELECT "job_id" FROM ir_job)
    AND e."event_type" = 'Evaluation'
    AND NULLIF(TRIM(e."who_event_created_for"),'') IS NOT NULL
)
SELECT
  s."job_id" "JOB_ID",
  -- 1) Evaluation event TA, 2) job.job_recruiter, 3) (unattributed)
  COALESCE(NULLIF(e.ta,''), NULLIF(j.job_recruiter,''), '(unattributed)') "TA",
  YEAROFWEEKISO(s.dsa) "ISO_YEAR",
  WEEKISO(s.dsa) "ISO_WEEK",
  COUNT(DISTINCT s."candidate_id") "ACTUAL_SCREENS"
FROM ir_actual s
LEFT JOIN eval_ranked e ON e."candidate_id" = s."candidate_id" AND e.rn = 1
LEFT JOIN ir_job j ON j."job_id" = s."job_id"
WHERE YEAROFWEEKISO(s.dsa) = 2026
GROUP BY 1,2,3,4
ORDER BY 1,2,3,4
