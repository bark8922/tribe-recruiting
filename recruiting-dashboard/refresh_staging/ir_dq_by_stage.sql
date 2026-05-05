-- ir_dq_by_stage.sql — per-job DQ counts at each stage for Tribe.xyz (IR)
--
-- Output schema:
--   JOB_TITLE, JOB_ID, STAGE_CONTACTED, STAGE_REC_SCREEN, STAGE_ACTUAL_SCREEN,
--   STAGE_MOVE_TO_ATS, STAGE_ONSITE, STAGE_OFFER, TOTAL
--
-- Per Andy's PBI "# of Candidates Disqualified at Each Stage" visual:
--   Filters: candidate.reason_not_interested <> blank
--            candidate_stage.Current Stage IN (Contacted, Recruiter Screen,
--                                              Actual Screen, Move to ATS,
--                                              Onsite, Offer)
--   Grouping: by job_title × Current Stage, COUNT(candidate_id)
--
-- "Current Stage" derivation (from Andy's M query):
--   stage_current_type='Contacted'                                  → 'Contacted'
--   stage_current_type='Recruiter Screen' AND date_screen_actual IS NULL → 'Recruiter Screen'
--   stage_current_type='Recruiter Screen' AND date_screen_actual NOT NULL → 'Actual Screen'
--   stage_current_type='Offsite'                                    → 'Move to ATS'
--   stage_current = 'Onsite'                                        → 'Onsite'
--   else stage_current_type
--
-- Andy's snapshot (2026-04-24, total 294 across 5 jobs):
--   DH ExecSearch 130, TS German 60, Tech TS 50, TAP Internal 31, Tech TAP APAC 23

WITH ir_job AS (
  SELECT j."job_id", j."job_title"
  FROM "KEBOOLA_855"."out.c-reporting-v2"."job"    j
  JOIN "KEBOOLA_855"."out.c-reporting-v2"."client" cl ON cl."client_id" = j."client_id"
  WHERE (cl."client_name" = 'Tribe.xyz (IR)' OR j."job_id" IN ('1761826848687x384161750920724500'))
    AND LOWER(NULLIF(j."is_job_archived",'')) <> 'true'
    AND LOWER(NULLIF(j."test",''))            <> 'true'
    AND NULLIF(j."job_title",'') IS NOT NULL
),
dq_cand AS (
  SELECT
    cs."candidate_id",
    c."job_id",
    CASE
      WHEN cs."stage_current_type" = 'Contacted'        THEN 'Contacted'
      WHEN cs."stage_current_type" = 'Recruiter Screen'
           AND TRY_TO_DATE(cs."date_screen_actual") IS NULL THEN 'Recruiter Screen'
      WHEN cs."stage_current_type" = 'Recruiter Screen'
           AND TRY_TO_DATE(cs."date_screen_actual") IS NOT NULL THEN 'Actual Screen'
      WHEN cs."stage_current_type" = 'Offsite'          THEN 'Move to ATS'
      WHEN cs."stage_current"      = 'Onsite'           THEN 'Onsite'
      WHEN cs."stage_current_type" = 'Offer'            THEN 'Offer'
      ELSE cs."stage_current_type"
    END AS current_stage
  FROM "KEBOOLA_855"."out.c-reporting-v2"."candidate_stage" cs
  JOIN "KEBOOLA_855"."out.c-reporting-v2"."candidate"       c ON c."candidate_id" = cs."candidate_id"
  WHERE c."job_id" IN (SELECT "job_id" FROM ir_job)
    AND NULLIF(c."reason_not_interested",'') IS NOT NULL
)
SELECT
  ij."job_title" AS "JOB_TITLE",
  ij."job_id"    AS "JOB_ID",
  COUNT(DISTINCT CASE WHEN d.current_stage = 'Contacted'        THEN d."candidate_id" END) AS "STAGE_CONTACTED",
  COUNT(DISTINCT CASE WHEN d.current_stage = 'Recruiter Screen' THEN d."candidate_id" END) AS "STAGE_REC_SCREEN",
  COUNT(DISTINCT CASE WHEN d.current_stage = 'Actual Screen'    THEN d."candidate_id" END) AS "STAGE_ACTUAL_SCREEN",
  COUNT(DISTINCT CASE WHEN d.current_stage = 'Move to ATS'      THEN d."candidate_id" END) AS "STAGE_MOVE_TO_ATS",
  COUNT(DISTINCT CASE WHEN d.current_stage = 'Onsite'           THEN d."candidate_id" END) AS "STAGE_ONSITE",
  COUNT(DISTINCT CASE WHEN d.current_stage = 'Offer'            THEN d."candidate_id" END) AS "STAGE_OFFER",
  COUNT(DISTINCT d."candidate_id") AS "TOTAL"
FROM ir_job ij
LEFT JOIN dq_cand d ON d."job_id" = ij."job_id"
GROUP BY ij."job_title", ij."job_id"
HAVING COUNT(DISTINCT d."candidate_id") > 0
ORDER BY "TOTAL" DESC
