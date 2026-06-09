-- new_project_health.sql — per-role health for roles opened in the last ~45 days.
--
-- Drives the gated "New Project Health" tab (Jacopo OKR KR2 + KR3). Frontend
-- filters to roles open <=30 days; we pull 45 here so a role doesn't pop in/out
-- right at the boundary and so days_open can be recomputed client-side daily.
--
-- Output schema (matches snowflake_new_project_health.csv):
--   JOB_ID, CLIENT, JOB_TITLE, TA, IS_EXTERNAL_RECRUITER, DATE_CREATED,
--   DATE_FIRST_ATS, W4_ACTUAL_SCREENS, W4_ATS
--
-- Metric definitions (locked 2026-06-09):
--   KR2 Days to first ATS = DATEDIFF(date_created, DATE_FIRST_ATS), computed
--       client-side. Baseline is role creation date (Jacopo's call).
--   KR3 Actual Screen -> ATS conversion = W4_ATS / W4_ACTUAL_SCREENS, cumulative
--       over the role's first 4 weeks (date_created .. date_created+28d).
--
-- House conventions (match project_dashboard.sql, 2026-06-03 event-gate fix):
--   ATS            = candidate has 'Moved to ATS' event; dated by candidate_stage.date_interview
--   Actual Screen  = candidate has 'Evaluation' event;  dated by candidate_stage.date_screen_actual
--   Filters: job.test<>true, candidate not archived, job not archived,
--            client NOT IN ('BD - Tribe','Tribe - Marketing','Kamila AI - TEST','Bubble test').
--   Tribe.xyz / Tribe.xyz (IR) are INCLUDED (internal-unfiltered convention).
--
-- Conversion can exceed 100%: a candidate can reach ATS via a route that never
-- recorded an in-window actual screen (applicant/referral, or screen outside the
-- 4-week window). That is a real funnel property; the frontend displays it as-is.

WITH job_meta AS (
  SELECT j."job_id" AS jid, TRIM(cl."client_name") AS client, j."job_title" AS title,
         TRIM(j."job_recruiter") AS ta, j."is_external_recruiter" AS ext,
         TRY_TO_DATE(j."date_created") AS dcreated
  FROM "KEBOOLA_855"."out.c-reporting-v2"."job" j
  JOIN "KEBOOLA_855"."out.c-reporting-v2"."client" cl ON cl."client_id" = j."client_id"
  WHERE LOWER(NULLIF(j."test",'')) <> 'true'
    AND cl."client_name" IS NOT NULL AND cl."client_name" <> ''
    AND cl."client_name" NOT IN ('BD - Tribe','Tribe - Marketing','Kamila AI - TEST','Bubble test')
    AND LOWER(NULLIF(j."is_job_archived",'')) <> 'true'
    AND TRY_TO_DATE(j."date_created") >= DATEADD('day', -45, CURRENT_DATE())
),
cand AS (
  SELECT c."candidate_id" AS cid, c."job_id" AS jid
  FROM "KEBOOLA_855"."out.c-reporting-v2"."candidate" c
  WHERE LOWER(NULLIF(c."is_candidate_archived",'')) <> 'true'
),
stage AS (
  SELECT cs."candidate_id" AS cid,
         TRY_TO_DATE(cs."date_screen_actual") AS dsa,
         TRY_TO_DATE(cs."date_interview") AS di
  FROM "KEBOOLA_855"."out.c-reporting-v2"."candidate_stage" cs
),
eval_ev AS (SELECT DISTINCT "candidate_id" AS cid FROM "KEBOOLA_855"."out.c-reporting-v2"."event" WHERE "event_type" = 'Evaluation'),
ats_ev  AS (SELECT DISTINCT "candidate_id" AS cid FROM "KEBOOLA_855"."out.c-reporting-v2"."event" WHERE "moved_to_stage" = 'Moved to ATS'),
cj AS (
  SELECT jm.jid, jm.client, jm.title, jm.ta, jm.ext, jm.dcreated, c.cid, s.dsa, s.di
  FROM job_meta jm JOIN cand c ON c.jid = jm.jid JOIN stage s ON s.cid = c.cid
)
SELECT cj.jid AS "JOB_ID", cj.client AS "CLIENT", cj.title AS "JOB_TITLE", cj.ta AS "TA",
  cj.ext AS "IS_EXTERNAL_RECRUITER", TO_VARCHAR(cj.dcreated) AS "DATE_CREATED",
  TO_VARCHAR(MIN(CASE WHEN cj.di IS NOT NULL AND cj.cid IN (SELECT cid FROM ats_ev) THEN cj.di END)) AS "DATE_FIRST_ATS",
  COUNT(DISTINCT CASE WHEN cj.dsa BETWEEN cj.dcreated AND DATEADD('day',28,cj.dcreated) AND cj.cid IN (SELECT cid FROM eval_ev) THEN cj.cid END) AS "W4_ACTUAL_SCREENS",
  COUNT(DISTINCT CASE WHEN cj.di  BETWEEN cj.dcreated AND DATEADD('day',28,cj.dcreated) AND cj.cid IN (SELECT cid FROM ats_ev)  THEN cj.cid END) AS "W4_ATS"
FROM cj
GROUP BY 1,2,3,4,5,6
ORDER BY 2 ASC, 6 DESC
