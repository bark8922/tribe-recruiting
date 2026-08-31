-- project_dashboard.sql — per-(client, job, TA, TS, category, source, external, ISO week) funnel counts
--
-- Drives the Project Dashboard tab. Output is weekly grain so production-scale
-- CSVs fit in MCP response limits (~80-100KB for 2026 YTD). React frontend can
-- filter by any dimension (Client, TA, TS, Category, Source, Job Title,
-- External Recruiter, Period) without a re-query.
--
-- Output schema (matches snowflake_project_dashboard.csv):
--   CLIENT, JOB_ID, JOB_TITLE, JOB_CATEGORY, TA, TS, CANDIDATE_SOURCE,
--   IS_EXTERNAL_RECRUITER, ISO_YEAR, ISO_WEEK,
--   CONTACTED, POSITIVE_RESPONSE, ACTUAL_SCREENS, ATS, OFFERED, HIRED
--
-- Attribution (validated 2026-04-20 against PBIX Overview, Apr 13-19 window;
-- 24/24 per-client metrics within 1-3 units):
--   TA = TRIM(job.job_recruiter)
--   TS = TRIM(candidate.candidate_sourcer) — persists original sourcing credit
--   Source = candidate.source
--   Category = job.job_category
--   External = job.is_external_recruiter
--
-- Metric sources:
--   Contacted / Offered / Hired                        → candidate_stage.date_*
--   Screens (Recruiter Screens)                        → candidate_stage.date_screen
--   Actual Screens (2026-06-03 fix)                    → candidate_stage.date_screen_actual
--                                                        AND candidate has 'Evaluation' event
--   ATS (2026-06-03 fix)                               → candidate_stage.date_interview
--                                                        AND candidate has 'Moved to ATS' event
--   Positive Response                                  → event table
--     event_type='Moved to stage' AND moved_to_stageType='Positive Response',
--     counted on event.date_created.
--
-- Why event gates on Actual Screens + ATS (added 2026-06-03):
--   Per Blake: "Actual Screens" means the candidate actually went through evaluation,
--   not just that date_screen_actual got typed in. Mirrors ts_summary.sql gates +
--   matches PBI canon. Pre-fix PD over-counted by ~5-7% on these two metrics vs the
--   correct definition (Andrea Apr 2026: 80 → ~75 act, 35 → ~31 ats).
--
-- Filters (canonical, matches WBR/MBR):
--   candidate.is_candidate_archived <> 'true'
--   job.test <> 'true'
--   client.client_name NOT IN ('BD - Tribe','Tribe - Marketing','Kamila AI - TEST','Bubble test')
--   NOTE: Tribe.xyz and Tribe.xyz (IR) are INCLUDED here (2026-04-23 parity fix).
--   PBI Project Dashboard displays these, and our prior exclusion caused a
--   ~9-16% top-of-funnel undercount vs PBI. App.jsx gates them behind the
--   showInternal toggle so users can still filter them out per-view.
--
-- External-recruiter filter is deliberately NOT applied here. PBIX includes
-- externals at per-client / per-job grain. Applied only at "All-clients"
-- aggregate KPI card, which App.jsx handles post-aggregation.
--
-- Date window: 2025 and later (lowered from 2026 on 2026-05-20 to unblock historical lookback).

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
    j."job_category",
    TRIM(j."job_recruiter")         AS ta,
    j."is_external_recruiter"       AS is_external_recruiter
  FROM "KEBOOLA_855"."out.c-reporting-v2"."job"    j
  JOIN "KEBOOLA_855"."out.c-reporting-v2"."client" cl ON cl."client_id" = j."client_id"
  WHERE LOWER(NULLIF(j."test",'')) <> 'true'
    AND cl."client_name" IS NOT NULL
    AND cl."client_name" <> ''
    AND cl."client_name" NOT IN ('BD - Tribe','Tribe - Marketing','Kamila AI - TEST','Bubble test')
),
stage AS (
  SELECT
    cs."candidate_id",
    TRY_TO_DATE(cs."date_contacted")     AS dc,
    TRY_TO_DATE(cs."date_screen")        AS ds,
    TRY_TO_DATE(cs."date_screen_actual") AS dsa,
    TRY_TO_DATE(cs."date_interview")     AS di,
    TRY_TO_DATE(cs."date_offer")         AS doff,
    TRY_TO_DATE(cs."date_hired")         AS dh
  FROM "KEBOOLA_855"."out.c-reporting-v2"."candidate_stage" cs
),
-- Event-gate CTEs (added 2026-06-03). Mirror ts_summary.sql so Actual Screens + ATS
-- mean what their labels say (candidate went through evaluation / was moved to ATS),
-- not just "the date field got filled in."
eval_ev AS (
  SELECT DISTINCT "candidate_id"
  FROM "KEBOOLA_855"."out.c-reporting-v2"."event"
  WHERE "event_type" = 'Evaluation'
),
ats_ev AS (
  SELECT DISTINCT "candidate_id"
  FROM "KEBOOLA_855"."out.c-reporting-v2"."event"
  WHERE "moved_to_stage" = 'Moved to ATS'
),
joined AS (
  SELECT
    jm.client,
    jm."job_id"                 AS job_id,
    jm."job_title"              AS job_title,
    COALESCE(jm."job_category",'') AS job_category,
    jm.ta,
    COALESCE(c.ts,'')           AS ts,
    COALESCE(c.candidate_source,'') AS candidate_source,
    jm.is_external_recruiter,
    c."candidate_id",
    s.dc, s.ds, s.dsa, s.di, s.doff, s.dh
  FROM cand c
  JOIN job_meta jm ON jm."job_id" = c."job_id"
  JOIN stage    s  ON s."candidate_id" = c."candidate_id"
),
viewed AS (
  SELECT
    jm.client              AS client,
    jm."job_id"            AS job_id,
    jm."job_title"         AS job_title,
    COALESCE(jm."job_category",'') AS job_category,
    jm.ta                  AS ta,
    ''                     AS ts,
    ''                     AS candidate_source,
    jm.is_external_recruiter AS is_external_recruiter,
    YEAROFWEEKISO(TRY_TO_DATE(e."date_created")) AS iso_year,
    WEEKISO(TRY_TO_DATE(e."date_created"))       AS iso_week,
    COUNT(DISTINCT e."talent_id") AS viewed
  FROM "KEBOOLA_855"."out.c-reporting-v2"."event" e
  JOIN job_meta jm ON jm."job_id" = e."job_id"
  WHERE e."event_type" = 'Linkedin Visited Profile'
    AND TRY_TO_DATE(e."date_created") IS NOT NULL
    AND YEAROFWEEKISO(TRY_TO_DATE(e."date_created")) >= 2025
  GROUP BY 1,2,3,4,5,6,7,8,9,10
),
screens AS (
  SELECT client, job_id, job_title, job_category, ta, ts, candidate_source, is_external_recruiter,
         YEAROFWEEKISO(ds) AS iso_year, WEEKISO(ds) AS iso_week,
         COUNT(DISTINCT "candidate_id") AS screens
  FROM joined WHERE ds IS NOT NULL AND YEAROFWEEKISO(ds) >= 2025
  GROUP BY 1,2,3,4,5,6,7,8,9,10
),
contacted AS (
  SELECT client, job_id, job_title, job_category, ta, ts, candidate_source, is_external_recruiter,
         YEAROFWEEKISO(dc) AS iso_year, WEEKISO(dc) AS iso_week,
         COUNT(DISTINCT "candidate_id") AS contacted
  FROM joined WHERE dc IS NOT NULL AND YEAROFWEEKISO(dc) >= 2025
  GROUP BY 1,2,3,4,5,6,7,8,9,10
),
actual_screens AS (
  -- 2026-06-03: gate on Evaluation event (was: date alone). See header comment.
  SELECT client, job_id, job_title, job_category, ta, ts, candidate_source, is_external_recruiter,
         YEAROFWEEKISO(dsa) AS iso_year, WEEKISO(dsa) AS iso_week,
         COUNT(DISTINCT "candidate_id") AS actual_screens
  FROM joined
  WHERE dsa IS NOT NULL
    AND YEAROFWEEKISO(dsa) >= 2025
    AND "candidate_id" IN (SELECT "candidate_id" FROM eval_ev)
  GROUP BY 1,2,3,4,5,6,7,8,9,10
),
int1_ev AS (
  -- 2026-08-31: candidates who ever reached Interview 1 (new pipeline, from 2026-07-14).
  SELECT DISTINCT "candidate_id" FROM "KEBOOLA_855"."out.c-reporting-v2"."event"
  WHERE "moved_to_stageType" = 'Interview 1'
),
int2_ev AS (
  SELECT DISTINCT "candidate_id" FROM "KEBOOLA_855"."out.c-reporting-v2"."event"
  WHERE "moved_to_stageType" = 'Interview 2'
),
int3_ev AS (
  SELECT DISTINCT "candidate_id" FROM "KEBOOLA_855"."out.c-reporting-v2"."event"
  WHERE "moved_to_stageType" = 'Interview 3'
),
ats_ AS (
  -- 2026-06-03: gate on 'Moved to ATS' event (was: date alone). See header comment.
  SELECT client, job_id, job_title, job_category, ta, ts, candidate_source, is_external_recruiter,
         YEAROFWEEKISO(di) AS iso_year, WEEKISO(di) AS iso_week,
         COUNT(DISTINCT "candidate_id") AS ats,
         COUNT(DISTINCT CASE WHEN "candidate_id" IN (SELECT "candidate_id" FROM int1_ev) THEN "candidate_id" END) AS int1,
         COUNT(DISTINCT CASE WHEN "candidate_id" IN (SELECT "candidate_id" FROM int2_ev) THEN "candidate_id" END) AS int2,
         COUNT(DISTINCT CASE WHEN "candidate_id" IN (SELECT "candidate_id" FROM int3_ev) THEN "candidate_id" END) AS int3
  FROM joined
  WHERE di IS NOT NULL
    AND YEAROFWEEKISO(di) >= 2025
    AND "candidate_id" IN (SELECT "candidate_id" FROM ats_ev)
  GROUP BY 1,2,3,4,5,6,7,8,9,10
),
offers AS (
  SELECT client, job_id, job_title, job_category, ta, ts, candidate_source, is_external_recruiter,
         YEAROFWEEKISO(doff) AS iso_year, WEEKISO(doff) AS iso_week,
         COUNT(DISTINCT "candidate_id") AS offered
  FROM joined WHERE doff IS NOT NULL AND YEAROFWEEKISO(doff) >= 2025
  GROUP BY 1,2,3,4,5,6,7,8,9,10
),
hires AS (
  SELECT client, job_id, job_title, job_category, ta, ts, candidate_source, is_external_recruiter,
         YEAROFWEEKISO(dh) AS iso_year, WEEKISO(dh) AS iso_week,
         COUNT(DISTINCT "candidate_id") AS hired
  FROM joined WHERE dh IS NOT NULL AND YEAROFWEEKISO(dh) >= 2025
  GROUP BY 1,2,3,4,5,6,7,8,9,10
),
pos_resp AS (
  SELECT
    jm.client,
    jm."job_id"                  AS job_id,
    jm."job_title"               AS job_title,
    COALESCE(jm."job_category",'') AS job_category,
    jm.ta,
    COALESCE(c.ts,'')            AS ts,
    COALESCE(c.candidate_source,'') AS candidate_source,
    jm.is_external_recruiter,
    YEAROFWEEKISO(TRY_TO_DATE(e."date_created")) AS iso_year,
    WEEKISO(TRY_TO_DATE(e."date_created"))       AS iso_week,
    COUNT(DISTINCT e."candidate_id") AS positive_response
  FROM "KEBOOLA_855"."out.c-reporting-v2"."event" e
  JOIN cand     c  ON c."candidate_id" = e."candidate_id"
  JOIN job_meta jm ON jm."job_id"      = c."job_id"
  WHERE e."event_type"        = 'Moved to stage'
    AND e."moved_to_stageType" = 'Positive Response'
    AND TRY_TO_DATE(e."date_created") IS NOT NULL
    AND YEAROFWEEKISO(TRY_TO_DATE(e."date_created")) >= 2025
  GROUP BY 1,2,3,4,5,6,7,8,9,10
),
keys AS (
  SELECT client, job_id, job_title, job_category, ta, ts, candidate_source, is_external_recruiter, iso_year, iso_week FROM viewed
  UNION SELECT client, job_id, job_title, job_category, ta, ts, candidate_source, is_external_recruiter, iso_year, iso_week FROM contacted
  UNION SELECT client, job_id, job_title, job_category, ta, ts, candidate_source, is_external_recruiter, iso_year, iso_week FROM screens
  UNION SELECT client, job_id, job_title, job_category, ta, ts, candidate_source, is_external_recruiter, iso_year, iso_week FROM actual_screens
  UNION SELECT client, job_id, job_title, job_category, ta, ts, candidate_source, is_external_recruiter, iso_year, iso_week FROM ats_
  UNION SELECT client, job_id, job_title, job_category, ta, ts, candidate_source, is_external_recruiter, iso_year, iso_week FROM offers
  UNION SELECT client, job_id, job_title, job_category, ta, ts, candidate_source, is_external_recruiter, iso_year, iso_week FROM hires
  UNION SELECT client, job_id, job_title, job_category, ta, ts, candidate_source, is_external_recruiter, iso_year, iso_week FROM pos_resp
)
SELECT
  k.client                AS "CLIENT",
  k.job_id                AS "JOB_ID",
  k.job_title             AS "JOB_TITLE",
  k.job_category          AS "JOB_CATEGORY",
  k.ta                    AS "TA",
  k.ts                    AS "TS",
  k.candidate_source      AS "CANDIDATE_SOURCE",
  k.is_external_recruiter AS "IS_EXTERNAL_RECRUITER",
  k.iso_year              AS "ISO_YEAR",
  k.iso_week              AS "ISO_WEEK",
  COALESCE(v.viewed,             0) AS "VIEWED",
  COALESCE(c.contacted,          0) AS "CONTACTED",
  COALESCE(p.positive_response,  0) AS "POSITIVE_RESPONSE",
  COALESCE(sc.screens,           0) AS "SCREENS",
  COALESCE(a.actual_screens,     0) AS "ACTUAL_SCREENS",
  COALESCE(t.ats,                0) AS "ATS",
  COALESCE(t.int1,               0) AS "INT1",
  COALESCE(t.int2,               0) AS "INT2",
  COALESCE(t.int3,               0) AS "INT3",
  COALESCE(o.offered,            0) AS "OFFERED",