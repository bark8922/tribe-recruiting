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
--   Contacted / Actual Screens / ATS / Offered / Hired → candidate_stage.date_*
--   Positive Response                                  → event table
--     event_type='Moved to stage' AND moved_to_stageType='Positive Response',
--     counted on event.date_created.
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
  SELECT client, job_id, job_title, job_category, ta, ts, candidate_source, is_external_recruiter,
         YEAROFWEEKISO(dsa) AS iso_year, WEEKISO(dsa) AS iso_week,
         COUNT(DISTINCT "candidate_id") AS actual_screens
  FROM joined WHERE dsa IS NOT NULL AND YEAROFWEEKISO(dsa) >= 2025
  GROUP BY 1,2,3,4,5,6,7,8,9,10
),
ats_ AS (
  SELECT client, job_id, job_title, job_category, ta, ts, candidate_source, is_external_recruiter,
         YEAROFWEEKISO(di) AS iso_year, WEEKISO(di) AS iso_week,
         COUNT(DISTINCT "candidate_id") AS ats
  FROM joined WHERE di IS NOT NULL AND YEAROFWEEKISO(di) >= 2025
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
  COALESCE(o.offered,            0) AS "OFFERED",
  COALESCE(h.hired,              0) AS "HIRED"
FROM keys k
LEFT JOIN viewed         v ON v.client=k.client AND v.job_id=k.job_id AND v.ta=k.ta AND v.ts=k.ts AND v.candidate_source=k.candidate_source AND v.iso_year=k.iso_year AND v.iso_week=k.iso_week
LEFT JOIN contacted      c ON c.client=k.client AND c.job_id=k.job_id AND c.ta=k.ta AND c.ts=k.ts AND c.candidate_source=k.candidate_source AND c.iso_year=k.iso_year AND c.iso_week=k.iso_week
LEFT JOIN screens       sc ON sc.client=k.client AND sc.job_id=k.job_id AND sc.ta=k.ta AND sc.ts=k.ts AND sc.candidate_source=k.candidate_source AND sc.iso_year=k.iso_year AND sc.iso_week=k.iso_week
LEFT JOIN actual_screens a ON a.client=k.client AND a.job_id=k.job_id AND a.ta=k.ta AND a.ts=k.ts AND a.candidate_source=k.candidate_source AND a.iso_year=k.iso_year AND a.iso_week=k.iso_week
LEFT JOIN ats_           t ON t.client=k.client AND t.job_id=k.job_id AND t.ta=k.ta AND t.ts=k.ts AND t.candidate_source=k.candidate_source AND t.iso_year=k.iso_year AND t.iso_week=k.iso_week
LEFT JOIN offers         o ON o.client=k.client AND o.job_id=k.job_id AND o.ta=k.ta AND o.ts=k.ts AND o.candidate_source=k.candidate_source AND o.iso_year=k.iso_year AND o.iso_week=k.iso_week
LEFT JOIN hires          h ON h.client=k.client AND h.job_id=k.job_id AND h.ta=k.ta AND h.ts=k.ts AND h.candidate_source=k.candidate_source AND h.iso_year=k.iso_year AND h.iso_week=k.iso_week
LEFT JOIN pos_resp       p ON p.client=k.client AND p.job_id=k.job_id AND p.ta=k.ta AND p.ts=k.ts AND p.candidate_source=k.candidate_source AND p.iso_year=k.iso_year AND p.iso_week=k.iso_week
ORDER BY k.client, k.job_id, k.iso_year, k.iso_week
