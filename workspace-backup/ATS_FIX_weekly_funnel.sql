-- PATCH: Project Dashboard - weekly funnel (Keboola config 01kpqh9r7g2z66c8vvdr5d87xd)
-- Purpose: anchor the ATS column on the "Moved to ATS" event date instead of
--          candidate_stage.date_interview, which Bubble overwrites when the
--          candidate later moves to Interview 1/2/3.
-- ONLY TWO CTEs CHANGE: ats_ev and ats_. Everything else is byte-identical to prod.
-- Prepared 2026-08-21. NOT DEPLOYED.

CREATE TABLE "project_dashboard" AS
WITH cand AS (
  SELECT
    c."candidate_id",
    c."job_id",
    TRIM(c."candidate_sourcer") AS ts,
    c."source" AS candidate_source
  FROM "KEBOOLA_855"."out.c-reporting-v2"."candidate" c
  WHERE LOWER(NULLIF(c."is_candidate_archived",'')) <> 'true'
),
job_meta AS (
  SELECT
    j."job_id",
    TRIM(cl."client_name") AS client,
    j."job_title",
    j."job_category",
    TRIM(j."job_recruiter") AS ta,
    j."is_external_recruiter" AS is_external_recruiter
  FROM "KEBOOLA_855"."out.c-reporting-v2"."job" j
  JOIN "KEBOOLA_855"."out.c-reporting-v2"."client" cl ON cl."client_id" = j."client_id"
  WHERE LOWER(NULLIF(j."test",'')) <> 'true'
    AND cl."client_name" IS NOT NULL AND cl."client_name" <> ''
    AND cl."client_name" NOT IN ('BD - Tribe','Tribe - Marketing','Kamila AI - TEST','Bubble test')
),
stage AS (
  SELECT
    cs."candidate_id",
    TRY_TO_DATE(cs."date_contacted") AS dc,
    TRY_TO_DATE(cs."date_screen") AS ds,
    TRY_TO_DATE(cs."date_screen_actual") AS dsa,
    TRY_TO_DATE(cs."date_interview") AS di,
    TRY_TO_DATE(cs."date_offer") AS doff,
    TRY_TO_DATE(cs."date_hired") AS dh
  FROM "KEBOOLA_855"."out.c-reporting-v2"."candidate_stage" cs
),
eval_ev AS (
  SELECT DISTINCT "candidate_id" FROM "KEBOOLA_855"."out.c-reporting-v2"."event"
  WHERE "event_type" = 'Evaluation'
),
-- >>> CHANGED 2026-08-21 <<<
-- WAS: SELECT DISTINCT "candidate_id" ... (membership only, no date)
-- NOW: carries the date of the candidate's FIRST Moved to ATS event.
-- MIN() is deliberate: if a candidate is re-moved to ATS later, we credit the
-- week they first entered ATS, not the re-entry.
ats_ev AS (
  SELECT
    "candidate_id",
    MIN(TRY_TO_DATE("date_created")) AS ats_date
  FROM "KEBOOLA_855"."out.c-reporting-v2"."event"
  WHERE "moved_to_stage" = 'Moved to ATS'
    AND TRY_TO_DATE("date_created") IS NOT NULL
  GROUP BY 1
),
joined AS (
  SELECT
    jm.client, jm."job_id" AS job_id, jm."job_title" AS job_title,
    COALESCE(jm."job_category",'') AS job_category,
    jm.ta, COALESCE(c.ts,'') AS ts,
    COALESCE(c.candidate_source,'') AS candidate_source,
    jm.is_external_recruiter, c."candidate_id",
    s.dc, s.ds, s.dsa, s.di, s.doff, s.dh
  FROM cand c
  JOIN job_meta jm ON jm."job_id" = c."job_id"
  JOIN stage s ON s."candidate_id" = c."candidate_id"
),
viewed AS (
  -- 2026-06-03 REVERTED: ts back to '' (job-level TA attribution). Per-sourcer
  -- split blew the row count from 26k to 112k -> data.json grew to 78MB -> exceeded
  -- Cloudflare Pages' 25MB asset limit, broke deploys. Sourcer-attributed viewed
  -- to be re-added as a separate smaller aggregate (sourcer x week x viewed).
  SELECT
    jm.client AS client,
    jm."job_id" AS job_id,
    jm."job_title" AS job_title,
    COALESCE(jm."job_category",'') AS job_category,
    jm.ta AS ta,
    '' AS ts,
    '' AS candidate_source,
    jm.is_external_recruiter AS is_external_recruiter,
    YEAROFWEEKISO(TRY_TO_DATE(e."date_created")) AS iso_year,
    WEEKISO(TRY_TO_DATE(e."date_created")) AS iso_week,
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
reacted AS (
  -- 2026-06-03: PBI Candidate Response measure. Validated EXACT vs PBI exports (2026-05-27 locked recipe).
  -- Counted by date_contacted; same attribution as contacted (candidate.candidate_sourcer).
  SELECT j.client, j.job_id, j.job_title, j.job_category, j.ta, j.ts, j.candidate_source, j.is_external_recruiter,
         YEAROFWEEKISO(j.dc) AS iso_year, WEEKISO(j.dc) AS iso_week,
         COUNT(DISTINCT j."candidate_id") AS reacted
  FROM joined j
  JOIN "KEBOOLA_855"."out.c-reporting-v2"."candidate" c2 ON c2."candidate_id" = j."candidate_id"
  LEFT JOIN "KEBOOLA_855"."out.c-reporting-v2"."candidate_stage" cs2 ON cs2."candidate_id" = j."candidate_id"
  WHERE j.dc IS NOT NULL AND YEAROFWEEKISO(j.dc) >= 2025
    AND (
      (TRIM(c2."reason_not_interested") IS NOT NULL
        AND TRIM(c2."reason_not_interested") <> ''
        AND TRIM(c2."reason_not_interested") <> 'Unresponsive')
      OR LOWER(NULLIF(c2."is_candidate_reacted",'')) = 'true'
      OR TRIM(COALESCE(cs2."stage_current",'')) NOT IN ('Contacted','Applied','Prospects','')
    )
  GROUP BY 1,2,3,4,5,6,7,8,9,10
),
actual_screens AS (
  SELECT client, job_id, job_title, job_category, ta, ts, candidate_source, is_external_recruiter,
         YEAROFWEEKISO(dsa) AS iso_year, WEEKISO(dsa) AS iso_week,
         COUNT(DISTINCT "candidate_id") AS actual_screens
  FROM joined WHERE dsa IS NOT NULL AND YEAROFWEEKISO(dsa) >= 2025
    AND "candidate_id" IN (SELECT "candidate_id" FROM eval_ev)
  GROUP BY 1,2,3,4,5,6,7,8,9,10
),
-- >>> CHANGED 2026-08-21 <<<
-- WAS: grouped by WEEKISO(di) where di = candidate_stage.date_interview, with
--      membership test "candidate_id IN (SELECT candidate_id FROM ats_ev)".
--      date_interview is overwritten by a later Interview 1/2/3 move, which
--      dragged the ATS credit into the wrong week, and was NULL for candidates
--      who never got an interview date, which dropped them entirely.
-- NOW: grouped by the ATS event date itself. Matches how pos_resp and viewed
--      already anchor on event."date_created".
ats_ AS (
  SELECT j.client, j.job_id, j.job_title, j.job_category, j.ta, j.ts, j.candidate_source, j.is_external_recruiter,
         YEAROFWEEKISO(a.ats_date) AS iso_year, WEEKISO(a.ats_date) AS iso_week,
         COUNT(DISTINCT j."candidate_id") AS ats
  FROM joined j
  JOIN ats_ev a ON a."candidate_id" = j."candidate_id"
  WHERE YEAROFWEEKISO(a.ats_date) >= 2025
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
    jm.client, jm."job_id" AS job_id, jm."job_title" AS job_title,
    COALESCE(jm."job_category",'') AS job_category,
    jm.ta, COALESCE(c.ts,'') AS ts,
    COALESCE(c.candidate_source,'') AS candidate_source,
    jm.is_external_recruiter,
    YEAROFWEEKISO(TRY_TO_DATE(e."date_created")) AS iso_year,
    WEEKISO(TRY_TO_DATE(e."date_created")) AS iso_week,
    COUNT(DISTINCT e."candidate_id") AS positive_response
  FROM "KEBOOLA_855"."out.c-reporting-v2"."event" e
  JOIN cand c ON c."candidate_id" = e."candidate_id"
  JOIN job_meta jm ON jm."job_id" = c."job_id"
  WHERE e."event_type" = 'Moved to stage'
    AND e."moved_to_stageType" = 'Positive Response'
    AND TRY_TO_DATE(e."date_created") IS NOT NULL
    AND YEAROFWEEKISO(TRY_TO_DATE(e."date_created")) >= 2025
  GROUP BY 1,2,3,4,5,6,7,8,9,10
),
keys AS (
  SELECT client, job_id, job_title, job_category, ta, ts, candidate_source, is_external_recruiter, iso_year, iso_week FROM viewed
  UNION SELECT client, job_id, job_title, job_category, ta, ts, candidate_source, is_external_recruiter, iso_year, iso_week FROM contacted
  UNION SELECT client, job_id, job_title, job_category, ta, ts, candidate_source, is_external_recruiter, iso_year, iso_week FROM reacted
  UNION SELECT client, job_id, job_title, job_category, ta, ts, candidate_source, is_external_recruiter, iso_year, iso_week FROM screens
  UNION SELECT client, job_id, job_title, job_category, ta, ts, candidate_source, is_external_recruiter, iso_year, iso_week FROM actual_screens
  UNION SELECT client, job_id, job_title, job_category, ta, ts, candidate_source, is_external_recruiter, iso_year, iso_week FROM ats_
  UNION SELECT client, job_id, job_title, job_category, ta, ts, candidate_source, is_external_recruiter, iso_year, iso_week FROM offers
  UNION SELECT client, job_id, job_title, job_category, ta, ts, candidate_source, is_external_recruiter, iso_year, iso_week FROM hires
  UNION SELECT client, job_id, job_title, job_category, ta, ts, candidate_source, is_external_recruiter, iso_year, iso_week FROM pos_resp
)
SELECT
  k.client AS "CLIENT",
  k.job_id AS "JOB_ID",
  k.job_title AS "JOB_TITLE",
  k.job_category AS "JOB_CATEGORY",
  k.ta AS "TA",
  k.ts AS "TS",
  k.candidate_source AS "CANDIDATE_SOURCE",
  k.is_external_recruiter AS "IS_EXTERNAL_RECRUITER",
  k.iso_year AS "ISO_YEAR",
  k.iso_week AS "ISO_WEEK",
  COALESCE(v.viewed, 0) AS "VIEWED",
  COALESCE(c.contacted, 0) AS "CONTACTED",
  COALESCE(re.reacted, 0) AS "REACTED",
  COALESCE(p.positive_response, 0) AS "POSITIVE_RESPONSE",
  COALESCE(sc.screens, 0) AS "SCREENS",
  COALESCE(a.actual_screens, 0) AS "ACTUAL_SCREENS",
  COALESCE(t.ats, 0) AS "ATS",
  COALESCE(o.offered, 0) AS "OFFERED",
  COALESCE(h.hired, 0) AS "HIRED"
FROM keys k
LEFT JOIN viewed v ON v.client=k.client AND v.job_id=k.job_id AND v.ta=k.ta AND v.ts=k.ts AND v.candidate_source=k.candidate_source AND v.iso_year=k.iso_year AND v.iso_week=k.iso_week
LEFT JOIN contacted c ON c.client=k.client AND c.job_id=k.job_id AND c.ta=k.ta AND c.ts=k.ts AND c.candidate_source=k.candidate_source AND c.iso_year=k.iso_year AND c.iso_week=k.iso_week
LEFT JOIN reacted re ON re.client=k.client AND re.job_id=k.job_id AND re.ta=k.ta AND re.ts=k.ts AND re.candidate_source=k.candidate_source AND re.iso_year=k.iso_year AND re.iso_week=k.iso_week
LEFT JOIN screens sc ON sc.client=k.client AND sc.job_id=k.job_id AND sc.ta=k.ta AND sc.ts=k.ts AND sc.candidate_source=k.candidate_source AND sc.iso_year=k.iso_year AND sc.iso_week=k.iso_week
LEFT JOIN actual_screens a ON a.client=k.client AND a.job_id=k.job_id AND a.ta=k.ta AND a.ts=k.ts AND a.candidate_source=k.candidate_source AND a.iso_year=k.iso_year AND a.iso_week=k.iso_week
LEFT JOIN ats_ t ON t.client=k.client AND t.job_id=k.job_id AND t.ta=k.ta AND t.ts=k.ts AND t.candidate_source=k.candidate_source AND t.iso_year=k.iso_year AND t.iso_week=k.iso_week
LEFT JOIN offers o ON o.client=k.client AND o.job_id=k.job_id AND o.ta=k.ta AND o.ts=k.ts AND o.candidate_source=k.candidate_source AND o.iso_year=k.iso_year AND o.iso_week=k.iso_week
LEFT JOIN hires h ON h.client=k.client AND h.job_id=k.job_id AND h.ta=k.ta AND h.ts=k.ts AND h.candidate_source=k.candidate_source AND h.iso_year=k.iso_year AND h.iso_week=k.iso_week
LEFT JOIN pos_resp p ON p.client=k.client AND p.job_id=k.job_id AND p.ta=k.ta AND p.ts=k.ts AND p.candidate_source=k.candidate_source AND p.iso_year=k.iso_year AND p.iso_week=k.iso_week
ORDER BY k.client, k.job_id, k.iso_year, k.iso_week
