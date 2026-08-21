-- PATCH: Project Dashboard - event-attr (Keboola config 01ks4qf6zate4m7f0cxng2hnyy)
-- Purpose: anchor the ATS column on the "Moved to ATS" event date instead of
--          candidate_stage.date_interview.
-- THREE EDITS: ev_attr (did_ats flag -> ats_date), base (carry ats_date), ats_ (group by ats_date).
-- Note: because this transform attributes per (candidate, TA), the ATS date is
--       taken per (candidate, TA) too, so a handover credits the TA who actually
--       did the ATS move. Everything else is byte-identical to prod.
-- Prepared 2026-08-21. NOT DEPLOYED.

CREATE TABLE "project_dashboard_eventattr" AS
WITH cand AS (
  SELECT c."candidate_id", c."job_id", TRIM(c."candidate_sourcer") AS ts, c."source" AS candidate_source
  FROM "KEBOOLA_855"."out.c-reporting-v2"."candidate" c
  WHERE LOWER(NULLIF(c."is_candidate_archived",'')) <> 'true'
),
job_meta AS (
  SELECT j."job_id", TRIM(cl."client_name") AS client, j."job_title", j."job_category", j."is_external_recruiter" AS is_external_recruiter
  FROM "KEBOOLA_855"."out.c-reporting-v2"."job" j
  JOIN "KEBOOLA_855"."out.c-reporting-v2"."client" cl ON cl."client_id" = j."client_id"
  WHERE LOWER(NULLIF(j."test",'')) <> 'true' AND cl."client_name" IS NOT NULL AND cl."client_name" <> ''
    AND cl."client_name" NOT IN ('BD - Tribe','Tribe - Marketing','Kamila AI - TEST','Bubble test')
),
stage AS (
  SELECT cs."candidate_id", TRY_TO_DATE(cs."date_contacted") AS dc, TRY_TO_DATE(cs."date_screen") AS ds,
    TRY_TO_DATE(cs."date_screen_actual") AS dsa, TRY_TO_DATE(cs."date_interview") AS di,
    TRY_TO_DATE(cs."date_offer") AS doff, TRY_TO_DATE(cs."date_hired") AS dh
  FROM "KEBOOLA_855"."out.c-reporting-v2"."candidate_stage" cs
),
ev_attr AS (
  SELECT e."candidate_id", TRIM(e."who_event_created_for") AS ta,
    MAX(CASE WHEN e."moved_to_stageType"='Contacted' AND e."moved_to_stage" <> 'Responded' THEN 1 ELSE 0 END) AS did_contacted,
    MAX(CASE WHEN e."moved_to_stageType"='Recruiter Screen' THEN 1 ELSE 0 END) AS did_screen,
    MAX(CASE WHEN e."event_type"='Evaluation' THEN 1 ELSE 0 END) AS did_eval,
    -- >>> CHANGED 2026-08-21 <<<
    -- WAS: MAX(CASE WHEN e."moved_to_stage"='Moved to ATS' THEN 1 ELSE 0 END) AS did_ats
    --      (a bare yes/no flag, so the ATS week had to come from date_interview)
    -- NOW: the actual date of this TA's first Moved to ATS event for this candidate.
    --      NULL means this TA never moved this candidate to ATS, which replaces
    --      the old did_ats = 0 test.
    MIN(CASE WHEN e."moved_to_stage"='Moved to ATS' THEN TRY_TO_DATE(e."date_created") END) AS ats_date,
    MAX(CASE WHEN e."moved_to_stageType"='Offer' THEN 1 ELSE 0 END) AS did_offer,
    MAX(CASE WHEN e."moved_to_stage"='Hired' THEN 1 ELSE 0 END) AS did_hired
  FROM "KEBOOLA_855"."out.c-reporting-v2"."event" e
  WHERE TRIM(e."who_event_created_for") IS NOT NULL AND TRIM(e."who_event_created_for") <> ''
  GROUP BY e."candidate_id", TRIM(e."who_event_created_for")
),
base AS (
  SELECT jm.client, jm."job_id" AS job_id, jm."job_title" AS job_title, COALESCE(jm."job_category",'') AS job_category,
    ea.ta, COALESCE(c.ts,'') AS ts, COALESCE(c.candidate_source,'') AS candidate_source, jm.is_external_recruiter,
    c."candidate_id", s.dc, s.ds, s.dsa, s.di, s.doff, s.dh,
    -- >>> CHANGED 2026-08-21 <<< carries ea.ats_date in place of ea.did_ats
    ea.did_contacted, ea.did_screen, ea.did_eval, ea.ats_date, ea.did_offer, ea.did_hired
  FROM cand c
  JOIN job_meta jm ON jm."job_id" = c."job_id"
  JOIN stage s ON s."candidate_id" = c."candidate_id"
  JOIN ev_attr ea ON ea."candidate_id" = c."candidate_id"
),
contacted AS (SELECT client, job_id, job_title, job_category, ta, ts, candidate_source, is_external_recruiter, YEAROFWEEKISO(dc) AS iso_year, WEEKISO(dc) AS iso_week, COUNT(DISTINCT "candidate_id") AS contacted FROM base WHERE dc IS NOT NULL AND did_contacted = 1 AND YEAROFWEEKISO(dc) >= 2025 GROUP BY 1,2,3,4,5,6,7,8,9,10),
screens AS (SELECT client, job_id, job_title, job_category, ta, ts, candidate_source, is_external_recruiter, YEAROFWEEKISO(ds) AS iso_year, WEEKISO(ds) AS iso_week, COUNT(DISTINCT "candidate_id") AS screens FROM base WHERE ds IS NOT NULL AND did_screen = 1 AND YEAROFWEEKISO(ds) >= 2025 GROUP BY 1,2,3,4,5,6,7,8,9,10),
actual_screens AS (SELECT client, job_id, job_title, job_category, ta, ts, candidate_source, is_external_recruiter, YEAROFWEEKISO(dsa) AS iso_year, WEEKISO(dsa) AS iso_week, COUNT(DISTINCT "candidate_id") AS actual_screens FROM base WHERE dsa IS NOT NULL AND did_eval = 1 AND YEAROFWEEKISO(dsa) >= 2025 GROUP BY 1,2,3,4,5,6,7,8,9,10),
-- >>> CHANGED 2026-08-21 <<<
-- WAS: YEAROFWEEKISO(di) / WEEKISO(di) ... WHERE di IS NOT NULL AND did_ats = 1
-- NOW: bucketed by the ATS event date. "ats_date IS NOT NULL" is exactly the old
--      did_ats = 1 test, so no candidate is added or removed by the filter change,
--      only the week they land in, plus the ones date_interview used to lose.
ats_ AS (SELECT client, job_id, job_title, job_category, ta, ts, candidate_source, is_external_recruiter, YEAROFWEEKISO(ats_date) AS iso_year, WEEKISO(ats_date) AS iso_week, COUNT(DISTINCT "candidate_id") AS ats FROM base WHERE ats_date IS NOT NULL AND YEAROFWEEKISO(ats_date) >= 2025 GROUP BY 1,2,3,4,5,6,7,8,9,10),
offers AS (SELECT client, job_id, job_title, job_category, ta, ts, candidate_source, is_external_recruiter, YEAROFWEEKISO(doff) AS iso_year, WEEKISO(doff) AS iso_week, COUNT(DISTINCT "candidate_id") AS offered FROM base WHERE doff IS NOT NULL AND did_offer = 1 AND YEAROFWEEKISO(doff) >= 2025 GROUP BY 1,2,3,4,5,6,7,8,9,10),
hires AS (SELECT client, job_id, job_title, job_category, ta, ts, candidate_source, is_external_recruiter, YEAROFWEEKISO(dh) AS iso_year, WEEKISO(dh) AS iso_week, COUNT(DISTINCT "candidate_id") AS hired FROM base WHERE dh IS NOT NULL AND did_hired = 1 AND YEAROFWEEKISO(dh) >= 2025 GROUP BY 1,2,3,4,5,6,7,8,9,10),
pos_resp AS (SELECT jm.client, jm."job_id" AS job_id, jm."job_title" AS job_title, COALESCE(jm."job_category",'') AS job_category, TRIM(e."who_event_created_for") AS ta, COALESCE(c.ts,'') AS ts, COALESCE(c.candidate_source,'') AS candidate_source, jm.is_external_recruiter, YEAROFWEEKISO(TRY_TO_DATE(e."date_created")) AS iso_year, WEEKISO(TRY_TO_DATE(e."date_created")) AS iso_week, COUNT(DISTINCT e."candidate_id") AS positive_response FROM "KEBOOLA_855"."out.c-reporting-v2"."event" e JOIN cand c ON c."candidate_id" = e."candidate_id" JOIN job_meta jm ON jm."job_id" = c."job_id" WHERE e."moved_to_stageType" = 'Positive Response' AND TRIM(e."who_event_created_for") <> '' AND TRY_TO_DATE(e."date_created") IS NOT NULL AND YEAROFWEEKISO(TRY_TO_DATE(e."date_created")) >= 2025 GROUP BY 1,2,3,4,5,6,7,8,9,10),
viewed AS (SELECT jm.client AS client, jm."job_id" AS job_id, jm."job_title" AS job_title, COALESCE(jm."job_category",'') AS job_category, TRIM(e."who_event_created_for") AS ta, '' AS ts, '' AS candidate_source, jm.is_external_recruiter AS is_external_recruiter, YEAROFWEEKISO(TRY_TO_DATE(e."date_created")) AS iso_year, WEEKISO(TRY_TO_DATE(e."date_created")) AS iso_week, COUNT(DISTINCT e."talent_id") AS viewed FROM "KEBOOLA_855"."out.c-reporting-v2"."event" e JOIN job_meta jm ON jm."job_id" = e."job_id" WHERE e."event_type" = 'Linkedin Visited Profile' AND TRIM(e."who_event_created_for") <> '' AND TRY_TO_DATE(e."date_created") IS NOT NULL AND YEAROFWEEKISO(TRY_TO_DATE(e."date_created")) >= 2025 GROUP BY 1,2,3,4,5,6,7,8,9,10),
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
SELECT k.client AS "CLIENT", k.job_id AS "JOB_ID", k.job_title AS "JOB_TITLE", k.job_category AS "JOB_CATEGORY", k.ta AS "TA", k.ts AS "TS", k.candidate_source AS "CANDIDATE_SOURCE", k.is_external_recruiter AS "IS_EXTERNAL_RECRUITER", k.iso_year AS "ISO_YEAR", k.iso_week AS "ISO_WEEK", COALESCE(v.viewed, 0) AS "VIEWED", COALESCE(c.contacted, 0) AS "CONTACTED", COALESCE(p.positive_response, 0) AS "POSITIVE_RESPONSE", COALESCE(sc.screens, 0) AS "SCREENS", COALESCE(a.actual_screens, 0) AS "ACTUAL_SCREENS", COALESCE(t.ats, 0) AS "ATS", COALESCE(o.offered, 0) AS "OFFERED", COALESCE(h.hired, 0) AS "HIRED"
FROM keys k
LEFT JOIN viewed v ON v.client=k.client AND v.job_id=k.job_id AND v.ta=k.ta AND v.ts=k.ts AND v.candidate_source=k.candidate_source AND v.iso_year=k.iso_year AND v.iso_week=k.iso_week
LEFT JOIN contacted c ON c.client=k.client AND c.job_id=k.job_id AND c.ta=k.ta AND c.ts=k.ts AND c.candidate_source=k.candidate_source AND c.iso_year=k.iso_year AND c.iso_week=k.iso_week
LEFT JOIN screens sc ON sc.client=k.client AND sc.job_id=k.job_id AND sc.ta=k.ta AND sc.ts=k.ts AND sc.candidate_source=k.candidate_source AND sc.iso_year=k.iso_year AND sc.iso_week=k.iso_week
LEFT JOIN actual_screens a ON a.client=k.client AND a.job_id=k.job_id AND a.ta=k.ta AND a.ts=k.ts AND a.candidate_source=k.candidate_source AND a.iso_year=k.iso_year AND a.iso_week=k.iso_week
LEFT JOIN ats_ t ON t.client=k.client AND t.job_id=k.job_id AND t.ta=k.ta AND t.ts=k.ts AND t.candidate_source=k.candidate_source AND t.iso_year=k.iso_year AND t.iso_week=k.iso_week
LEFT JOIN offers o ON o.client=k.client AND o.job_id=k.job_id AND o.ta=k.ta AND o.ts=k.ts AND o.candidate_source=k.candidate_source AND o.iso_year=k.iso_year AND o.iso_week=k.iso_week
LEFT JOIN hires h ON h.client=k.client AND h.job_id=k.job_id AND h.ta=k.ta AND h.ts=k.ts AND h.candidate_source=k.candidate_source AND h.iso_year=k.iso_year AND h.iso_week=k.iso_week
LEFT JOIN pos_resp p ON p.client=k.client AND p.job_id=k.job_id AND p.ta=k.ta AND p.ts=k.ts AND p.candidate_source=k.candidate_source AND p.iso_year=k.iso_year AND p.iso_week=k.iso_week
ORDER BY k.client, k.job_id, k.iso_year, k.iso_week
