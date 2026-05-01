-- ir_funnel_weekly.sql — per-(ISO year, ISO week) funnel for Tribe.xyz (IR) jobs
--
-- Output schema:
--   ISO_YEAR, ISO_WEEK, CONTACTED, POS_RESPONSE, REC_SCREENS, ACTUAL_SCREENS,
--   ATS, ONSITE, CULTURE, CALL_W_CLIENT, OFFERED, HIRED
--
-- Inverts the Tribe.xyz (IR) exclusion used by WBR/MBR — this query is the
-- ONLY query in the pipeline that *requires* IR jobs. Filters mirror Andy's
-- PBI Internal Recruitment page (legacy-pbix/pages/internal-recruitment.md).
--
-- Filters (per Andy's PBI applied filter list):
--   client.client_name = 'Tribe.xyz (IR)'
--   job.is_job_archived <> 'true'
--   job.test <> 'true'
--   job.job_title is not blank
--   client.client_name does not contain 'test' or 'fake' (auto-satisfied)
--
-- Stage definitions (mirroring Andy's DAX, see internal-recruitment.md):
--   Contacted     = candidate_stage.date_contacted     <> blank in week
--   Pos Response  = event[moved_to_stageType="Positive Response", date_created in week]
--   Rec Screens   = candidate_stage.date_screen        <> blank in week
--                   AND event.moved_to_stage="Recruiter Screen"
--   Actual Screens= candidate_stage.date_screen_actual <> blank in week
--                   AND event.event_type="Evaluation"
--   Moved to ATS  = candidate_stage.date_interview     <> blank in week
--                   AND event.moved_to_stage="Moved to ATS"
--   Onsite        = event[moved_to_stageType="Final Interview", moved_to_stage="Onsite"]
--   Culture       = event[moved_to_stageType="Final Interview", moved_to_stage="Culture Interview"]
--   Call w/ Client= event[moved_to_stageType="Final Interview", moved_to_stage="Call with Client"]
--   Offered       = candidate_stage.date_offer in week
--   Hired         = candidate_stage.date_hired in week

WITH ir_job AS (
  SELECT j."job_id"
  FROM "KEBOOLA_855"."out.c-reporting-v2"."job"    j
  JOIN "KEBOOLA_855"."out.c-reporting-v2"."client" cl ON cl."client_id" = j."client_id"
  WHERE cl."client_name" = 'Tribe.xyz (IR)'
    AND LOWER(NULLIF(j."is_job_archived",'')) <> 'true'
    AND LOWER(NULLIF(j."test",''))            <> 'true'
    AND NULLIF(j."job_title",'') IS NOT NULL
),
ir_cand AS (
  SELECT c."candidate_id"
  FROM "KEBOOLA_855"."out.c-reporting-v2"."candidate" c
  JOIN ir_job j ON j."job_id" = c."job_id"
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
  WHERE cs."candidate_id" IN (SELECT "candidate_id" FROM ir_cand)
),
ev AS (
  SELECT
    e."candidate_id",
    TRY_TO_DATE(e."date_created")  AS de,
    e."event_type",
    e."moved_to_stage",
    e."moved_to_stageType"
  FROM "KEBOOLA_855"."out.c-reporting-v2"."event" e
  WHERE e."job_id" IN (SELECT "job_id" FROM ir_job)
),
contacted AS (
  SELECT YEAROFWEEKISO(dc) AS y, WEEKISO(dc) AS w, COUNT(DISTINCT "candidate_id") AS n
  FROM stage WHERE dc IS NOT NULL AND YEAROFWEEKISO(dc) = 2026 GROUP BY 1,2
),
pos_response AS (
  SELECT YEAROFWEEKISO(de) AS y, WEEKISO(de) AS w, COUNT(DISTINCT "candidate_id") AS n
  FROM ev WHERE de IS NOT NULL AND YEAROFWEEKISO(de) = 2026
    AND "moved_to_stageType" = 'Positive Response' GROUP BY 1,2
),
rec_screens AS (
  SELECT YEAROFWEEKISO(ds) AS y, WEEKISO(ds) AS w, COUNT(DISTINCT "candidate_id") AS n
  FROM stage WHERE ds IS NOT NULL AND YEAROFWEEKISO(ds) = 2026 GROUP BY 1,2
),
actual_screens AS (
  SELECT YEAROFWEEKISO(dsa) AS y, WEEKISO(dsa) AS w, COUNT(DISTINCT "candidate_id") AS n
  FROM stage WHERE dsa IS NOT NULL AND YEAROFWEEKISO(dsa) = 2026 GROUP BY 1,2
),
ats_ AS (
  SELECT YEAROFWEEKISO(di) AS y, WEEKISO(di) AS w, COUNT(DISTINCT "candidate_id") AS n
  FROM stage WHERE di IS NOT NULL AND YEAROFWEEKISO(di) = 2026 GROUP BY 1,2
),
onsite AS (
  SELECT YEAROFWEEKISO(de) AS y, WEEKISO(de) AS w, COUNT(DISTINCT "candidate_id") AS n
  FROM ev WHERE de IS NOT NULL AND YEAROFWEEKISO(de) = 2026
    AND "moved_to_stageType" = 'Final Interview'
    AND "moved_to_stage"     = 'Onsite' GROUP BY 1,2
),
culture AS (
  SELECT YEAROFWEEKISO(de) AS y, WEEKISO(de) AS w, COUNT(DISTINCT "candidate_id") AS n
  FROM ev WHERE de IS NOT NULL AND YEAROFWEEKISO(de) = 2026
    AND "moved_to_stageType" = 'Final Interview'
    AND "moved_to_stage"     = 'Culture Interview' GROUP BY 1,2
),
call_w_client AS (
  SELECT YEAROFWEEKISO(de) AS y, WEEKISO(de) AS w, COUNT(DISTINCT "candidate_id") AS n
  FROM ev WHERE de IS NOT NULL AND YEAROFWEEKISO(de) = 2026
    AND "moved_to_stageType" = 'Final Interview'
    AND "moved_to_stage"     = 'Call with Client' GROUP BY 1,2
),
offered AS (
  SELECT YEAROFWEEKISO(doff) AS y, WEEKISO(doff) AS w, COUNT(DISTINCT "candidate_id") AS n
  FROM stage WHERE doff IS NOT NULL AND YEAROFWEEKISO(doff) = 2026 GROUP BY 1,2
),
hired AS (
  SELECT YEAROFWEEKISO(dh) AS y, WEEKISO(dh) AS w, COUNT(DISTINCT "candidate_id") AS n
  FROM stage WHERE dh IS NOT NULL AND YEAROFWEEKISO(dh) = 2026 GROUP BY 1,2
),
keys AS (
  SELECT y, w FROM contacted
  UNION SELECT y, w FROM pos_response
  UNION SELECT y, w FROM rec_screens
  UNION SELECT y, w FROM actual_screens
  UNION SELECT y, w FROM ats_
  UNION SELECT y, w FROM onsite
  UNION SELECT y, w FROM culture
  UNION SELECT y, w FROM call_w_client
  UNION SELECT y, w FROM offered
  UNION SELECT y, w FROM hired
)
SELECT
  k.y AS "ISO_YEAR",
  k.w AS "ISO_WEEK",
  COALESCE(c.n,   0) AS "CONTACTED",
  COALESCE(p.n,   0) AS "POS_RESPONSE",
  COALESCE(rs.n,  0) AS "REC_SCREENS",
  COALESCE(asc_.n,0) AS "ACTUAL_SCREENS",
  COALESCE(a.n,   0) AS "ATS",
  COALESCE(on_.n, 0) AS "ONSITE",
  COALESCE(cu.n,  0) AS "CULTURE",
  COALESCE(cwc.n, 0) AS "CALL_W_CLIENT",
  COALESCE(o.n,   0) AS "OFFERED",
  COALESCE(h.n,   0) AS "HIRED"
FROM keys k
LEFT JOIN contacted      c    ON c.y=k.y    AND c.w=k.w
LEFT JOIN pos_response   p    ON p.y=k.y    AND p.w=k.w
LEFT JOIN rec_screens    rs   ON rs.y=k.y   AND rs.w=k.w
LEFT JOIN actual_screens asc_ ON asc_.y=k.y AND asc_.w=k.w
LEFT JOIN ats_           a    ON a.y=k.y    AND a.w=k.w
LEFT JOIN onsite         on_  ON on_.y=k.y  AND on_.w=k.w
LEFT JOIN culture        cu   ON cu.y=k.y   AND cu.w=k.w
LEFT JOIN call_w_client  cwc  ON cwc.y=k.y  AND cwc.w=k.w
LEFT JOIN offered        o    ON o.y=k.y    AND o.w=k.w
LEFT JOIN hired          h    ON h.y=k.y    AND h.w=k.w
ORDER BY k.y, k.w
