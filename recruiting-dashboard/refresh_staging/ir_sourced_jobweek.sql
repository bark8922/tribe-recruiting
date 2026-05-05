-- ir_sourced_jobweek.sql — per-(job_id, sourcer, ISO week) Contacted/Pos Resp/Hired
-- for Tribe.xyz (IR). Frontend aggregates by filter selection.
--
-- Output: JOB_ID, SOURCER, ISO_YEAR, ISO_WEEK, CONTACTED, POS_RESPONSE, HIRED

WITH ir_job AS (
  SELECT j."job_id" FROM "KEBOOLA_855"."out.c-reporting-v2"."job" j
  JOIN "KEBOOLA_855"."out.c-reporting-v2"."client" cl ON cl."client_id" = j."client_id"
  WHERE cl."client_name" IN ('Tribe.xyz (IR)', 'Tribe.xyz')
    AND LOWER(NULLIF(j."test",'')) <> 'true'
    AND NULLIF(j."job_title",'') IS NOT NULL
),
cand_job AS (SELECT c."candidate_id", c."job_id" FROM "KEBOOLA_855"."out.c-reporting-v2"."candidate" c WHERE c."job_id" IN (SELECT "job_id" FROM ir_job)),
sourcer_per_cand AS (
  SELECT e."candidate_id", MAX(NULLIF(TRIM(e."who_created_event_first"),'')) AS sourcer
  FROM "KEBOOLA_855"."out.c-reporting-v2"."event" e
  WHERE e."job_id" IN (SELECT "job_id" FROM ir_job) AND NULLIF(e."who_created_event_first",'') IS NOT NULL
  GROUP BY e."candidate_id"
),
stage AS (
  SELECT cj."job_id", cj."candidate_id",
         TRY_TO_DATE(cs."date_contacted") AS dc,
         TRY_TO_DATE(cs."date_hired")     AS dh
  FROM "KEBOOLA_855"."out.c-reporting-v2"."candidate_stage" cs
  JOIN cand_job cj ON cj."candidate_id" = cs."candidate_id"
),
contacted AS (
  SELECT s."job_id", spc.sourcer, YEAROFWEEKISO(s.dc) y, WEEKISO(s.dc) w, COUNT(DISTINCT s."candidate_id") n
  FROM stage s JOIN sourcer_per_cand spc ON spc."candidate_id" = s."candidate_id"
  WHERE s.dc IS NOT NULL AND YEAROFWEEKISO(s.dc) = 2026 GROUP BY 1,2,3,4
),
pos_response AS (
  SELECT e."job_id", NULLIF(TRIM(e."who_created_event_first"),'') sourcer,
         YEAROFWEEKISO(TRY_TO_DATE(e."date_created")) y, WEEKISO(TRY_TO_DATE(e."date_created")) w,
         COUNT(DISTINCT e."candidate_id") n
  FROM "KEBOOLA_855"."out.c-reporting-v2"."event" e
  WHERE e."job_id" IN (SELECT "job_id" FROM ir_job)
    AND e."moved_to_stageType" = 'Positive Response'
    AND NULLIF(e."who_created_event_first",'') IS NOT NULL
    AND TRY_TO_DATE(e."date_created") IS NOT NULL AND YEAROFWEEKISO(TRY_TO_DATE(e."date_created")) = 2026
  GROUP BY 1,2,3,4
),
hired AS (
  SELECT s."job_id", spc.sourcer, YEAROFWEEKISO(s.dh) y, WEEKISO(s.dh) w, COUNT(DISTINCT s."candidate_id") n
  FROM stage s JOIN sourcer_per_cand spc ON spc."candidate_id" = s."candidate_id"
  WHERE s.dh IS NOT NULL AND YEAROFWEEKISO(s.dh) = 2026 GROUP BY 1,2,3,4
),
keys AS (
  SELECT "job_id", sourcer, y, w FROM contacted
  UNION SELECT "job_id", sourcer, y, w FROM pos_response
  UNION SELECT "job_id", sourcer, y, w FROM hired
)
SELECT k."job_id" "JOB_ID", k.sourcer "SOURCER", k.y "ISO_YEAR", k.w "ISO_WEEK",
  COALESCE(c.n,0) "CONTACTED", COALESCE(p.n,0) "POS_RESPONSE", COALESCE(h.n,0) "HIRED"
FROM keys k
LEFT JOIN contacted    c ON c."job_id"=k."job_id" AND c.sourcer=k.sourcer AND c.y=k.y AND c.w=k.w
LEFT JOIN pos_response p ON p."job_id"=k."job_id" AND p.sourcer=k.sourcer AND p.y=k.y AND p.w=k.w
LEFT JOIN hired        h ON h."job_id"=k."job_id" AND h.sourcer=k.sourcer AND h.y=k.y AND h.w=k.w
WHERE k.sourcer IS NOT NULL
ORDER BY k."job_id", k.sourcer, k.y, k.w
