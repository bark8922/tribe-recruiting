-- ir_sourced_by.sql — per-sourcer Contacted/Pos Response/Hired for Tribe.xyz (IR)
--
-- Output schema:
--   SOURCER, CONTACTED, POS_RESPONSE, HIRED
--
-- Sourcer attribution (per Andy's PBI Sourced By visual):
--   sourcer = event.who_created_event_first
--   Contacted    = COUNT(DISTINCT candidate_stage.candidate_id) where date_contacted <> blank
--                  AND candidate is on an IR job AND has an event with who_created_event_first
--   Pos Response = COUNT(DISTINCT event.candidate_id) where event.moved_to_stageType="Positive Response"
--   Hired        = COUNT(DISTINCT candidate_stage.candidate_id) where date_hired <> blank
--                  AND has any event with who_created_event_first set
--
-- KNOWN GOTCHA (verified 2026-05-01):
--   Sanja Pavlovikj was deactivated 2026-04-23 in Ashby. Her historical
--   who_created_event_first was overwritten in Bubble for ~174 candidates,
--   dropping her totals 180 → 6. Numbers are accurate to current Bubble
--   state but historical attribution can shift when sourcers depart.

WITH ir_job AS (
  SELECT j."job_id"
  FROM "KEBOOLA_855"."out.c-reporting-v2"."job"    j
  JOIN "KEBOOLA_855"."out.c-reporting-v2"."client" cl ON cl."client_id" = j."client_id"
  WHERE cl."client_name" = 'Tribe.xyz (IR)'
    AND LOWER(NULLIF(j."is_job_archived",'')) <> 'true'
    AND LOWER(NULLIF(j."test",''))            <> 'true'
    AND NULLIF(j."job_title",'') IS NOT NULL
),
sourcer_per_cand AS (
  SELECT
    e."candidate_id",
    MAX(NULLIF(TRIM(e."who_created_event_first"),'')) AS sourcer
  FROM "KEBOOLA_855"."out.c-reporting-v2"."event" e
  WHERE e."job_id" IN (SELECT "job_id" FROM ir_job)
    AND NULLIF(e."who_created_event_first",'') IS NOT NULL
  GROUP BY e."candidate_id"
),
stage AS (
  SELECT
    cs."candidate_id",
    TRY_TO_DATE(cs."date_contacted") AS dc,
    TRY_TO_DATE(cs."date_hired")     AS dh
  FROM "KEBOOLA_855"."out.c-reporting-v2"."candidate_stage" cs
  WHERE cs."candidate_id" IN (
    SELECT c."candidate_id"
    FROM "KEBOOLA_855"."out.c-reporting-v2"."candidate" c
    WHERE c."job_id" IN (SELECT "job_id" FROM ir_job)
  )
),
pos_resp AS (
  SELECT
    NULLIF(TRIM(e."who_created_event_first"),'') AS sourcer,
    e."candidate_id"
  FROM "KEBOOLA_855"."out.c-reporting-v2"."event" e
  WHERE e."job_id" IN (SELECT "job_id" FROM ir_job)
    AND e."moved_to_stageType" = 'Positive Response'
    AND NULLIF(e."who_created_event_first",'') IS NOT NULL
),
contacted AS (
  SELECT s.sourcer, COUNT(DISTINCT st."candidate_id") AS n
  FROM stage st
  JOIN sourcer_per_cand s ON s."candidate_id" = st."candidate_id"
  WHERE st.dc IS NOT NULL
  GROUP BY s.sourcer
),
pos_response_agg AS (
  SELECT sourcer, COUNT(DISTINCT "candidate_id") AS n
  FROM pos_resp
  GROUP BY sourcer
),
hired_agg AS (
  SELECT s.sourcer, COUNT(DISTINCT st."candidate_id") AS n
  FROM stage st
  JOIN sourcer_per_cand s ON s."candidate_id" = st."candidate_id"
  WHERE st.dh IS NOT NULL
  GROUP BY s.sourcer
),
keys AS (
  SELECT sourcer FROM contacted
  UNION SELECT sourcer FROM pos_response_agg
  UNION SELECT sourcer FROM hired_agg
)
SELECT
  k.sourcer            AS "SOURCER",
  COALESCE(c.n, 0)     AS "CONTACTED",
  COALESCE(p.n, 0)     AS "POS_RESPONSE",
  COALESCE(h.n, 0)     AS "HIRED"
FROM keys k
LEFT JOIN contacted        c ON c.sourcer = k.sourcer
LEFT JOIN pos_response_agg p ON p.sourcer = k.sourcer
LEFT JOIN hired_agg        h ON h.sourcer = k.sourcer
WHERE k.sourcer IS NOT NULL
ORDER BY "CONTACTED" DESC, k.sourcer
