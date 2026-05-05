-- ir_funnel_jobweek.sql — per-(job_id, ISO year, ISO week) full funnel for Tribe.xyz (IR)
--
-- Output schema:
--   JOB_ID, ISO_YEAR, ISO_WEEK, CONTACTED, POS_RESPONSE, REC_SCREENS,
--   ACTUAL_SCREENS, ATS, ONSITE, CULTURE, CALL_W_CLIENT, OFFERED, HIRED
--
-- Replaces ir_funnel_weekly.sql (per-week only) with a (job, week) shape so
-- the frontend can filter by Job and aggregate the funnel + Weekly Performance
-- table dynamically. Each metric uses its own date column for week assignment.
-- Intentionally does NOT filter is_job_archived (UI handles active-only filter).

WITH ir_job AS (
  SELECT j."job_id" FROM "KEBOOLA_855"."out.c-reporting-v2"."job" j
  JOIN "KEBOOLA_855"."out.c-reporting-v2"."client" cl ON cl."client_id" = j."client_id"
  WHERE cl."client_name" IN ('Tribe.xyz (IR)', 'Tribe.xyz')
    AND LOWER(NULLIF(j."test",'')) <> 'true'
    AND NULLIF(j."job_title",'') IS NOT NULL
),
cand_job AS (
  SELECT c."candidate_id", c."job_id"
  FROM "KEBOOLA_855"."out.c-reporting-v2"."candidate" c
  WHERE c."job_id" IN (SELECT "job_id" FROM ir_job)
),
stage AS (
  SELECT cj."job_id", cj."candidate_id",
         TRY_TO_DATE(cs."date_contacted")     AS dc,
         TRY_TO_DATE(cs."date_screen")        AS ds,
         TRY_TO_DATE(cs."date_screen_actual") AS dsa,
         TRY_TO_DATE(cs."date_interview")     AS di,
         TRY_TO_DATE(cs."date_offer")         AS doff,
         TRY_TO_DATE(cs."date_hired")         AS dh
  FROM "KEBOOLA_855"."out.c-reporting-v2"."candidate_stage" cs
  JOIN cand_job cj ON cj."candidate_id" = cs."candidate_id"
),
ev AS (
  SELECT e."job_id", e."candidate_id", TRY_TO_DATE(e."date_created") AS de,
         e."moved_to_stage", e."moved_to_stageType"
  FROM "KEBOOLA_855"."out.c-reporting-v2"."event" e
  WHERE e."job_id" IN (SELECT "job_id" FROM ir_job)
),
contacted      AS (SELECT "job_id", YEAROFWEEKISO(dc)   y, WEEKISO(dc)   w, COUNT(DISTINCT "candidate_id") n FROM stage WHERE dc   IS NOT NULL AND YEAROFWEEKISO(dc)   = 2026 GROUP BY 1,2,3),
pos_response   AS (SELECT "job_id", YEAROFWEEKISO(de)   y, WEEKISO(de)   w, COUNT(DISTINCT "candidate_id") n FROM ev    WHERE de   IS NOT NULL AND YEAROFWEEKISO(de)   = 2026 AND "moved_to_stageType" = 'Positive Response' GROUP BY 1,2,3),
rec_screens    AS (SELECT "job_id", YEAROFWEEKISO(ds)   y, WEEKISO(ds)   w, COUNT(DISTINCT "candidate_id") n FROM stage WHERE ds   IS NOT NULL AND YEAROFWEEKISO(ds)   = 2026 GROUP BY 1,2,3),
actual_screens AS (SELECT "job_id", YEAROFWEEKISO(dsa)  y, WEEKISO(dsa)  w, COUNT(DISTINCT "candidate_id") n FROM stage WHERE dsa  IS NOT NULL AND YEAROFWEEKISO(dsa)  = 2026 GROUP BY 1,2,3),
ats_           AS (SELECT "job_id", YEAROFWEEKISO(di)   y, WEEKISO(di)   w, COUNT(DISTINCT "candidate_id") n FROM stage WHERE di   IS NOT NULL AND YEAROFWEEKISO(di)   = 2026 GROUP BY 1,2,3),
onsite         AS (SELECT "job_id", YEAROFWEEKISO(de)   y, WEEKISO(de)   w, COUNT(DISTINCT "candidate_id") n FROM ev    WHERE de   IS NOT NULL AND YEAROFWEEKISO(de)   = 2026 AND "moved_to_stageType" = 'Final Interview' AND "moved_to_stage" = 'Onsite' GROUP BY 1,2,3),
culture        AS (SELECT "job_id", YEAROFWEEKISO(de)   y, WEEKISO(de)   w, COUNT(DISTINCT "candidate_id") n FROM ev    WHERE de   IS NOT NULL AND YEAROFWEEKISO(de)   = 2026 AND "moved_to_stageType" = 'Final Interview' AND "moved_to_stage" = 'Culture Interview' GROUP BY 1,2,3),
call_w_client  AS (SELECT "job_id", YEAROFWEEKISO(de)   y, WEEKISO(de)   w, COUNT(DISTINCT "candidate_id") n FROM ev    WHERE de   IS NOT NULL AND YEAROFWEEKISO(de)   = 2026 AND "moved_to_stageType" = 'Final Interview' AND "moved_to_stage" = 'Call with Client' GROUP BY 1,2,3),
offered        AS (SELECT "job_id", YEAROFWEEKISO(doff) y, WEEKISO(doff) w, COUNT(DISTINCT "candidate_id") n FROM stage WHERE doff IS NOT NULL AND YEAROFWEEKISO(doff) = 2026 GROUP BY 1,2,3),
hired          AS (SELECT "job_id", YEAROFWEEKISO(dh)   y, WEEKISO(dh)   w, COUNT(DISTINCT "candidate_id") n FROM stage WHERE dh   IS NOT NULL AND YEAROFWEEKISO(dh)   = 2026 GROUP BY 1,2,3),
keys AS (
  SELECT "job_id", y, w FROM contacted UNION SELECT "job_id", y, w FROM pos_response
  UNION SELECT "job_id", y, w FROM rec_screens UNION SELECT "job_id", y, w FROM actual_screens
  UNION SELECT "job_id", y, w FROM ats_ UNION SELECT "job_id", y, w FROM onsite
  UNION SELECT "job_id", y, w FROM culture UNION SELECT "job_id", y, w FROM call_w_client
  UNION SELECT "job_id", y, w FROM offered UNION SELECT "job_id", y, w FROM hired
)
SELECT k."job_id" AS "JOB_ID", k.y AS "ISO_YEAR", k.w AS "ISO_WEEK",
  COALESCE(c.n,0)    AS "CONTACTED",
  COALESCE(p.n,0)    AS "POS_RESPONSE",
  COALESCE(rs.n,0)   AS "REC_SCREENS",
  COALESCE(asc_.n,0) AS "ACTUAL_SCREENS",
  COALESCE(a.n,0)    AS "ATS",
  COALESCE(on_.n,0)  AS "ONSITE",
  COALESCE(cu.n,0)   AS "CULTURE",
  COALESCE(cwc.n,0)  AS "CALL_W_CLIENT",
  COALESCE(o.n,0)    AS "OFFERED",
  COALESCE(h.n,0)    AS "HIRED"
FROM keys k
LEFT JOIN contacted      c    ON c."job_id"=k."job_id"    AND c.y=k.y    AND c.w=k.w
LEFT JOIN pos_response   p    ON p."job_id"=k."job_id"    AND p.y=k.y    AND p.w=k.w
LEFT JOIN rec_screens    rs   ON rs."job_id"=k."job_id"   AND rs.y=k.y   AND rs.w=k.w
LEFT JOIN actual_screens asc_ ON asc_."job_id"=k."job_id" AND asc_.y=k.y AND asc_.w=k.w
LEFT JOIN ats_           a    ON a."job_id"=k."job_id"    AND a.y=k.y    AND a.w=k.w
LEFT JOIN onsite         on_  ON on_."job_id"=k."job_id"  AND on_.y=k.y  AND on_.w=k.w
LEFT JOIN culture        cu   ON cu."job_id"=k."job_id"   AND cu.y=k.y   AND cu.w=k.w
LEFT JOIN call_w_client  cwc  ON cwc."job_id"=k."job_id"  AND cwc.y=k.y  AND cwc.w=k.w
LEFT JOIN offered        o    ON o."job_id"=k."job_id"    AND o.y=k.y    AND o.w=k.w
LEFT JOIN hired          h    ON h."job_id"=k."job_id"    AND h.y=k.y    AND h.w=k.w
ORDER BY k."job_id", k.y, k.w
