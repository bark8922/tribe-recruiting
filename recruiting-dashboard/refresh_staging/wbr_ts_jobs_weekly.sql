-- wbr_ts_jobs_weekly.sql — per-(TS, ISO year, ISO week) # Jobs / # TAs / TA names
--
-- Output schema (matches snowflake_ts_jobs.csv):
--   TS, ISO_YEAR, ISO_WEEK, NUM_JOBS, NUM_TAS, TA_NAMES
--
-- Drives the TS Weekly tab's `# Jobs`, `# TA`, and `TA Names` columns.
--
-- PBI DAX replica (validated 2026-04-20 vs PBI w16 — 10/11 TSes exact on # Jobs,
-- 11/11 exact on # TAs and TA names):
--
--   # Jobs per week for TS = COUNT(DISTINCT event.job_id) WHERE:
--     - TS = candidate.candidate_sourcer
--     - event in the ISO week
--     - job.is_job_archived <> true, job.test <> true, job.job_title non-blank
--     - candidate.is_candidate_archived <> true
--     - client.client_name NOT IN ('Tribe.xyz', 'Kamila AI - TEST')
--     - event is a "Contacted" event — either:
--         event_type = 'Moved to stage' AND moved_to_stage = 'Contacted', OR
--         event_type = 'Candidate created' AND moved_to_stage = 'Contacted'
--
-- The "Contacted events only" filter is what narrows the count from "all
-- events" (which gives Jonaed/Elena/Marina 2x over) to PBI's "pipelines the
-- TS is actively sourcing this week". Without this filter: Jovana 9 vs PBI 4,
-- Marina 11 vs 5, Elena 8 vs 4 — big regressions.
--
-- # TA and TA Names: DISTINCT job.job_recruiter over those same jobs,
-- EXCLUDING self-attribution (when TS = job.job_recruiter).
--
-- Validation vs PBI w16 (2026-04-20):
--   Andrea Akovic       6/4 (PBI 7/4, -1 jobs drift)   — TA names EXACT
--   Elena Petrovska     4/0  (PBI 4/0)  — EXACT
--   Gustavo Loureiro    4/2  (PBI 4/2)  — EXACT (Ella Darie, Filip Nogowski)
--   Jovana Drakula      4/0  (PBI 4/0)  — EXACT
--   Marina Lazarevic    5/2  (PBI 5/2)  — EXACT (Kristina, Wladyslaw)
--   Milica Veselinovic  1/0  (PBI 1/0)  — EXACT
--   Naledi Ngwenya      4/0  (PBI 4/0)  — EXACT
--   Nare Avetisyan      7/3  (PBI 7/3)  — EXACT (Adis, Anna, Lejla)
--   Rodrigo Gomes       5/3  (PBI 5/3)  — EXACT (Aleksandra, Jaksa, Vladimir)
--   Valeriia Yurykova   4/4  (PBI 4/4)  — EXACT (Ella, Jelena L., Marina N., Nenad)
--   Zelimir Stajcic     1/0  (PBI 1/0)  — EXACT

WITH filtered AS (
  SELECT DISTINCT j."job_id",
    TRIM(cd."candidate_sourcer") AS ts,
    TRIM(j."job_recruiter")      AS ta,
    YEAROFWEEKISO(TRY_TO_DATE(e."date_created")) AS iso_year,
    WEEKISO(TRY_TO_DATE(e."date_created"))       AS iso_week
  FROM "KEBOOLA_855"."out.c-reporting-v2"."event"     e
  JOIN "KEBOOLA_855"."out.c-reporting-v2"."candidate" cd ON cd."candidate_id" = e."candidate_id"
  JOIN "KEBOOLA_855"."out.c-reporting-v2"."job"       j  ON j."job_id"        = cd."job_id"
  JOIN "KEBOOLA_855"."out.c-reporting-v2"."client"    cl ON cl."client_id"    = j."client_id"
  WHERE LOWER(NULLIF(cd."is_candidate_archived", '')) <> 'true'
    AND (LOWER(NULLIF(j."test", '')) <> 'true' OR j."test" IS NULL)
    AND LOWER(NULLIF(j."is_job_archived", '')) <> 'true'
    AND j."job_title" IS NOT NULL AND j."job_title" <> ''
    AND cl."client_name" IS NOT NULL AND cl."client_name" <> ''
    AND cl."client_name" NOT IN ('Tribe.xyz', 'Kamila AI - TEST')
    AND cd."candidate_sourcer" IS NOT NULL AND cd."candidate_sourcer" <> ''
    AND cd."candidate_sourcer" <> '-not available-'
    AND TRY_TO_DATE(e."date_created") IS NOT NULL
    AND (
      (e."event_type" = 'Moved to stage'    AND e."moved_to_stage" = 'Contacted') OR
      (e."event_type" = 'Candidate created' AND e."moved_to_stage" = 'Contacted')
    )
)
SELECT
  ts AS "TS",
  iso_year AS "ISO_YEAR",
  iso_week AS "ISO_WEEK",
  COUNT(DISTINCT "job_id") AS "NUM_JOBS",
  COUNT(DISTINCT CASE WHEN ta <> ts AND ta <> '-not available-' AND COALESCE(ta, '') <> '' THEN ta END) AS "NUM_TAS",
  LISTAGG(DISTINCT CASE WHEN ta <> ts AND ta <> '-not available-' AND COALESCE(ta, '') <> '' THEN ta END, ', ')
    WITHIN GROUP (ORDER BY CASE WHEN ta <> ts AND ta <> '-not available-' AND COALESCE(ta, '') <> '' THEN ta END) AS "TA_NAMES"
FROM filtered
WHERE iso_year = 2026
GROUP BY 1, 2, 3
ORDER BY "TS", "ISO_YEAR", "ISO_WEEK"
