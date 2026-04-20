-- wbr_ts_jobs_weekly.sql — per-(TS, ISO year, ISO week) # Jobs / # TAs / TA names
--
-- Output schema (matches snowflake_ts_jobs.csv):
--   TS, ISO_YEAR, ISO_WEEK, NUM_JOBS, NUM_TAS, TA_NAMES
--
-- Drives the TS Weekly tab's `# Jobs`, `# TA`, and `TA Names` columns.
-- Attribution: candidate.candidate_sourcer (same as the rest of the TS
-- pipeline). For each candidate_sourcer in a week, count:
--   - num_jobs:  DISTINCTCOUNT(event.job_id) where the sourcer is
--                candidate_sourcer and the event fires in the week
--   - num_tas:   DISTINCTCOUNT(job.job_recruiter) — EXCLUDING the TS
--                themselves (PBI convention: when a TS is also their own
--                job's TA, that self-attribution doesn't count in # TA)
--   - ta_names:  comma-separated DISTINCT job.job_recruiter values from
--                the same filter (matches PBI's CONCATENATEX presentation)
--
-- Validation vs PBI w16 (2026-04-20):
--   Andrea Akovic 7/4 (Chené, Kristina, Samantha, Wladyslaw) — EXACT
--   Rodrigo Gomes 5/3 (Aleksandra, Jaksa, Vladimir)            — EXACT
--   Naledi Ngwenya 4/0                                         — EXACT
--   Other TSes close on # TAs, job counts run higher than PBI
--   for Elena/Marina/Valeriia — TBD if narrower filter needed.

WITH filtered_events AS (
  SELECT
    TRIM(c."candidate_sourcer") AS ts,
    j."job_id",
    TRIM(j."job_recruiter")     AS ta,
    TRY_TO_DATE(e."date_created") AS de
  FROM "KEBOOLA_855"."out.c-reporting-v2"."event"     e
  JOIN "KEBOOLA_855"."out.c-reporting-v2"."candidate" c  ON c."candidate_id" = e."candidate_id"
  JOIN "KEBOOLA_855"."out.c-reporting-v2"."job"       j  ON j."job_id"       = c."job_id"
  JOIN "KEBOOLA_855"."out.c-reporting-v2"."client"    cl ON cl."client_id"   = j."client_id"
  WHERE LOWER(NULLIF(c."is_candidate_archived", '')) <> 'true'
    AND j."job_title" IS NOT NULL AND j."job_title" <> ''
    AND (LOWER(NULLIF(j."test", '')) <> 'true' OR j."test" IS NULL)
    AND cl."client_name" IS NOT NULL AND cl."client_name" <> ''
    AND cl."client_name" NOT IN ('Tribe.xyz', 'Kamila AI - TEST')
    AND c."candidate_sourcer" IS NOT NULL AND c."candidate_sourcer" <> ''
    AND c."candidate_sourcer" <> '-not available-'
    AND j."job_recruiter" IS NOT NULL AND j."job_recruiter" <> ''
)
SELECT
  ts                    AS "TS",
  YEAROFWEEKISO(de)     AS "ISO_YEAR",
  WEEKISO(de)           AS "ISO_WEEK",
  COUNT(DISTINCT "job_id")                                   AS "NUM_JOBS",
  COUNT(DISTINCT CASE WHEN ta <> ts THEN ta END)             AS "NUM_TAS",
  LISTAGG(DISTINCT CASE WHEN ta <> ts THEN ta END, ', ')
    WITHIN GROUP (ORDER BY CASE WHEN ta <> ts THEN ta END)   AS "TA_NAMES"
FROM filtered_events
WHERE de IS NOT NULL AND YEAROFWEEKISO(de) = 2026
GROUP BY 1, 2, 3
ORDER BY "TS", "ISO_YEAR", "ISO_WEEK"
