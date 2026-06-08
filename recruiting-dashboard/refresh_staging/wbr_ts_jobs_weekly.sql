-- wbr_ts_jobs_weekly.sql — per-(TS, ISO year, ISO week) # Jobs / # TAs / TA names
--
-- Output schema (matches snowflake_ts_jobs.csv):
--   TS, ISO_YEAR, ISO_WEEK, NUM_JOBS, NUM_TAS, TA_NAMES
--
-- Drives the TS Weekly tab's `# Jobs`, `# TA`, and `TA Names` columns.
--
-- DEFINITION (rewritten 2026-06-08, per Blake):
--   TA Names = the people actually doing recruiter-side work ("touching the
--   role") on the jobs a sourcer contacted into that ISO week — NOT the static
--   job.job_recruiter assignment, which is frequently set to the sourcer
--   themselves or left unset and produced spurious blanks.
--
--   # Jobs  = COUNT(DISTINCT job_id) the TS contacted into that week
--             (same Contacted-event definition as before).
--   TA Names / # TAs = DISTINCT event.who_event_created_for on RECRUITER-STAGE
--             events (Recruiter Screen / Moved to ATS / Onsite / Offer / Hired)
--             logged on those same jobs in the SAME ISO week, excluding the
--             sourcer themselves (whitespace-normalised so "Jelena  Lacmanovic"
--             never self-matches "Jelena Lacmanovic"), excluding '-not available-'.
--
--   "Strict same-week" is intentional (confirmed by Blake): if no TA did
--   recruiter-stage work on the role that week, TA Names is blank — that is a
--   real signal, not a data artefact.
--
-- Filters on the touched-jobs set: job.is_job_archived <> true, job.test <> true,
-- job.job_title non-blank, client.client_name non-blank & not 'Kamila AI - TEST',
-- candidate.is_candidate_archived <> true, candidate_sourcer present & not
-- '-not available-'.

WITH touched AS (
  SELECT DISTINCT
    TRIM(cd."candidate_sourcer") AS "ts",
    j."job_id"                   AS "job_id",
    YEAROFWEEKISO(TRY_TO_DATE(e."date_created")) AS "iso_year",
    WEEKISO(TRY_TO_DATE(e."date_created"))       AS "iso_week"
  FROM "KEBOOLA_855"."out.c-reporting-v2"."event"     e
  JOIN "KEBOOLA_855"."out.c-reporting-v2"."candidate" cd ON cd."candidate_id" = e."candidate_id"
  JOIN "KEBOOLA_855"."out.c-reporting-v2"."job"       j  ON j."job_id"        = cd."job_id"
  JOIN "KEBOOLA_855"."out.c-reporting-v2"."client"    cl ON cl."client_id"    = j."client_id"
  WHERE LOWER(NULLIF(cd."is_candidate_archived", '')) <> 'true'
    AND (LOWER(NULLIF(j."test", '')) <> 'true' OR j."test" IS NULL)
    AND LOWER(NULLIF(j."is_job_archived", '')) <> 'true'
    AND j."job_title" IS NOT NULL AND j."job_title" <> ''
    AND cl."client_name" IS NOT NULL AND cl."client_name" <> ''
    AND cl."client_name" NOT IN ('Kamila AI - TEST')
    AND cd."candidate_sourcer" IS NOT NULL AND cd."candidate_sourcer" <> ''
    AND cd."candidate_sourcer" <> '-not available-'
    AND TRY_TO_DATE(e."date_created") IS NOT NULL
    AND (
      (e."event_type" = 'Moved to stage'    AND e."moved_to_stage" = 'Contacted') OR
      (e."event_type" = 'Candidate created' AND e."moved_to_stage" = 'Contacted')
    )
),
ta AS (
  SELECT DISTINCT
    t."ts", t."iso_year", t."iso_week", t."job_id",
    TRIM(ev."who_event_created_for") AS "actor"
  FROM touched t
  JOIN "KEBOOLA_855"."out.c-reporting-v2"."event" ev ON ev."job_id" = t."job_id"
  WHERE YEAROFWEEKISO(TRY_TO_DATE(ev."date_created")) = t."iso_year"
    AND WEEKISO(TRY_TO_DATE(ev."date_created"))       = t."iso_week"
    AND ev."event_type" = 'Moved to stage'
    AND ev."moved_to_stage" IN ('Recruiter Screen', 'Moved to ATS', 'Move to ATS stage', 'Onsite', 'Offer', 'Hired')
    AND ev."who_event_created_for" IS NOT NULL
    AND TRIM(ev."who_event_created_for") <> ''
    AND TRIM(ev."who_event_created_for") <> '-not available-'
    AND REGEXP_REPLACE(TRIM(ev."who_event_created_for"), ' +', ' ') <> REGEXP_REPLACE(t."ts", ' +', ' ')
),
job_counts AS (
  SELECT "ts", "iso_year", "iso_week", COUNT(DISTINCT "job_id") AS "num_jobs"
  FROM touched GROUP BY 1, 2, 3
),
ta_agg AS (
  SELECT "ts", "iso_year", "iso_week",
    COUNT(DISTINCT "actor") AS "num_tas",
    LISTAGG(DISTINCT "actor", ', ') WITHIN GROUP (ORDER BY "actor") AS "ta_names"
  FROM ta GROUP BY 1, 2, 3
)
SELECT
  jc."ts"       AS "TS",
  jc."iso_year" AS "ISO_YEAR",
  jc."iso_week" AS "ISO_WEEK",
  jc."num_jobs" AS "NUM_JOBS",
  COALESCE(ta_agg."num_tas", 0)   AS "NUM_TAS",
  COALESCE(ta_agg."ta_names", '') AS "TA_NAMES"
FROM job_counts jc
LEFT JOIN ta_agg
  ON ta_agg."ts" = jc."ts" AND ta_agg."iso_year" = jc."iso_year" AND ta_agg."iso_week" = jc."iso_week"
WHERE jc."iso_year" = 2026
ORDER BY "TS", "ISO_YEAR", "ISO_WEEK"
