-- wbr_jobs_weekly.sql — per-(client, TA, ISO year, ISO week) # Jobs metric
--
-- Output schema (matches snowflake_wbr_jobs.csv):
--   CLIENT, TA, ISO_YEAR, ISO_WEEK, JOBS
--
-- Replicates the PBI DAX for the Client's Target # Jobs column:
--   CALCULATE(DISTINCTCOUNT(event[job_id]),
--             USERELATIONSHIP('Calendar'[Date], event[date_created]))
--   filtered by:
--     job[job_title] <> BLANK()
--     candidate[is_candidate_archived] = FALSE()
--     client[client_name] <> BLANK() AND NOT IN {'Kamila AI - TEST'}
--     job[test] <> TRUE()
--     event[who_event_created_for] <> BLANK()
--     'Calendar'[Year] >= 2024
--
-- Attribution note:
--   TA = TRIM(event.who_event_created_for) — the person CREDITED for the
--   event that touched the job in this week. This is DIFFERENT from
--   wbr_weekly.sql (which attributes weekly candidate metrics to
--   job.job_recruiter). PBI's # Jobs column groups by event.who_event_created_for
--   specifically, and the Client Summary visual implicitly filters to TAs
--   that exist in the WBR TA Target sheet (via the Target↔Actual
--   relationship), so App.jsx applies the target-roster filter on read.
--
-- Validation (2026-04-20 against PBI w16 screenshot):
--   14/15 clients exact, Wolt NB&B -1, totals 129 vs PBI 130 (99.2%).
--   The -1 is one of 3 unallocated Wolt TAs (Anna Golubeva, Jaksa
--   Marojevic, Nemanja Erdevički) not present in any Wolt sub-BU row of
--   data.targets — likely Wolt Volume TAs. Adding them to the target
--   sheet or extending the recruiter→sub-BU fallback map in App.jsx
--   closes the gap.

WITH filtered_events AS (
  SELECT
    cl."client_name"                  AS client,
    TRIM(e."who_event_created_for")   AS ta,
    j."job_id"                        AS job_id,
    TRY_TO_DATE(e."date_created")     AS de
  FROM "KEBOOLA_855"."out.c-reporting-v2"."event"     e
  JOIN "KEBOOLA_855"."out.c-reporting-v2"."candidate" c  ON c."candidate_id" = e."candidate_id"
  JOIN "KEBOOLA_855"."out.c-reporting-v2"."job"       j  ON j."job_id"       = c."job_id"
  JOIN "KEBOOLA_855"."out.c-reporting-v2"."client"    cl ON cl."client_id"   = j."client_id"
  WHERE LOWER(NULLIF(c."is_candidate_archived", '')) <> 'true'
    AND j."job_title" IS NOT NULL AND j."job_title" <> ''
    AND (LOWER(NULLIF(j."test", '')) <> 'true' OR j."test" IS NULL)
    AND cl."client_name" IS NOT NULL AND cl."client_name" <> ''
    AND cl."client_name" NOT IN ('Kamila AI - TEST')
    AND e."who_event_created_for" IS NOT NULL AND e."who_event_created_for" <> ''
)
SELECT
  client            AS "CLIENT",
  ta                AS "TA",
  YEAROFWEEKISO(de) AS "ISO_YEAR",
  WEEKISO(de)       AS "ISO_WEEK",
  COUNT(DISTINCT job_id) AS "JOBS"
FROM filtered_events
WHERE de IS NOT NULL
  AND YEAROFWEEKISO(de) = 2026
GROUP BY 1, 2, 3, 4
ORDER BY "CLIENT", "TA", "ISO_YEAR", "ISO_WEEK"
