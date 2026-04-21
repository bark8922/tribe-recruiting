-- mbr_contacted_ev.sql — per-(client, TA, ISO week) Contacted counts using
-- event-based attribution (matches PBI's MBR Contacted DAX).
--
-- Attribution: event.who_event_created_for (whoever the event was done for),
-- not job.job_recruiter. Previously MBR used candidate_stage.date_contacted +
-- job.job_recruiter, which credits the ORIGINAL job TA even after reassignment.
-- PBI credits the TA on the event itself, which correctly handles TA
-- reassignment (e.g. Iryna Dyda picking up Alexandra Richiteanu's candidates
-- mid-window). This was the root of Aviv +103 and Eucalyptus +219 MBR drift.
--
-- Output: CLIENT, TA (= who_event_created_for), ISO_YEAR, ISO_WEEK, CONTACTED_EV

SELECT TRIM(cl."client_name") AS "CLIENT",
       TRIM(e."who_event_created_for") AS "TA",
       YEAROFWEEKISO(TRY_TO_DATE(e."date_created")) AS "ISO_YEAR",
       WEEKISO(TRY_TO_DATE(e."date_created")) AS "ISO_WEEK",
       COUNT(DISTINCT e."candidate_id") AS "CONTACTED_EV"
FROM "KEBOOLA_855"."out.c-reporting-v2"."event" e
JOIN "KEBOOLA_855"."out.c-reporting-v2"."candidate" c ON c."candidate_id" = e."candidate_id"
JOIN "KEBOOLA_855"."out.c-reporting-v2"."job" j ON j."job_id" = c."job_id"
JOIN "KEBOOLA_855"."out.c-reporting-v2"."client" cl ON cl."client_id" = j."client_id"
WHERE e."event_type" = 'Moved to stage'
  AND e."moved_to_stage" = 'Contacted'
  AND LOWER(NULLIF(j."test",'')) <> 'true'
  AND LOWER(NULLIF(c."is_candidate_archived",'')) <> 'true'
  AND cl."client_name" IS NOT NULL AND cl."client_name" <> ''
  AND cl."client_name" NOT IN ('Tribe.xyz','Tribe.xyz (IR)','BD - Tribe','Tribe - Marketing','Kamila AI - TEST','Bubble test')
  AND e."who_event_created_for" IS NOT NULL AND e."who_event_created_for" <> ''
  AND YEAROFWEEKISO(TRY_TO_DATE(e."date_created")) = 2026
GROUP BY 1,2,3,4
ORDER BY 1,3,4,2
