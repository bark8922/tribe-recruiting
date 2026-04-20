-- Aux metrics (12w + jobs_60d) per (client_wolt_group, TA) and per TS.
-- ------------------------------------------------------------------
-- Matches PBI WBR TA Actual partition spec:
--   Calendar range: DATE_TRUNC(week) over last 12 weeks (inclusive of current week start)
--   Filters (CALCULATETABLE):
--     job[job_title] <> BLANK()
--     candidate[is_candidate_archived] = FALSE
--     client[client_name] <> BLANK() AND NOT IN {'Tribe.xyz','Kamila AI - TEST'}
--     job[test] <> TRUE
--     event[who_event_created_for] <> BLANK() (for stage-event attribution)
--     Calendar[Year] >= 2024
--   Metrics:
--     Hires       = DISTINCTCOUNT candidate on date_hired via matching Hired event (v2 fix)
--     ActualScrn  = DISTINCTCOUNT candidate on date_screen_actual requiring Evaluation event + matching event
--     MovedToATS  = DISTINCTCOUNT candidate on date_interview via matching Interview/Offsite event
--     TTF         = AVG(candidate_stage.date_hired - job.date_created) in days for hires in window
-- ------------------------------------------------------------------
-- TA attribution (2026-04-16 revision):
--   hires:       event.who_event_created_for (authoritative Hired event, dedup rn=1)
--   screens/ats: job.job_recruiter (the candidate's assigned TA — matches live)
--   jobs_60d:    job.job_recruiter
--   NB: event.who_event_created_for mis-attributes screens/ats to the last
--   person who touched the candidate (often a sourcer), not the owning TA.
-- TS attribution: candidate.candidate_sourcer for all metrics.
-- ------------------------------------------------------------------
-- Emits UNION ALL of rows tagged by ROLE ('TA','TS') and METRIC ('hires','screens','ats','ttf_sum','ttf_cnt','jobs_60d').
-- TA rows emit RAW client_name so downstream rollup can split SevenRooms/Doordash
-- (→ Wolt HQ via normalize_client) from Wolt (→ sub-BU via target lookup).

WITH
anchor AS (
  -- cur_wk = Sunday of the CURRENT ISO week (inclusive). PBI's "Last 12 weeks"
  -- rolling window includes the current week — not excludes it. Previous
  -- DATEADD('day', -1, …) landed on last Sunday, dropping current-week events
  -- and causing -12 screens for Jonaed (Parloa), -4 for Chené (Glovo), etc.
  SELECT
    DATEADD('day',  6, DATE_TRUNC('week', CURRENT_DATE()))    AS cur_wk,
    DATEADD('week', -12, DATE_TRUNC('week', CURRENT_DATE()))  AS wk12
),
cjc AS (
  SELECT
    c."candidate_id",
    c."candidate_sourcer",
    j."job_id",
    j."job_recruiter",
    cl."client_name",
    CASE WHEN cl."client_name" IN ('SevenRooms','Doordash') THEN 'Wolt' ELSE cl."client_name" END AS client_wolt_group,
    TRY_TO_DATE(j."date_created")      AS job_date_created,
    TRY_TO_DATE(j."date_first_hired")  AS job_date_first_hired,
    LOWER(NULLIF(j."is_job_archived",'')) AS job_archived,
    LOWER(NULLIF(j."test",''))            AS job_test,
    NULLIF(j."job_title",'')              AS job_title
  FROM "KEBOOLA_855"."out.c-reporting-v2"."candidate"  c
  LEFT JOIN "KEBOOLA_855"."out.c-reporting-v2"."job"     j  ON j."job_id"    = c."job_id"
  LEFT JOIN "KEBOOLA_855"."out.c-reporting-v2"."client"  cl ON cl."client_id" = j."client_id"
  WHERE LOWER(NULLIF(c."is_candidate_archived",'')) <> 'true'
    AND j."job_title" IS NOT NULL AND j."job_title" <> ''
    AND cl."client_name" IS NOT NULL AND cl."client_name" <> ''
    AND cl."client_name" NOT IN ('Tribe.xyz','Kamila AI - TEST','Tribe.xyz (IR)')
    AND (LOWER(NULLIF(j."test",'')) <> 'true' OR j."test" IS NULL)
),
cs_dates AS (
  SELECT
    "candidate_id"                    AS candidate_id,
    TRY_TO_DATE("date_screen_actual") AS date_screen_actual,
    TRY_TO_DATE("date_interview")     AS date_interview,
    TRY_TO_DATE("date_hired")         AS date_hired
  FROM "KEBOOLA_855"."out.c-reporting-v2"."candidate_stage"
),
cand_eval AS (
  SELECT DISTINCT "candidate_id"
  FROM "KEBOOLA_855"."out.c-reporting-v2"."event"
  WHERE "event_type" = 'Evaluation'
),

-- Hired authoritative event per candidate (rn=1) -------------------
-- NOTE (2026-04-16): TA rows now emit RAW client_name (not client_wolt_group).
-- This lets downstream rollup split SevenRooms/Doordash→Wolt HQ from Wolt→sub-BU.
-- Required for Adelya Khakimova-style TAs with candidates across multiple Wolt sub-BUs.
hired_auth AS (
  SELECT
    cs.candidate_id,
    cs.date_hired,
    cjc."client_name" AS client_name,
    cjc.client_wolt_group,
    cjc."candidate_sourcer" AS candidate_sourcer,
    cjc.job_date_created,
    e."who_event_created_for" AS ta,
    ROW_NUMBER() OVER (PARTITION BY cs.candidate_id ORDER BY e."datetime_created" DESC) AS rn
  FROM cs_dates cs
  JOIN cjc ON cjc."candidate_id" = cs.candidate_id
  JOIN "KEBOOLA_855"."out.c-reporting-v2"."event" e
    ON e."candidate_id" = cs.candidate_id
   AND e."moved_to_stageType" = 'Hired'
   AND TRY_TO_DATE(e."date_created") = cs.date_hired
   AND e."who_event_created_for" IS NOT NULL AND e."who_event_created_for" <> ''
  WHERE cs.date_hired >= (SELECT wk12 FROM anchor)
    AND cs.date_hired <= (SELECT cur_wk FROM anchor)
),
hires_ta AS (
  SELECT 'TA'::VARCHAR AS role, 'hires'::VARCHAR AS metric, client_name AS client, ta AS who, COUNT(*)::NUMERIC AS val
  FROM hired_auth WHERE rn=1 GROUP BY 1,2,3,4
),
hires_ts AS (
  SELECT 'TS'::VARCHAR, 'hires'::VARCHAR, ''::VARCHAR AS client, candidate_sourcer AS who, COUNT(*)::NUMERIC
  FROM hired_auth WHERE rn=1 AND candidate_sourcer IS NOT NULL AND candidate_sourcer <> ''
    AND candidate_sourcer <> '-not available-'
  GROUP BY 1,2,3,4
),
ttf_ta_sum AS (
  SELECT 'TA'::VARCHAR, 'ttf_sum'::VARCHAR, client_name, ta, SUM(DATEDIFF('day', job_date_created, date_hired))::NUMERIC
  FROM hired_auth WHERE rn=1 AND job_date_created IS NOT NULL AND DATEDIFF('day', job_date_created, date_hired) >= 0
  GROUP BY 1,2,3,4
),
ttf_ta_cnt AS (
  SELECT 'TA'::VARCHAR, 'ttf_cnt'::VARCHAR, client_name, ta, COUNT(*)::NUMERIC
  FROM hired_auth WHERE rn=1 AND job_date_created IS NOT NULL AND DATEDIFF('day', job_date_created, date_hired) >= 0
  GROUP BY 1,2,3,4
),
ttf_ts_sum AS (
  SELECT 'TS'::VARCHAR, 'ttf_sum'::VARCHAR, ''::VARCHAR, candidate_sourcer, SUM(DATEDIFF('day', job_date_created, date_hired))::NUMERIC
  FROM hired_auth WHERE rn=1 AND candidate_sourcer IS NOT NULL AND candidate_sourcer <> '' AND candidate_sourcer <> '-not available-'
    AND job_date_created IS NOT NULL AND DATEDIFF('day', job_date_created, date_hired) >= 0
  GROUP BY 1,2,3,4
),
ttf_ts_cnt AS (
  SELECT 'TS'::VARCHAR, 'ttf_cnt'::VARCHAR, ''::VARCHAR, candidate_sourcer, COUNT(*)::NUMERIC
  FROM hired_auth WHERE rn=1 AND candidate_sourcer IS NOT NULL AND candidate_sourcer <> '' AND candidate_sourcer <> '-not available-'
    AND job_date_created IS NOT NULL AND DATEDIFF('day', job_date_created, date_hired) >= 0
  GROUP BY 1,2,3,4
),

-- Actual screens ------------------------------------------------------
-- TA attribution (2026-04-20 revision): event.who_event_created_for on the
--   latest Evaluation event per (candidate, screen-date). PBI Last 12w
--   Screens validates exactly: Aviv Lejla 168 (PBI 169), Anna 154 exact,
--   Jovana 41 exact, Kristina 129 (PBI 131), Wladyslaw 141 (PBI 142);
--   Wolt Nenad 121 exact. The earlier job.job_recruiter attribution was
--   off by -44 for Lejla and +35 for Jovana because it credited the job-
--   assigned TA rather than whoever actually ran the screen.
-- TS attribution: candidate.candidate_sourcer (unchanged).
evaluation_auth AS (
  SELECT
    cs.candidate_id,
    cs.date_screen_actual,
    cjc."client_name"              AS client_name,
    cjc."candidate_sourcer"        AS candidate_sourcer,
    TRIM(e."who_event_created_for") AS ta,
    ROW_NUMBER() OVER (
      PARTITION BY cs.candidate_id, cs.date_screen_actual
      ORDER BY e."datetime_created" DESC
    ) AS rn
  FROM cs_dates cs
  JOIN cjc ON cjc."candidate_id" = cs.candidate_id
  JOIN "KEBOOLA_855"."out.c-reporting-v2"."event" e
    ON e."candidate_id" = cs.candidate_id
   AND e."event_type"   = 'Evaluation'
   AND e."who_event_created_for" IS NOT NULL AND e."who_event_created_for" <> ''
  WHERE cs.date_screen_actual IS NOT NULL
    AND cs.date_screen_actual >= (SELECT wk12 FROM anchor)
    AND cs.date_screen_actual <= (SELECT cur_wk FROM anchor)
),
screens_ta AS (
  SELECT 'TA'::VARCHAR, 'screens'::VARCHAR, client_name, ta, COUNT(*)::NUMERIC
  FROM evaluation_auth WHERE rn = 1 AND ta IS NOT NULL AND ta <> ''
  GROUP BY 1,2,3,4
),
screens_ts AS (
  SELECT 'TS'::VARCHAR, 'screens'::VARCHAR, ''::VARCHAR, candidate_sourcer, COUNT(*)::NUMERIC
  FROM evaluation_auth
  WHERE rn = 1 AND candidate_sourcer IS NOT NULL AND candidate_sourcer <> ''
    AND candidate_sourcer <> '-not available-'
  GROUP BY 1,2,3,4
),

-- Moved to ATS -------------------------------------------------------
-- TA attribution (2026-04-20 revision): event.who_event_created_for on the
--   latest Interview/Offsite event per (candidate, interview-date). Match
--   PBI's event-based Candidate attribution for the Moved-to-ATS column.
-- TS attribution: candidate.candidate_sourcer on the same event.
ats_auth AS (
  SELECT
    cs.candidate_id,
    cs.date_interview,
    cjc."client_name"              AS client_name,
    cjc."candidate_sourcer"        AS candidate_sourcer,
    TRIM(e."who_event_created_for") AS ta,
    ROW_NUMBER() OVER (
      PARTITION BY cs.candidate_id, cs.date_interview
      ORDER BY e."datetime_created" DESC
    ) AS rn
  FROM cs_dates cs
  JOIN cjc ON cjc."candidate_id" = cs.candidate_id
  JOIN "KEBOOLA_855"."out.c-reporting-v2"."event" e
    ON e."candidate_id" = cs.candidate_id
   AND e."moved_to_stageType" IN ('Interview','Offsite')
   AND e."who_event_created_for" IS NOT NULL AND e."who_event_created_for" <> ''
  WHERE cs.date_interview IS NOT NULL
    AND cs.date_interview >= (SELECT wk12 FROM anchor)
    AND cs.date_interview <= (SELECT cur_wk FROM anchor)
),
ats_ta AS (
  SELECT 'TA'::VARCHAR, 'ats'::VARCHAR, client_name, ta, COUNT(*)::NUMERIC
  FROM ats_auth WHERE rn = 1 AND ta IS NOT NULL AND ta <> ''
  GROUP BY 1,2,3,4
),
ats_ts AS (
  SELECT 'TS'::VARCHAR, 'ats'::VARCHAR, ''::VARCHAR, candidate_sourcer, COUNT(*)::NUMERIC
  FROM ats_auth
  WHERE rn = 1 AND candidate_sourcer IS NOT NULL AND candidate_sourcer <> ''
    AND candidate_sourcer <> '-not available-'
  GROUP BY 1,2,3,4
),

-- Jobs active >= 60 days (regardless of first_hired) ------------------
-- Live dashboard counts ALL active 60d+ jobs per (client,TA), including
-- those with a first hire AND jobs that have no candidates at all.
-- Reads directly from job table (no candidate-join filter) to match live.
-- Empirical: matches live exactly for Adelya(16), Alisa(10), Jaksa(4),
-- Rafael(11), Etienne(16), Andreas Weins/Nexi (17), etc.
jobs_60d_base AS (
  SELECT
    cl."client_name"        AS client_name,
    j."job_recruiter"       AS ta,
    j."job_id"              AS job_id
  FROM "KEBOOLA_855"."out.c-reporting-v2"."job"    j
  JOIN "KEBOOLA_855"."out.c-reporting-v2"."client" cl ON cl."client_id" = j."client_id"
  WHERE LOWER(NULLIF(j."is_job_archived",'')) <> 'true'
    AND j."job_title" IS NOT NULL AND j."job_title" <> ''
    AND (LOWER(NULLIF(j."test",'')) <> 'true' OR j."test" IS NULL)
    AND cl."client_name" IS NOT NULL AND cl."client_name" <> ''
    AND cl."client_name" NOT IN ('Tribe.xyz','Kamila AI - TEST','Tribe.xyz (IR)')
    AND DATEDIFF('day', TRY_TO_DATE(j."date_created"), CURRENT_DATE()) >= 60
    AND j."job_recruiter" IS NOT NULL AND j."job_recruiter" <> ''
),
jobs_60d_ta AS (
  SELECT 'TA'::VARCHAR, 'jobs_60d'::VARCHAR, client_name, ta, COUNT(DISTINCT job_id)::NUMERIC
  FROM jobs_60d_base GROUP BY 1,2,3,4
)

SELECT * FROM hires_ta
UNION ALL SELECT * FROM hires_ts
UNION ALL SELECT * FROM ttf_ta_sum
UNION ALL SELECT * FROM ttf_ta_cnt
UNION ALL SELECT * FROM ttf_ts_sum
UNION ALL SELECT * FROM ttf_ts_cnt
UNION ALL SELECT * FROM screens_ta
UNION ALL SELECT * FROM screens_ts
UNION ALL SELECT * FROM ats_ta
UNION ALL SELECT * FROM ats_ts
UNION ALL SELECT * FROM jobs_60d_ta
