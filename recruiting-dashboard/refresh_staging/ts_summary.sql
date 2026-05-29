-- ============================================================================
-- ts_summary.sql — KPI - TS Summary per-sourcer × per-week aggregate
-- ============================================================================
-- Replicates Andy's PBI "KPI - TS Summary" page (legacy-pbix Bucket A).
--
-- Attribution:  event.who_created_event_first  (per-event; PBI Visual 21 row dim)
-- Page filters (5, all from PBIP page.json filterConfig):
--   1. Calendar.Year >= 2022 (default 2026 in snapshot)
--   2. is_job_archived = False
--   3. job.test ≠ True (and NULL → kept; 'true' lowercase comparison)
--   4. client_name NOT IN test-client list
--   5. who_created_event_first IN Current_TS roster (Google Sheet "TS Weekly")
--
-- Validated 2026-04-27 vs PBI snapshot data (2).xlsx (Apr 24, full 2026):
--   11/11 sourcers within 10% drift. Total: +3.5% (498 contacted), explained
--   by 3 days of fresh data between PBI snapshot (Apr 24) and our refresh.
--
-- TODO post-MVP:
--   - Read Current_TS roster from in.c-wbr-sheet.wbr_ts_weekly latest week
--     (currently hardcoded). Andy adds/removes sourcers via the Google Sheet.
--   - Add reason_not_interested + Current Stage matrix for the
--     disqualification visual (Visual 16 in legacy PBIP).
--
-- Outputs:  out.c-WBRMBR-weekly-aggregations.ts_summary_per_sourcer
--           Columns: TS, ISO_YEAR, ISO_WEEK, CONTACTED, POSITIVE_RESPONSE,
--                    SCREENS, ACTUAL_SCREENS, ATS, OFFERS, HIRES, JOBS
-- ============================================================================

CREATE TABLE "ts_summary_per_sourcer" AS
WITH current_ts AS (
  SELECT * FROM (VALUES
    ('Andrea Akovic'),('Elena Petrovska'),('Gustavo Loureiro Castro'),
    ('Jovana Drakula'),('Marina Lazarevic'),('Mia Gjorgievska'),
    ('Milica Veselinovic'),('Naledi Ngwenya'),('Nare Avetisyan'),
    ('Rodrigo Gomes'),('Valeriia Yurykova'),('Zelimir Stajcic')
  ) AS t(ts)
), test_clients AS (
  SELECT * FROM (VALUES
    ('fakecompany'),('Lilit - Test Partner Sales'),
    ('Lilit Test - Technical content creator'),('Test - Lilit HR admin'),
    ('Test by Nare'),('Test Client Alex'),('Test company by Iryna'),
    ('Test Iryna'),('Test Marko New Client'),('Test_Lilit'),
    ('testclient'),('TestClientMelani'),('Kamila AI - TEST')
  ) AS t(client_name)
), good_jobs AS (
  SELECT j."job_id", j."job_category"
  FROM "KEBOOLA_855"."out.c-reporting-v2"."job" j
  JOIN "KEBOOLA_855"."out.c-reporting-v2"."client" cl ON cl."client_id" = j."client_id"
  WHERE LOWER(NULLIF(j."is_job_archived",'')) <> 'true'
    AND (LOWER(NULLIF(j."test",'')) <> 'true' OR j."test" IS NULL)
    AND j."job_title" IS NOT NULL AND j."job_title" <> ''
    AND cl."client_name" NOT IN (SELECT client_name FROM test_clients)
), pairs AS (
  SELECT DISTINCT e."candidate_id", e."who_created_event_first" AS ts
  FROM "KEBOOLA_855"."out.c-reporting-v2"."event" e
  WHERE e."who_created_event_first" IN (SELECT ts FROM current_ts)
    AND e."job_id" IN (SELECT "job_id" FROM good_jobs)
), eval_ev AS (
  SELECT DISTINCT "candidate_id"
  FROM "KEBOOLA_855"."out.c-reporting-v2"."event"
  WHERE "event_type"='Evaluation'
), pos_ev AS (
  SELECT "candidate_id", MIN(TRY_TO_DATE("date_created")) AS pr_date
  FROM "KEBOOLA_855"."out.c-reporting-v2"."event"
  WHERE "moved_to_stageType"='Positive Response'
    AND TRY_TO_DATE("date_created") >= DATE '2025-04-14'
  GROUP BY "candidate_id"
), rs_ev AS (
  SELECT DISTINCT "candidate_id"
  FROM "KEBOOLA_855"."out.c-reporting-v2"."event"
  WHERE "event_type"='Moved to stage' AND "moved_to_stage"='Recruiter Screen'
), ats_ev AS (
  SELECT DISTINCT "candidate_id"
  FROM "KEBOOLA_855"."out.c-reporting-v2"."event"
  WHERE "moved_to_stage"='Moved to ATS'
), cs AS (
  SELECT cs."candidate_id",
    TRY_TO_DATE(cs."date_contacted")     AS dc,
    TRY_TO_DATE(cs."date_screen")        AS dsr,
    TRY_TO_DATE(cs."date_screen_actual") AS dsa,
    TRY_TO_DATE(cs."date_interview")     AS di,
    TRY_TO_DATE(cs."date_offer")         AS doff,
    TRY_TO_DATE(cs."date_hired")         AS dh
  FROM "KEBOOLA_855"."out.c-reporting-v2"."candidate_stage" cs
), cand_jobs AS (
  -- Track distinct jobs per (sourcer, year, week) for the Jobs column
  SELECT DISTINCT p.ts, c."job_id",
    YEAROFWEEKISO(cs.dc) AS iso_year, WEEKISO(cs.dc) AS iso_week
  FROM pairs p
  JOIN "KEBOOLA_855"."out.c-reporting-v2"."candidate" c ON c."candidate_id"=p."candidate_id"
  JOIN cs ON cs."candidate_id"=p."candidate_id"
  WHERE cs.dc IS NOT NULL AND YEAROFWEEKISO(cs.dc) >= 2024
), contacted AS (
  SELECT p.ts, YEAROFWEEKISO(cs.dc) AS iso_year, WEEKISO(cs.dc) AS iso_week,
         COUNT(DISTINCT cs."candidate_id") AS contacted
  FROM pairs p JOIN cs ON cs."candidate_id"=p."candidate_id"
  WHERE cs.dc IS NOT NULL AND YEAROFWEEKISO(cs.dc) >= 2024
  GROUP BY 1,2,3
), pr AS (
  SELECT p.ts, YEAROFWEEKISO(pe.pr_date) AS iso_year, WEEKISO(pe.pr_date) AS iso_week,
         COUNT(DISTINCT pe."candidate_id") AS positive_response
  FROM pairs p JOIN pos_ev pe ON pe."candidate_id"=p."candidate_id"
  WHERE pe.pr_date IS NOT NULL AND YEAROFWEEKISO(pe.pr_date) >= 2024
  GROUP BY 1,2,3
), screens AS (
  SELECT p.ts, YEAROFWEEKISO(cs.dsr) AS iso_year, WEEKISO(cs.dsr) AS iso_week,
         COUNT(DISTINCT cs."candidate_id") AS screens
  FROM pairs p JOIN cs ON cs."candidate_id"=p."candidate_id"
  JOIN rs_ev rs ON rs."candidate_id"=p."candidate_id"
  WHERE cs.dsr IS NOT NULL AND YEAROFWEEKISO(cs.dsr) >= 2024
  GROUP BY 1,2,3
), actual_screens AS (
  SELECT p.ts, YEAROFWEEKISO(cs.dsa) AS iso_year, WEEKISO(cs.dsa) AS iso_week,
         COUNT(DISTINCT cs."candidate_id") AS actual_screens
  FROM pairs p JOIN cs ON cs."candidate_id"=p."candidate_id"
  JOIN eval_ev ee ON ee."candidate_id"=p."candidate_id"
  WHERE cs.dsa IS NOT NULL AND YEAROFWEEKISO(cs.dsa) >= 2024
  GROUP BY 1,2,3
), ats_ AS (
  SELECT p.ts, YEAROFWEEKISO(cs.di) AS iso_year, WEEKISO(cs.di) AS iso_week,
         COUNT(DISTINCT cs."candidate_id") AS ats
  FROM pairs p JOIN cs ON cs."candidate_id"=p."candidate_id"
  JOIN ats_ev ate ON ate."candidate_id"=p."candidate_id"
  WHERE cs.di IS NOT NULL AND YEAROFWEEKISO(cs.di) >= 2024
  GROUP BY 1,2,3
), offers AS (
  SELECT p.ts, YEAROFWEEKISO(cs.doff) AS iso_year, WEEKISO(cs.doff) AS iso_week,
         COUNT(DISTINCT cs."candidate_id") AS offers
  FROM pairs p JOIN cs ON cs."candidate_id"=p."candidate_id"
  WHERE cs.doff IS NOT NULL AND YEAROFWEEKISO(cs.doff) >= 2024
  GROUP BY 1,2,3
), hires AS (
  SELECT p.ts, YEAROFWEEKISO(cs.dh) AS iso_year, WEEKISO(cs.dh) AS iso_week,
         COUNT(DISTINCT cs."candidate_id") AS hires
  FROM pairs p JOIN cs ON cs."candidate_id"=p."candidate_id"
  WHERE cs.dh IS NOT NULL AND YEAROFWEEKISO(cs.dh) >= 2024
  GROUP BY 1,2,3
), jobs_per_wk AS (
  SELECT ts, iso_year, iso_week, COUNT(DISTINCT "job_id") AS jobs
  FROM cand_jobs GROUP BY 1,2,3
), all_keys AS (
  SELECT ts, iso_year, iso_week FROM contacted
  UNION SELECT ts, iso_year, iso_week FROM pr
  UNION SELECT ts, iso_year, iso_week FROM screens
  UNION SELECT ts, iso_year, iso_week FROM actual_screens
  UNION SELECT ts, iso_year, iso_week FROM ats_
  UNION SELECT ts, iso_year, iso_week FROM offers
  UNION SELECT ts, iso_year, iso_week FROM hires
)
SELECT
  k.ts                            AS "TS",
  k.iso_year                      AS "ISO_YEAR",
  k.iso_week                      AS "ISO_WEEK",
  COALESCE(c.contacted, 0)        AS "CONTACTED",
  COALESCE(pr.positive_response,0) AS "POSITIVE_RESPONSE",
  COALESCE(s.screens, 0)          AS "SCREENS",
  COALESCE(asc_.actual_screens,0) AS "ACTUAL_SCREENS",
  COALESCE(at.ats, 0)             AS "ATS",
  COALESCE(o.offers, 0)           AS "OFFERS",
  COALESCE(h.hires, 0)            AS "HIRES",
  COALESCE(j.jobs, 0)             AS "JOBS"
FROM all_keys k
LEFT JOIN contacted      c   ON c.ts=k.ts AND c.iso_year=k.iso_year AND c.iso_week=k.iso_week
LEFT JOIN pr             pr  ON pr.ts=k.ts AND pr.iso_year=k.iso_year AND pr.iso_week=k.iso_week
LEFT JOIN screens        s   ON s.ts=k.ts AND s.iso_year=k.iso_year AND s.iso_week=k.iso_week
LEFT JOIN actual_screens asc_ ON asc_.ts=k.ts AND asc_.iso_year=k.iso_year AND asc_.iso_week=k.iso_week
LEFT JOIN ats_           at  ON at.ts=k.ts AND at.iso_year=k.iso_year AND at.iso_week=k.iso_week
LEFT JOIN offers         o   ON o.ts=k.ts AND o.iso_year=k.iso_year AND o.iso_week=k.iso_week
LEFT JOIN hires          h   ON h.ts=k.ts AND h.iso_year=k.iso_year AND h.iso_week=k.iso_week
LEFT JOIN jobs_per_wk    j   ON j.ts=k.ts AND j.iso_year=k.iso_year AND j.iso_week=k.iso_week
ORDER BY k.ts, k.iso_year, k.iso_week
