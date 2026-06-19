-- ============================================================================
-- wbr_view.sql — Tribe.xyz Weekly Business Review (WBR) materializations
-- ----------------------------------------------------------------------------
-- Purpose: Reproduce Power BI's `WBR TA Actual`, `WBR TS Actual`, and
--          `WBR Client History` calculated tables directly against
--          `out.c-reporting-v2` so the April MVP dashboard (VBR + Project) can
--          ditch Power BI without regressing the numbers Andy's DAX produced.
--
-- Dialect: Snowflake (KEBOOLA_855_942138244.READER_SCHEMA_855_942138244)
-- Source tables used:
--   - job, client, candidate, candidate_stage, event
-- Static sources (all now exported to ./wbr_static/*.csv in the workspace):
--   - wbr_ta_target.csv          (from Andy's "TA Target" tab, 1,540 rows)
--       Grain: Client × TA × Year × Month
--       Cols:  contacted, actual_screens, moved_to_ats, hires
--   - wbr_ts_weekly.csv          (from Andy's "TS Weekly Note" tab, 2,989 rows)
--       Grain: TS × Year × Week, incl. contacted_target + comment + reasoning
--   - sourcing_team_list.csv     (Gustavo's "Levels" tab, 126 rows)
--       Grain: one row per person  (lead, role, name, kpi_dashboard, archived, current_level)
--       Purpose: canonical TS/TA roster + dashboard-slice membership
--       (replaces Andy's "Sourcing Team List" static table in Power BI)
--   - sourcer_ta_transitions.csv (Gustavo's sourcer→TA move dates, 72 rows)
--       Cols: name, employee_number, moved_to_ta_date
--       Purpose: when a week's data needs to classify someone as TA or TS,
--       pre/post the move date, without waiting for BambooHR.
--   - bamboohr_roster_current.csv     (75 rows, pulled 2026-04-08)
--       Grain: one row per active employee
--       Cols:  employee_id, name, job_title, department, division, location,
--              email, supervisor   (CURRENT state — for today's WBR week)
--   - bamboohr_supervisor_history.csv (833 rows, 238 unique employees,
--       pulled via BambooHR custom report 227 "Report_to_historical" 2026-04-08)
--       Grain: one row per (employee × job-info effective date)
--       Cols:  employee_id, employee_number, name, effective_date,
--              reports_to, job_title, division
--       Use:   as-of lookup for the leadership-chain cut on historical WBR
--              weeks (pandas: sort by effective_date asc, forward-fill per
--              employee_id, then left-join onto event_date).
--
-- Validation status (2026-04-08 v2, post-transcript reconciliation):
--
--   Andy's 6-point brain dump (from 2026-04-07 call) applied in this rewrite:
--     1. Credit sourcer = first person to CONTACT the candidate (not
--        job.official_sourcer). Implemented via event.who_created_event_first.
--     2. Current stage = source of truth. For TA stage counts we now read
--        the authoritative candidate_stage.date_* columns and only attribute
--        the stage move to the event whose date matches the authoritative
--        date. This filters out forward-then-back "mistake" moves.
--     3. First hired candidate's hire+contact dates per job → Project Dashboard
--        (still pending, NOT in this file).
--     4. Leadership view = weekly numbers per TA + credit sourcer (done).
--     5. BambooHR historical report_to (pulled today, merged in pandas layer).
--     6. Employee number sync issue — flag only (join happens by name/email).
--
--   Filters now applied across all CTEs (Andy 2026-04-07 @L713-720):
--     - job.test        <> 'true'
--     - job.is_job_archived <> 'true'
--     - client.test     <> 'true' and client_name not in Tribe/Kamila test list
--     - candidate.is_candidate_archived <> 'true'
--     - TA slice also excludes job.is_external_recruiter = 'true'
--       (Andy @L808-815: "by default I do not show that for any measurement.
--       But for sourcer they want to know."). TS slice keeps externals.
--
--   TODO VERIFY WITH FRANTISEK (Friday call):
--     * Sourced semantics — I currently use event_type='Candidate created'.
--       Andy's transcript is ambiguous: he talks about "sourced already, but
--       hasn't contacted" (Prospect stage) AND "whoever contact the person
--       is the owner credit". The question is whether WBR "sourced" column
--       counts "Candidate created" events or "first move to Contacted".
--       See `-- !! VERIFY FRANTISEK` in ts_fact_raw.
--     * Event-level Contacted double-count — if a candidate is moved
--       Contacted→Screen→Contacted (mistake revert) the raw event stream
--       records 2 "contacted" events. Andy's DAX `# events - contacted`
--       counts events so it would report 2. Does that match his intent or
--       should we de-dupe to 1 per candidate?
--
-- Gotchas baked in (per sanity check 2026-04-08):
--   - booleans are stored as lowercase TEXT ('true'/'false'), not BOOLEAN.
--     Compare with LOWER(NULLIF(col,'')) <> 'true' to survive NULL / blank.
--   - date columns are stored as TEXT — use TRY_TO_DATE() everywhere.
--   - WBR year floor is 2024 (matches Andy's DAX).
--
-- Pre-rewrite smoke test (2026-04-08 v1, raw-event TA side, no filters):
--   ~2–4 k contacted events/week, ~280 screens/wk, ~200 actual screens/wk,
--   ~150 ATS/wk, ~60 offers/wk, ~30–77 hires/wk.
--
-- Post-rewrite smoke test (2026-04-08 v2, candidate_stage-gated TA slice,
-- last 6 ISO weeks, external recruiters excluded):
--   wk       contacted  screens  actual  ats  offers  hires  TAs
--   2026-15  1,791      144      83      49    1      1      22
--   2026-14  3,081      237     146      90    4      3      25
--   2026-13  2,047      161     146      87    5      6      24
--   2026-12  1,996      147     114      69    8      3      23
--   2026-11  2,432      264     179      99    8      4      24
--   2026-10  2,823      239     192     136    8      4      25
--   2026-09  1,598      174     155      93    6      5      24
-- Cross-check: ran a diagnostic `SELECT ... FROM candidate_stage WHERE
--   TRY_TO_DATE(date_hired) IS NOT NULL` with the same base filters and got
--   identical hires per week (1,3,6,3,4,4,5). This confirms the authoritative-
--   date filter in ta_stage_moves is working: v1's 30-77/wk was inflated by
--   counting reverted/mistake "move to Hired" events that Andy's stage logic
--   says to discard. v2 matches the candidate_stage source-of-truth count.
-- Contacted / screens / ATS still need Andy-sheet reconciliation but are in
-- a plausible range vs Power BI screenshots.
--
-- Output:
--   Two SELECT statements at the bottom, labelled WBR_TA_ACTUAL and
--   WBR_TS_ACTUAL, meant to be run independently by the pipeline and dumped
--   to JSON for the dashboard. A third block (WBR_CLIENT_HISTORY) rolls
--   WBR_TA_ACTUAL up to client × week for the heat-map tile.
-- ============================================================================


-- ---------- 0. Shared base filters ------------------------------------------
-- Power BI filter set + Andy's Tuesday-call rules, centralised so every
-- downstream CTE inherits them consistently.
--   - job.test                   <> 'true'           (Andy @L720)
--   - job.is_job_archived        <> 'true'           (Andy @L720, "you don't count that")
--   - client.test                <> 'true'
--   - client_name NOT IN test companies
--   - candidate.is_candidate_archived <> 'true'      (Andy @L713-716)
-- NOTE: is_external_recruiter is CARRIED through but NOT filtered here. The
-- TA slice filters it out downstream; the TS slice keeps external recruiters
-- (Andy @L808-815: "for sourcer they want to know").
WITH base_job AS (
    SELECT
        j."job_id",
        j."client_id",
        j."job_title",
        j."job_recruiter",
        j."job_sourcer",
        TRY_TO_DATE(j."date_first_hired") AS date_first_hired,
        LOWER(NULLIF(j."is_external_recruiter", '')) = 'true' AS is_external_recruiter
    FROM "job" j
    WHERE LOWER(NULLIF(j."test",            '')) <> 'true'
      AND LOWER(NULLIF(j."is_job_archived", '')) <> 'true'
      AND COALESCE(j."job_title", '')            <> ''
),
base_client AS (
    SELECT
        c."client_id",
        c."client_name"
    FROM "client" c
    WHERE LOWER(NULLIF(c."test", ''))            <> 'true'
      AND COALESCE(c."client_name", '')          <> ''
      AND c."client_name" NOT IN ('Tribe.xyz','Kamila AI - TEST')
),
base_candidate AS (
    SELECT
        cd."candidate_id",
        cd."job_id",
        cd."talent_id",
        cd."candidate_sourcer"
    FROM "candidate" cd
    WHERE LOWER(NULLIF(cd."is_candidate_archived", '')) <> 'true'
),
-- Join spine used by every downstream CTE. Each row is one (candidate, job,
-- client) that survives the base filters. `is_external_recruiter` is carried
-- here so the TA slice can exclude externals while TS keeps them.
cjc AS (
    SELECT
        cd."candidate_id",
        cd."talent_id",
        j."job_id",
        j."job_title",
        j.is_external_recruiter,
        c."client_id",
        c."client_name"
    FROM base_candidate cd
    JOIN base_job     j ON j."job_id"    = cd."job_id"
    JOIN base_client  c ON c."client_id" = j."client_id"
),

-- Shared authoritative stage dates from `candidate_stage`. These are Andy's
-- backfilled columns (REPORTING_V2_ORIGINS.md), so NULL = "this candidate
-- never actually reached that stage" (or was reverted). Used by BOTH the TA
-- authoritative-date match and the TS candidate-level metrics below.
candidate_stage_facts AS (
    SELECT
        cs."candidate_id",
        TRY_TO_DATE(cs."date_lnkdin_viewed")  AS date_lnkdin_viewed,
        TRY_TO_DATE(cs."date_contacted")      AS date_contacted,
        TRY_TO_DATE(cs."date_screen")         AS date_screen,
        TRY_TO_DATE(cs."date_screen_actual")  AS date_screen_actual,
        TRY_TO_DATE(cs."date_interview")      AS date_interview,
        TRY_TO_DATE(cs."date_offer")          AS date_offer,
        TRY_TO_DATE(cs."date_hired")          AS date_hired
    FROM "candidate_stage" cs
    JOIN cjc ON cjc."candidate_id" = cs."candidate_id"
),


-- ---------- 1. Calendar helpers ---------------------------------------------
-- Use ISO weeks (Mon–Sun). Power BI's `WeekInt` column is YYYY*100+WW — we
-- reproduce that as an INT for easy range filters (e.g. last 12 weeks).
calendar_anchor AS (
    SELECT
        CURRENT_DATE() AS today,
        DATE_TRUNC('week', CURRENT_DATE())                         AS current_week_start,
        DATEADD('week', -12, DATE_TRUNC('week', CURRENT_DATE()))   AS twelve_weeks_ago,
        DATEADD('week',  -4, DATE_TRUNC('week', CURRENT_DATE()))   AS four_weeks_ago
),


-- ---------- 2. Event-level TA: contacted ------------------------------------
-- Matches the DAX measure `[# events - contacted (date created)]`. EVENT
-- count, not candidate count. TA slice excludes external-recruiter jobs.
--
-- !! KNOWN GAP !! If a candidate is moved Contacted→Screen→Contacted (revert),
-- this counts 2 contacted events. Andy's DAX `# events` would also count 2,
-- so we match. Verify with Frantisek on Friday whether that's desired.
ta_contacted_events AS (
    SELECT
        e."who_event_created_for"                        AS ta,
        cjc."client_id",
        cjc."client_name",
        TRY_TO_DATE(e."date_created")                    AS event_date,
        e."event_id",
        e."candidate_id",
        cjc."job_id"
    FROM "event" e
    JOIN cjc ON cjc."candidate_id" = e."candidate_id"
    WHERE e."moved_to_stageType" = 'Contacted'
      AND COALESCE(e."moved_to_stage", '') <> 'Responded'
      AND COALESCE(e."who_event_created_for", '') <> ''
      AND TRY_TO_DATE(e."date_created") >= DATE '2024-01-01'
      AND NOT cjc.is_external_recruiter      -- TA slice excludes externals
),


-- ---------- 3. Candidate-stage-driven TA metrics ----------------------------
-- Andy (Tuesday call @L482-488, L728-729): "the key is from the current stage.
-- So if person like current stage contact you should not have any date in the
-- future." Implementation:
--   1. Take each candidate's authoritative stage date from candidate_stage
--      (already stage_current_num-gated + cascading-backfilled upstream).
--   2. For attribution, find the event whose moved_to_stageType matches the
--      stage and whose date_created = authoritative date. That's the event
--      that truly caused the stage to stick. Mistake/revert events land on
--      a different date and are discarded.
--   3. Dedupe to one event per (candidate × stage) via ROW_NUMBER() —
--      latest datetime_created on the authoritative date wins.
--
-- This is the v2 fix for Andy point #2 (2026-04-08). Previously read raw
-- events and over-counted reverted moves.
ta_stage_move_events AS (
    SELECT
        e."candidate_id",
        cjc."client_id",
        cjc."client_name",
        cjc."job_id",
        e."datetime_created",
        TRY_TO_DATE(e."date_created")                AS event_date,
        e."who_event_created_for"                    AS ta,
        CASE
            WHEN e."moved_to_stageType" = 'Recruiter Screen' THEN 'screen'
            WHEN e."event_type"         = 'Evaluation'       THEN 'screen_actual'
            WHEN e."moved_to_stageType" IN ('Offsite','Interview')
                 OR e."moved_to_stage" ILIKE '%interview%'   THEN 'ats'
            WHEN e."moved_to_stageType" = 'Offer'            THEN 'offer'
            WHEN e."moved_to_stageType" = 'Hired'            THEN 'hired'
            ELSE NULL
        END AS stage_bucket
    FROM "event" e
    JOIN cjc ON cjc."candidate_id" = e."candidate_id"
    WHERE COALESCE(e."who_event_created_for", '') <> ''
      AND TRY_TO_DATE(e."date_created") >= DATE '2024-01-01'
      AND NOT cjc.is_external_recruiter      -- TA slice excludes externals
),

ta_stage_moves AS (
    SELECT
        stage_bucket,
        ta,
        client_id,
        client_name,
        job_id,
        candidate_id,
        event_date
    FROM (
        SELECT
            me.*,
            CASE me.stage_bucket
                WHEN 'screen'        THEN s.date_screen
                WHEN 'screen_actual' THEN s.date_screen_actual
                WHEN 'ats'           THEN s.date_interview
                WHEN 'offer'         THEN s.date_offer
                WHEN 'hired'         THEN s.date_hired
            END AS authoritative_date,
            ROW_NUMBER() OVER (
                PARTITION BY me."candidate_id", me.stage_bucket
                ORDER BY me."datetime_created" DESC
            ) AS rn
        FROM ta_stage_move_events me
        JOIN candidate_stage_facts s USING ("candidate_id")
        WHERE me.stage_bucket IS NOT NULL
    ) x
    WHERE authoritative_date IS NOT NULL
      AND event_date = authoritative_date
      AND rn = 1
),


-- ---------- 4. Week-grain TA fact table -------------------------------------
-- Union contacted events + stage moves, pivot stage_bucket into columns.
ta_fact_raw AS (
    SELECT
        DATE_TRUNC('week', event_date)                                  AS week_start,
        DATEADD('day', 6, DATE_TRUNC('week', event_date))               AS week_end,
        YEAR(event_date)                                                AS year,
        YEAR(event_date) * 100 + WEEKISO(event_date)                    AS week_int,
        client_id,
        client_name,
        ta,
        SUM(CASE WHEN kind = 'contacted' THEN 1 ELSE 0 END)             AS contacted,
        COUNT(DISTINCT CASE WHEN kind = 'screen'        THEN candidate_id END) AS screens,
        COUNT(DISTINCT CASE WHEN kind = 'screen_actual' THEN candidate_id END) AS actual_screens,
        COUNT(DISTINCT CASE WHEN kind = 'ats'           THEN candidate_id END) AS moved_to_ats,
        COUNT(DISTINCT CASE WHEN kind = 'offer'         THEN candidate_id END) AS offers,
        COUNT(DISTINCT CASE WHEN kind = 'hired'         THEN candidate_id END) AS hires,
        COUNT(DISTINCT job_id)                                          AS num_jobs
    FROM (
        SELECT 'contacted' AS kind, ta, client_id, client_name,
               job_id, candidate_id, event_date
        FROM ta_contacted_events
        UNION ALL
        SELECT stage_bucket AS kind, ta, client_id, client_name,
               job_id, candidate_id, event_date
        FROM ta_stage_moves
    )
    WHERE event_date IS NOT NULL
    GROUP BY 1,2,3,4,5,6,7
),


-- ---------- 5. Attach monthly TA targets ------------------------------------
-- Andy's sheet stores targets at the month grain (Year × Month × Client × TA).
-- For a given week we take the target row of the month the week_start belongs
-- to. Target is duplicated across all ~4 weeks of that month — this matches
-- how Power BI's WBR TA Target table is joined to WBR TA Actual via Year,
-- Month, Client, TA.
--
-- NOTE: `wbr_ta_target` is loaded as a static Keboola table that mirrors the
-- sheet. Until it's wired up, this CTE will fail — swap to a VALUES() stub
-- during local testing.
ta_target AS (
    SELECT
        t."client"                AS client_name,
        t."ta"                    AS ta,
        CAST(t."year"  AS INTEGER) AS year,
        CAST(t."month" AS INTEGER) AS month,
        CAST(NULLIF(t."contacted",        '') AS NUMERIC(12,2)) AS contacted_target,
        CAST(NULLIF(t."actual_screens",   '') AS NUMERIC(12,2)) AS screens_target,
        CAST(NULLIF(t."moved_to_ats",     '') AS NUMERIC(12,2)) AS ats_target,
        CAST(NULLIF(t."hires",            '') AS NUMERIC(12,2)) AS hires_target
    FROM "wbr_ta_target" t
),


-- ---------- 6. WBR TA Actual ------------------------------------------------
-- Final TA slice: one row per (week, client, TA) with actuals and targets.
wbr_ta_actual AS (
    SELECT
        f.year,
        f.week_int,
        f.week_start,
        f.week_end,
        CASE
            WHEN f.week_start >= (SELECT twelve_weeks_ago FROM calendar_anchor)
             AND f.week_start <= (SELECT current_week_start FROM calendar_anchor)
            THEN 'Yes' ELSE 'No'
        END                                         AS is_last_12_weeks,
        CASE
            WHEN f.week_start >= (SELECT four_weeks_ago FROM calendar_anchor)
             AND f.week_start <= (SELECT current_week_start FROM calendar_anchor)
            THEN 'Yes' ELSE 'No'
        END                                         AS is_last_4_weeks,
        f.client_id,
        f.client_name,
        f.ta,
        f.num_jobs,
        f.contacted,
        f.screens,
        f.actual_screens,
        f.moved_to_ats,
        f.offers,
        f.hires,
        t.contacted_target,
        t.screens_target,
        t.ats_target,
        t.hires_target,
        -- % attainment (null-safe)
        DIV0(f.contacted,       t.contacted_target) AS pct_contacted,
        DIV0(f.actual_screens,  t.screens_target)   AS pct_actual_screens,
        DIV0(f.moved_to_ats,    t.ats_target)       AS pct_moved_to_ats
    FROM ta_fact_raw f
    LEFT JOIN ta_target t
      ON t.client_name = f.client_name
     AND t.ta          = f.ta
     AND t.year        = f.year
     AND t.month       = MONTH(f.week_start)
),


-- ---------- 7. WBR Client History (roll-up) ---------------------------------
-- Client × week summary, filtered to last 12 weeks. Drives the client-level
-- heat map tile from the legacy Power BI dashboard.
wbr_client_history AS (
    SELECT
        week_start,
        week_end,
        week_int,
        client_name,
        SUM(contacted)          AS contacted,
        SUM(actual_screens)     AS actual_screens,
        SUM(moved_to_ats)       AS moved_to_ats,
        SUM(contacted_target)   AS contacted_target,
        SUM(screens_target)     AS screens_target,
        SUM(ats_target)         AS ats_target
    FROM wbr_ta_actual
    WHERE is_last_12_weeks = 'Yes'
      AND client_name IS NOT NULL
    GROUP BY 1,2,3,4
),


-- ---------- 8. Sourcer (TS) fact --------------------------------------------
-- TS slice uses event.who_created_event_first = "credit sourcer" (first person
-- to touch the candidate).
--
-- Two distinct join paths, because some event types pre-date the candidate:
--   (a) Candidate-linked events: join event.candidate_id → cjc (for
--       contacted_evt, positive_response, Candidate created).
--   (b) Talent-level events: join event.job_id → job → client directly,
--       skipping candidate (for LinkedIn Visited Profile, which has no
--       candidate_id but has who_created_event_first via part_1's
--       talent-level fallback).
ts_events_cand AS (
    SELECT
        e."who_created_event_first"      AS ts,
        cjc."client_name",
        cjc."job_id",
        e."candidate_id",
        TRY_TO_DATE(e."date_created")    AS event_date,
        e."event_type",
        e."moved_to_stageType",
        e."moved_to_stage"
    FROM "event" e
    JOIN cjc ON cjc."candidate_id" = e."candidate_id"
    WHERE COALESCE(e."who_created_event_first", '') <> ''
      AND TRY_TO_DATE(e."date_created") >= DATE '2024-01-01'
),
-- Talent-level view events: join straight through job/client.
ts_events_talent AS (
    SELECT
        e."who_created_event_first"      AS ts,
        bc."client_name",
        bj."job_id",
        e."talent_id",
        TRY_TO_DATE(e."date_created")    AS event_date,
        e."event_type"
    FROM "event" e
    JOIN base_job    bj ON bj."job_id"    = e."job_id"
    JOIN base_client bc ON bc."client_id" = bj."client_id"
    WHERE COALESCE(e."who_created_event_first", '') <> ''
      AND e."event_type" = 'Linkedin Visited Profile'
      AND TRY_TO_DATE(e."date_created") >= DATE '2024-01-01'
),

-- (ts_candidate_stage was merged into the shared `candidate_stage_facts`
-- CTE above so TA and TS use identical authoritative dates.)

-- Each candidate has ONE credit sourcer (who_created_event_first), so pick
-- a distinct row per candidate.
credit_sourcer_per_candidate AS (
    SELECT DISTINCT
        ts,
        "candidate_id",
        "client_name",
        "job_id"
    FROM ts_events_cand
),

ts_fact_raw AS (
    SELECT
        DATE_TRUNC('week', d.event_date)                        AS week_start,
        DATEADD('day', 6, DATE_TRUNC('week', d.event_date))     AS week_end,
        YEAR(d.event_date)                                      AS year,
        YEAR(d.event_date) * 100 + WEEKISO(d.event_date)        AS week_int,
        d.ts,
        SUM(CASE WHEN d.kind = 'viewed'  THEN 1 ELSE 0 END)                    AS viewed,
        SUM(CASE WHEN d.kind = 'sourced' THEN 1 ELSE 0 END)                    AS sourced,
        SUM(CASE WHEN d.kind = 'contacted_evt' THEN 1 ELSE 0 END)              AS contacted_events,
        COUNT(DISTINCT CASE WHEN d.kind = 'cand_contacted' THEN d.candidate_id END) AS contacted,
        COUNT(DISTINCT CASE WHEN d.kind = 'positive_resp'  THEN d.candidate_id END) AS positive_response,
        COUNT(DISTINCT CASE WHEN d.kind = 'cand_screen'    THEN d.candidate_id END) AS screens,
        COUNT(DISTINCT CASE WHEN d.kind = 'cand_screen_a'  THEN d.candidate_id END) AS actual_screens,
        COUNT(DISTINCT CASE WHEN d.kind = 'cand_ats'       THEN d.candidate_id END) AS ats,
        COUNT(DISTINCT CASE WHEN d.kind = 'cand_hired'     THEN d.candidate_id END) AS hires,
        COUNT(DISTINCT d.job_id)                                               AS num_jobs
    FROM (
        -- Viewed: raw LinkedIn Visited Profile events via the talent-level
        -- join path. 24k/wk, 100% carry credit sourcer via part_1 fallback.
        SELECT ts, client_name, job_id,
               NULL AS candidate_id,
               event_date, 'viewed' AS kind
        FROM ts_events_talent
        WHERE event_type = 'Linkedin Visited Profile'
        UNION ALL
        -- Sourced: `Candidate created` event. 4.7k/wk, 99.9% carry credit
        -- sourcer and candidate_id.
        -- !! VERIFY FRANTISEK (Friday 2026-04-10): Andy's transcript talks
        -- about a distinct "Prospect" stage for candidates who are sourced
        -- but not yet contacted (@L689-694), AND says "whoever contact the
        -- person is the owner credit" (@L515). It's ambiguous whether WBR
        -- "sourced" counts Candidate-created events or first-move-to-Contacted
        -- events. Current choice: Candidate created (matches raw-event
        -- volumes Andy reports in the Power BI cards). Switch to Contacted
        -- if Frantisek says otherwise.
        SELECT ts, client_name, job_id, candidate_id, event_date, 'sourced'
        FROM ts_events_cand
        WHERE event_type = 'Candidate created'
        UNION ALL
        -- Event-level: Contacted (kept for parity with a legacy tile; primary
        -- DAX measure on TS is candidate-level below)
        SELECT ts, client_name, job_id, candidate_id, event_date, 'contacted_evt'
        FROM ts_events_cand
        WHERE moved_to_stageType = 'Contacted'
          AND COALESCE(moved_to_stage,'') <> 'Responded'
        UNION ALL
        -- Candidate-level: Positive Response
        SELECT ts, client_name, job_id, candidate_id, event_date, 'positive_resp'
        FROM ts_events_cand
        WHERE moved_to_stageType = 'Positive Response'
        UNION ALL
        -- Candidate-level rollouts driven by cs.date_* filtered to each TS's
        -- candidate set. Bucketed into the week that the stage-date falls in,
        -- NOT the week of the event that logged it.
        SELECT cs_ts.ts, cs_ts.client_name, cs_ts.job_id, cs_ts.candidate_id,
               s.date_contacted AS event_date, 'cand_contacted'
        FROM credit_sourcer_per_candidate cs_ts
        JOIN candidate_stage_facts s USING ("candidate_id")
        WHERE s.date_contacted IS NOT NULL
        UNION ALL
        SELECT cs_ts.ts, cs_ts.client_name, cs_ts.job_id, cs_ts.candidate_id,
               s.date_screen, 'cand_screen'
        FROM credit_sourcer_per_candidate cs_ts
        JOIN candidate_stage_facts s USING ("candidate_id")
        WHERE s.date_screen IS NOT NULL
        UNION ALL
        SELECT cs_ts.ts, cs_ts.client_name, cs_ts.job_id, cs_ts.candidate_id,
               s.date_screen_actual, 'cand_screen_a'
        FROM credit_sourcer_per_candidate cs_ts
        JOIN candidate_stage_facts s USING ("candidate_id")
        WHERE s.date_screen_actual IS NOT NULL
        UNION ALL
        SELECT cs_ts.ts, cs_ts.client_name, cs_ts.job_id, cs_ts.candidate_id,
               s.date_interview, 'cand_ats'
        FROM credit_sourcer_per_candidate cs_ts
        JOIN candidate_stage_facts s USING ("candidate_id")
        WHERE s.date_interview IS NOT NULL
        UNION ALL
        SELECT cs_ts.ts, cs_ts.client_name, cs_ts.job_id, cs_ts.candidate_id,
               s.date_hired, 'cand_hired'
        FROM credit_sourcer_per_candidate cs_ts
        JOIN candidate_stage_facts s USING ("candidate_id")
        WHERE s.date_hired IS NOT NULL
    ) d
    WHERE d.event_date IS NOT NULL
    GROUP BY 1,2,3,4,5
),


-- ---------- 9. TS weekly target ---------------------------------------------
-- The TS sheet stores a weekly contacted target per sourcer. Week format is
-- '2023W21 (15/5-21/5)' — we parse the year-week prefix to an INT for join.
ts_target AS (
    SELECT
        t."ts"                     AS ts,
        CAST(t."year" AS INTEGER)  AS year,
        -- Extract the '21' from '2023W21 (15/5-21/5)' and combine:
        CAST(t."year" AS INTEGER) * 100
          + TRY_CAST(REGEXP_SUBSTR(t."week", 'W([0-9]+)', 1, 1, 'e', 1) AS INTEGER)
                                   AS week_int,
        CAST(NULLIF(t."contacted_target", '') AS NUMERIC(12,2))
                                   AS contacted_target
    FROM "wbr_ts_weekly_note" t
),


-- ---------- 10. WBR TS Actual -----------------------------------------------
wbr_ts_actual AS (
    SELECT
        f.year,
        f.week_int,
        f.week_start,
        f.week_end,
        CASE
            WHEN f.week_start >= (SELECT twelve_weeks_ago FROM calendar_anchor)
             AND f.week_start <= (SELECT current_week_start FROM calendar_anchor)
            THEN 'Yes' ELSE 'No'
        END                                        AS is_last_12_weeks,
        CASE
            WHEN f.week_start >= (SELECT four_weeks_ago FROM calendar_anchor)
             AND f.week_start <= (SELECT current_week_start FROM calendar_anchor)
            THEN 'Yes' ELSE 'No'
        END                                        AS is_last_4_weeks,
        f.ts,
        f.num_jobs,
        f.viewed,
        f.sourced,
        f.contacted,
        f.positive_response,
        f.screens,
        f.actual_screens,
        f.ats,
        f.hires,
        t.contacted_target,
        DIV0(f.contacted, t.contacted_target) AS pct_contacted
    FROM ts_fact_raw f
    LEFT JOIN ts_target t
      ON t.ts        = f.ts
     AND t.week_int  = f.week_int
)


-- ============================================================================
-- OUTPUT 1 — WBR_TA_ACTUAL (default result for this file)
-- ============================================================================
SELECT * FROM wbr_ta_actual
ORDER BY year DESC, week_int DESC, client_name, ta;

-- ============================================================================
-- To get WBR_TS_ACTUAL instead, replace the final SELECT with:
--     SELECT * FROM wbr_ts_actual ORDER BY year DESC, week_int DESC, ts;
--
-- To get WBR_CLIENT_HISTORY instead:
--     SELECT * FROM wbr_client_history ORDER BY week_int DESC, client_name;
-- ============================================================================
