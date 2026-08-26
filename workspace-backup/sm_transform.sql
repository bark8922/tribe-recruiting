-- Silver Medalists — Keboola transformation
-- Output: out.c-SM.sm_candidates  (one row per silver-medalist candidate)
-- Validated against live data 2026-08-26. Returns 3 rows today (all retro-tagged tests).
--
-- Source of truth: Candidate.Sourcedsource = Silver Medalist
-- matched  = moved into the "Silver Medalists" stage (Prospects type)
--            falls back to Candidate.Created_Date if she accepted the default stage
-- intro    = first move to a Contacted-type stage
-- Everything below intro is the client's normal ladder, untouched.

CREATE OR REPLACE TABLE "sm_candidates" AS
WITH sm AS (
  SELECT
    c."bubbleinternal_id"                   AS candidate_id,
    c."Talent"                              AS talent_id,
    c."Job"                                 AS job_id,
    c."disqualified"                        AS disqualified,
    TO_TIMESTAMP_NTZ(c."Created_Date")      AS added_at
  FROM "Candidate" c
  WHERE c."Sourcedsource" = '1787139290725x775285588014012800'
),
ev AS (
  SELECT
    e."Candidate"                           AS candidate_id,
    st."stage_type_name"                    AS stage_type,
    s."stageName"                           AS stage_name,
    MIN(TO_TIMESTAMP_NTZ(e."Created_Date")) AS first_at
  FROM "Event" e
  JOIN "stages"     s  ON s."bubbleinternal_id"  = e."moved_to_stage"
  JOIN "stagesType" st ON st."bubbleinternal_id" = s."stagesType"
  WHERE e."Candidate" IN (SELECT candidate_id FROM sm)
  GROUP BY 1, 2, 3
),
agg AS (
  SELECT
    sm.candidate_id, sm.talent_id, sm.job_id, sm.added_at, sm.disqualified,
    MAX(CASE WHEN ev.stage_name = 'Silver Medalists' THEN ev.first_at END) AS matched_stage_at,
    MAX(CASE WHEN ev.stage_type = 'Contacted'        THEN ev.first_at END) AS intro_at,
    MAX(CASE WHEN ev.stage_type = 'Recruiter Screen' THEN ev.first_at END) AS screen_at,
    MAX(CASE WHEN ev.stage_type IN ('Final Interview','Interview 1','Interview 2','Interview 3')
             THEN ev.first_at END)                                          AS interview_at,
    MAX(CASE WHEN ev.stage_type = 'Offer'            THEN ev.first_at END) AS offer_at,
    MAX(CASE WHEN ev.stage_type = 'Hired'            THEN ev.first_at END) AS hired_at
  FROM sm
  LEFT JOIN ev ON ev.candidate_id = sm.candidate_id
  GROUP BY 1, 2, 3, 4, 5
)
SELECT
  a.candidate_id,
  t."Full_name"                                        AS candidate_name,
  t."linkedin"                                         AS linkedin,
  co."Name"                                            AS client,
  j."Title"                                            AS job_title,
  a.job_id,
  u."full_name"                                        AS recruiter,
  COALESCE(a.matched_stage_at, a.added_at)             AS matched_at,
  a.intro_at,
  a.screen_at,
  a.interview_at,
  a.offer_at,
  a.hired_at,
  a.disqualified,

  -- timings
  DATEDIFF('hour', COALESCE(a.matched_stage_at, a.added_at), a.intro_at) AS req_to_intro_hours,
  DATEDIFF('day',  a.intro_at, a.hired_at)                               AS intro_to_hire_days,

  -- data-quality flags, excluded from timing averages but NOT from counts
  --   synthetic intro: matched and intro landed within 10s, so Contacted was
  --   either the default landing stage or a backfill, not a real introduction
  CASE WHEN ABS(DATEDIFF('second', COALESCE(a.matched_stage_at, a.added_at), a.intro_at)) <= 10
       THEN 1 ELSE 0 END                                                 AS flag_intro_synthetic,
  --   introduced but never recorded: reached screen or beyond with no Contacted event
  CASE WHEN a.intro_at IS NULL AND COALESCE(a.screen_at, a.interview_at, a.offer_at, a.hired_at) IS NOT NULL
       THEN 1 ELSE 0 END                                                 AS flag_intro_missing,
  --   she used the default stage instead of Silver Medalists
  CASE WHEN a.matched_stage_at IS NULL THEN 1 ELSE 0 END                 AS flag_no_matched_stage,

  TO_VARCHAR(COALESCE(a.matched_stage_at, a.added_at), 'YYYY-"W"WW')     AS matched_week,
  TO_VARCHAR(a.intro_at, 'YYYY-"W"WW')                                   AS intro_week
FROM agg a
LEFT JOIN "Talent"  t  ON t."bubbleinternal_id"  = a.talent_id
LEFT JOIN "Jobs"    j  ON j."bubbleinternal_id"  = a.job_id
LEFT JOIN "Company" co ON co."bubbleinternal_id" = j."Company"
LEFT JOIN "User"    u  ON u."bubbleinternal_id"  = j."recruiter_responsible"
WHERE COALESCE(co."Name",'') <> 'Bubble test';
