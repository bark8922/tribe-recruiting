-- ============================================================================
-- weekly_summary.sql — PBI "Weekly Progress" page, faithful port
-- ============================================================================
-- Reproduces Andy's PBI Weekly Progress page to the unit (validated 2026-05-27
-- vs exports for Andrea+Nare [TS] and Anna+Dušan [TA]; viewed/contacted/reacted/
-- screens/actual/ats all EXACT across 13 weeks each).
--
-- Output: out.c-WBRMBR-weekly-aggregations.weekly_summary
--   DIM_TYPE  ('company' | 'ta' | 'ts' | 'client')
--   DIM_VALUE (person name / client name / '' for company)
--   ISO_YEAR, ISO_WEEK,
--   VIEWED, CONTACTED, REACTED, POSITIVE_RESPONSE, REC_SCREENS,
--   ACTUAL_SCREENS, ATS, OFFERED, HIRED
--
-- Recipe (see memory canonical-sourcer-attribution-20260526):
--   * Archived JOBS included (no is_job_archived filter); archived CANDIDATES
--     excluded (is_candidate_archived=FALSE).
--   * Client filter: client_name NOT ILIKE '%test%' / '%fake%' (keeps Tribe.xyz).
--   * Page filter: who_event_created_for not blank; job_title not blank; job.test<>true.
--   * Attribution: TS = "Correct" blend (who_created_event for top-of-funnel events,
--     who_created_event_first for screen-onward); TA = who_event_created_for.
--   * contacted & reacted = LOOSE (candidate has ANY attributed event), by date_contacted.
--   * viewed & positive_response = stage-specific top-funnel event, by event date_created.
--   * screens/actual/ats/offered/hired = stage-specific (that stage's event), by the
--     candidate_stage stage date.
--   * reacted = PBI "Candidate Response": reason_not_interested (<>Unresponsive) OR
--     is_candidate_reacted OR stage_current past Contacted/Applied/Prospects.
-- ============================================================================
CREATE TABLE "weekly_summary" AS
WITH gj AS (
  SELECT j."job_id" jid, cl."client_name" client
  FROM "KEBOOLA_855"."out.c-reporting-v2"."job" j
  JOIN "KEBOOLA_855"."out.c-reporting-v2"."client" cl ON cl."client_id"=j."client_id"
  WHERE (LOWER(NULLIF(j."test",''))<>'true' OR j."test" IS NULL)
    AND j."job_title" IS NOT NULL AND j."job_title"<>''
    AND cl."client_name" NOT ILIKE '%test%' AND cl."client_name" NOT ILIKE '%fake%'
), na AS (
  SELECT "candidate_id" cid FROM "KEBOOLA_855"."out.c-reporting-v2"."candidate"
  WHERE COALESCE("is_candidate_archived",FALSE)=FALSE
), evb AS (
  SELECT e."candidate_id" cid, gj.client, e."event_type" et, e."moved_to_stage" ms,
         e."moved_to_stageType" mst, TRY_TO_DATE(e."date_created") edt,
         e."who_event_created_for" wecf,
         CASE WHEN e."event_type"='Linkedin Visited Profile' THEN e."who_created_event"
           WHEN e."event_type"='Moved to stage' AND e."moved_to_stage" IN ('Contacted','Prospects') THEN e."who_created_event"
           WHEN e."event_type"='Moved to stage' AND e."moved_to_stageType"='Positive Response' THEN e."who_created_event"
           WHEN e."event_type"='Disqualified' THEN e."who_created_event"
           WHEN e."event_type"='Candidate created' AND e."moved_to_stage" IN ('Contacted','Prospects') THEN e."who_created_event"
           WHEN e."event_type"='Candidate created' AND e."moved_to_stageType"='Prospects' THEN e."who_created_event"
           WHEN e."event_type"='Moved to stage' AND e."moved_to_stage"='Recruiter Screen' THEN e."who_created_event_first"
           WHEN (e."event_type"='Evaluation' OR (LEFT(e."event_id",9)='recruitee' AND e."moved_to_stageType"='evaluation')) THEN e."who_created_event_first"
           WHEN e."moved_to_stage"='Moved to ATS' THEN e."who_created_event_first"
           WHEN e."moved_to_stageType"='Offer' THEN e."who_created_event_first"
           WHEN e."moved_to_stage"='Hired' THEN e."who_created_event_first" ELSE NULL END AS corr
  FROM "KEBOOLA_855"."out.c-reporting-v2"."event" e JOIN gj ON gj.jid=e."job_id"
  WHERE e."candidate_id" IN (SELECT cid FROM na) AND NULLIF(e."who_event_created_for",'') IS NOT NULL
), dimmap AS (
  SELECT 'company' dt, '' dv, cid, et, ms, mst FROM evb
  UNION ALL SELECT 'client', client, cid, et, ms, mst FROM evb
  UNION ALL SELECT 'ta', wecf, cid, et, ms, mst FROM evb
  UNION ALL SELECT 'ts', corr, cid, et, ms, mst FROM evb WHERE corr IS NOT NULL
), lv AS (
  SELECT gj.client, e."talent_id" tid, e."job_id" jid, TRY_TO_DATE(e."date_created") dcr,
         e."who_created_event" wce, e."who_event_created_for" wecf
  FROM "KEBOOLA_855"."out.c-reporting-v2"."event" e JOIN gj ON gj.jid=e."job_id"
  WHERE e."event_type"='Linkedin Visited Profile' AND NULLIF(e."who_event_created_for",'') IS NOT NULL
), vdim AS (
  SELECT 'company' dt,'' dv, dcr,tid,jid FROM lv
  UNION ALL SELECT 'client',client,dcr,tid,jid FROM lv
  UNION ALL SELECT 'ta',wecf,dcr,tid,jid FROM lv
  UNION ALL SELECT 'ts',wce,dcr,tid,jid FROM lv
), prsrc AS (
  SELECT 'company' dt,'' dv, cid, edt FROM evb WHERE mst='Positive Response' AND edt>=DATE '2025-04-14'
  UNION ALL SELECT 'client', client, cid, edt FROM evb WHERE mst='Positive Response' AND edt>=DATE '2025-04-14'
  UNION ALL SELECT 'ta', wecf, cid, edt FROM evb WHERE mst='Positive Response' AND edt>=DATE '2025-04-14'
  UNION ALL SELECT 'ts', corr, cid, edt FROM evb WHERE mst='Positive Response' AND edt>=DATE '2025-04-14' AND corr IS NOT NULL
), cs AS (
  SELECT "candidate_id" cid, TRY_TO_DATE("date_contacted") dc, TRY_TO_DATE("date_screen") dsr,
         TRY_TO_DATE("date_screen_actual") dsa, TRY_TO_DATE("date_interview") di,
         TRY_TO_DATE("date_offer") doff, TRY_TO_DATE("date_hired") dh, "stage_current" stg
  FROM "KEBOOLA_855"."out.c-reporting-v2"."candidate_stage"
), cand AS (
  SELECT "candidate_id" cid, "reason_not_interested" rni, "is_candidate_reacted" rct
  FROM "KEBOOLA_855"."out.c-reporting-v2"."candidate"
), mloose AS (SELECT DISTINCT dt,dv,cid FROM dimmap),
viewed AS (SELECT dt,dv,YEAROFWEEKISO(dcr) y,WEEKISO(dcr) w, COUNT(DISTINCT tid||'|'||jid) v FROM vdim WHERE YEAROFWEEKISO(dcr)>=2025 GROUP BY 1,2,3,4),
posresp AS (SELECT dt,dv,YEAROFWEEKISO(edt) y,WEEKISO(edt) w, COUNT(DISTINCT cid) v FROM prsrc WHERE YEAROFWEEKISO(edt)>=2025 GROUP BY 1,2,3,4),
contacted AS (SELECT m.dt,m.dv,YEAROFWEEKISO(c.dc) y,WEEKISO(c.dc) w, COUNT(DISTINCT m.cid) v FROM mloose m JOIN cs c ON c.cid=m.cid WHERE c.dc IS NOT NULL AND YEAROFWEEKISO(c.dc)>=2025 GROUP BY 1,2,3,4),
reacted AS (SELECT m.dt,m.dv,YEAROFWEEKISO(c.dc) y,WEEKISO(c.dc) w, COUNT(DISTINCT m.cid) v FROM mloose m JOIN cs c ON c.cid=m.cid JOIN cand cd ON cd.cid=m.cid WHERE c.dc IS NOT NULL AND YEAROFWEEKISO(c.dc)>=2025 AND (cd.rct=TRUE OR (cd.rni IS NOT NULL AND cd.rni<>'' AND cd.rni<>'Unresponsive') OR (c.stg IS NOT NULL AND c.stg NOT IN ('Contacted','Applied','Prospects',''))) GROUP BY 1,2,3,4),
screens AS (SELECT m.dt,m.dv,YEAROFWEEKISO(c.dsr) y,WEEKISO(c.dsr) w, COUNT(DISTINCT m.cid) v FROM (SELECT DISTINCT dt,dv,cid FROM dimmap WHERE ms='Recruiter Screen' AND et='Moved to stage') m JOIN cs c ON c.cid=m.cid WHERE c.dsr IS NOT NULL AND YEAROFWEEKISO(c.dsr)>=2025 GROUP BY 1,2,3,4),
actual AS (SELECT m.dt,m.dv,YEAROFWEEKISO(c.dsa) y,WEEKISO(c.dsa) w, COUNT(DISTINCT m.cid) v FROM (SELECT DISTINCT dt,dv,cid FROM dimmap WHERE et='Evaluation') m JOIN cs c ON c.cid=m.cid WHERE c.dsa IS NOT NULL AND YEAROFWEEKISO(c.dsa)>=2025 GROUP BY 1,2,3,4),
ats AS (SELECT m.dt,m.dv,YEAROFWEEKISO(c.di) y,WEEKISO(c.di) w, COUNT(DISTINCT m.cid) v FROM (SELECT DISTINCT dt,dv,cid FROM dimmap WHERE ms='Moved to ATS') m JOIN cs c ON c.cid=m.cid WHERE c.di IS NOT NULL AND YEAROFWEEKISO(c.di)>=2025 GROUP BY 1,2,3,4),
offered AS (SELECT m.dt,m.dv,YEAROFWEEKISO(c.doff) y,WEEKISO(c.doff) w, COUNT(DISTINCT m.cid) v FROM (SELECT DISTINCT dt,dv,cid FROM dimmap WHERE mst='Offer') m JOIN cs c ON c.cid=m.cid WHERE c.doff IS NOT NULL AND YEAROFWEEKISO(c.doff)>=2025 GROUP BY 1,2,3,4),
hired AS (SELECT m.dt,m.dv,YEAROFWEEKISO(c.dh) y,WEEKISO(c.dh) w, COUNT(DISTINCT m.cid) v FROM (SELECT DISTINCT dt,dv,cid FROM dimmap WHERE ms='Hired') m JOIN cs c ON c.cid=m.cid WHERE c.dh IS NOT NULL AND YEAROFWEEKISO(c.dh)>=2025 GROUP BY 1,2,3,4),
keys AS (
  SELECT dt,dv,y,w FROM viewed UNION SELECT dt,dv,y,w FROM posresp UNION SELECT dt,dv,y,w FROM contacted
  UNION SELECT dt,dv,y,w FROM reacted UNION SELECT dt,dv,y,w FROM screens UNION SELECT dt,dv,y,w FROM actual
  UNION SELECT dt,dv,y,w FROM ats UNION SELECT dt,dv,y,w FROM offered UNION SELECT dt,dv,y,w FROM hired
)
SELECT k.dt AS "DIM_TYPE", k.dv AS "DIM_VALUE", k.y AS "ISO_YEAR", k.w AS "ISO_WEEK",
  COALESCE(v.v,0) AS "VIEWED", COALESCE(c.v,0) AS "CONTACTED", COALESCE(r.v,0) AS "REACTED",
  COALESCE(p.v,0) AS "POSITIVE_RESPONSE", COALESCE(s.v,0) AS "REC_SCREENS",
  COALESCE(a.v,0) AS "ACTUAL_SCREENS", COALESCE(t.v,0) AS "ATS",
  COALESCE(o.v,0) AS "OFFERED", COALESCE(h.v,0) AS "HIRED"
FROM keys k
LEFT JOIN viewed v ON v.dt=k.dt AND v.dv=k.dv AND v.y=k.y AND v.w=k.w
LEFT JOIN posresp p ON p.dt=k.dt AND p.dv=k.dv AND p.y=k.y AND p.w=k.w
LEFT JOIN contacted c ON c.dt=k.dt AND c.dv=k.dv AND c.y=k.y AND c.w=k.w
LEFT JOIN reacted r ON r.dt=k.dt AND r.dv=k.dv AND r.y=k.y AND r.w=k.w
LEFT JOIN screens s ON s.dt=k.dt AND s.dv=k.dv AND s.y=k.y AND s.w=k.w
LEFT JOIN actual a ON a.dt=k.dt AND a.dv=k.dv AND a.y=k.y AND a.w=k.w
LEFT JOIN ats t ON t.dt=k.dt AND t.dv=k.dv AND t.y=k.y AND t.w=k.w
LEFT JOIN offered o ON o.dt=k.dt AND o.dv=k.dv AND o.y=k.y AND o.w=k.w
LEFT JOIN hired h ON h.dt=k.dt AND h.dv=k.dv AND h.y=k.y AND h.w=k.w
