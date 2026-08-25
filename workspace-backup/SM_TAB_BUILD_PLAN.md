# Silver Medalists tab — build plan

## Scope
Six metrics Sashka asked for. Nothing else. The pool/matching side is Martin and Sashka's system, not ours.

## Gating
Follow the `project_health` precedent already in App.jsx:
- Cookie `tribe_sm=1`, set by `/functions/api/login.ts` for blake, martin, sashka
- `?sm=1` URL param as a test fallback
- Add `'silver_medalists'` to `visibleTabs` when the flag is on
- Add it to a `SM_TABS` set so the snap-back at line 5693 sends unauthorised users to `project`

No new auth mechanism. Same shape as `canProjectHealth`.

## Blocked on
1. **Mikhail** creating the Silver Medalists stage (type Prospects, hidden, undeletable) across active clients. Until it exists there is no matched event to read.
2. **Keboola connector** dropped from the session. The transformation and the `render_json` wiring both need it.

## Data definitions

| Term | Bubble source |
|---|---|
| Silver medalist | `Candidate.Sourcedsource = '1787139290725x775285588014012800'` |
| Matched | Event `moved_to_stage` → stage named `Silver Medalists` (stagesType = Prospects) |
| Intro | Event `moved_to_stage` → stage type `Contacted` |
| Screen / Interview / Offer / Hired | Existing stage events, unchanged |

SLA on matched → intro is 24 hours.

## Transformation SQL (draft, to paste into Keboola)

```sql
-- sm_candidates: one row per silver-medalist candidate with a date per stage
WITH sm AS (
  SELECT c."bubbleinternal_id" AS candidate_id, c."Talent" AS talent_id,
         c."Job" AS job_id, c."disqualified"
  FROM "Candidate" c
  WHERE c."Sourcedsource" = '1787139290725x775285588014012800'
),
ev AS (
  SELECT e."Candidate" AS candidate_id,
         st."stage_type_name" AS stage_type,
         s."stageName" AS stage_name,
         MIN(TO_TIMESTAMP_NTZ(e."Created_Date")) AS first_at
  FROM "Event" e
  JOIN "stages" s      ON s."bubbleinternal_id"  = e."moved_to_stage"
  JOIN "stagesType" st ON st."bubbleinternal_id" = s."stagesType"
  WHERE e."Candidate" IN (SELECT candidate_id FROM sm)
  GROUP BY 1,2,3
)
SELECT sm.candidate_id, sm.job_id, j."Title" AS job_title, co."Name" AS client,
       u."full_name" AS recruiter,
       MAX(CASE WHEN ev.stage_name = 'Silver Medalists' THEN ev.first_at END) AS matched_at,
       MAX(CASE WHEN ev.stage_type = 'Contacted'        THEN ev.first_at END) AS intro_at,
       MAX(CASE WHEN ev.stage_type = 'Recruiter Screen' THEN ev.first_at END) AS screen_at,
       MAX(CASE WHEN ev.stage_type IN ('Final Interview','Interview 1','Interview 2','Interview 3')
                THEN ev.first_at END)                                          AS interview_at,
       MAX(CASE WHEN ev.stage_type = 'Offer'            THEN ev.first_at END) AS offer_at,
       MAX(CASE WHEN ev.stage_type = 'Hired'            THEN ev.first_at END) AS hired_at,
       sm."disqualified"
FROM sm
LEFT JOIN ev ON ev.candidate_id = sm.candidate_id
LEFT JOIN "Jobs" j    ON j."bubbleinternal_id"  = sm.job_id
LEFT JOIN "Company" co ON co."bubbleinternal_id" = j."Company"
LEFT JOIN "User" u    ON u."bubbleinternal_id"  = j."recruiter_responsible"
GROUP BY 1,2,3,4,5,12;
```

Everything the tab needs comes off that one table.

## The six metrics, all from `sm_candidates`

| Metric | Calculation |
|---|---|
| Introductions per week | `COUNT(*) WHERE intro_at IS NOT NULL` grouped by ISO week of `intro_at` |
| Request → intro, avg/median days | `intro_at - matched_at`, plot against the 24h SLA line |
| Total introductions | Same count, weekly / monthly / all-time |
| Total hires | `COUNT(*) WHERE hired_at IS NOT NULL` |
| Funnel conversion | matched → intro → interview → offer → hired, each as a % of the step before, computed across all intros not within a job |
| Intro → hire, avg/median days | `hired_at - intro_at` |

## Tab layout

1. KPI row: intros this week, intros all-time, hires all-time, median request→intro days, median intro→hire days
2. Intros per week bar chart, week over week
3. Funnel with the five conversion rates
4. Request → intro distribution with the 24h SLA marked
5. Table: candidate, client, role, recruiter, current stage, days since intro

## Order of work
1. Mikhail creates the stage
2. Keboola transformation from the SQL above → new `out.c-SM` bucket
3. Add `sm_candidates` to the `render_json` config so it lands in `dashboard_data_snowflake.json.gz`
4. Add the tab to App.jsx **in the repo, not the local copy** (local goes stale, diff first)
5. Deploy

## Note
The `tribe-recruiting-dashboard` skill is stale. It describes a DuckDB + n8n pipeline with `bubble_extract.py`. The real pipeline is Keboola/Snowflake rendering a gzipped JSON the dashboard fetches at runtime. Worth fixing that file separately.
