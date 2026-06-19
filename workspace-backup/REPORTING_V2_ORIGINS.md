# Reporting-v2 Origins — One-Page Reference

**Source:** Frantisek's Keboola Snowflake transformation `[PROD] Data preparation V2`, specifically `part_1_-_bubble_data.sql` (842 lines), `part_3_-_final_tables.sql` (merge + backfill), and `part_4_-_Andy.sql` (2 trivial tables).

**Purpose:** For every non-obvious column in `out.c-reporting-v2`, explain where it comes from in Bubble.io so we can port the WBR/MBR/Project Dashboard SQL to our own layer without opening Keboola again.

---

## 1. The 17 reporting-v2 tables and who builds each

| Table | Built in | Part | Complexity |
|---|---|---|---|
| `job` | part_1 `final_job` | simple select + joins; `date_first_hired` backfilled in part_3 | Low |
| `user` | part_1 `final_user` | latest-role lookup via `bubble_Event` Change-positions events | Low-Med |
| `client` | part_1 `final_client` | distinct from `tmp_job` — **not a separate source table** | Low |
| `job_goal` | part_1 `final_job_goals` | direct select from `bubble_Goals`, parses JSON `date_range` array | Low |
| `talent` | part_1 `final_talent_bubble` → part_3 UNION with recruitee | 4-pass dedup on linkedin_link / LinkedinMainID / linkedin_nick / main_email | **High** |
| `talent_email` | part_1 `final_email` | simple dedup by email+latest Created_Date | Low |
| `talent_position` | part_1 `final_talent_position` | `bubble_Positions` ordered by `Worked_from desc` with row_number | Low |
| `talent_employer` | part_1 `final_talent_employer` | `bubble_Company` + latest `bubble_bd_crunchbase` for revenue/funding data | Low |
| `event` | part_1 `final_event` → part_3 UNION with recruitee | 14 joins; `who_created_event_first` backfill; automation read/replied logic | **Very High** |
| `candidate` | part_1 `final_candidate_bubble` → part_3 UNION | dedup by (job, talent.linkedin) via row_number; `is_candidate_createdby_ai` from event.AI flag | Med |
| `candidate_stage` | part_1 `final_candidate_stage_bubble` → part_3 `final_candidate_stage_all` | **stage-date backfill + cascading date fill + hired_order + hired_views/contacts/screens**. This is the hardest table. | **Very High** |
| `screen` | part_1 `final_screen` | simple select from `bubble_recruiter_screeen_notes` with dropdown joins | Low |
| `screen_techstack` | part_1 `final_screen_techstack` | lateral flatten of JSON `tech_stack` array | Low |
| `screen_lang` | part_1 `final_screen_lang` | lateral flatten of JSON `Languages` array | Low |
| `analytic` | part_4 (Andy) | `count(bubbleinternal_id) from bubble_Analytic group by page/user/date` | Trivial |
| `job_ai_filter` | part_4 (Andy) | flat select from `bubble_JobAiFilter` | Trivial |
| `client_cost` | NOT in part_1 — comes from the Revenue transformation (finance) | n/a | n/a for recruiting |

**Total load-bearing logic: 14 tables in part_1, 0 in part_4 that matters for recruiting.**

---

## 2. Non-obvious column origins

### `job.date_first_hired`, `job.date_first_hired_contacted`

NOT populated in part_1 — both start as `NULL::DATE`. Backfilled in part_3 via:

```sql
update "final_job" as j
set j."date_first_hired" = (
    select min(s."date_hired")
    from "final_candidate_stage_all" as s
    join "final_candidate_all" c on c."candidate_id" = s."candidate_id"
    where c."job_id" = j."job_id" and s."date_hired" is not null
);
-- date_first_hired_contacted uses min(date_contacted) filtered to hired candidates
```

So "when was this job first filled?" = earliest date_hired across all hired candidates on that job. **Only counts actual hires — ignores offers, withdrawn offers, etc.**

### `job.job_recruiter`, `job.job_sourcer`

These are the **officially assigned** recruiter and sourcer from `bubble_Jobs.recruiter_responsible` and `bubble_Jobs.sourcer_responsible`. These are the official role fields.

**⚠️ This is NOT the field used in leadership WBR.** The WBR uses the `event` table's `who_event_created_for` (TA) and `who_created_event_first` (credit sourcer = first to contact). Same names, different semantics:

| Column | Source | Semantic |
|---|---|---|
| `job.job_recruiter` | `bubble_Jobs.recruiter_responsible` | Who's officially on the job |
| `event.who_event_created_for` | `bubble_Event.who_event_created_for` (join to User) | Who the event was created FOR (the TA) |
| `event.who_created_event` | `bubble_Event.who_created_event` | Who actually did the action (often the sourcer) |
| `event.who_created_event_first` | first value of `who_created_event` per candidate ordered by date | **The credit sourcer** — first person to touch the candidate |

Andy's rule "first to contact = credit sourcer" is baked into this `who_created_event_first` column via this update:

```sql
update "final_event" e
set e."who_created_event_first" = x."who_created_event"
from (
    select t."candidate_id", t."who_created_event",
           row_number() over (partition by t."candidate_id" order by t."date_created" asc) rown
    from "final_event" t
    where t."who_created_event" <> '' and t."candidate_id" <> ''
) x
where x.rown = 1 and e."candidate_id" = x."candidate_id";
-- fallback: if no candidate_id, group by talent_id
```

### `candidate_stage.date_contacted` / `date_screen` / `date_screen_actual` / `date_interview` / `date_offer` / `date_hired`

These are NOT direct fields on the Bubble candidate. They're **max(event.date_created) per candidate filtered by moved_to_stageType and gated on stage_current_num**:

| Stage date | Filter |
|---|---|
| `date_lnkdin_viewed` | `event_type = 'Linkedin Visited Profile'`, per **talent_id**, only if <= candidate.date_created |
| `date_contacted` | `moved_to_stageType = 'Contacted' AND moved_to_stage <> 'Responded'`, gated on `stage_current_num >= 1` |
| `date_screen` | `moved_to_stageType = 'Recruiter Screen'`, gated on `stage_current_num >= 2` |
| `date_screen_actual` | primary: `event_type = 'Evaluation'`; fallback: `max(Created_Date) from bubble_recruiter_screeen_notes` |
| `date_interview` | `moved_to_stageType IN ('Offsite', 'Interview') OR stage LIKE '%interview%'`, gated on `stage_current_num >= 3` |
| `date_offer` | `moved_to_stageType = 'Offer'`, gated on `stage_current_num >= 4` |
| `date_hired` | `moved_to_stageType = 'Hired'`, gated on `stage_current_num >= 5` |

The `stage_current_num` gating is what Andy meant by "current_stage is source of truth, ignore stale forward dates". It's computed at the top of `final_candidate_stage_bubble`:

```sql
CASE
    WHEN stage_current_type = 'Offer' THEN 4
    WHEN stage_current_type = 'Hired' THEN 5
    WHEN stage_current IN ('Referred','Sourced%','Downloaded','Prospects','Applied') THEN 0
    WHEN stage_current_type IN ('Contacted','Positive Response') THEN 1
    WHEN stage_current_type = 'Recruiter Screen' THEN 2
    WHEN stage_current_type LIKE '%interview%' OR 'Reference Check' OR 'Offsite' THEN 3
    ELSE 0
END as stage_current_num
```

**This is also where `'Positive Response'` gets handled** — it's mapped to num=1 alongside `'Contacted'`. So the undocumented `'Positive Response'` stage_current_type Finding 4 from the sanity check is actually handled here, bucketed as "Contacted". The Power Query M expression in Power BI just happens to leave it uncategorized; the upstream SQL treats it as Contacted-equivalent.

### Cascading date backfill (the critical "Andy rule")

After all the per-stage `max()` fills, part_1 runs a cascade **from hired backward to lnkdin_viewed**:

```sql
-- if hired, force offer
update c set date_offer = iff(date_hired is not null and date_offer is null, date_hired, date_offer);
-- if offer, force interview
update c set date_interview = iff(date_offer is not null and date_interview is null, date_offer, date_interview);
-- if interview, force screen_actual
update c set date_screen_actual = iff(date_interview is not null and date_screen_actual is null, date_interview, date_screen_actual);
-- if screen_actual, force screen
update c set date_screen = iff(date_screen_actual is not null and date_screen is null, date_screen_actual, date_screen);
-- if screen, force contacted
update c set date_contacted = iff(date_screen is not null and date_contacted is null, date_screen, date_contacted);
-- if contacted, force lnkdin_viewed (or override if lnkdin_viewed > contacted)
update c set date_lnkdin_viewed = iff(date_contacted is not null and (date_lnkdin_viewed is null or date_lnkdin_viewed > date_contacted), date_contacted, date_lnkdin_viewed);
```

**Consequence:** time-to-X metrics computed from these columns never have gaps. If someone is hired, they have a full chain of synthetic dates back to lnkdin_viewed, even if the intermediate events weren't logged. This is exactly why Time-to-Hire calculations work at all on Tribe's data — the raw event stream is too sparse.

### `candidate_stage.hired_order`, `hired_views`, `hired_contacts`, `hired_screens`

Built in part_3, not part_1:

```sql
row_number() over (partition by c.job_id order by cs.date_hired) as hired_order
-- then NULL'd where date_hired is null

-- per hired candidate, count preceding talents/candidates on same job:
hired_views    = count(distinct talent_id) in events of type 'Linkedin Visited Profile' <= date_lnkdin_viewed
hired_contacts = count(*) from candidate_stage where job_id matches and date_contacted <= this.date_contacted
hired_screens  = count(*) from candidate_stage where job_id matches and date_screen <= this.date_screen
```

These are "how many views/contacts/screens did it take to get to this hire" on a per-hire basis. **Blake's "hired-order=1 = first hire" rule uses this column.**

### `talent.is_talent_duplicated`, `talent.duplicates`

Four sequential dedup passes in part_1:
1. By normalized `linkedin_link`
2. By `LinkedinMainID`
3. By `linkedin_nick`
4. By `main_email`

Each pass sets `is_talent_duplicated = TRUE` if any **earlier** talent record has a matching key, and appends the matching talent_ids to a semicolon-separated `duplicates` string. "Earlier" = lower `timestamp_created`. So the first record wins and subsequent records are flagged as dupes.

### `candidate.is_candidate_duplicated`

Dedup by `(job_id, talent.linkedin)` — if the same person appears twice on the same job with the same LinkedIn, the older one wins. Implemented via row_number on bubble_Candidate joined to bubble_Talent.

### `candidate.is_candidate_createdby_ai`

Set to `TRUE` if any `bubble_Event` with `AI='True'` exists with event_type `1542180373448x729603979969397200` (the "Candidate created" event type, a Bubble hardcoded ID).

### `event.who_created_event_first`

**The credit sourcer.** First non-empty `who_created_event` per candidate, ordered by `date_created asc`. Fallback: if candidate_id is blank, first per talent_id. This is what WBR TS Actual slices on.

### `event.automation_is_message_read`, `automation_is_message_replied`, `automation_message_version_id`

Backfilled per event by joining to other events on the same `(candidate_id, Automation_flow, Automation_step)` and looking for paired events:
- `'Email Sent'` is "replied" if there's a paired `'Email Replied'`, "read" if there's a paired `'Email Read'` OR if it was replied
- `'Linkedin Sent Contact'` paired with `'Linkedin Connected'` (read) and `'Linkedin Responded'` (replied)
- `'Message sent'` paired with `'Linkedin Responded'` (replied)
- `'Linkedin inMail sent'` paired with `'Linkedin inMail received'` (replied)

Plus a direct lookup to `bubble_duxsoup_messages` and `bubble_Nylas_Email_message` for `version_id` and read flags.

---

## 3. The hardcoded Bubble IDs you need to know about

These appear in part_1 as magic strings — they're Bubble-specific primary keys that will bite you if they ever change:

| ID | What it means |
|---|---|
| `1642420714568x807043530709183200` | event_type for "Change positions" (used to build latest-role in `final_user`) |
| `1542180373448x729603979969397200` | event_type for "Candidate created" (used for `is_candidate_createdby_ai` flag) |
| `'FrantisekDelete'` in `Content` | Soft-delete marker — all events with this content are excluded |

---

## 4. Sources not coming from reporting-v2

A few things Power BI uses aren't in reporting-v2 at all:

- **WBR targets** — lives in Andy's Google Sheet (`WBR TA Target` table in Power BI is connected to a Google Sheet, per the Power Query M expression I extracted yesterday). Not in Keboola. We need to ask Andy for the sheet URL and either connect it directly or import it as a static CSV.
- **BambooHR historical `report_to`** — Power BI pulls this via a separate Snowflake table `Historical Manager Structure WBR`. Not sourced from Bubble. This is for the manager hierarchy cut Andy mentioned. Needs its own feed.
- **Org hierarchy** — `Org` and `Org_WBR` tables in Power BI are static uploaded tables (probably from Google Sheets). Not in reporting-v2.
- **Sourcing Team List** — another static table, Andy's manual mapping of "who is a sourcer" outside of Bubble's sourcer role.

---

## 5. Implications for `wbr_view.sql`

Based on this walkthrough, the MVP WBR view needs:

1. **Join `job → candidate → candidate_stage → event → client`** — all already in reporting-v2, no custom transforms needed.
2. **Use `event.who_created_event_first` for the credit sourcer**, NOT `job.job_sourcer` or `candidate.candidate_sourcer`.
3. **Use `event.who_event_created_for` for the TA slice**.
4. **Use `candidate_stage.date_*` columns directly** for time-to-X — they're already backfilled with the cascade.
5. **Use `candidate_stage.hired_order = 1` for "first hire on this job"** — Andy's rule is a single filter.
6. **Import 3 static sources Power BI uses but reporting-v2 doesn't have:** WBR targets, BambooHR manager hierarchy, Sourcing Team List. Ask Andy for all three before he leaves. Static CSVs are fine.
7. **Filter set**: `test = 'false'` on job AND client, `is_candidate_archived = 'false'`, `client_name NOT IN ('Tribe.xyz', 'Kamila AI - TEST')`. Booleans are lowercase TEXT — same gotcha as yesterday's sanity check.

Total SQL for the WBR view should be ~150-250 lines once the three static sources are in place. Almost all of Andy's "brain" is either already captured in Frantisek's pre-computed columns or in the three CSVs we still need to grab.
