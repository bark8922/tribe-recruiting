# New Role Briefing ("Similar Roles" Slack Bot) — v1 Spec

**Status:** Draft for review (Blake + Gustavo)
**Date:** 2026-07-16
**Owner:** Blake
**Depends on:** tribe-job-intel Supabase project, Keboola dashboard export (`dashboard_data_snowflake.json.gz`), existing Slack bot infra (roles weekly bot)

---

## 1. What it does

When a new role opens in Bubble, the assigned TA and sourcer get one Slack DM within a few hours: a briefing built from every similar role Tribe has ever run. Benchmarks, funnel math, drop reasons, who has done this before, and any intake notes we have. Zero manual effort, zero new UI. The goal is that nobody starts a role cold.

This is deliberately a push briefing, not a chatbot. Every number in the message is deterministic SQL/aggregation over data we already trust. A conversational layer (Tribot) can come later as the "ask follow-up questions" entry point.

---

## 2. Trigger: how we know a role is new

The tribe-job-intel Supabase project already syncs Bubble jobs into `public.jobs` (731 rows today) via the existing `reconcile-intake` cron, and every row carries `first_seen_at`.

- New edge function `new-role-briefing`, on a cron ~30 min after each jobs sync.
- "New role" = `first_seen_at` since last run, `is_active_role = true`, `is_archived = false`, and `is_client_role = true` (skip internal/BD entries).
- New table `role_briefings` (`job_id` PK, `sent_at`, `slack_ts`, `payload jsonb`) guarantees exactly-once delivery and gives us an audit trail of what we claimed.
- Optional debounce: skip roles archived within 24h of creation (test entries).

No new infrastructure: this is the same pattern (cron → edge fn → Slack) the roles weekly bot already runs on.

## 3. Similarity: what counts as a "similar role"

v1 is deterministic, no AI matching. The Supabase `jobs` table already has `job_category`, `seniority`, `location`, `comp_min/max` per job, and the Keboola export's `tth_jobs` has `job_category` + `job_subcategory` on 2,266 historical jobs.

Matching ladder (take the first rung that yields **≥ 8 historical jobs**, and always report which rung was used and the sample size):

1. Same `job_subcategory` + same client
2. Same `job_subcategory` (e.g. "Sales / Account Manager": 230 historical jobs; "Software Engineering / Backend": 64)
3. Same `job_category` + same seniority band
4. Same `job_category`
5. Fallback: all jobs for this client

Seniority and remote-vs-onsite act as boosts for ordering within a rung, not hard filters (data is too sparse to filter hard on them).

**v2 upgrade path:** `intake_records` and `weekly_updates` already have pgvector `embedding` columns (currently unpopulated). Populating them + embedding job titles gives fuzzy matching for the cases the category ladder misses. Not needed for v1.

---

## 4. The message, section by section, with provenance

Everything below names the exact source table. Two sources live in Supabase (tribe-job-intel); everything else comes from the Keboola dashboard export `.gz` that the bot already downloads daily for the roles roundup.

### Header — the new role's basics
> 🆕 New role: **Senior Backend Engineer** at **Wolt** (Berlin, hybrid) · TA: Lisa · Sourcer: Jovana

| Field | Source |
|---|---|
| Title, client, category, location, seniority, comp range | Supabase `jobs` (synced from Bubble) |
| TA / sourcer names + Slack IDs | Supabase `people` |

### Section 1 — Benchmarks ("what good looks like")
> 14 similar roles run since 2023. Median time-to-hire **47 days** (find 21, fill 26). A healthy week 4: **~6 screens, 3 in ATS**. Slowest quartile took 90+ days.

| Fact | Source |
|---|---|
| Time-to-hire / time-to-find / time-to-fill medians + quartiles | Keboola export `tth_jobs` (per-job `tth`, `t2find`, `t2fill`) |
| Week-4 health benchmark (screens, ATS by end of week 4) | Keboola export `new_project_health` (`w4_actual_screens`, `w4_ats`, `date_first_ats`) |

### Section 2 — Funnel math ("what it takes")
> For this profile it took a median **~180 contacted per hire**. Positive response rate ran **12%**, screen→ATS **41%**.

| Fact | Source |
|---|---|
| Contacted → positive response → screens → ATS ratios, per similar job | Keboola export `weekly_summary_byjob` (11k rows: per job, per week, full funnel counts), summed per job then aggregated as medians |
| Whole-funnel trend context | Keboola export `weekly_summary` (dim-level) if per-job data is too thin |

### Section 3 — Drop reasons ("what kills candidates here")
> Top drop reasons on similar roles: **Salary (18%)**, **Location (15%)**, **Skills (12%)**. (Generic reasons like "Timing" and "Position closed" excluded.)

| Fact | Source |
|---|---|
| Drop reason counts, filtered to the similar job_ids | Keboola export `drops_by_sourcer` (25.7k rows) + `drops_by_recruiter` (19.2k rows), each with job_id, week, reason |

Note: "Timing" is by far the biggest raw reason (~27k) and is mostly noise; v1 excludes Timing / Position closed / Other from the headline and shows the informative ones (Salary, Location, Skills, Language, Remote only, Seniority).

### Section 4 — Who's done it before
> Hired on similar roles: **Lisa (Wolt x3)**, **Vladimir (Aiven)**. Jovana sourced 3 of the last 5 backend hires. 2 similar roles are open right now.

| Fact | Source |
|---|---|
| TA per historical similar job + outcome | Keboola export `tth_jobs` (`ta`, `date_first_hired`) |
| Sourcer per hire + candidate source (LinkedIn, referral, ...) | Keboola export `project_dashboard_hires` (3,915 hires with `ts`, `ta`, `candidate_source`, stage dates) |
| Currently open similar roles (avoid collision / enable teaming) | Supabase `jobs` where `is_active_role` |

(Deeper "find candidates we already talked to" lookups belong to the existing Candidate Finder in Keboola, not this message. The briefing can link to it.)

### Section 5 — Notes from humans (include only when present)
> 📋 Intake (June, similar Wolt role): client insists on Kotlin over Java; comp band €85–100k; 4-stage process.
> 📝 From weekly updates on similar roles: "many candidates want fully remote, conversion suffered" (Kristina, Aviv W6).

| Fact | Source | Caveat |
|---|---|---|
| Structured intake summary: `overview`, `aboutTheRole`, `candidateProfile`, `compensation`, `interviewProcess`, `sourcingStrategy`, `valueProposition` | Supabase `intake_records.extracted_fields` (64 records, all 7 sections populated, linked by `job_id`) | Only 64 records so far; match via job → client + category. Will get richer as intake tool adoption grows. |
| Sourcer-written blockers / risks / experiments per role per week | Supabase `weekly_updates` (`blocker_and_dropoff`, `risks_and_recommendations`, `experiments_this_week`; 50 submitted rows, growing weekly from the roles roundup go-live) | Young data set; job-level, so attribution is exact. |
| TA weekly WBR comments per client | Keboola export `ta_weekly_notes` (1,099 rows, 816 with comments) | **Client-level, not job-level** (see §6). v1 uses these only when the similar set is same-client. |

v1 renders these as short verbatim quotes with attribution. v2 can have Claude summarize them into one paragraph (bounded input, low hallucination risk since it only compresses provided text).

---

## 5. Delivery

- Slack DM to the role's TA and sourcer (resolved via `people.slack_id`, same mapping the roles bot uses). If TA == sourcer (client-embedded), one DM.
- Message ends with: "Numbers are from N similar roles (matched on X). Ask Tribot for drill-downs." (Tribot link once v3 exists; plain text until then.)
- Sections with insufficient data are dropped, not shown empty. If even the fallback rung has < 3 jobs, send a minimal message ("First role of its kind for us; here's the org-wide benchmark instead") using org-wide `tth_monthly` / `weekly_trend`.

---

## 6. What ta_weekly_notes actually is (context for Gustavo)

These are the free-text **reasoning + comment fields TAs fill in per client per week in the WBR** (weekly business review) flow in Bubble. Example, verbatim: "Conversion rate wasn't optimal, mainly because many candidates are looking for fully remote roles, and some profiles were either more junior or not a strong match..." (Kristina, Aviv, W6).

They're useful color, but they attach to **client + TA + week**, not to a specific job. So for a new Aviv role we can quote "what TAs said about Aviv recently," which is legitimate, but we cannot claim a note was about the same kind of role. v1 therefore only pulls them when the similarity rung is same-client, and labels them "client notes."

---

## 7. call_records (Fireflies → Keboola): where it fits

Not in Keboola yet (in progress in a parallel workstream). When it lands, it's potentially the strongest narrative source: real intake and check-in calls, not just what someone typed into a form.

To plug into this briefing, the ingested table needs, per call:

- `call_date`, `participants`
- **A join key**: ideally `job_id`; realistically `client_name` + fuzzy title match, or the meeting title convention
- `transcript_text` or (better) a pre-extracted summary
- A call-type tag (intake vs check-in vs debrief), even if heuristic

Recommendation: don't feed raw transcripts into the briefing. Add one extraction step (same pattern as `intake_records.extracted_fields`) that pulls the same 7 structured fields from intake-type calls, and store those. Then call_records simply becomes a second feeder into Section 5 with identical rendering. If the join key problem is hard, client-level attribution (like ta_weekly_notes) is an acceptable start.

## 8. Phasing

| Phase | Scope | Effort |
|---|---|---|
| **v1** | Trigger + category-ladder matching + Sections 1–4 (pure stats) + intake quotes when a direct `job_id`/client match exists. Template-rendered, no LLM. | 1 edge fn, 1 cron, 1 table. Small. |
| **v1.5** | Section 5 fully: weekly_updates quotes + client-level ta_weekly_notes. Dry-run mode (all briefings DM to Blake) for 1–2 weeks before go-live, same as the roles bot playbook. | Small |
| **v2** | LLM-composed narrative (Claude API summarizes Section 5 inputs), populate embeddings for fuzzy similarity, call_records feeder once ingestion lands. | Medium |
| **v3** | Tribot: conversational drill-down, briefing becomes the entry point. | Separate project |

## 9. Open decisions

1. **Recipients:** DM only TA + sourcer, or also post to a team channel? (DM-only recommended for v1.)
2. **Timing:** send on detection (up to 4x/day) or batch to one morning digest? (On detection recommended; it's one role at a time.)
3. **Similarity floor:** is 8 jobs the right minimum per rung, and is client-fallback acceptable?
4. **ta_weekly_notes consent:** TAs wrote WBR comments for leadership, not for peer broadcast. OK to quote them with attribution in DMs?
5. **Housekeeping (pre-existing, matters more once intake/call content flows through this system):** all 8 tables in tribe-job-intel have RLS disabled.

## 10. Explicitly out of scope for v1

Embedding-based matching, raw transcript processing, any write-back to Bubble, channel-wide posting, per-candidate recommendations ("contact these 12 people again": that's the Candidate Finder's job), and any Tribot/Q&A surface.
