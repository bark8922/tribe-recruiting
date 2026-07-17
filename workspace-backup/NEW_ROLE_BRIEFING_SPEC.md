# New Role Briefing ("Similar Roles" Slack Bot) — v1 Spec

**Status:** v1.1 — decisions locked with Blake 2026-07-17, validated on two live test runs (Backend Engineer / Berlin, Marketing Manager / Germany)
**Owner:** Blake (idea: Blake + Gustavo)
**Depends on:** tribe-job-intel Supabase project, Keboola project 855 (raw Bubble tables + dashboard export), existing Slack bot infra (roles weekly bot)

---

## 1. What it does

When a new role opens in Bubble, the assigned TA and sourcer get one Slack DM within a few hours: a briefing built from every similar role Tribe has run. Benchmarks, drop reasons, where hires actually came from, who has done it before, overlapping open roles, and any intake notes we have. Zero manual effort, zero new UI. The goal: nobody starts a role cold.

This is deliberately a push briefing, not a chatbot. Every number is deterministic aggregation over data we already trust. A conversational layer (Tribot) can come later as the "ask follow-up questions" entry point.

## 2. Trigger: how we know a role is new

Source of truth = **raw Bubble jobs** (`out.c-reporting-v2.job` in Keboola, or the Supabase `jobs` mirror once its sync catches up). NOT the dashboard export's legacy behavior: the export's `jobs` list was frozen at 2026-04-12 until fixed on 2026-07-17 (render_json now rebuilds it fresh each run; see PROJECT_LOG.md same date).

- Edge function `new-role-briefing` on a cron ~30 min after each jobs sync.
- "New role" = `first_seen_at` since last run, active, not archived, client role (skip internal/BD), client name not matching test patterns (`test`/`feedback` substrings — do NOT use Bubble's `client.test` flag, it falsely marks Wolt and Statista as test clients).
- New table `role_briefings` (`job_id` PK, `sent_at`, `slack_ts`, `payload jsonb`) for exactly-once delivery + audit trail.
- Debounce: skip roles archived within 24h of creation.

## 3. Similarity: what counts as a "similar role" (LOCKED)

Deterministic, no AI matching in v1, and **location-aware** — a Backend Engineer in Tokyo says nothing about hiring one in Berlin (Blake, 2026-07-17). Location comes from the raw job's `job_location`, mapped to a country/region group (DACH, Nordics, Iberia, CEE, UK/IE, remote-EU, etc.).

Matching ladder — take the first rung with **≥ 8 historical roles**, always tell the reader which rung matched and the sample size:

1. Same `job_subcategory` + same client
2. Same `job_subcategory` + same country/region group
3. Same `job_category` + same country/region group  ← this matched "Marketing Manager, Germany" (36 roles)
4. Same `job_subcategory` anywhere  ← "Backend Engineer" fallback shape (64 roles)
5. Same `job_category` anywhere
6. Same client, any role

Seniority acts as an ordering boost within a rung, not a hard filter (too sparse). Subcategory mis-tags in Bubble (e.g. a C++ client dev tagged Backend) are accepted as noise — Blake: leave as is.

**v2 upgrade path:** `intake_records` / `weekly_updates` already have unpopulated pgvector `embedding` columns; populate + embed titles for fuzzy matching later. Not needed for v1.

## 4. The message, section by section, with provenance

Sections with insufficient data are dropped, never shown empty. Every stat line carries its sample size.

### Header — the new role's basics
| Field | Source |
|---|---|
| Title, client, category, location, seniority, comp range | raw Bubble job (via Supabase `jobs` sync) |
| TA / sourcer names + Slack IDs | Supabase `people` |

### Section 1 — Benchmarks
> Median time-to-hire **38 days** (fast quartile 23, slow 47+). First qualified candidate ~day 16.

| Fact | Source | Rule |
|---|---|---|
| tth / t2find medians + quartiles over the matched set | Keboola export `tth_jobs` | **Exclude tth < 3 days** — those are quick duplicate postings created to log a 2nd hire, not real searches (LOCKED). Report how many were excluded. |

Note `tth_jobs` only contains roles that hired; benchmarks describe successful searches. Fine for "what good looks like."

### Section 2 — Drop reasons ("what kills candidates here")
> Skills (26%), Location (16%), Salary (14%).

| Fact | Source | Rule |
|---|---|---|
| Drop reason distribution over matched job_ids | export `drops_by_sourcer` + `drops_by_recruiter` | Exclude generic reasons (Timing, Position closed, Position, Other). Show top 4-6 with %. |

### Section 3 — Where hires come from
> 9 of 16 hires were inbound applicants; push the job ad.

| Fact | Source |
|---|---|
| candidate_source distribution of hires on matched roles | export `project_dashboard_hires` |

Added after the Marketing/Germany test: source mix flips by role family (backend = 57% sourced; German marketing = majority applicants) and directly changes how the sourcer should spend week 1.

### Section 4 — Who's done it before + collisions
> Marina hired 8 of 16. ⚠️ 2 similar roles are open right now (Elena, Wolt B2B).

| Fact | Source |
|---|---|
| TA per historical matched role | export `tth_jobs` |
| Sourcer + TA per hire, recency | export `project_dashboard_hires` |
| Currently open similar roles (collision / teaming) | raw Bubble jobs, active + matching rung |

Deep candidate lookups ("who did we already talk to") stay in the Candidate Finder; the briefing links to it.

### Section 5 — Intake notes (include only when present)
| Fact | Source | Status |
|---|---|---|
| Structured intake summary (overview, aboutTheRole, candidateProfile, compensation, interviewProcess, sourcingStrategy, valueProposition) | Supabase `intake_records.extracted_fields`, joined via `bubble_job_id` | Join UNBLOCKED 2026-07-17 (jobs feed fixed). Renders as short verbatim quotes with date + attribution. |

### Explicitly REMOVED from v1 (Blake, 2026-07-17)
- **Week-4 health benchmark** (`new_project_health`) — not relevant.
- **ta_weekly_notes** — hit and miss (OOO notes etc.), and client-level rather than job-level, so attribution is fuzzy. Out entirely.
- **Funnel math** (contacted-per-hire, response rates) — deferred; computing it from hired-only roles has survivorship bias, and doing it honestly (all similar roles via `weekly_summary_byjob`) is a later refinement. Use tth numbers only for now.

## 5. Delivery

- Slack DM to the role's TA and sourcer (`people.slack_id`, same mapping as the roles bot). TA == sourcer (client-embedded) → one DM.
- Footer states the match rung + sample size, plainly: "Numbers from 36 marketing roles in Germany."
- If even the widest rung has < 3 roles: minimal message with org-wide `tth_monthly` benchmark instead.
- Rollout playbook copied from the roles bot: dry-run mode DMs everything to Blake for 1-2 weeks first.

## 6. call_records (Fireflies/AI-call ingestion): Phase 2 feeder

`call_record` ingestion into Keboola went live 2026-07-16 (see PROJECT_LOG.md) but the table is young and its usefulness is unproven — Blake: park it. When it matures, run intake-type calls through the same 7-field extraction as `intake_records` and feed Section 5 with identical rendering. Needs a job join key (job link exists but ~15% of calls have no job linked today).

## 7. Phasing

| Phase | Scope | Effort |
|---|---|---|
| **v1** | Trigger + location-aware ladder + Sections 1-4, template-rendered, no LLM. Dry-run to Blake first. | 1 edge fn, 1 cron, 1 table |
| **v1.5** | Section 5 intake quotes (join now works), go-live to TAs/sourcers | Small |
| **v2** | LLM-composed narrative, embeddings for fuzzy matching, call_records feeder, honest funnel math | Medium |
| **v3** | Tribot drill-down; briefing becomes its entry point | Separate project |

## 8. Validated test runs (real data, 2026-07-17)

**Backend Engineer, Berlin** — rung 4 (subcategory, 64 roles): median tth 26d (13/42 quartiles), find ~day 18. Drops: Location 20% + Remote-only 7% (top killer), Skills 19%. 57% of 141 hires sourced. Go-to people: Simon Siew, Vladimir Stankovic (TAs); Vladimir, Filip Nogowski (sourcers).

**Marketing Manager, Germany** — rung 3 (category + country, 36 roles, 30 after tth<3d exclusion): median tth 38d (23/47), find ~day 16. Drops: Skills 26%, Location 16%, Salary 14%, contract type notable at 7%. Hires majority inbound applicants (9/16), Marina Nikolic hired 8 of 16. Two overlapping Wolt B2B marketing roles open (collision flag worked, powered by the fixed jobs feed).

The two runs tell opposite stories (sourcing-led vs applicant-led, location-killed vs skills-killed), which is exactly the per-role guidance the briefing exists to deliver.

## 9. Decisions (locked by Blake 2026-07-17)

1. **Recipients:** DM to both TA and sourcer. No channel posting.
2. **Timing:** send on detection (up to 4x/day, one role at a time).
3. **Region groups:** grouped regions (DACH, Nordics, Iberia, CEE, UK/IE, remote-EU, ...) as proposed.
4. **Build status: NOT green-lit yet.** Waiting for team feedback on the concept (Gustavo one-pager: `NEW_ROLE_BRIEFING_ONE_PAGER.md`). Nothing gets built until Blake says go.

## 10. Out of scope for v1

Embedding matching, raw transcripts, write-back to Bubble, channel posting, per-candidate recommendations (Candidate Finder's job), funnel math, any Tribot/Q&A surface.
