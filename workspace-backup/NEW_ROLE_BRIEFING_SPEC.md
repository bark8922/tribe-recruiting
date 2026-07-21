# New Role Briefing ("Similar Roles" Slack Bot) — v1 Spec

**Status:** v1.2 — concept validated + team feedback positive (2026-07-17). Trigger/filter design added below. Still NOT green-lit to build.
**Owner:** Blake (idea: Blake + Gustavo)
**Depends on:** tribe-job-intel Supabase project, Keboola project 855 (raw Bubble tables + dashboard export), existing Slack bot infra (roles weekly bot)

---

## 1. What it does

When a new role opens in Bubble, the assigned TA and sourcer get one Slack DM within a few hours: a briefing built from every similar role Tribe has run. Benchmarks, drop reasons, where hires actually came from, who has done it before, overlapping open roles, and any intake notes we have. Zero manual effort, zero new UI. The goal: nobody starts a role cold.

This is deliberately a push briefing, not a chatbot. Every number is deterministic aggregation over data we already trust. A conversational layer (Tribot) can come later as the "ask follow-up questions" entry point.

## 2. Trigger + filters: firing on every new job creation

Source of truth = **raw Bubble jobs** (`out.c-reporting-v2.job` in Keboola, or the Supabase `jobs` mirror once its sync catches up). NOT the dashboard export's legacy `jobs` list, which was frozen at 2026-04-12 until fixed 2026-07-17 (render_json now rebuilds it fresh each run; see PROJECT_LOG.md same date).

### 2a. Volume reality (measured 2026-07-17)

- Non-test job creation runs **~15-20 jobs/week = 3-4 per business day** (10-week range: 3 to 48/wk). Sending to both TA and sourcer, that's a handful of DMs a day across the whole team, each person pinged only on their own roles. Not spammy.
- **BUT ~half of created jobs are noise.** Of 200 non-test jobs created in the last 60 days, **102 were already archived** — duplicates, roles that never launched, quick re-posts to log a 2nd hire, drafts. Briefing on the raw creation event would waste half the sends and train people to ignore the DM. This is why the filter funnel (2c) matters.

### 2b. Detection: batch, not webhook (v1)

Two ways to catch a new job:

- **Bubble webhook** — fires the instant someone clicks create. Real-time, but needs a Bubble-side build and catches the job at its emptiest (fields not filled in yet).
- **Keboola-pull-based** — piggyback on the jobs extractor that already runs 3-4x/day. Detect within a few hours, zero new infra.

**v1 uses pull-based.** A few hours' lag is fine for a "here's your starting point" briefing and avoids a Bubble change. Edge function `new-role-briefing` on a cron ~30 min after each jobs pull. (Webhook is a possible v2 if the team wants it faster.)

### 2c. The filter funnel (in order)

A new job must survive filters 1-4 and 6-7 before a briefing sends. Filter 5 never blocks a send — it only changes the message. Filters 1-4 are also the "readiness" gate that solves the shell-job problem (2d).

1. **Not test / not internal.** Client name not matching `test`/`feedback` (do NOT use Bubble's `client.test` flag — it falsely marks Wolt and Statista as test). Skip internal Tribe/BD-outreach entries.
2. **Still alive.** Not archived at briefing time. This single gate drops the ~50% quick-kill noise.
3. **Has a TA.** Needs a TA assigned (~99% do). **Sourcer is NOT required** (Blake, 2026-07-17): send regardless — TA-only DM when no sourcer, both when present. A missing sourcer never holds or delays the briefing.
4. **Enough to match on.** Has a location AND a category or subcategory. ~26% arrive with no subcategory (fine, falls to the category rung); ~3.5% have no location; a fully blank shell is HELD, not sent.
5. **History depth → confidence level, not a gate (LOCKED).** Always send. If ≥ N similar roles (proposed N=8), send the full briefing. Below N, send an **honest low-confidence version**: say plainly we don't have many similar roles, show the few that are somewhat related, and name people who've run similar roles. Never fabricate benchmarks off a tiny sample. (Section 4a details the low-confidence template; this is a first cut meant to be improved.)
6. **Not a re-post (LOCKED).** If the same client + same/near-identical title was created within ~30 days, **do NOT send** — it's the duplicate-posting-for-a-2nd-hire pattern and the TA/sourcer already got the original briefing. Suppress silently (log it in `role_briefings` as suppressed for audit).
7. **Never briefed before.** One row per job in the `role_briefings` ledger (`job_id` PK) → exactly-once, plus an audit trail of what was claimed.

### 2d. The settling problem (why "on creation" ≠ "the instant the row appears")

Bubble jobs are usually created as a near-empty shell and filled in over the following minutes/hours (title → category → sourcer). So the rule is **"fire once the job is ready AND has survived a bit,"** not on first sight. Implementation: on each pull, select jobs first seen in the lookback window that now pass filters 1-4 and aren't in `role_briefings`. A shell simply doesn't qualify until a later pull once its fields populate; a job archived within a day never qualifies. That one "ready and still alive" gate converts the 50% noise into clean sends with no hand-maintenance.

### 2e. Build size

Same shape as the existing roles weekly bot: one edge function on a cron (fires after each Keboola jobs pull), one new `role_briefings` table, plus the matching + render logic from this spec. ~1 day, most of it the message assembly already prototyped twice. The filter funnel is ~30 lines of SQL.

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

### 4a. Two confidence modes

- **Full briefing** (≥ N similar roles): Sections 1-5 as below.
- **Low-confidence briefing** (< N similar roles, LOCKED — Blake 2026-07-17): honest and useful, not padded with fake precision. Roughly:
  > 🆕 **New role: [title], [location]**
  > ⚠️ We haven't run many roles like this — only **[k] loosely related** (matched on [widest rung that returned anything]). Treat the numbers as a rough steer, not a benchmark.
  > **Somewhat related roles:** [list the k roles: client · title · time-to-hire].
  > **People who've run something close:** [TAs/sourcers from those roles + any adjacent category].
  > _Ask for the full picture anytime._

  No fabricated medians off a handful of roles. If literally nothing relates (even the widest rung is empty), fall back to the org-wide `tth_monthly` benchmark and say so. This template is a deliberate first cut — flagged to improve with use.

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
- Below the similar-roles floor: honest low-confidence briefing (Section 4a), never silence. Org-wide `tth_monthly` benchmark only as the last resort when nothing relates at all.
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

## 9. Decisions

**Locked by Blake 2026-07-17:**

1. **Recipients:** DM to both TA and sourcer. No channel posting.
2. **Timing:** send on detection (up to 4x/day, one role at a time).
3. **Region groups:** grouped regions (DACH, Nordics, Iberia, CEE, UK/IE, remote-EU, ...) as proposed.
4. **Detection:** pull-based off the Keboola jobs extractor (not a Bubble webhook) for v1.
5. **Team feedback:** positive (2026-07-17).

**Filter thresholds (locked by Blake 2026-07-17):**

6. **Low-history roles:** always send, in an honest low-confidence mode (Section 4a) — "we don't have many similar roles, here's what's loosely related, here's who's run something close." First cut, to be improved with use. Never stay silent.
7. **No sourcer assigned:** send anyway. TA-only DM when there's no sourcer; both when present. A missing sourcer never holds the briefing.
8. **Re-posts:** do NOT send. Same client + same/near title within ~30 days is a duplicate posting; suppress silently (logged for audit).

**Still to tune during build (not blockers):** exact value of N, the title-similarity test for re-post detection, and the country/region groupings.

**Build status: NOT green-lit.** Concept validated, team feedback positive, trigger + filter design complete. Nothing gets built until Blake says go.

## 10. Out of scope for v1

Embedding matching, raw transcripts, write-back to Bubble, channel posting, per-candidate recommendations (Candidate Finder's job), funnel math, any Tribot/Q&A surface.
