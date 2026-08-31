# Interview Stages in Reporting — Build Plan

Goal: make the Interview 1 / 2 / 3 stages visible across the everyday reporting, the same way Move to ATS and Offer are, so we can see where candidates fall off between ATS and Offer and read the conversion between each interview round.

Status: **Step 1 (Project Dashboard) SHIPPED 2026-08-31.** Transform v19 + render + App.jsx (commit fb81261) live; Int 1/2/3 columns between ATS and Offered, additive (ATS/Offer/Hired unchanged, verified). Waves 2-3 still pending (WBR/MBR/TS/Weekly/IR, then Circle/Tribe Bot/Slack bot). Asterisk for old rows deferred (needs per-job on_new_pipeline flag). See PROJECT_LOG.md 2026-08-31. Last updated 2026-08-31.

---

## 1. The model (decided)

Add three columns, Interview 1, Interview 2, Interview 3, between Move to ATS and Offer, everywhere the funnel is shown:

Move to ATS → Interview 1 → Interview 2 → Interview 3 → Offer → Hired

- **Old roles stay exactly as they are.** ATS → Offer → Hired. We do not touch them, and we ignore the custom interview stage names people invented (Onsite, Culture, etc.).
- **New-pipeline roles populate the three columns.**
- **Old / historical roles show a dash and an asterisk** in the three interview columns (no interview events ever existed for them). This is expected, not a bug. It self-cleans as old roles phase out.
- **Purely additive.** ATS, Offer, Hired numbers do not change. We only add columns computed from the Interview 1/2/3 events. Lower risk than the ATS week-shift fix.

### One shared definition (must be identical on every surface)
- Interview 1/2/3 counts come straight from `moved_to_stageType` = 'Interview 1' / 'Interview 2' / 'Interview 3', at whatever grain the surface uses (role-week, recruiter-week, client-week, etc.).
- Interview COUNTS: old roles contribute 0. Simple.
- Interview CONVERSION (ATS→Int1, Int1→Int2, etc.): computed over new-pipeline roles ONLY. If you divide by total ATS (which includes old roles that can never reach an interview column), the rate looks artificially terrible. Scope the denominator to new-pipeline roles using `on_new_pipeline` (already exists on the coverage table and in candidate_stage_rungs).

### Old vs new handling by surface type
- **Role-level views** (one row per role): old row shows dashes + asterisk. Clean.
- **Person-level views** (one row per recruiter, aggregating many of their roles): you cannot asterisk a person because their row blends old and new roles. So:
  - Headline funnel (Contacted → ATS → Offer → Hired) stays all-roles, unchanged.
  - Interview columns are volume across that person's new-pipeline roles.
  - Interview conversion uses a new-pipeline base.
  - Add a small context cell per person, e.g. "3 of 4 roles on new pipeline", so the smaller interview numbers make sense.

---

## 2. Surface inventory

Every funnel tab is fed by its own Keboola table, so the real work is adding the interview counts in each transform, then surfacing them in each React tab. The frontend is the easy half.

| Surface (React tab) | Data source (Keboola) | Grain | Change |
|---|---|---|---|
| Project Dashboard | project_dashboard (01kpqh9r…) + event-attr sibling (01ks4qf…) | client / job / week | add INT1/2/3 counts between ATS and OFFERED |
| WBR | WBR/MBR weekly aggregations (01kpr0tr0…) → wbr_actuals / wbr_weekly | recruiter / week | add INT1/2/3 + person-level "on new pipeline" context |
| MBR | same aggregation transform → mbr tables | recruiter / rolling window | same as WBR |
| TS Summary | ts_weekly / ts_conversion (same aggregation transform) | sourcer / week | add INT1/2/3 to sourcer funnel + the 8-stage funnel chart |
| Weekly Summary | weekly_summary (Andy's PBI port) | dim / week | add INT1/2/3 to the Viewed→…→ATS→Offers→Hired funnel |
| Internal Recruitment (IR) | ir_funnel_jobweek / ir_dq_by_stage | job / week | add INT1/2/3 (IR = Tribe's own roles; same shape) |

### Different shape
- **Time to Hire (TTH). DEFERRED (Blake, not for now).** Would show time-in-interview-stage. Revisit later.
- **New Project Health.** Looks like a RAG/health view, not a stage funnel. Confirm before deciding; probably no.

### Downstream / outside the recruiting dashboard
- **Circle** (bark8922/tribe-circle). Purely downstream: `build_circle_data.py` + `inject_jobs.py` read the recruiting dashboard's published data (`wbr_actuals`, `ts_weekly`, `project_dashboard.rows`) and re-slice it per member; rebuilt via the `notify_circle` GitHub dispatch after each recruiting refresh.
  - **DECISION (Blake): interviews ONLY on the by-job table, NOT the top target circles/gauges.** The top per-member target gauges stay top-of-funnel (Contacted / Screen / ATS with targets) — no interview columns. The per-job breakdown (`inject_jobs.py`, the job list) gets Interview 1/2/3.
  - No new data work: once Project Dashboard carries INT1/2/3, `inject_jobs.py` (which reads `project_dashboard.rows`) can pick them up.
  - Circle-specific work is small and DEPENDENT (must come after Project Dashboard): add int1/2/3 to `METRIC_FIELDS` in `inject_jobs.py` and surface the columns in the job-level table in `circle.html`. Leave `build_circle_data.py` member gauges untouched.
- **Tribe Bot** (Candidate DQ by Stage, 01m0hpf…). **DECISION (Blake): expose Interview 1/2/3; the collapsed single "Interview" rung should not exist anywhere.** Change the stage ladder to Move to ATS → Interview 1 → Interview 2 → Interview 3 → Offer → Hired. Old-role interview-band events (Onsite / Culture / Final Interview / Call with Client) fold back to Move to ATS.
  - Supabase path is automatic: transform 01m0hpf… → writer 01m0htjpz… pushes candidate_dq.json.gz to GitHub → Onyx loader on the DigitalOcean droplet loads it into Supabase daily (build 09:00, Supabase load 10:00 Prague). Changing the transform ladder is the ONLY change needed; the new rungs flow through the gz into Supabase with no bot/Supabase edits.
- **tribe-job-intel Supabase + Slack roles bot** (project garopkilxgpcmlkiqvbg). VERIFIED: this Supabase DOES carry the funnel, in several tables, all stopping at ATS → Offered → Hired with NO interview columns:
  - `dash_funnel` (client/job/TA/TS/week, 29k rows): viewed…actual_screens, ats, offered, hired.
  - `pipeline_weekly_cache` (job/week, 14.5k) and `pipeline_weekly_cache_by_ts` (sourcer/week, 12k): same funnel columns.
  - `weekly_updates.pipeline_snapshot` (jsonb per role/week): the per-role snapshot the weekly Slack digest posts.
  - `dash_candidate_dq` (92k): the Tribe Bot's DQ data (has stage / stage_detail / date_ats / date_interview) — covered by decision 1.
  - So the Slack roles bot IS in scope. Chain to update (contingent on the upstream funnel change): (1) add int1/2/3 columns to the source that loads these tables, (2) add the columns to the Supabase table schemas, (3) update the loader that populates them, (4) update the digest/bot code that renders the snapshot. The exact loader (edge function that pulls the dashboard export into dash_*/pipeline_weekly_cache) needs tracing when we build this piece.
  - SECURITY (separate from this project): RLS is disabled on 17 tables here incl. dash_candidates (95k) and dash_candidate_dq (92k) — anyone with the anon key can read/modify every row. Flag to Blake; do not auto-enable (would block access without policies).

### Not affected
- Silver Medalists, Profitability, Candidate Finder (no funnel).
- Role Tracker (Roles / Owners / Coverage) — already has Interview 1/2/3; it's the source.

---

## 3. Sequencing

**Step 1 — Project Dashboard (prove the pattern end to end).**
Add INT1/2/3 to the two project_dashboard transforms (weekly funnel + event-attr), surface them in the ProjectDashboardTab with the asterisk treatment, and validate against a couple of known new-pipeline roles (e.g. Account Manager UK South: ATS → Int1 → Int2 → Int3 should match the tracker). This is also the surface where we just did the ATS fix, so we know it well.

**Step 2 — roll the identical pattern to the other core funnels.**
WBR, MBR, TS Summary, Weekly Summary, IR. Same shared definition. WBR/MBR/TS get the person-level "on new pipeline X of Y" context cell. Validate each transform independently.

**Step 3 — dependent downstream.**
Once WBR/TS/PD carry the interview counts:
- Circle: update `build_circle_data.py`, `inject_jobs.py`, `circle.html`.
- Slack bots: add interview steps to whichever posts the funnel.
- Tribe Bot: apply the decision (collapsed vs 1/2/3).

**Step 4 — optional.**
Time to Hire (time-in-interview-stage), if wanted.

---

## 4. Risk

- Additive: ATS / Offer / Hired totals do not move on any surface. Only new columns appear.
- Main care items:
  1. One shared "Interview 1/2/3" definition, byte-identical across every transform, so surfaces can never disagree.
  2. Interview conversion denominators scoped to new-pipeline roles (else diluted by old roles).
  3. Standard per-transform validation; roll out one surface at a time.
- The whole thing is transitional: once every role is on the new pipeline, the asterisks and the "X of Y" context disappear on their own.

---

## 5. Decisions (resolved 2026-08-31)

1. Tribe Bot: **expose Interview 1/2/3.** Remove the collapsed "Interview" rung everywhere; old interview events fold to Move to ATS.
2. Time to Hire: **deferred**, not now.
3. Circle: **interviews on the by-job table only**, not the top target gauges.
4. Bots via Supabase:
   - Tribe Bot: covered by decision 1 — the DQ transform change flows to Supabase automatically (gz → droplet loader). No extra work.
   - Slack roles bot (tribe-job-intel Supabase): **IN SCOPE (verified).** The funnel is mirrored there (dash_funnel, pipeline_weekly_cache, weekly_updates.pipeline_snapshot), all without interview columns. Needs schema + loader + digest updates, contingent on the upstream funnel change. Trace the exact loader when building this piece.

Note (separate security item): RLS disabled on 17 tables in the tribe-job-intel Supabase — flagged to Blake, not auto-fixed.

Next action: build Step 1 (Project Dashboard) and validate, then proceed down the sequence.
