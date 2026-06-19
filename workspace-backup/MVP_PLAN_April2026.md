# Recruiting Dashboard — MVP Plan (April 2026)

**Author:** Blake (with Claude)
**Date:** 2026-04-07
**Decision deadline:** Want Martin sign-off this week
**Ship deadline:** 2026-04-30 (Andy's last working day, internal scope deadline)
**Hard deadline:** 2026-08-31 (Power BI / Keboola contract expiry)

---

## TL;DR

Stop trying to replace the whole Power BI + Keboola stack by April 30. Instead:

1. **Ship two reports by April 30** — VBR (leadership review) and Project Dashboard — by reading directly from Keboola's existing `reporting-v2` tables. Skip the Bubble extraction rebuild for now.
2. **Capture Andy's brain in writing** before he leaves end of April. Extract the DAX measures from his `.pbix` file, document each one in plain English + a SQL approximation.
3. **Make the Keboola decision in May, not April.** With Andy gone (€72K saved) and a 4-month runway to August 31, you have time to either negotiate Keboola down or port Frantisek's transforms to a cheaper stack — without doing it under panic.

---

## Why we're cutting scope

### Martin's actual brief (2026-04-07 1:1)

> "Don't rebuild everything. Rebuild VBR and rebuild the project. Like the first project dashboard. That's the two things that we need to get done by the end of this round. And then all of the other transformations. Screw it. It seems like nobody's using it anyways."

He also said: *"We should definitely have it documented and know what was going on there. But let's cut back."* So drop the surface area, keep the institutional knowledge.

### Cost reality check

| Line item | Monthly | Annual | Notes |
|---|---|---|---|
| Andy (data analyst) | ~€6,060 | ~€72,720 | Leaves end of April — already saved |
| Keboola | $1,600 | ~$19,200 | Locked through 2026-08-31 |
| Power BI | ~$100 | ~$1,200 | Locked through 2026-08-31 |
| **Total** | **~€7,700** | **~€93,000** | |

**Andy alone is ~78% of the savings.** That's locked in by his departure regardless of what we do with the tools. Power BI is a rounding error. The remaining decision is just "what do we do about $19K/year of Keboola" — and we have until August 31 to decide. That's not a 3-week problem.

### Real deadlines

| Date | Event | Implication |
|---|---|---|
| 2026-04-30 | Andy's last day | Lose the only person who knows the 30% of business logic in Power BI DAX |
| 2026-08-31 | Power BI + Keboola contracts expire | Must decide: cancel both, cancel one, or renew |

The April deadline is about **capturing Andy's knowledge**, not about killing tools. The August deadline is about **killing tools**. Don't conflate them.

---

## Kill / Keep / Defer

### KEEP for MVP (build by April 30)

**1. VBR (Leadership Review)** — color-coded targets vs. actuals, by client and/or role
- One screen, table-shaped, exportable
- Targets come from a separate spreadsheet (currently Andy's)
- Cadence: weekly refresh is fine — does NOT need 6×/day
- Consumer: Martin + leadership

**2. Project Dashboard** — per-job operational view
- One row per active job: client, recruiter, sourcer, days open, candidates by stage, hires, time-to-fill (first hire only)
- Color-coded health flags (long-open, low-hire-rate, etc. — Andy's "early warnings")
- Cadence: daily refresh is fine
- Consumer: TBD — need to confirm whether this is for leadership or for recruiters themselves

### KEEP for parallel work (capture-only, no code)

**3. Andy DAX extraction doc** — for every measure in his `.pbix` file, write down:
- Measure name
- Plain English description
- DAX formula (copy-paste)
- SQL approximation
- Which dashboard/report uses it
- Any edge cases Andy mentions

This is the deliverable that protects against Andy leaving. It's a Word/Markdown doc, not code. Aim for ~80% coverage by April 30.

### DEFER until May or later

- **Recruiter Performance tab** — Martin said nobody uses it
- **Time-to-Hire trend tab** — Martin said nobody uses it
- **Overview KPI tiles tab** — Martin said nobody uses it
- **Sourcer leaderboards** — needs the manual sourcing-team mapping file from Gustavo; not blocking VBR
- **Chatbot / "ask anything" interface** — Andy explicitly warned this won't work reliably; revisit in 2027 when models are better
- **Full Bubble extraction rebuild** — only needed if we kill Keboola; that decision is for May

### DELETE outright (or hide behind feature flag)

- The 4 dashboard tabs above, in the current React build
- The custom DuckDB transform layer in `transform.py`, IF we're reading from `reporting-v2` directly
- The bulk Keboola CSV → JSON conversion path, IF we're reading from `reporting-v2` directly
- The DigitalOcean VPS pipeline, IF we're reading from `reporting-v2` directly

(All "IFs" depend on confirming the `reporting-v2` route works — see Week 1 plan.)

---

## Architecture proposal — read from `reporting-v2` directly

### Current architecture (as built)
```
Bubble.io API ──→ bubble_extract.py ──→ transform.py (DuckDB) ──→ data.json ──→ React
                  (running on VPS, n8n 3x daily)
```

Problems:
- We're rebuilding what Frantisek already built in Keboola
- Bubble API is slow and brittle (Frantisek: "weeks of work + ongoing maintenance")
- Andy's 30% of patching logic isn't ported yet
- Numbers won't match Power BI by construction — every difference is a debugging session

### Proposed MVP architecture
```
Keboola reporting-v2 (17 tables, refreshed 6x/day, 99% reliable)
        │
        ▼
Daily SELECT via Keboola Storage API or Snowflake creds
        │
        ▼
Single SQL view per report (VBR view + Project view)
        │
        ▼
data.json ──→ React (2 tabs only)
```

Benefits:
- **Same source as Power BI** → numbers match by construction
- **Frantisek's transforms = free** → 4 years of cleanup logic preserved
- **No Bubble extraction problem** → skip the hardest part of the migration
- **Smaller surface area** → easier to validate, easier to ship

What this does NOT solve (by design — defer to Phase 2):
- Andy's 30% DAX layer still needs porting
- Keboola is still costing $1,600/mo until we make a Phase 2 decision

### Phase 2 options (decide in May, execute by August)

| Option | What it means | Pros | Cons |
|---|---|---|---|
| **A. Negotiate Keboola down** | Try to get $1,600 → $500-800/mo on a smaller plan | Lowest effort, preserves everything, Frantisek's work intact | They may say no; still paying for a heavy tool |
| **B. Port transforms off Keboola** | Move Frantisek's SQL to a cheaper backend (DuckDB on VPS, BigQuery, Postgres + dbt). Replace Keboola Flows with n8n or GitHub Actions cron. | Cheap ongoing ($0-100/mo), portable, modern stack | 1-2 months of focused work; you become the on-call when Bubble API breaks |
| **C. Status quo** | Pay Keboola $19K/yr, pocket the €72K Andy savings, move on | Zero migration risk; €72K saved is still huge | Leaves $19K/yr on the table |

**Blake's stated preference:** Option B (keep Frantisek's transforming, move off Keboola). That's the right long-term answer if you have the appetite for the work. **Option A is the right short-term answer** — try to negotiate first (a 30-min call with the Keboola rep), and if that fails, fall back to B with a 3-month runway.

---

## 3-Week Execution Plan

### Week 1 (Apr 7–13) — Validate the path

**Goal:** Prove that reading from `reporting-v2` gives us numbers that match Power BI, so we can confidently delete the rebuild work.

- [ ] Get a non-guest Keboola read token (or Snowflake credentials) so Claude/scripts can SELECT from `reporting-v2`. Currently the token is `guest`-only.
- [ ] Open Power BI Desktop, load Andy's `.pbix` from the leadership data dashboard folder. Save a local copy.
- [ ] Run 5 sanity-check queries against `reporting-v2` and compare to Power BI:
  1. Active job count (should match Project Dashboard top number)
  2. Hires this month, by client
  3. Top 10 jobs by days-open
  4. Time-to-fill for last 20 hires (first-hire-only, EUR-converted, exclude test)
  5. Candidate count by current stage
- [ ] Friday meeting with Frantisek — use the prepared question list (see `FRANTISEK_QUESTIONS.md`)
- [ ] Schedule 30-min Mikhail call before Andy's last day (he's the backup brain for the Bubble event model per Andy)
- [ ] Confirm with Martin: VBR + Project Dashboard are the only two MVPs. Get explicit "yes drop the others" in writing.
- [ ] Get the VBR target spreadsheet from Andy
- [ ] Get the sourcer mapping file from Gustavo (only needed for VBR if it slices by sourcer; otherwise defer)

**Exit criteria for Week 1:** Sanity-check numbers from `reporting-v2` match Power BI within ~5%. Martin has explicitly approved the 2-tab scope cut.

### Week 2 (Apr 14–20) — Build Project Dashboard

**Goal:** Project Dashboard live and validated end-to-end.

- [ ] Strip the React app down to just the Pipeline + Jobs tabs. Hide the rest behind a feature flag or comment them out.
- [ ] Write a single SQL view (`view_project_dashboard`) that joins job + candidate + user + client + job_goal and produces one row per active job
- [ ] Pull view results into `data.json` via a daily script (Python or n8n)
- [ ] Re-skin the React tab to match the new schema
- [ ] Side-by-side comparison vs. Power BI for 20 sample jobs — every discrepancy gets logged and triaged
- [ ] **Mikhail call** — walk through Bubble event semantics, mistaken-stage-move handling, sourcer credit logic
- [ ] **Andy weekly call** — DAX extraction session #1: extract and document the 5–10 most important measures used in his Project view
- [ ] Deploy to Cloudflare Pages, share preview URL with Martin

**Exit criteria for Week 2:** Project Dashboard live, numbers match Power BI within ~5%, Martin can open it.

### Week 3 (Apr 21–30) — Build VBR + finalize Andy doc

**Goal:** VBR live, Andy DAX doc at ~80% coverage, both reports validated.

- [ ] Write SQL view for VBR (`view_vbr`) — joins targets spreadsheet (probably as a small CSV/Google Sheet input) with actuals from `reporting-v2`
- [ ] Build the React VBR tab — color-coded target table (red/orange/yellow/green tiers Andy described)
- [ ] Side-by-side validation vs. Power BI VBR
- [ ] **Andy weekly call** — DAX extraction session #2 + #3: document remaining measures, edge cases, manual workflows (sourcing fixes, stage corrections, etc.)
- [ ] Write the operations runbook: "what to do when a recruiter says the numbers are wrong"
- [ ] Demo to Martin — VBR + Project Dashboard live, both validated
- [ ] **Andy off-boarding call (last day)** — final knowledge transfer, capture anything missed
- [ ] Make a recommendation to Martin on Phase 2 (Option A vs B for Keboola)

**Exit criteria for April:** Both reports live and trusted by Martin. Andy's tribal knowledge captured in a doc that someone else could pick up. Phase 2 recommendation made.

---

## Open questions / risks

1. **Project Dashboard consumer** — leadership view or recruiter self-service view? Affects layout. Need to ask Martin.
2. **VBR cadence** — weekly? bi-weekly? monthly? Affects refresh frequency. Andy implied it's tied to a leadership review meeting.
3. **VBR target spreadsheet ownership** — who maintains it after Andy? Martin? A sourcing lead?
4. **Sourcer mapping file** — needed for VBR if it slices by sourcer. Owner: Gustavo. Need to track down.
5. **Keboola read access** — current MCP token is guest-role. Need to provision either a higher-privilege token, Snowflake creds, or work from cached files on local machine.
6. **Power BI .pbix access** — Blake said he can get it but hasn't yet. Critical path.
7. **What happens to `reporting-v2` after Keboola is cancelled?** — answered by Phase 2 decision (Option A keeps it; Option B replaces it with a portable equivalent).
