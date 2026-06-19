# Tribe Recruiting Dashboard — Project Plan

**Date:** March 30, 2026
**Author:** Blake Barkley
**Goal:** Replace Keboola + Power BI with a self-hosted recruiting/staffing dashboard, following the same architecture pattern that already works for the financial dashboard.

---

## 1. Current State & What We're Replacing

### What Keboola Does Today (from Frantisek's report)

The Keboola pipeline runs 6x daily and takes ~90–120 minutes per run:

| Phase | What it does | Duration |
|-------|-------------|----------|
| Extract 0 | Bubble.io incremental + Google Drive | ~5 min |
| Extract 1 | Bubble.io full + Python transform + Talent Locations SQL | ~10 min |
| Geocoding | Google Geocoding enrichment | ~3 min |
| Transform 1 | Data Preparation V2 — 50 inputs → 16 outputs | ~50 min |
| Transform 2 | Revenue pipeline — invoicing → client_cost | ~15 min |
| Load | Snowflake writer (Data Gateway) | ~5 min |
| Reports | Power BI refresh (4 datasets) | ~10–20 min |

### The 17 Reporting Tables We Need to Reproduce

| Category | Tables |
|----------|--------|
| Core entities | job, client, talent, candidate, user |
| Recruitment pipeline | candidate_stage, event, screen, job_ai_filter |
| Details | talent_email, talent_position, talent_employer, screen_techstack, screen_lang |
| Business metrics | job_goal, analytic, client_cost |

### What Power BI Shows (4 datasets)

| Dataset | Purpose |
|---------|---------|
| TRIBE.XYZ | Main recruiting performance dashboards |
| TRIBE.XYZ DEV | Development/testing version |
| Client Alpas | Client-specific recruiting reporting |
| Client Circula | Client-specific recruiting reporting |

### Cost Savings

| Item | Monthly | Annual |
|------|---------|--------|
| Keboola subscription | ~€5,000 | ~€60,000 |
| Power BI licenses | ~€1,700 | ~€20,400 |
| Andy's time (dashboard maintenance) | ~€1,300 | ~€15,600 |
| **Total** | **~€8,000** | **~€96,000** |

---

## 2. Why This Is More Complex Than the Financial Dashboard

The financial dashboard pulls from 4 CSV sources and produces a single data.json (~900KB). The recruiting dashboard is harder because:

1. **Bubble.io API extraction** — 52 tables, custom REST API, pagination handling, incremental loads, error recovery. No off-the-shelf connector exists (Frantisek is right about that part — but wrong that it takes 2–4 months).
2. **Relational data model** — Candidates flow through stages across jobs, with events, screens, and scoring. The financial dashboard is flat (employee × period × client). Recruiting data has deep joins.
3. **Volume** — Tens of thousands of candidates, hundreds of jobs, millions of events. Need efficient extraction and transformation.
4. **Multiple audiences** — Leadership wants KPIs and trends. BU leads want their pipeline. Recruiters want their personal metrics. Clients want their specific reporting.
5. **Freshness expectations** — Recruiting data should ideally be refreshed more frequently than 4 hours.

### Why It's Still Very Doable

1. **Proven architecture** — The financial dashboard already proves n8n → Python → React → Cloudflare Pages works beautifully.
2. **Sanja and Rodrigo are building related pieces** — Sanja's Supabase backend and Rodrigo's data plugin are complementary, not competing. The recruiting dashboard can consume their data.
3. **You already have Claude + n8n + BambooHR MCP** — The orchestration layer is solved.
4. **Bubble.io API is well-documented** — It's a REST API with cursor-based pagination. Building the connector is ~2 weeks of work, not 2–4 months.

---

## 3. Architecture Design

### Option A: Mirror the Financial Dashboard (Recommended for Phase 1)

```
BUBBLE.IO API                    PIPELINE                     FRONTEND                   HOSTING
─────────────                    ────────                     ────────                   ───────
Bubble.io REST API       ─┐
  52 tables (incremental)  │
  Jobs, Candidates,        │
  Talent, Events,          ├─→  recruiting_pipeline.py  ─→  recruiting_data.json  ─→  RecApp.jsx       ─→  GitHub
  Screens, Stages          │    Extract + Transform         (~2-5MB)                   React + Vite         (bark8922/tribe-recruiting)
                           │    Join + Aggregate                                       Recharts + TW            │
Google Sheets             ─┤                                                                               Cloudflare Pages
  (client goals, targets)  │                                                                               (auto-deploy)
                           │
BambooHR (via MCP)        ─┘
  (recruiter names,
   department mapping)
```

**Why this works:** Same pattern as the financial dashboard. Single Python pipeline produces a JSON blob. Single-file React app renders it. No database to manage. No server to maintain.

**Tradeoff:** Data.json could be 2–5MB (vs 900KB for financial). Still fine for a Cloudflare Pages site — compresses to ~500KB with gzip.

### Option B: Supabase-Backed (Consider for Phase 2)

```
Bubble.io API  ─→  Python ETL  ─→  Supabase (PostgreSQL)  ─→  React App (API calls)  ─→  Cloudflare
```

**Why you might want this later:** If the data grows beyond what a single JSON file can handle, or if you need real-time filtering/querying that client-side JS can't do efficiently. Sanja is already setting up Supabase ($25/month) for candidate/transcript data — you could share the instance.

### Recommendation

**Start with Option A.** It's proven, fast to build, zero ongoing infrastructure cost, and you can always migrate to Option B later if needed. The pipeline code stays the same — you'd just change the output target from JSON file to Supabase insert.

---

## 4. Phased Build Plan

### Phase 1: Bubble.io Connector (Week 1–2)

**Goal:** Build a Python script that extracts all 52 Bubble.io tables via REST API and saves them as local CSV/JSON files.

**What needs to happen:**
- Get Bubble.io API key and understand the data model (table names, field types, relationships)
- Build `bubble_extract.py` with: cursor-based pagination, incremental extraction (modified_since), rate limiting, error handling with retries, logging
- Map Bubble.io table/field names to clean column names
- Test against all 52 tables, validate row counts against Keboola's output
- Set up n8n workflow to trigger extraction on schedule

**Key technical decisions:**
- Incremental vs full extraction: Start with full for correctness, add incremental once validated
- Storage format: JSON files per table (same as Keboola's extract phase)
- Where to run: n8n can trigger the Python script, or we can use a scheduled task like the financial dashboard

**Deliverable:** `bubble_extract.py` that produces 52 clean data files matching what Keboola currently extracts.

### Phase 2: Transformation Pipeline (Week 2–3)

**Goal:** Build the SQL/Python transformations that turn raw Bubble.io data into the 17 reporting tables.

**What needs to happen:**
- Get the 4 Snowflake SQL transformations from Keboola (Frantisek can export these — ask him)
- Translate Snowflake SQL to Python/pandas or DuckDB (local SQL engine, no server needed)
- Replicate the key transformations:
  - Talent Locations (geocoding enrichment)
  - Data Prep V2 (the big one — 50 inputs → 16 outputs)
  - Revenue pipeline (invoicing → client_cost)
  - Python transform (already Python, likely portable)
- Validate output against Keboola's reporting-v2 tables

**Key technical decision:**
- **DuckDB** (recommended): Run SQL transformations locally without a server. You can literally copy the Snowflake SQL with minor syntax changes. This is the fastest path.
- Pandas: More Pythonic but harder to port SQL transformations to.

**Deliverable:** `recruiting_transform.py` that produces the 17 reporting tables from raw extracts.

### Phase 3: Dashboard Frontend (Week 3–5)

**Goal:** Build the React dashboard that replaces Power BI.

**Tabs/Views (from original project plan + meeting discussions + Keboola data model):**

| Tab | Key Metrics | Audience |
|-----|------------|----------|
| **Overview** | Open roles, active candidates, hires this month/quarter, time-to-fill, pipeline velocity | Leadership |
| **Pipeline / Funnel** | Funnel visualization (Contacted → Screens Scheduled → Screens Completed → Moved to ATS → Offers → Hires), conversion rates by stage, bottleneck identification | Leadership, BU leads |
| **New Role Estimate** | How many people needed at each stage to make 1 hire (e.g., "1.2 offers, 5 ATS, 10 screens, 100 contacted per hire"), historical trend of these ratios | Leadership, Nenad |
| **Candidate Analytics** | Pipeline by job/role, rejection tracking (HM rejected without meeting vs. HM met and rejected vs. other), lost reasons, time-to-hire by stage, sourcing channel effectiveness | Recruiting team, BU leads |
| **Recruiter Performance** | Submissions per recruiter, screens completed, hires, per-hiring-manager acceptance/rejection rates, activity metrics, utilization | Ops, team leads, Nenad |
| **Client Delivery** | Open roles by client, fill rate, candidates submitted, time-to-fill by client, hiring velocity per BU, pipeline health (candidates vs. open positions ratio), cost per hire | BU leads, client reporting |
| **Jobs** | Individual job detail — candidates per stage, days open, hiring manager, recruiter assigned, job_ai_filter scores | Recruiters |

**Detailed Tab Specs (from project plan v2):**

**A. Hiring Pipeline / New Role Estimate:**
- Funnel Visualization: Contacted → Screens Scheduled → Screens Completed → Moved to ATS → Offers → Hires
- New Role Estimate: Shows how many people needed at each stage to make 1 hire
- Conversion rates between each funnel stage, with historical trends
- Per-Hiring-Manager acceptance/rejection rates
- Per-BU hiring velocity and pipeline health

**B. Candidate Analytics:**
- Pipeline by Job/Role: Active candidates at each stage per open position
- Rejection Tracking: HM rejected without meeting vs. HM met and rejected vs. other reasons
- Lost Reasons: Why candidates drop out or are rejected (skills, culture, compensation, etc.)
- Time-to-Hire: Average days from sourcing to offer acceptance, broken down by stage
- Sourcing Performance: Which channels produce the best candidates

**C. Recruitment Performance by BU:**
- Hiring Velocity: Time-to-fill per business unit
- Pipeline Health: Candidates in pipeline vs. open positions ratio
- Cost per Hire: Recruiting costs allocated per successful placement

**Key dashboard users (from project plan v2):**
| Person | Role | What they need |
|--------|------|---------------|
| Nenad | Recruiting funnel/pipeline metrics | Primary user — Pipeline, New Role Estimate |
| Vlad | Leadership — uses KPI overview | Overview, Pipeline summary |
| Salem | BU lead — uses for client calls | BU-filtered Client Delivery |
| Kristjana | BU lead | BU-filtered Client Delivery |
| Jacopo | BU lead | BU-filtered Client Delivery |
| Simon | Recruiting ops | Recruiter Performance |
| Chené | Power BI dashboard user | Overview, KPI summary |

**Technical approach:**
- Same stack as financial dashboard: React + Vite + Recharts + Tailwind
- Single-file App.jsx with tab-based navigation
- Data loaded from recruiting_data.json (static import)
- Period filters (month/quarter/year), BU filters, client filters, recruiter filters
- Client-specific views (replaces Client Alpas / Client Circula Power BI datasets)
- Access control: BU leads see only their BU data (start with separate URLs per BU, migrate to Cloudflare Access later)

**Deliverable:** Working recruiting dashboard deployed to Cloudflare Pages.

### Phase 4: Automation & Integration (Week 5–6)

**Goal:** Automate the full pipeline and integrate with existing systems.

**What needs to happen:**
- n8n workflow to trigger `bubble_extract.py` → `recruiting_transform.py` → build → deploy
- Schedule: 4x daily (every 6 hours) to start, can increase later
- Monitoring: Slack notification on success/failure (same as financial dashboard)
- GitHub Actions: auto-deploy to Cloudflare Pages on push to main
- Integration with Sanja's Supabase (share candidate data, avoid duplication)
- Client-specific URL paths or auth for client reporting views

**Deliverable:** Fully automated, self-healing pipeline running on schedule.

### Phase 5: Validation & Cutover (Week 6–7)

**Goal:** Run in parallel with Keboola/Power BI, validate, then cut over.

**What needs to happen:**
- Run both systems side-by-side for 2 weeks
- Compare numbers: row counts, KPI values, trend consistency
- Get feedback from key stakeholders (Jacopo, Kristjana, Salem, Simon)
- Fix discrepancies
- Give Andy 2-month notice (per Martin's agreement from March 23)
- Cancel Keboola after parallel run validates

**Deliverable:** Keboola and Power BI decommissioned. New dashboard is the single source of truth.

---

## 5. What to Ask Frantisek

Before the call next week, request these from Frantisek (they'll dramatically speed up the build):

1. **Export the 4 Snowflake SQL transformations** — The SQL code for Talent Locations, Data Prep V2, Revenue pipeline, and the Python transform. This is the most valuable thing he can give you. Even if he's reluctant, these are YOUR transformations running on YOUR data.
2. **Bubble.io table schema** — Column names, types, and relationships for all 52 tables. Keboola's extractor config has this.
3. **Reporting-v2 sample data** — A snapshot of the 17 output tables so we can validate our pipeline against known-good output.
4. **Extractor configuration** — The Bubble.io API endpoints, pagination settings, and incremental logic Keboola uses.

Frame it as: "We want to understand our data better regardless of platform decisions" — not "we're leaving."

---

## 6. Dependencies & Coordination

### Sanja's Work (Supabase + Ashby ATS)
- She's building candidate data centralization in Supabase
- Her backend stores transcripts, interview feedback, AI-generated rejection drafts
- **Synergy:** Our recruiting dashboard can pull enriched data FROM Supabase once she's live, or push transformed Bubble.io data TO Supabase for her tools to use
- **Risk:** Two parallel databases (our JSON + her Supabase) could create inconsistency. Coordinate early.

### Rodrigo's Work (Data Plugin + Transcription)
- His Chrome extension automates recruiter screen data entry into the Tribe Dashboard / Bubble
- Uses Assembly AI for transcription with speaker identification
- **Synergy:** His plugin writes data INTO Bubble.io, which our pipeline then extracts. No conflict — his work improves our input data quality.
- **Action:** Invite him to the weekly sync you proposed.

### Andy's Transition
- Currently maintains Power BI dashboards
- Martin agreed to give 2-month notice (from March 23 1:1)
- **Critical:** Get the Power BI report definitions and any custom DAX measures before his access ends. These are the specification for what the recruiting dashboard needs to show.

---

## 7. Risk Assessment

| Risk | Likelihood | Impact | Mitigation |
|------|-----------|--------|------------|
| Bubble.io API changes or rate limits | Medium | High | Build with retry logic, monitor API changelog, maintain fallback to Keboola for 1 month |
| Data model differences between Bubble.io raw and Keboola-transformed | High | Medium | Get Keboola SQL transforms early, validate against known output |
| Stakeholder resistance (people used to Power BI) | Medium | Medium | Build dashboard with same metrics, get early feedback from Jacopo and Kristjana |
| Scope creep (everyone wants custom views) | High | Medium | Deliver core tabs first, add views iteratively based on usage |
| Sanja's Supabase work conflicts with our approach | Low | Medium | Weekly sync, agree on data ownership boundaries |

---

## 8. Timeline Summary

| Week | Phase | Deliverable |
|------|-------|-------------|
| 1–2 | Bubble.io Connector | `bubble_extract.py` extracting all 52 tables |
| 2–3 | Transformation Pipeline | `recruiting_transform.py` producing 17 reporting tables |
| 3–5 | Dashboard Frontend | Working React dashboard with 6 tabs |
| 5–6 | Automation | Automated pipeline running 4x daily |
| 6–7 | Validation & Cutover | Parallel run, stakeholder sign-off, Keboola decommission |

**Total: ~7 weeks to full independence from Keboola + Power BI.**

This is realistic because:
- The financial dashboard proved the architecture works
- You have Claude + n8n + BambooHR already operational
- The hardest part (Bubble.io extraction) is well-understood REST API work
- DuckDB lets us port Snowflake SQL with minimal changes

---

## 9. Immediate Next Steps

1. **This week:** Ask Frantisek to export the Snowflake SQL transformations and Bubble.io extractor config. Frame it as data audit, not migration.
2. **This week:** Get Bubble.io API key and test basic extraction (1-2 tables) to validate approach.
3. **Monday:** Meeting with Rodrigo and Sanja — align on Supabase as shared backend vs separate systems.
4. **Before Andy's notice:** Export all Power BI report definitions and DAX measures.
5. **Start building:** Phase 1 (Bubble.io connector) can begin immediately with API access.

---

## 10. Frantisek's Concerns — Addressed

| His concern | Our answer |
|-------------|-----------|
| "No off-the-shelf Bubble.io connector" | Correct — but we're building a custom Python extractor, not using Fivetran. Bubble's REST API is well-documented with cursor pagination. |
| "2–4 months of data engineering work" | That estimate assumes rebuilding Keboola's full orchestration layer. We're using n8n (already running) + Python + DuckDB — much lighter. 7 weeks realistic. |
| "Need to manage your own Snowflake" | We're using DuckDB (in-process, zero infrastructure). No database to manage. |
| "Ongoing maintenance burden" | The financial dashboard runs itself with a scheduled task. Same pattern here. Failure notifications via Slack. |
| "Parallel run needed" | Agreed — we're planning 2 weeks of parallel operation before cutover. |

His incentive is to keep you as a customer. The €96K/year savings and full control over your data stack are worth the 7-week investment.

---

## 11. Parallel Work Streams (Sanja + Rodrigo)

From the March 30 call (Rodrigo/Blake/Sanja) and March 26 call (Sanja/Blake):

**Sanja's recruiting dashboard (in development):**
- Adaptive dashboards for leadership and recruiters (personalized views)
- Backend: Supabase ($25/month) — SQL tables, background jobs, AI integration
- Ashby ATS integration (pending server activation)
- Rejection email automation via Slack (AI-drafted, manual approval)
- LinkedIn profile extraction via Puppeteer + Claude AI
- Candidate experience surveys (API-driven from Ashby)

**Rodrigo's data plugin (live, rolling out):**
- Chrome extension for recruiter screen data entry (5 clicks vs 5-6 min)
- Assembly AI transcription with speaker identification
- Transcripts stored in Google Sheets, shared with leads
- Now being extended for intake call automation
- 17 TA users onboarded as of March 27

**How these fit together:**
- Rodrigo's plugin writes data INTO Bubble.io → our pipeline extracts it (better input data quality)
- Sanja's Supabase can serve as a shared data layer if we move to Option B architecture
- Our recruiting dashboard is the **company-wide reporting layer** — Sanja's tools are the **recruiter workflow layer**
- Weekly sync (you, Rodrigo, Sanja) to keep aligned — first meeting Monday

---

## 12. Keboola MCP — Quick Wins Before Migration

You have a Keboola MCP connector available right now. Before building the full Bubble.io extraction, we can use it to:

1. **Query the existing reporting-v2 tables** to understand the exact data model
2. **Export sample data** to build and validate the dashboard frontend while the pipeline is being built
3. **Get the SQL transformation code** (the 4 transformations) to port to DuckDB

This means we can start building the dashboard UI immediately using Keboola as a temporary data source, then swap in the Bubble.io pipeline later. No need to wait for the full pipeline to be done before making progress on the frontend.

---

## 13. Relationship to Master Project Plan

This document is a detailed expansion of Section 4.2 (Recruiting/Staffing Dashboard) from `Tribe_Project_Plan_v2.docx`. That master plan covers all 5 dashboards:

1. Finance Dashboard — **DONE** (v5, deployed, auto-refreshing)
2. Recruiting/Staffing Dashboard — **THIS DOCUMENT** (starting now)
3. KPI Leadership Dashboard — Not started (depends on #1 + #2)
4. Sales Dashboard — Not started (needs HubSpot API key)
5. Ops Dashboard — Future phase
