# Tribe-Dashboard-Project Folder Inventory — 2026-04-08

**Purpose:** Document what's in `C:\Users\blake\Downloads\Tribe-Dashboard-Project\`, separate Finance from Recruiting cleanly, identify what's already built for recruiting, and decide what to keep / retire / port to the new MVP.

A clean snapshot of the recruiting-relevant subset has been copied to `Recruiting Dashboard\tribe-project-snapshot\` (no Finance files included).

---

## 1. Cleanly separated: Finance vs Recruiting vs Shared

### FINANCE — do not touch (your existing live dashboard)

| Path | Purpose |
|---|---|
| `dashboard-src/App.jsx` (103 KB), `data.json` (1 MB) | Live Finance React app source |
| `dashboard_data_v5.json` (1 MB) | Latest Finance pipeline output |
| `pipeline/build_all.py`, `rebuild_v4.py`, `rebuild_v5.py` | Finance pipeline scripts |
| `data/active_employees.json`, `data/bamboohr/*`, `data/linkedin-backfill/*`, `data/revenue/*` | Finance source data (BambooHR, LinkedIn, revenue CSVs) |
| `tribe-dashboard-dist/*` | Built Finance dashboard (deployed) |
| `n8n-workflows/n8n-workflow-actual-spend.json`, `job-history.json`, `salary-history.json`, `revenue-per-tribster.json` | Finance n8n automations |
| `scheduled-tasks/tribe-dashboard-refresh.md`, `tribe-dashboard-watchdog.md` | Finance scheduled tasks |
| `March_2026_Staff_Costs_Audit.xlsx`, `March_2026_Deployed_Dashboard.xlsx`, `March_2026_Blake_vs_Dashboard_Diff.xlsx` | Finance reconciliation files |
| `docs/Tribe_Dashboard_Project_Plan.docx`, `docs/Tribe_Project_Plan_v2.docx`, `docs/Finance_Dashboard_Audit.docx` | Finance plans/audits |
| `REBUILD_CHECKLIST.md`, `DISASTER-RECOVERY.md`, `APP_FIELDS_EXTRACTION.md`, `FIELD_EXTRACTION_INDEX.md`, `FIELD_EXTRACTION_SUMMARY.txt` | Finance ops docs |
| `archive/Tribe_Financial_Dashboard.html`, `archive/tribe-dashboard.zip`, etc. | Older Finance dashboard versions |
| `skill/SKILL.md`, `skill/references/*`, `skill/tribe-dashboard.skill` | Finance dashboard skill |

**Verdict: 100% Finance, recruiting work should never write to any of this.** Keep it isolated.

### RECRUITING — what we're working on (snapshotted to workspace)

| Path | Size | Purpose | Status |
|---|---:|---|---|
| `recruiting-pipeline/bubble_extract.py` | 24 KB | Async aiohttp Bubble.io REST extractor, 40 endpoints, cursor pagination | Built, never validated end-to-end (sandbox can't reach Bubble.io) |
| `recruiting-pipeline/transform.py` | 48 KB / 1,136 lines | DuckDB port of Keboola parts 0+1+3 → produces dashboard data.json | Built, ran in test on cached data |
| `recruiting-pipeline/run_pipeline.py` | 4 KB | Orchestrator: extract → transform → write | Built |
| `recruiting-pipeline/keboola_csv_to_json.py`, `keboola_export_events.py` | 18 KB | One-shot helpers to bootstrap from Keboola CSV exports | Used once to seed data |
| `recruiting-pipeline/n8n-workflow.json`, `n8n-workflow-v2.json`, `n8n-debug-workflow.json` | 35 KB | n8n workflow definitions for 3x daily refresh | Defined, **never deployed live** |
| `recruiting-dashboard/src/App.jsx` | 32 KB / 779 lines | React dashboard, **6 tabs already wired**: overview, pipeline, recruiter, client, tth, jobs | Built, runs on cached data.json |
| `recruiting-dashboard/src/data.json` | 494 KB | Latest cached output from a transform.py test run | Stale |
| `recruiting-dashboard/src/main.jsx`, `index.css`, `index.html`, `vite.config.js`, `package.json` | — | Vite + React 18 + Recharts 2 + Tailwind CDN | Builds clean (~185 KB gzipped) |
| `recruiting-dashboard/dist/*` | — | Last production build | Stale |
| `recruiting-dashboard/docs/BUILD_LOG.md` | 6 KB | Architecture decisions log | Up to date through 2026-03-30 |
| `docs/Recruiting_Dashboard_Project_Plan.md` | 21 KB | The previous 7-week build plan | **Now superseded by `MVP_PLAN_April2026.md`** |
| `tribe-recruiting-dashboard.skill` | 5 KB | Skill bundle for recruiting dashboard | Already installed in your skills |

### SHARED — Frantisek's SQL (the Rosetta stone)

This is the most valuable thing in the folder for our MVP work. It's the actual source SQL that produces `reporting-v2`.

| File | Size | What it is |
|---|---:|---|
| `docs/keboola-sql/PROD_Data_preparation_V2--part_0_-_temporary_tables.sql` | 5.6 KB | Stage type mappings, lookup temp tables |
| `docs/keboola-sql/PROD_Data_preparation_V2--part_1_-_bubble_data.sql` | **39 KB** | The big one — 50 inputs → 16 reporting-v2 outputs from Bubble |
| `docs/keboola-sql/PROD_Data_preparation_V2--part_2_-_recruitee_data.sql` | 8.9 KB | Legacy Recruitee ATS merge — irrelevant for 2025+ data |
| `docs/keboola-sql/PROD_Data_preparation_V2--part_3_-_final_tables.sql` | 7.7 KB | Union dedup logic + `hired_order` + `date_first_hired` backfill |
| `docs/keboola-sql/PROD_Data_preparation_V2--part_4_-_Andy.sql` | **557 bytes** | **Andy's only custom Snowflake contribution: 2 trivial CREATE TABLE statements (`analytic`, `job_ai_filter`)** |
| `docs/keboola-sql/Revenue_-_1._unpivoting--code.sql` | 391 B | Revenue unpivot (finance) |
| `docs/keboola-sql/Revenue_-_2._data_prep--code.sql` | 4.6 KB | Revenue data prep (finance) |
| `docs/keboola-sql/talent_location--code.sql` | 460 B | Geocoding helper (negligible) |
| `docs/keboola-sql/reporting_v2_schema.md` | 5.7 KB | **Full schema for all 17 reporting-v2 tables with row counts** |
| `docs/keboola-sql/bubble_extractor_config.md` | 3.3 KB | Bubble.io extractor settings (endpoints, pagination) |

---

## 2. The big strategic finds

### Finding 1 — Andy's Snowflake footprint is 557 bytes

`part_4_-_Andy.sql` is the only file with Andy's name on it. It contains exactly two `CREATE OR REPLACE TABLE` statements: `analytic` (a row count rollup of `bubble_Analytic`) and `job_ai_filter` (a flat select from `bubble_JobAiFilter`). **Neither is used by the WBR or Project Dashboard.**

Implication: when Andy leaves, **none of the reporting-v2 SQL transformations leave with him**. The 39 KB of part_1 + 7.7 KB of part_3 (the actually-load-bearing SQL) are 100% Frantisek's. Andy's value-add lives entirely in (a) the Power BI DAX (already extracted yesterday into `POWERBI_DAX_MEASURES.md`) and (b) the WBR target spreadsheet that we still need to grab from him. The data engineering itself is Frantisek-owned and survives Andy's departure.

This **further weakens any "ship before Andy leaves" panic argument** — the part of Andy that's actually irreplaceable is small and we already captured most of it.

### Finding 2 — Frantisek's part_3 SQL gives us the formulas for `hired_order`, `hired_views`, `hired_contacts`, `hired_screens`, and `date_first_hired`

These columns I've been treating as opaque from `reporting-v2` are computed by:

```sql
row_number() over (partition by c.job_id order by cs.date_hired) as hired_order
-- then NULL'd if date_hired is null
-- hired_views/contacts/screens computed as count of preceding events on the same job
-- date_first_hired = min(date_hired) per job_id
```

This means the SQL layer of the MVP can reproduce, audit, or override any of these without re-running the Bubble extraction. Time-to-fill, time-to-hire, and "first hire only" semantics all live in plain SQL we now have in front of us.

### Finding 3 — I already built ~80% of the React UI

`recruiting-dashboard-src/App.jsx` (779 lines) has all 6 tabs wired up with Recharts: overview KPIs + funnel + monthly trend, per-job pipeline table, recruiter performance bars, client delivery WBR table with target color-coding, time-to-hire breakdown, and a job-detail tab. It currently consumes a `data.json` produced by `transform.py`.

**What this changes for the MVP:** the WBR + Project Dashboard scope cut doesn't mean building from zero — it means **swapping the data source under the existing UI**. Instead of `bubble_extract → transform.py → data.json`, we point the same App.jsx at a SQL view materialized from `reporting-v2`. Two of the six tabs (Overview-as-WBR, Jobs-as-Project-Dashboard) ship as-is once the data shape matches; the other four can stay hidden or be deleted.

This collapses the MVP from "build a dashboard" to "write one SQL view + adapt one data.json shape." Days, not weeks.

### Finding 4 — The blocker on the old plan was the wrong blocker

`BUILD_LOG.md` says "Sandbox can't reach Bubble.io API — DNS resolution fails." That's why end-to-end testing never happened. But for the MVP we don't need Bubble at all — `reporting-v2` is already populated by Frantisek 6× daily, and we have query access (proven yesterday). **The bubble_extract.py path is the right Phase-2 solution but the wrong Phase-1 solution.**

### Finding 5 — n8n workflows for recruiting were drafted but never deployed

`n8n-workflow.json` and `n8n-workflow-v2.json` exist in `recruiting-pipeline/` but BUILD_LOG confirms they were never put live. This is fine — we have a clean slate to register the workflow once the MVP data path is decided.

---

## 3. Updated MVP architecture (revised after Step 3)

```
                    ┌─────────────────────────────────────┐
                    │  Frantisek's Keboola pipeline       │
                    │  (Bubble → Snowflake reporting-v2)  │
                    │  Runs 6×/day, 99% reliable          │
                    └──────────────┬──────────────────────┘
                                   │
                                   ▼
              ┌────────────────────────────────────┐
              │ KEBOOLA_855."out.c-reporting-v2"   │
              │   17 tables, ~1.28 GB              │
              │   Validated query access           │
              └─────────────┬──────────────────────┘
                            │   ONE SQL view (WBR + Project)
                            ▼
              ┌────────────────────────────────────┐
              │  recruiting_view.sql               │  ← NEW, write this
              │  (port of WBR TA Actual /          │
              │   WBR TS Actual DAX calc tables)   │
              └─────────────┬──────────────────────┘
                            │   shape into data.json
                            ▼
              ┌────────────────────────────────────┐
              │  recruiting-dashboard/src/App.jsx  │  ← KEEP, hide 4 tabs
              │  React + Vite + Recharts           │
              │  (only Overview + Jobs visible)    │
              └─────────────┬──────────────────────┘
                            │   GitHub → Cloudflare Pages
                            ▼
              ┌────────────────────────────────────┐
              │  Live MVP dashboard                │
              │  Phase 1 Apr deliverable           │
              └────────────────────────────────────┘
```

**Retired for MVP (kept on shelf for Phase 2 / off-Keboola path):**
- `bubble_extract.py` — async API connector
- `transform.py` — DuckDB SQL port
- `n8n-workflow*.json` — 3×/day Bubble pipeline
- 4 of the 6 dashboard tabs (Pipeline detail, Recruiter Perf, Client Delivery, TTH analytics)

---

## 4. Files that should now exist in `Recruiting Dashboard\` (workspace)

| File | Created | Purpose |
|---|---|---|
| `MVP_PLAN_April2026.md` | 2026-04-07 | Kill/keep + 3-week MVP plan |
| `FRANTISEK_QUESTIONS.md` | 2026-04-07 | Friday meeting prep |
| `POWERBI_DAX_MEASURES.md` | 2026-04-08 | Andy's full DAX (200 measures + 9 tables) |
| `SANITY_CHECK_2026-04-08.md` | 2026-04-08 | 5-query validation against reporting-v2 |
| `TRIBE_PROJECT_INVENTORY.md` | 2026-04-08 | This file |
| `tribe-project-snapshot/` | 2026-04-08 | Curated copy of recruiting files + Frantisek's SQL |

---

## 5. What I would do next (suggested order)

1. **Read part_1 (39 KB)** end-to-end and write a one-page summary of how each of the 16 reporting-v2 tables is built. That's the rosetta stone for any "where does this column come from" question.
2. **Write `wbr_view.sql`** — a single CTE-based query against `reporting-v2` that reproduces the WBR TA Actual + WBR TS Actual columns from Andy's DAX. Validate row-by-row against a screenshot from Power BI.
3. **Adapt `transform.py`** to read from Snowflake instead of Bubble JSON dumps (or: skip transform.py entirely and have the dashboard fetch the JSON directly from a thin Python wrapper around the Keboola query). Decide based on Friday's Frantisek call.
4. **Hide 4 tabs in App.jsx**, leave Overview + Jobs, redeploy.
5. **Get the WBR target spreadsheet from Andy** so the WBR tab can color-code against actual targets.

Item #1 will inform everything else — let me know if you want me to do that next.
