# Recruiting Dashboard — Build Log & Architecture Decisions

**Last updated:** March 30, 2026

---

## Architecture

```
Bubble.io REST API  →  bubble_extract.py  →  transform.py (DuckDB)  →  data.json  →  App.jsx (React+Vite)  →  GitHub  →  Cloudflare Pages
     40 endpoints        Python async           SQL transforms          ~500KB         6-tab dashboard        bark8922/     auto-deploy
     Bearer token        aiohttp                Port of Keboola SQL                    Recharts + TW          tribe-recruiting
     cursor pagination   rate-limited           2025+ date filter
```

**Pipeline orchestrator:** `run_pipeline.py` (called by n8n 3x/day: 9am, 3pm, 9pm CET)

**Stack:** React 18 + Vite 5 + Recharts 2 + Tailwind CDN (same as finance dashboard)

---

## What Worked

1. **DuckDB as Snowflake replacement** — Ported all Keboola SQL transformations with minimal syntax changes. Only needed `NULL::VARCHAR` explicit casts (DuckDB type inference is stricter than Snowflake). Test run passed on first real attempt.

2. **Same architecture as finance dashboard** — React + Vite + Recharts + Tailwind CDN. Proven pattern. Build succeeds, output is ~185KB gzipped.

3. **Bubble.io API connector** — Async Python with aiohttp, cursor-based pagination, rate limiting (0.3s delay, 3 concurrent). Handles all 40 endpoints (29 full-load lookups + 11 incremental core tables).

4. **2025+ date filter** — Reduces events from ~14.5M total to ~3-5M for the dashboard. Older data stays queryable on-demand via Claude/chatbot.

5. **Vite 5 downgrade** — Vite 8 had dependency resolution issues in sandboxed environments. Vite 5.4.2 + React 18.3.1 + Recharts 2.12.7 builds cleanly.

---

## What Failed / Lessons Learned

1. **Sandbox can't reach Bubble.io API** — DNS resolution fails from Cowork sandbox. Pipeline must run on Blake's n8n server or local machine. Not a blocker — just means we can't test extraction from here.

2. **Vite 8 + React 19 dependency issues** — `@rolldown/pluginutils` package import error. Downgraded to Vite 5 + React 18 which is battle-tested.

3. **Power BI scraping (previous session)** — Trying to programmatically scrape Power BI layouts via Chrome was unreliable. Taking screenshots manually and documenting the layouts was much more productive.

4. **Keboola MCP shortcut** — Blake explicitly chose "the long route but correct" — build the full pipeline from scratch rather than depending on Keboola as a data source.

---

## Dashboard Tabs (6 total)

| Tab | What it shows | Key metrics |
|-----|-------------|-------------|
| **Overview** | High-level KPIs + funnel + trends | Open roles, active candidates, hires, screens, avg TTH, pipeline funnel, monthly hiring trend, activity by recruiter |
| **Pipeline** | Per-job pipeline detail table | Job title, client, recruiter, sourcer, stage counts, days open, health color-coding |
| **Recruiter Performance** | Per-recruiter metrics + charts | Screens done, candidates sourced, hires, events, hire/screen ratio, comparative bars |
| **Client Delivery** | Per-client delivery + WBR table | Open roles, candidates, hires, fill rate, avg TTF, color-coded target performance |
| **Time to Hire** | TTH analytics + stage durations | Avg TTH/TTS/TTO, monthly trend, stage duration breakdown |
| **Jobs** | Individual job cards with detail | Candidates by stage, days open, recruiter/sourcer, client, search/filter |

---

## Data Model (data.json sections)

| Section | Records | Description |
|---------|---------|-------------|
| jobs | ~35 | Active + recent jobs with recruiter/sourcer/client assignments |
| candidates | ~250 | Pipeline records with stage dates and current status |
| events_monthly | ~540 | Aggregated events per recruiter/type/job/month |
| events_recent | ~56 | Last 90 days of individual events |
| users | ~10 | Recruiters and sourcers |
| clients | ~14 | Client companies |
| screens | ~100 | Completed recruiter screens with ratings |
| job_goals | ~29 | Hiring targets per job |
| summary | 1 | Metadata (generated_at, period range, totals) |

---

## Key Decisions

1. **No access control for recruiting dashboard** — Everyone can see everything. Different from the finance dashboard which has BU-level filtering.

2. **Date filter: 2025-01-01 to present** — Only recent data in the dashboard. Historical data available on-demand via Claude.

3. **3x daily refresh** (9am, 3pm, 9pm CET) — More frequent than the 6-hour Keboola cycle. Can increase if needed.

4. **Color-coded target thresholds** — Red (<50%), Orange (50-75%), Yellow (75-100%), Light green (100-120%), Green (>120%). Used in WBR/MBR tables and pipeline health.

5. **Problem pipeline detection** — Jobs with 25+ screens but 0 hires, or >32:1 screen-to-hire ratio get flagged.

---

## Files

```
recruiting-pipeline/
  bubble_extract.py     — Bubble.io API connector (40 endpoints, async, incremental)
  transform.py          — DuckDB SQL transformations (port of Keboola)
  run_pipeline.py       — Pipeline orchestrator (n8n calls this)

recruiting-dashboard/
  src/App.jsx           — Main React dashboard (752 lines, 6 tabs)
  src/data.json         — Dashboard data (generated by pipeline)
  src/main.jsx          — React entry point
  src/index.css         — Dark theme styles
  index.html            — HTML entry
  vite.config.js        — Vite config (base: './')
  package.json          — Dependencies (React 18, Recharts 2, Vite 5)
  dist/                 — Production build output

docs/
  Recruiting_Dashboard_Project_Plan.md  — Full project plan
  BUILD_LOG.md                          — This file
  keboola-sql/                          — Original Keboola SQL (reference)
```

---

## Remaining Work

- [ ] Test end-to-end pipeline with live Bubble.io data (needs n8n server)
- [ ] Deploy to GitHub (bark8922/tribe-recruiting)
- [ ] Set up Cloudflare Pages auto-deploy
- [ ] Create n8n workflow for 3x daily refresh
- [ ] Create recruiting-dashboard skill for Claude
- [ ] Parallel run validation against Power BI
- [ ] Stakeholder feedback (Nenad, Salem, Kristjana, Jacopo)
