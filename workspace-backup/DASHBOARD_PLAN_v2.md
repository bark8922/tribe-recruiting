# Recruiting Dashboard — Comprehensive Build Plan v2
**Updated:** 2026-04-10 (after studying full Power BI layout + Google Sheet)
**Ship by:** 2026-04-30 (Andy's last day)

---

## What Power BI actually has (full inventory)

### Report 1: "Project Performance" (10 pages)
| Page | What it shows | MVP? |
|---|---|---|
| **Overview** | KPI cards (Contacted, Positive Response, Actual Screens, ATS, Offered, Hired) + bar charts per person for each metric. Filters: date range, TS/sourced-by, TA, client, job title, job category, source, sourcing team | **YES** |
| **Internal Recruitment** | 4 tables: (1) Client & Job Performance (per job: full funnel + conversion %), (2) TA Overview (Client × TA), (3) TS Overview (Client × TS), (4) Hired Candidate details (individual hires with dates/sourcer/TA/LinkedIn). Plus Disqualified Reason pie chart | **YES** |
| Weekly Progress | Time series of weekly metrics | Defer |
| Recruiting Trends | Trend charts | Defer |
| Data Download | Raw data export | Defer |
| Candidate Response Rate | Response rate analytics | Defer |
| Time to Hire | Time-to-hire distributions | Defer |
| New Role Estimate | Estimation tool | Defer |
| KPI - TA Summary | TA performance summary | Defer |
| KPI - TS Summary | TS performance summary | Defer |

### Report 2: "KPI Dashboard (Leadership)" (10 pages)
| Page | What it shows | MVP? |
|---|---|---|
| **WBR** | (1) Client's Target summary table (client × metrics, color heat-map), (2) TA Weekly Target tables (split by team: "Ponies & Unicorns" + "Dolphins & Whales", with COMMENTS column), (3) TS Weekly Target table (with COMMENTS), (4) TS Overall Conversion Rate. Filters: week selector, manager, current-week toggle | **YES** |
| WBR II | Second WBR page (likely overflow/detail) | Defer |
| Missing Comment | Flags TAs/TSs who haven't written their weekly comment | Nice-to-have |
| Data Cleanliness/Hygiene | Data quality checks | Defer |
| TA Actual Screens Target | Screen targets tracking | Defer |
| **MBR** | Monthly business review — same shape as WBR, monthly grain | **Phase 1.5** |
| Sourcing Stats | Sourcing team analytics | Defer |
| KPI - TS Summary | TS KPI summary | Defer |
| KPI - TA Summary (Color) | TA KPI with color coding | Defer |
| Pipelines Health | Pipeline health metrics | Defer |

---

## MVP scope (April 30 ship)

### Tab 1: WBR (Weekly Business Review)

This is the leadership view. Three sections stacked vertically, matching Power BI layout:

**Section 1.1 — Client's Target Summary**
A heat-map table with one row per client, columns:
- Client name
- Last 12w Hires (rolling)
- Hires (this week) — color-coded vs target
- Contacted — color-coded vs target
- Actual Screens — color-coded vs target
- Moved to ATS — color-coded vs target
- Offers — color-coded vs target
- # Jobs (active)

Color thresholds (from Power BI):
- Red: <50% of target
- Orange: 50-75%
- Yellow: 75-100%
- Light green: 100-120%
- Green: >120%

**Section 1.2 — TA Weekly Target**
Split into two team tables (Ponies & Unicorns, Dolphins & Whales — from BambooHR manager hierarchy):
- TA name
- Client
- Contacted (actual / target) — color-coded
- Actual Screens (actual / target) — color-coded
- Moved to ATS (actual / target) — color-coded
- Hires
- Comment (from Google Sheet "TA Weekly Note" tab)

CRITICAL: Only TAs present in the `wbr_ta_target.csv` for the selected week's month appear. This is the VBR target join — (week × client × TA).

**Section 1.3 — TS Weekly Target**
- TS name
- Contacted (actual / target) — color-coded
- Reasoning (from Google Sheet)
- Comment (from Google Sheet "TS Weekly Note" tab)

**Section 1.4 — TS Overall Conversion Rate**
Per-pipeline conversion rate drill-down showing sourcing effectiveness.

**Filters (top bar):**
- Week selector (ISO week picker, default = current week)
- Manager filter (dropdown, filters TAs/TSs by manager chain)
- "Current Week" toggle

**Data sources:**
- `candidate_stage` table (via Keboola MCP) — all stage counts use date_* columns with DISTINCT COUNT
- `wbr_ta_target.csv` — monthly TA targets (already local)
- `wbr_ts_weekly.csv` — weekly TS targets + comments (already local)
- Google Sheet "TA Weekly Note" tab — weekly TA comments (need to export or pull via Google Sheets API)
- `bamboohr_roster_current.csv` — current manager hierarchy for team splits
- `job` table — for active job count, test job filter
- `client` table — for client names (NOT using client.test — use name-based exclusion)

### Tab 2: Project Dashboard (Internal Recruitment)

This is the operational view. Four sections:

**Section 2.1 — Client & Job Performance Table**
One row per active job:
- Client
- Job title
- TA assigned
- TS assigned
- Days open (job_created → today)
- Funnel columns: Contacted → Positive Response → Actual Screens → ATS → Offered → Hired
- Conversion % at each stage
- Status flag (color-coded health: green/yellow/red based on days open + conversion)

**Section 2.2 — TA Overview Table**
Aggregated by Client × TA:
- All funnel metrics summed
- Conversion rates

**Section 2.3 — TS Overview Table**
Aggregated by Client × TS:
- Contacted counts
- Attribution to downstream stages

**Section 2.4 — Hired Candidates Detail**
Individual hire records:
- Candidate name/ID
- Client
- Job title
- Hire date
- Contact date
- TA
- Credit sourcer (first to contact)
- Time to hire (contact → hire)
- LinkedIn profile link (if available)

**Section 2.5 — Overview KPIs (top cards)**
Six KPI cards with totals for the filtered date range:
- Contacted
- Positive Response
- Actual Screens
- ATS
- Offered
- Hired

Plus bar charts showing top performers for each metric.

**Filters (top bar):**
- Date range (relative: last 7d/30d/90d, or custom between)
- TA filter (multi-select)
- TS / Sourced By filter
- Client filter
- Job Title filter
- Job Category filter

**Data sources:**
- `candidate_stage` table — all stage metrics (date_contacted, date_screen, date_screen_actual, date_interview, date_offer, date_hired)
- `event` table — Positive Response only (not in candidate_stage)
- `job` table — job details, days open, TA/TS assignment
- `client` table — client names

---

## Architecture

### Stack
- **Frontend:** React + Vite + Tailwind CSS (single-page app)
- **Hosting:** Cloudflare Pages (already set up for the financial dashboard)
- **Data pipeline:** Python script → Keboola MCP queries → JSON
- **Automation:** n8n workflow (3x daily refresh, already running for financial dashboard)
- **Static data:** CSV files bundled at build time (targets, comments)

### Data flow
```
Keboola (reporting-v2)          Google Sheet (WBR Target)
        │                                │
        ▼                                ▼
  Python runner                   Google Sheets API
  (query_data MCP)               (export as CSV)
        │                                │
        ▼                                ▼
   DuckDB local ◄────── merge ──────► Static CSVs
        │
        ▼
  recruiting_data.json
        │
        ▼
  React dashboard (Cloudflare Pages)
```

### Pipeline steps (Python runner)
1. Query `candidate_stage` from Keboola — weekly aggregates by TA × client
2. Query `event` table — Positive Response counts only
3. Query `job` table — active jobs with metadata
4. Read Google Sheet tabs via API (or periodic CSV export):
   - TA Weekly Note → ta_weekly_comments
   - TS Weekly Note → ts_weekly_comments (already have)
   - TA Target → ta_targets (already have)
5. Read BambooHR roster → manager hierarchy
6. Merge all sources in pandas/DuckDB
7. Apply filters: `job.test <> 'true'`, name-based client exclusion, `is_candidate_archived`
8. Apply VBR target join: only include TAs in target sheet for WBR
9. Compute color thresholds (actual/target ratios)
10. Output `recruiting_data.json` with all precomputed views

### JSON structure (output)
```json
{
  "generated_at": "2026-04-10T10:00:00Z",
  "week_current": "2026-W15",
  "wbr": {
    "client_summary": [...],
    "ta_weekly": {
      "ponies_and_unicorns": [...],
      "dolphins_and_whales": [...]
    },
    "ts_weekly": [...],
    "ts_conversion": [...]
  },
  "project": {
    "kpi_cards": {...},
    "job_performance": [...],
    "ta_overview": [...],
    "ts_overview": [...],
    "hired_detail": [...]
  },
  "filters": {
    "weeks_available": [...],
    "managers": [...],
    "clients": [...],
    "tas": [...],
    "tss": [...]
  }
}
```

### Interactive features
- **Week selector:** Dropdown to pick any ISO week (WBR tab). Changes all tables.
- **Manager filter:** Filter TA/TS tables by manager chain (uses BambooHR hierarchy).
- **Date range picker:** For Project Dashboard tab.
- **Multi-select filters:** Client, TA, TS, Job Title on Project Dashboard.
- **Sortable columns:** Click column headers to sort any table.
- **Color heat-map:** Automatic color coding based on actual/target thresholds.
- **Export:** CSV download button for any table.
- **"Weekly note spreadsheet" link:** Direct link to the Google Sheet for comment entry.
- **Responsive:** Works on laptop screens (primary use case is WBR meetings on projector).

---

## What's NOT in the MVP (deferred)

- MBR (monthly view) — same data, different grain. Ship as Phase 1.5 after WBR validated.
- Weekly Progress time series charts
- Recruiting Trends
- Data Download (raw export)
- Candidate Response Rate analytics
- Time to Hire distributions
- New Role Estimate tool
- Missing Comment tracker
- Data Cleanliness/Hygiene checks
- Sourcing Stats
- Pipelines Health

---

## Open questions (blocking or semi-blocking)

### Must resolve before ship
1. **TA Weekly Note tab** — Need to export from Google Sheet. Structure unknown (likely: ta, client, year, week, comment). Can we get Google Sheets API access, or should we periodically export to CSV?
2. **date_screen vs date_screen_actual** — Which column is Power BI's "Actual Screens"? Ask Andy.
3. **Actual screen note cross-check** — Andy uses events to verify evaluation notes were added. What event_type/field? Ask Andy.
4. **Team split logic** — How are TAs split into "Ponies & Unicorns" vs "Dolphins & Whales"? Is this from BambooHR division/department, or from the target sheet structure, or manual?
5. **Meho's +18 extra contacted** — Small per-TA discrepancy. Acceptable or needs investigation?

### Can ship without (nice-to-have)
6. **IR tab in Google Sheet** — What is this? Internal Recruitment comments?
7. **Reasoning Guidance tab** — Instructions for TLs writing weekly reasoning. Could display as help text.
8. **TS Conversion Rate drill-down** — Exact calculation for the per-pipeline conversion table.
9. **"Current Week" toggle** — What exactly does this do in Power BI? Auto-select current week?

---

## Build sequence (2 weeks)

### Week 1 (Apr 10-16): Data pipeline + WBR
- [ ] Export TA Weekly Note from Google Sheet (ask Blake to share or set up API)
- [ ] Rewrite wbr_view.sql with Andy's confirmed rules (correct event columns, candidate_stage for metrics)
- [ ] Build Python runner: Keboola MCP queries → merge with static CSVs → JSON
- [ ] Build WBR tab: Client summary + TA tables + TS tables + comments
- [ ] Validate against Power BI week 14 numbers (already 95% match on per-TA)

### Week 2 (Apr 17-23): Project Dashboard + polish
- [ ] Build Project Dashboard tab: KPI cards + job table + TA/TS overviews + hired detail
- [ ] Add all filters (week, manager, date range, client, TA/TS)
- [ ] Add color heat-map logic
- [ ] Add CSV export
- [ ] Deploy to Cloudflare Pages
- [ ] Set up n8n automation (daily refresh)

### Week 3 (Apr 24-30): Validation + handover
- [ ] Side-by-side validation with Andy on all numbers
- [ ] Fix edge cases
- [ ] Document the pipeline for post-Andy maintenance
- [ ] Ship to Martin for sign-off

---

## Google Sheet integration plan (Option A — Blake's choice)

**Read-only pull at pipeline runtime:**
1. Python runner uses Google Sheets API (or gspread library) to read:
   - "TA Weekly Note" tab → TA comments per week
   - "TS Weekly Note" tab → TS comments per week (backup; also in local CSV)
   - "TA Target" tab → targets (backup; also in local CSV)
2. TLs continue entering comments in the Google Sheet as they do today
3. Dashboard pulls fresh comments on each refresh (3x daily via n8n)
4. Link to the Google Sheet displayed in dashboard header for comment entry

**Why this works:** Zero workflow disruption for TLs. They keep using the same Google Sheet. The dashboard just reads it.

**Fallback if API access is tricky:** Periodic manual CSV export (weekly before WBR meeting). Already works for TA Target and TS Weekly Note.
