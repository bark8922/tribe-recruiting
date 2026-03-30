# Tribe Recruiting Dashboard

Replaces Keboola + Power BI for recruiting/staffing analytics. Saves ~€96K/year.

## Architecture

```
Bubble.io API → bubble_extract.py → transform.py (DuckDB) → data.json → React Dashboard → Cloudflare Pages
```

## Structure

- `recruiting-pipeline/` — Data extraction and transformation
  - `bubble_extract.py` — Bubble.io API connector (40 endpoints, async)
  - `transform.py` — DuckDB SQL transformations (port of Keboola/Snowflake)
  - `run_pipeline.py` — Pipeline orchestrator (n8n calls this)
- `recruiting-dashboard/` — React frontend
  - `src/App.jsx` — 6-tab dashboard (Overview, Pipeline, Recruiter, Client, TTH, Jobs)
  - `dist/` — Production build for Cloudflare Pages
- `docs/` — Project plan, architecture, build log

## Dashboard Tabs

1. **Overview** — KPIs, pipeline funnel, hiring trends, recruiter activity
2. **Pipeline** — Per-job pipeline detail with health indicators
3. **Recruiter Performance** — Per-recruiter metrics and comparisons
4. **Client Delivery** — Fill rates, WBR targets with color-coded thresholds
5. **Time to Hire** — TTH analytics, stage durations, trends
6. **Jobs** — Individual job detail with candidate stage breakdown

## Refresh

Pipeline runs 3x daily via n8n (9am, 3pm, 9pm CET).
