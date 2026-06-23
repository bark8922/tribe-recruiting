# Recruiting Dashboard — Project Log

Single source of truth for status, decisions, and open items. Survives Cowork resets because it lives in this folder (on Blake's computer) and is backed up to GitHub. Claude updates this at the end of any session where real work happened. Blake can also say "update the log" anytime.

Last updated: 2026-06-23

---

## What this project is

Tribe.xyz recruiting/staffing analytics stack. It replaced **Power BI + the in-house data analyst** (~€96K/year saved). **Keboola/Snowflake stays — it IS the pipeline** (this is the contract Blake is renewing; only the parallel PBI chain dies 2026-08-31).

Real flow (verified live; see `DATA_LINEAGE.md`): Bubble.io API → Keboola extractors (incremental `122527414` + full `122491135`) + Geocoding → PROD V2 Snowflake SQL transform (`out.c-reporting-v2`, 17 tables) → Flow A's 7 Snowflake SQL transforms (24 output tables) → render-to-JSON + push → React/Vite dashboards → GitHub → Cloudflare Pages. Data ships as a snapshot bundle (`dashboard_data.json`, ~5.7MB), client-side React.

Orchestration = **Keboola Flows** (Flow B 3x/day `5 7,10,16` CET; Flow A 4x/day), NOT n8n. Transforms = **Snowflake SQL in Keboola**, NOT local DuckDB. (n8n at Tribe runs other things — expenses, fleet — not this pipeline.)

> Note: the `tribe-recruiting-dashboard` skill still describes the old local-Python/DuckDB-on-n8n design, which was never the production reality. That skill is a read-only cache and can't be edited from here; treat THIS log + `DATA_LINEAGE.md` as authoritative until the skill is updated in Settings.

## Durable locations (where things actually live)

- **Main dashboard code:** GitHub `bark8922/tribe-recruiting` → Cloudflare Pages
- **Sourcing dashboard code:** GitHub `bark8922/tribe-sourcing` → `tribe-sourcing.pages.dev`
- **Working folder (this one):** local on Blake's computer, also backed up to GitHub (see Backup below)
- **Architecture reference:** the `tribe-recruiting-dashboard` skill (read before changing anything)

## Live now

- Main recruiting dashboard: 6 tabs (Overview, Pipeline, Recruiter Performance, Client Delivery, Time to Hire, Jobs). Built and deployed.
- Sourcing dashboard: live, refreshes 4x/day via Keboola Flow. Phase 1 (quarterly funnel, methodology v1.5) and Phase 2 (cost of sourcing team) both shipped.
- **Keboola Data App PoC (Time to Hire):** live. First in-Keboola data app, reads `out.c-TTH---tth-jobs.tth_jobs` from Snowflake (input mapping → `/data/in/tables/tth_jobs.csv`), password-gated, auto-sleeps 15 min. Code: GitHub `bark8922/tribe-tth-app` (private), Flask + `keboola-config/` layout. App config `01kvq9zgsrkrt5yevw6djvqz0f` ("TTH Test"), URL `tth-test-985851138.hub.eu-central-1.keboola.com`. Kai chat is wired in code (`kai_chat.py`) but OFF until a master token is added. Purpose: prove the Kai-powered stakeholder-app path from the 2026-06-22 Keboola renewal call before building more.

## In flight / open decisions

| Item | Status | Blocked on |
|---|---|---|
| Cortex Analyst pilot | Scoped 2026-06-12, not started | Blake go/no-go: green-light free Snowflake trial, pick 2-3 pilot users, confirm aggregates-only data |
| CSV export buttons | Scoped 2026-06-11, nothing built | Blake decision: which tables in Phase 1 vs all 25 at once |