# Andy's Power BI — Page Inventory

**Source:** `powerbi_export/Tribe-recruiting-WBR-MBR.*` (PBIP export of the WBR/MBR Power BI project)
**Extracted:** 2026-04-22
**Why this exists:** Andy leaves end of April 2026. This is our archive of every page he built, bucketed by whether we've already replaced it, whether it's load-bearing analysis we need to keep, or whether it's operational fluff that can die with Power BI.

Stats: 35 pages, 621 visuals, 34 tables in the semantic model, 200 DAX measures, 123 calculated columns, 9 calculated tables.

See `../POWERBI_DAX_MEASURES.md` (105KB) for the 2026-04-08 extraction of every measure + M query.
See `./pages/*.md` for per-page deep-dives (Bucket C + D pages).
See `./andy-homework/` for Andy's completed handover packet (Apr 23-24).
See `./metric-ownership.md` for who owns retuning the non-derivable thresholds/formulas post-handover.
See `./snapshots-2026-04-24/INDEX.md` for Andy's ground-truth data exports (115 xlsx files from 2 dashboards; 60 with real data, 55 empty chart-visual placeholders). This is the last running-Power-BI snapshot we'll ever have.

---

## Bucket A — Already replaced or directly equivalent to what we ship today

Don't waste cycles resurrecting these; confirm the new dashboard is the source of truth, then archive.

| Page | Visuals | Primary tables | Status |
|------|---------|----------------|--------|
| WBR | 42 | WBR TA Actual, Metrics, WBR TS Actual | Replaced — our WBR section |
| MBR | 26 | WBR TA Actual, WBR TS Actual, WBR Client History | Replaced — our MBR section |
| WBR II | 11 | Metrics, job, client, event | Likely folded into our WBR — verify |
| KPI - TS Summary | 31 | Metrics, candidate_stage, candidate, client | Covered by our TS section |
| KPI - TS Summary (Old) | 26 | — | Marked "Old" by Andy — skip |
| KPI - TA Summary (Color) | 24 | Metrics, event, job, client | Covered by our TA section |
| KPI - TA Summary | 25 | — | Older non-color version — skip |
| TA Actual Screens Target | 16 | WBR TA Actual, Historical Manager Structure WBR | Probably in WBR/MBR already |
| Missing Comment | 16 | WBR TA Comment, WBR TS Comment | Workflow nudge — decide if we still need it |

## Bucket B — Tooltips, not real pages

These are hover popups Power BI shows when you mouse over a row in another page. Document as part of WBR/MBR, not as standalone dashboards.

- WBR TS Tooltip (2 visuals)
- WBR TA Tooltip (2 visuals)
- WBR TA Tooltip (Non-Wolt) (2 visuals)
- WBR Client Tooltip (2 visuals)
- TS summary tooltip (1 visual)

## Bucket C — Load-bearing, NOT yet replaced

This is where the real risk lives. Each page answers a distinct analytical question that disappears if we don't port or archive the logic. Per-page deep-dives live in `./pages/`.

| Page | Visuals | Primary tables | One-liner |
|------|---------|----------------|-----------|
| Sourcing Stats | 23 | Sourcing Stats, client, job, event | Sourcer-level performance — the Q2 OKR reporting target |
| Time to Hire | 22 | job, candidate_stage, talent | Days from job-open to hire by client/role |
| Recruiting Trends & Conversion Rate | 30 | job, candidate, event (is_external_recruiter, source) | Full-funnel conversion analytics over time |
| New Role Estimate | 21 | candidate, job (Conv rate Visited→Hires, Contacted→Hires) | Predicts hiring timeline for a new role |
| Hired Candidate Salary Audit | 18 | candidate, talent, Hired Salary cal | Comp audit for hires |
| Open Roles | 17 | job, client | Operational view of currently-open jobs |
| Executive Search | 15 | talent, job, candidate | Exec-search funnel (narrower stage logic) |
| OKR | 14 | OKR TA, Metrics | OKR tracking by TA |
| Candidate Response Rate | 19 | event, candidate_stage | Response-rate trends — maybe fold into WBR |
| Weekly Progress | 19 | Metrics, job, event | Weekly trending — probably overlaps WBR |
| Pipelines Health | 16 | job, client | Pipeline health — may overlap Project Dashboard |
| Active Pipelines | 14 | job, client | Active pipelines — may overlap Project Dashboard |
| Viewed / Contacted Health | 18 | event, candidate_stage | Top-of-funnel health — overlaps WBR |

## Bucket D — Internal recruiting (different data system)

| Page | Visuals | Primary tables | Note |
|------|---------|----------------|------|
| Internal Recruitment | 21 | IR Comment, candidate_stage, event | Internal roles in our standard recruiting tables |
| Internal Recruiting | 10 | **recruitee_*** tables | **Pulls from Recruitee, a different system.** If Andy is the only one who knows this exists, that's institutional risk on its own. |

## Bucket E — Ops / meta. Safe to archive without rebuilding

- **Overview** (37) — landing-page summary, navigation aid
- **Data Cleanliness/Hygiene** (12) — data-quality monitoring
- **Data Download** (20) — bulk export page
- **Active Page Visits** (17) — dashboard usage analytics (`analytic_usage`)
- **Session Cookie Expiration** (13) — infra/login monitoring
- **TEST - Client Interview** (19) — name says TEST

---

## Decision log

Track each page's fate as decisions get made. `kept` = we've rebuilt or confirmed coverage. `dropped` = consciously decided to lose it. `pending` = still need to decide.

| Page | Bucket | Decision | Owner | Notes |
|------|--------|----------|-------|-------|
| WBR | A | kept | Blake | New dashboard WBR section |
| MBR | A | kept | Blake | New dashboard MBR section |
| Sourcing Stats | C | pending | — | Q2 OKR dependency |
| Time to Hire | C | pending | — | Classic exec metric |
| Recruiting Trends & Conversion Rate | C | pending | — | |
| New Role Estimate | C | pending | — | Sales/intake predictive tool |
| Hired Candidate Salary Audit | C | pending | — | |
| Open Roles | C | pending | — | |
| Executive Search | C | pending | — | |
| OKR | C | pending | — | |
| Candidate Response Rate | C | pending | — | Fold into WBR? |
| Weekly Progress | C | pen