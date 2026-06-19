# Recruiting Dashboard — Project Log

Single source of truth for status, decisions, and open items. Survives Cowork resets because it lives in this folder (on Blake's computer) and is backed up to GitHub. Claude updates this at the end of any session where real work happened. Blake can also say "update the log" anytime.

Last updated: 2026-06-19

---

## What this project is

Tribe.xyz recruiting/staffing analytics stack that replaced Power BI + Keboola/Snowflake (~€96K/year saved). Flow: Bubble.io API → Python pipeline (DuckDB transforms) → data.json → React/Vite dashboard → GitHub → Cloudflare Pages (auto-deploy).

## Durable locations (where things actually live)

- **Main dashboard code:** GitHub `bark8922/tribe-recruiting` → Cloudflare Pages
- **Sourcing dashboard code:** GitHub `bark8922/tribe-sourcing` → `tribe-sourcing.pages.dev`
- **Working folder (this one):** local on Blake's computer, also backed up to GitHub (see Backup below)
- **Architecture reference:** the `tribe-recruiting-dashboard` skill (read before changing anything)

## Live now

- Main recruiting dashboard: 6 tabs (Overview, Pipeline, Recruiter Performance, Client Delivery, Time to Hire, Jobs). Built and deployed.
- Sourcing dashboard: live, refreshes 4x/day via Keboola Flow. Phase 1 (quarterly funnel, methodology v1.5) and Phase 2 (cost of sourcing team) both shipped.

## In flight / open decisions

| Item | Status | Blocked on |
|---|---|---|
| Cortex Analyst pilot | Scoped 2026-06-12, not started | Blake go/no-go: green-light free Snowflake trial, pick 2-3 pilot users, confirm aggregates-only data |
| CSV export buttons | Scoped 2026-06-11, nothing built | Blake decision: which tables in Phase 1 vs all 25 at once |
| Sourcing dashboard polish | Live, minor items | Validate post-Zelimir-fix refresh; ask Gustavo to sanity-check Phase 2 cost numbers |

## Decisions locked (do not relitigate without reason)

- Sourcing methodology v1.5: count work during Bench/Internal, drop onboarding contacts when Bench window ≤30 days, Sanja Pavlovikj excluded entirely, <5 contacts/quarter dropped as noise, half-open `[start, end)` division intervals.
- Tribe internal jobs (Tribe.xyz, IR) included; test clients excluded; archived jobs included.
- Cross-client sourcing work excluded (counts as TA work, not internal sourcing).

## Known gotchas

- App.jsx is huge (~4,500 lines). Edit via Python search/replace, run `npx vite build` before every push.
- Local folder can lag GitHub — clone the repo fresh and diff before copying anything.
- Cowork sandbox cannot reach `overview.tribe.xyz` (DNS fails). Pipeline runs on Blake's n8n server, not here.

## Backup

- This folder is git-tracked and pushed to GitHub for off-machine safety (repo: TBD — see Task 3).
- Daily auto-push scheduled (TBD).
- Conversations themselves are NOT auto-saved. Anything important from a chat must be written into this log to survive.

---

## Session history

### 2026-06-19
- Reconstructed project status after Blake reinstalled Claude desktop (chat history lost, files survived).
- Set up durability: this PROJECT_LOG.md, project memory, and GitHub folder backup.
- Note: `Lejla_week25_screens.csv` added today — ad-hoc week-25 screen-credit tally for Lejla Silva (AVIV QA Automation roles), not dashboard code.
