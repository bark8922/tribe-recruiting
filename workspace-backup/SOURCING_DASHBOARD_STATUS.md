# Sourcing Dashboard — Status (2026-06-03)

Live: https://tribe-sourcing.pages.dev/?member=<email>
Repo: bark8922/tribe-sourcing
Refresh: 4x/day via Keboola Flow 01kpqyq1pz6qpmk7m9s4qx8gmg

## What's live

### Phase 1 — TS-Summary (quarterly funnel)
- Per-quarter rollup of Contacted → PosResp → RS → Actual Screens → ATS → Offer → Hire
- Team size column shows the active sourcer count, hover reveals names
- Methodology v1.5: count work done while sourcer was on Bench/Internal, drop onboarding contacts when Bench window ≤30 days, Sanja excluded, <5 contacts/quarter dropped as noise

### Phase 2 — Cost of Sourcing Team
- Int cost, Ext rev, Ext cost, Ext margin, Sustainability Score, Hires, €/hire per quarter
- Pulls from Finance dashboard (`tribe-dashboard/data-next/data.json`) on every refresh
- ct-based classification: Bench → full pr to internal; Client with bd>0 → bd/wd-weighted split
- 19-sourcer roster, Sanja excluded for Phase 1 parity

## Pipeline architecture (both tables refresh together)

```
Snowflake (event + candidate_stage + job)
    │
    ▼  b0.c13 SQL in Keboola transformation 01kpr0tr0dt5ryf96a5zk85bx7
    │  hardcoded BambooHR division periods
    │  v1.5 filters (contact-date in Bench window, onboarding drop)
    ▼
sourcing_dashboard_per_sourcer table (per sourcer × ISO week)
    │
    ▼  Python component 01kt1ns5mq87k9tmgmtapf8bhm (keboola_entry.py)
    │  Phase 1: aggregate to quarterly funnel
    │  Phase 2: fetch Finance data.json, compute cost
    ▼
GitHub Contents API push → bark8922/tribe-sourcing/data.json
    │
    ▼  GitHub Action deploy.yml
    ▼
Cloudflare Pages (tribe-sourcing.pages.dev)
```

## Pending polish on Phase 1/2

| Item | Severity | Notes |
|---|---|---|
| Validate first refresh post-Zelimir-fix | Low | Date-boundary half-open interval pushed 2026-06-03. Need to confirm next Flow run produces expected (slightly lower) numbers. |
| Phase 1 mockup-to-live hire delta explanation | Low | Live numbers higher than the manual mockup I showed during Phase 2 design (Q1: 22 vs 12, Q2: 22 vs 14, etc.). Production SQL is correct — mockup used a stricter "hire-date in Bench window" filter; v1.5 methodology uses contact-date. |
| Gustavo's Cost sanity-check | Open | He hasn't seen Phase 2 yet. Send him a heads-up so he can compare against his own Sustainability Score model. |
| Phase 1 final sign-off | Low | He'd already greenlit the methodology. No explicit approve-and-ship. Treating as implicit. |

## Methodology decisions locked

- Tribe internal jobs (Tribe.xyz, IR) **included** (per Gustavo Q1 + the broader Tribe-internal-unfiltered memo)
- Test clients excluded (standard list)
- Archived jobs included
- Roster: Sourcer L1–L3 + Sourcing Lead + Sourcing Mgr, active OR terminated 2025+
- Sanja Pavlovikj excluded entirely (full-time IR, not sourcing)
- Cross-client work excluded ("Option C"): if a sourcer was on-client AND sourced for their own billed client, that's TA work, not internal sourcing
- <5 contacts per sourcer-quarter dropped as noise
- Contiguous division periods use half-open intervals `[start, end)` to avoid double-counting transition days
