# Sourcing Dashboard — Project Plan

**Status:** Planning — no code yet.
**Owner:** Blake. **Customer:** Gustavo (Head of Sourcing).
**Mandate:** Slow. Accurate. 98% parity vs Gustavo's existing Notion numbers before anything ships.

---

## 1. Audience & Access

Gated view, sourcing leadership only:

- Jacopo (jacopo@tribe.xyz)
- Andrea (andrea@tribe.xyz)
- Sanja (sanja@tribe.xyz)
- Gustavo (gustavo@tribe.xyz)
- Martin (martin@tribe.xyz)
- Kristjana (kristjana@tribe.xyz)
- Salem (salem@tribe.xyz)
- Blake (blake@tribe.xyz)

No one else — not even other TAs/sourcers.

## 2. Source material

- **Gustavo's Notion Hub:** https://app.notion.com/p/Sourcing-Department-Performance-Hub-2878584e90ab80b6982efd0edd7d8d39 *(his personal workspace — captured via Chrome 2026-05-28)*
- **Kickoff meeting transcript:** https://app.fireflies.ai/view/01KSQ46MQ9Z5DTR8XR1NZT9HSA *("Gustavo / Blake — What should we build?", 2026-05-28)*

## 3. Architecture — DECIDED 2026-05-28

**A. Deployment shape: Cloudflare Workers deployment, NO Cloudflare Access in front.**
- New repo `bark8922/tribe-sourcing`, deployed to `tribe-sourcing.tribe-bamboohr.workers.dev` (mirrors `tribe-circle.tribe-bamboohr.workers.dev` and the Finance dashboard's workers domain).
- Optional: bind a `sourcing.tribe.xyz` custom domain for direct-link cleanliness, but the iframe-embed URL stays the workers.dev URL.
- Reuses Keboola pipeline (same data.json deploy pattern as Circle/recruiting/finance).
- **Why no Cloudflare Access at the edge:** Mikhail confirmed 2026-05-12 that Cloudflare Access DOES NOT WORK INSIDE IFRAMES. This dashboard will be iframed inside `overview.tribe.xyz`, so Access would block the iframe from loading.

**B. Gating: app-side email allowlist (the Finance Dashboard pattern).**
- The dashboard reads the user's email from URL params (`?member={email}` passed by Bubble inside the iframe) and validates against an allowlist of the 8 sourcing leadership emails in §1.
- For direct visitors (not via the iframe): they hit a login page that calls Bubble's chromelogin API (`POST https://overview.tribe.xyz/api/1.1/wf/chromelogin` with email + password, returns a `bus|...` token). Same Tribe login users already know from overview.tribe.xyz.
- If email not in allowlist → show "no access" screen.
- This is exactly how `overview.tribe.xyz/finance` works today — clone that pattern.

**Bubble integration sequence (Mikhail's side):**
- Adds a nav button labeled "Sourcing Dashboard" in `overview.tribe.xyz` left sidebar.
- Button visible only to the 8 emails in §1 (Bubble-side conditional visibility).
- Button opens a page that iframes `tribe-sourcing.tribe-bamboohr.workers.dev/?member={Current User's Email}&first_name={Current User's First Name}`.
- Same pattern he used for Finance + Circle.

**C. Build order: easiest-first as listed in §4.** Confirmed Blake 2026-05-28.

## 4. The 7 views — build order easiest → hardest

Build order is driven by **how much data we already have at parity**, not by Gustavo's stated cadence priority. We tackle the wins first so Gustavo sees something credible early.

| # | View | Cadence | Difficulty | Already have? | Notes |
|---|---|---|---|---|---|
| 1 | **TS-Summary (quarterly rollup)** | Quarterly | XS | ✅ KPI-TS Summary tab @ 11/11 within 10% vs PBI | Quarterly rollup of existing block `b0.c6`. Add team-size column. |
| 2 | **Cost of Sourcing Team** | Ad-hoc | S | ✅ Finance dashboard has `actual_spend` per Tribster; recruiting has hires per sourcer | New SQL block joining the two on `tribster_name`. |
| 3 | **WBR Comments for Context** | 6-monthly | S | ✅ Comments live in `wbr_comments` already (commit 29886dc) | Per-sourcer filter + export-friendly view; same data as the existing drill-down. |
| 4 | **Closing Rates** | Monthly | M | ✅ Bubble has `jobs.archived_at` + sourcer-attribution on hires | New SQL: archived-with-sourcer-hire ÷ archived-total. |
| 5 | **Internal vs External TS** | Quarterly | M | ✅ on-client/off-client lives in BambooHR `client_allocation` (already plumbed in Finance) | Pivot existing TS-Summary by client_allocation flag. |
| 6 | **TS Job Ratio** | Monthly | M-L | Partial — need a "currently open" snapshot of jobs at report time + roster denominator | New aggregation: assigned-to-sourcing ÷ total-open. |
| 7 | **Allocation automation** | Weekly (Mon AM) | L | Inputs exist (12w contacted, currently open jobs per sourcer); thresholds don't | Capacity check + Slack/email digest. Pattern: tribe-circle GH Action. |

## 5. Process per view (the gate before each build)

For every single view, we follow this order. No shortcuts:

1. **Brief** — one-pager that captures: what the view shows, who reads it, what cadence, what action it should drive.
2. **Data lineage** — exact table / SQL / CSV inputs. Identify any gaps before we write a line of code.
3. **Mock-up** — static HTML/markdown of the view (numbers + chart shapes), no live data. Compare to Gustavo's Notion side-by-side.
4. **Parity check** — pull Gustavo's most recent quarter/month from his Notion; reproduce the numbers in a notebook; tolerance ≤2%.
5. **Blake approval** — explicit go-ahead in chat before we commit.
6. **Build** — SQL block in Keboola → render in App.jsx (or new sub-route) → push.
7. **Validation** — re-run parity check on the live page. Document the delta in this file under "Parity log."
8. **Gustavo review** — share + walk through; record his feedback.

If parity drops below 98% at step 4 or step 7, we stop and figure out why before moving on.

## 6. Open questions for Blake

Decisions §3.A, §3.B, §3.C closed 2026-05-28. Remaining:

- **Allocation automation thresholds (phase 7)** — Gustavo gave one rule in the meeting: ">250 contacted in 12w = no slots." Full ruleset still needs to be negotiated with him before phase 7 begins.
- **Plan doc location after repo split** — currently `Recruiting Dashboard/SOURCING_DASHBOARD_PLAN.md`. Move to a new `tribe-sourcing/` repo root once it exists, or keep as a workspace-level project doc?
- **Bubble integration coordination** — Mikhail needs to add the nav button + iframe page once the Workers deploy is live. Coordinate the iframe URL + visibility allowlist with him (same flow as Finance dashboard 2026-05-19).
- **Direct-access login UX** — for users hitting `tribe-sourcing.tribe-bamboohr.workers.dev` directly (not via the Bubble iframe), should we show a Bubble-login form (chromelogin API → token), or just a "open this inside the Tribe Tool" redirect? The Finance dashboard's current behavior should be checked and matched.

## 7. Parity log

*(Empty until we start parity checks. Per-view rows go here.)*

| Phase | View | Source quarter/month | Gustavo's value | Our value | Δ | Notes |
|---|---|---|---|---|---|---|

## 8. Next action

Blake reviews this doc, answers the open questions in §6, then we draft the **Phase 1 brief** for TS-Summary (the smallest, highest-confidence win).
