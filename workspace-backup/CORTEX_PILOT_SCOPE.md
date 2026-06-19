# Cortex Analyst Pilot — Scope (Scenario A: own Snowflake, Keboola stays)

**Goal:** prove (or kill) the idea that people can ask recruiting questions in plain English
and get accurate, database-computed answers — before building anything user-facing.

**Decisions needed from Blake:**
1. Green-light the 30-day Snowflake trial (free, $400 credits, no card needed — covers the whole pilot)
2. Which 2–3 people get to ask questions during the pilot
3. Confirm pilot data = aggregates only (no candidate names/PII) — recommended

## Setup
- Snowflake trial account: AWS **eu-central-1 (Frankfurt)** — Cortex Analyst is natively available there; Standard edition. Blake signs up (~5 min), shares the account URL + a service login.
- Keboola Snowflake **writer**: new phase on the existing flow pushing ~8 core output tables daily (WBR/MBR weekly aggregations, tth_jobs, tth_monthly, ts_summary, project dashboard rollups). I configure this via the Keboola MCP. Adds a few credits/month.
- No changes to anything that exists today. Dashboard, flows, PROD V2 untouched.

## The real work: semantic model (~2–3 days)
- Port our validated metric definitions (attribution rules, archived/test filters, ISO weeks,
  client groupings like Wolt=DoorDash=SevenRooms) into a Cortex semantic model.
- Add ~15 **verified queries** lifted from the battle-tested WBR/MBR/TTH SQL — these anchor accuracy.
- Source of truth: the SQL already in Keboola transformations, not rewritten logic.

## Test protocol (~half day)
- 20 questions with known dashboard answers ("how many actual screens did X do in May",
  "hires per client last 4 weeks", "avg time to hire 2026").
- Run in Snowsight's built-in chat — no UI built until this passes.
- **Pass:** exact match vs dashboard on in-scope questions; clean refusal (not a guess) on out-of-scope.
- Track cost per question (expect ~$0.15–0.20 all-in).

## Kill criteria (decide at day 30, $0 spent)
- Accuracy below ~95% on the test set after model iteration → stop, fall back to the
  pre-built-CSV/digest approach.
- If it passes → convert trial to paid (~$30–100/mo) and scope the Slack front end as phase 2.

## Risks
- **Semantic ceiling:** good at "how many X by Y in period Z", weak at open-ended "why did it drop". Set expectations with pilot users.
- **Model drift:** when we change a metric definition in Keboola, the semantic model must be updated too — same discipline as the dashboard.
- **Region/feature gaps on trial:** Cortex Analyst is GA in Frankfurt, but if anything is trial-gated we find out in week 1 at zero cost.

## Timeline
Week 1: account + writer + first semantic model draft. Week 2: verified queries + test rounds.
Decision by end of week 2. Total effort ~4 days of my time, ~30 min of Blake's.
