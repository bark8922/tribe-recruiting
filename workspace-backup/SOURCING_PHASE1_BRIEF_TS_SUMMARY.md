# Phase 1 — TS-Summary Quarterly Rollup

**View:** Quarterly table — Quarter / Contacted / Pos Response / RS / Actual Screens / ATS / Offered / Hired / Team Size / Hires-per-Sourcer.
**Audience:** 8 sourcing leads. **Cadence:** Quarterly.

**Data source:** Existing Keboola block `b0.c6` (same one driving the live KPI-TS Summary tab, commit `0e4f07b`). No new SQL. Quarter = SUM of existing weekly rows.

**Team Size:** Distinct sourcers from the WBR/KPI-TS Summary roster, ≥1 week of activity in the quarter, BambooHR cross-check for active status.

**Parity check:** Our new quarterly rollup vs the live recruiting dashboard's KPI-TS Summary for the same window. Threshold ≥98%. If pass → mock. If fail → trace.

**Historical scope:** 2025 onward, include all 4 quarters with whatever Bubble has, null where it doesn't.

**Decisions I need from Blake:**
1. Brief approved → proceed to parity pull + mock?
2. Pre-2025 (2022–2024) rows: include with backfill where possible, or skip?

**Out of scope:** No SQL written, no Keboola changes, no deploys, no Slack to Gustavo/Mikhail.
