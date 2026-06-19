# Phase 2 Brief — Cost of Sourcing Team

**Phase:** 2 of 7. **Status:** Draft for Blake's review. No code touched.
**Step:** Brief (1/8 in plan §5).

## 1. What the view shows

A "Cost" tab/section showing the **all-in cost of the internal sourcing team** and the unit economics it implies:

- Cost of sourcing team per quarter (sum of allocated salary cost)
- Cost per hire (cost ÷ hires from Phase 1)
- Cost per contacted (efficiency proxy)
- Cost per sourcer per quarter (rollup for capacity planning)
- Optional: profitability sketch (revenue from hires made vs cost spent)

## 2. Audience / cadence / action

- **Readers:** same 8 sourcing leads.
- **Cadence:** Quarterly (rolls up monthly cost).
- **Action:** Direct answer to Gustavo's kickoff question — "is the internal sourcing team profitable, can we afford another sourcer." Same lever the Finance dashboard provides for the company at large, but scoped to sourcing.

## 3. Carry-over from Phase 1 (do NOT re-derive)

- 20-sourcer BambooHR roster + division periods (Bench/Internal vs on-client) — same source table.
- v1.5 methodology to decide who counts as "internal sourcer for this quarter."
- Sanja excluded (fully-IR).
- <5 noise threshold.
- Hires-per-quarter from `sourcing_dashboard_per_sourcer` — already in data.json.

## 4. New data needed

- **`actual_spend` per Tribster** from the Finance dashboard pipeline (`bark8922/tribe-dashboard`, BambooHR-driven, all-in cost incl salary + employer taxes + benefits — per memory `finance-dashboard-runs-on-github-actions`).
- Granularity needed: per Tribster per month, ideally for 2025-onwards.
- Two ways to plumb it: (a) Keboola transformation pulls from same BambooHR source the Finance dashboard uses, or (b) Finance dashboard exposes a small CSV we read at refresh time. Decide at lineage step.

## 5. Methodology — three calls Blake needs to make in the brief

These mirror Phase 1's "what counts as internal sourcing" question, applied to cost:

**A. Cost attribution while on-client.** If Marina is Internal Oct-Apr then Aviv May+, do we:
- (i) Pro-rate her cost: count Jan-Apr in "internal sourcing cost," May+ in "client billable cost." OR
- (ii) Cliff: include her quarter cost only if she was internal for ≥50% of the quarter.

Phase 1's "only count work during Bench/Internal" rule maps cleanest to (i) pro-rate. Recommend (i).

**B. Leadership cost (Gustavo, Andrea).** Per Phase 1 we counted their work when they were on Bench. For cost:
- (i) Include their full quarter cost — they manage the team, their cost is the team's cost.
- (ii) Pro-rate same as ICs.

Gustavo himself said "no need to separate us" in the kickoff. Recommend (i).

**C. Sanja (fully-IR).** Phase 1 excludes her from output. For cost:
- (i) Also exclude her cost (she's IR overhead, not sourcing).
- (ii) Include her cost (she's still on the org chart under sourcing).

Recommend (i) for symmetry with Phase 1.

## 6. Out of scope for Phase 2

- Revenue side of profitability (Phase 2.1 if Gustavo wants it).
- BambooHR client-allocation history is shared with Phase 1; not a new dataset.
- Per-month breakdown view (quarterly only for v1).

## 7. Open questions for Blake

1. **Sign off on this brief?** If yes, move to Step 2 (data lineage — verify the Finance dashboard's `actual_spend` table is queryable from our Keboola side, or we replicate the BambooHR pull).
2. **Methodology A/B/C above** — confirm (i) pro-rate, (i) include leads, (i) exclude Sanja? Or override.
3. **Profitability v1?** Should we sketch the revenue side too, or keep this strictly a cost view? Revenue per hire is non-trivial to attribute.

## 8. What I'll NOT do without explicit approval

- Pull any cost data into Keboola.
- Touch the Finance dashboard pipeline.
- Build the view.

Just the methodology decisions in §5 and parity check on a single quarter before the mock.
