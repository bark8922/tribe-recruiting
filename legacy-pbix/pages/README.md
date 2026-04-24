# Per-Page Deep Dives

One markdown file per Bucket C + D page (load-bearing Power BI pages that haven't been replaced in our new dashboard, plus the two internal-recruiting pages).

Each file contains:

1. Metadata — visual count, tables referenced, measure/calc-column counts
2. **"What this page answers"** — TODO section for Blake/Andy to write plain-English
3. **Rebuild decision checkbox** — Keep / Fold in / Archive / Drop
4. Visual-by-visual list with title, type, and which table → field references it pulls from
5. Every DAX measure used on the page, with its full formula
6. Every calculated column used, with its formula
7. List of plain columns referenced
8. Power Query M source for each table used

## Page index

| File | Page | Visuals | Measures | Calc cols |
|------|------|---------|----------|-----------|
| `sourcing-stats.md` | Sourcing Stats | 23 | 9 | 6 |
| `time-to-hire.md` | Time to Hire | 22 | 12 | 3 |
| `recruiting-trends.md` | Recruiting Trends & Conversion Rate | 30 | 21 | 3 |
| `new-role-estimate.md` | New Role Estimate | 21 | 12 | 1 |
| `hired-candidate-salary-audit.md` | Hired Candidate Salary Audit | 18 | 3 | 2 |
| `open-roles.md` | Open Roles | 17 | 3 | 3 |
| `executive-search.md` | Executive Search | 15 | 5 | 0 |
| `okr.md` | OKR | 14 | 6 | 1 |
| `candidate-response-rate.md` | Candidate Response Rate | 19 | 10 | 2 |
| `weekly-progress.md` | Weekly Progress | 19 | 16 | 2 |
| `pipelines-health.md` | Pipelines Health | 16 | 20 | 3 |
| `active-pipelines.md` | Active Pipelines | 14 | 3 | 1 |
| `viewed-contacted-health.md` | Viewed / Contacted Health | 18 | 5 | 4 |
| `internal-recruitment.md` | Internal Recruitment | 21 | 17 | 3 |
| `internal-recruiting.md` | Internal Recruiting _(Recruitee)_ | 10 | 1 | 1 |

## How to use these

For each page, decide one of four fates and check the box in the file:

- **Keep** — rebuild in `bark8922/tribe-recruiting`. Use the DAX measures as the spec.
- **Fold in** — the logic gets merged into an existing tab (e.g. Candidate Response Rate probably goes into WBR).
- **Archive only** — the DAX + M are preserved in this repo; if anyone ever asks "how did we calculate this?", they can find it here. No live dashboard.
- **Drop** — nobody actually used this.

For "Keep" and "Fold in" decisions, the DAX in the file is the spec. Translate each measure to SQL (we already have patterns for most — see `reference_verified_query_logic.md` memory) and point the new visual at the equivalent Snowflake column.

## Related

- `../PAGE_INVENTORY.md` — all 35 pages bucketed A-E with the decision log table
- `../dax_index.json` — machine-readable index of all 200 measures, 290 columns, 34 table sources, and 5 shared M expressions. Useful for scripts that want to look up a measure by name.
- `../../POWERBI_DAX_MEASURES.md` — the original 105KB extraction from 2026-04-08 (redundant with the structured index but human-readable)
- `../../powerbi_export/` — the raw PBIP export (authoritative source)
