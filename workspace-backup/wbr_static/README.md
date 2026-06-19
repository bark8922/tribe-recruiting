# wbr_static/ — static sources for wbr_view.sql

These CSVs are the non-Bubble sources Power BI's WBR report relies on. They live
outside Keboola/reporting-v2 and must be merged into the WBR output in pandas
(per Blake's constraint: do not load anything into Keboola).

| File | Source | Grain | Rows |
|---|---|---|---|
| wbr_ta_target.csv | Andy's "TA Target" tab in the WBR Google Sheet | Client × TA × Year × Month | 1,540 |
| wbr_ts_weekly.csv | Andy's "TS Weekly Note" tab | TS × Year × Week | 2,989 |
| sourcing_team_list.csv | Gustavo's "Levels" sheet | one row per person | 126 |
| sourcer_ta_transitions.csv | Gustavo's Sourcer→TA transition sheet | one row per person | 72 |
| bamboohr_roster_current.csv | BambooHR MCP `get_employee_directory` | one row per active employee | 75 |
| **bamboohr_supervisor_history.csv** | **BambooHR custom report — TO BE CREATED** | one row per (employee × effective_date) | — |

---

## BambooHR custom report — what Blake needs to create

The BambooHR MCP exposes `get_employee_directory` (current snapshot only) and
`run_custom_report(report_id)` (returns whatever fields a pre-saved report has).
It does **not** expose the tabular `jobInfo` history endpoint, so historical
`report_to` is not reachable programmatically without a saved report.

Report 149 ("Active Tribster List") already exists but only contains
`hireDate`, `terminationDate`, `employmentHistoryStatus`, `payGroup`. No
supervisor, no history.

### Action: create a new BambooHR custom report

1. Go to BambooHR → Reports → Custom Reports → New Report.
2. Report type: **Table report**.
3. Name it something like **"WBR Historical Manager Structure"**.
4. **Enable "Include historical data"** (this is the key toggle — without it
   the report returns current values only, which is what we already have).
5. Fields to include:
   - Employee # (id)
   - Last Name, First Name (fullName2)
   - Effective Date  ← crucial for history
   - Job Title
   - Department
   - Division
   - Location
   - Supervisor (Reports To)
   - Employment Status
   - Hire Date
   - Termination Date
6. Filter: **all employees** (include terminated — we need history going back
   to 2024 for the WBR backfill).
7. Save the report. BambooHR assigns it a numeric ID (visible in the URL after
   save, e.g. `/app/reports/291`).
8. Send me that report ID and I'll pull it via `run_custom_report` and drop it
   into `bamboohr_supervisor_history.csv`.

### Why we need this

Power BI's `Historical Manager Structure WBR` Snowflake table backs the
"cut by manager chain" slice of the WBR dashboard. Without historical
report_to, we can't answer "in week X, which TAs rolled up to Tijana?" because
people have moved managers over the last two years. The snapshot in
`bamboohr_roster_current.csv` is fine for today's numbers but will produce
wrong history on past weeks.

### Fallback if historical reports are disabled in your BambooHR tier

If the "Include historical data" toggle isn't available on our plan, the
alternatives are:
1. Export the `jobInfo` table from BambooHR Settings → Data Tools → Tabular
   Data → Export (supervisor changes live here).
2. Or: ignore history for the April MVP and use current supervisor for all
   weeks. Accuracy impact is small for Q1-Q2 2026 (most people haven't moved)
   but will drift as we look further back.
