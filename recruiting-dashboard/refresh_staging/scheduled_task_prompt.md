# Scheduled-task prompt: tribe-recruiting-dashboard-refresh

This is the exact prompt registered with `create_scheduled_task`. Keep it
self-contained — future runs do NOT have access to the session where it was
created.

---

## Objective

Rebuild the Snowflake-side `dashboard_data_snowflake.json` for the Tribe.xyz
recruiting dashboard. This is the "new pipeline" half of a parallel-run setup
— the Power BI side (`dashboard_data.json`) is fed by a separate Bubble/n8n
pipeline and MUST NOT be touched by this task.

Success = a new commit on `bark8922/tribe-recruiting` main branch with an
updated `recruiting-dashboard/src/dashboard_data_snowflake.json` and no other
file changes.

## Working directory

Resolve at runtime (session IDs change between runs):

```bash
WORKDIR=$(find /sessions/*/mnt -maxdepth 3 -type d -name refresh_staging 2>/dev/null | head -1)
cd "$WORKDIR"
```

If that returns nothing, the mounted "Recruiting Dashboard" folder isn't
present in this session — abort and report. Otherwise `$WORKDIR` should end
with `Recruiting Dashboard/refresh_staging`.

## Step 1a — Sync the WBR Google Sheet (MANDATORY, do not skip)

The per-week roster that drives the dashboard's Week filter and MBR window is
read from local CSVs under `wbr_static/`. Those CSVs come from Andy's WBR
Target Google Sheet. If this step is skipped, the refresh will render off a
stale roster — the dashboard will not show newly-added weeks even when the
Snowflake data is up to date. Always run this step before Step 1 so that new
weeks (e.g. w16, w17) propagate to the UI.

```bash
cd "$WORKDIR/.."
python3 sync_google_sheet.py 2>&1 | tail -15
cd "$WORKDIR"
```

Expected output: five "[export] ... -> ....csv  OK: N data rows exported" lines
and a final "DONE" line (10000+ total rows). If the script reports ERROR
(missing credentials, auth failure, missing deps), abort and report — do NOT
continue with stale CSVs. One-time dep install if needed:
`pip install gspread google-auth --break-system-packages`.

## Step 1 — Run 5 Keboola MCP queries

For each of these SQL files, read it from disk and call the Keboola MCP
`query_data` tool (find the full tool name with ToolSearch, query
"query_data keboola"). Pass the full file contents as the `sql_statement`
argument. Write the returned rows (header row + data rows) to the matching
CSV file next to the SQL:

| SQL file                 | Output CSV                    |
| ------------------------ | ----------------------------- |
| `wbr_weekly.sql`         | `snowflake_wbr.csv`           |
| `wbr_jobs_weekly.sql`    | `snowflake_wbr_jobs.csv`      |
| `ts_weekly.sql`          | `snowflake_ts.csv`            |
| `ts_conversion.sql`      | `snowflake_ts_conversion.csv` |
| `aux_12w.sql`            | `snowflake_aux_12w.csv`       |

Rough expected row counts: wbr ~670, wbr_jobs ~780, ts ~700,
ts_conversion ~65, aux_12w ~250. If any query fails or returns zero rows,
abort — do NOT render with partial data.

Note on wbr_jobs_weekly.sql: it uses a DIFFERENT TA attribution than
wbr_weekly.sql. Jobs are grouped by `event.who_event_created_for` (PBI DAX
replica) rather than `job.job_recruiter`, so some TAs in the output CSV
won't appear in wbr_weekly.sql and vice versa. render_json.py handles both
attributions separately — don't try to merge them into one CSV.

## Step 2 — Render + mirror locally

```bash
cd "$WORKDIR"
python3 refresh_daily.py
```

This will:
- Fail fast if any of the 5 CSVs is missing
- Run `render_json.main()` → produces `rendered_dashboard_data.json`
- Copy it to `<workspace>/dashboard_data.json` — WAIT, that mirrors the PBI
  file path. Override the mirror destination so this task mirrors ONLY the
  snowflake file:

```bash
cp "$WORKDIR/rendered_dashboard_data.json" "$WORKDIR/../dashboard_data_snowflake.json"
```

(In other words: run `refresh_daily.py --render-only` to skip its automatic
mirror step, then copy the rendered JSON to `dashboard_data_snowflake.json`
manually. This avoids touching the PBI-side local file.)

If the render step fails, abort — do not push.

## Step 3 — Push to GitHub (Snowflake file only)

Use the `github` skill to clone `bark8922/tribe-recruiting`, replace
ONLY `recruiting-dashboard/src/dashboard_data_snowflake.json` with the
rendered JSON, commit, and push to main.

CRITICAL:
- Do NOT modify `recruiting-dashboard/src/dashboard_data.json` (the Power BI
  source). Git diff before commit must show exactly one changed file:
  `recruiting-dashboard/src/dashboard_data_snowflake.json`.
- Do NOT modify any other files (App.jsx, main.jsx, index.css, etc.).
- Do NOT force-push.

Commit message:
```
refresh: Snowflake-side dashboard_data_snowflake.json rebuild (YYYY-MM-DD HH:MM CET)
```

Cloudflare Pages auto-builds on push to main — the Snowflake tab in the
live dashboard at tribe-recruiting.pages.dev will show the new numbers
after ~1-2 min.

## Step 4 — Verify & report

In the task output, report:
- Row counts for each of the 5 CSVs
- Top-level key counts from `render_json.main()` stdout
- Git commit SHA
- Confirmation that the git diff contained only `dashboard_data_snowflake.json`
- Any parity warnings logged during render

## Validation envelope

Healthy envelope = single-cell drift ≤ ±3 vs the previous snowflake JSON. If
any of the following trip, do NOT auto-push — write a diff report to
`refresh_staging/diff_reports/refresh_YYYY-MM-DD.md` and stop:

1. **MBR TA actuals drift** — `mbr_ta_actuals` rostered field-exact drops
   below 90% (normally ~97.6%) OR a single (TA, metric) drifts > ±5 for two
   consecutive runs.

2. **Per-client 12w Hires sanity check (WBR Client Summary)** — sum
   `mbr_client_totals[c].hires_12w` across all clients. If any single client
   drifts > ±3 vs the previous run, flag it. Hard red flag: if the Wolt
   sub-BU total (`Wolt HQ + Wolt Tech + Wolt Market + Wolt C&S + Wolt NBB
   + Wolt Germany + Wolt Volume`, excluding raw `Wolt` catch-all) exceeds
   200 — that's the signature of `event.who_event_created_for` attribution
   bleeding back in (the bug we fixed 2026-04-20 where Ketevan Khorava alone
   got 193 hires credited to her as TA). PBI reference is ~140-160 total
   across the 6 Wolt sub-BUs excluding Wolt Volume, so a threshold of 200
   gives comfortable headroom but catches the 709 regression immediately.

3. **Schema presence** — the output JSON must contain all of:
   `targets`, `wbr_actuals`, `mbr_client_totals`, `wbr_ta_weekly_roster`,
   `jobs`, `mbr_ta_actuals`, `ta_jobs_weekly`. If any is missing or has
   zero length, abort.

4. **Roster freshness** — `wbr_ta_weekly_roster` must contain the current
   ISO week (e.g. `w16` when running in 2026 week 16). If it doesn't, Step
   1a's Google Sheet sync is likely stale — abort.

5. **# Jobs per-week sanity check (WBR Client Summary)** — after rendering,
   compute total # Jobs for the current week using the same filter App.jsx
   applies: sum `ta_jobs_weekly[wNN][client|TA]` where `(normalizeClient,
   normalizeTa)` exists in `targets` with non-empty team_group, split Wolt
   catch-all via recruiter→sub-BU map. Expected envelope: 90-160 total
   across the 6 Wolt sub-BUs + non-Wolt active clients (PBI reference was
   130 for w16 2026-04-20). Hard red flag: total > 200 or any single
   display client drifts > ±5 vs previous run's rendered JSON.

When any check trips, still save the rendered file locally under
`refresh_staging/rendered_dashboard_data.json` for debugging but do NOT
copy it to `dashboard_data_snowflake.json` and do NOT push.

## App.jsx schema dependencies — do NOT silently drop or rename

The frontend WBR Client Summary relies on the following fields being present
in `dashboard_data_snowflake.json` (and the PBI-side `dashboard_data.json`)
with their current semantics. If a future `render_json.py` refactor removes
or renames any of them, the live dashboard will silently go wrong:

| Field | Used for | Semantics |
|-------|----------|-----------|
| `targets[].team_group` | Filter roster-only TAs out of Client Summary | Empty string = roster placeholder (skip); non-empty = real TA (include). Do not collapse to a default value. |
| `targets[].contacted / actual_screens / moved_to_ats / hires` | Weekly target denominators for color thresholds | **WEEKLY** targets, not monthly. PBI compares weekly actual directly to this value. The "Month" column in the source sheet is the period the target applies to, not the cadence. Never apply a /4.33 divide. |
| `mbr_client_totals[client].hires_12w` | Last 12w Hires column in Client Summary | Per-display-client rollup with Wolt sub-BU correctly split. Keyed by MBR-style names (`'Wolt C&S'`, `'Wolt NBB'`) — the UI maps these to display names. |
| `wbr_ta_weekly_roster[wNN]` | Filter which TAs/clients are shown per week, and which jobs count towards # Jobs | List of `"client|TA"` pair strings. Must include the current ISO week as soon as the Weekly Note is updated. |
| `jobs[]` (with `job_id, client_name, job_recruiter, is_job_archived, is_external_recruiter`) | # Jobs fallback when `ta_jobs_weekly` is missing (older snapshots) | Raw Keboola client names (e.g. `'AVIV '`, `'Wolt'`); App.jsx normalizes. |
| `ta_jobs_weekly[wNN][raw_client\|raw_ta]` | # Jobs column in WBR Client Summary (PBI DAX replica, 99.2% accurate vs PBI w16) | TA attribution = `event.who_event_created_for` (different from wbr_weekly's `job.job_recruiter`). Raw client name. App.jsx applies Wolt sub-BU split via `recruiterToWoltSubBu` + target-roster filter (`team_group` non-empty). |

**Why these are pinned:** verified against the PBI w16 screenshot
2026-04-20. WBR totals at 100.0–100.8% accuracy rely on the full set.
Removing `team_group` re-introduces the Iryna-Dyda +101 contacted over-count
on Aviv. Removing `mbr_client_totals` re-introduces the 709 Wolt 12w-hires
inflation. Removing `wbr_ta_weekly_roster` breaks the # Jobs filter and the
per-week Client Summary itself.

## Pending pipeline work (not yet in scope of this scheduled task)

- **Wolt Volume sub-BU assignment.** Three TAs (Anna Golubeva, Jaksa
  Marojevic, Nemanja Erdevički) emit jobs against raw `Wolt` but aren't
  in `data.targets` under any Wolt sub-BU. Adding them to Andy's WBR TA
  Target Google Sheet with team_group set (probably Wolt Volume) would
  close the single remaining Wolt NBB -1 gap in # Jobs. Flag this to Andy
  when doing the next target-sheet review.

## Do NOT

- Touch `dashboard_data.json` (the Power BI source). It belongs to a
  different pipeline.
- Hard-code Snowflake credentials — auth goes through Keboola MCP only.
- Commit anything other than `dashboard_data_snowflake.json`.
- Force-push. If `main` has diverged, abort and flag for human review.
- Delete or modify files in `refresh_staging/` other than the 5 output CSVs
  and `rendered_dashboard_data.json`.
