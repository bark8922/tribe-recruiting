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

## Step 1 — Run 6 Keboola MCP queries

For each of these SQL files, read it from disk and call the Keboola MCP
`query_data` tool (find the full tool name with ToolSearch, query
"query_data keboola"). Pass the full file contents as the `sql_statement`
argument. Write the returned rows (header row + data rows) to the matching
CSV file next to the SQL:

| SQL file                    | Output CSV                    |
| --------------------------- | ----------------------------- |
| `wbr_weekly.sql`            | `snowflake_wbr.csv`           |
| `wbr_jobs_weekly.sql`       | `snowflake_wbr_jobs.csv`      |
| `wbr_ts_jobs_weekly.sql`    | `snowflake_ts_jobs.csv`       |
| `ts_weekly.sql`             | `snowflake_ts.csv`            |
| `ts_conversion.sql`         | `snowflake_ts_conversion.csv` |
| `aux_12w.sql`               | `snowflake_aux_12w.csv`       |

Rough expected row counts: wbr ~670, wbr_jobs ~780, ts_jobs ~820, ts ~700,
ts_conversion ~65, aux_12w ~600. If any query fails or returns zero rows,
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
- Fail fast if any of the 6 CSVs is missing
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
- Row counts for each of the 6 CSVs
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
   `jobs`, `mbr_ta_actuals`, `ta_jobs_weekly`, `ts_jobs_weekly`,
   `ts_weekly`, `ta_weekly_notes`, `ts_conversion`. If any is missing or
   has zero length, abort.

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

## Attribution rules (pinned by PBI validation 2026-04-20)

These are the attribution rules `aux_12w.sql`, `wbr_weekly.sql`,
`wbr_jobs_weekly.sql`, and `wbr_ts_jobs_weekly.sql` must preserve.
Drifting any of these silently regresses accuracy by 5-35%+ on
individual TAs/TSes.

| Metric | Attribution | SQL location | PBI reference |
|---|---|---|---|
| Weekly Contacted / Screened / Actual Screens / ATS / Offers / Hires (TA) | `job.job_recruiter` on `candidate_stage.date_*` in week | `wbr_weekly.sql` | Aviv/Enam/Glovo exact |
| 12w Hires (TA) | `event.who_event_created_for` on authoritative Hired event (rn=1) | `aux_12w.sql` `hired_auth` | Aviv 9 exact |
| **12w Screens / 12w ATS (TA)** | **`event.who_event_created_for`** on latest Evaluation / Interview event per `(candidate, date)` | `aux_12w.sql` `evaluation_auth` + `ats_auth` | Lejla 169/104, Jonaed 170/96 — ALL EXACT |
| # Jobs per week (TA — WBR Client Summary) | `DISTINCTCOUNT(event.job_id)` per `(client, event.who_event_created_for, week)` via `event.date_created` | `wbr_jobs_weekly.sql` | 129/130 = 99.2% |
| **# Jobs / # TA / TA Names per week (TS Weekly)** | **`DISTINCTCOUNT(event.job_id)` filtered to CONTACTED events only**: `(event_type='Moved to stage' AND moved_to_stage='Contacted') OR (event_type='Candidate created' AND moved_to_stage='Contacted')`, grouped by `TRIM(candidate.candidate_sourcer)`. `# TA` = DISTINCT `job.job_recruiter` EXCLUDING self (when TS = job_recruiter). | `wbr_ts_jobs_weekly.sql` | **10/11 TSes exact vs PBI w16, 11/11 TA names exact** (Andrea 7/4, Elena 4/0, Gustavo 4/2, Jovana 4/0, Marina 5/2, Milica 1/0, Naledi 4/0, Nare 7/3, Rodrigo 5/3, Valeriia 4/4, Zelimir 1/0) |
| **TS Overall Conversion Rate — Active Pipelines + funnel (AP, C, PR, RS, AS, ATS per TS)** | **Active Pipelines** = `job.job_sourcer = TS` **AND** at least one event with `credit_sourcer(e) = TS` on a candidate of that job (Andy Hsu rule, 2026-04-14). Funnel counts = candidates on those active pipelines where an event credited to TS matches the stage. Filters: `job.test <> 'true'`, `candidate.is_candidate_archived <> 'true'`, `job.is_job_archived <> 'true'`, `job.job_sourcer` not in NULL/''/'-not available-'. Positive Response has DAX date gate `event.date_created >= '2025-04-14'`. AS/ATS require `candidate_stage.date_screen_actual`/`date_interview` 2024+. | `ts_conversion.sql` | **AP 12/12 exact + colour triplets 12/12 exact vs PBI w16; aggregate volume accuracy 98.99%** (sum of \|PR+AS+ATS deltas\| = 34 on a 3367 total). Per-row max cell delta ≤7 (Marina PR+7, Naledi PR+7, others ≤4). |
| Jobs open ≥ 60 days | `job.job_recruiter` | `aux_12w.sql` `jobs_60d_base` | matches live |

**Do NOT** change 12w Screens / ATS back to `job.job_recruiter`. **Do
NOT** change 12w Hires to `job.job_recruiter`. **Do NOT** remove the
Contacted-only filter from `wbr_ts_jobs_weekly.sql` — counting all
events (downstream screens/interviews/hires) inflates TS # Jobs by 2x
(Jovana 4 → 9, Marina 5 → 11, Elena 4 → 8). The Contacted filter is
what separates "jobs the TS is actively sourcing this week" from
"jobs the TS has any historical touch on."

**12w window boundary (2026-04-20 fix):** PBI's "Last 12 weeks" rolling
window INCLUDES the current week. The anchor CTE must be
`cur_wk = DATEADD('day', 6, DATE_TRUNC('week', CURRENT_DATE()))` (this
week's Sunday inclusive), NOT `DATEADD('day', -1, …)` (last Sunday).
Incorrect: Jonaed 158 screens. Correct: 170 (matches PBI).

## App.jsx schema dependencies — do NOT silently drop or rename

The frontend WBR Client Summary relies on the following fields being present
in `dashboard_data_snowflake.json` with their current semantics. If a future
`render_json.py` refactor removes or renames any of them, the live dashboard
will silently go wrong:

| Field | Used for | Semantics |
|-------|----------|-----------|
| `targets[].team_group` | Filter roster-only TAs; Wolt sub-BU recruiter map | Empty string = roster placeholder (skip); non-empty = real TA. Do not collapse to a default value. Wolt Tech screens inflated from 1 → 7 if we include empty-team_group rows in the Wolt sub-BU map. |
| `targets[].contacted / actual_screens / moved_to_ats / hires` | Weekly target denominators for color thresholds | **WEEKLY** targets, not monthly. PBI compares weekly actual directly to this value. The "Month" column in the source sheet is the period the target applies to, not the cadence. Never apply a /4.33 divide. |
| `targets` overall | Built fresh from `wbr_ta_target.csv` on every render | `render_json.load_ta_targets_from_csv(preserve_from_live=live['targets'])` picks the newest 2026 month per (Client, TA) with values; preserves team_group from live when present. Do NOT fall back to `live.targets` verbatim — that's the bug where Aiven stayed at 0/0/0 and lost its colors. |
| `ta_weekly_notes` | Comments in TA Detail | Rebuilt from `wbr_ta_weekly_note.csv` (not carried forward from live). Without this rebuild, new-week comments (e.g. w16 after Andy edits the sheet) don't appear until the Bubble/n8n PBI-pipeline catches up. |
| `mbr_client_totals[client].hires_12w` | Last 12w Hires column in Client Summary | Per-display-client rollup with Wolt sub-BU correctly split. Keyed by MBR-style names (`'Wolt C&S'`, `'Wolt NBB'`) — the UI maps these to display names. Total row sums ALL clients incl. Wolt Volume (matches PBI's Total=741 for w16 instead of the visible-sum 141). |
| `wbr_ta_weekly_roster[wNN]` | Filter which TAs/clients are shown per week, and which jobs count towards # Jobs | List of `"client\|TA"` pair strings. Must include the current ISO week as soon as the Weekly Note is updated. |
| `jobs[]` (with `job_id, client_name, job_recruiter, is_job_archived, is_external_recruiter`) | # Jobs fallback when `ta_jobs_weekly` is missing (older snapshots) | Raw Keboola client names (e.g. `'AVIV '`, `'Wolt'`); App.jsx normalizes. |
| `ta_jobs_weekly[wNN][raw_client\|raw_ta]` | # Jobs column in WBR Client Summary (PBI DAX replica, 99.2% accurate vs PBI w16) | TA attribution = `event.who_event_created_for` (different from wbr_weekly's `job.job_recruiter`). Raw client name. App.jsx applies Wolt sub-BU split via `recruiterToWoltSubBu` + target-roster filter (`team_group` non-empty). |
| `ts_conversion[]` (one row per TS, current-state snapshot) | TS Overall Conversion Rate table (AP, C, PR, RS, AS, ATS per TS) | Built from `snowflake_ts_conversion.csv` (`ts_conversion.sql`). Roster-filtered on the dashboard by `ts_weekly[week=N].ts`. Columns: `ts, active_pipelines, contacted, positive_response, recruiter_screens, actual_screens, ats`. 12/12 AP exact + 12/12 colour triplets vs PBI w16; 98.99% aggregate volume accuracy. Do NOT drop `recruiter_screens` — it's the hidden denominator for the `% Screens to Actual Screen` column. |

**Why these are pinned:** verified against the PBI w16 screenshot
2026-04-20. WBR totals at 100.0–100.8% accuracy rely on the full set.
Removing `team_group` re-introduces the Iryna-Dyda +101 contacted over-count
on Aviv. Removing `mbr_client_totals` re-introduces the 709 Wolt 12w-hires
inflation. Removing `wbr_ta_weekly_roster` breaks the # Jobs filter and the
per-week Client Summary itself. Reverting the 12w window to last-Sunday
loses Jonaed Iqbal's current-week screens (-12 gap).

## App.jsx behavior rules — pinned by user requests

The following behaviors in `recruiting-dashboard/src/App.jsx` were set
deliberately during the 2026-04-20 calibration; they reference data field
semantics above. Don't revert them without checking here.

| Rule | Where | Why |
|---|---|---|
| Default dashboard tab = `'snowflake'` (not `'pbi'`) | `useState('snowflake')` | Snowflake pipeline is the accurate source of truth; the "Power BI" toggle is for legacy Bubble-pipeline comparison only. |
| Client BU grouping is purely by display client name (NOT per-TA team_group) | `getBuGroup(displayClient)` + `DOLPHINS_WHALES_CLIENTS` set | Aviv TAs are individually labelled Ponies/Unicorns in the sheet (internal team-mgmt labels) even though Aviv the client is Dolphins & Whales. Using per-TA team_group split Aviv across both groups. |
| Dolphins & Whales = `Aviv`, `Aiven`, any client starting with `Wolt` | `DOLPHINS_WHALES_CLIENTS` set | Everything else → Ponies & Unicorns. |
| TA rows in WBR Client Summary actuals loop skip `team_group === ''` | `if (!t.team_group) return;` in clientSummary forEach | Keeps Iryna-Dyda-style weekly-note placeholders out of client totals. |
| Wolt sub-BU recruiter map (`recruiterToWoltSubBu`) also filters `team_group === ''` | inside WBR tab | Without this, Simon Siew + Vladimir Stankovic (roster-only Wolt Tech entries) absorb cross-client Wolt screens and inflate Wolt Tech from 1 → 7. |
| TA Detail table hides rows with no weekly activity AND no comment/reasoning | `if (!hasWeeklyActivity && !hasNote) return;` | Keeps the table focused on people with something to review. Rostered but idle TAs with a comment still render. |
| TA Detail dedupes by `(display_client, normalizeTa(TA))`, keeps highest-activity row | `deduped` Map in taDetail | Zelimir Stajcic has target rows under both SevenRooms and Wolt HQ, both normalize to Wolt HQ — would render twice without dedupe. |
| A repeated header `<tr>` renders under the Ponies & Unicorns banner | `repeatHeader` const in the render | sticky headers don't work because the table container is `overflow-x-auto` only; the duplicate is the guaranteed-working fallback. |
| 12w Hires Total row sums ALL `mbr_client_totals` (includes Wolt Volume) | `Object.values(data.mbr_client_totals).reduce(...)` | Matches PBI Total=741 behaviour (visible rows sum to 141, but PBI's Total includes hidden Wolt Volume etc.). |
| TS Overall Conversion Rate colour thresholds: `%C→PR ≥20 green`, `%RS→AS ≥60 green`, `%AS→ATS ≥50 green` | `cell(pct, greenAt)` helper in TS Conversion table | Calibrated 12/12 vs PBI w16 screenshot. Do NOT revert to 75/55 (old eyeballed thresholds) — both regress 4+ colour cells (Nare/Marina/Milica). |
| TS Conversion percent cells show `—` when numerator is 0 (not `0%` red) | `(contacted > 0 && positiveResponse > 0) ? ... : null` | PBI renders blank for 0-numerator rows (e.g. Valeriia w16 PR=0). Showing `0%` red distorts the scorecard. |

## Pending pipeline work (not yet in scope of this scheduled task)

- **Wolt sub-BU event-level routing** (Elena Petrovska +34 case). She
  works across multiple Wolt sub-BUs but `recruiterToWoltSubBu` assigns
  her 100% to Wolt HQ, so cross-sub-BU events pile on Wolt HQ. Needs
  per-event sub-BU lookup (use the specific job's sub-BU at event time,
  not the TA's canonical assignment). Affects `aux_12w.sql` `evaluation_auth`
  + `ats_auth` TA grouping — or handle downstream in `render_json.py`
  when splitting raw `Wolt` into sub-BUs.

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
- Delete or modify files in `refresh_staging/` other than the 6 output CSVs
  and `rendered_dashboard_data.json`.
