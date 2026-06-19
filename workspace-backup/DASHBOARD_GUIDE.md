# Tribe Recruiting Dashboard — Architecture & Operations Guide

Read this entire file before making ANY changes to the dashboard, the data pipeline, or the refresh workflow.

Last refreshed: 2026-04-21 (after Phase 2 ship — Keboola Flow runs the full refresh pipeline autonomously 3x/day; Cowork scheduled task being decommissioned 2026-04-22). Before Phase 2: 2026-04-15 (after MBR v13 ship, commit 062b8c0 — TA 99%+ / TS 99.7% vs PBI on contacted).

**Quick orientation for future self:** The sections below still describe the pre-Phase-2 Cowork-driven refresh (§8a "current manual refresh" and the "design — not yet built" section §8b). As of 2026-04-21 that design IS built and live in Keboola. The authoritative current-state snapshot is in memory: `project_refresh_pipeline_state_20260421.md`. See also `project_phase2_wbrmbr_transformation.md`, `project_phase2_gsheet_extractor.md`, `project_phase2_python_component.md` for step-by-step Phase 2 build notes. This guide's §8 will be rewritten after the dual-run validation period completes and the Cowork task is disabled.

## Table of Contents

1. Current MVP Scope
2. Architecture Overview
3. Canonical File Map
4. Data Pipeline (Keboola Snowflake + Google Sheet + CSVs → dashboard_data.json)
5. dashboard_data.json Schema (the contract with App.jsx)
6. Dashboard Tabs & Per-Section Data Lineage
7. Known Gotchas (consolidated)
8. Refresh Runbook (8a current manual, 8b planned daily automation)
9. Build & Deploy (GitHub + Cloudflare)
10. Related Memory Files
11. Open Items / Known Deferred Work

---

## 1. Current MVP Scope

As of 2026-04-07 (Martin's brief, captured in `project_recruiting_mvp_scope.md`): the dashboard ships with only **three tabs** by end of April — everything else is deferred.

The three tabs in `App.jsx` (current: ~1210 lines, 3 tabs):

- **WBR (Weekly Business Review)** — `WBRTab`, lines ~61-626
- **MBR (Monthly Business Review)** — `MBRTab`, lines ~629-930
- **Project Dashboard** — `ProjectDashboardTab`, lines ~933-1077

The tabs described in older planning docs (Overview, Pipeline, Recruiter Performance, Time to Hire, Jobs) are NOT in active scope. Do not add them unless Blake explicitly asks.

Related deadlines:

- Power BI + Keboola contract is locked through **2026-08-31**. April 30 is an internal target, not contractual.
- Andy Hsu leaves end of April. April work is about capturing his institutional knowledge before it walks out the door.

---

## 2. Architecture Overview

```
┌─ Keboola Snowflake ────┐
│  project KEBOOLA_855    │
│  bucket out.c-reporting-v2
│  tables: event, candidate,
│          job, client, ...├──► Keboola MCP query_data ──┐
│  (bubble is upstream    │    (direct SQL against        │
│   of Keboola, not used  │     Snowflake; DAX-equivalent │
│   directly by us)       │     filter set)               │
└─────────────────────────┘                                │
                                                           │
┌─ Andy's WBR Google ────┐                                 ├─► dashboard_data.json
│  Sheet (1Hyb5M244b...)  │                                 │   (~375 KB, 21 top-level keys)
│  5 tabs (TA Target,     ├──► sync_google_sheet.py ─► wbr_static/
│  TA Weekly Note,        │    (n8n workflow             *.csv
│  TS Weekly Note,        │     j5QsaTUpk4Nk1xhn,
│  IR, Reasoning)         │     daily 7am UTC)
└─────────────────────────┘
                                                           │
┌─ data_inputs/ hand-patched CSVs ─► ts_conversion.csv ───┘
│  (back-solved from PBI for scoped denoms)

                                                    App.jsx (React 18 + Vite 5)
                                                    ▼
                                                    GitHub bark8922/tribe-recruiting
                                                    ▼
                                                    Cloudflare Pages (auto-build from main)
                                                    ▼
                                                    https://tribe-recruiting.pages.dev
```

**Data sourcing (verified 2026-04-15 against `dashboard_data.json.mbr_source_note`):**

- **All actuals — WBR weekly, MBR window, TS — come directly from Keboola Snowflake via the Keboola MCP `query_data` tool.** The current `dashboard_data.json` (commit 062b8c0) was produced this way end-to-end. There is no Bubble extract running in the live pipeline; there is no DuckDB transform running in the live pipeline.
- Targets, notes, and TS roster still come from Andy's Google Sheet → `wbr_static/*.csv` (daily via n8n).
- `ts_conversion.csv` is hand-patched from PBI screenshots until the scoped-denominator SQL (§11 item #5) replaces it.

**What exists but isn't running:**

- `tribe-project-snapshot/recruiting-pipeline/bubble_extract.py` + `transform.py` + `run_pipeline.py` are a **frozen snapshot from 2026-04-08**, retained as reference/backup in case we ever need to fall back off Keboola. Treat as archived. Do not run.
- `tribe-project-snapshot/keboola-sql/` — reference copies of the original Keboola SQL. Useful for checking the DAX filter set against.

**Keboola / Snowflake access — the one gotcha:** `get_buckets` and bucket-wide `get_tables` return entries with `fullyQualifiedName: null`. This does NOT mean the warehouse is unreachable. Call `get_tables` with specific `table_ids=['out.c-reporting-v2.event', ...]` — that populates the FQN (`"KEBOOLA_855"."out.c-reporting-v2"."event"`) and `query_data` works normally. See §7 + `reference_keboola_snowflake_access.md`.

**Long-term:** Blake is OK keeping Keboola as the warehouse indefinitely. The decision to replace PBI was about DAX, not Snowflake.

---

## 3. Canonical File Map

### Blake's working folder
`/sessions/bold-amazing-pascal/mnt/Recruiting Dashboard/` — Blake's working copy. This is NOT a git repo on its own. Edits here are manually synced to the GitHub repo for deploy.

Top-level files that matter:
- `App.jsx` — main dashboard source (3 tabs). ~1210 lines.
- `main.jsx`, `index.html`, `index.css`, `package.json`, `vite.config.js` — Vite plumbing.
- `dashboard_data.json` — the baked-in data file the React app imports. ~470 KB.
- `sync_google_sheet.py` — pulls Andy's WBR Sheet tabs to `wbr_static/*.csv`.
- `n8n_wbr_sheet_sync.js` / `wbr_sheet_sync_workflow.json` — n8n workflow definitions for daily sheet sync.
- `ts_queries_v4.sql` — latest reference SQL for TS (sourcer) metrics aligned to Andy's PBI DAX.
- `wbr_view.sql` — legacy Power BI WBR denorm (reference).
- `rebuild_mbr_v13.py` — current MBR TA rebuilder. Runs Snowflake MCP query against event/candidate/job/client (via Keboola), applies PBI DAX filter set, overwrites `mbr_ta_actuals`. Per-TA values exact match PBI DAX for the 40 core TAs; grand-total within ±3% on v11 roster of 44. Supersedes `rebuild_mbr_v9.py`/v10/v11/v12.
- `build_mbr.py` — assembles `mbr_ts_actuals` and other MBR keys. Line 239 = TS_CSV built from `candidate.candidate_sourcer` attribution (NOT `who_event_created_for`). Alpha sort + Latest Comment rendering is baked in.
- `rebuild_mbr_v9.py` — retained as the canonical reference for `SQL_MBR_WINDOW` DAX filter SQL. Do not run against prod; v13 supersedes.
- `POWERBI_DAX_MEASURES.md` — 105 KB dump of Andy's DAX measures for porting.
- `DASHBOARD_PLAN_v2.md`, `MVP_PLAN_April2026.md`, `REPORTING_V2_ORIGINS.md`, `FRANTISEK_QUESTIONS.md` — planning docs.

Subdirectories:

- `data_inputs/` — **working-folder only; not in the repo, not read by App.jsx at build time.** Local CSVs used by rebuild scripts to patch `dashboard_data.json`.
  - `ts_conversion.csv` — Andy's TS conversion scoped to active pipelines. 66 rows × 7 cols (ts, active_pipelines, positive_response, actual_screens, ats, contacted, recruiter_screens). The last two are nullable and for week-15 were back-solved from the PBI screenshot. The resulting array is embedded in `dashboard_data.json` as `data.ts_conversion` — that's what App.jsx reads (line 302).
  - `temp_inactive_jobs_sourcers.csv` — inactive-jobs list (excluded from TS active pipelines). A copy of this ships in the repo under `wbr_static/` (see below).
- `wbr_static/` — synced CSVs from Andy's Google Sheet. **Working folder contains 13 files; repo `wbr_static/` contains only the 4 the n8n sync writes.** Working-folder extras (bamboohr_roster_current.csv, bamboohr_supervisor_history.csv, sourcer_ta_transitions.csv, sourcing_team_list.csv, wbr_ir.csv, wbr_reasoning_guidance.csv, plus auth JSON and sync_log.txt) are dev-time reference, not consumed by App.jsx at build time. Do NOT delete the 4 n8n-synced files during rebuilds (see §7).
- `dist/` — Vite build output (git-ignored).
- `tribe-project-snapshot/` — **archived reference, not live.** Contains `keboola-sql/` (copies of original Keboola transforms — useful for cross-checking DAX), `recruiting-pipeline/bubble_extract.py` + `transform.py` + `run_pipeline.py` (frozen 2026-04-08 Bubble→DuckDB fallback — do not run), n8n workflow JSONs, and docs.

### GitHub repo
`bark8922/tribe-recruiting` (main branch) — what Cloudflare auto-builds.

```
bark8922/tribe-recruiting/
├── recruiting-dashboard/
│   ├── src/
│   │   ├── App.jsx
│   │   ├── main.jsx
│   │   ├── index.css
│   │   └── dashboard_data.json    ← baked data (copy of working folder)
│   ├── index.html
│   ├── package.json
│   └── vite.config.js             (base: './')
├── wbr_static/                    ← CRITICAL: n8n writes the daily sheet sync here.
│   ├── wbr_ta_target.csv           4 files; do NOT delete during rebuilds.
│   ├── wbr_ta_weekly_note.csv      (Restored 2026-04-14 in commit 0585e70
│   ├── wbr_ts_weekly.csv            after accidental drop.)
│   └── temp_inactive_jobs_sourcers.csv
└── README.md
```

Cloudflare builds with `cd recruiting-dashboard && npm install && npm run build`, publishing `recruiting-dashboard/dist`.

### Memory files (context for future sessions)
Listed in Section 10. Always skim `MEMORY.md` before big changes.

---

## 4. Data Pipeline

There are three independent inputs that all feed `dashboard_data.json`.

### 4a. Keboola Snowflake → dashboard_data.json (THE live pipeline — covers both WBR and MBR actuals)

All actuals (`wbr_actuals`, `ta_ats_12w`, `ta_screens_12w`, `ta_ttf_12w`, `ta_jobs_60d`, `hires_12w`, `ts_actuals`, `mbr_ta_actuals`, `mbr_ts_actuals`, `mbr_client_totals`, `weekly_trend`) are built by running DAX-equivalent SQL directly against Keboola Snowflake via the Keboola MCP. That's what produced the current `dashboard_data.json` (commit 062b8c0, 2026-04-15).

Access pattern — see §2 and §4d for the FQN gotcha.

**Filter set, metric expressions, and attribution rules** are all spelled out in §4d below. Same logic applies to both WBR (weekly breakdown) and MBR (4-week window).

**Current refresh mechanism: manual.** A session (this one, or the MBR session) runs the SQL via MCP, pastes results into a rebuild script, and the script patches the JSON in place. This is what we're about to automate (§8b → `refresh_daily.py`).

### 4b. Andy's Google Sheet → CSVs

Sheet: `1Hyb5M244bkh9ygxnssq_wppWxrT0KtGeVFEYvHVoouc` (WBR Target).

n8n workflow `j5QsaTUpk4Nk1xhn` runs `sync_google_sheet.py` daily (7am UTC). Tabs → CSVs in `wbr_static/`:

| Sheet tab | CSV | Rows | Purpose |
|-----------|-----|------|---------|
| TA Target | `wbr_ta_target.csv` | ~1,540 (monthly) | Client × TA × Year × Month targets |
| TA Weekly Note | `wbr_ta_weekly_note.csv` | ~5,908 (full history) | TA weekly reasoning + comment |
| TS Weekly Note | `wbr_ts_weekly.csv` | ~2,983 | TS weekly target + comment. Acts as the **roster filter** for the selected week. |
| IR | `wbr_ir.csv` | ~45 | Internal recruitment data |
| Reasoning Guidance | `wbr_reasoning_guidance.csv` | ~21 | Scoring guidance |

> ⚠ Use the **Sheets API**, not the gviz endpoint — gviz returns only the current week (~42 rows for TA Weekly Note) vs the full 5,908.

### 4c. data_inputs/ hand-maintained CSVs

- `ts_conversion.csv` — dropped straight into the JSON as the `ts_conversion` array. When PBI is the reference, populate the `contacted` and `recruiter_screens` columns for the active-pipeline roster by back-solving from PBI screenshots (see §7 gotcha "Scoped denominators").
- `temp_inactive_jobs_sourcers.csv` — inactive-jobs list used by the TS pipeline logic.

### 4d. Keboola Snowflake query spec (Andy's canonical logic)

This is the SQL that produces every TA/TS actuals key in `dashboard_data.json`. Bucket: `out.c-reporting-v2` in project `KEBOOLA_855`.

Access pattern (the one that actually works — see `reference_keboola_snowflake_access.md`):

1. Call `get_tables(table_ids=['out.c-reporting-v2.candidate_stage','out.c-reporting-v2.candidate','out.c-reporting-v2.job','out.c-reporting-v2.client','out.c-reporting-v2.event'])`. This populates `fullyQualifiedName` (e.g. `"KEBOOLA_855"."out.c-reporting-v2"."candidate_stage"`). Listing buckets or calling `get_tables` on the whole bucket returns `fullyQualifiedName: null` and will make you think the warehouse is unreachable — it isn't.
2. Call `query_data(sql=...)`.

**IMPORTANT — TA attribution correction (2026-04-15):** Earlier versions of this guide (§4d pre-fix) described an event-table approach using `event.who_event_created_for` + event_type CASE expressions. That approach is what rebuild_mbr_v9..v12 used and it gives wrong values for any TA whose live numbers include sourced-but-not-stage-moved candidates. Verification on 2026-04-15 against the shipping `mbr_ta_actuals` (commit 062b8c0) showed Andy's candidate_stage approach produces 42 exact matches / 5 single-unit drift / 0 big deltas across the roster. It is the correct production logic. See `reference_ta_attribution_correction.md`.

Key tables (all columns TEXT; booleans stored as `'true'`/`'false'` strings):
- `candidate_stage` — ~1.36M rows. `candidate_id, stage_current_type, date_contacted, date_screen, date_screen_actual, date_interview, date_offer, date_hired`.
- `candidate` — `candidate_id, job_id, is_candidate_archived, candidate_sourcer`.
- `job` — `job_id, client_id, job_recruiter, job_sourcer, test, date_created, is_job_archived, is_external_recruiter`.
- `client` — `client_id, client_name, is_client_archived, test`.
- `event` — ~14.6M rows. Used ONLY for Positive Response counts and actual-screen verification (`event_type='Evaluation'` with a note).

**PBI DAX filter set — apply ALL when reconstructing MBR/WBR metrics:**

```sql
WHERE c."is_candidate_archived" <> 'true'
  AND j."test" <> 'true'
  AND cl."client_name" NOT IN ('Tribe.xyz','Kamila AI - TEST')
```

(Do NOT filter `client.test`, `is_job_archived`, or `is_external_recruiter` for TA actuals — verified against PBI week 14.)

**TA section — canonical SQL (reproduces live `mbr_ta_actuals` to 42/47 exact, 5/47 within ±1–3, 0 big deltas):**

```sql
SELECT cl."client_name" AS client,
       TRIM(j."job_recruiter") AS ta,
       COUNT(DISTINCT CASE WHEN TRY_TO_DATE(cs."date_contacted")     BETWEEN :start AND :end THEN cs."candidate_id" END) AS contacted,
       COUNT(DISTINCT CASE WHEN TRY_TO_DATE(cs."date_screen_actual") BETWEEN :start AND :end THEN cs."candidate_id" END) AS actual_screens,
       COUNT(DISTINCT CASE WHEN TRY_TO_DATE(cs."date_interview")     BETWEEN :start AND :end THEN cs."candidate_id" END) AS ats,
       COUNT(DISTINCT CASE WHEN TRY_TO_DATE(cs."date_offer")         BETWEEN :start AND :end THEN cs."candidate_id" END) AS offers,
       COUNT(DISTINCT CASE WHEN TRY_TO_DATE(cs."date_hired")         BETWEEN :start AND :end THEN cs."candidate_id" END) AS hires
FROM "KEBOOLA_855"."out.c-reporting-v2"."candidate_stage" cs
JOIN "KEBOOLA_855"."out.c-reporting-v2"."candidate" c ON c."candidate_id" = cs."candidate_id"
JOIN "KEBOOLA_855"."out.c-reporting-v2"."job"       j ON j."job_id"       = c."job_id"
JOIN "KEBOOLA_855"."out.c-reporting-v2"."client"    cl ON cl."client_id"   = j."client_id"
WHERE c."is_candidate_archived" <> 'true'
  AND j."test"                   <> 'true'
  AND cl."client_name" NOT IN ('Tribe.xyz','Kamila AI - TEST')
GROUP BY cl."client_name", TRIM(j."job_recruiter");
```

Metric → candidate_stage column mapping (per Andy):
- `contacted`       → `date_contacted`
- `actual_screens`  → `date_screen_actual` (NOT `date_screen`; the latter is "moved to stage" only)
- `ats`             → `date_interview`
- `offers`          → `date_offer`
- `hires`           → `date_hired`
- `positive_response` → events table (`event_type='Moved to stage'`, `moved_to_stageType='Positive Response'`)

**TS section — different table AND different grouping:**

The TS section does NOT use `candidate_stage`. Instead, it queries the `event` table joined to `candidate → job → client`, grouped by `TRIM(c.candidate_sourcer)`, counting distinct candidates where specific `event_type` + `moved_to_stage` combinations happen in the window:

```sql
SELECT TRIM(c."candidate_sourcer") AS ts,
       COUNT(DISTINCT CASE WHEN e."event_type" IN ('Moved to stage','Candidate created')
                            AND e."moved_to_stage"='Contacted'
                            AND TRY_TO_DATE(e."date_created") BETWEEN :start AND :end
                           THEN e."candidate_id" END) AS contacted_4w,
       COUNT(DISTINCT CASE WHEN e."event_type"='Moved to stage' AND e."moved_to_stage"='Recruiter Screen'
                            AND TRY_TO_DATE(e."date_created") BETWEEN :start AND :end
                           THEN e."candidate_id" END) AS recruiter_screens_4w,
       COUNT(DISTINCT CASE WHEN e."event_type"='Evaluation'
                            AND TRY_TO_DATE(e."date_created") BETWEEN :start AND :end
                           THEN e."candidate_id" END) AS actual_screens_4w,
       COUNT(DISTINCT CASE WHEN e."event_type"='Moved to stage' AND e."moved_to_stage"='Moved to ATS'
                            AND TRY_TO_DATE(e."date_created") BETWEEN :start AND :end
                           THEN e."candidate_id" END) AS ats_4w
FROM event e JOIN candidate c ... JOIN job j ... JOIN client cl ...
WHERE [same filter set as TA]
GROUP BY TRIM(c."candidate_sourcer");
```

**Why the different approach:** TS tracks what a sourcer *did* in the window (events they generated on candidates they sourced), not what happened *to candidates they sourced* (which includes stage progressions the TA did). Validated 2026-04-15: 61/64 exact matches vs live `mbr_ts_actuals`, 3/64 within ±1 (PBI drift), 0 big deltas.

**Attribution rule (DO NOT MIX):**
- TA section → `candidate_stage` × `TRIM(j.job_recruiter)` (job-level, count candidates whose stage-date lands in window).
- TS section → `event` × `TRIM(c.candidate_sourcer)` (candidate-level, count candidates where the TS-attributed event fired in window).
- `event.who_event_created_for` is NOT used for either TA or TS actuals. It was the v9–v12 approach and is incorrect.

Canonical SQL lives in `refresh_daily.py` (the new orchestrator). The old `rebuild_mbr_v9..v13.py` scripts mixed the approaches and are superseded.

### 4e. Putting it all together

Each refresh cycle produces a new `dashboard_data.json` by running the §4d SQL against Snowflake for each WBR week and for the MBR window, then layering in the Google Sheet CSVs (`wbr_static/`) for targets/notes/roster and the hand-patched CSVs (`data_inputs/`) for TS scoped denominators. App.jsx imports `dashboard_data.json` directly at build time.

The rebuild scripts that have been used to do this live across session scratch space (e.g. `rebuild_mbr_v9.py` through `v13`, `build_mbr.py`) — they're single-purpose patchers that read Snowflake CSVs and rewrite specific keys. None of them currently live in the working folder as of 2026-04-15; that's what §8b's `refresh_daily.py` replaces (one canonical script in the repo).

---

## 5. dashboard_data.json Schema

21 top-level keys. This is the contract with App.jsx. Do not rename keys without updating both sides.

| Key | Type | Rows/Keys | Source | Purpose |
|-----|------|-----------|--------|---------|
| `targets` | array | 38 | `wbr_ta_target.csv` rollup | Baseline KPI targets per (client, TA). Fields: client, ta, contacted, actual_screens, moved_to_ats, hires, team_group |
| `wbr_actuals` | object | 37 | Snowflake query (§4d) | Keyed `"client\|ta"` → `{w5..w14: {contacted, screened, actual_screens, ats, offers, hires}}` |
| `roles` | object | 84 | Snowflake `job` count | `"client\|ta"` → role count |
| `hires_12w` | object | 46 | Snowflake `candidate.date_hired` | `"client\|ta"` → rolling 12-week hire count |
| `ta_ats_12w`, `ta_screens_12w`, `ta_ttf_12w`, `ta_jobs_60d` | object | | Snowflake query (§4d)s | Rolling 12-week ATS/screens/TTF per (client, TA); 60-day open-jobs count |
| `ta_weekly_notes` | array | 678 | `wbr_ta_weekly_note.csv` | `{client, ta, year, week, reasoning, comment}` |
| `ts_weekly` | array | 151 | `wbr_ts_weekly.csv` | `{ts, year, week, contacted_target, reasoning, comment}`. **Canonical TS roster.** |
| `ts_actuals` | object | 14 | Snowflake query (§4d) | `"ts"` → `{w4..w11: {contacted, recruiter_screens, actual_screens, ats, offers, hires}}`. Includes external recruiters (TS slice differs from TA slice). |
| `ts_jobs` | object | 13 | Snowflake `job` | `"ts"` → `{num_jobs, num_tas, ta_names}` |
| `ts_hires_12w` | object | 10 | Snowflake `candidate.date_hired` | `"ts"` → 12w hires |
| `ts_positive_responses` | object | 11 | Snowflake `event` | `"ts"` → PR count |
| `ts_conversion` | array | 66 | `data_inputs/ts_conversion.csv` verbatim | **7 cols**: ts, active_pipelines, positive_response, actual_screens, ats, contacted, recruiter_screens. Last two nullable. |
| `mbr_client_totals` | object | 23 | Snowflake query (§4d) (window = `mbr_window.weeks`) | Client rollup for the MBR window |
| `mbr_ta_actuals` | object | 87 | Snowflake query (§4d) | `"client\|ta"` → `{contacted, actual_screens, ats, offers, hires, hires_12w, ats_12w, screens_12w, jobs_60d}` for MBR window. Note: 87 `client\|ta` pairs from the v13 41-TA roster (TAs span multiple clients). |
| `mbr_ts_actuals` | object | 64 | Snowflake query (§4d) (grouped by `candidate.candidate_sourcer`) | `"ts"` → MBR window actuals |
| `mbr_ta_targets` | array | 38 | `targets` filtered to active clients | MBR TA targets |
| `mbr_window` | object | — | metadata | `{start, end, weeks}` e.g. `{start:"2026-03-16", end:"2026-04-12", weeks:["w12","w13","w14","w15"]}` |
| `mbr_active_excludes` | array | 4 | config | Currently `["ABOUT YOU","DualEntry","Fever","Wolt Business"]` |
| `mbr_active_clients` | array | 18 | config | Clients in MBR scope |
| `mbr_source_note` | string | — | build metadata | e.g. `"v7: ..."` — used to track rebuild iterations |
| `jobs` | array | 423 | Snowflake `job` filtered | `{job_id, client_name, job_title, job_recruiter, job_sourcer, date_created, is_job_archived, is_external_recruiter, test, executive_search}` |
| `weekly_trend` | array | 15 | Snowflake `event` aggregate (ISO-week grouped) | `{week, contacted, screened, ats, offers, hires}`. Covers weeks 2..16; week 16 is partial (current week). |

**Week format:** `w{N}` string keys inside objects. For `wbr_actuals` the active range right now is roughly w5..w14 per TA (TAs have sparse weeks — only weeks they produced data in are keyed). `weekly_trend` spans w2..w16 for 2026. `ts_weekly` / `ta_weekly_notes` carry explicit `year` + integer `week`. ISO weeks implicitly 2026.

---

## 6. Dashboard Tabs & Per-Section Data Lineage

### WBRTab (lines ~61-626)

Top-level state: `selectedWeek` (1..15, default 15).

**Section order, top to bottom:**

1. **Week selector** — dropdown, drives every memo below.
2. **Client Summary — Week {selectedWeek}**
   - Memo: `clientSummary` (lines ~65-118)
   - Reads: `targets`, `wbr_actuals`, `roles`, `hires_12w`
   - Columns: Client, Roles, Contacted, Screens, ATS, Offers, Hires, 12w Hires
   - Cell coloring via `getCellStyle()` (5-tier heatmap: red <50, orange 50-75, yellow 75-100, light-green 100-120, green >120).
3. **TA Weekly Detail — Week {selectedWeek}**
   - Memo: `taDetail` (lines ~121-244)
   - Reads: `targets`, `wbr_actuals`, `roles`, `hires_12w`, `ta_ats_12w`, `ta_screens_12w`, `ta_ttf_12w`, `ta_jobs_60d`, `ta_weekly_notes`
   - Grouped: "Dolphins/Whales" (Aviv + Wolt*) vs "Ponies/Unicorns" (everyone else). Group assignment in `getBuGroup()` (lines ~42-45).
   - Columns (15): 12w H, 12w ATS, 12w Scr, 12w %S→H, 12w TTF, Hires, Contacted (colored), Screens (colored), ATS (colored), %S→A, # Jobs, >60d, Comment.
4. **TS (Sourcer) Weekly — Week {selectedWeek}**
   - Memo: `tsData` (lines ~247-282)
   - Reads: `ts_weekly` filtered to `selectedWeek` (this is the roster), `ts_actuals`, `ts_jobs`, `ts_hires_12w`
   - Columns: Sourcer, 12w Hires, Contacted (colored), Target, Recruiter Screens, Actual Screens, ATS, # Jobs, # TA, TA Names, Comment.
   - **Attribution:** `ts_actuals` is sourced from `candidate.candidate_sourcer` (not `who_event_created_for`). See §7 attribution rule.
5. **TS Overall Conversion Rate with Officially Assigned Active Pipelines**
   - Memo: `tsConversion` (lines ~296-331)
   - Reads: `ts_weekly` filtered to `selectedWeek` (roster), `ts_conversion` (preferred), `ts_actuals` (fallback)
   - Columns: TS, Active Jobs, % Contacted→PR (green ≥ 20%), Positive Response, % Screen→Actual Screen (green ≥ 75%), Actual Screens, % Actual→ATS (green ≥ 55%), ATS
   - **Thresholds 20/75/55 are eyeballed from PBI — Andy should confirm real DAX values.**
   - Footer note: "Sourced from ts_queries_v4.sql; validated vs PBI week 15."

### MBRTab (lines ~629-930)

Top-level state: uses `data.mbr_window.weeks` directly (no selector).

1. **Client's Target — Last 4 Weeks** — memo `clientRows`, reads `mbr_client_totals`.
2. **Ponies & Unicorns — TAs** — memo `taRows` filtered to group, reads `mbr_ta_targets`, `mbr_ta_actuals`, `ta_weekly_notes`.
3. **Dolphins & Whales — TAs** — same memo, different filter.
4. **TS's Target — Last 4 Weeks** — memo `tsRows`, reads `ts_weekly` (weeks 12-15 aggregated target) + `mbr_ts_actuals`.

#### MBR rebuild provenance (v13 — live 2026-04-15, commit 062b8c0)

Both `mbr_ta_actuals` and `mbr_ts_actuals` are sourced **directly from Keboola Snowflake via MCP**. Scripts: `rebuild_mbr_v13.py` (TA) and `build_mbr.py` (TS — `TS_CSV` starts at line 239).

**Attribution rule (corrected 2026-04-15 — see `reference_ta_attribution_correction.md`):**
- TA section uses `TRIM(j.job_recruiter)` grouping over `candidate_stage` date columns. Job-level recruiter credit.
- TS section uses `TRIM(c.candidate_sourcer)` grouping over the same join. Candidate-level sourcing credit.
- `event.who_event_created_for` is NOT used — it's an event-level field that misattributes sourcer-dominant TAs.

**Pipeline (both scripts):**
1. Keboola MCP `get_tables(table_ids=['out.c-reporting-v2.event','out.c-reporting-v2.candidate','out.c-reporting-v2.job','out.c-reporting-v2.client'])` — populates FQNs (the critical step; bucket listing returns `fullyQualifiedName=null`).
2. Run the DAX-equivalent SQL with window `mbr_window.start` → `mbr_window.end`. Canonical SQL: `SQL_MBR_WINDOW` in `rebuild_mbr_v9.py`. Filter set and metric expressions are in §4d above.
3. Paste CSV results into `EVENT_CSV` (v13) or `TS_CSV` (build_mbr.py line 239).
4. Run the script — overwrites the relevant keys in `dashboard_data.json` in place.
5. `mbr_client_totals` is still inherited from v8 — Wolt sub-BU attribution (HQ/NBB/Tech/Market/C&S/Germany/Volume) isn't derivable from `client.client_name` alone in Keboola. v8's values are 98%+ accurate; re-derivation is deferred (§11).

**v13 accuracy vs PBI (MBR window 2026-03-16 → 2026-04-12):**

TA section — per-TA values match PBI DAX exactly for 40+ TAs (Elena 496c, Zelimir 528/29/19, Milica V 315/20/12, Akash 47/11/10/8h, Nidhi 134/13/12, Mark 18/23/8, Jelena 144/29/14, Rafael 1c). Three TAs off by 1 unit (Dolores, Nenad, Kristina) due to PBI's inconsistent Position-closed exclusion rule. v13 roster = 41 TAs (per `mbr_source_note`). The 87 `mbr_ta_actuals` keys are `"client|ta"` pairs (TAs span multiple clients).

TS section — 99.7% on contacted counts; 7/11 exact matches; worst residual ≤10 units. TS renderer in App.jsx uses alpha sort + Latest Comment (commit 062b8c0).

**Roster rule (v10 fix, still in effect):** The roster must include any TA with a target in ANY month spanned by the MBR window, not just the current month. The default window 2026-03-16 → 2026-04-12 spans months {(2026,3),(2026,4)}. v10 broadened the roster from 40 → 49; v11 trimmed zero-activity sourcers; v13 lands at 41 TAs. See `project_mbr_v9_roster_gap.md`.

**Ghost TAs kept out of v13 roster** (per commit 5a8b68f): `Fever|Andrea Akovic`, `Grover|Rodrigo Gomes`, `Grover|Eduardo Moral`, `Eucalyptus|Dusan Spica`. **Also kept out:** Ella Darie (Internal Recruiting) and Gustavo Loureiro Castro (TS). Blake confirmed these absences against PBI TA section.

**v11 sourcer-exclusion heuristic:** Dolores Palotas (Sourcer L2), Tinatini Karaulashvili, Tomer Fattal, Zarina Amanbekova, Evagelina Rapanaki were dropped from the TA roster. Kept-IN overrides: Ketevan Khorava (32h), Chantal Bozkurt (13h — Sourcer L3 per BambooHR but PBI treats as TA). Codify-before-add: if `bamboohr.job_title` contains "Sourcer", default to TS section unless explicitly overridden.

**Andrea residual (ruled-out 2026-04-15):** Andrea was −10 vs PBI. Checked against Snowflake; the delta is PBI snapshot drift (PBI re-queries nightly; our CSV was captured a day earlier). Not a bug.

**Version history (for the file inventory):**
- v8 (deprecated): pre-aggregated `ACTUALS_4W_CSV` + Wolt-HQ merge heuristic. Missed archived/test filters.
- v9 (2dd585f): first Snowflake-direct cut; per-TA exact, grand-total off by −36% on hires (roster gap). Stats vs PBI 40-target: contacted 6509/6532 (-0.4%), screens 927/919 (+0.9%), ats 590/627 (-5.9%), offers 80/122, hires 74/116.
- v10 (b8ea0e2): roster fix (months spanned). Grand total overshoot due to double-counted Fuad + sourcer pollution.
- v11 (325f636): sourcer exclusions + Fuad split across Aiven/Wolt Volume. ±3% vs PBI.
- v12: TS experiment with `who_event_created_for` — worse than v11, rejected.
- v13 TA (5a8b68f): refresh all TA actuals from live Snowflake event query; 41-TA roster; ghost TAs dropped.
- v13 TS (062b8c0): TS section uses `candidate.candidate_sourcer` + alpha sort + Latest Comment. **Current shipping state.**

### ProjectDashboardTab (lines ~933-1077)

Reads `data.jobs` (filter `is_ext === false`) and `data.weekly_trend`.

- KPI row: Open Roles, Total Contacted, Screened, Hires
- Weekly Contacted Trend (LineChart)
- Job Performance (filterable table, sortable by 8 fields)
- Hires by Client (Top 10, BarChart)
- Row coloring: red background if `screened > 25 && hires === 0` (bottleneck flag).
- "Days Open" computed vs hardcoded `2026-04-10` reference date (line ~958) — **update this when the year rolls**.

### Helper functions (top of App.jsx)

- `normalizeClient(raw)` — AVIV→Aviv; DoorDash→Wolt HQ; SevenRooms→Wolt HQ; others trim.
- `kebolaClientMatches(raw, display)` — accepts "Wolt" + Doordash + SevenRooms when display starts with "Wolt". Used for 12w/roles lookups.
- `normalizeTa(name)` — collapse multiple spaces + trim.
- `getBuGroup(client)` — Dolphins/Whales for Aviv + Wolt*; Ponies/Unicorns otherwise.
- `getCellStyle(actual, target)` — heatmap colors for actuals vs target.

---

## 7. Known Gotchas (CONSOLIDATED)

These are the things that have bitten us repeatedly. Read all of them before touching metrics.

### Andy's semantic rules (authoritative)

1. **Use the CANDIDATE table, not events**, for contacted / screens / ATS / offers / hires. Exceptions where events are required:
   - **Positive Response** (event-only; no stage timestamp).
   - **Actual Screens verification** — actual screens require an `evaluation` event **with a note**. Without a note, it doesn't count as an actual screen. Use `date_screen_actual` (candidate table) + event cross-check.
   - LinkedIn views (rarely used).
2. **Current stage = source of truth.** If a candidate has stale forward-dated timestamps from old stages, ignore them; use `stage_current_type` + cascading backfill.
3. **Cascading stage dates**: `hired → offer → interview → screen → contacted → viewed`. If hired, backfill missing earlier dates.
4. **Sourcer credit = first person to contact**, NOT the `official_sourcer` field on the job. Except for "Officially Assigned Active Pipelines" (below).
5. **External recruiters**: include in TS metrics, exclude from TA conversion rates.
6. **Manager hierarchy** comes from BambooHR historical `report_to` (as-of date, not "latest").

### Attribution rule (TA vs TS) — DO NOT MIX

- **TA actuals** (contacted/screens/ats/offers/hires per TA): group by `TRIM(job.job_recruiter)` over rows from `candidate_stage` joined to `candidate → job → client`. Candidate-level counts of candidates whose `date_X` falls in the window. Validated 2026-04-15: 42/47 exact matches vs live `mbr_ta_actuals`, 5/47 within ±1–3 (PBI snapshot drift), 0 big deltas. See `reference_ta_attribution_correction.md`.
- **TS actuals** (same metrics per sourcer): group by `TRIM(candidate.candidate_sourcer)` over the same join. Candidate-level sourcing credit. See `reference_ts_attribution.md`.
- **DO NOT USE `event.who_event_created_for`** for either TA or TS actuals. It is an event-level field that under-counts sourcer-dominant TAs and over-counts interviewers. v9–v12 used it and produced per-TA errors up to 30 units on pure-sourcer TAs (Meho, Richiteanu).

Mixing the TA and TS attribution columns produces silently-wrong results. The shipping v13 (`mbr_ta_actuals` + `mbr_ts_actuals`) uses `job_recruiter` for TA and `candidate_sourcer` for TS.

### PBI DAX filter set (apply ALL when rebuilding MBR/WBR)

1. `candidate.is_candidate_archived = 'false'` (string, not boolean)
2. `job.test = 'false'` (string)
3. `client.client_name NOT IN ('Tribe.xyz', 'Kamila AI - TEST')`
4. `TO_DATE(event.date_created) BETWEEN <window_start> AND <window_end>`

Aggregation: `COUNT(DISTINCT candidate_id)` grouped by attribution column. Metric CASE expressions are in §4d. Filter omissions historically gave +3–5% overshoots (v8 era).

### Keboola Snowflake FQN gotcha

`get_buckets` and bucket-wide `get_tables` return entries with `fullyQualifiedName: null`. This does NOT mean the warehouse is unreachable. Call `get_tables` with specific `table_ids=['out.c-reporting-v2.event', …]` — the server lazy-resolves the FQN only on a specific-table request. That populates `"KEBOOLA_855"."out.c-reporting-v2"."event"` which is directly usable in `query_data`. Cost hours before being discovered on 2026-04-14 during MBR v9 rebuild. See `reference_keboola_snowflake_access.md`.

### MBR roster rule (v10 — months spanned)

The MBR roster must include any TA with a target in ANY month spanned by the MBR window, not just the current month. Compute `months_covered(window.start, window.end)` and filter `wbr_ta_target.csv` to rows whose `(Year, Month)` is in that set; dedup on `(client, TA)` preferring the most recent month.

Strict `Year=2026 AND Month=4` drops legitimate TAs for any window that spans a month boundary — which is almost every window. Reverting to it will re-open the 4-TA hole that caused the v9 −36% hires gap.

When broadening by window-month, cross-reference against BambooHR `job_title` and the TS roster **before** adding a new name. "Sourcer" in the title → TS section, not TA section, unless Blake explicitly says otherwise (Chantal Bozkurt is the current exception).

### TS Overall Conversion Rate — the hard one

Andy's definitive rule for **Active Pipelines**: `job.job_sourcer = TS` **AND** there exists ≥1 event on a candidate of that job where `credit_sourcer = TS`. **Both conditions.**

- Funnel metrics (Positive Response → Actual Screens → ATS) use `credit_sourcer` only (permissive).
- **Denominators (`contacted`, `recruiter_screens`) are scoped to the active-pipeline job list** — not a whole-book aggregate. PBI shows scoped values; `ts_actuals` aggregate is unscoped. **This is why the % columns diverged from PBI until fixed on 2026-04-14.**
- Inactive jobs are removed via `data_inputs/temp_inactive_jobs_sourcers.csv` (synced daily from Google Sheet `1WApKTTxsXuwMgK5UBuEyTqzCgiXajLIX54RN3FHsNcA`).
- **Roster filter: `ts_weekly.filter(t => t.week === selectedWeek)`** gives the 11 sourcers active in PBI week 15. This is the canonical way to avoid ex-employees. Do NOT hardcode.
- `ts_queries_v4.sql` does NOT yet emit the scoped denominators. For week 15 they were back-solved from a PBI screenshot and stored in `data_inputs/ts_conversion.csv`. See §11 open items.

### SQL filters / joins (verified against PBI week 14, 2-3% delta)

- Join path: `candidate_stage → candidate → job → client`.
- Apply `job.test ≠ 'true'` and `candidate.is_candidate_archived ≠ 'true'`. **Do NOT filter `client.test` or `is_job_archived`.**
- Metric mappings (candidate table):
  - Contacted = `date_contacted`
  - Actual Screens = `date_screen_actual` (NOT `date_screen`; the latter is "moved to stage" only)
  - ATS = `date_interview`
  - Offers = `date_offer`
  - Hires = `date_hired`
  - Positive Response = events table

### Name normalization (MUST match PBI display)

- **Client**: AVIV → Aviv; DoorDash → Wolt HQ; SevenRooms → Wolt HQ; Nexi stays Nexi; others trim. Handled by `normalizeClient`.
- **Wolt merge**: When matching Keboola rows to a display client, accept "Wolt", "Doordash", "SevenRooms" for any Wolt* display. Handled by `kebolaClientMatches`.
- **TA names**: collapse multiple spaces and trim. Watch for diacritics (Chene, Dusan) and double-spaces (e.g. "Jelena  Lacmanovic").
- **Client grouping is mandatory**: always GROUP BY (client, TA), not TA alone.

### ts_weekly has known gaps

Mia Gjorgievska appeared in PBI week 15 but not in Andy's `ts_weekly` sheet. When a sourcer is visibly in PBI but missing from the dashboard, **ask Andy to add her to the Sheet** — don't patch the code.

### Misc

- **Snowflake boolean strings**: `is_candidate_archived`, `job.test`, `is_archived` are stored as `'true'`/`'false'` text, not real booleans. Compare as strings.
- **Date format**: `event.date_created` is ISO 8601; wrap in `TO_DATE(...)` when doing window filters.
- **Keboola Snowflake IS queryable via MCP.** The gotcha: `get_buckets` and bucket-wide `get_tables` return entries with `fullyQualifiedName: null`, which makes the warehouse look inaccessible. Call `get_tables` with specific `table_ids=['out.c-reporting-v2.event', ...]` to populate FQNs. Then `query_data` works normally. This cost hours before being discovered on 2026-04-14 during the MBR v9 rebuild.
- **Do NOT delete `wbr_static/`** during rebuilds. n8n writes the daily sheet sync there; losing it breaks the next sync. Restored on 2026-04-14 in commit 0585e70 after a rebuild dropped it.
- **Dedup talent** by `talent_id`, not `candidate_id`. A talent can appear as many candidates across jobs.
- **`MVP_PLAN_April2026.md` + `DASHBOARD_PLAN_v2.md`** still contain references to the 6-tab architecture; current scope is 3 tabs (see §1). Trust §1 over those docs.

---

## 8. Refresh Runbook

> **NOTE (2026-04-21):** The runbook below is the *pre-Phase-2* state. As of 2026-04-21 the refresh is fully Keboola-native:
>
> - Keboola Flow `01kpqyq1pz6qpmk7m9s4qx8gmg` runs 3 phases on cron `40 14,20,8` Prague: `sheet-ingest` → `transformations` → `render-and-push`. No laptop needed.
> - Validated via commit `810a67f refresh: Keboola-driven rebuild (2026-04-21 19:17 UTC)`.
> - Cowork scheduled task `tribe-recruiting-dashboard-refresh` runs in parallel for 24h as belt-and-suspenders; will be disabled 2026-04-22 PM if the Flow stays clean.
>
> Full current-state doc: **memory/`project_refresh_pipeline_state_20260421.md`**. Read that first; §8 below describes legacy manual procedures that are now obsolete but kept for reference in case the Flow has to be bypassed.

Two cadences today: **Google Sheet sync (n8n, automatic)** and **actuals refresh (session-driven, manual)**. Blake wants the second one moved to daily automation — see §8b.

**How refreshes appear in git (verified 2026-04-15):**
- Sheet sync → commits titled `chore: sync TA Target / TA Weekly Note / TS Weekly / Inactive Jobs Sourcers` touching only `wbr_static/*.csv`. Runs multiple times per day.
- Actuals refresh → MBR v-series commits (e.g. `MBR v13: refresh all TA actuals ...`) touching only `recruiting-dashboard/src/dashboard_data.json`. Manual; currently driven by a Cowork session running Snowflake queries via Keboola MCP and rewriting keys in the JSON.
- There is **no build artifact pipeline** — no Bubble extract commit, no DuckDB dump, no pre-aggregated CSV commit. The only way data changes is via one of those two commit types.

### 8a. Current manual refresh (what ships today)

Purpose: when week N+1 data becomes available (each Monday), refresh the dashboard with minimal mistakes.

### Prerequisites

- Andy's WBR Google Sheet has the new week filled in (TA Target, TA Weekly Note, TS Weekly Note).
- Keboola Snowflake has new data (Keboola's Bubble extractor runs on its own cadence — we consume, don't produce).
- The `ts_conversion.csv` either has new scoped denominators (if Andy provides them) OR you have a PBI week-N+1 screenshot to back-solve from.

### Step-by-step

**1. Sync the Google Sheet to CSVs**

```
# From /sessions/bold-amazing-pascal/mnt/Recruiting Dashboard/
python sync_google_sheet.py
```

Verify `wbr_static/wbr_ts_weekly.csv` has rows for the new week (`week == N+1`). If missing, ping Andy.

**2. Refresh actuals from Snowflake (WBR + MBR)**

No local pipeline. From a Cowork session with the Keboola MCP connected:

1. Call `get_tables(table_ids=['out.c-reporting-v2.event','out.c-reporting-v2.candidate','out.c-reporting-v2.job','out.c-reporting-v2.client'])` to populate FQNs.
2. Run the §4d SQL for the new window (both week N+1 and the rolling MBR window if the month rolled).
3. Patch the affected keys in `dashboard_data.json`. Today this is done by a session-scratch script; §8b's `refresh_daily.py` will collapse this to one command.

**3. Refresh `ts_conversion.csv` (the one manual step)**

- If Andy has provided scoped denominators (contacted + recruiter_screens per sourcer for the week): overwrite those columns in `data_inputs/ts_conversion.csv`.
- If not: pull the TS Overall Conversion Rate view from PBI for week N+1, back-solve the denominators from the % columns × numerators, and update the CSV for the active sourcers.
- Keep nullable cells `null` (empty in CSV) for sourcers who aren't in the current active roster.

The TS Overall Conversion Rate view reads `ts_conversion.csv` directly; no rebuild needed after editing it — just save.

**4. Sanity checks before committing**

Spot-check these against PBI week N+1:

- Client Summary totals (should be ~100% match).
- TA Weekly Detail for 2-3 TAs across different clients.
- TS Weekly roster matches PBI (no ex-employees, no missing people).
- TS Overall Conversion Rate: active pipelines count per sourcer matches PBI; % columns within 1-2pt of PBI.

If anything is off by >3%, stop and investigate before deploying. Re-read §7 gotchas.

**5. Commit & deploy**

```
cd /tmp
git clone https://<PAT>@github.com/bark8922/tribe-recruiting.git
cd tribe-recruiting
# Copy updated files from working folder:
#   App.jsx (if changed) → recruiting-dashboard/src/App.jsx
#   dashboard_data.json  → recruiting-dashboard/src/dashboard_data.json
cp "/sessions/.../Recruiting Dashboard/App.jsx" recruiting-dashboard/src/App.jsx
cp "/sessions/.../Recruiting Dashboard/dashboard_data.json" recruiting-dashboard/src/dashboard_data.json
git add -A
git commit -m "Refresh week N+1"
git push origin main
```

Cloudflare auto-deploys ~90s after push. Verify at https://tribe-recruiting.pages.dev.

**6. Rollback (if needed)**

```
git revert HEAD
git push
```

Cloudflare re-deploys the previous build.

### When this goes wrong

- **n8n sheet sync failed** → check `wbr_static/sync_log.txt`, re-run `sync_google_sheet.py` manually.
- **Keboola MCP returning `fullyQualifiedName: null`** → don't panic, don't fall back to Bubble. Call `get_tables` with specific `table_ids` (§2 + §7). Snowflake is fine.
- **Name mismatch** (client/TA not found) → diff normalized names between `dashboard_data.json.targets` and the PBI export.
- **% columns off despite correct numerators** → almost certainly scoped denominator problem (§7, TS Overall Conversion Rate).
- **`wbr_static/` missing from repo** → restore from prior commit (see 0585e70 on 2026-04-14); don't push without it.

### MBR refresh (when the 4-week window rolls)

1. Update `mbr_window.start` / `end` in `dashboard_data.json` (or in whichever script seeds it). Current window is 2026-03-16 → 2026-04-12.
2. Run the canonical SQL from §4d against Snowflake via Keboola MCP (two queries: one grouped by `j.job_recruiter` for TA, one grouped by `c.candidate_sourcer` for TS). Both use the `candidate_stage → candidate → job → client` join with candidate_stage date columns.
3. `python refresh_daily.py --mbr` → overwrites `mbr_ta_actuals` and `mbr_ts_actuals` in place. (The old `rebuild_mbr_v13.py` / `build_mbr.py` are deprecated — they used the incorrect event-based attribution.)
4. Apply roster rule from §7 (months spanned). Check exclusion list: Dolores Palotas, Tinatini K., Tomer F., Zarina A., Evagelina R. stay OUT. Ketevan Khorava, Chantal Bozkurt stay IN.
5. Spot-check per-TA against PBI DAX — should be exact for 40 core TAs, ±3% on grand totals.
6. Commit + push as in step 5 above.

### 8b. Planned daily automation (design — not yet built)

Target: a single cron-driven script `refresh_daily.py` that produces a candidate `dashboard_data.json`, validates it against the live one, and only pushes if safe.

**Constraints (critical — do not relax without Blake's say-so):**

1. **Schema assertion first.** Before writing anything, load current `dashboard_data.json` from the repo, assert the 21 top-level keys (§5) exist in the new payload, and that each one is non-empty. If anything is missing, abort and alert — do not push.
2. **Diff guardrail.** Per-TA change >3% or grand-total change >5% aborts auto-push and flags for human review. Regression-test the current commit 062b8c0 as the golden — a fresh run with the same EVENT_CSV must reproduce those numbers exactly.
3. **Staging mode first.** The first 3+ runs write to a branch `staging-refresh` on the repo, not `main`. Cloudflare preview deploys it; Blake eyeballs before flipping to auto-push.
4. **Per-push tags for rollback.** Every auto-push tags `refresh-YYYY-MM-DD` so `git revert refresh-…` restores the previous data payload cleanly.
5. **Never delete `wbr_static/`.** The refresher only writes `dashboard_data.json`. If it touches anything in `wbr_static/`, that's a bug.
6. **Log everything.** Write `refresh_log/YYYY-MM-DD.log` with the SQL query hashes, row counts, diff summaries, and push decision. Blake reads these, not the console output.

**Components to build:**

- `refresh_daily.py` — orchestrator. Calls Keboola MCP (or a service-account SQL runner), runs the DAX-equivalent queries, reassembles the payload, diffs, writes log, pushes.
- `ts_queries_v4.sql` upgrade — add the scoped-denominator CTE (§11) so TS Overall Conversion Rate no longer needs hand-patched `ts_conversion.csv`.
- A cron entry or n8n schedule (we already have `UzQPu4UnZwFJaoB0` sitting inactive — reuse).

**What this does not replace:** WBR weekly targets and notes still come from Andy's Google Sheet. That sync is independent (`j5QsaTUpk4Nk1xhn`, daily 7am UTC).

---

## 9. Build & Deploy

**Local stack:** React 18.3.1 + Vite 5.4.2 + Recharts 2.12.7 + Tailwind CDN.

**Local dev:**
```
cd "/sessions/.../Recruiting Dashboard"
npm install
npm run dev       # Vite dev server, hot reload
npm run build     # → dist/
```

**Production build:** Cloudflare Pages connected to `bark8922/tribe-recruiting` main branch.
- Root dir: repo root
- Build command: `cd recruiting-dashboard && npm install && npm run build`
- Output directory: `recruiting-dashboard/dist`
- Auto-deploy on push.

**GitHub deployment uses the `github` skill** for pushing changes. Username `bark8922`. PAT in session secrets.

---

## 10. Related Memory Files

When the user asks about something ambiguous, skim these first (they're all indexed in `/sessions/bold-amazing-pascal/mnt/.auto-memory/MEMORY.md`):

- `project_recruiting_dashboard.md` — last repo state + PBI accuracy check (MBR v13, commit 062b8c0).
- `project_mbr_pickup_20260415.md` — MBR v13 ship notes, per-TA exact matches, Andrea residual verdict, still-open list.
- `project_mbr_v9_roster_gap.md` — roster rule (v10 months-spanned fix) and v11 sourcer-exclusion list.
- `project_recruiting_mvp_scope.md` — Martin's scope cut (VBR + Project Dashboard only).
- `project_powerbi_contract.md` — contract through 2026-08-31; Andy leaves end of April.
- `project_recruiting_roadmap.md` — Q2 automation OKR (intake form, sourcer reports, feedback digests).
- `project_ts_conversion_deferred.md` — deferred TS Conversion items (scoped denoms CTE, drill-down, real color thresholds).
- `reference_ts_attribution.md` — TS uses `candidate_sourcer` (candidate-level).
- `reference_ta_attribution_correction.md` — TA uses `job.job_recruiter` over `candidate_stage` (NOT `who_event_created_for`). 2026-04-15 correction.
- `reference_mbr_pbi_dax_filters.md` — exact 4-filter DAX set + metric SQL expressions.
- `reference_keboola_snowflake_access.md` — FQN gotcha; bucket listing returns null, specific `get_tables` populates.
- `reference_recruiting_resources.md` — VPS, n8n IDs, Google Sheet URLs, Cloudflare, Bubble.
- `reference_recruiting_data_semantics.md` — Andy's full brain dump on event/candidate/job tables.
- `reference_wbr_google_sheet.md` — the 5 tabs, their row counts, gviz-vs-Sheets-API warning.
- `reference_verified_query_logic.md` — validated SQL patterns + 2.7% delta note vs PBI w14.
- `reference_andy_screen_logic.md` — 2026-04-13 screen/client-grouping/merge rules.
- `reference_ts_overall_conversion_rate.md` — Andy's definitive rule for Active Pipelines + AI-context warning.

---

## 11. Open Items / Known Deferred Work

Logged here so they survive session boundaries. MBR-specific items also live in `project_mbr_pickup_20260415.md`; WBR TS Conversion items in `project_ts_conversion_deferred.md`.

### MBR v13 deferred

1. **Wolt Volume dedup.** Fuad Safarov is split across Aiven and Wolt Volume from the per-(TA, client) Snowflake breakdown (v11). Any future TA that works across Wolt Volume and another client needs the same split — currently done by hand.
2. **`ta_jobs_60d` refresh.** The 60-day open-jobs count in the MBR view is still from the WBR pipeline pull. Needs to be re-derived from Snowflake in the same script as `mbr_ta_actuals` so it picks up post-snapshot changes.
3. **Codified sourcer-exclusion rule.** The v11 exclusion list (Dolores/Tinatini/Tomer/Zarina/Evagelina) and kept-in exceptions (Ketevan, Chantal) live in the script — should be a JSON config consumed by both `rebuild_mbr_v13.py` and `build_mbr.py` so they can't drift.
4. **Wolt sub-BU attribution for `mbr_client_totals`.** Still inherited from v8. Needs re-derivation from Snowflake using `client.client_wolt_group` → display-client mapping.

### WBR TS Conversion deferred

5. **Emit scoped denominators from SQL.** `ts_queries_v4.sql` should gain a CTE that joins events → active-pipeline job list (`job.job_sourcer = TS AND credited-event-on-job`) so `contacted` and `recruiter_screens` come out of the pipeline automatically instead of being back-solved from PBI each week.
6. **Drill-down per sourcer.** Blake wants to click a sourcer name in the TS Overall Conversion Rate table to see the specific active jobs that make up their pipeline. Not implemented.
7. **Real color-coding thresholds.** Current 20% / 75% / 55% thresholds on the TS Overall Conversion Rate table are eyeballed from PBI. Get actual DAX conditional-formatting thresholds from Andy before he leaves.
8. **Mia Gjorgievska missing from `ts_weekly`.** Data gap in Andy's sheet, not a code fix. Needs Andy to add her.

### Bigger pieces

9. **Daily refresh automation** — design is in §8b; not yet built. This is the next major piece of work.
10. **Reactivate n8n ETL workflow** `UzQPu4UnZwFJaoB0` (currently inactive). Likely the execution harness for #9.
11. **DAX measure extraction doc.** ~30% of PBI's accuracy lives in DAX that isn't documented anywhere. `POWERBI_DAX_MEASURES.md` is the raw dump — still needs annotation before Andy leaves.
12. **"Days Open" hardcoded reference date** in `ProjectDashboardTab` (2026-04-10). Update when appropriate, or replace with `new Date()`.
13. **Roadmap items (Q2 OKR):** intake form auto-fill, weekly sourcer-to-TA Slack report, TA-to-sourcer feedback digest. Prereq: dashboard stable.

---

**End of guide.** If you change any contract in §5 (dashboard_data.json schema) or §7 (gotchas), update this file in the same session.
