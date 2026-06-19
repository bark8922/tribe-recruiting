# Tribe Data Lineage

**Originally as of 2026-06-03. Last updated 2026-06-09 to reflect Phase 2 cost-reduction refactor.** Source of truth for "where does each number come from?" Built so we never make Keboola changes without seeing the full graph first.

> **How to read this doc.** Sections 1-5 are the live React dashboard chain (Bubble → Keboola → React). Section 6 is the parallel PowerBI chain that dies 2026-08-31. Section 7 maps cost to stage. Section 8 captures the surprises worth remembering.
>
> **Accuracy.** Every table name, row count, and refresh time was pulled live from Keboola Storage API on 2026-06-03. Code-part summaries from a parsed copy of the transformation config JSON. Dashboard tab → key mapping read directly from `App.jsx` and `refresh_staging/render_json.py`. PBI dependencies cross-referenced against `legacy-pbix/dax_index.json` + `relationships_raw.json`. Anything I'm not certain of is flagged inline with **(unverified)**.
>
> **What changed 2026-06-09 (Phase 2):** PROD V2 went from 5 code parts to 3. Part 0 (Recruitee staging) and Part 2 (Recruitee data) were removed. Their outputs now come from a pre-computed static bucket (`out.c-recruitee-static`) that gets staged as Storage Input. Section 3 reflects the new structure. Cost figures in section 7 are pre-Phase-2 baseline (60-day actuals from telemetry refreshed 2026-06-01) — post-Phase-2 actuals will be visible in the July 1 telemetry refresh.

---

## 1. Pipeline at a glance

```
┌─────────────────┐   ┌─────────────────┐   ┌─────────────────┐   ┌─────────────────┐
│  BUBBLE.IO API  │ → │   PROD V2 +     │ → │ Flow A: 7 SQL   │ → │  React dashboards│
│  42 raw tables  │   │  talent loc +   │   │  transforms     │   │  6 tabs + sourcing│
│  in 2 Keboola   │   │  Geocoding      │   │  24 output      │   │  GH→Cloudflare   │
│  buckets        │   │  17 output      │   │  tables         │   │                 │
└─────────────────┘   │  tables in      │   └─────────────────┘   └─────────────────┘
                      │  out.c-         │            │                      ↑
                      │  reporting-v2   │            ↓                      │
                      └────────┬────────┘   ┌─────────────────┐             │
                               │            │ Custom Python   │  ─ render+push ─┘
                               │            │ writers (2)     │
                               │            └─────────────────┘
                               │
                               ↓ (parallel, dies 2026-08-31)
                      ┌─────────────────┐
                      │ Data Gateway →  │
                      │ PBI semantic    │ ← Andy's 35 PBI pages
                      │ model           │
                      └─────────────────┘
```

**Flow B (cron `5 7,10,16 * * *` CET)** runs Bubble extract → talent loc → Geocoding → PROD V2 → Data Gateway → PowerBI refreshes. 3×/day.

**Flow A (cron `40 10,14,16,8 * * *` CET)** runs the WBR sheet extract → 7 Snowflake transforms → 2 Python writers. 4×/day. Reads from `out.c-reporting-v2` (Flow B's output) + `in.c-wbr-sheet`.

---

## 2. Stage 1 — Raw Bubble extracts

Two Keboola extractors pull from the Bubble.io app via the Data API. Both write to the same project but to separate buckets.

### 2a. Incremental extractor — `122527414`

Pulls the **last 7 days** of changes for high-traffic tables. Writes to **`in.c-kds-team-ex-bubble-io-122527414`** (alias: `ex-bubbleio-incremental`).

| Table | Rows | Size | What it is |
|---|---:|---:|---|
| `Events` | 14.88 M | 1.28 GB | Every event (status change, contact, screen, hire) on every candidate since 2020. Largest single table in the stack. |
| `Position` | 8.04 M | 621 MB | Every work-history position scraped from LinkedIn for every Talent. |
| `Nylas_Email_message` | 2.57 M | 97 MB | Email sync via Nylas integration. |
| `duxsoup_messages` | 2.09 M | 64 MB | LinkedIn outreach messages via Dux-Soup. |
| `Talent` | 1.62 M | 163 MB | Master talent profile (LinkedIn link, name, current company/title). |
| `Candidate` | 1.35 M | 86 MB | Candidate = (talent, job) pair. One candidate per pipeline. |
| `Analytic` | 1.09 M | 49 MB | Bubble pageview/event tally. |
| `Company` | 991 K | 90 MB | Company entities (Bubble's company database). |
| `Emails` | 105 K | 6 MB | Talent emails. |
| `stages` | 82 K | 4 MB | Stage history per candidate. |
| `recruiter_screeen_notes` | 80 K | 7 MB | Recruiter screening notes (rating, salary, visa). |

**Total: 2.47 GB**, refreshes every Flow B run.

### 2b. Full extractor — `122491135`

Pulls the **entire table** for low-traffic reference data + table aliases for the big tables. Writes to **`in.c-kds-team-ex-bubble-io-122491135`** (alias: `ex-bubbleio-all`).

This bucket holds:
- **30 reference tables** (always full-loaded): `atsOptions`, `Automationflow`, `Automationstep`, `bd_crunchbase`, `Conditional`, `EventType`, `Goals`, `HiringManager`, `JobAiFilter`, `Jobs`, `job_category`, `Job_sub_category`, `Languages`, `Languages_levels`, `Language_talent`, `ReasonNotInterested`, `recruiter_screen_notice_period_dropdown`, `recruiter_screen_relocation_dropdown`, `recruiter_screen_visa_dropdown`, `Roles`, `RoleWithinPlatform`, `SalaryType`, `Salary_currency`, `Sourced_source`, `stagesType`, `Sub_conditional`, `sub_roles`, `TechStack`, `TechStackType`, `User`. Total ~6.5 MB.
- **12 aliases** of the high-traffic tables from bucket 2a (e.g. `Event` in this bucket → points to `Events` in the Incremental bucket). The config description says verbatim: *"Loading: whole tables, stored incrementally → it is because the extractor is failing when the response is empty array."*

**Net: bucket 2b is mostly reference data; the heavy lifting lives in bucket 2a.**

### 2c. Staging side-paths

- **`in.c-stage-data.talent_locations`** (3.8 K rows): list of unique raw location strings extracted from `Talent` that still need geocoding.
- **`in.c-stage-data.talent_locations_processed`** (29 K rows / 4 MB): same locations with country/city resolved by the Geocoding extractor.

The Geocoding component (`554463170`, `keboola.ag-geocoding`) reads `talent_locations` and writes `talent_locations_processed`. PROD V2 then joins it onto Bubble Talent to enrich `location_country` / `location_city`.

### 2d. WBR sheet extract

The Keboola Google Drive extractor `01kpr3tek8ezs48pg02e60jdpe` reads Andy's WBR Target sheet (file `1Hyb5M244bkh9ygxnssq_wppWxrT0KtGeVFEYvHVoouc`) and writes 5 tables to `in.c-wbr-sheet`:

| Tab | Rows | Purpose |
|---|---:|---|
| `wbr_ta_target` | 1597 | TA monthly targets + Wolt sub-BU mapping. **Single source of truth for who appears in the WBR/MBR dashboards.** |
| `wbr_ta_weekly_note` | 6152 | Per-week (Client, TA) active roster + comments. Drives the WBR comment column. |
| `wbr_ts_weekly` | 3065 | TS roster + per-week targets. |
| `wbr_ir` | 45 | IR comments (Missed Opportunities visual). |
| `wbr_reasoning_guidance` | 21 | Static reasoning hint texts. |

---

## 3. Stage 2 — PROD V2 (config `375145203`)

The monolithic Snowflake transformation that turns raw Bubble data into the 17 reporting tables everything downstream consumes. **Runs 3×/day, ~13 min wall-clock (~800 sec).** Writes to `out.c-reporting-v2`.

Structurally it's a single `code` block with **3 ordered code parts** (down from 5 after Phase 2 refactor on 2026-06-08/09):

### Part 1 — Bubble data (~39 KB, ~88% of all PROD V2 code)
The expensive bit. Builds:
- `tmp_job` from `bubble_Jobs` (6.6K rows)
- `final_event` from `bubble_Event` with 11 LEFT JOINs + a `ROW_NUMBER() OVER (PARTITION BY talent,job,event_type,date::DATE)` window scan over the entire 14.9M-row Events table — runs the scan twice (the post-CTAS `UPDATE final_event SET who_created_event_first = ...` repeats it)
- `final_candidate_stage_bubble` from `bubble_Candidate` + `bubble_stages`, then 6 sequential `UPDATE` passes adding stage dates (date_contacted, date_screen, date_screen_actual, date_interview, date_offer, date_hired). Three of these UPDATEs reference the **statically-staged `RECRUITEE_EVENTS`** table (provided as Storage Input from `out.c-recruitee-static.recruitee_events`, uppercase destination because Part 1 references the table unquoted).
- `final_talent_bubble` from `bubble_Talent` + `bubble_Emails` + `talent_locations_processed`
- `final_email` from `bubble_Emails`
- `final_talent_position` from `bubble_Positions` (filter empty Worked_from)
- `final_talent_employer` from `bubble_bd_crunchbase` joined onto distinct employers from `final_talent_position`
- `final_screen` from `bubble_recruiter_screeen_notes` + 8 dropdown lookup tables
- `final_screen_techstack` + `final_screen_lang` (JSON-flattened from screen notes)
- `final_user` from `bubble_User` + latest "Change positions" event + Roles + sub_roles
- `final_job_goals` from `bubble_Goals` (filtered to ≥2021-07-11)

### Part 3 — Final tables (~8 KB)
Unions Bubble + Recruitee variants into the 5 final core tables. The Recruitee side comes from **Storage Input** (`out.c-recruitee-static.final_*_recruitee`) — pre-computed once on 2026-06-08 and never recomputed since Recruitee data is permanently frozen at 2023-06-29. Each UNION SELECT against a recruitee_* table uses `TRY_TO_BOOLEAN` / `TRY_TO_DATE` casts to convert Storage Input's default TEXT typing back to BOOLEAN/DATE.

- `final_talent_all` → output as `talent` (UNION of `final_talent_bubble` + staged `final_talent_recruitee`)
- `final_candidate_all` → output as `candidate`
- `final_event_all` → output as `event`
- `final_candidate_stage_all` → output as `candidate_stage` (multi-step: builds `final_candidate_stage_tmp` UNION, then enriches with `hired_order`, `hired_views`, `hired_contacts`, `hired_screens`)
- `final_client_all` → output as `client`

Also includes post-UNION updates: `final_candidate_all.is_candidate_reacted`, `final_job.date_first_hired`, `final_job.date_first_hired_contacted`.

### Part 4 — Andy tail (~430 bytes)
Two small CTAS: `analytic` (aggregated from `bubble_Analytic`) + `job_ai_filter` (passthrough of `bubble_JobAiFilter`).

### Static Recruitee bucket (`out.c-recruitee-static`)
Pre-computed once on 2026-06-08 via transformation `01ktkfs2j50hre305cv1w1kqpg` (Recruitee static rebuild). Contains 6 tables totaling ~5.8 MB: `recruitee_stage`, `recruitee_events`, `final_talent_recruitee`, `final_candidate_recruitee`, `final_candidate_stage_recruitee`, `final_event_recruitee`. The rebuild transformation is idle (never re-runs) because Recruitee source data hasn't changed since 2023-06-29 and the rebuild outputs are deterministic.

**Why the refactor:** Original Part 0 (5.5 KB Recruitee staging) and Part 2 (8.76 KB Recruitee data) recomputed the same frozen Recruitee outputs every PROD V2 run, 3 times daily. Both parts removed in Phase 2 cost-reduction work. Net ~7% runtime saved (~62 sec/run × 3 runs/day × 30 days = ~10-15 cr/mo). The pre-computed tables now flow through as Storage Inputs.

### Part 4 — Andy tail (432 bytes)
Two small CTAS: `analytic` (aggregated from `bubble_Analytic`) + `job_ai_filter` (passthrough of `bubble_JobAiFilter`).

### PROD V2 → 17 output tables in `out.c-reporting-v2`

| Table | Rows | Size | Built in part | Used by Flow A? | Used by PBI? |
|---|---:|---:|:---:|:---:|---|
| **`candidate`** | 1.38 M | 106 MB | 3 | ✓ | ✓ (heavily) |
| **`candidate_stage`** | 1.38 M | 63 MB | 3 | ✓ | ✓ (heavily) |
| **`client`** | 121 | 6 KB | 3 | ✓ | ✓ |
| **`event`** | 14.86 M | 825 MB | 3 | ✓ | ✓ (heavily) |
| **`job`** | 6,583 | 548 KB | 1 (`tmp_job`) | ✓ | ✓ (heavily) |
| `talent` | 1.65 M | 123 MB | 3 | ✗ | ✓ (3 pages: executive-search, hired-candidate-salary-audit, time-to-hire) |
| `talent_email` | 105 K | 7 MB | 1 | ✗ | ✓ (contact-freshness filter, 10 pages) |
| `talent_position` | 6.87 M | 187 MB | 1 | ✗ | ✗ (loaded nowhere) |
| `talent_employer` | 432 K | 14 MB | 1 | ✗ | ✗ (loaded by PBI but 0 rels / 0 measures / 0 page refs — dead in PBI too) |
| `screen` | 80 K | 5 MB | 1 | ✗ | ✓ (4 pages, 2 relationships) |
| `screen_techstack` | 1,852 | 44 KB | 1 | ✗ | ✗ |
| `screen_lang` | 4,509 | 119 KB | 1 | ✗ | ✗ |
| `user` | 282 | 16 KB | 1 | ✗ | ✓ **(central recruiter join hub: 5 active relationships)** |
| `analytic` | 54,590 | 445 KB | 4 | ✗ | ✓ (renamed to `analytic_usage` — 2 measures, 4 pages) |
| `job_ai_filter` | 1,300 | 77 KB | 4 | ✗ | ✗ |
| `job_goal` | 40 | 4 KB | 1 | ✗ | ✗ |

**Sidecar:** `client_cost` (97 rows, 5 KB) is also in `out.c-reporting-v2` but is produced by a different transformation — see Section 8.

---

## 4. Stage 3 — Flow A transforms (7 SQL configs, 24 output tables)

Flow A runs at 8:40, 10:40, 14:40, 16:40 CET. Reads from `out.c-reporting-v2` + `in.c-wbr-sheet`. Writes everything to dashboard-specific output buckets.

### 4a. `01kpqh9r7g2z66c8vvdr5d87xd` — Project Dashboard weekly funnel
- **Reads:** `candidate`, `candidate_stage`, `client`, `event`, `job`
- **Writes:** `out.c-Project-Dashboard---weekly-funnel.project_dashboard`
- Per-(client, job, TA, TS, source, external, ISO week) funnel: viewed → contacted → positive → screen → ATS → offer → hired.
- TA attribution: `job.job_recruiter`. TS attribution: `candidate.candidate_sourcer`.

### 4b. `01ks4qf6zate4m7f0cxng2hnyy` — Project Dashboard event-attr
- **Reads:** Same 5 tables.
- **Writes:** `out.c-Project-Dashboard---event-attr.project_dashboard_eventattr`
- Parallel funnel using **event-based attribution** (`event.who_event_created_for`). Captures cross-team activity the job-based attribution misses. Validated 73% exact / 95% within 1 unit vs PBI W16.

### 4c. `01kpqharhz3seww52sms915216` — Project Dashboard hires drill-down
- **Reads:** `candidate`, `candidate_stage`, `client`, `job` (no event).
- **Writes:** `out.c-Project-Dashboard---hires-drill-down.project_dashboard_hires`
- One row per hired candidate since 2025-01-01 with stage dates + talent metadata.

### 4d. `01kpqxgczrvb92e95y6dh7zxmh` — MBR Contacted event-attribution
- **Reads:** `candidate`, `client`, `event`, `job`.
- **Writes:** `out.c-MBR-Contacted---event-based-attribution.mbr_contacted_ev`
- Override for MBR Contacted column using event-based attribution. Fixes Aviv +103 / Eucalyptus +219 drift the job-based attribution introduced.

### 4e. `01kpr0tr0dt5ryf96a5zk85bx7` — WBR/MBR weekly aggregations (the megatransform)
- **Reads:** `candidate`, `candidate_stage`, `client`, `event`, `job` + `in.c-wbr-sheet.wbr_ts_weekly` + self-ref `out.c-WBRMBR-weekly-aggregations.ts_weekly`.
- **Writes 15 tables** to `out.c-WBRMBR-weekly-aggregations`:

| Output table | Drives which dashboard feature |
|---|---|
| `wbr_weekly` | WBR per-(client, TA, week) funnel |
| `wbr_jobs_weekly` | WBR `# Jobs` column |
| `wbr_ts_jobs_weekly` | WBR per-TS `# Jobs` + TS metadata |
| `ts_weekly` | WBR TS per-week funnel |
| `ts_conversion` | WBR TS conversion rate column |
| `aux_12w` | WBR/MBR 12-week + 60-day rollups (hires, screens, ATS, ttf, jobs_60d) |
| `ts_summary_per_sourcer` | KPI - TS Summary tab |
| `ir_funnel_jobweek` | IR Total Progress funnel |
| `ir_sourced_jobweek` | IR Sourced By table |
| `ir_interviewed_jobweek` | IR Interviewed By table |
| `ir_dq_by_stage` | IR DQ at Each Stage |
| `ir_jobs_active` | IR Active Jobs |
| `ir_dq_byjob_reason` | IR DQ Reasons (per-job) |
| `sourcing_dashboard_per_sourcer` | Sourcing Dashboard (separate repo) |
| `sourcing_wbr_comments` | Sourcing Dashboard WBR Comments tab |

This single config does ~38 cr / 60d — second-most expensive after PROD V2.

### 4f. `01kpztmw7d7911kbmyrdf7gcq5` — Time to Hire jobs
- **Reads:** `candidate`, `candidate_stage`, `client`, `job`.
- **Writes:** `out.c-TTH---tth-jobs.tth_jobs`
- Per-job time-to-first-hire / time-to-fill with per-year flags (2023-2026).

### 4g. `01ksm8rz0qfrhgzekke65bkd28` — Weekly Summary PBI Weekly Progress port
- **Reads:** 5 core tables.
- **Writes:** `out.c-Weekly-Summary---PBI-Weekly-Progress-port.weekly_summary` + `.weekly_summary_byjob`
- Standalone validation: faithful port of Andy's PBI Weekly Progress page. **Not currently wired into the React dashboards** — created for validation before wiring. 23 runs / 60d.

---

## 5. Stage 4 — Dashboards

Two separate Cloudflare Pages sites consume the Flow A outputs, both fed by Custom Python writers in Keboola.

### 5a. Recruiting Dashboard — `recruiting.tribe.xyz`

The 7 Flow A transformation outputs get downloaded by **Custom Python `01kpr863ypqr5pt74wms8fdj67`** which:
1. Pulls each output table via Storage API into a CSV.
2. Runs `render_json.py` to assemble `dashboard_data.json`.
3. PUTs the JSON to `bark8922/tribe-recruiting/dashboard_data.json` via GitHub Contents API.
4. Cloudflare Pages auto-deploys.

**Tab → data.json key → source table:**

| Tab | data.json keys (read in App.jsx) | Source table(s) |
|---|---|---|
| **WBR** (line 102) | `wbr_actuals`, `weekly_trend`, `ta_jobs_weekly`, `ts_actuals`, `ts_conversion`, `ts_positive_responses`, `targets`, `ts_weekly`, `ta_weekly_notes` | `wbr_weekly`, `wbr_jobs_weekly`, `ts_weekly`, `ts_conversion` + WBR sheet |
| **MBR** (line 1188) | `mbr_ta_actuals`, `mbr_ts_actuals`, `mbr_client_totals`, `mbr_ta_targets`, `mbr_window`, `mbr_active_excludes`, `mbr_active_clients`, plus `wbr_actuals` for per-week | `aux_12w` + `wbr_weekly` + WBR sheet |
| **Project Dashboard** (line 1714) — *default tab* | `project_dashboard.rows`, `project_dashboard_hires`, `project_dashboard_eventattr.rows` | `project_dashboard` + `project_dashboard_hires` + `project_dashboard_eventattr` |
| **Time to Hire** (line 2322) | `tth_jobs` | `tth_jobs` |
| **KPI - TS Summary** (line 2688) | `ts_summary`, `project_dashboard.rows` (fallback), `project_dashboard_hires`, `tth_jobs`, `ts_summary_pipelines` | `ts_summary_per_sourcer` (primary) + project_dashboard variants |
| **Internal Recruiting** (line 3428) | `ir_funnel_jobweek`, `ir_sourced_jobweek`, `ir_interviewed_jobweek`, `ir_dq_by_stage`, `ir_jobs_active`, `ir_dq_byjob_reason` + Ashby fallbacks (`ir_ashby_*`) | All 6 `ir_*` tables from WBR/MBR megatransform. Ashby tables still empty (Phase 2b reverted). |

Access: Cloudflare Access (Google SSO, `@tribe.xyz`). WBR/MBR hidden unless `?role=leadership` URL param.

### 5b. Sourcing Dashboard — `tribe-sourcing.pages.dev`

Custom Python **`01kt1ns5mq87k9tmgmtapf8bhm`** pulls `sourcing_dashboard_per_sourcer` + `sourcing_wbr_comments` from Storage, aggregates weekly→quarterly, PUTs `data.json` to `bark8922/tribe-sourcing`, which auto-deploys via a `deploy.yml` GH Action. Gated by 8-email allowlist + `?member=email` URL param.

### 5c. Circle (separate Worker, independent of this pipeline)

`overview.tribe.xyz` iframes `tribe-circle`, which runs on Cloudflare Workers + GH Actions and refreshes off Keboola directly every 2h. Not part of Flow A. Worth mentioning because it shares Keboola data but its own deploy path.

### 5d. Finance Dashboard

Runs on GitHub Actions, separate Bubble pull, independent of this Keboola project. See [[feedback_finance_dashboard_runs_on_github_actions]]. Not detailed here.

---

## 6. Stage 5 — PowerBI risk register (dies 2026-08-31)

Flow B's tail: `Data Gateway` (`01k9vy0j2t8mnkvz2y6fap7327`) exposes a Snowflake schema `READER_SCHEMA_855_942138244` from which PBI reads. After Data Gateway runs, two PBI refresh configs trigger:
- `121745647` PowerBI TRIBE.XYZ — wired in Flow B
- `377097411` PowerBI TRIBE.XYZ_DEV — wired in Flow B
- `1688082790` PowerBI Recruitee reports — orphan (0 runs/60d)
- `284738875` PowerBI Alpas + `471758878` PowerBI Circula — orphan but on independent schedules, ~1×/day

Andy's PBIX has **35 pages**, **200 DAX measures**. Consumes `out.c-reporting-v2` tables via M queries `v2_candidate`, `v2_event`, etc.

### What's at risk on 2026-08-31

Features that PBI delivers AND we have NOT yet rebuilt in the React stack:

| PBI feature | Tables required | Rebuild status |
|---|---|---|
| **Executive Search page** (talent profile drill-down) | `talent`, `talent_email`, `screen` | Not rebuilt. Drives Rodrigo's Data Download ask. |
| **Hired Candidate Salary Audit** | `talent`, `screen` | Not rebuilt. |
| **Sourcing Stats (LinkedIn viewed/contacted by day/hour)** | `event` (filtered to View/Contact event types) | Partially rebuilt in Sourcing Dashboard. |
| **Internal Recruiting (Recruitee data version)** | Recruitee tables (frozen since 2023) | N/A — page already dead. |
| **Bubble Usage tiles** (7-day moving avg) | `analytic` (= `analytic_usage` in PBI) | Not rebuilt. Low priority. |
| **Revenue tiles** (revenue €, revenue salary €) | `client_cost` | Not rebuilt. **And the data is stale since 2026-04-14** — see Section 8. |
| **Candidate Response Rate, Pipelines Health, OKR, Open Roles, New Role Estimate, etc.** | candidate + candidate_stage + event + job | Partially overlaps with WBR/PD/TS Summary. Worth a per-page audit before 8/31. |

Features that we **have** rebuilt and don't depend on PBI:
- WBR (TA + TS weekly) — done
- MBR (4-week TA + TS + client) — done
- Project Dashboard — done
- Time to Hire — done
- KPI - TS Summary — done
- Internal Recruitment (Bubble version) — Phase 2a done, Phase 2b (Ashby) pending
- Sourcing Dashboard Phase 1-3 — done

### Decision required before 8/31

For each "not rebuilt" feature above, decide: rebuild it, archive it, or accept its loss. Tracking that decision is out of scope for this doc.

---

## 7. Cost map

May 2026 monthly PPU = **370 credits** (highest in 6 months). After Phase 1 cron cut (5/day → 3/day on 2026-06-03), projected July 2026 monthly PPU ≈ **265 credits** (-28%).

### Per-stage cost breakdown (60-day actuals, billed credits)

| Stage | Component | Configs | 60-day cr | Notes |
|---|---|---:|---:|---|
| 1. Bubble extract | `kds-team.ex-bubble-io` | 2 | 70 | Full + Incremental |
| 1. Sheet extract | `keboola.ex-google-drive` | 1 | 3 | WBR sheet |
| 1. Telemetry | `keboola.ex-telemetry-data` | 1 | 0.3 | Monthly cron |
| 2. Talent loc SQL | `keboola.snowflake-transformation` (555826655) | 1 | 16 | Feeds Geocoding |
| 2. Geocoding | `keboola.ag-geocoding` | 1 | 13 | External API |
| 2. PROD V2 | `keboola.snowflake-transformation` (375145203) | 1 | **395** | The monolith |
| 2. Revenue 1 (orphan) | `keboola.python-transformation-v2` (456805675) | 1 | 0 | Not running |
| 2. Revenue 2 (disabled) | `keboola.snowflake-transformation` (458760018) | 1 | 4 | Disabled 2026-04-14; pre-disable jobs only |
| 3. Flow A — PD funnel | `keboola.snowflake-transformation` (01kpqh9r…) | 1 | 18 | |
| 3. Flow A — PD event-attr | (01ks4qf6…) | 1 | 3 | Shipped 2026-05-21 |
| 3. Flow A — PD hires | (01kpqharh…) | 1 | 9 | |
| 3. Flow A — MBR Contacted | (01kpqxgcz…) | 1 | 8 | |
| 3. Flow A — WBR/MBR weekly | (01kpr0tr0…) | 1 | 38 | 15 outputs in one config |
| 3. Flow A — TTH | (01kpztmw7…) | 1 | 10 | |
| 3. Flow A — Weekly Summary | (01ksm8rz…) | 1 | 2.4 | Validation only, not wired |
| 4. Custom Python writers | `kds-team.app-custom-python` | 2 | 5.5 + ~0.5 | Recruiting + Sourcing |
| 5. Data Gateway | `keboola.app-data-gateway` | 1 | 28 | **PBI-only** |
| 5. PowerBI refreshes | `kds-team.app-powerbi-refresh` | 5 | 2 | TRIBE.XYZ + DEV + 3 orphans |
| **Total (60d)** | | | **~625** | |

After Phase 1 cron cut, stages 1+2+5 drop ~60% (3x/day instead of 5x). Stage 3 stays at 4×/day so its credits don't change. Stage 4 also stays.

### Storage (4.6 GB total project)

| Bucket | Size | Status |
|---|---:|---|
| `in.c-kds-team-ex-bubble-io-122527414` (Incremental) | 2.47 GB | Active |
| `out.c-reporting-v2` | 1.33 GB | Active |
| `out.c-reporting` (v1) | **472 MB** | **Stale since 2023-06-30 — safe to delete** |
| `in.c-ex-recruitee` + variants (4 buckets) | ~190 MB | Stale since 2023 / 2025-04 — safe to delete |
| `in.c-kds-team-ex-bubble-io-122491135` | 9 MB | Active (mostly reference tables) |
| `in.c-keboola-ex-telemetry-data-*` | 45 MB | Active monthly |
| `in.c-ex-replyio`, `in.c-keboola-ex-gcalendar-*`, `in.c-keboola-ex-google-drive-454298387` | ~300 KB combined | Stale since 2021-2023 |
| Project Dashboard outputs (3 buckets) | ~1.3 MB | Active |
| `out.c-WBRMBR-weekly-aggregations` | 82 KB | Active |
| `out.c-TTH---tth-jobs` | 196 KB | Active |
| `out.c-Weekly-Summary---PBI-Weekly-Progress-port` | 513 KB | Active |
| `in.c-wbr-sheet` | 239 KB | Active |
| `in.c-stage-data` | 4 MB | Active |

---

## 8. Surprises worth remembering

### 8a. Revenue 2 was running until 2026-04-14, then went silent

Config `458760018` "Revenue - 2. data prep" (Snowflake) is currently marked `enabled=false` in Flow B. But its last 10 jobs all ran on 2026-04-13 and 2026-04-14 at 5×/day — exactly the Flow B cadence. Someone disabled it on 2026-04-14. Result:
- Its output `out.c-reporting-v2.client_cost` has been frozen since 2026-04-14.
- PBI has **3 active revenue measures** (`revenue €`, `revenue - hired count`, `revenue - salary €`) on `client_cost`.
- **Andy's PBI revenue tiles have been showing data frozen at 2026-04-14 for ~7 weeks.** Either intentional, or nobody noticed.

Action: ask Andy whether this was intentional. If not, either re-enable Revenue 2 (cheap — 4 credits/60d at 5×/day) or formally retire the PBI revenue tiles.

### 8b. `talent_employer` is dead even inside PBI

Loaded by Andy's PBI from `out.c-reporting-v2.talent_employer` but has **0 relationships, 0 measures, 0 page references** in the semantic model. It's wasting 432 K rows / 14 MB of materialization every PROD V2 run AND wasting refresh time in PBI. The only reason not to drop is Rodrigo's potential Data Download ask. Decision deferred to 8/31.

### 8c. `analytic` is renamed to `analytic_usage` in PBI

PBI's M query reads `v2_analytic` and aliases the table to `analytic_usage`. Name-only searches for `analytic` in the PBIX will miss the 2 measures (`7 days moving avg`, `7 days moving avg limited`) that use it. Documented here so we don't get caught.

### 8d. The Full Bubble extractor is mostly aliases

The "Full" bucket (122491135) holds 42 tables, but the 12 high-traffic ones are aliases pointing back to the Incremental bucket (122527414). When we trace a dependency through PROD V2's SQL on `bubble_Event`, the actual source row is in `in.c-kds-team-ex-bubble-io-122527414.Events`, not the same-named alias in 122491135. **(verified by row counts: 14,878,545 identical in both buckets)**

### 8e. PROD V2 runs the same Events scan twice

Part 1 includes both a CTAS for `final_event` (with ROW_NUMBER window over all 14.9M events) AND a post-CTAS `UPDATE final_event SET who_created_event_first = ...` that re-runs the same ROW_NUMBER. Combined with the 11 LEFT JOINs in the CTAS, this is the cost driver. If we ever want to optimize PROD V2 (not planned), this is where the gain is.

### 8f. WBR/MBR megatransform is 15 tables in one config

Config `01kpr0tr0dt5ryf96a5zk85bx7` produces 15 output tables across WBR, MBR, TS Summary, IR (6 tables), and Sourcing Dashboard (2 tables). Convenient operationally but means a single failure breaks every dashboard except Project Dashboard + Time to Hire.

---

## Appendix A — Flow schedules (Europe/Prague TZ)

| Flow | ID | Cron | Frequency | Notes |
|---|---|---|---:|---|
| Recruiting Dashboard | `01kpqyq1pz6qpmk7m9s4qx8gmg` | `40 10,14,16,8 * * *` | 4×/day | 8:40, 10:40, 14:40, 16:40 |
| 6x daily NEW (Flow B) | `118392817` | `5 7,10,16 * * *` | **3×/day (as of 2026-06-03)** | Was 5×/day until today |
| 1× monthly telemetry | `270972256` | `0 4 1 * *` | 1×/month | 1st of month, 04:00 |

## Appendix B — Storage buckets quick index

Live & needed: `in.c-kds-team-ex-bubble-io-122491135`, `in.c-kds-team-ex-bubble-io-122527414`, `in.c-stage-data`, `in.c-wbr-sheet`, `in.c-keboola-ex-telemetry-data-270972036`, `out.c-reporting-v2`, `out.c-Project-Dashboard---weekly-funnel`, `out.c-Project-Dashboard---event-attr`, `out.c-Project-Dashboard---hires-drill-down`, `out.c-MBR-Contacted---event-based-attribution`, `out.c-WBRMBR-weekly-aggregations`, `out.c-TTH---tth-jobs`, `out.c-Weekly-Summary---PBI-Weekly-Progress-port`.

Safe to delete (saves ~700 MB storage): `out.c-reporting` (v1), `in.c-ex-recruitee`, `in.c-ex-recruitee-current`, `in.c-ex-recruitee-internal`, `in.c-ex-recruitee-sourcing-backup`, `in.c-ex-replyio`, `in.c-keboola-ex-gcalendar-585671508`, `in.c-keboola-ex-google-drive-454298387`.

## Appendix C — How to update this doc

This document is regenerated by reading the current state from:
1. Keboola Storage API (buckets, tables, row counts) — `mcp__keboola__get_buckets/get_tables`
2. Keboola Component API (transformations, mappings) — `mcp__keboola__get_configs`
3. Keboola Flow API (schedules, task DAG) — `mcp__keboola__get_flows`
4. Keboola Telemetry (cost) — `KEBOOLA_855.in.c-keboola-ex-telemetry-data-270972036.kbc_job` + `.kbc_usage_metrics_values`
5. Repo source: `App.jsx` (tab → key map), `refresh_staging/render_json.py` (key → CSV map), `legacy-pbix/` (PBI model)

To refresh: open this doc in Cowork, say "rebuild data lineage from current state". Should take ~10 min and the agent has the queries.

---

*Last updated 2026-06-03 by Claude (with Blake's review on accuracy).*
