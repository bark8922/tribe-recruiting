# Recruiting Dashboard — Project Log

Single source of truth for status, decisions, and open items. Survives Cowork resets because it lives in this folder (on Blake's computer) and is backed up to GitHub. Claude updates this at the end of any session where real work happened. Blake can also say "update the log" anytime.

Last updated: 2026-08-31

---

## What this project is

Tribe.xyz recruiting/staffing analytics stack. It replaced **Power BI + the in-house data analyst** (~€96K/year saved). **Keboola/Snowflake stays — it IS the pipeline** (this is the contract Blake is renewing; only the parallel PBI chain dies 2026-08-31).

Real flow (verified live; see `DATA_LINEAGE.md`): Bubble.io API → Keboola extractors (incremental `122527414` + full `122491135`) + Geocoding → PROD V2 Snowflake SQL transform (`out.c-reporting-v2`, 17 tables) → Flow A's 7 Snowflake SQL transforms (24 output tables) → render-to-JSON + push → React/Vite dashboards → GitHub → Cloudflare Pages. Data ships as a snapshot bundle (`dashboard_data.json`, ~5.7MB), client-side React.

Orchestration = **Keboola Flows** (Flow B 3x/day `5 7,10,16` CET; Flow A 4x/day), NOT n8n. Transforms = **Snowflake SQL in Keboola**, NOT local DuckDB. (n8n at Tribe runs other things — expenses, fleet — not this pipeline.)

> Note: the `tribe-recruiting-dashboard` skill still describes the old local-Python/DuckDB-on-n8n design, which was never the production reality. That skill is a read-only cache and can't be edited from here; treat THIS log + `DATA_LINEAGE.md` as authoritative until the skill is updated in Settings.

## Durable locations (where things actually live)

- **Main dashboard code:** GitHub `bark8922/tribe-recruiting` → Cloudflare Pages
- **Sourcing dashboard code:** GitHub `bark8922/tribe-sourcing` → `tribe-sourcing.pages.dev`
- **Working folder (this one):** local on Blake's computer, also backed up to GitHub (see Backup below)
- **Architecture reference:** the `tribe-recruiting-dashboard` skill (read before changing anything)

## Live now

- Main recruiting dashboard: 6 tabs (Overview, Pipeline, Recruiter Performance, Client Delivery, Time to Hire, Jobs). Built and deployed.
- Sourcing dashboard: live, refreshes 4x/day via Keboola Flow. Phase 1 (quarterly funnel, methodology v1.5) and Phase 2 (cost of sourcing team) both shipped.
- **Keboola Data App PoC (Time to Hire):** live. First in-Keboola data app, reads `out.c-TTH---tth-jobs.tth_jobs` from Snowflake (input mapping → `/data/in/tables/tth_jobs.csv`), password-gated, auto-sleeps 15 min. Code: GitHub `bark8922/tribe-tth-app` (private), Flask + `keboola-config/` layout. App config `01kvq9zgsrkrt5yevw6djvqz0f` ("TTH Test"), URL `tth-test-985851138.hub.eu-central-1.keboola.com`. Kai chat is now LIVE end-to-end (team@tribe.xyz master token stored as an encrypted `#STORAGE_API_TOKEN` secret; Kai feature enabled on the project). Purpose: prove the Kai-powered stakeholder-app path from the 2026-06-22 Keboola renewal call before building more.

## In flight / open decisions

| Item | Status | Blocked on |
|---|---|---|
| Intake Eligibility Tracker (Google Sheet + n8n daily feed) | LIVE + verified: daily-feed PUBLISHED (Supabase node, weekdays ~06:00 CEST), 7-day lookback, scheduled runs firing clean, sheet complete through 07-24 | Nothing (done). |
| Silver Medalist role alert to Sashka (n8n `UDTat0pq3EueGdxb`) | LIVE 2026-08-26: weekdays 09:00 Prague, DMs Sashka as **Tribe Bot**, Engineering subcategory filter + title blocklist, dedupe ledger `sashka_role_alerts`, roles linked to Bubble. Test message sent to her and received. Expect silence until a new Engineering role opens. See `SILVER_MEDALIST_BACKEND_ALERT_SPEC.md` | Sashka's feedback on whether the picks are relevant |
| AI call tool usage dashboard (Bubble `call_record`) | Ingestion live, two-tab snapshot delivered 2026-07-17 | Blake: team feedback, then green-light auto-refresh (transform + build wiring) |
| Role Pipeline tab (per-role candidate deep-dive) | Scoped + all decisions locked 2026-07-15, see `ROLE_PIPELINE_SCOPE.md`, nothing built | Blake: green-light / schedule the ~1.5-day build |
| Cortex Analyst pilot | Scoped 2026-06-12, not started | Blake go/no-go: green-light free Snowflake trial, pick 2-3 pilot users, confirm aggregates-only data |
| CSV export buttons | Scoped 2026-06-11, nothing built | Blake decision: which tables in Phase 1 vs all 25 at once |
| Sourcing dashboard polish | Live, minor items | Validate post-Zelimir-fix refresh; ask Gustavo to sanity-check Phase 2 cost numbers |

## Decisions locked (do not relitigate without reason)

- Sourcing methodology v1.5: count work during Bench/Internal, drop onboarding contacts when Bench window ≤30 days, Sanja Pavlovikj excluded entirely, <5 contacts/quarter dropped as noise, half-open `[start, end)` division intervals.
- Tribe internal jobs (Tribe.xyz, IR) included; test clients excluded; archived jobs included.
- Cross-client sourcing work excluded (counts as TA work, not internal sourcing).

## Known gotchas

- App.jsx is huge (~4,500 lines). Edit via Python search/replace, run `npx vite build` before every push.
- Local folder can lag GitHub — clone the repo fresh and diff before copying anything. **This bites for real:** on 2026-08-14 the local `App.jsx` was months behind and would have reverted the TTH filters, Tech Role fix, PD heatmap and viewer-attributed views. Apply changes to the freshly-cloned repo copy, never to this folder's copy.
- Some people exist under TWO spellings in the funnel data (double-space vs single-space, e.g. `"Simon  Siew"` / `"Simon Siew"`, `"Jelena  Lacmanovic"` / `"Jelena Lacmanovic"`), which splits their numbers across two identities. Always match/group names through `normalizeTa` — never raw string equality. Fixed in the WBR team filter 2026-08-14; other aggregates not yet swept.
- The deployed dashboard is gated behind a login wall (`/api/login`), so the live `dashboard_data_snowflake.json.gz` can't be fetched unauthenticated from the sandbox (returns the sign-in HTML). Verify data via the Keboola table or a local `npm run build` instead.
- Data ships as a committed gzip at `recruiting-dashboard/public/dashboard_data_snowflake.json.gz` (~2.3MB binary), written directly by `keboola_entry.py`. Do NOT reintroduce the 52MB plaintext `src/dashboard_data_snowflake.json`. Build is just `vite build` (the old `gzip-data.mjs` step is gone).
- Keboola→GitHub push uses a PAT in the render component config (`kds-team.app-custom-python`, `01kpr863ypqr5pt74wms8fdj67`, field `#github_token`). If pushes fail with `git push rc=128 / 403`, the PAT lost write access — rotate it (re-authorize SSO if the org enforces it). This silently freezes the WHOLE deployed dashboard at the last good push.
- Cowork sandbox cannot reach `overview.tribe.xyz` (DNS fails). Doesn't matter for the pipeline: extraction + transforms run inside Keboola (cloud), reachable via the Keboola MCP, not via the sandbox or n8n.
- **Any downstream consumer of the recruiting data MUST read the gz** (`public/dashboard_data_snowflake.json.gz`, gunzip first) — never `src/dashboard_data_snowflake.json` (now gitignored / transient). Two Actions broke on 2026-06-22 because they still read the old src path (see session history). Circle builder is a SINGLE source of truth: `tribe-circle/build_circle_data.py` (+ `inject_jobs.py`), fed the gunzipped data by `refresh.yml`, triggered by Keboola `notify_circle` dispatch + 2h cron. Do NOT recreate the deleted `refresh_staging/build_circle_data.py` duplicate.

## Backup

- Working files are backed up to GitHub `bark8922/tribe-recruiting` under `workspace-backup/` (off-machine copy).
- Daily auto-push at 18:00 local via scheduled task `backup-recruiting-dashboard-folder`.
- Backup excludes node_modules, build output, large data dumps, legacy binary archives (powerbi_export, legacy-pbix), and any files containing secrets.
- Conversations themselves are NOT auto-saved. Anything important from a chat must be written into this log to survive.
- SECURITY: local folder contains plaintext credentials (Google service account key in wbr_static/, GitHub PAT + Google API keys in n8n/sheet files). These are excluded from backup and were never pushed. Consider rotating them.

---

## Session history

### 2026-08-31 — Wave 2: Interview 1/2/3 in the WBR/MBR/TS/Weekly-Summary DATA layer (backend live, frontend tabs pending)

Extended the interview columns from Project Dashboard (Wave 1) to the other weekly grids. **Transforms live and validated; the 4 React tabs still need the columns surfaced.**

Transforms edited (same gating pattern as Wave 1 — interviews are a strict subset of each table's own ATS, added as `CASE WHEN candidate IN intN_ev` inside that table's `ats` CTE, so they can never move ATS/Offer/Hired):
- `WBR/MBR weekly aggregations` `01kpr0tr0dt5ryf96a5zk85bx7` (live v60): **wbr_weekly** (feeds WBR + MBR; MBR is a frontend rolling-4-week window on the same table), **ts_weekly**, **ts_summary_per_sourcer**. Rollback v53.
- `Weekly Summary` `01ksm8rz0qfrhgzekke65bkd28` (live v11): **weekly_summary** + **weekly_summary_byjob**. Rollback v5.
- `render_json.py` (repo commit `ddb32a6`): int1/2/3 added to load_wbr, load_ts, load_ts_summary, load_weekly_summary(+byjob). Render re-run so the data.gz carries them.

Validated numerically (Keboola columns are TEXT — compare with TO_NUMBER, not string): **zero** subset-invariant violations at the headline on every table; company-level Weekly Summary totals match everything else exactly (ATS 20,589, INT1 104, INT2 26, INT3 14). ATS/Offer/Hire sums unchanged. Only edge case: 2 weekly_summary_byjob rows show Int3>Int2 — real stage-skips (a candidate reached Interview 3 without a logged Interview 2), correct under the raw-stage definition.

**Deferred within Wave 2:** ts_conversion (the TS funnel *chart*, extends past ATS — more surgery), and IR (`ir_funnel_jobweek` already has its own Onsite/Culture/Call-with-Client interview columns; Tribe internal roles are on the old stages, so Int1/2/3 would be ~0).

Frontend decision (Blake, 2026-08-31): **raw Int 1/2/3 volume columns**, no "X of Y on new pipeline" context cell / scoped conversion for now (those still need the per-job `on_new_pipeline` flag, deferred with the PD asterisk).

**SHIPPED frontend (commit `ab7871f` area):** TSSummaryTab (per-sourcer table + drilldown + the 8→11-stage funnel chart) and WeeklySummaryTab (Int 1/2/3 between Moved to ATS and Offered). Production build clean.

**REMAINING:** WBRTab + MBRTab. The data already flows — `build_wbr_actuals`/`build_ts_actuals` copy all metrics generically, so `wbr_actuals`/`ts_actuals` already carry int1/2/3 (v.int1 exists in the drill buckets). Only frontend rendering is left: add Int 1/2/3 to the 6-week drill-down funnel (header + weekly rows + 6w total) in each, plus the weekly-bucket aggregators (drillTaWeekly/drillClientWeekly init+accumulate). These are the densest, most target-heavy tables in the app, so do them carefully. The wide per-TA summary grid (with targets) can stay without Int columns unless Blake wants them (interviews have no targets). See INTERVIEW_STAGES_REPORTING_BUILD_PLAN.md.

### 2026-08-31 — SHIPPED: Interview 1/2/3 columns on the Project Dashboard funnel

Made the middle of the funnel visible. The Project Dashboard Client/Job table now shows **Int 1 / Int 2 / Int 3** between **ATS** and **Offered**, so we can see where candidates fall off between ATS and Offer (previously the report jumped ATS → Offer).

Chain shipped end to end:
- **Transform** `01kpqh9r7g2z66c8vvdr5d87xd` (weekly funnel), live **v19**. Added `int1_ev/int2_ev/int3_ev` membership CTEs and three count columns inside the existing `ats_` CTE (same ATS-gated population, same week bucket), plus `INT1/INT2/INT3` in the final SELECT between ATS and OFFERED. **Purely additive** — rollback is v16.
- **Gating lesson (important):** interviews are counted only over the candidates the **ATS column already counts** (real `candidate_stage` population: `di` not null + in `ats_ev`), then split by which Interview event they hit. This makes interviews a strict subset of ATS and excludes the phantom-burst candidates exactly the way ATS does. A naive raw-event count inflated phantom roles (e.g. Engineering Manager Belgium showed Int1 41 vs a real ATS of 3; gated it reads Int1 2).
- **Validated additive:** global ATS/Offer/Hired byte-identical before/after (ATS 20,589 = original logic recomputed on current data; Offer 4,274; Hired 3,971). Org interview funnel: ATS-pop 20,585 → Int1 103/104 → Int2 26 → Int3 14 (small because the pipeline only started mid-July).
- **Render** `render_json.py` (`load_project_dashboard`): carries `int1/2/3` into `project_dashboard.rows`. The render component `01kpr863…` input mapping had no column filter, so the new columns flowed automatically; re-ran it, published `dashboard_data_snowflake.json.gz` now contains int1/2/3 in all 29,140 rows.
- **Frontend** `App.jsx` `ProjectDashboardTab` (repo commit `fb81261`): Int 1/2/3 columns added to the Client/Job funnel table (header + client + job rows) and the `byClient`/`jobsByClient` aggregators. Production build verified clean (2300 modules). Cloudflare auto-deploys from main.
- **SQL mirror** `refresh_staging/project_dashboard.sql` synced with the interview additions (still lags Keboola on other fixes; Keboola is authoritative).

**Old roles show 0** in the three columns (correct — they never had interview events). The **asterisk** treatment for old rows was deferred: it needs a per-job `on_new_pipeline` flag (the PD `JOB_ID` space aligns with `candidate_stage_rungs.job_id`, 67/73 overlap, so the clean source is that job set — needs one extra data wire). Blake's hard requirement was "show old roles with 0s"; the asterisk was a "maybe".

**Not yet done (next waves):** WBR, MBR, TS Summary, Weekly Summary, IR still stop at ATS → Offer (Wave 2). Circle by-job table, Tribe Bot ladder, tribe-job-intel Supabase/Slack roles bot (Wave 3). TTH deferred. See `INTERVIEW_STAGES_REPORTING_BUILD_PLAN.md`.

### 2026-08-24 — SHIPPED: Contacted was bucketed on `date_contacted` (max of events), not the first contact event

- **Trigger:** Blake, WBR: Jelena Lacmanovic w34 read 260, then 181 after a refresh. Same root-cause family as the ATS fix below, one statement up in the same script.
- **Root cause.** `candidate_stage.date_contacted` is rebuilt every run in config `375145203` as `max(date_created)` of events where `moved_to_stageType='Contacted' AND moved_to_stage <> 'Responded'`. Because it is `max()`, any NEW Contacted event drags the candidate out of the week they were already counted in. On 2026-08-24 07:39–07:42 UTC Mikhail Kuzmin bulk-moved 229 of Jelena's candidates into Contacted on two No Isolation roles; all 229 re-dated to 24 Aug, draining w32/33/34 by **64/86/79** and creating a phantom **229 in w35**. Her total never moved (2,136), which is why it looked like the refresh "lost" numbers.
- **Fix.** Bucket on the candidate's **first** Contacted event, `COALESCE(first_contact_event, date_contacted)`.
- **The COALESCE is load-bearing.** Inbound applicants have NO Contacted event by definition. Their `date_contacted` comes from the cascade backfill. Modelling without the fallback silently deleted **52 DualEntry applicants from Simon Siew**, plus Dolores Palotas and Tinatini Karaulashvili. With it, all three are unaffected.
- **Only one person's yearly total moves:** Elena Petrovska −63. Those 63 are one job (Wolt Payroll Specialist), first contacted early Dec 2025, all re-moved on a single day 13 Jan 2026. Same bug; they belong in December.
- **Re-contact question, settled with data.** Of 85,809 repeat Contacted events in 2026: 84,786 same-day (system noise), 936 within 30 days (same-campaign re-staging), **87 with a gap over 30 days**. Genuine dormant re-engagement is ~0.1%, so no threshold rule was needed. An earlier 30-day-gap design was scrapped as over-engineering.
- **Applied to all three transforms at once** so WBR and Project Dashboard cannot disagree on Contacted:
  - `WBR/MBR weekly aggregations` `01kpr0tr0dt5ryf96a5zk85bx7` v50 → **v51** (job `1014894145`)
  - `Project Dashboard - weekly funnel` `01kpqh9r7g2z66c8vvdr5d87xd` v11 → **v12** (job `1014894180`)
  - `Project Dashboard - event-attr` `01ks4qf6zate4m7f0cxng2hnyy` v2 → **v3** (job `1014894223`)
- **Validation.** Predicted every figure before applying; all hit exactly. wbr_weekly 2026 CONTACTED 75,533 → **75,473**; weekly funnel 2025 138,283 → **138,349** and 2026 73,033 → **72,973**; event-attr 2025 138,201 → **138,267** and 2026 73,004 → **72,944**. REACTED moves only in the weekly funnel (−1 / +4) because it is deliberately bucketed on the same date. ATS, SCREENS, HIRED all unchanged. **Jelena w32/33/34/35 = 185 / 250 / 260 / 0**, total 2,136 unchanged — exactly her pre-bulk-move readings.
- **Rollback:** `ROLLBACK_CONTACTED_FIX_2026-08-24.md`. Restoring v50 / v11 / v2 undoes Contacted but KEEPS the ATS fix.
- **GAP CLOSED 2026-08-25.** `wbr_weekly` had its **own** `ats_` CTE still on `di`, so WBR disagreed with the Project Dashboard (Jelena wk33/34 read 2/11 vs 6/8). Fixed, plus a second defect found while checking: the Project Dashboard matched the literal stage name `'Moved to ATS'`, which **missed Aiven's renamed `'Move to ATS stage'` (20 candidates) and Tribe.xyz IR's `'Language Check'` (1)**. All three stage names share `moved_to_stageType = 'Offsite'`. WBR's looser rule had been catching them by accident, so WBR was not wrong there — Blake was right to make me check before calling it a bug. All three now match on stage TYPE. Versions: WBR **v52**, weekly funnel **v13**, event-attr **v4**. Verified: all three tables read 2026 ATS **7,127** and agree; 2025 unchanged at **14,031**; WBR Jelena wk33/34 = **6/8** matching PD No Isolation **6/8**; Jelena CONTACTED wk32/33/34 = **185/250/260**. Predicted 7,123 vs actual 7,127 — the +4 is a few hours of new activity, not drift.
- **Keboola MCP connector went read-only mid-session (2026-08-25 ~09:14).** All write endpoints returned `401 Invalid access token` (`ai.eu-central-1.keboola.com`, `queue.eu-central-1.keboola.com`) while Snowflake reads kept working; reconnecting did not fix it. Exception IDs `eab3987dfc2d8f35c99055c843779b95` and `7c027dcf2799ca01facff17d37476dd6` if it needs reporting. Blake applied the two PD edits by hand in the UI. A **second Keboola connector** was then added (token `11797764` "API Claude -token", non-expiring, unrestricted) which works for reads AND writes — that one ran the final jobs. Prefer it. Context: Keboola shipped 6 CLI releases in 5 days around this date and removed the MCP passthrough in kbagent 0.85.0, so their MCP layer is in active churn; no public incident matching this 401 was found though, so the link is circumstantial.
- **Also outstanding:** the other 8 transformations reading `date_contacted` (sourcing dashboards, TTH, weekly summary, hires drill-down, Candidate DQ by Stage, Supabase/Tribe-Bot push) still use the old field. Blake's instruction on `BD - Tribe`: drop it as a client row in WBR but keep the sourcing credited to the sourcer — separate task, not started.

### 2026-08-24 — SHIPPED: ATS was bucketed on `date_interview`, not the ATS event (Project Dashboard, both funnels)

- **Trigger:** Mikhail Kuzmin, 2026-08-21: *"current week in dashboard shows 8 Moved to ATS while in database I can see only 6 events and another 2 last week."* No Isolation / Account Manager UK (South). He was right.
- **Root cause.** `candidate_stage.date_interview` is **not a Bubble field** (Mikhail's own tip put me onto this). It is derived by us in Keboola config **`375145203 [PROD] Data preparation V2`**, script "part 1 - bubble data":
  `set date_interview = (select max(date_created) from final_event where candidate_id=... and stage_current_num >= 3 and (moved_to_stageType IN ('Offsite','Interview') or lower(moved_to_stage) LIKE '%interview%'))`.
  `Moved to ATS` is stageType `Offsite`, so the ATS move legitimately sets it. Two ways it then goes wrong: **`max()`** means a later `Interview 1` move overwrites it (count lands a week late), and the **`stage_current_num >= 3` gate reads the candidate's CURRENT stage**, so moving someone backwards after ATS erases the date entirely (count vanishes).
- **NOT new, and not caused by the Interview 1/2/3 rollout.** Same overwrite already happened in Feb 2026 under the old stage names (`Onsite`, `Culture Interview`, `Call with Client`, all stageType `Final Interview`). The rollout only raised the volume.
- **The 2025 gap was one client.** +624 of the +633 is **Circula**, 2025 ATS restating **85 → 698**. 592 moved by Rodrigo Gomes, 21 by Nenad Skoko, 27 Jan–20 May 2025, all now parked in a stage called `Sequence` (type Contacted, num 1) which sits below the gate. In that window Circula was 622/700 broken (89%) vs 3/6,025 (0.05%) for everyone else. Ruled out bulk-action (timestamps minutes apart, not seconds) and stage-config difference (identical `Offsite` type). Root cause on the Bubble side still unknown; asked Mikhail.
- **Fix.** Anchor `ats_` on the **first `Moved to ATS` event date** (`MIN(date_created)`), the way `pos_resp` and `viewed` already anchor on `event.date_created`. Bypasses both the `max()` and the gate. One CTE pair changed in each of two transforms; nothing else touched, and **config `375145203` was NOT modified**.
  - `Project Dashboard - weekly funnel` `01kpqh9r7g2z66c8vvdr5d87xd` v10 → **v11** (job `1014888499`)
  - `Project Dashboard - event-attr` `01ks4qf6zate4m7f0cxng2hnyy` v1 → **v2** (job `1014888556`)
- **Validation.** Ran the patched script end-to-end BEFORE applying and diffed every column: CONTACTED, SCREENS, ACTUAL_SCREENS, OFFERED, HIRED, VIEWED, POSITIVE_RESPONSE all **exactly zero delta** in both years; only ATS moved. Post-apply, every predicted figure hit exactly: ATS 2025 13,407 → **14,031**, ATS 2026 7,069 → **7,078**, No Isolation UK (South) w34 8 → **6** and w33 0 → **2**, Circula 2025 → **698**. Only 21 of 382 client-week cells in 2026 change at all.
- **Rollback:** `ROLLBACK_ATS_FIX_2026-08-24.md` in this folder — before/after numbers, the original SQL verbatim, and version-restore steps.
- **Next, NOT yet done:** the same `max()` flaw on `date_contacted` (Jelena w34 260→181 after a bulk move, see below). Agreed fix is first-contact-event **with a fallback to today's logic for inbound applicants**, who have no contact event and would otherwise vanish — that caught out Simon Siew (52 DualEntry applicants), Dolores Palotas and Tinatini Karaulashvili in modelling. With the fallback, only Elena Petrovska moves materially (-63), and her 63 are themselves the same bug: a bulk re-move on 13 Jan 2026 that dragged Wolt Payroll Specialist candidates out of Dec 2025.

### 2026-08-24 — WBR **# Jobs** columns were reading frozen static fields (TA Detail + TS Weekly)

- **Trigger:** Blake, looking at WBR: *"how come some people show 0 # of jobs? If they have data that means they have jobs at least one."* Correct instinct — it was a broken join, not missing jobs.
- **Root cause (one sentence):** `render_json.py` never writes `out["roles"]` or `out["ts_jobs"]`; both ride along on the `out = dict(live)` shallow copy at ~line 2295 and are **byte-identical between the 2026-06-03 snapshot and the 2026-08-24 gz**. Same class of bug as the frozen `jobs` list fixed 2026-07-17 — that comment even names them as "static fields we don't explicitly touch".
- **TA Weekly Detail.** `data.roles` is keyed `raw_client|TA` on each TA's client **at freeze time**. Any TA who changed client fails the `kebolaClientMatches` join and renders 0; clients created after the freeze (Pliant, No Isolation, Reaktor, Fever, Voize, every Wolt sub-BU, Aviv) have no entry at all. **w34: 7 of 17 rows read 0** (Ejla Suljcic, Iryna Dyda, Simon Siew, Jan Dokulil, Mateja Jokovic, Chené Elliot, Jelena Lacmanovic) **and every remaining row was also wrong — column total 46 vs 66 real.** e.g. Jan Dokulil is on Eucalyptus, `roles` had `Wolt|Jan Dokulil: 19`. The zeros were *growing over time* (3 in w9 → 9 in w34) as more TAs moved client, which is the signature of a rotting freeze.
- **TS Weekly.** `ts_jobs` was the `||` fallback behind `ts_jobs_weekly`. It holds **one all-time value per sourcer with no week dimension**, so it printed identical `num_jobs`/`num_tas`/`ta_names` into every week it covered, naming TAs long gone (Ella Darie, Nenad Skoko, Wladyslaw Gadomski). Fired in **162 (week, TS) cells** across w1-w35 and, because `num_jobs` feeds the row-visibility filter, **conjured 12 sourcer rows that should have been hidden entirely**.
- **`mbr_ta_targets` swept and CLEAN** (37 → 25 rows, genuinely rebuilt each run). `ts_positive_responses` is also identical Jun→Aug but App.jsx never reads it — dead weight, not a bug.
- **Fix (commit `f176c85`, pushed to main):** TA Detail now sums `data.ta_jobs_weekly[weekKey]`; TS Weekly drops the `ts_jobs` fallback. **Neither falls back to the frozen map** — falling back to a known-stale source just reprints the lie somewhere new. `ta_jobs_weekly` is what the Client Summary # Jobs column already uses (PBI DAX replica, validated 99.2% vs PBI w16), so **the two tables finally reconcile instead of disagreeing by construction**. Verified no TA is ever targeted on two Wolt sub-BUs in any week, so `kebolaClientMatches` can't double-credit a raw `Wolt|TA` row.
- **Validation (both layers, not just "it builds"):**
  1. Re-ran `wbr_jobs_weekly.sql` against Snowflake for w34 — returns **exactly** the gz values for all 7 affected TAs, and the underlying job titles are real and on the right client (Chené = 5 Taxfix roles, Simon = 3 Reaktor Senior Fullstack Amsterdam/Helsinki/Lisbon, Jan = 1 Eucalyptus Patient Support Specialist).
  2. jsdom harness rendering the **real `WBRTab`** via `react-dom/server` against the live gz, every week w1-w34, before vs after: TA rows **1155 → 1155** (visibility filter untouched), blank `# Jobs` cells **145 → 91**, TS rows **382 → 370** (exactly the 12 predicted phantoms, w30-w34 untouched), **0 crashes** either way. Added a named `export { WBRTab }` to enable this; tree-shaken, build hash identical (`index-fWI7H0yz.js`).
- **⚠️ SEMANTIC CHANGE worth telling recruiters before they ask:** `ta_jobs_weekly` counts roles the TA was **credited on by an event that week** (`who_event_created_for`), not roles **assigned** to them. Usually identical (Iryna 4/4, Kristina 9/9, Mateja 3/3) but diverges hard for dormant portfolios: **w34 Ejla touched 5 of 46 assigned, Simon 3 of 13**. The header still says "# Active Roles", which now means "roles you moved something on". That is what PBI always showed, but it is a different question from workload — if we want workload that's a second column off `job_recruiter`.
- **Second attribution wrinkle — "activity but 0 jobs" means SOMEONE COVERED, not a bug.** `wbr_actuals` attributes by `job_recruiter` while `ta_jobs_weekly` attributes by `who_event_created_for`. So when a TA is out and a colleague works their roles, the activity stays on the absent TA's row (the roles are still assigned to them) but the # Jobs credit follows whoever did the work. **Blake confirmed the live case 2026-08-24: w32/w33 Marina Nikolic showed 2 contacted / 2 ATS with 0 jobs because Marina was OoO and Chené Elliot covered her Taxfix roles.** Her w31 (6) and w34 (5) rows are normal. Treat this pattern as a leave/coverage signal. Note the OLD frozen column read `8` for Marina in all four weeks regardless — it could never have surfaced this. The remaining 91 blank cells across all weeks are note-only rows with genuinely zero activity, which is correct.
- **Considered and DECLINED (Blake, 2026-08-24):** a tripwire in `render_json.py` that fails the run when a field the dashboard consumes comes back byte-identical N runs running, to catch the next frozen field automatically. The `out = dict(live)` shallow copy remains a standing landmine (any field not explicitly reassigned freezes forever and rots invisibly — this is the third instance after `jobs` in July), but we are living with it and catching them by eye. **If a fourth weird-looking column shows up, check whether `render_json.py` ever writes that key before debugging anything else.**

### 2026-08-21 — Project Overview TA/TS dropdowns scoped to the selected period (Salem's complaint)

- **The complaint didn't match the data, and that mattered.** Salem flagged TAs in the Project Overview dropdown who "haven't worked here in years" and asked to drop anyone who left before he joined. Checked it: `project_dashboard` starts at 2025-W01 and **every one of the 108 TA names and 120 TS names has genuine rows in 2025 or later**. Salem's BambooHR hire date is 2021-11-10, so nobody who left before he joined can possibly be in the list. A "remove the pre-window people" filter would have removed exactly zero names. Agreed rule with Blake up front: anyone active during 2025 stays clickable, because the data goes back to 2025 and gets used for case studies.
- **Actual cause: the pickers ignored the period selector.** `uniqueTas` / `uniqueTses` were built from `pdRows` (all rows, all-time) rather than from the period-filtered set, so someone sitting on "last week" still saw 20 months of staff churn. Roughly 43 of the 108 TA names and 45 of the 120 TS names match a current BambooHR employee; the rest are 2025 leavers and external recruiters with real in-window activity.
- **SHIPPED (commit `25ac4b8` on `main`, frontend only, no pipeline run needed):** options now come from rows inside the selected period. List sizes: TA 108 all-time → 78 (2026 YTD) → 49 (13 wks) → 25 (2 wks); TS 120 → 81 → 55 → 31. Nothing is permanently removed — widen the period or set a custom 2025 range and the 2025 roster is selectable again. The **currently-selected TA/TS is always re-added** to its own list so changing the period can't silently drop an active filter. Also dropped names whose ENTIRE all-time footprint is ≤ 5 funnel events (`PD_GHOST_ACTIVITY_MAX`, 11 TA / 19 TS names — Tijana Mosic, Adrijana Cetkovic, Joe McAllister, Alvaro Pedraza, Maurice Stuart, Kanita Sahinovic Bajic, Manuel Carvalho, Andras Toth, Volodymyr Romanenko, Gino Lodola, Milica Milicevic, Dusan Lazovic…), threshold judged all-time so it doesn't move with the period. Dropdown labels now carry the count; tab subtitle explains the scoping. Verified with a real `npm run build` (2300 modules, clean).
- **⚠️ Near-miss worth remembering: the local `App.jsx` in this folder was STALE by one shipped feature.** The repo copy (5714 lines) had the WBR/MBR `NotesRefreshBtn` + `/api/wbr-notes` refresh work from ~Aug 17; the local copy (5686 lines) did not. Overwriting the repo file with the local one would have silently reverted the refresh button. Fix was to clone, apply the four edits **to the repo's copy** with an exact-match-or-abort script, then copy the result back over the local file. **Rule: always diff the repo copy against the local copy before pushing App.jsx, and patch the repo's version rather than uploading the local one.** Also note `git clone` of this repo times out without `--depth 1` (history still carries the old 52MB plaintext blobs).
- **FOUND, NOT FIXED: the ISO-year 2202 row.** Candidate `1730981204199x508857751828430960` on Wolt "Product Lead, Merchant" has `date_screen_actual = 2202-11-26` while contacted = 2024-11-07, screen = 2024-11-25, interview = 2024-11-26 — a fat-fingered year in Bubble. Every CTE in the `Project Dashboard - weekly funnel` transform (`01kpqh9r7g2z66c8vvdr5d87xd`, v10) guards with `YEAROFWEEKISO(...) >= 2025` but has **no upper bound**, so it sails through and stamps a phantom 2202-W47 week onto Nare Avetisyan. Two options, both open: (a) correct the date in Bubble — the row then falls below the 2025 floor and disappears on its own; (b) add `AND YEAROFWEEKISO(x) <= YEAROFWEEKISO(CURRENT_DATE())` alongside the nine `>= 2025` guards so the next typo can't invent a week. Not applied — production config change, awaiting Blake's go-ahead.

### 2026-08-20 — Role Pipeline Tracker: new **Coverage** tab (open roles vs actually tracked)

- **Why:** OKRs Dashboards meeting (Blake, Jacopo, Kristjana, Salem, [transcript](https://app.fireflies.ai/view/01M0FRCBTXH58E9F28H1PBCDFV)). Salem asked for total open roles vs how many are tracked on the new Interview 1/2/3 pipeline, **with the open date**, so leads don't have to walk roles line by line. Blake's steer: this belongs ON the tracker, not in a spreadsheet, with a toggle for the role types that don't count and a signal for roles with no movement in ~90 days.
- **The blind spot it fixes:** the tracker was built from `candidate_stage_rungs`, which only contains jobs that already have the Interview rungs. So by construction it could never show a role that *isn't* on the new pipeline. The "43 roles" in the meeting was 43 roles *on the pipeline*, not 43 roles open.
- **The actual numbers (2026-08-20):** **452 open client-facing roles, only 45 on the new pipeline (10%), and 264 of them (58%) have had zero candidate movement in 90+ days** (20 of those never had any activity at all). Total non-archived rows in Bubble = 491, the rest being BD 17 / Internal 11 / Test 5 / Marketing 4 / Unassigned 2. That gap is the archive backlog Jacopo and Salem were circling; the 90-day column is the evidence for which ones to kill.
- **Definition of "on the new pipeline" (reused, not reinvented):** the job's `stages` array resolves to a `stagesType` named `Interview 1/2/3` — byte-for-byte the same derivation `candidate_stage_rungs` uses, so the Coverage tab and the Roles tab can never disagree. Verified: the Roles tab still reports 43 / 15 / 35%, matching what Jacopo showed on screen.
- **Built (all additive, nothing existing modified):**
  - New Snowflake transform **`01m0ftpar7gtbqdjzpe0wwjay4`** ("Role Tracker open-role coverage") → `out.c-Role-Tracker-open-role-coverage.role_tracker_open_roles`. One row per non-archived job: role, client, owner, opened, days_open, `role_type` (Client/BD/Internal/Marketing/Test/Unassigned), `on_new_pipeline`, candidates, `last_activity`, `days_since_activity`. Movement signal = max event date per job from `out.c-reporting-v2.event`.
  - Writer `recruiting-dashboard/role_tracker_writer/keboola_entry.py` now emits `{generated_at, rows[], open_roles[]}`. The coverage CSV is **optional**: missing or unparseable → warning + still writes `rows`.
  - `public/role-tracker/index.html` gains a third tab. Defaults to client-facing only; checkbox pulls in BD/internal/marketing/test and reveals a Type column. Filters: owner, client, on/off pipeline, quiet-only, search. Badges: `Nd ago` green / `Nd quiet` red past 90 / `Never` grey.
  - Flow `118392817` phase `phase-tracker-3` gained `task-tracker-coverage` (continueOnFailure, parallel with the summary task); writer input mapping gained the second table (config v2).
- **Tested before push** with jsdom against the real 491-row dataset: all filters, both toggles, sort, and the empty state. Regression-checked the Roles and Owners tabs (48 rows / 17 owners, unchanged). Confirmed graceful degradation — with `open_roles` absent the Coverage tab hides itself and the old tabs render normally.
- **Deployed:** commit `6d8fb87` on `main`, Cloudflare serving 200.
- **⚠️ OPEN — the tab is live but EMPTY until the flow runs.** The Keboola MCP token is rejected by the job queue (`401 Invalid access token` on `queue.eu-central-1.keboola.com`) so the new transform has **never been executed**; `role_tracker_open_roles` does not exist yet. Reads and config writes work fine, so this is a job-run permission gap, not a dead token. **Fix: hit Run on flow `118392817` (or just the coverage transform) in the Keboola UI**, otherwise it self-heals on the next scheduled run (08:15 Prague). Until then the tab correctly hides rather than showing a broken view.
- **Follow-up same day (2026-08-21), commit `b58a1bc`:** the list is long, so Blake asked for a filtered row counter and a download.
  - **Row counter** on the controls row, updating on every filter: reads `267 roles of 454` when filtered, plain `454 roles` when not.
  - **Export CSV** of exactly the current filtered view, in the current sort order. Filename encodes the active filters (`open-roles-quiet90-adis-prepoljac-2026-08-21.csv`). UTF-8 BOM, because 81 of the open roles have a comma or quote in the title and plenty of owner names carry diacritics.
  - **`job_id` is in the export but deliberately NOT on the page** (Blake's call). Added to the coverage transform + `COVERAGE_STR_FIELDS` in the writer. Export degrades safely: until the transform re-runs, the Job ID column is simply empty rather than the export failing.
  - Verified with jsdom against the live 495-row dataset: counter tracks every filter, CSV row count always equals the displayed count, comma-in-title quoting confirmed on a real row (`"Key Account Manager Value Added Services (DCC, Dynamic Currency Conversion)"`), BOM confirmed at the byte level (`EF BB BF`), Roles/Owners tabs unchanged at 48/17.
  - ⚠️ **The Job ID column stays empty until the coverage transform runs again** (job-queue 401 still blocks a manual run; next scheduled run fills it). Everything else on this commit is live immediately since it is frontend-only.
- **Two bugs Blake caught on the live tab (2026-08-21), commit `87567a7` — both were mine, both worth remembering:**
  1. **Hardcoded test-client list leaked a new test client.** `TEST CLIENT 123` (created 2026-08-20 by Andreea) appeared as a client-facing role because the transform matched test clients against a literal list `('Bubble test','Test')`. Now matches **`test` as a whole word in the CLIENT name** via `regexp_like(name, '(.*[^A-Za-z])?[Tt][Ee][Ss][Tt]([^A-Za-z].*)?')`. **Snowflake gotcha: `regexp_like` is a FULL-STRING match**, so the obvious `'(^|[^a-z])test([^a-z]|$)'` silently matched only the client literally named `Test` — needed the `.*` wrappers. Validated against all 31 live client names: catches the 3 test clients, leaves Taxfix / PhantomBuster / SevenRooms alone. **Matches the client name ONLY, never the job title** — matching titles would have swallowed real roles like "Test Automation Engineer", of which we have several. The page re-applies the same rule client-side as a guard for test clients created between refreshes.
  2. **Roles with no activity were always counted as "quiet 90+ days", regardless of age.** A role opened yesterday with no candidates yet is not stale, it just hasn't started. 15 of 31 no-activity roles were opened inside 90 days and were all wrongly flagged. Staleness now falls back to **days open** when a role has never had activity: `quiet_days = coalesce(days_since_activity, days_open)`. Badge reads `new, none yet` under 90 days and `none in 680d` beyond it. Client quiet count 267 → 258; the quiet list now contains nothing opened in the last 90 days. The "never any activity" card likewise now means never-and-open-90+ (23 → 14).
  - Both fixes are computed in the frontend as well as the SQL, so they took effect immediately rather than waiting on the blocked transform run. Movement column now sorts by staleness and is the default sort; export gained a days-quiet column.
  - **Lesson for this dataset: never classify by a hardcoded list of Bubble names.** People create clients and roles freely; anything enumerated by hand will silently rot. Pattern-match, and validate the pattern against the full live name list before shipping.
- **Also raised in the same meeting, NOT yet done:** reinstate the BambooHR leaver notification so departing recruiters' roles come off the job board (ask Andrea); prune TAs who predate the 2025 data window from the Project Overview page, keeping anyone active during 2025 clickable for case studies; rename the "Owners" tab, which Blake himself called "not a great name".

### 2026-08-14 — WBR team-lead filter now includes the lead themselves (+ split-identity name bug)

**Ask:** on the WBR page, picking a team lead showed only their reports. Blake wanted the lead in the list too — pick Gustavo's team, see Gustavo and his people.

**Shipped:** commit `38f6f7a` on `bark8922/tribe-recruiting`, frontend only (`recruiting-dashboard/src/App.jsx`, +29/-6, all inside `WBRTab`). No SQL, no Keboola, no render-script change — the leads' rows already existed in the data, they were just being filtered out. `dist/` is gitignored so Cloudflare rebuilds from source.

Three changes to `selectedLeadReports`:

1. Seed the Set with the lead's own name alongside `lead.reports`. Still **non-transitive** — Kristjana gives Kristjana + Chené + Simon + Vladimir, not Chené's eight.
2. **Lejla name resolution.** `team_leads.json` stores `reports` under FUNNEL names but `name` under the BambooHR name. Those only diverge for leads in `name_overrides_applied`, currently just Lejla (funnel "Lejla Silva" vs Bamboo "Lejla Dizdarevic"). Inverted that map in the frontend so a lead resolves to both spellings. If another lead ever gets a married-name override this keeps working; the cleaner long-term fix is to have `refresh_team_leads.py` emit a `funnel_name` per lead.
3. **Whitespace normalization** on both sides of the match, via the existing `normalizeTa` helper (line ~49) rather than a new function.

**The real bug found underneath (3):** the funnel data itself contains BOTH `"Simon  Siew"` and `"Simon Siew"`, and BOTH `"Jelena  Lacmanovic"` and `"Jelena Lacmanovic"` — double-spaced and single-spaced variants as separate strings. Those two people's numbers were **split across two identities**, and the exact-string team filter only ever matched one half. So Simon was silently missing from Kristjana's team view and Jelena from Chené's. Normalizing merges them. The TA-detail dedup key already used `normalizeTa`, so no duplicate rows appear.

> **Open, not investigated:** this split-identity problem may affect any other WBR aggregate keyed on a raw name. Only the team filter was fixed. Worth a sweep.

**Verified** by simulating the new logic against `dashboard_data_snowflake.json` before pushing: Andrea, Chené, Gustavo, Kristina, Meho, Niki, Simon, Vladimir each gain their own row; Lejla gains hers via the Silva alias; Kristjana gains Simon; Chené gains Jelena. Build clean (2300 modules).

**Jacopo, Salem, Kristjana, Sanja gain nothing** — they appear nowhere in the WBR funnel under any spelling (fuzzy-matched to confirm it isn't a name problem). First three are directors per the 2026-05-28 `_team_lead_audit/AUDIT_SUMMARY.md`, which flags an open question about whether they belong in the dropdown at all. **Sanja is explained by the locked sourcing decision above: "Sanja Pavlovikj excluded entirely."** Their teams still render exactly as before; only the lead's own row is absent.

> **Gotcha confirmed the hard way:** the local `App.jsx` in this folder was WAY behind GitHub — missing the TTH multi-select filters and the same-day Tech Role fix, the Project Overview heatmap + viewer-attributed views, and the Candidate Finder city/recency filters. Pushing it would have reverted all of that. Caught by diffing a fresh clone against local before copying. The changes were re-applied to the repo's `App.jsx` instead. Local copy has since been synced to repo HEAD; the stale one is kept as `App.jsx.stale-2026-08-14`. **Always clone fresh and diff — never copy this folder's App.jsx over the repo's.**

### 2026-08-10 — New client (No Isolation) pushed live on demand — the 2-step "sheet → dashboard now" runbook

Blake added **No Isolation** (TA: Jelena Lacmanovic, Aug 2026 targets 150 contacted / 12 actual screens / 5 to ATS) to the WBR target sheet and needed it on the dashboard within 15 min. No code change needed — the structure already handles new clients dynamically (no allowlist since 2026-07-13). It's purely a **staleness** problem: the sheet syncs on a schedule, so a mid-day addition waits for the next cycle.

**Runbook to force it through (~5 min end to end):**

1. **n8n `WBR Google Sheet Sync`** (workflow `j5QsaTUpk4Nk1xhn`, active, runs 02:00 + 07:00 UTC). Execute manually. Reads Google Sheet `1Hyb5M244bkh9ygxnssq_wppWxrT0KtGeVFEYvHVoouc` (tabs incl. "TA Target") → commits `wbr_static/*.csv` to `bark8922/tribe-recruiting`. Takes ~13s.
2. **Keboola render + push** — `run_job` on `kds-team.app-custom-python` / `01kpr863ypqr5pt74wms8fdj67`. ~230s (plus queue time if a scheduled build is already running). Clones the repo fresh, so it picks up whatever step 1 just committed, then pushes the rebuilt `public/dashboard_data_snowflake.json.gz`. Cloudflare auto-deploys.

**Order matters:** the 08:46 scheduled render started ~1 min *before* the n8n sync committed, so it missed the new client. Always run the sheet sync first, confirm it succeeded, *then* fire the render.

**Verification done:** `in.c-wbr-sheet.wbr_ta_target` in Keboola has the row; the deployed gz (commit `45af2bd`, job `1011460688`) carries No Isolation in `targets`, `wbr_actuals`, `mbr_ta_targets`, `mbr_ta_actuals`, `mbr_client_totals` and `wbr_ta_weekly_roster`. Note the Hires target is blank in the sheet — if that column should be populated, it's a sheet edit, not a pipeline fix.

### 2026-08-03 — Screen attribution REVERSED to role owner (Vladimir's MBR complaint)

**Trigger:** Vladimir Stankovic told Blake his MBR numbers were wrong — contacted showed 489 when he thought ~800, and screens were "way off". Both claims investigated.

**Finding 1 — Contacted is NOT a bug. He was reading a calendar month into a rolling window.**
- The MBR window is the **last 4 complete ISO weeks** (set in `render_json.py` from Andy's TA weekly roster, dropping the in-progress week). Right now that's W28–W31 = Jul 6 → Aug 2. It is not, and never was, a calendar month.
- Vladimir's actual calendar July, straight from `event`: **752 contacted**. That's his "closer to 800".
- The entire gap is ISO week 27 (Jun 29 – Jul 5) = **296 contacted**, his biggest week of the year by ~3x, driven by Jul 1/2/3 (86/92/86 per day). The window cuts off exactly that push. Data correct, framing misleading.
- Also explained the 489 (Weekly Summary) vs 488 (MBR) split: by design. MBR overrides Contacted with the event-attributed `mbr_contacted_ev` table (`who_event_created_for`); Weekly Summary uses `wbr_weekly.CONTACTED` (job_recruiter). One W29 candidate differs. Left alone.

**Finding 2 — Screens WERE wrong. Real bug, team-wide, now fixed.**
- Weekly Summary showed Vladimir 46 rec screens for the same 4 weeks; MBR showed 41. Root cause: the two surfaces credit screens to different people.
  - `weekly_summary` (May, PBI port) → `who_event_created_for` = the role owner.
  - `wbr_weekly` (June, the doer spec) → `who_created_event` = whoever clicked.
- In W28, **Rodrigo Gomes logged 5 Evaluations for Vladimir's candidates** (all Aiven, all `job_recruiter` = Vladimir). Those 5 screens moved off Vladimir onto Rodrigo. 10 − 5 = the 41 he saw. His other 3 weeks matched exactly, so it was entirely this.
- Not Vladimir-specific. Across W26–31 the doer rule reassigned ~130 screens (Jelena Lacmanovic → Kristina Colovic 23 in W28 alone, Gustavo ↔ Filip both directions, Mariam → Ella, etc.).
- **Blake's key insight:** `who_created_event` conflates two different things — Jelena *actually ran* screens covering for Kristina and Iryna on holiday, whereas Rodrigo *just typed in* what Vladimir ran. The data cannot tell them apart: both write an identical row. Verified there is no other signal — `screen.user_recruiter` is `-not available-` on **100%** of screens since June, there is no `call_record` table in reporting-v2, and `user.role_current` in Bubble is stale (it lists Kristina Colovic as "Sourcer / Sr. Sourcer"; BambooHR says **Team Lead, Leadership, Aviv**). **Do not build anything on `user.role_current` — use BambooHR.**

**DECISION (Jacopo, 2026-08-03): the ROLE OWNER always gets the recruiter screen.** Irrespective of holiday cover, coordinator data entry, or who physically ran it. This **voids `SPEC_screen_attribution_by_doer.md` (2026-06-19)**, whose whole premise was the opposite ("credit whoever ran it"). That spec should be treated as dead.

**Also noted:** the June spec's Section 7 promised "MBR tables: stays exactly the same". It didn't — MBR is built from `wbr_weekly` via `mbr_ta_actuals`, so the doer change leaked straight into MBR. Spec/impact-analysis miss, worth remembering next time.

**IMPLEMENTED.** Keboola `keboola.snowflake-transformation` config `01kpr0tr0dt5ryf96a5zk85bx7` ("WBR/MBR weekly aggregations"), block `b0`, code `b0.c0` (Build wbr_weekly). The `eval_doer` CTE now reads `who_event_created_for` instead of `who_created_event` (two spots: the `doer_ta` SELECT and the `IS NOT NULL` filter). Comment header rewritten to describe the new rule. **Config v47 → v49.** Job `1009783744` ran clean (241s, all 27 tables, `wbr_weekly` 1187 rows).
- Chose `who_event_created_for` over a plain revert to `job.job_recruiter` deliberately. `job_recruiter` is a *current snapshot*, so a req changing hands retroactively drags months-old screens to the new owner. Identical output for W28–31, but across full-year 2026 they diverge on reassigned reqs (Ella Darie 540 vs 481 — she'd inherit 59 screens Anna/Iryna/Wladyslaw/Kristina did in April on the Tribe.xyz IR reqs before she took them over). It's also the same field `weekly_summary` uses, so WBR/MBR and Weekly Summary now agree.

**Verified live in `wbr_weekly` after the run (W28–31 SCREENED, before → after):** Vladimir Stankovic 41 → **46** (matches Weekly Summary exactly), Iryna Dyda 57 → 72, Ella Darie 42 → 53, Kristina Colovic 48 → 58, Wladyslaw Gadomski 21 → 26, Mia Gjorgievska 0 → 1, Filip Nogowski 57 → 56, Rodrigo Gomes 6 → 0, Mariam Chkhikvadze 11 → 0, Jelena Lacmanovic 29 → 0. Totals conserved; CONTACTED untouched (Vladimir still 489).

**Open follow-ups from this session:**
- **Tell Jelena, Mariam and Rodrigo** their screen counts drop to zero. Intended under Jacopo's rule, but they shouldn't discover it in the WBR.
- **The dashboard won't show this until the next render+push** (Flow build ~16:10 Prague). Only the Snowflake table was rebuilt here.
- **Consider relabelling the MBR window** with its date range on the tab, so the calendar-month misread doesn't recur.
- **A req literally named "Tribe.xyz / Test Job" has 73 Evaluations in 2026 and is NOT being filtered out** (its `test` flag isn't `true`). Polluting totals. Separate cleanup.
- Minor, verified immaterial: `wbr_weekly` / `mbr_contacted_ev` filter jobs with `LOWER(NULLIF(j."test",'')) <> 'true'`, which in Snowflake silently drops the 1,761 jobs whose `test` is blank. Real, but only 210 candidates of 70,033 in 2026 (0.3%). `weekly_summary` uses the correct `(... OR test IS NULL)` form.

### 2026-07-24 — Intake Eligibility Tracker BUILT (Google Sheet + n8n daily feed)
- **Context / why:** supports the Q3 OKR "80% of eligible roles have their intake form completed via the autofill tool in the final 6 consecutive weeks of Q3" (Q3 ends 2026-09-30). Before chasing a TA to run an intake call, we want evidence the role actually deserves one. This tracker is that evidence layer. Kept entirely separate from the live new-role briefing bot (Supabase) so it can't disturb it.
- **THE ELIGIBILITY RULE (locked with Blake, recruiter-level):** eligibility keys off whether the assigned TA has personally run that same position at that client before. `This TA ran this position here` count ≥1 → likely NO intake (they know the role). 0 for this TA but ≥1 for any colleague → INTAKE (or a handover). 0/0 → brand-new position → INTAKE. Archived roles flagged "killed". This is a hint/signal column, not an auto-decision — a human still confirms.
- **The position matcher (distinctive-token, retuned with Blake):** normalize the title (strip location, seniority, recruiter-name tags, and segment/level qualifiers), split tokens into *distinctive* vs *role-type*. A prior role matches if it contains ALL of the new role's distinctive tokens AND shares a role-type token. Retune Blake signed off on: strip SEG words (`brands brand longtail long tail l1 l2 l3 top key regions region smb volume internal groceries retail ondemand qcommerce media specialties menu`) and normalize `tech`→`technical`, but KEEP role-type distinctions so e.g. a Senior UX Manager stays distinct from a Product Designer (Blake explicitly wanted these treated as different → eligible). Validated cases Blake confirmed: Glovo "Global Sales Executive" = not brand-new, no intake; Tech Support = not brand-new; Globo Senior UX Manager = distinct from Product Designer → new/intake. Matcher source: `/home/claude/matcher.py` (Python reference, validated against real priors), ported verbatim into the n8n Code node as JS.
- **The Google Sheet (LIVE, this is the artifact everyone sees):** id `15baav5HBvBh9oEdgEv7z7NwlonCukIJSRlWbcSG4Itw`. Two tabs. **"Roles Opened"** = one row per new role with 12 columns: ISO Week, Date Opened, Client, Role, Assigned TA, Assigned Sourcer, Status, `This TA ran this\nposition here`, `Same position,\nany recruiter`, Matched prior roles (sample), Signal (hint), `Intake already\non file?`. **GOTCHA: three header cells contain literal newlines** ("This TA ran this\nposition here", "Same position,\nany recruiter", "Intake already\non file?") — any Sheets write MUST map those exact newline strings or the append rejects the columns. **"Summary"** tab = auto-expanding aggregates (TOTAL/OKR ranges to row 200; per-TA formula in B7, by-week in B30 — dynamic LET/MAP/FILTER/COUNTIFS Blake pasted). Built from `Intake_Eligibility_Tracker.xlsx` (`/home/claude/`, via build_tracker.py); Blake did the xlsx→Google-Sheet one-click conversion himself. Backfilled with the 14 real new jobs 2026-07-17→07-24 (5 client roles after dedup).
- **n8n workflows (Blake's personal project):**
  - **"Intake Eligibility - Sheet Append"** (id `ejsqySr6aU1uitF2`, webhook `/intake-eligibility-append`): Webhook → Explode Rows (Code) → Append Rows (Google Sheets append, useAppend true) → Build Response. LIVE. Used for one-off/manual row appends. (Was briefly flipped to update-by-role mode to fix 3 rows, then reverted to append.)
  - **"Intake Eligibility - Daily Feed"** (id `h6zd1GWVqz6caAKh`): **PUBLISHED + LIVE.** Schedule Trigger (cron `0 0 6 * * 1-5` = 08:00 CET weekdays) → Get Jobs → Get People → Get Intake (all three = n8n **Supabase** "Get Many rows" nodes; returnAll OFF + high limit + executeOnce TRUE) → Read Sheet (Google Sheets read, executeOnce) → Match and Dedup (Code node: builds people/intake maps, derives new roles [last 3 days, has ta_id, not archived, excludes test/feedback/BD-/tribe] + full history, runs matcher, dedups on Client|Role|ISO Week) → Append New Rows (Google Sheets append, newline headers mapped). Reads-only against the DB; joins done in the Code node, no schema changes. Verified end-to-end 2026-07-24 (run 42220 success in 3s; 0 rows appended because all recent roles were already in the sheet from the backfill = correct).
  - **Why NOT the Postgres node:** the n8n "Postgres account" credential points at 157.230.31.88 (a different DB), not tribe-job-intel → "connection refused". Switched to the Supabase node with a new "Supabase account" credential (id `dNdTbItf3GRYrfvX`, project URL `https://garopkilxgpcmlkiqvbg.supabase.co` + service_role key). Build gotchas hit + fixed (all in memory `reference-intake-eligibility-tracker.md`): Supabase node **returnAll:true HANGS** (use returnAll:false + high limit, one request); `jobs` has **no `id` column** (PK bubble_job_id); a read node after a many-item node MUST be **executeOnce** or it fires one call per row (Get People crawled 1944x before this fix).
- **STATUS: DONE — published and live.** Runs weekdays ~06:00 CEST (cron `0 0 6 * * 1-5` in the workflow tz; earlier notes said 08:00, actual is ~6am), appends only genuinely-new, non-duplicate roles. Dedup on Client|Role|Week makes re-appends impossible. Full scope/decision record: `INTAKE_ELIGIBILITY_SCOPE.md` (this folder), section 3 has the locked model + validated matcher.
- **2026-07-28 tuning:** (1) Lookback window widened 3 -> 7 days (Match and Dedup Code node) because the jobs mirror can surface a role a few days after it opens in Bubble and the feed filters on Bubble date_created, so a tight window let late arrivals slip past; 7 days + dedup is the margin. (2) Confirmed the jobs mirror is fed by the `pipeline-cache-refresh` edge fn (triggered by `roles-roundup-daily-ping` pg_cron, bundled with the daily sourcer DMs); it pulls the tribe-recruiting gz, NOT Bubble directly, so mirror freshness lags. Moved that sync 16:30 -> 17:00 CEST (`cron.alter_job` to `0 15 * * 1-5`) at Blake's request; sourcer DMs moved with it; fixed-UTC so 4pm in winter. (3) Filled the 2 missing 07-24 roles (Aiven Account Executive - India = TA ran it, likely no intake; Wolt SSC Operational excellence team manager = brand-new, intake). Ops detail in memory `reference-intake-eligibility-tracker.md`.

### 2026-07-22 — New-role briefing bot BUILT (dry-run to Blake)
- **Built + deployed the "similar roles" briefing bot** (spec: `NEW_ROLE_BRIEFING_SPEC.md`, now v1.3). Lives in the tribe-job-intel Supabase project (same as the roles weekly bot), NOT Cowork. Reads ONE gz snapshot and does everything from it.
- **Edge fns:** `new-role-briefing` (gated on WEEKLY_DM_API_KEY) downloads `dashboard_data_snowflake.json.gz`, detects new client roles, matches similar history (location-aware ladder, N=8, exclude tth<3d, re-post suppression, low-confidence fallback), renders a Slack briefing, DMs recipients, dedupes via new `role_briefings` table. `scheduled-new-role-briefing` = unauth wrapper (holds the key, dry-runs to Blake, rolling 4-day lookback). Cron `new-role-briefing-dryrun` = `0 7,11,15 * * 1-5` (09/13/17 CET weekdays).
- **Data plumbing added to enable it (render_json.py, tribe-recruiting):** widened `load_jobs_list()` so the gz jobs list now carries job_category/subcategory/location; added `load_job_meta()` → `job_meta` map (cat/subcat/loc for ALL non-test jobs any year, 6627 entries) for region matching across pre-2025 history. Keboola config 01kpr863 v24 stages the extra columns. Reason: the Supabase `jobs` mirror only has category/location for the ~127 intake jobs (0 for new roles), so it can't feed matching — the gz is the source of truth. Commits aabf468, 9940151.
- **Live test (2026-07-22) — 3 recent roles, all delivered to Blake's Slack, recipients resolved to real slack_user_ids:** Glovo Sales Exec Zagreb → Sales Manager·CEE/SEE (76 roles, tth 23d, Salary 29%, would go to Milica); Wolt Program Manager Berlin → Operations·DACH (99, tth 30d, Recruiter-Rejected 32%, Elena); Eucalyptus PM → Product Manager any (24, tth 40d, Skills 31%, Dušan).
- **STATUS: dry-run to Blake, running forward.** Every new client role now triggers one DM to Blake (only), 3x/business day. GO-WIDE to real TAs/sourcers = POST {dry_run:false} to the wrapper or edit the cron body — recipients already resolve correctly. Tuning items (not blockers): drop reasons include internal-decision drops (Recruiter/Hiring-Manager Rejected) worth relabelling; N, region groups, re-post title match all tunable. Full ops in memory `reference-new-role-briefing-bot`.

### 2026-07-17 (incl. 2026-07-16 afternoon)
- **ADDED (production Keboola, Blake-approved): Bubble `call_record` ingestion.** The AI screening-call Chrome extension's log table was API-enabled in Bubble; added as endpoint #29 to the full-load extractor config `122491135` (v111 minimal probe → v113 final). Lands in `in.c-kds-team-ex-bubble-io-122491135.call_record` (~617 rows), refreshes with the daily 04:16 run. Field list deliberately EXCLUDES the full-text `transcription` and `captions` (Blake: too much volume); the small `*_status` flags stay. Also audited Bubble's enabled endpoints vs what Keboola ingests: 13 enabled-but-not-ingested types (recruiterScreens, Nylas_email, DuxSoup_profiles, calendars, calendar_events.type, Notes, Location, both message templates, errors, Date, API_ID) — reference list in the chat if ever needed.
- **INCIDENT + fix (schema mismatch broke the daily run):** one run on Jul 16 18:58 executed the interim full-field config (v112) and created the `transcription`/`captions` columns; after trimming to v113, the Jul 17 04:16 run and a manual rerun both failed with "Some columns are missing in the csv file". This extractor requires the storage table's columns to match the field list exactly — **removing fields means the storage columns must be dropped too**. Blake deleted the two columns in the Storage UI (Claude's Keboola MCP cannot delete storage columns/tables); verified healthy with job `1005752115`.
- **Analysis conventions locked with Blake:** reporting start = **2026-07-13** (team rollout; Jun 4–Jul 12 was Andreea/Rodrigo building the tool, 376 of the first 610 rows were test jobs). Always exclude `"jobName" ILIKE '%test%'`. Attribution via `user` → `User.bubbleinternal_id` (`Creator` is always empty — backend workflow writes the rows). Buckets are mutually exclusive and sum to calls: no_show (`assignment_status='pending_no_show'` OR `rejection_reason='No Show'`) → pending (`status='running'` OR assignment pending/needs_decision/blank) → completed, split by `linkedinUrl` present. % complete = (completed + no shows) / calls. NB: the tool writes the LinkedIn URL at resolution, so completed-without-LI is structurally ~0.
- **Deliverable: two-tab static dashboard** (`call_tool_dashboard.html`, committed to this folder): **Usage** (week toggle, per-recruiter table: calls / with LI / no LI / no shows / pending / % complete) + **Quality & Coverage** (salary captured 94% of completed, note sent 100%, 9 clients + 33 jobs covered — Taxfix & AVIV lead; processing health: 1 failed + 1 timeout transcript). Week of Jul 13: 144 calls, 19 recruiters, 40% complete; Marina Nikolic is the power user (36 calls, 89% complete).
- **Findings for the team:** 22 calls (15%) have no job linked, from 8 different recruiters → extension workflow gap; 72 calls all-time have no signed-in user → attribution hole (both worth raising with Andreea). Several recruiters never resolve their calls (Simon's 14 Reaktor calls all pending) — "% complete" partly measures follow-through. No call duration/end-time exists in the table. `extension_version` only started populating on the newest calls (5.0.6).
- **Status: snapshot only, parked pending team feedback.** Next step when green-lit: `call_record_usage` Snowflake transform + wire the view into the build cycle for auto-refresh.

### 2026-07-16
- **CHANGED (production Keboola, Blake-approved): Candidate Finder transform cadence 4x/day → 1x/day.** Rationale: the Finder is a historical search tool and doesn't need constant refreshes; the upcoming Role Pipeline tab (live view) will stay at 4x/day.
- New flow **"Candidate Finder — daily refresh"** (`01kxn10awrwf4pda3kz42zhqp6`): single task running transform `01kvzgpgwh38awepha7eey08pe`, schedule `20 10 * * *` Europe/Prague (schedule id `01kxn1ygdrvajwbsbw2x5rc9wp`) — after the ~8:35 morning data pull, before the 10:40 build, so finder_data.json.gz ships same-morning data once daily.
- Main build flow `01kpqyq1pz6qpmk7m9s4qx8gmg` bumped to **v10**: candidate-finder task removed from the transformations phase. Verified post-change: all 10 remaining tasks intact, both schedules (`40 8,10,13` + `10 16` Prague) untouched.
- **Validation run:** manual run of the new flow, job `1005520807`, success in 48s; transform wrote 93,416 rows to `out.c-Candidate-Finder.candidate_finder`. Render step is unaffected by design (it stages the table every build regardless of when it last refreshed). Watch item: confirm the 13:40 build + render succeed (self-check scheduled for ~14:00 Prague today).
- **Rollback if needed:** re-add the candidate-finder task to the build flow (config is in v9 history) and disable the new flow's schedule.
- Credit effect: ~6 credits/month saved; net after Role Pipeline ships at 4x/day: roughly +4/month vs before.
- **Role Pipeline UI designed + data validated via interactive board (`role_pipeline_validation.html` in this folder — treat it as the design spec for the real tab).** Iterated live with Blake from CSV → table → kanban board. Four real roles embedded (snapshot 2026-07-16): Glovo Backend Engineer II (Chené), Glovo Data Internship (Samantha Nel), AVIV Senior Android Germany (Alexandra), Eucalyptus Senior SWE UK Fullstack (Dušan). Shareable by dropping the file in Slack (self-contained, opens in browser; do NOT host publicly — candidate PII).
- **UI locked:** stage-column board with compact cards; dropped/declined/archived collapsed per column (dimmed, reason chips); stale badge ◷ Nd on active cards idle 30+ days; recruiter→client→role cascading selectors (not tabs — must scale to ~870 roles); "Check a week's numbers" panel (ISO week + metric chips → per-sourcer breakdown + exact candidates, WBR-compatible windows) — designed off Samantha's real WBR-discrepancy question, plus planned "all my roles" mode; hired-but-DQ-flagged records shown as review items.
- **Duplicate taxonomy (validated in Snowflake):** same-talent-same-job dup rows reliably carry `is_candidate_duplicated=true` on exactly one row (7/7 live pairs) → production transform drops flagged rows (NB: the unflagged row is canonical even when the flagged one is further progressed). Same-human-different-talent dups (accents/suffixes/second profiles: Picchio, Yumna, Rakshith, Magalhães, Zeeshan+PhD) have NO flags — talent-level dedupe fields unused in Bubble — so they only get a ⚠ near-dup badge, never auto-merge.
- **Data hygiene findings for the team:** Glovo BE II has 63 of 75 "active" candidates with no movement in 30+ days (Bubble cleanup needed, Chené); Ahmed Jamal (Glovo BE II) is Hired (2026-06-29) AND disqualified with reason "Location" — contradictory record, ask Chené which is true; archived flag barely used (2 records on that whole role).
- **Cross-check vs live dashboards:** board hires per role match Project Dashboard hires drill-down exactly (Glovo BE II 5, Data Internship 4, AVIV/Euca roles 0). Same reporting-v2 source as all dashboard tabs; WBR/MBR aggregates use weekly windows + own attribution so they don't compare card-for-card by design.
- Scope doc updated with the locked UI + dup/archived rules; frontend estimate now ~1d (total ~2d) since the board replaces the simple table.

### 2026-07-15
- **SCOPED (nothing built): "Role Pipeline" tab — per-role candidate deep-dive with CSV export.** Blake relayed recruiter requests (Sam et al.) for the old PBI "Data Download"-style view: pick a role, see its candidates, stages, dates, export. This is the item CSV_EXPORT_SCOPE.md parked in June ("Rodrigo's PBI Data Download tab... separate 1-2 day build if/when approved"), now approved for scoping. Full spec in `ROLE_PIPELINE_SCOPE.md` (this folder).
- **Decisions locked with Blake:** (1) candidate floor = positive response or beyond (is_candidate_reacted OR any screen+ date) — contacted-no-reply people appear only in a per-role aggregate funnel strip, not as rows; (2) role window = open roles + roles CLOSED in the last 6 months (Blake rejected the activity-only window: recently archived roles must stay visible even if their pipeline went quiet earlier); (3) visible to all employees behind the normal login, like the Candidate Finder (confirmed the Finder is not role-gated); (4) standalone tab.
- **Archive-date finding (matters for anything else that ever needs "when did a role close"):** Bubble has NO archive date — only the `archived` flag and `Modified_Date`, which changes on ANY edit. Validated live: ~580 archived roles with last activity in 2020-2024 were "modified" within the last 6 months (bulk edits), so Modified_Date alone is a broken close-date proxy (would have given 1,446 in-scope jobs / 84k rows of zombies). Agreed rule: backfill = last activity within 6 months OR Modified_Date within 6 months AND within 90 days of last activity; going forward = the transform snapshots the archived flag each run into `role_pipeline_archive_log` (job_id, first date seen archived), giving exact close dates from ship date onward.
- **Live sizing (Snowflake, 2026-07-15, final hybrid rule):** 866 jobs in scope; positive-response+ = 40,996 rows (~2.5-3MB gz, lighter than finder_data.json.gz); all-contacted would have been ~117k (rejected); screen+ only ~23k (rejected — hides in-conversation pipeline). Full history for context: ~1.38M candidate rows, which is why the window matters.
- **Build shape (est. ~1.5 days, no new infra):** new Snowflake transform (clone the Candidate Finder pattern, two outputs: role_pipeline_candidates ~40k rows + role_pipeline_jobs ~833 rows with aggregate funnel counts) → input-mapping + `build_role_pipeline()` in keboola_entry.py → `role_pipeline.json.gz` (best-effort, can't break the main refresh) → new lazy-loaded App.jsx tab (client → role cascading pickers, stage-grouped table with stage dates, funnel header strip, Export CSV 5k cap + UTF-8 BOM) → `role-pipeline` task in the build flow's transformations phase.
- Read-only session otherwise: only writes were `ROLE_PIPELINE_SCOPE.md` (new) + this log entry. Next step: Blake green-lights the build.

### 2026-07-06
- **FIXED: Milica Mladzic (on leave) wasn't showing in MBR.** WBR shows a TA in a week if they have activity OR a comment/reasoning for that week; the MBR gate only checked activity, so Milica (zero week-27 activity but a real week-27 lead note, "OOO 2 days and came back from her 2 week holidays…") was dropped. Added a last-week-note check to `taRows` in `App.jsx` mirroring WBRTab's `hasNote` rule; the filter is now `(_last_week_activity > 0) || _last_week_note`. While here, removed two now-obsolete blocks: the manual Milica leave hack (she gets a proper target row + comment from the pipeline rebuild now) and the actuals "union" completeness block (a verified no-op after the rebuild gives every rostered TA a target row). Frontend-only, commit `64c424b`; live once Cloudflare rebuilds (no Keboola run needed — data already has her target row + comment). Note: a TA active on a client they have no target row for still won't appear (pre-existing target-driven limitation), but that didn't affect anyone real this window.
- **FIXED (root cause): MBR TA table was missing TAs and mis-attributing hires vs WBR (Iryna Dyda missing, Aviv hires wrong, Niki Vokalkova / Adelya Khakimova dropped).** Root cause: `mbr_ta_targets` was never rebuilt from the target sheet — it was carried forward verbatim from the live JSON (`live["mbr_ta_targets"]`) every run, so the MBR target list froze months ago. WBR reads `wbr_ta_target.csv` fresh each run via `load_ta_targets_from_csv`, which is why WBR was correct and MBR was not. Two symptoms: (1) TAs added to the sheet mid-period (Iryna Dyda / Aviv, added June) never got a target row so the target-driven MBR dropped them, while their hires still hit the client total → Aviv showed 12 hires but only 7 across visible TAs; (2) the frozen list stored Wolt sub-BU TAs under long-form client names ("Wolt North, Baltics & Benelux") which mismatched the ABBREV-keyed actuals ("Wolt NBB") and silently dropped them (Adelya, Jelena; same class as Niki / "Wolt Central & South").
- **Fix (option 1, sheet ∩ roster):** added `load_mbr_ta_targets_from_csv()` to `render_json.py` — rebuilds `mbr_ta_targets` from `wbr_ta_target.csv` every run, ABBREV-normalizing clients and scoping to (client, TA) pairs on the weekly roster for the MBR window. Wired to override `live["mbr_ta_targets"]` right after `mbr_weeks` is computed (feeds both `active_target_pairs` and the final output). Verified in isolation against live data: adds Iryna/Milica/Adelya/Jelena, drops rolled-off names (Anna Tyulpanova + the long-form Wolt dupes), no long-form Wolt clients remain. Commit `9baf6e1` on `main`. Takes effect on the next Keboola Flow run (component `01kpr863ypqr5pt74wms8fdj67` clones `main` and runs `render_json.main()`); can be triggered manually to apply immediately.
- **Interim frontend patches (commits `b324735`, earlier):** `App.jsx` got (a) a last-week-activity client matcher that bridges Wolt long↔abbrev names — still needed because the gate reads `wbr_actuals` which is long-form; keep it — and (b) a "union with actuals" completeness band-aid that surfaced Iryna before the pipeline fix — now redundant once the refresh runs; can be removed. Also earlier same day: MBR TS targets switched from contacted-ratio (15/10/5%) to flat weekly (RecScreen 10, ActScreen 7, ATS 4) × weekCount (commit `1d28d23`).
- **Watch-item:** any future "MBR missing someone / numbers differ from WBR" is most likely this same class — confirm the TA is in `wbr_ta_target.csv` (source) and on the weekly roster; the rebuild now keeps MBR in sync with the sheet automatically.
- **FIXED: Rodrigo Gomes' Circle contacted goal showed 240 while the WBR target sheet said 180 for last week.** Root cause was a data-entry error in the TS Weekly Google Sheet, not a dashboard bug. The week of 29/6–5/7 was labelled `2026W26` instead of `2026W27`, so Rodrigo had two W26 rows (240 for 22/6–28/6, 180 for 29/6–5/7) and no W27 row. The pipeline parses the week number straight from the label prefix (`build_ts_weekly_from_csv` regex `(\d{4})W(\d+)` in `render_json.py`), so both rows became week 26; the consumer takes the first match (240) and week 27 never existed. Circle therefore showed 240; the sheet cell Blake was reading (the mislabelled 29/6–5/7 row) said 180.
- **Resolution:** Blake corrected the label to `2026W27 (29/6-5/7)` in the source sheet. Confirmed W26=240 was intentionally correct (W25 was 300, step-down). Verified live in Keboola `in.c-wbr-sheet.wbr_ts_weekly` after re-extract: W26=240, W27=180, duplicate gone.
- **Propagated the fix:** ran the Google Drive extractor (`keboola.ex-google-drive` / `01kpr3tek8ezs48pg02e60jdpe`, job `1003188718`, success) then the render+push+notify_circle config (`kds-team.app-custom-python` / `01kpr863ypqr5pt74wms8fdj67`, job `1003188949`, success, configVersion 19). gz rebuilt + pushed to GitHub + Circle dispatch fired; Circle rebuilds off the dispatch (2h cron backstop).
- **Takeaway / watch-item:** the week label is free-text and drives the week number. A mistyped `W##` silently creates duplicate-week rows (first-match-wins downstream) and a missing week. Sheet instruction #6 already warns duplicated TA/TS in the same week breaks the refresh. Worth considering a validation check on the sheet extract that flags label-vs-date-range mismatches per person.

### 2026-06-26
- **SHIPPED: Internal Recruiting tab now attributes the Ashby right-side funnel per job (the cross-system crosswalk).** Replaces the old `/* TODO: cross-system job match */ true` no-op. Verified live in production (render job `1000930111` → commit `c2a0b7c`): the deployed gz now stamps `bubble_job_id` on 54 `ir_ashby_funnel` rows across 7 internal jobs + 8 hires.
  - **Design = two link sources, unified + de-duped in `render_json.attach_bubble_job_ids()`:**
    1. `recruiting-dashboard/refresh_staging/ir_crosswalk.csv` — committed backfill of the 17 confirmed 2026 IR reqs (Bubble job_id ↔ Ashby UUID). This is the history, no external dependency.
    2. Bubble's `atsID` field (staged as `snowflake_job.csv`) — lets the team self-link NEW reqs going forward by typing the Ashby UUID **or** the `TXYZ-N` tag into Bubble's `atsID`; resolves via Ashby `customRequisitionId`. Zero code/CSV edits per new req.
  - Each ref resolves to an `ashby_job_id` by UUID (direct) or by `customRequisitionId` (TXYZ). Many-to-one (two Bubble projects sharing one Ashby req — the German TAP pair, the US+EMEA Senior Tech TA pair) de-dupes to the **most-recently-created** Bubble job so the funnel isn't double-counted.
  - **Files (pushed to `main` this session):** `ir_crosswalk.csv` (new), `render_json.py` (two-source join), `ashby_extract.py` (`_trim_job` now keeps `customRequisitionId` + `customFields`), `keboola_entry.py` (whitelists `ir_crosswalk.csv` for staging), `src/App.jsx` (real per-job `inJob` match on `bubble_job_id`; `in_ashby` reflects real links; Bubble-fallback when unlinked so it never shows 0; badge "live from Ashby ✓" vs "not linked to Ashby" per job).
  - **Keboola change (render config `01kpr863…` → v19):** added `out.c-reporting-v2.job` (cols `job_id`, `job_ats_id`, `date_created`) as input `snowflake_job.csv`. Updated **storage only** — parameters/encrypted tokens untouched (verified `#github_token`/`#ashby_api_key`/git `#token` intact). Harmless before the repo push (old code ignored the extra input).
  - **Crosswalk decided by:** fuzzy title + creation-date match of the 17 Bubble IR jobs (client "Tribe.xyz (IR)" `1769076744468x…` + legacy "Tribe.xyz" `1644…`) to Ashby reqs, then Sanja/Blake confirmed (US East Coast & EMEA/CET both → `4d56606d`; German pair → `615be379`; UK → `97cfea87`). Authoritative file `IR_ashby_crosswalk.csv` + review diff `IR_crosswalk_changes.diff` in this folder.
  - **No Bubble bulk-edit needed** (Blake couldn't easily edit Bubble's data tab → hardcoded history in repo). Decided AGAINST the WBR sheet or a new GDrive-extracted sheet.
  - **FOLLOW-UP DONE + bigger finding → IR tab redesigned into TWO funnels (commit `a088ba0`).** While fixing the stage-name mapping (Ashby uses "Final Interview", not "Onsite" — that's why Onsite read 0), candidate-level analysis revealed the two systems track **largely disjoint populations**: Bubble = outbound sourcing, Ashby = ATS dominated by **inbound** applicants. Evidence: TA Partner German = **3 Bubble contacted vs 780 Ashby applicants** (~1 shared); Talent Sourcer German = 268 vs 372 (<2% shared); the one fully-tracked converter (Marina Lazarević) was tracked in **parallel** (Bubble "Contacted" = Ashby "Initial Screen" same day), not as a sequential handoff. So a single stitched funnel double-counts the few converters with no clean seam to cut at. **Resolution:** split the IR tab into two labelled funnels, no shared bars:
    - **Sourcing (Bubble):** Contacted → Positive Response → Recruiter Screen → Actual Screen → Moved to ATS.
    - **Ashby (interviews, all sources):** Application Review → Initial Screen → WHO → Case Study → Onsite (Final Interview + Call with Martin) → Culture Interview → Call with Client (+ Client Prep / Presented to Client) → Offer → Hired. WHO and Case Study are their own bars. Ignored non-progress stages: Archived, Talent Pool, Sourced, Uncontacted, Tier 2, Language Check.
    - Validated against live data: Onsite 0→15, WHO/Initial Screen/Call-with-client populate. Frontend-only change (App.jsx) → Cloudflare rebuild, no pipeline run needed. Note for future: Bubble has a `source` field (Sourced / Applicant / recruitee_*) if we ever want to filter the sourcing funnel to outbound-only.
  - Team will start putting `TXYZ-N` into Bubble `atsID` going forward → new reqs auto-link, no further work. Makes the planned n8n fuzzy auto-matcher unnecessary.
  - Also this session: Ashby **MCP connector** (custom Cloudflare Worker `tribe-ashby-mcp`) still 401s and was PARKED — see the long 2026-06-23 entry. Not a blocker (the crosswalk uses the Keboola pipeline's Ashby pull, which works).

### 2026-06-25
- **Shipped the Candidate Finder tab to the recruiting dashboard (live).** A searchable directory of engaged candidates (reached Recruiter Screen or beyond), ~92.5k rows, gated to employees like the rest of the dashboard. Filters: Function → Role type (cascading), Client, Country, Stage reached, Disqualification reason, plus free-text search and an "only with LinkedIn" toggle. Each row links to LinkedIn and shows current title/company/location, the client+role sourced for, stage, and drop reason. Names link out; table caps at 300 rows with "Show more".
- **New Keboola Snowflake transform `Candidate Finder`** (`keboola.snowflake-transformation`, config `01kvzgpgwh38awepha7eey08pe`) → output `out.c-Candidate-Finder.candidate_finder` (13 cols, 92,525 rows). Population = candidate joined to job/client/candidate_stage/talent (+ latest employer via talent_position→talent_employer), filtered to stage_current_type in (Recruiter Screen, Offsite, Final Interview, Offer, Hired), non-test jobs. Two data-quality fixes baked into SQL: strips the LinkedIn scrape artifact "Related to search terms in your query" from titles and nulls LinkedIn tenure strings ("Dec 2025 - Present · 7 mos") mis-stored as titles; role_type = job_subcategory, else keyword inference on job title, else "<Function> (other)" (no "Unclassified" — function fallback covers the long tail; ~92% land on a specific role type, ~8% on the function catch-all). Country split from location_address; non-breaking/trailing spaces scrubbed.
- **Pipeline wiring (all on the existing Keboola → GitHub → Cloudflare path, no new infra):**
  - Render component (`kds-team.app-custom-python`, `01kpr863…`) input mapping gained `out.c-Candidate-Finder.candidate_finder` → `snowflake_candidate_finder.csv` (now 30 input tables). Config v18.
  - `recruiting-dashboard/refresh_staging/keboola_entry.py`: added `build_finder()` (reads the staged CSV → lean JSON `{generated_at, candidates[]}`) and `push_to_github(..., finder_content)` writes/commits `recruiting-dashboard/public/finder_data.json.gz`. `render_json.py` untouched. Best-effort: if the CSV isn't staged or anything errors, it logs a warning and the rest of the refresh proceeds — cannot break the main dashboard build.
  - `recruiting-dashboard/src/App.jsx`: added `CandidateFinderTab` (+135 lines) that lazy-loads `/finder_data.json.gz` (gunzip via DecompressionStream, same pattern as the main bundle) only when the tab opens. Tab registered for all roles. Full `vite build` passed before push.
  - Build flow `01kpqyq1pz6qpmk7m9s4qx8gmg` (v9): added `candidate-finder` task to the **transformations** phase so the table refreshes each cycle (~4×/day) before the render step. It only reads reporting-v2 (already fresh from the upstream data pull), so it's independent of the other transforms.
- **Deploy:** commit `d800d9d` (code) then a manual render run (job `1000709878`, success, 168s) produced `build_finder: 92525 rows`, wrote `finder_data.json.gz` (5,354 KB gzipped) + rebuilt the main dashboard gz (2,279 KB) and pushed commit `ca7a9725db` to `main`; Circle dispatch HTTP 204. Cloudflare auto-deploys. The finder file is ~5.2MB gzipped — fine but a candidate for slimming later (drop fields / pre-filter) if load feels heavy.
- **Follow-up (same day): multi-select filters + CSV export.** Reworked the Candidate Finder filters from single-select to multi-select (custom checkbox dropdowns with a per-list search; OR within a filter, AND across filters; options still cascade). Added an Export CSV button that downloads the *current filtered view* (all matching rows, not the 300-row display cap) with a hard cap of 5,000 rows — over that it disables and nudges "Filter to ≤5,000 to export" (PII guard against bulk dumps). Frontend-only, App.jsx commit `d5cc6ea`; Cloudflare rebuilds on push (no pipeline/data change). role_type accuracy left as-is per Blake (audit: subcategory ~94% function-consistent vs ~89% for title inference; both noisy, no clean auto-fix — the real job title is shown in the table as ground truth).
- **Parked earlier in session:** interview-notes/transcriber-notes are NOT yet in Keboola — the Bubble `transcriber_notes` and `Recruiter_screeen_notes.summary` fields exist but `transcriber_notes` is not exposed on the Bubble Data API (checkbox off) and per Mikhail nothing is being written there yet (will eventually flow to `call_record`). No pipeline work possible until Bubble starts populating + exposes it.

### 2026-06-24
- **Added a data-freshness note to both surfaces.** Recruiting dashboard header (`recruiting-dashboard/src/App.jsx`) now shows, under "Snowflake pipeline" so it sits atop every tab: `Refreshes ~4×/day · data fresh by 09:00, 11:00, 14:00 & 16:30 CET` plus `· last updated <time> CET` when available. The live timestamp is read from the gz fetch's `Last-Modified` response header (the recruiting data has no embedded `generated_at`); the clause is omitted gracefully if the header is absent. Circle hiring overview (`tribe-circle/circle.html` + `index.html`) had a stale line ("08:40, 10:40, 14:40, 16:40 Prague") — replaced with the same ready-by schedule + `last updated <formatRefreshTime(generated_at)>` (Circle data DOES carry `generated_at`, so its timestamp always renders). Times reflect the 2026-06-13 cadence change (build ready by ~8:50/10:50/13:50/16:20, targets 9:00/11:00/14:00/16:30).
- **Pushed live:** tribe-recruiting `main` → `c4c3110`; tribe-circle `main` → `5310e22`. Both auto-deploy via Cloudflare.
- **⚠️ Incident (self-inflicted, fixed): partial-clone wipe + recovery.** First push of App.jsx was done from a `--filter=blob:none --no-checkout` clone; with the working tree unpopulated, the commit recorded 1074 files as deleted and pushed it (origin briefly had only 4 files — `18c98ca`). Caught immediately. Rebuilt a clean commit from the original tree using plumbing (`read-tree <good> → update-index App.jsx blob (mode 100755) → write-tree → commit-tree -p <good>`), verified the only diff vs the pre-incident tree was App.jsx, and `--force-with-lease`'d over the bad commit. origin/main restored to 1078 files with just the freshness change. **Lesson: never commit/push from a `--no-checkout` or sparse clone; use a full checkout (or stage explicitly and confirm `git status` shows no spurious deletions) before committing.**
- Local working copies in this folder synced to match live: `App.jsx` (was a stale 4542-line scratch copy; now the live 5235-line version) and `circle_patch/circle.html` + `circle_patch/index.html`.

### 2026-06-23
- **Full health check — all green.** Keboola: last 40 jobs all `success`, zero errors. PROD V2 (`375145203`) ran 05:52 + 07:53 CET (816s / 1054s); Flow A 4x and Flow B 3x clean; render+push (`01kpr863…`) succeeded 06:46 + 08:46, committed `6ec1e54` to `main` and dispatched Circle (HTTP 204). PAT write access healthy (6/22 403 freeze stays resolved). GitHub: tribe-recruiting HEAD `6ea8a4a` (BambooHR team_leads refresh 10:09 UTC) + TA/TS weekly syncs, all today. Circle fresh (`generated_at 2026-06-23T08:49:02Z`). recruiting.tribe.xyz returns HTTP 200. Finance dashboard (tribe-dashboard, separate GH Actions) also refreshed today. Data freshness sane across WBR/MBR/Project Dashboard/TTH/KPI-TS/New Project Health (141 rows, newest project 2026-06-20).
- **CORRECTION — the Internal Recruiting tab is NOT wired to Ashby (Blake was right).** Earlier in the session I claimed the IR tab shows "live from Ashby." Verified against the deployed `src/App.jsx` (HEAD `6ea8a4a`) — it does not, in any job-accurate way. Findings:
  - Everything job-specific on the IR tab (job dropdown, active-jobs list, left funnel Contacted→ATS, Sourced By, Interviewed By, DQ reasons, weekly perf) is built 100% from the Keboola `ir_*` tables (Bubble source). No Ashby in that path.
  - The Bubble↔Ashby job join was never built. Two proofs in code: (1) `in_ashby` is hardcoded `false` for every job (only one assignment in the file). (2) `ashbyStageMap`'s job filter is literally `const inJob = (r) => jobFilter === 'all' || /* TODO: cross-system job match */ true;` — the cross-system match is an unfinished TODO that returns `true`, so the right-side stages (Onsite→Hired) show a **global Ashby aggregate** across all Ashby reqs in the window, never matched to the selected job. Selecting a single job still shows that same all-jobs lump (no-op filter).
  - The green "live from Ashby ✓" badge (`hasAshby` = `ir_ashby_funnel_jobweek` non-empty, currently 178 rows) therefore oversells reality. Treat the right-side Ashby bars as cosmetic/unreliable.
  - **Ashby data plumbing exists but is half-built and partly stale.** `keboola_entry.py` → `run_ashby()` → `ashby_extract.py` seeds from a static baseline (`ashby_applications_baseline.json`, committed once `ff57f64` 2026-05-05, never rebuilt; no workflow regenerates it) then applies per-job incremental sync tokens. This morning 27 jobs threw `sync_token_expired` (only **7 are Open** active internal roles; the other 20 are Draft/Closed/Archived/TEST). Expired-token jobs never advance past the May-5 baseline because the failing call can't refresh the token. Late-stage *histories* are re-fetched live, so pre-existing apps still update, but candidates added after token expiry are missing. Net impact is immaterial *today* because none of this Ashby data is attributed per job anyway.
  - **Separate:** the Ashby **MCP connector** returns HTTP 401 on every call. This is NOT the pipeline key — the pipeline's `#ashby_api_key` works fine (pulled 19,427 apps this morning). Only live Ashby lookups via the connector are blocked.
- **Ashby MCP connector — long debugging session, NOT resolved, PARKED 2026-06-23.** The connector is a custom Cloudflare Worker `tribe-ashby-mcp` (account `tribe-bamboohr`, URL `tribe-ashby-mcp.tribe-bamboohr.workers.dev`), an OAuth-gated MCP server that calls Ashby with a Worker secret `ASHBY_API_KEY`. Two secrets matter: `MCP_PASSWORD` (the `/authorize` login gate) and `ASHBY_API_KEY` (the Ashby creds); also a vestigial unused `MCP_BEARER_TOKEN`. What we did and ruled out:
  - Generated a fresh Ashby key ("Blake - Claude MCP", scopes jobs/candidates/interviews/offers/organization/apiKeys/reports/etc.; Ashby has NO separate `applications` scope — applications read under `candidates:read`; no per-key IP allowlist). Key is VALID: direct `curl -u "<key>:" POST api.ashbyhq.com/apiKey.info` returns success, including a request that exactly replicates the Worker's (same `Accept: application/json; version=1` header).
  - Set `ASHBY_API_KEY` on the Worker every which way (wrangler pipe → added a trailing CRLF; wrangler interactive; `cmd /c "wrangler secret put ... < file"` with a verified 64-byte no-newline file; and finally the **dashboard** secret field). `/health` confirms `ASHBY_API_KEY: true` (bound). Still 401.
  - Found + fixed a real issue: Worker had versioned/gradual deployments, so secret puts and the dashboard compat-flag edit kept landing on *different* versions. Confirmed the active version eventually had BOTH the clean secret AND the `global_fetch_strictly_public` compat flag (added per the Worker's own CIMD startup warning; compat date Apr 1 2025; `nodejs_compat` also present). Still 401.
  - Worker code is correct: `authHeader = "Basic " + btoa(\`${key}:\`)` (matches the working curl), and the OAuth provider threads the full Worker `env` into the `/mcp` ApiHandler → `handleRpc` → `tool.handler(env2,...)` → `post(env2,...)` which reads `env2.ASHBY_API_KEY`. No code path drops the secret.
  - Decisive clue: a deliberately-wrong key via curl from Blake's machine ALSO returns bare `Unauthorized` (same as the Worker), proving the bare `Unauthorized` is Ashby's normal *rejected-credential* response, NOT an IP/edge block. So the Worker is sending a credential Ashby rejects, despite the secret being set correctly and bound. Most likely a Cloudflare version/isolate propagation quirk on this specific Worker or a stale Claude-connector session; a full connector disconnect/reconnect was the last untried step. **Blake called it here — done with it for now.**
  - **Bottom line: the connector is NOT required for the dashboard goal.** It only powers Claude's live ad-hoc Ashby queries. The IR-tab Ashby wiring runs entirely off the Keboola pipeline's Ashby pull (works). If revisiting: try the full disconnect/reconnect first; if still failing, rehost the MCP off Cloudflare Workers (small VM/container). Note: a live Ashby key value was pasted into chat during this session — rotate it.
- Open: decide whether to (a) finish the real Bubble↔Ashby job crosswalk so the IR right-side means something per job, or (b) hide the "live from Ashby ✓" badge and drop the unmapped aggregate until then. Discussion of wiring options started with Blake (no shared job key is the core blocker; manual crosswalk of the ~6-15 active internal reqs is the likely pragmatic path).
- **Refresh cadence re-timed: data pull 6×→4×/day, aligned with the dashboard build (credit saving + reliable "ready-by" freshness).** All times Europe/Prague. Done via `modify_flow` schedules — the Keboola MCP DOES manage schedules (an earlier assumption it couldn't was wrong; corrected mid-session).
  - **Data pull** (legacy orchestrator "4x daily - NEW", `118392817`, Bubble incremental → PROD V2 `375145203`): now **8:15, 10:15, 13:15, 15:45** (schedule `813304500` = `15 8,10,13 * * *`; `01kvt2b0kmd2q98bz4jr8v9013` = `45 15 * * *`). Was 6× every 2h at :51.
  - **Dashboard build** (flow `01kpqyq1pz6qpmk7m9s4qx8gmg`): now **8:40, 10:40, 13:40, 16:10** (schedule `01kpqyy8erysz0gahj63zwkwtj` = `40 8,10,13 * * *`; `01kvt2d3farbr4d98yg61pbjgn` = `10 16 * * *`). Was `40 8,10,14,16`.
  - Each cycle: pull (~20 min) → build (~10 min) → dashboard **ready by ~8:50 / 10:50 / 13:50 / 16:20**, ahead of Blake's targets of 9:00 / 11:00 / 14:00 / 16:30. Pull at 4× = fewer of the expensive ~17-min PROD V2 runs (credit saving).
  - Approach = **aligned crons, NOT the event-trigger** (Orchestration Trigger). Chosen because fixed "ready-by" deadlines need the build on a predictable clock rather than drifting with pull-completion. Residual risk: if a pull ever runs >25 min (never observed; max ~20) one build could read slightly stale data. Fully reversible.
  - Blast radius noted: the data pull feeds more than the hiring overview — recruiting + sourcing dashboards and the Keboola Reader (`keboola.app-data-gateway` `01k9vy0j2t8mnkvz2y6fap7327`, a live Snowflake reader schema exposing reporting-v2 tables externally; assumed Power BI but NOT confirmed — config is just named "Keboola Data Gateway"). All fine at 4×.

### 2026-06-30
- Kai-in-app now working end-to-end on the TTH data app. Debug chain that got us there: (1) `uv sync` build fail on flat multi-script repo -> `[tool.uv] package=false`; (2) token secret must be encrypted `#STORAGE_API_TOKEN` and Kai reads it fine; (3) Kai agent-chat 401 "feature not enabled" -> org admin enables Kai in Settings > Features; (4) blank/no-answer -> was using low-level `send_message` stream (only emits tool *requests*); fix = high-level `client.chat(text)` which runs the full agent loop and returns `(chat_id, answer)`; (5) 502 Bad Gateway -> Kai takes 20-40s, longer than the apps-proxy holds a request, so app now runs Kai in a background thread and the page polls `/chat_status`. Also hit a mount-write null-byte corruption; fix = write files into the git clone via heredoc, not cp from the Windows mount. I can trigger redeploys myself via `deploy_data_app` (MCP role is now admin, not guest).
- IMPORTANT CAVEAT (Kai accuracy): Kai is NOT anchored to our curated metrics. Asked "how many Sales roles hired in June 2026" it answered 19, then 7 on a re-run — never the dashboard's 13. It improvises a different table (candidate-level vs project_dashboard_hires) and a different "Sales" definition (job_category vs `JOB_TITLE ILIKE '%sales%'`) each run. Correct curated answer = 13 (job_category='Sales', first hire in June, from tth_jobs). Takeaway: great for exploration, not trustworthy for a specific KPI without the semantic layer or constraining it to curated tables. This is the concrete case for the semantic layer Anna pitched.

### 2026-06-22
- Keboola renewal call with Anna Duskova (rep). Contract renews 2026-08-31; legacy discounted rate ($250/mo license vs $1k list, 600 credits/mo, using ~300, ~4,000 banked credits that only survive if renewing at same tier or higher; min 3 user licenses). New features pitched: Kai (in-platform AI assistant, billed in credits) and Python/JS Data Apps with Kai embedded for stakeholders (~$230/mo per always-on app, sleeps when idle) + native semantic layer. Action items: loop in Martin, Anna sends deck + tier pricing, follow up in ~1 month.
- Corrected the stale architecture in PROJECT_LOG + the `tribe-recruiting-dashboard` skill: pipeline is Keboola/Snowflake (Flows + SQL transforms), NOT local DuckDB/n8n. Keboola STAYS; only PBI dies 8/31.
- Built + shipped the Time to Hire Data App PoC (see "Live now"). Deploy gotcha solved: `uv sync` fails on flat multi-script repos with default build-system; fix = `[tool.uv] package = false` in pyproject.toml. Keboola Python/JS apps need the `keboola-config/` layout (nginx/supervisord/setup.sh) + pyproject.toml, app on port 5000, nginx 8888.
- Next: add Kai (needs a master token, ideally a dedicated service admin user e.g. kai-app@tribe.xyz); then clone the pattern to the Project Dashboard tab for the Martin demo.
- **"New Project Health" tab stale-data fix.** Tab is leadership-only (Blake + Jacopo, `tribe_ph=1` cookie), reads `data.new_project_health` (KR2 = first move to ATS within 4 business days; KR3 = Actual Screen→ATS ≥60% by week 4). Tab/SQL/renderer were all fine — the Snowflake table `out.c-WBRMBR-weekly-aggregations.new_project_health` was fresh (145 rows). Root cause: the Keboola render+push component was failing its final `git push` with GitHub **403 (PAT lost write access)** → `rc=128`. When that push fails the ENTIRE deployed dataset freezes at the last good push; this tab is just what got noticed. Blake rotated the PAT; confirmed fixed via job `999962733` (11:17 CET, commit `573a3e7`, Circle dispatch HTTP 204).
- **Plaintext churn fix (commit `4118ced`).** Pipeline was committing the 52MB plaintext `dashboard_data_snowflake.json` every run (3x/day, ~1.16M line insert/delete each). Now `keboola_entry.py` gzips the rendered JSON and commits `public/dashboard_data_snowflake.json.gz` (~2.3MB binary) directly; build dropped `gzip-data.mjs` (now just `vite build`); plaintext removed from the tree + gitignored. Verified with a local `npm run build` (dist gz valid, 145 rows).
- Open follow-ups: git history still carries old 52MB plaintext blobs (one-time `git filter-repo`/BFG could reclaim space); committing the binary gz still grows history ~2.3MB/run; Ashby threw `sync_token_expired` on ~27 jobs during extraction (separate; Ashby-fed stages may lag).
- **Side-effect of the gzip migration: Circle + team_leads Actions broke (afternoon, fixed).** Switching `keboola_entry.py` to commit only `public/dashboard_data_snowflake.json.gz` broke two downstream consumers that still read the old uncompressed `recruiting-dashboard/src/dashboard_data_snowflake.json`: (a) tribe-circle "Refresh Circle data" workflow `curl -fsSL`'d the raw src path → 404 → failed; (b) tribe-recruiting "Refresh team_leads.json (BambooHR)" Action (`refresh_team_leads.py`) `open()`'d the src path → FileNotFoundError. Circle froze at its last good build (09:21), so Andrea Akovic (and others) showed no current-week data while the dashboard — which reads the gz — was fine. **No data lost** (gz holds the full 27k-row dataset; the morning's `git push rc=128` was a separate, already-recovered token-rotation blip).
- **Fixes pushed (this session):** (1) tribe-circle `refresh.yml` now curls the `.gz` and `gunzip`s it (build_circle_data.py + inject_jobs.py read the same `/tmp/sf.json`); regenerated + committed `circle_data.json` for an immediate unblock — Andrea live at this-wk 10 / last-wk 119, file `generated_at 14:14`. (2) `refresh_team_leads.py` reads the gz with a legacy-path fallback so a future rename won't hard-fail it. (3) Deleted the dormant duplicate `recruiting-dashboard/refresh_staging/build_circle_data.py` (incomplete migration reading the leaner `dashboard_data.json`; grep confirmed nothing referenced it). Marked the retired `refresh_staging/scheduled_task_prompt.md` SUPERSEDED.
- **Blast-radius audit (clean):** finance `tribe-dashboard` (own `data-next/data.json`, fresh 12:25), sourcing `tribe-sourcing` (own `data.json`, fresh 12:46), `tribe-tth-app` (Snowflake-direct) are all independent of the recruiting data file — none affected. No active scheduled task rebuilds the recruiting file (the old Cowork `tribe-recruiting-dashboard-refresh` task was retired and replaced by the Keboola `kds-team.app-custom-python` app).

### 2026-06-19
- Reconstructed project status after Blake reinstalled Claude desktop (chat history lost, files survived).
- Set up durability: this PROJECT_LOG.md, project memory, and GitHub folder backup.
- Note: `Lejla_week25_screens.csv` added today — ad-hoc week-25 screen-credit tally for Lejla Silva (AVIV QA Automation roles), not dashboard code.

---

## 2026-08-27 — Flow unblocked, pipeline healthy, full table audit

### Root cause of "the flow won't run"
On 2026-08-26 16:21 Prague I added the Silver Medalists task to flow
`01kpqyq1pz6qpmk7m9s4qx8gmg` and wrote four optional keys as explicit nulls
(`configData`, `delay`, `retry`, `variableOverrides`). The `keboola.flow` schema types these as
object / string-number / object / array and **none accept null**. The config became invalid, so the
flow could not start by scheduler or by hand. **No error job is ever created**, which is why it
looked like the platform was idle rather than broken.

Fixed by removing the four keys (v12 → v13). Flow ran clean end to end in ~9 min, job `1015661934`.
Full write-up in memory: `incident-flow-config-null-keys`.

### Verification after the run

| Check | Result |
|---|---|
| Rodrigo Gomes 2026 W35 ATS | **4** in project_dashboard, event_attr AND weekly_summary (was 143) |
| Jelena Lacmanovic Contacted wk32/33/34 as TA | **185 / 250 / 260** as expected |
| ATS 2026 total | **7,105** identical across project_dashboard, event_attr and all 4 weekly_summary dimensions |
| Hires 2026 | **1,323** identical across all tables |
| Per-sourcer ATS wk33-35 | project_dashboard vs weekly_summary diff = **0 on every row**, max 17, no outliers |
| Bucket freshness | every dashboard bucket refreshed 09:19-09:23 Prague |

### Contacted discrepancy — RECONCILED, not a bug
`wbr_weekly` 76,270 vs `project_dashboard` 73,768, gap **2,502**. Fully explained:
`BD - Tribe` (2,081) + `Tribe - Marketing` (421) = 2,502 exactly. wbr_weekly includes Tribe's own
internal hiring, project_dashboard excludes it. Scoping decision, not a defect. **Open question for
Blake: should internal hiring be in the WBR numbers at all?**

### New issues found during the audit (none urgent)
1. **Trailing-space client names.** `AVIV ` / `Nexi ` / `Reaktor ` / `Statista ` in wbr_weekly vs
   trimmed in project_dashboard. Totals are unaffected but any join across the two on CLIENT will
   silently drop these. Same class as the `Jelena  Lacmanovic` double-space problem.
2. **weekly_summary internal inconsistency.** CONTACTED 2026 by dimension: ta = 77,149 but
   client / company / ts = 76,261, an 888 gap. ATS is identical (7,105) across all four, so this is
   specific to how CONTACTED is computed per dimension. Not caused by our changes. No blank
   DIM_VALUEs.
3. **`sourcing_closing_hires` is stale since 2026-06-04.** Every other table in that bucket
   refreshed today. Either intentionally static or a dead table. Needs a decision.
4. **`out.c-prodv2-rewrite-test` is 1.35 GB / 27M rows**, created 2026-08-26 during the rewrite
   testing. Safe to drop once confirmed unused.

### Still open from before
- Mikhail's original ATS bug is UNFIXED. ATS still buckets on `date_interview`, so a candidate
  moved to ATS in week 33 with an interview date in week 34 counts in 34. Any event-based fix MUST
  handle the phantom stage-event bursts (see `incident-phantom-stage-event-bursts`).
  `is_event_duplicated` is NOT a usable filter.
- Message to Mikhail about the phantom bursts drafted, NOT SENT. Blake's call.
- `ts_conversion` has no week dimension and mixes time windows.

---

## 2026-09-01 — SHIPPED: ATS now anchored on the ATS move, not the last interview

### The problem
`date_interview` is a derived column holding the LATEST interview-ish event. `Moved to ATS` is stage
type `Offsite`, so the ATS move fed that same column and lost to any later interview. Once the new
Interview 1/2/3 funnel rolled out (mid-July), every advance up the interview ladder dragged the
candidate's ATS credit into the current week. Mis-dating rate went 0.1% (Jan-Jun) to **9.0% (Aug)**,
with only 15 of 122 clients migrated, so it was getting worse on its own.

### What shipped

**PROD V2 `375145203` v241 -> v246.** New `date_ats` column = `min()` of `Moved to ATS` events,
gated `stage_current_num >= 3`, identical to every other stage date. Deliberately OUTSIDE the
coalesce cascade so it can never be backfilled from a later stage (that cascade is what invents
Jonaed's phantom screens). Four edit points: column placeholder, the UPDATE, and BOTH sides of the
`final_candidate_stage_tmp` UNION.

**Weekly funnel `01kpqh9r7g2z66c8vvdr5d87xd` v19 -> v24.** `ats_` CTE now buckets on `da` not `di`.
Int1/2/3 follow automatically since they are computed inside `ats_`.

### Gotcha that cost one failed run
Keboola stores each script array element as exactly ONE statement. Inserting a second `update`
inside an existing element fails with *"Actual statement count 2 did not match the desired statement
count 1"*. Fix: set `date_ats` as a second SET column inside the existing `date_offer` UPDATE.
The failed run wrote NOTHING; prod data was untouched throughout.

### Verification
| Check | Result |
|---|---|
| `date_ats` populated | **49,239**, exactly as predicted |
| Candidates entering/leaving the ATS count | **0 / 0** |
| Aaron Dilley / Luis Alves / Parmeet Singh / Mathias Reck / Vladislav Sakharov | all match raw events |
| `date_offer` integrity | 24 exceptions, all with offer dates <= 2023-04-12, outside reporting |
| Live weeks 29,30,31,32,33,35 vs prediction | **exact match** on ATS and Int1 |
| Weeks 34, 36 | higher than predicted because a scheduled PROD V2 run landed 12 min of newer data between prediction and run. Week 36 = 10 (Mon) + 5 (Tue), genuine new work |

### Impact on weekly numbers
Week 36 (org): 31 -> 15. Weeks 29-33 gained back +3/+7/+3/+7/+7. Simon Siew 18 -> 3.
Mateja Jokovic W34 11 -> 4. Yearly totals per person unchanged (net zero for all ten affected).
Int1/2/3 totals preserved exactly (108/27/15), redistributed to the correct weeks.

### Why the moves are correct
Candidate-level audit: of Simon's 18 in W36, only 3 actually went to ATS this week. Aaron Dilley
went **19 Jul** (43 days earlier) and sits at Interview 2; Luis Alves went **26 Jul** and is at
**Offer**. Of Mateja's 11 in W34, one belonged there; the rest went to Pliant between 31 Jul and
13 Aug and all sit at Interview 1 or 2.

### NOT fixed, still open
- `ts_weekly` RECRUITER_SCREENS counts raw ungated events (Rodrigo 184 vs true ~7)
- POSITIVE_RESPONSE ungated in every table
- the cascade inventing screens (Jonaed 97 vs 18 real)
- WBR and event-attr still bucket ATS on `date_interview` -> **they now disagree with the Project
  Dashboard by the amounts above until Wave 3**
- Blake's stage-TYPE edit still lost (Aiven `Move to ATS stage`, 20 candidates, still invisible)

### Rollback
PROD V2 -> **241**. Weekly funnel -> **19**.

---

## 2026-09-01 (later) — SHIPPED: Int1/2/3 now count in their OWN week

### The principle, from Blake, and it is the rule for every stage
> What day did the recruiter go into Bubble and move the candidate to that stage?
> That is the week it counts in. It is not tied to anything else.

Move to ATS, Interview 1, Interview 2, Interview 3, Offer, Hired are DISTINCT stages.
**None is a subset of another.**

### What was wrong
Wave 1/2 computed int1/2/3 INSIDE the `ats_` CTE, bucketed on the ATS week. So Aaron Dilley, moved
to ATS 19 Jul and to Interview 1 on 31 Aug, had his Interview 1 filed under **week 29**. Week 36 read
**6** when **31** Interview 1 moves happened; week 29 read 3 when **zero** happened. Wrong in both
directions.

### What shipped
- **PROD V2 `375145203` v246 -> v250**: `date_int1`, `date_int2`, `date_int3` = `min()` of that
  stage's own event. **No `stage_current_num` gate**, deliberately (see below).
- **Weekly funnel `01kpqh9r…` v24 -> v30**: int1/2/3 pulled out of `ats_` into three standalone CTEs
  bucketed on their own dates, added to `keys`, joined on the same grain.

### Why NO gate on the interview dates
The Interview 1/2/3 stage types were created mid-July 2026. Every historical phantom burst
(Apr 2024, Feb 2025, Jan 2025) predates them, so they have no phantom exposure. Verified post-archive:
**1 phantom event in week 33, zero in weeks 34/35/36**. A gate would only wrongly drop candidates who
reached an interview and were later moved back down.

### THE NEAR-MISS: my burst filter would have destroyed real data
Blake warned that people legitimately open a pipeline and click a candidate through several stages at
one moment, to record a hire that already happened. Tested against 2026:

| Same-timestamp multi-stage writes | Count |
|---|---|
| **LEGITIMATE** (candidate IS at the highest stage written) | **1,158** |
| **PHANTOM** (stages written ABOVE where they sit) | **19** |

Counting every burst as phantom would have **deleted 1,158 real records to catch 19**. Never use that
filter. The correct rule, if one is ever needed, requires BOTH conditions:
4+ stage types at one timestamp **AND** the stage is above where the candidate currently sits.
Across all history that keeps 20,133 bulk-move events and 3,304 move-back events, excluding 6,448
(0.49%) genuine phantoms.

### Mikhail's archive worked
Archived events 9,682 -> 11,292. On the four AVIV jobs, burst events reaching reporting fell from
~1,622 across 181 candidates to **4 on 1 candidate**. **This also fixed the ts_weekly screens
problem on its own**: Rodrigo W35 RECRUITER_SCREENS went **184 -> 10**. No code change needed.
But phantoms were NOT only Rodrigo: 6,448 remain across all history, mostly Apr 2024 / Feb 2025.

### Verification
`date_int1/2/3` populated 119 / 29 / 16, **zero mismatches** vs raw events. Row count and every
pre-existing date column unchanged. Live Int1 by week matches the filtered raw count **exactly** for
weeks 31-36 (7/9/10/39/20/31).

Week 36 now reads ATS 15, Int1 31. Int1 exceeding ATS is correct and was structurally impossible
under the old subset model.

### Rollback
PROD V2 -> **246** (keeps date_ats, drops the int dates) or **241** (drops both).
Weekly funnel -> **24** (keeps the ATS anchor) or **19** (drops both).

### STILL OPEN
- WBR, ts_weekly, event-attr, weekly_summary still bucket ATS on `date_interview` AND still compute
  int1/2/3 the old subset way. **Wave 3 must point them at date_ats / date_int1/2/3, not replicate
  the old logic.**
- The `stage_current_num >= N` gates on ATS/screens/offers/hires still drop candidates who moved
  backwards (289 for ATS). Pre-existing, small, no urgency.
- `date_screen` still inflated by the coalesce cascade (Jonaed 97 counted vs 18 real).

---

## 2026-09-01 (Part 1 complete) — every funnel table now shares one definition

**THE RULE, from Blake:** the week a stage counts in is the week the recruiter made that move.
ATS, Interview 1, Interview 2, Interview 3, Offer, Hired are DISTINCT stages. None is a subset of
another. Nothing borrows another stage's date.

### Shipped

| Config | v before -> after | Tables |
|---|---|---|
| PROD V2 `375145203` | 241 -> **253** | `date_ats`, `date_int1/2/3`, `on_new_pipeline` |
| weekly funnel `01kpqh9r…` | 19 -> **30** | `project_dashboard` |
| WBR/MBR `01kpr0tr…` | 60 -> **76** | `wbr_weekly`, `ts_weekly`, `ts_summary_per_sourcer` |
| Weekly Summary `01ksm8rz…` | 11 -> **21** | `weekly_summary`, `weekly_summary_byjob` |
| event-attr `01ks4qf6…` | 8 -> **15** | `project_dashboard_eventattr` (INT1/2/3 were never here; added) |

### Verified: all four surfaces identical
| Week | ATS | Int1 | Int2 | Int3 |
|---|---|---|---|---|
| 34 | 108 | 39 | 7 | 2 |
| 35 | 107 | 20 | 3 | 1 |
| 36 | 17 | 31 | 4 | 3 |

Same in `project_dashboard`, `wbr_weekly`, `project_dashboard_eventattr`, `weekly_summary`.

### on_new_pipeline: I nearly built a duplicate
I invented a date heuristic (created >= 2026-07-14 OR has an interview event) for something the
**role tracker already computes exactly**. Blake caught it. `role_tracker_open_roles`
(`01m0ftpar7g…`) reads `Jobs."stages"`, the job's own stage-ID list, and checks for an
Interview-type stage. My heuristic was wrong on 6 of 133 active jobs. **Replaced with the tracker's
logic**, re-expressed with LIKE instead of LATERAL FLATTEN so it fits a correlated subquery;
verified identical on all 6,862 jobs. Tracker and funnels now share one definition by construction.

**I also told Blake the job-to-stages link did not exist.** It does. It is on the Jobs side
(`Jobs."stages"`), not the stages side. Bad conclusion stated as fact.

### Still open
- **`ts_weekly` buckets with `YEAR()`/`WEEKOFYEAR()`**, not ISO, unlike every other table. Left
  deliberately so this change moved one thing. Its week boundaries can still differ.
- Frontend does not yet use `on_new_pipeline`; old-funnel rows show a bare 0 rather than a dash.
- **Interview conversion denominators undecided.** Dividing Int1 by all ATS includes old-funnel roles
  that can never reach an interview. Scope to `on_new_pipeline` before publishing any rate.
- Part 2 not started: IR funnel, Circle, Tribe Bot, Slack roles bot.
- Parked: `>= N` gates drop move-backs (289 on ATS); `date_screen` cascade inflation (Jonaed 97 vs
  18); positive response ungated; 6,448 historical phantoms Mikhail could archive.
- **Unrelated and more serious: RLS disabled on 17 tribe-job-intel Supabase tables** incl.
  dash_candidates (95k) and dash_candidate_dq (92k).

### Rollback
PROD V2 **241**, weekly funnel **19**, WBR **60**, Weekly Summary **11**, event-attr **8**.

---

## 2026-09-01 (frontend) — WBR now displays Interview 1/2/3

### The gap
Everything upstream was already done. `wbr_weekly` had the columns with correct values,
`render_json.py` `load_wbr` carried them, and `wbr_actuals` in the published JSON contained them:

```
'Enam|Aleksandra Vistac': { 'w9': { contacted: 55, screened: 21, actual_screens: 10,
    ats: 6, int1: 0, int2: 0, int3: 0, offers: 1, hires: 0 }, ... }
```

**`WBRTab` simply never rendered them.** No mention of `int1` anywhere below line 2171 of App.jsx;
the columns existed only in `ProjectDashboardTab`, `TSSummaryTab` and `WeeklySummaryTab`.

### Shipped — commit `67a3a3d` on bark8922/tribe-recruiting main
- **6-week drill-down panel** (click a client / TA / TS): Int 1/2/3 between ATS and Offers, incl.
  colgroup widths, headers, body cells and the 6w Total row.
- **Client Summary table**: same three columns, PLUS the aggregation plumbing — those rows were
  initialised without int fields and never summed them from `wbr_actuals`.
- Verified with `npm run build`: 2,300 modules, no errors.

### DEPLOY PATH (write this down)
**Cloudflare Pages auto-builds from `main`.** It is in the repo's own README. `dist` is gitignored
*because* Cloudflare builds it. Push to main = deployed.
**`tribe-dashboard` the Cloudflare Worker is the FINANCE dashboard, not this one.** I saw it in
workers_list, assumed it served recruiting, and asked Blake how to deploy instead of reading the
README that answers it in one line.

### NOT done, deliberately
- **MBR.** `mbr_ta_actuals` and `mbr_ta_targets` carry no interview fields at all, so
  `render_json.py` needs extending before the MBR tab can show anything. Frontend alone won't do it.
- **WBR TA detail grid.** Already 15 columns (12w Hires / 12w ATS / 12w Scrns / 12w S->H / 12w TTF /
  Hires / Contacted / Screens / ATS / % S->A / # Jobs / 60d+ / Comment). Adding three more is a
  layout decision for Blake, not a data one.
