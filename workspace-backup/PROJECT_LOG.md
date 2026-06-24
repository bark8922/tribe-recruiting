# Recruiting Dashboard — Project Log

Single source of truth for status, decisions, and open items. Survives Cowork resets because it lives in this folder (on Blake's computer) and is backed up to GitHub. Claude updates this at the end of any session where real work happened. Blake can also say "update the log" anytime.

Last updated: 2026-06-24

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
- **Keboola Data App PoC (Time to Hire):** live. First in-Keboola data app, reads `out.c-TTH---tth-jobs.tth_jobs` from Snowflake (input mapping → `/data/in/tables/tth_jobs.csv`), password-gated, auto-sleeps 15 min. Code: GitHub `bark8922/tribe-tth-app` (private), Flask + `keboola-config/` layout. App config `01kvq9zgsrkrt5yevw6djvqz0f` ("TTH Test"), URL `tth-test-985851138.hub.eu-central-1.keboola.com`. Kai chat is wired in code (`kai_chat.py`) but OFF until a master token is added. Purpose: prove the Kai-powered stakeholder-app path from the 2026-06-22 Keboola renewal call before building more.

## In flight / open decisions

| Item | Status | Blocked on |
|---|---|---|
| Cortex Analyst pilot | Scoped 2026-06-12, not started | Blake go/no-go: green-light free Snowflake trial, pick 2-3 pilot users, confirm aggregates-only data |
| CSV export buttons | Scoped 2026-06-11, nothing built | Blake decision: which tables in Phase 1 vs all 25 at once |
| Sourcing dashboard polish | Live, minor items | Validate post-Zelimir-fix refresh; ask Gustavo to sanity-check Phase 2 cost numbers |

## Decisions locked (do not relitigate without reason)

- Sourcing methodology v1.5: count work during Bench/Internal, drop onboarding contacts when Bench window ≤30 days, Sanja Pavlovikj excluded entirely, <5 contacts/quarter dropped as noise, half-open `[start, end)` division intervals.
- Tribe internal jobs (Tribe.xyz, IR) included; test clients excluded; archived jobs included.
- Cross-client sourcing work excluded (counts as TA work, not internal sourcing).

## Known gotchas

- App.jsx is huge (~4,500 lines). Edit via Python search/replace, run `npx vite build` before every push.
- Local folder can lag GitHub — clone the repo fresh and diff before copying anything.
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

### 2026-06-22
- Keboola renewal call with Anna Duskova (rep). Contract renews 2026-08-31; legacy discounted rate ($250/mo license vs $1k list, 600 credits/mo, using ~300, ~4,000 banked credits that only survive if renewing at same tier or higher; min 3 user licenses). New features pitched: Kai (in-platform AI assistant, billed in credits) and Python/JS Data Apps with Kai embedded for stakeholders (~$230/mo per always-on app, sleeps when idle) + native semantic layer. Action items: loop in Martin, Anna sends deck + tier pricing, follow up in ~1 month.
- Corrected the stale architecture in PROJECT_LOG + the `tribe-recruiting-dashboard` skill: pipeline is Keboola/Snowflake (Flows + SQL transforms), NOT local DuckDB/n8n. Keboola STAYS; only PBI dies 8/31.
- Built + shipped the Time to Hire Data App PoC (see "Live now"). Deploy gotcha solved: `uv sync` fails on flat multi-script repos with default build-system; fix = `[tool.uv] package = false` in pyproject.toml. Keboola Python/JS apps need the `keboola-config/` layout (nginx/supervisord/setup.sh) + pyproject.toml, app on port 5000, nginx 8888.
- Next: add Kai (needs a master token, ideally a dedicated service admin user e.g. kai-app@tribe.xyz); then clone the pattern to the Project Dashboard tab for the Martin demo.
- **"New Project Health" tab stale-data fix.** Tab is leadership-only (Blake + Jacopo, `tribe_ph=1` cookie), reads `data.new_project_health` (KR2 = first move to ATS within 4 business days; KR3 = Actual Screen→ATS ≥60% by week 4). Tab/SQL/renderer were all fine — the Snowflake table `out.c-WBRMBR-weekly-aggregations.new_project_health` was fresh (145 rows). Root cause: the Keboola render+push component was failing its final `git push` with GitHub **403 (PAT lost write access)** → `rc=128`. When that push fails the ENTIRE deployed dataset freezes at the last good push; this tab is just what got noticed. Blake rotated the PAT; confirmed fixed via job `999962733` (11:17 CET, commit `573a3e7`, Circle dispatch HTTP 204).
- **Plaintext churn fix (commit `4118ced`).** Pipeline was committing the 52MB plaintext `dashboard_data_snowflake.json` every run (3x/day, ~1.16M line insert/delete each). Now `keboola_entry.py` gzips the rendered JSON and commits `public/dashboard_data_snowflake.json.gz` (~2.3MB binary) directly; build dropped `gzip-data.mjs` (now just `vite build`); plaintext removed from the tree + gitignored. Verified with a local `npm run build` (dist gz valid, 145 rows).
- Open follow-ups: git history still carries old 52MB plaintext blobs (one-time `git filter-repo`/BFG could reclaim space); committing the binary gz still grows history ~2.3MB/run; Ashby threw `sync_token_expired` on ~27 jobs during extraction (separate; Ashby-fed stages may lag).
- **Side-effect of the gzip migration: Circle + team_leads Actions broke (afternoon, fixed).** Switching `keboola_entry.py` to commit only `public/dashboard_data_snowflake.json.gz` broke two downstream consumers that still read the old uncompressed `recr