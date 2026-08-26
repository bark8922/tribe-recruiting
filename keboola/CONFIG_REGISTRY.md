# Keboola config registry — project 855 (Tribe.XYZ)

Baseline established 2026-08-25. This file is the diff target for the weekly drift check.
Anything running in project 855 that is not in this file is drift and gets flagged.

Rules:
- New config → add a row with a one-line justification for its cadence, or cut it.
- Changing a cadence → update the row and the reason.
- Accepting a flagged item → add it here. That is what stops the weekly report re-raising it.
- `cr/mo` is billed credits per month from the telemetry export, refreshed each month.

Contract in force: $12,000/yr, 300 credits/month pooled to **3,600 per Term**, 0.2 TB, 1 project,
3 users. Overage: 100-credit blocks at $300, manual order amendment through Anna Dušková
(anna.duskova@keboola.com), not self-serve. Past that, $4/credit under clause 3.2.
Rollover credits do not carry over on a tier decrease — the ~4,000 banked were forfeited.

---

## Live pipeline

### Flows and orchestrators

| ID | Name | Cadence | Why this cadence | cr/mo |
|---|---|---|---|---|
| `118392817` | 4x daily - NEW (orchestrator) | 08:15, 10:15, 13:15, 15:45 Prague | Bubble incremental pull + PROD V2 + rung/tracker chain | 0 (wrapper) |
| `01kpqyq1pz6qpmk7m9s4qx8gmg` | Recruiting dashboard build | 08:40, 10:40, 13:40, 16:10 Prague | Runs 25 min after the orchestrator so it reads fresh reporting-v2 | 0 (wrapper) |
| `01kt8x8jkxb59sd07tvd96bw7b` | Bubble Full — daily reference refresh | 06:00 daily | Split out 2026-06-04. Reference tables are slow-changing. Cut ~11 cr/mo | 0 (wrapper) |
| `01ktr6gpq25h0eweds1e012gge` | Daily Geocoding | 1x daily | Split out 2026-06-09. Country distribution does not move intraday. Cut ~9 cr/mo | 0 (wrapper) |
| `01kxn10awrwf4pda3kz42zhqp6` | Candidate Finder — daily refresh | 10:20 daily | Split out 2026-07-16. Historical search tool, does not need 4x | 0 (wrapper) |
| `01m0hqmdvtt98sseg7jnb1n547` | Candidate DQ by Stage — daily refresh | 09:00 daily | Feeds Tribe Bot Supabase load at 10:00. Once daily by design | 0 (wrapper) |
| `270972256` | 1x monthly - kbc telemetry | monthly | Pulls the billing export. **This is the source of every credit number here** | 0 (wrapper) |

### Extractors

| ID | Name | Cadence | Consumer | cr/mo |
|---|---|---|---|---|
| `122527414` | Bubble Incremental | 4x/day in flow B | PROD V2. Hot tables (Events, Candidate, Talent) | 7.1 |
| `122491135` | Bubble Full | 1x/day | PROD V2. ~28 reference tables | 7.2 |
| `01kpr3tek8ezs48pg02e60jdpe` | WBR Target sheet (Andy) | 4x/day in flow A | render_json. Andy edits it during the day | 2.2 |
| `554463170` | Geocoding — talent location | 1x/day | `talent location` transform → PROD V2 | 2.3 |
| `270972036` | Telemetry data | monthly | The credit numbers in this file | 0.2 |

### Transformations

| ID | Name | Cadence | Output → consumer | cr/mo |
|---|---|---|---|---|
| `375145203` | **[PROD] Data preparation V2** | 4x/day | 16 tables → `out.c-reporting-v2.*` → everything downstream | **165.7** |
| `01kpr0tr0dt5ryf96a5zk85bx7` | WBR/MBR weekly aggregations | 4x/day | 6 tables → render_json + sourcing dashboard | 52.4 |
| `01ksm8rz0qfrhgzekke65bkd28` | Weekly Summary, PBI port | 4x/day | `weekly_summary`, `weekly_summary_byjob` → render_json inputs 25, 26 | 15.9 |
| `01kpqh9r7g2z66c8vvdr5d87xd` | Project Dashboard, weekly funnel | 4x/day | → render_json, Project Dashboard tab | 15.0 |
| `01ks4qf6zate4m7f0cxng2hnyy` | Project Dashboard, event-attr | 4x/day | → render_json, attribution toggle (non-default) | 9.5 |
| `01kpztmw7d7911kbmyrdf7gcq5` | TTH jobs aggregation | 4x/day | → render_json, Time to Hire tab | 9.4 |
| `01kpqharhz3seww52sms915216` | Project Dashboard, hires drill-down | 4x/day | → render_json, hires panel | 7.9 |
| `01kpqxgczrvb92e95y6dh7zxmh` | MBR Contacted, event-based | 4x/day | → render_json, overrides Contacted in mbr_ta_actuals | 7.5 |
| `01kvzgpgwh38awepha7eey08pe` | Candidate Finder | 1x/day | → finder_data.json.gz | 3.5 |
| `555826655` | talent location | 1x/day | → geocoding extractor | 3.1 |
| `01ky9xkbr9fzswz7meddr73agn` | Candidate stage rungs (Interview 1/2/3) | 4x/day | → role_tracker_summary | 0.9 |
| `01kzdtarb81nskrjdn4jr2rrex` | Role Tracker summary | 4x/day | → Role Tracker GitHub push | <0.5 |
| `01m0ftpar7gtbqdjzpe0wwjay4` | Role Tracker open-role coverage | 4x/day | → Role Tracker Coverage tab (added 2026-08-20) | <0.5 |
| `01m0hpfwrz98tvaxn4km3y8zx7` | Candidate DQ by Stage (2024+) | 1x/day | → Tribe Bot push → Supabase (added 2026-08-21) | <0.5 |
| `01ktkfs2j50hre305cv1w1kqpg` | Recruitee static rebuild | **IDLE — must never run on a schedule** | One-shot, ran 2026-06-08. Outputs are frozen and deterministic. **Flag immediately if it appears in a job list** | 0 |

### Applications (writers to GitHub)

| ID | Name | Cadence | Target | cr/mo |
|---|---|---|---|---|
| `01kpr863ypqr5pt74wms8fdj67` | Dashboard refresh, render_json + push | 4x/day | bark8922/tribe-recruiting → recruiting.tribe.xyz | 7.2 |
| `01kt1ns5mq87k9tmgmtapf8bhm` | Sourcing Dashboard push | 4x/day | bark8922/tribe-sourcing → tribe-sourcing.pages.dev | 1.0 |
| `01kxqrgn4mw0j6pyvkx3r66w9w` | AI Call Tool usage push | 4x/day | bark8922/tribe-calls | 0.2 |
| `01kzdtkvn7ddt35dgbn2sfz7m0` | Role Tracker push | 4x/day | tribe-recruiting/role-tracker/data.json | <0.5 |
| `01m0htjpz4gkmzf2vw7rbjzrhh` | Tribe Bot candidate DQ push | 1x/day | tribe-recruiting/tribe-bot/candidate_dq.json.gz → Supabase | <0.5 |
| `270973275` | Telemetry data writer | monthly | Storage | 0 |

---

## External consumers (not visible from inside Keboola)

The zombie check must not flag these. Their tables look unconsumed from a config search
because the reader lives outside Keboola.

| Table | Read by |
|---|---|
| `out.c-Weekly-Summary---PBI-Weekly-Progress-port.weekly_summary` | render_json input tables 25, 26 |
| `out.c-Weekly-Summary---PBI-Weekly-Progress-port.weekly_summary_byjob` | render_json |
| `out.c-Candidate-DQ-by-Stage-2026.candidate_dq_by_stage` | Onyx "Tribe Bot" loader on the DigitalOcean droplet → Supabase |
| `out.c-Candidate-Finder.candidate_finder` | render_json → finder_data.json.gz |
| `out.c-recruitee-static.*` | PROD V2 input mapping tables 41–46 (except `recruitee_stage`, which is genuinely unused) |

---

## Accepted and closed — do not re-raise

| Date | Item | Decision |
|---|---|---|
| 2026-06-04 | Bubble Full at 3x/day | Split to 1x/day. Saved ~11 cr/mo |
| 2026-06-09 | Geocoding at 4x/day | Split to 1x/day. Saved ~9 cr/mo |
| 2026-06-11 | Main flows at 5x/day | Reduced to 4x/day |
| 2026-07-16 | Candidate Finder at 4x/day | Split to 1x/day |
| 2026-08-19 | Contract tier | Dropped to $12k / 300 cr. Rollover credits forfeited, knowingly |
| 2026-08-25 | `weekly_summary` looked orphaned | Not a zombie. render_json reads it. Do not flag again |
| 2026-08-25 | PROD V2 reads `out.c-recruitee-static` | Phase 2 was completed. Do not re-propose it |
| 2026-08-25 | **Cut 1, PROD V2 v233-v237** | SHIPPED and verified. Removed the unused `recruitee_stage` input mapping, two dead LEFT JOINs (`bubble_Conditional as con`, `bubble_Jobs as j`), and two no-op ORDER BY clauses one of which sorted 8M rows. Verified live 2026-08-26: 46 inputs, 16 outputs, zero duplicate keys, `automation_step_con` still populated |
| 2026-08-25 | Cut 2, reducing funnel-table cadence | **REJECTED by Blake.** Funnel events land all working day (4,787 events at 07:00 UTC, 7,964 at 10:00, 7,142 at 13:00). Dashboards must stay current. Do not re-propose |
| 2026-08-26 | **PROD V2 v238, `date_contacted` max to min** | Made by a separate session, not the spend work. In 552 cases the recorded "contact date" was the date the candidate was **disqualified**. 10,446 candidates corrected, annual totals moved <0.1%. Recruiter and sourcer dashboards now agree. Keep |
| 2026-08-26 | Talent dedup columns | Confirmed dead in Power BI (hidden, zero measures, zero page refs) and absent from all four GitHub repos. No decision taken on removal |

---

## HAZARD — two sessions writing to the same config

On 2026-08-25/26 two Cowork sessions edited `375145203` without knowing about each other. Symptoms: an edit that appeared to vanish, several hours lost chasing a phantom bug, and a real production change briefly misattributed to the wrong author.

**Before editing this config, always run `kbc_list_config_versions` and check nothing unexpected has landed.** The weekly waste-review task now does this automatically as Check 1.

---

## Open

| Item | Saving | Status |
|---|---|---|
| Cut 3 Piece 2a — collapse the six date-cascade UPDATEs into one | ~25 cr/mo | **TESTED, NOT SHIPPED.** Proven byte-identical to the unchanged code on a parallel copy; 913s vs 1,059s and 1,134s unchanged |
| Cut 3 Piece 2b — collapse the other 15 candidate_stage UPDATEs | not measured | not started. Has real subtleties: one joins on talent_id with a correlated date bound, two write the same column in sequence, several deliberately write NULLs |
| Cut 3 Piece 3 — collapse the 12 final_event UPDATEs | not measured | not started |
| Cut 3 Piece 1 — talent dedup, 8 self-joins over 1.6M rows | largest single item | not started. Delete or convert, undecided |
| Reserve — drop the 4th daily cycle | −75 cr/mo | held. Costs real freshness, Blake's call not a technical one |
| Housekeeping — delete 14 idle configs | 0 cr | open |

Parallel test config `01m0yfxnc5g1cnq62rq5jgva4b` ("ZZ REWRITE TEST") writes to `out.c-prodv2-rewrite-test`, is not scheduled and is in no flow. Delete it and the scratch bucket when the rewrite is finished or abandoned.

## Watch list

- **Candidate revivals.** `date_contacted` now uses the first contact event. If a candidate is pulled BACK to Contacted after advancing or being disqualified, the re-contact is credited to the original date and vanishes from the current week. 659 historical cases (387 in 2020, 99 in 2024, **1 in 2026**). Monitored weekly. Fix is specified and tested if volume appears: use the first contact after the most recent disqualification or advancement. Adding a candidate to a NEW pipeline is safe and correct (proven across 159,669 people).
- **`ts_summary_per_sourcer` erases history.** Only counts work on jobs still open. 100% of 2023 and 2024 jobs are archived, so those years read near-zero. Pre-existing, not caused by any 2026-08 change. No decision taken.
- **Built but read by nothing**: `talent_position` (6.87M rows), `talent_employer` (432K rows, loaded by PBI with zero relationships/measures/page refs).
- **Storage**: 0.13 TB against a 0.2 TB cap. Verify what that counts, since live buckets total under 5 GB.
- **Users**: contract caps at 3.
- **Telemetry lag**: `kbc_job` lands at the start of each month. Current-month credits are always estimated from the live Jobs API.
