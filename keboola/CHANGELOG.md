# Keboola project 855 — change log

Every change to `[PROD] Data preparation V2` (component `keboola.snowflake-transformation`,
config `375145203`) made during the August 2026 credit optimisation, with evidence and rollback.

Rollback for any of these is one call:
`kbc_rollback_config(component_id="keboola.snowflake-transformation", configuration_id="375145203", version=<N>)`
Rollback creates a new version and does not destroy history.

Files in this folder:

| File | What it is |
|---|---|
| `prodv2_FULL_CONFIG_v233_before.json` | Complete config as fetched at v233, before the SQL edits |
| `prodv2_CONFIG_v234_proposed.json` | The same config with items 2 to 4 applied, ready to paste |
| `../prodv2_storage_BACKUP_v232.json` | Storage mapping only, at v232 (pre item 1) |
| `../prodv2_storage_PROPOSED_v233.json` | Storage mapping only, at v233 (post item 1) |

---

## v233 — APPLIED 2026-08-25 14:04 CEST

**Change:** removed input table mapping `out.c-recruitee-static.recruitee_stage` → `RECRUITEE_STAGE`.
Input mappings went 47 → 46. No SQL changed. No output mapping changed.

**Applied by:** token "Cowork-temp" (not this session; my write at 13:17 failed on a 401).

**Evidence it was safe:**

- Zero references to `recruitee_stage` in any casing or quoting across all 1,117 lines of SQL.
  Only 8 occurrences of the string "recruitee" exist and all map to the other five tables.
- Keboola's own config index confirmed it: config 375145203 matched only on
  `storage.input.tables[46].source` and `.destination`, never on `codes[*].script`.
  Contrast the static rebuild transform, which does match on `codes[0].script`.
- The table holds 1 row, all fields null except a stray `job_id`.
- Its source, `in.c-ex-recruitee-current.offers_stages`, has held 1 row since the Recruitee
  extractor stopped on 2023-06-29.
- Its purpose was already spent. It was the lookup that translated per-job Recruitee stage names
  into six standard categories. That translation is already materialised in
  `recruitee_events.stage_to_category` (82,984 rows: candidate 72,681, phone_screen 7,691,
  interview 1,841, evaluation 476, offer 162, hire 133), which Part 1 does read.

**How it got there:** staged on 2026-06-09 as the sibling of `recruitee_events` during the Phase 2
migration, which had already been rolled back once that day. Version history:
v212 "EMERGENCY ROLLBACK: Re-add Part 0 (Recruitee staging)", v217 "Update input table
out.c-recruitee-static.recruitee_stage", v218 "Phase 2 Step 3a (retry after case-sensitivity fix):
Remove Part 0. recruitee_stage and recruitee_events now staged with UPPERCASE destinations matching
Part 1's unquoted references." Only one of the pair was ever referenced.

**Verification after the fact:** diffed live v233 against the v232 backup. Input mappings 47 → 46,
exactly one removed and nothing added. All 16 output mappings byte-identical. All three SQL code
steps byte-identical once whitespace is normalised (the two connectors return `script` as a string
vs a list, which changes blank-line handling but no content).

**Rollback:** `version=232`. Or re-add one input mapping: source
`out.c-recruitee-static.recruitee_stage`, destination `RECRUITEE_STAGE`. The table itself and the
transform that builds it were left untouched in Storage.

### v233 post-deployment verification — 2026-08-25 15:48 CEST

First production run under v233. Confirmed from the job record itself, not inferred.

Job `1015179370`, **success**, 807 seconds, `configVersion: 233`.

- **46 input tables loaded** (was 47). Five Recruitee-sourced inputs present:
  `final_candidate_recruitee`, `final_candidate_stage_recruitee`, `final_event_recruitee`,
  `final_talent_recruitee`, `recruitee_events`. `recruitee_stage` absent.
- **All 16 output tables written** to `out.c-reporting-v2`. Nothing missing.
- Full chain green: Bubble incremental 116s → PROD V2 807s → stage rungs 43s →
  Role Tracker summary 76s + coverage 78s → tracker GitHub push 210s.

**Runtime:** 2026-08-25 runs were 845s, 935s, 759s (all v232) and 807s (v233). The v233 run sits
mid-band. As predicted, a single item's saving is not separable from run-to-run variance of roughly
176s. Judge Cut 1 as a whole over a week, not on one run.

**Storage integrity:** bucket count unchanged at 24 (10 input, 14 output), identical to the
pre-change inventory. No new or orphaned buckets.

### Unrelated finding on the same day — resolved, no action

One errored job appeared in the 2026-08-25 log, the only error across the whole project that day:

```
job 1015168889 | config 01m0wdaax6mh9y66fn6ssx5kta | token "Cowork-temp"
error (user) | 1 second | "Configuration 01m0wdaax6mh9y66fn6ssx5kta not found"
```

Cause: a race between a throwaway test config and its own cleanup. The config was real and is now
deleted, named **"ZZ worker test - auto-deleted"**, created 14:11:29, job started 14:11:32. The
delete won.

Four such configs were created and deleted that afternoon while the replacement Keboola connector
was being set up: `01m0wbc24a1m20bw00jaf9de3y` "ZZ MCP write test - safe to delete" (13:37),
`01m0wdaax6mh9y66fn6ssx5kta` "ZZ worker test - auto-deleted" (14:11),
`01m0weezgmsa6azt8wjpwje0x3` "ZZ deployed-worker check" (14:31),
`01m0wengdvn30gy3mtd3r7abxa` "ZZ connector check - auto-deleted" (14:35).

Impact: zero credits, no Storage writes, no data touched. All four are soft-deleted and sit in the
Keboola trash. Purging them is optional tidy-up; they run nothing.

**Note for the drift check:** these are the expected signature of connector setup. If `ZZ`-prefixed
configs appear again outside a known setup window, that is worth a question.

---

## Connector and token notes — 2026-08-25

Two Keboola connectors are in play and they do not have the same reach.

| | Official connector (`mcp__58bcbaa7-…`) | API connector (`kbc_*`) |
|---|---|---|
| Token | n/a (OAuth) | `11797764` "API Claude -token", never expires, unrestricted |
| Storage / management API | works | works |
| `query_data` (Snowflake) | works | n/a |
| Jobs API (`queue.eu-central-1`) | **401 since 2026-08-25** | works |
| Config writes (`ai.eu-central-1`) | **401 since 2026-08-25** | works |

Use the official connector for `query_data`, `get_flows`, `get_buckets`, `get_tables` and `search`.
Use the `kbc_*` connector for anything touching jobs or writes.

**Do not delete token `11797764`.** The working connector runs on it. The `Cowork-temp` token
(`11797376`) was a separate short-lived key, since deleted by Blake; deleting it did not affect
v233, which was already verified live by the 15:48 run.

**Write-path constraint — tested, not assumed.** `kbc_update_config` replaces the configuration
object wholesale. Verified 2026-08-25 16:56 on a throwaway config (`01m0wps0nb5gxzsbhs8hm7588w`,
"ZZ merge-semantics probe", since deleted): created it with both `storage` and two code steps, then
sent an update containing only `parameters` with only step A. Result — storage was wiped and step B
was gone. There is no deep merge and no partial-update path.

Consequence: any SQL edit to PROD V2, however small, requires sending all 61,500 minified characters
of the configuration, which for an agent means reproducing the entire production SQL inline. That is
not a reliable operation and is the wrong trade for a change worth single-digit credits.

The correct tool is the official connector's granular `str_replace` on `update_sql_transformation`,
which needs only the search and replace strings. It is blocked solely by the `ai.eu-central-1` 401.

**Fix the 401 and this becomes a one-line operation.** Until then, small SQL edits belong in the
Keboola UI.

---

## v234–v237 — APPLIED 2026-08-25 17:12 CEST

Applied as four separate versions by token "API Claude -token":

| Version | Change |
|---|---|
| 234 | Spend optimisation 1/4: drop the dead `bubble_Conditional` join (alias `con` unused) |
| 235 | Spend optimisation 2/4: drop the dead `bubble_Jobs` join (alias `j` unused in that statement) |
| 236 | Spend optimisation 3/4: drop the pointless ORDER BY on a CREATE TABLE (talent positions) |
| 237 | Spend optimisation 4/4: drop the pointless ORDER BY 3,2,1 in `part 4 - Andy` |

### Verification, live v237 against both references

**Against the prepared target** (`prodv2_CONFIG_v234_proposed.json`, machine-generated from v233):
all three SQL code steps **MATCH**. The deployed config is exactly what was intended, not an
approximation of it.

**Against the v233 baseline** — only the four intended lines differ, nothing else:

```
[part 1 - bubble data]
  -order by "talent_id", "position_worked_from" desc;
  +;
  -left join "bubble_Conditional" as con on sub."Conditional"=con."bubbleinternal_id"
  -left join "bubble_Jobs" as j on c."Job"=j."bubbleinternal_id"
[part 3 - final tables]  unchanged
[part 4 - Andy]
  -order by 3, 2, 1
```

**Storage:** 46 input tables, 16 output tables, byte-identical to v233. `recruitee_stage` absent.
No mapping was touched by these edits.

**Not yet exercised.** v237 was written at 17:12 and the last PROD V2 run of the day finished at
16:01 under v233. Job query for config 375145203 after 15:00 UTC returns zero rows. The first run
under v237 is the scheduled 08:15 cycle tomorrow.

**Rollback:** `version=233` restores all four lines and is a single call.

---

## Original edit specification (kept for reference)

Three edits, four lines. All four verified to occur exactly once in the config.

### Edit 1 — drop the dead `con` join

Code step `part 1 - bubble data`. Delete this line in full:

```sql
left join "bubble_Conditional" as con on sub."Conditional"=con."bubbleinternal_id"
```

Keep the line immediately below it, which joins the same table as `pcon` and IS used
(`ifnull(pcon."type", '?') as "automation_step_con"`).

### Edit 2 — drop the dead `j` join

Code step `part 1 - bubble data`, inside `create or replace table "final_candidate_bubble"`.
Delete this line in full:

```sql
left join "bubble_Jobs" as j on c."Job"=j."bubbleinternal_id"
```

### Edit 3 — drop the 8M-row sort

Code step `part 1 - bubble data`, end of the `final_talent_position` statement. Change:

```sql
order by "talent_id", "position_worked_from" desc;
```

to just:

```sql
;
```

### Edit 4 — drop the analytic sort

Code step `part 4 - Andy`. Delete this line, keeping the `;` on the line after:

```sql
order by 3, 2, 1
```

### Evidence these are safe

**The two joins contribute nothing and cannot fan out.**

- `con.` appears exactly once in the whole config, in its own ON clause. `j."` likewise, within a
  statement spanning the full `final_candidate_bubble` select. Neither alias is projected.
- Both right-hand tables declare `bubbleinternal_id` as **primary key** in Keboola Storage, so the
  extractor deduplicates on load and duplicates cannot appear later.
- Tested on the full live data, not a sample:

  | Join | Rows without | Rows with | Difference |
  |---|---|---|---|
  | `bubble_Jobs` as `j` | 1,378,344 | 1,378,344 | **0** |
  | `bubble_Conditional` as `con` | 15,139,133 | 15,139,133 | **0** |

- The risk runs the other way. Since nothing reads a column from either alias, if a duplicate ever
  did appear the join would *create* duplicate candidate and event rows. Removing it removes a
  failure mode rather than adding one.

**The two sorts have no effect on stored data.**

- No `LIMIT`, `TOP` or `FETCH FIRST` exists anywhere in the config, so neither `ORDER BY` can change
  which rows are stored.
- Row order does not survive export to Storage. Verified by reading
  `out.c-reporting-v2.talent_position` with no ORDER BY: the first 30 `talent_id` values come back
  unsorted (1619526721884…, 1616498524619…, 1686823128120…, 1647442585751…). The sort is already
  being discarded at the door.
- Ordering is preserved in a **column**, not in row order. `position_order_desc` is computed by a
  separate window function that this change does not touch.
- The only live consumer is the Candidate Finder transformation (`01kvzgpgwh38awepha7eey08pe`),
  which is order-independent:
  ```sql
  WITH "cur" AS (SELECT "talent_id" AS "tid", MAX("employer_id") AS "eid"
                 FROM "KEBOOLA_855"."out.c-reporting-v2"."talent_position"
                 WHERE "position_order_desc"=1 GROUP BY "talent_id")
  ```
  The other two references are dead: the Data Gateway config row (idle since 2026-06-09) and a dev
  sandbox from 2023.
- `final_talent_position` is read once inside PROD V2 itself, via
  `from (select distinct "employer_id" from "final_talent_position")`. A DISTINCT is indifferent to
  order. `analytic` is read by nothing inside the config and goes straight to output.

**Rollback:** `version=233`.

---

## Not changed, deliberately

- **Column pruning on the fat input tables.** Floated then withdrawn. On `bubble_Event` only 8 of
  35 columns are provably unreferenced, and that is a naive text search that breaks if a column
  name is shared with another table. The largest column, `Content`, is used in a WHERE clause.
- **Cadence reductions on the funnel tables.** Proposed then withdrawn after testing. Funnel events
  land throughout the working day (4,787 events at 07:00 UTC, 7,964 at 10:00, 7,142 at 13:00,
  3,175 at 15:00 in August 2026), so Weekly Summary, Project Dashboard funnel, event-attr,
  MBR Contacted and WBR/MBR genuinely do move between runs and stay at 4x/day.
- **Dropping the fourth daily cycle.** Held in reserve. Worth ~75 credits/month but it is the only
  option that costs real dashboard freshness, and the 16:10 run feeds the 16:30 pulse.

## Baseline for measuring the effect

PROD V2 runs on 2026-08-25 before any change: 845s, 935s, 759s (all successful, all under v232).
Trailing weekly average around 820s at 1.36 credits per run, 28 runs/week.
Run-to-run variance is already about 176s wide, so a single item's saving will not be visible.
Judge the whole of Cut 1 over a week, not a run.
