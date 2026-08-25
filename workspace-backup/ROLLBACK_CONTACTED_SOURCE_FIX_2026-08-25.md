# ROLLBACK RECORD — Contacted fixed at SOURCE, 2026-08-25

**One-line change in `candidate_stage`. This file is how you undo it.**

Third and final piece of the mutable-date work. See also `ROLLBACK_ATS_FIX_2026-08-24.md`
and `ROLLBACK_CONTACTED_FIX_2026-08-24.md`.

---

## 1. What changed

`candidate_stage.date_contacted` is built by us in Keboola config **`375145203`
"[PROD] Data preparation V2"**, code block **`part 1 - bubble data`**, as `max()` of the
candidate's Contacted events. Because it is a `max()`, any NEW Contacted event moves the
candidate out of the week they were already counted in. Changed `max` to `min`.

## 2. THE EDIT (and the trap)

`max(t."date_created")` appears **7 times** in that script — it also builds `date_lnkdin_viewed`,
`date_screen_actual`, `date_screen`, `date_interview`, `date_offer` and `date_hired`.
**A plain find-and-replace on `max(` will corrupt all of them.**

Use this exact search string, verified to occur exactly once in the whole configuration:

```
set c."date_contacted" = (select max(t."date_created")::DATE
```

Replace with:

```
set c."date_contacted" = (select min(t."date_created")::DATE
```

Single space either side of `=`. No trailing space after `::DATE`.

**TO ROLL BACK:** reverse it — change that one `min` back to `max` — or restore the previous
config version in Keboola. Then re-run the Flow.

## 3. Full original statement, verbatim

```sql
update "final_candidate_stage_bubble" as c
set c."date_contacted" = (select max(t."date_created")::DATE
                            from "final_event" as t
                           where 1=1
                            and c."candidate_id"=t."candidate_id"
                            and t."moved_to_stageType"='Contacted'
                            and not (t."moved_to_stage"='Responded') -- Responded is not Contacted
                            and c."stage_current_num">=1
                         );
```

The applicant backfill further down is **not** changed and still protects inbound applicants:

```sql
update "final_candidate_stage_bubble" as c set c."date_contacted"  = iff(c."date_screen" is not NULL and c."date_contacted" is NULL, c."date_screen", c."date_contacted");
```

## 4. BEFORE numbers, captured 2026-08-25

| Table / metric | Before |
|---|---|
| `ts_weekly` CONTACTED 2026 | **75,358** |
| `ts_summary_per_sourcer` CONTACTED | **32,044** |
| `ts_summary_per_sourcer` REACTED | **7,625** |
| `ts_summary_per_sourcer` JOBS | **1,115** |
| `ts_summary_per_sourcer` ROWS | **1,597** |
| `sourcing_dashboard_per_sourcer` CONTACTED | **123,769** |
| `sourcing_dashboard_per_sourcer` ROWS | **1,074** |
| `sourcing_wbr_comments` CONTACTED | **131,440** |
| `ir_funnel_jobweek` CONTACTED | **5,243** |
| `ir_sourced_jobweek` CONTACTED | **5,243** |
| `sourcing_int_vs_ext` CONTACTED | **140,426** |
| `ts_summary_by_client` REACTED | **95,150** |
| `tth_jobs` ROWS | **2,283** |
| *already fixed* `wbr_weekly` CONTACTED 2026 | 75,763 |
| *already fixed* `weekly_funnel` CONTACTED 2026 | 73,416 |
| *already fixed* `event_attr` CONTACTED 2026 | 73,387 |

The three "already fixed" tables read `COALESCE(first_contact_event, date_contacted)`, so the
source change makes their COALESCE redundant rather than double-applying. They should move only
by ordinary new activity.

## 5. Measured impact before applying

| Check | Result |
|---|---|
| Candidates whose date moves | 10,416 of ~1.4M (**0.7%**) |
| Candidates with no contact event (applicants, keep backfill) | 30,945 |
| TTH time-to-fill flags that flip | **4 job-years** (3 in 2024, 1 in 2026) |
| Candidates dropping below the 2024 floor | **zero** |
| Cross-year moves | 199 (197 with a sourcer) |

Movement distribution for candidates with a sourcer: 1,032,701 no move; 2,693 move 1-7 days;
1,605 move 8-31 days; 2,318 move 1-3 months; 3,661 move over 3 months.

## 6. The two things to check after it runs

1. **`sourcing_dashboard_per_sourcer`** — joins the contact date against each sourcer's
   Bench/Internal division periods (`dp.start_date <= dc AND dc < dp.end_date`). ~6,000 candidates
   move by more than a month, so some may land in a different period. **This was NOT measurable in
   advance** without replicating that CTE. Watch CONTACTED (123,769) and ROWS (1,074).
2. **`ts_summary_per_sourcer`** — `YEAROFWEEKISO(dc)` sits inside the `current_roster` join
   predicate, so a cross-year move can add or drop a sourcer row entirely. 197 candidates cross a
   year. Watch ROWS (1,597) and JOBS (1,115).

If either looks wrong, reverse the one word and re-run.

## 7. Why the source, not the nine downstream places

Each downstream fix needs a new CTE plus a COALESCE plus a LEFT JOIN — roughly eight hand edits
across four transforms, and it would still leave five other consumers drifting. One word at source
fixes all nine permanently and cannot drift apart again. It also handles inbound applicants for
free via the existing cascade, which each downstream fix otherwise has to hand-code.
