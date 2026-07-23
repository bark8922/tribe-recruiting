# LinkedIn Views Attribution — Fix Spec

**Status: DRAFT for review. Nothing has been changed in Keboola, Snowflake, GitHub, or the dashboard. This document describes a proposed change only.**
Date: 2026-07-23

---

## The problem in one line

A LinkedIn profile view gets counted for a different person depending on which tab you are on, so the sourcer who actually did the viewing can read 0 while the role owner reads the full number.

## Live proof (Reaktor, Andrea Akovic, weeks 28-30 2026)

Andrea personally visited **396** Reaktor profiles (144 + 156 + 96). Simon Siew owns the Reaktor roles and personally visited essentially none. Yet:

- Project dashboard, default view: the 396 shows under **Simon**, and **Andrea reads 0**.
- Weekly summary, sourcer view: the 396 correctly shows under **Andrea**.

Same 396 views, two different people, purely depending on the tab.

## The one rule

A LinkedIn view belongs to the person who did the clicking. In the data that is the `who_created_event` field. This applies on every tab. The role and client keep their totals by summing all views on the role, no matter who did them. Person level answers "how many did Andrea do." Role level answers "how many happened on Reaktor." Same events, two lenses, no double count.

## What does NOT change

- **Contacted, reacted, screens, and the rest.** They are already consistent. The sourcer view credits the sourcer, the owner view credits the owner. Verified: Andrea's 157 Reaktor contacts show under her on both the project dashboard and the weekly summary sourcer view.
- **Role and client totals.** A view is one event. We are only relabeling who did it. Reaktor stays at 396 either way.
- **On-sheet sourcers like Andrea in the sourcer tables.** They already read correctly.

---

## Corrected scorecard: what each table does today

| Table / view | Views credited to | Verdict |
|---|---|---|
| `project_dashboard` (default, job based) | the role owner (`job_recruiter`) | WRONG. Andrea reads 0, Simon reads 396 |
| `project_dashboard_eventattr` (event toggle) | the doer (`who_event_created_for`) | already credits the doer |
| `weekly_summary`, sourcer view | the doer (`who_created_event`) | correct, this is the template to copy |
| `ts_summary_per_sourcer` / `ts_summary_by_client` | the doer, but only if the person is on the WBR sheet | correct for on-sheet people, silently drops everyone else |

Only two of the four are actually wrong: the project dashboard default view, and the sourcer tables' roster gate.

---

## Before / after, per table, real numbers

### 1. project_dashboard (default "job based" view) — Reaktor, weeks 24-30

| Person | Viewed now | Viewed after |
|---|---|---|
| Andrea (did the work) | 0 | 396 |
| Simon (owns the role) | 396 | 0 |
| Reaktor role total | 396 | 396 |

The 396 moves off Simon onto Andrea. Role total does not move.

### 2. project_dashboard_eventattr (the "event based" toggle) — Reaktor

Already shows Andrea 396 and Simon 0. No change to the number. This is the other side of the attribution toggle that already exists in the dashboard, and it already credits the viewer. It is proof the rule is right.

### 3. weekly_summary, sourcer view — Reaktor, Andrea, weeks 28-30

| Week | Viewed | Contacted |
|---|---|---|
| 28 | 144 | 58 |
| 29 | 156 | 61 |
| 30 | 96 | 38 |
| total | 396 | 157 |

Already correct. Views and contacts both sit under Andrea. This is the template.

### 4. ts_summary — the roster gate — Mateja Jokovic, weeks 24-30, all clients

| | Viewed now | Viewed after |
|---|---|---|
| Mateja's sourcer row | 0 | ~2,463 |

Mateja did roughly 2,463 real profile views on live roles. The table shows 0 only because he is not on the WBR sourcer sheet.

---

## The actual changes, and one landmine to avoid

### Landmine — read first

The `project_dashboard` view number is kept at job / owner level on purpose. There is a comment in that transformation dated 2026-06-03:

> "REVERTED: ts back to '' (job-level TA attribution). Per-sourcer split blew the row count from 26k to 112k, data.json grew to 78MB, exceeded Cloudflare Pages' 25MB asset limit, broke deploys. Sourcer-attributed viewed to be re-added as a separate smaller aggregate (sourcer × week × viewed)."

**Do not re-add `who_created_event` directly into the `project_dashboard` viewed CTE.** That is exactly what broke the deploy last time. The fix uses a separate small aggregate instead, which is what that comment already planned.

### The intended design is already half built

The separate aggregate the comment describes already effectively exists: `ts_summary_by_client` is a per person × client × week × viewed table keyed to `who_created_event`. It is small. The dashboard frontend already reads sourcer-level views from it (see `App.jsx`, the "surface client-level viewed totals from ts_summary_by_client" block). The only reason it reads 0 for people like Mateja is the roster gate on that table.

### Change A — remove the roster gate on the views aggregate

Transformation **"WBR/MBR weekly aggregations"** (config `01kpr0tr0dt5ryf96a5zk85bx7`), blocks **b0.c6** (`ts_summary_per_sourcer`) and **b0.c15** (`ts_summary_by_client`). The `viewed` CTE currently ends with:

```
... vv
LEFT JOIN current_roster rj ON rj.ts = vv.ts AND rj.yr = vv.iso_year
WHERE (vv.for_other = 1 OR rj.ts IS NOT NULL)
GROUP BY ...
```

Remove the `WHERE (vv.for_other = 1 OR rj.ts IS NOT NULL)` line **from the VIEWED CTE only**. Keep the `sys_accounts` exclusion and the `good_jobs` filter. Views then count for every real person who did them, on or off the WBR sheet.

This is additive. It does not move any number for anyone already shown. It only adds the people who currently read 0.

**Scope note on "no TAs in the sourcer summary":** the change above will make bench TAs like Mateja appear in `ts_summary_per_sourcer` with a views number. If you want their views credited but do NOT want them listed in the sourcer target summary, the cleaner version is a dedicated `views_by_person` aggregate (`who_created_event` × week × client, ungated) that the person views read, leaving the roster-scoped sourcer summary untouched. Both options give Mateja his views. One keeps him out of the sourcer target table. This is the one open design choice in this spec.

### Change B — point the project dashboard's person views at the ungated aggregate

The frontend already pulls sourcer views from `ts_summary_by_client` for the sourcer lens. Confirm the same read is used on the "job performance" / TA-filtered view where Andrea currently shows 0, so that every person lens uses the one ungated views source instead of the job-level `project_dashboard.VIEWED`. This is a check and possibly a small edit in `App.jsx`, not a pipeline change, and not a change to the `project_dashboard` SQL.

### No change to

`project_dashboard_eventattr` and the `weekly_summary` sourcer view. Both already credit the doer.

---

## Validation before shipping

- Confirm `data.json` (the deployed `dashboard_data_snowflake.json.gz`) stays well under the 25MB Cloudflare limit. The gate removal only adds a handful of off-sheet people to an already small per-person aggregate. It does not touch the per-job funnel that caused the 78MB blowup.
- Spot checks: Andrea Reaktor = 396, unchanged. Mateja total views become roughly 2,463 where they read 0 today. Simon and every role and client total unchanged.
- Sequence: rebuild the WBR/MBR transformation, then the render + push config, then confirm on the deployed dashboard.

## Rollback

Re-add the `WHERE (vv.for_other = 1 OR rj.ts IS NOT NULL)` line to the two viewed CTEs and rebuild. The change is isolated to the views metric and reverses cleanly.

---

## One-paragraph summary for the team

LinkedIn views are being counted for the wrong person on the project dashboard's default view: the role owner gets credited for views the sourcer actually did, so active sourcers can read 0. The rule we are standardizing on is that a view belongs to whoever did the clicking, everywhere, while roles and clients keep their totals. Two tabs already do this correctly (the event-based toggle and the weekly summary sourcer view). The fix is to stop hiding sourcer views behind the WBR-sheet roster gate and make the project dashboard's person views read from that ungated per-person aggregate, without re-inflating the main table that previously broke deploys. Contacted and all other metrics are unaffected, and no role or client total changes.
