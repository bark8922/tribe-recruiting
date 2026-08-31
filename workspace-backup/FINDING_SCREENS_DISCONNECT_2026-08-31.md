# Why the screen numbers disagree between WBR and Project Dashboard

2026-08-31. Read-only investigation. **Nothing changed.**

Blake: "WBR says 183 screens for Rodrigo last week, which I know isn't true, and I see 183 positive
response on another tab."

He was right on both counts, and it is worse than a mislabel. **Neither report is reliably correct.
They fail in opposite directions for different reasons, and the org-wide totals hide both.**

---

## 1. Rodrigo, week 35: WBR says 184 screens. The real number is about 7.

| Source | Recruiter screens |
|---|---|
| project_dashboard | **7** |
| weekly_summary | **7** |
| **ts_weekly (WBR)** | **184** |

**Cause.** `ts_weekly` counts raw `Recruiter Screen` events with **no gate**:

```sql
ev_rs AS (SELECT DISTINCT "candidate_id", TRY_TO_DATE("date_created") AS evd
          FROM event WHERE "event_type"='Moved to stage' AND "moved_to_stage"='Recruiter Screen')
```

`project_dashboard` uses `date_screen` from `candidate_stage`, which is gated on
`stage_current_num >= 2`. The gate is what filters phantom bursts.

**Proof.** Rodrigo, week 35:

| Stage | Candidates counted | In a phantom burst | Real |
|---|---|---|---|
| Recruiter Screen | 185 | **178** | **8** |
| Positive response | 186 | **178** | **15** |

This is the same failure that produced the ATS 143 on 26 August: counting events without the gate.

## 2. The "183 positive response" is the SAME 178 candidates

Not a coincidence and not a mislabel. The bursts write `Recruiter Screen` and `Positive response` at
the same millisecond, so both metrics inflate together on the same people.

**Positive response is event-based and ungated in EVERY table, including project_dashboard and
weekly_summary.** So unlike screens, positive response is inflated everywhere, not just in WBR.
2026 total is 16,880 and an unknown share of that is burst noise.

## 3. The opposite failure: Jonaed Iqbal, weeks 22 and 23

Here `project_dashboard` is the wrong one.

| Week | ts_weekly | project_dashboard |
|---|---|---|
| 22 | 6 | **56** |
| 23 | 12 | **41** |

97 candidates have a `date_screen` in those weeks. **Only 18 have a Recruiter Screen event at all.**
The other 79 have a screen date with no screen behind it, backfilled from a later stage by the
cascade in PROD V2. Every other week of Jonaed's year matches exactly, so this is not a week-shift.

**Here ts_weekly (18) is right and project_dashboard (97) is inflated.**

## 4. The org totals look fine, which is why nobody caught it

| Metric 2026 | Value |
|---|---|
| ts_weekly RECRUITER_SCREENS | 14,183 |
| project_dashboard SCREENS | 14,291 |

A 0.8% difference across the year, while individual rows are out by 26x. Same trap as 26 August:
aggregate checks pass, row-level checks fail.

Only three sourcer-weeks in all of 2026 diverge by 25 or more: Rodrigo w35 (+177), Jonaed w22 (-50),
Jonaed w23 (-29). The problem is contained, but it is real and it hit the person being reviewed.

## 5. Two more defects found in ts_weekly while reading it

- **Non-ISO week functions.** `ts_weekly` buckets with `YEAR()` and `WEEKOFYEAR()`. Every other table
  uses `YEAROFWEEKISO()` and `WEEKISO()`. Week boundaries can differ between reports.
- **Matches on stage NAME, not stage TYPE.** `moved_to_stage='Recruiter Screen'` misses the 20 other
  names that carry the `Recruiter Screen` type (`Phone Screen`, `Javier Screen (Recruiter)`, etc).
  Same name-vs-type trap as the funnel work.

---

## What this means

All four of these are the same family of problem the stage model is meant to fix:

1. Some metrics gate against phantom bursts, some do not, and it is not documented which.
2. Some match on stage name, some on stage type.
3. Some use ISO weeks, some do not.
4. The cascade backfill invents stage dates with no event behind them, and no metric distinguishes
   a backfilled date from a real one.

**Recommendation: do not patch `ts_weekly` in isolation.** Fixing the gate there would make Rodrigo
right and leave Jonaed wrong, and would not touch positive response. These need one consistent
definition of "reached this stage", which is exactly what `candidate_stage_events` is for.

**What to tell Rodrigo now:** his week 35 screens figure of 184 in WBR is wrong, the real number is
around 7 to 8, and it is a data issue on our side, not his numbers.
