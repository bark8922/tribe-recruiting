# DESIGN — stage-level model for the new funnel

Written 2026-08-27, **corrected** after Blake caught an error in the first version.
**Nothing has been changed in prod. Analysis and design only.**

---

## CORRECTION to the first version

I published a ladder table showing Interview 1 next to Onsite, Interview 2 next to Offer, and
Interview 3 next to Hired. **That was wrong and it did not describe any real funnel.** I had
aggregated stages at CLIENT level across many jobs with different ladder lengths. Jobs without
Interview 1/2/3 put Offer at position 6; jobs with them put Offer at 8. Averaging the two smeared
everything and produced a nonsense table. Blake caught it.

**The real ladder, from three live jobs that use Interview 1:**

```
Contacted → Positive Response → Recruiter Screen → Moved to ATS
    → Interview 1 → Interview 2 → Interview 3 → Offer → Hired
```

Interview 1/2/3 sit **between ATS and Offer**, exactly as Blake described.

---

## PART 1 — What we are measuring

For any candidate on any job: which stages they reached, the date of each, which they skipped,
conversion between any two, and time between any two. Today none of this is possible because the
reporting layer flattens the whole interview region into one column, `date_interview`, holding only
the LATEST interview-ish event.

---

## PART 2 — What the data says

### 2.1 The template is the model. Key on stage TYPE, never on name.
Recruiters rename stages but keep the type. Proven in live data on one job:

| Stage NAME (renamed) | Stage TYPE (held) | Events |
|---|---|---|
| `Technical Interview` | **Interview 1** | 43 |
| `Take home test` | **Interview 2** | 16 |

This is the whole answer to "what if someone adds their own". They almost always rename within the
template, the type survives, and the model keys on type. **No new columns, ever.**

### 2.2 Non-template `Offsite` stages are legacy, not a live problem
I overstated this in the first version. The 44 odd stage names (`Chirag (Coding)`, `Mauricio`,
`Vlad`) exist as definitions but are barely used:

| Period | Moved to ATS | Other Offsite | Final Interview (Onsite) | Interview 1/2/3 |
|---|---|---|---|---|
| 2025 | 14,522 | **15** | 4,625 | 0 |
| 2026 | 8,634 | **24** | 2,108 | 287 |

**For practical purposes `Offsite` means "Moved to ATS".** 8,634 against 24.

### 2.3 The old funnel is still the majority
`Final Interview` (stage name `Onsite`) is the old funnel's single client-interview stage: 2,108
events in 2026 against 287 for Interview 1/2/3. Expected, since only 15 of 122 clients have
migrated. Both funnels must work side by side for a long time.

### 2.4 `Onsite` is invisible to today's metrics
`Final Interview` / `Onsite` matches NEITHER condition in the `date_interview` filter. So for
old-funnel candidates `date_interview` is usually just the ATS move wearing a misleading name.
Pre-existing bug, unrelated to the new funnel, worth fixing in passing.

---

## PART 3 — THE BLOCKER: phantom bursts

A stage-level model reads per-stage dates from the event log, and that log is contaminated worst
where we most want to measure:

| Stage type | Total events | In a burst | % fake |
|---|---|---|---|
| Interview 1 | 158 | 100 | 63.3% |
| **Offer** | **10,303** | **3,879** | **37.6%** |
| **Hired** | **9,903** | **3,532** | **35.7%** |
| **Final Interview** | **12,928** | **4,310** | **33.3%** |
| Offsite | 53,022 | 5,073 | 9.6% |
| Recruiter Screen | 129,839 | 4,967 | 3.8% |
| Contacted | 1,067,845 | 2,527 | 0.2% |

**Caveat on the headline number:** Interview 1's 63% is only 100 events out of 158, a tiny base, so
it is not the scary figure it looks like. **The serious ones are Offer, Hired and Final Interview at
33-38% across roughly 10,000 events each.** Those are the stages the business actually reports on.

**Not new, not caused by the new funnel.** ~4,800 bursts back to Jan 2024, worst month Apr 2024.

### 3.1 The defence, and it generalises
`candidate_stage.stage_current` records where a candidate sits NOW, and bursts do not move it
(37,538 sitting at `Moved to ATS`, 3,289 at `Onsite`, 7,531 at `Hired`). That is why today's metrics
survive at all.

> **A candidate counts as having reached a rung only if their CURRENT rung is at least that far.**
> The DATE for that rung then comes from that rung's own event.

Same `stage_current_num >= N` gate that already protects every existing metric, extended to every
rung. Burst events belong to candidates parked low in the funnel, so the gate excludes them.

**Measured residual risk:** for candidates who genuinely reached a rung, a burst may still have
written that rung's event at the wrong moment. For ATS this is 3,358 of 49,156 gated candidates
(6.8%), landing in the correct ISO week 99.6-100% of the time.

---

## PART 4 — The model

### 4.1 One canonical ladder, both funnels, no new columns

| Rung | Name | Identified by | Old funnel | New funnel |
|---|---|---|---|---|
| 1 | Contacted | type `Contacted` | yes | yes |
| 2 | Positive Response | type `Positive Response` | yes | yes |
| 3 | Recruiter Screen | type `Recruiter Screen` | yes | yes |
| 4 | **Handed to client (ATS)** | type `Offsite` | yes | yes |
| 5 | Interview 1 | type `Interview 1` | — | yes |
| 6 | Interview 2 | type `Interview 2` | — | yes |
| 7 | Interview 3 | type `Interview 3` | — | yes |
| 5-7 | Client Interview | type `Final Interview` | **yes (single rung)** | rare |
| 8 | Offer | type `Offer` | yes | yes |
| 9 | Hired | type `Hired` | yes | yes |

Old-funnel jobs collapse rungs 5-7 into one `Client Interview` rung fed by `Final Interview`.
New-funnel jobs use the three explicit types. Both sit between ATS and Offer, so ATS → interview →
Offer → Hired conversion is comparable across both. A renamed stage lands on its type automatically.
A genuinely novel stage falls into the nearest template rung and never creates a column.

### 4.2 Two new tables

**`candidate_stage_events`** — one row per candidate × job × rung reached: candidate_id, job_id,
client, TA, TS, rung, stage_type, stage_name (raw, drill-down only), date_reached, was_skipped,
flag_in_burst.

**`stage_conversion_weekly`** — the aggregate dashboards read: client × job × TA × TS × week × rung,
counts entering and leaving each rung.

Every question then becomes a simple query: ATS by week, Interview 1 → Interview 2 conversion, who
skipped straight to Offer, time between any two rungs.

### 4.3 The ATS fix falls out for free
"ATS in week 33 counts in week 33" is just rung 4 anchored on its own date. Not a separate project.

---

## PART 5 — Risks

1. **Burst contamination.** Handled by the gate, built in from the first line, never bolted on.
   `is_event_duplicated` is NOT usable: it partitions on `event_type` (the literal string "Moved to
   stage"), not target stage, so it flags legitimate same-day progressions.
2. **Two funnels for a long time.** 107 of 122 clients are still old-funnel. Any conversion report
   must state which funnel a number came from, or it will mislead.
3. **Thin samples on the new funnel.** 287 Interview 1/2/3 events ever. Early conversion rates are
   statistically weak and must be labelled as such.
4. **Blast radius.** New tables are additive and safe. Risk arrives only when existing dashboard
   metrics are repointed at them.
5. **Numbers move.** Week 34 ATS drops from 127 to 112 org-wide. Targets were set on old behaviour.

---

## PART 6 — Sequence

1. **Ask Mikhail about the bursts.** Blake is happy to. Frame it as a question, not a diagnosis, and
   lead with Offer/Hired at 33-38%, not the Interview 1 headline.
2. **Build `candidate_stage_events` as a NEW table.** Breaks nothing. Validate named candidates by
   hand against Bubble.
3. **Reconcile against today.** Rung 4 by week must reproduce the current ATS number apart from the
   known week-shift. If not, stop.
4. **Per-TA, per-sourcer, per-week old vs new for review.** Row level. This is the check skipped on
   2026-08-26 that cost two days.
5. **Repoint dashboards**, one transform at a time, run and check between each.
6. **Tell recruiters before they notice.**
