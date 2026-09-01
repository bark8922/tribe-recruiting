# Funnel metric reconciliation — every table diffed against Andy's DAX

Written 2026-08-31, measurement completed 2026-09-01.
**Nothing has been executed. No config has been touched.**

Trigger: Rodrigo W35 screens read 184 in WBR and 7 in Weekly Summary. Simon W36 actual screens
read 4 in WBR and 2 in Weekly Summary.

**Headline: nothing needs rebuilding.** 5 tables × 7 metrics = 35 cells. **21 already match the DAX
or are non-binding.** The real problems are four specific things, three of which are small and
uncontroversial, and one of which should be investigated rather than patched.

---

## PART 1 — The standard

Source: `POWERBI_DAX_MEASURES.md`, extracted from `KPI Dashboard (Leadership).pbix` 2026-04-08.

Every stage metric in Andy's DAX is a hybrid: **a date column on `candidate_stage` anchors the week,
AND an event filter gates whether it counts.**

| Metric | Week anchored on | Event gate |
|---|---|---|
| Contacted | `date_contacted` | none |
| Positive Response | `event.date_created` | `moved_to_stageType='Positive Response'`, floor 2025-04-14. Purely event-based. |
| Recruiter Screens | `date_screen` | `moved_to_stage='Recruiter Screen'` |
| Actual Screens | `date_screen_actual` | `event_type='Evaluation'` |
| Moved to ATS | `date_interview` | `moved_to_stage='Moved to ATS'` |
| Offers | `date_offer` | `moved_to_stageType='Offer'` |
| Hired | `date_hired` | `moved_to_stage='Hired'` |

`weekly_summary` is documented (`DATA_LINEAGE.md` §4g) as a faithful port of Andy's Weekly Progress
page, and the measurement confirms it: **6 of its 7 metrics match exactly.**

Note TA and TS were deliberately different in the original PBIX — `WBR TA Actual` used the event
Contacted measure, `WBR TS Actual` the date one, and TA had no Screens column. Andy's reason
(`DASHBOARD_GUIDE.md` §4d): *"TS tracks what a sourcer did in the window, not what happened to
candidates they sourced."* Those differences are by design and are not in scope here.

---

## PART 2 — Measured diff, 2026 full year

Only cells that differ are listed. Everything omitted matches.

| Metric | Table | Problem | Current | DAX | Δ |
|---|---|---|---|---|---|
| Actual Screens | `wbr_weekly` | missing Evaluation gate | 11,997 | 10,937 | **−1,060 (−8.8%)** |
| Contacted | `eventattr` | per-TA first-event anchor | 74,729 | 75,369 | +640 (+0.9%) |
| Contacted | `ts_weekly` | **calendar year, not ISO** | 76,786 | 77,251 | +465 (+0.6%) |
| Recruiter Screens | `wbr_weekly` | missing gate | 14,314 | 14,110 | −204 (−1.4%) |
| Recruiter Screens | `project_dashboard` | missing gate | 14,314 | 14,110 | −204 (−1.4%) |
| Recruiter Screens | `ts_weekly` | anchors on event date, not `date_screen` | 14,205 | 14,110 | −95 (−0.7%) |
| Offers | `ts_weekly` | calendar year | 1,360 | 1,419 | +59 (+4.3%) |
| Hired | `ts_weekly` | calendar year | 1,267 | 1,326 | +59 (+4.7%) |
| Moved to ATS | `ts_weekly` | calendar year + missing gate | 7,127 | 7,184 | +57 (+0.8%) |
| Moved to ATS | `wbr_weekly` | missing gate | 7,195 | 7,184 | −11 (−0.15%) |
| Contacted | `wbr_weekly` / `project_dashboard` | first-event anchor (deliberate 24 Aug fix) | 77,260 / 74,758 | 77,251 / 74,749 | −9 each |

**Non-binding gaps — no action needed.** `wbr_weekly` and `project_dashboard` are missing the Offers
and Hired gates, but the delta is **exactly zero** in both. Every candidate with a `date_offer` or
`date_hired` already has the matching event. Adding those gates would change no number.

---

## PART 3 — The four things that actually matter

### 1. `ts_weekly` buckets on calendar year, not ISO. Pure bug, no debate.

All 465 missing Contacted have `date_contacted` between **29 and 31 December 2025** — ISO week 1 of
2026, calendar 2025. `YEAR(dc)=2026` drops them into a nonsense `iso_year=2025, iso_week=1` row where
nobody sees them. Same mechanism costs 68 ATS, 59 Offers, 59 Hires.

**Nenad Skoko alone loses 211 of the 465.** Aleksandra Markovic 63, Rodrigo Gomes 53.

Nobody can defend calendar-year bucketing when every other table uses ISO. This is the cleanest fix
on the list and it recovers real work that people did.

### 2. `wbr_weekly` actual screens, −1,060. **Do not patch this blind.**

The delta looked like an ongoing 8.8% inflation. It is not. **89% of it sits in weeks 1 to 12, and it
stops dead at week 13:**

| W | 1 | 2 | 3 | 4 | 5 | 6 | 7 | 8 | 9 | 10 | 11 | 12 | **13** | 14 | 15 | 16–36 |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| n | 56 | 50 | 68 | 80 | 107 | 110 | 72 | 89 | 92 | 82 | 71 | 70 | **12** | 7 | 7 | ≤11 each |

And two people carry 39% of it:

| TA | phantom screens | of which W1–12 |
|---|---|---|
| Ketevan Khorava | 223 | 223 |
| Chantal Bozkurt | 192 | 192 |
| Alisa Liddell | 99 | 90 |
| Zarina Amanbekova | 97 | 97 |
| Tinatini Karaulashvili | 83 | 83 |

Top 10 carry 87%. **Something specific happened in Q1 2026 to a small group of TAs and then stopped.**
That is a data question, not a definition question. The gate would paper over it. We should find out
what those 1,060 candidates are first — bulk import, a migration, a stage config that got fixed
around week 13 — because whatever it is may be affecting other metrics we have not looked at.

Current weeks barely move: W36 13.3%, W35 4.0%, W34 1.5%. So the obvious sanity check (compare this
week before and after) would show almost nothing and give false confidence.

### 3. Recruiter Screens missing gate in `wbr_weekly` and `project_dashboard`, −204 each (−1.4%).

Small, matches the DAX, brings both in line with `weekly_summary` which already does it. This is the
Rodrigo case: `ts_weekly` counts raw events on the event date and read 184; the DAX answer is 7.
Fixing `ts_weekly`'s anchor and adding the two gates makes all four tables agree.

### 4. Blank TA name — a one-word bug.

`eval_doer` filters `who_event_created_for IS NOT NULL` but does not exclude empty string, so
`COALESCE(d.doer_ta, j.ta)` returns `''` and **7 actual screens in 2026 are credited to a TA called
""**. Fix is `NULLIF(...,'')`.

---

## PART 4 — Questions now answered by measurement

**The credit-attribution question is a non-issue.** "Evaluation in the same week" vs "Evaluation
anywhere in history" differ on **7 candidate rows out of 11,997**, and the entire difference is the
blank-name artifact above. The org total is identical either way by construction. Nothing to decide.

For context, on those 11,997: 89.5% have an Evaluation doer equal to the job recruiter, 1.6% (189)
differ and get reassigned, 0.1% blank, and **8.8% (1,060) have no Evaluation at all and fall back to
the job recruiter** — exactly the rows the gate would remove.

**The targets question is much smaller than I said.** Because 89% of the actual-screens delta is in
weeks 1 to 12, and current weeks move 0 to 4%, this is not a live attainment problem. It changes how
Q1 reads in hindsight. Worth telling the team, not worth rebasing targets over.

---

## PART 5 — A caveat that matters more than any of the above

**The April PBI numbers can no longer be reproduced, under any definition.**

| Metric | PBI recorded (W14 2026) | Live today | DAX-faithful today |
|---|---|---|---|
| Contacted | 1,871 | 2,211 | 2,211 |
| Positive Response | 396 | 448 | 448 |
| Actual Screens | 210 | 234 | 227 |
| Moved to ATS | 131 | 141 | 141 |
| Offers | 13 | 17 | 17 |
| Hires | 13 | 18 | 18 |

The DAX-faithful rebuild lands within 0–1 row of live production on every metric, so **the definition
diff explains none of the gap to PBI.** Everything has drifted upward by 7% to 38% since April. That
is the signature of retroactive accretion: `date_contacted`, `date_interview`, `date_offer` and
`date_hired` are all rebuilt as `max()` of events each run, so a candidate touched today back-dates
into an old week. Swept weeks 10–20: no 2026 week produces the recorded 131 ATS.

**Consequence: we cannot use the April PBI figures as a regression test any more.** Any future
"is it still right?" check has to be against a frozen snapshot we take ourselves, not against Andy's
numbers.

---

## PART 6 — Proposed order. Nothing starts without an explicit yes to that specific step.

**A. Fix `ts_weekly`'s ISO bucketing.** Recovers 465 Contacted, 68 ATS, 59 Offers, 59 Hires that are
currently invisible. No definitional debate. Tell Nenad, he is owed 211 contacts.

**B. Fix the blank TA name.** `NULLIF(...,'')`. 7 rows.

**C. Add the Recruiter Screen gate to `wbr_weekly` and `project_dashboard`, and move `ts_weekly`'s
screens onto `date_screen`.** −204 / −204 / −95. Makes all four tables agree and matches the DAX.

**D. Investigate the 1,060 before deciding anything.** Pull the Q1 candidates behind Ketevan's 223
and Chantal's 192 and find out what they are and what changed at week 13.

**E. Leave alone:** the Contacted anchors in `wbr_weekly` / `project_dashboard` (the 24 Aug first-event
fix is a deliberate improvement on the DAX and differs by 9 candidates); the Offers and Hired gates
(zero delta); TA vs TS differences (by design).

**Separately, and now unblocked:** Simon's ATS 18 is the `date_interview` re-dating problem, not any
of the above. Mikhail has fixed the phantom-burst cause at source (Rodrigo's manually created stages
in the wrong order on four AVIV jobs; bursts fell from 121/day on 25 Aug to 1/day on 31 Aug). The
bursts were the only reason the ATS event anchor was reverted on 26 Aug. That decision can now be
revisited on its own merits, with a fresh before/after.

---

## Open questions

1. A, B and C are low-risk and defensible. Ship them one at a time, or leave everything until D is
   understood?
2. D: do we want to know what the Q1 1,060 are, or accept the gate and move on?
3. Now that the bursts are fixed at source, do we retry the ATS event anchor for Simon's problem?
