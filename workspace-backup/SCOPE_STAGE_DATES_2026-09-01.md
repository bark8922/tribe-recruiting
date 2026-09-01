# SCOPE — every stage on its own date, nothing a subset of anything

2026-09-01. **Analysis only, nothing changed by this document.**
All numbers verified against live prod today, AFTER Mikhail's archive ran.

---

## THE PRINCIPLE (Blake, and it is the right one)

> What day did the recruiter go into Bubble and move the candidate to that stage?
> **That is the week it counts in. It is not tied to anything else.**

Move to ATS, Interview 1, Interview 2, Interview 3, Offer, Hired are **distinct stages**.
None is a subset of another. Each has its own date.

---

## WHERE WE ACTUALLY ARE

| Metric | Anchored on | Correct? |
|---|---|---|
| **ATS** (Project Dashboard, shipped today) | the `Moved to ATS` event date | **YES** |
| **Int1/2/3** (Project Dashboard Wave 1, WBR/etc Wave 2) | **the ATS week** | **NO** |
| Everything else (screens, offers, hires) | its own stage date, gated | mostly, see 4.2 |

**Int1/2/3 is the thing to fix, and it must be fixed before Wave 3 spreads it further.**

Live example: Aaron Dilley was moved to ATS on **19 Jul** and to Interview 1 on **31 Aug**.
The dashboard files his Interview 1 under **week 29 (July)**. Result: this week reads **6** Interview 1s
when **30** happened, and week 29 reads **3** when **zero** happened.

---

## MIKHAIL'S ARCHIVE: it ran, and it worked

Archived events went 9,682 -> **11,292** today. On the four AVIV jobs, burst events still reaching
reporting fell from ~1,622 across 181 candidates to **4 events on 1 candidate**.

**But phantom bursts were NOT only a Rodrigo thing.** 183 burst-shaped events on 37 other candidates
remain in August alone, and 6,448 events across 1,431 candidates across all history, concentrated in
Apr 2024 (2,383), Feb 2025 (1,490), Jan 2025 (664). Those months long predate Rodrigo's issue.
So we still need a rule. We cannot rely on the archive alone.

---

## THE CRITICAL FINDING: my earlier burst filter would have destroyed real data

Blake's warning was exactly right. People legitimately open a pipeline and click a candidate through
several stages at one moment, to record a hire that already happened.

Counting every "4+ stage types at one timestamp" as phantom, which is what I proposed earlier:

| | Bursts (2026) | Candidates |
|---|---|---|
| **Legitimate** (candidate IS at the highest stage written) | **1,158** | 1,151 |
| **Phantom** (stages written ABOVE where they sit) | **19** | 19 |

**It would have deleted 1,158 real records to catch 19.** That filter must never be used.

---

## THE RULE THAT ACTUALLY WORKS

Exclude a single event **only if BOTH** are true:

1. **4 or more distinct stage types were written for that candidate at that exact timestamp**, AND
2. **the stage it writes is ABOVE where the candidate currently sits**

Validated across all history:

| Decision | Events | Candidates |
|---|---|---|
| KEEP: ordinary event | 1,295,910 | |
| **KEEP: bulk move up to where they actually are** | **20,133** | 3,802 |
| **KEEP: real move-back (reached it, later moved down)** | **3,304** | 2,337 |
| **EXCLUDE: phantom** | **6,448** (0.49%) | 1,431 |

Both cases Blake named survive:
- **Bulk moves** survive because condition 2 fails: the candidate IS at that stage.
- **Move-backs** survive because condition 1 fails: a genuine later move-back was a standalone
  action at its own timestamp, not part of a 4-stage same-millisecond write.

The excluded set is entirely candidates parked in **Sequence** with a full funnel written above them.

---

## WHAT NEEDS TO CHANGE

### 4.1 Int1/2/3 — the actual bug
Stop bucketing on the ATS week. Bucket each on its own event date, with the rule above.

| Week | Int1 live now | Int1 correct |
|---|---|---|
| 31 | 9 | 7 |
| 32 | 16 | 6 |
| 33 | 16 | 7 |
| 34 | 40 | 39 |
| 35 | 18 | 20 |
| **36** | **6** | **30** |

### 4.2 The `stage_current_num >= N` gates are the same mistake, older
Every stage date in PROD V2 is gated on the candidate's CURRENT stage. That is why:
- **289 candidates** who were genuinely moved to ATS are not counted at all, because they were later
  moved back down. By the principle above they should count in the week they were moved.
- `date_screen` is separately inflated by the coalesce cascade inventing dates with no event behind
  them (Jonaed: 97 counted, 18 real).

The rule in section 3 replaces these gates properly: it is per-event, not per-candidate, so a
move-back no longer erases history.

### 4.3 Build it once
This is the `candidate_stage_events` table scoped on 2026-08-27: one row per candidate x job x stage
reached, with that stage's own date and the exclusion rule applied once. Every report reads from it.
Patching each transform separately is how we ended up with `ts_weekly` counting raw ungated events
while `project_dashboard` counts backfilled ones.

---

## WHAT IS SAFE TO LEAVE ALONE

**Today's ATS change stays.** It anchors on the ATS event date, which satisfies the principle. Its
only flaw is the inherited `>=3` gate (the 289), which is pre-existing and small. Rolling it back
would put ATS back on `date_interview` and re-break Simon and No Isolation.

---

## SEQUENCE

1. Agree the rule in section 3.
2. Fix Int1/2/3 bucketing **before Wave 3**. Wave 3 as currently planned spreads the wrong logic to
   WBR, MBR and TS Summary.
3. Then replace the `>=N` gates with the per-event rule, stage by stage, each verified row-level.
4. Ask Mikhail whether the remaining 6,448 historical phantoms can also be archived. If he does, the
   rule becomes belt-and-braces rather than load-bearing.
