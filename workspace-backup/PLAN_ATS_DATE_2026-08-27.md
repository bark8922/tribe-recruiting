# SCOPE — fixing the ATS week-bucketing without losing burst protection

Written 2026-08-27. **Nothing has been changed. This is analysis only.**
Every number below came from a query against live prod data this morning.

---

## PART 1 — What is actually happening (this was the missing piece)

### 1.1 "Moved to ATS" is not its own stage type. It is an `Offsite`.

```
stage_type_name = 'Offsite'   stageName = 'Moved to ATS'    8,746 stage records
```

### 1.2 So the ATS move is ALREADY one of the events feeding `date_interview`

In PROD V2 (`375145203`), `date_interview` is built as:

```sql
set c."date_interview" = (select max(t."date_created")
      from "final_event" as t
     where c."candidate_id"=t."candidate_id"
       and c."stage_current_num">=3
       and (t."moved_to_stageType" IN ('Offsite','Interview')
            or lower(t."moved_to_stage") LIKE '%interview%'));
```

`Moved to ATS` is an `Offsite` event, so it matches that filter. **`date_interview` is the max of
the ATS move AND every later interview event.** That single `max()` is Mikhail's entire bug. Move a
candidate to ATS in week 33, give them an interview date in week 34, and `max()` returns week 34.
The ATS count lands a week late.

### 1.3 The thing protecting us from phantom bursts is the GATE, not the date column

`and c."stage_current_num">=3`

A phantom burst writes ATS, Onsite, Offer and Hired at one millisecond for a candidate who is
sitting in Sequence. Their **current** stage stays Contacted, so `stage_current_num` = 1, the gate
fails, `date_interview` stays empty and they never count. That is the shield.

**This is why the event-based fix exploded into Rodrigo's 143.** It counted raw ATS events and
dropped the gate. The date column was never the protection. The gate was.

**Consequence: we can change the date column freely as long as we keep the gate.**

### 1.4 THE NEW FUNNEL IS WHY THIS SURFACED NOW, AND IT IS GETTING WORSE

Blake flagged that the funnel changed around mid-July: Offsite (Moved to ATS), then Interview 1,
Interview 2, Interview 3, then Offer, then Hired. That is the trigger.

`Interview 1`, `Interview 2` and `Interview 3` all match the `LIKE '%interview%'` clause, so **every
advance through the new interview ladder pushes `date_interview` forward again** and drags the ATS
count with it.

The old funnel did not do this. Its main interview stage is `Onsite` (8,667 stage records, type
`Final Interview`), and `Onsite` matches **neither** condition: the type is not in
`('Offsite','Interview')` and the name does not contain "interview". So under the old funnel there
was usually nothing later than the ATS move, and the max landed on the ATS move by default. That is
why this bug was invisible for years.

**Measured lag, by month of the ATS move, 2026:**

| Month | ATS moves | Lagged behind | % lagged |
|---|---|---|---|
| Jan | 1,034 | 0 | 0.0% |
| Feb | 1,460 | 4 | 0.3% |
| Mar | 977 | 1 | 0.1% |
| Apr | 901 | 0 | 0.0% |
| May | 981 | 0 | 0.0% |
| Jun | 925 | 2 | 0.2% |
| Jul | 802 | 6 | 0.7% |
| **Aug** | **377** | **34** | **9.0%** |

**What is doing the pushing (July onward):** Interview 1 (33), Interview 2 (11), Interview 3 (6),
Technical Interview (4), Final Interview (3), Moved to ATS (1). It is almost entirely the new ladder.

**Trajectory:** only **15 of 122 clients** are on the new funnel today and the lag rate is already
9%. As the rollout continues this gets materially worse. This is not a stable 9%.

**So the fix is more urgent than it looked, and the reason is a process improvement, not a data bug.**

---

## PART 2 — The proposed fix

Add a `date_ats` column to `candidate_stage`, built with the **identical gate** but anchored on the
ATS move itself:

```sql
update "final_candidate_stage_bubble" as c
set c."date_ats" = (select min(t."date_created")::DATE
      from "final_event" as t
     where c."candidate_id"=t."candidate_id"
       and t."moved_to_stage" = 'Moved to ATS'
       and c."stage_current_num">=3);
```

Then the three dashboard transforms bucket ATS on `date_ats` instead of `date_interview`.

Two properties worth being explicit about:
- **The population does not change at all.** Exactly the same candidates qualify, because the gate
  is byte-identical. Only the week they are assigned to changes.
- **`date_interview` is untouched**, so screens, interviews, offers and hires are all unaffected.

---

## PART 3 — Evidence

### 3.1 It reproduces Mikhail's ground truth exactly

He reported No Isolation / Account Manager UK (South) showing 8 in week 34, when Bubble had 6 that
week and 2 the week before.

| Model | wk 33 | wk 34 | wk 35 |
|---|---|---|---|
| Current (`date_interview`) | 0 | **8** | 1 |
| Proposed (`date_ats`) | **2** | **6** | 1 |

The simulation produces 6 and 2. That is Bubble's own numbers, from the person who reported the bug,
arrived at independently.

### 3.2 History barely moves

| ISO year | Current count | Lands in the SAME week under the fix | % unchanged |
|---|---|---|---|
| 2023 | 8,031 | 8,026 | **99.9%** |
| 2024 | 11,536 | 11,534 | **100.0%** |
| 2025 | 13,632 | 13,585 | **99.7%** |
| 2026 | 7,525 | 7,495 | **99.6%** |

**Not a single candidate changes ISO YEAR.** Roughly 84 candidates across four years move week.

### 3.3 The 2026 weekly deltas are small and concentrated where the bug lives

Identical in 26 of 35 weeks. The largest movements:

| Week | Current | Proposed | Delta |
|---|---|---|---|
| 32 | 102 | 109 | +7 |
| 33 | 103 | 108 | +5 |
| **34** | **127** | **112** | **-15** |
| 35 | 41 | 39 | -2 |

Week 34 losing candidates to weeks 32 and 33 is exactly the correction we are trying to make.

### 3.4 MIN vs MAX is immaterial

2,400 candidates have more than one ATS move, average 3.1 days apart. Only 174 would land in a
different week depending on the choice, and **zero of those are in 2026**. `min()` is the right
semantic (first time they reached ATS) and costs nothing.

---

## PART 4 — Risks, honestly

### 4.1 Burst contamination is real but measured, and it is the thing to watch
Of 49,156 gated candidates with an ATS event, **3,358 (6.8%) have their first ATS event inside a
phantom burst timestamp.** In practice it does not move the numbers, because the burst fires at the
moment of a genuine progression, so the burst date and the real date fall in the same week. That is
what the 99.6-100% table above is measuring. **But this is the assumption to re-test if anything
looks wrong after deployment.**

### 4.2 Blast radius: PROD V2 is the big one
Adding a column means editing `375145203`, the 38k-character config that builds `candidate_stage`
and feeds everything including Andy's PBI pipeline. Adding a nullable column and one `update`
statement is additive and should not disturb existing consumers, but this is the config with the
widest reach in the project.

### 4.3 Three transforms must change together
Weekly funnel (`01kpqh9r...`), event-attr (`01ks4qf6...`) and WBR/MBR (`01kpr0tr...`) all compute
ATS. If they are not changed consistently the tables diverge, which is exactly the failure mode from
2026-08-26 that produced 143 in one table and 3 in another. **Change all three or none.**

### 4.4 Reported numbers will move, and someone's week gets worse
Week 34 drops from 127 to 112 org-wide. Anyone whose card is measured on that week will see a
decline they did not cause. WBR and MBR targets were calibrated against the old behaviour. This
needs telling people before it lands, not after.

### 4.5 `Onsite` does not feed `date_interview` at all — separate issue, worth a look
`Onsite` (8,667 stage records, type `Final Interview`) matches neither condition in the filter. It
is the most-used interview stage in the system and it never sets `date_interview`. That is almost
certainly not intentional. It does not affect the ATS fix proposed here, but it means any metric
that reads `date_interview` as "when they interviewed" is wrong for old-funnel candidates. Flagging
it, not fixing it here.

### 4.6 What this does NOT fix
`date_interview` remains `max()` of Offsite plus Interview events, so the ATS move still contaminates
the interview date itself. Interview-stage timing metrics keep whatever inaccuracy they have today.
Out of scope here, worth a separate look.

---

## PART 5 — Proposed sequence. Nothing starts without a yes to that specific step.

**Step 1.** Blake reviews this document and the numbers in it.

**Step 2.** Produce the full per-sourcer, per-week, 2026 old-vs-new table for review. Row level, not
aggregate. This is the check that was skipped on 2026-08-26.

**Step 3.** Add `date_ats` to PROD V2 only. Run it. **Change no dashboard transform yet.** The new
column sits there unused and nothing on any dashboard moves. Verify `date_ats` against raw events
for a handful of named candidates.

**Step 4.** Switch ONE transform (weekly funnel) to `date_ats`. Run. Compare against the other two,
which still use `date_interview`. The difference should match the simulation above, week for week.

**Step 5.** Switch the remaining two. Run. Confirm all three agree again.

**Step 6.** Tell the recruiters what changed and which weeks moved, before they notice it themselves.

Rollback at every step is a single `kbc_rollback_config`. Current versions to roll back to:
PROD V2 **240**, weekly funnel **16**, event-attr **8**, WBR **53**.
