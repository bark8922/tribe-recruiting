# Questions for Frantisek — Friday meeting

**Context:** Tribe is winding down Power BI + (likely) Keboola by August 31, 2026. Andy leaves end of April. Blake is rebuilding the recruiting reporting on a lighter stack. Frantisek built the Keboola pipeline ~4 years ago and is the only person who fully understands the Snowflake transforms. The goal of this meeting is **knowledge transfer + sizing the migration**, not a debate about whether Keboola should stay.

---

## 🔥 TOP OF MEETING — v2 semantics (ask these FIRST, 15 min)

After rewriting the WBR TA slice to read `candidate_stage.date_*` (Andy's "current stage = source of truth" rule) and diffing against Power BI screenshots of week **2026-14 (Mar 30 – Apr 5)**, we have two directional problems:

- **Contacted is 59% HIGH** in v2 (raw event counting, likely inflated by revert double-counts)
- **Hires is 73% LOW** in v2, **Actual/ATS are ~28% LOW** (candidate_stage.date_* is filtering too aggressively)

These go opposite directions, which means v2 has two independent bugs. **Get both resolved before getting into Phase-2 infra discussion.**

### 🚨 Q0 — Stage-move counting: what does Power BI actually do?

Power BI screenshots (week 2026-14, Mar 30 – Apr 5, across all TAs both tabs):

CRITICAL SCHEMA FINDING: `event_type = 'Moved to Contacted'` DOES NOT EXIST.
All stage moves use `event_type = 'Moved to stage'` with the specific stage
in the `moved_to_stage` (free-text name) and `moved_to_stageType` (canonical
bucket) columns. stageType values: Contacted, Recruiter Screen, Positive
Response, Offsite, Final Interview, Offer, Hired.

Using the correct columns (`moved_to_stageType`), week 14, WITH external
recruiters (since that matches PBI on Contacted):

|            | Power BI | v3 events | v3 distinct cand. | Delta (events) |
|------------|---------:|----------:|------------------:|---------------:|
| Contacted  |    1,941 |     1,892 |             1,745 | **-2.5%** ✅   |
| Actual Scr |      202 |       ???  |               ??? | mapping needed |
| ATS        |      127 |       100 |                91 | -21%           |
| Hires      |       11 |         3 |                 3 | **-73%** 🔴    |

Other stageTypes in the same week (to help Frantisek identify mapping):
- Recruiter Screen: 270 events / 270 distinct
- Positive Response: 333 events / 330 distinct
- Final Interview: 12 events / 12 distinct
- Offsite (= ATS?): 100 events / 91 distinct

Test job filter had ZERO impact (identical results with/without it).

**Ask Frantisek (show him the table above, then ask):**

> "We found that `event_type = 'Moved to Contacted'` doesn't exist — all stage moves use `event_type = 'Moved to stage'` with the stage in `moved_to_stageType`. Once we fixed that, Contacted lands within 2.5% of Power BI (1,892 vs 1,941). But we're stuck on three things:
>
> **1. Stage-to-PBI-column mapping:**
> Power BI TA shows Contacted → Actual Screens → ATS → Hires. The stageType
> values are: Contacted, Recruiter Screen, Positive Response, Offsite, Final
> Interview, Offer, Hired. Which stageType is Power BI's 'Actual Screens'?
> Is it Recruiter Screen (270 events) or Positive Response (333 events)?
> And is 'ATS' = stageType 'Offsite' (100 events)?
>
> **2. Hires: 3 events vs PBI 11**
> Only 3 `moved_to_stageType = 'Hired'` events in week 14 (with externals).
> Power BI shows 11. Where are the other 8 coming from? Possibilities:
> (a) PBI counts from a different field (candidate_stage.date_hired? candidate.date_hired?)
> (b) PBI counts offer-accepted or contract-signed as 'hired'
> (c) PBI uses a wider date window or rolling logic
> Can you check what column the `WBR TA Actual` hire measure reads?
>
> **3. Does PBI include external recruiters in TA Contacted?**
> With externals: 1,892 (2.5% off). Without: 1,673 (14% off).
> Andy said TA excludes externals, but the numbers suggest PBI keeps them
> for Contacted at least."

**Why it matters:** Contacted is basically solved. The mapping question (#1) determines whether Actual Screens and ATS are 'close' or 'way off'. Hires (#2) is the headline number for the VBR — getting it wrong is a trust-killer.

### Q1 — "Sourced" definition for WBR TS

### Q1 — "Sourced" definition for WBR TS

Andy's DAX `WBR TS Actual` computes a `sourced` metric per sourcer per week. From his Tuesday brain dump it's ambiguous whether "sourced" means:

- **(a)** Count of `event_type = 'Candidate created'` events (i.e. the moment the candidate enters the ATS, regardless of whether anyone contacted them), OR
- **(b)** Count of first `event_type = 'Moved to Contacted'` events per candidate (i.e. the first outreach, which Andy also calls "credit sourcer" elsewhere)

These produce very different numbers because many candidates are created but never contacted ("prospects" in Andy's language). The wbr_view.sql currently uses (a) with a `-- !! VERIFY FRANTISEK` marker.

**Ask:** "In the Power BI `WBR TS Actual` measure, is `sourced` counting Candidate-created events or first-move-to-Contacted events per candidate? And should the sourcer credit (the `who_created_event_first` field) be applied to both, or only to Contacted?"

*Why it matters:* If (b) is correct, every sourcer's weekly number drops by the prospect-fallout rate (~30-50% in spot checks). We need the right one before we ship anything to the VBR.

### Q2 — Event-level Contacted double-count on reverts

When a candidate is moved Contacted → Screen → Contacted (a "mistake revert", which Andy says happens ~5% of the time), the event stream records **two** `Moved to Contacted` events. Andy's DAX `# events - contacted` appears to count events, not distinct candidates, so it would report 2.

**Ask:** "For the contacted count in `WBR TA Actual`, does the Power BI measure count events (so reverts double-count) or distinct candidates? And is the existing reporting-v2 view already deduping these, or is it left to the consumer?"

*Why it matters:* I'm currently counting events (matches Andy's DAX). If Frantisek de-dupes upstream, my numbers are slightly high. If he doesn't, we need to decide whether to match Andy's DAX or fix it for v2.

### Q3 — External recruiter asymmetry

Andy @L808-815 of the Tuesday transcript: TA metrics exclude `job.is_external_recruiter = 'true'`, but TS metrics include them ("for sourcer they want to know"). wbr_view.sql now matches this.

**Ask (quick confirm):** "Does the Power BI `WBR TA Actual` measure apply a `NOT is_external_recruiter` filter? And does `WBR TS Actual` deliberately keep them?"

### Q4 — Base filter set (**PARTIALLY RESOLVED — Andy answered 2026-04-10**)

Andy's confirmed definitive filter rules (2026-04-10 written answers):
- `job.test <> 'true'` — YES, use this
- `client.test` — **DO NOT USE** (not clean — real clients like Wolt have test=true)
- `is_job_archived` — **DO NOT EXCLUDE** (just a status flag, not a filter)
- `is_candidate_archived` — YES, exclude
- External recruiters — only exclude for conversion rates + company-wide, NOT from TA weekly

**Remaining question for Frantisek:** "Is there a canonical test-client list in reporting-v2, or do we need to hard-code the name-based exclusion (Tribe, Kamila test accounts)? Andy confirmed client.test is unreliable."

### Q5 — Validation check

(Power BI diff already done — see Q0 table. Use this time to walk through the week-15 partial numbers together on his screen and see which CTE/column he points to as the source.)

---

## Framing for the rest of the meeting

## Goal of the meeting (state up front)

> "Frantisek, we read your infrastructure report and I agree the pipeline is solid. The decision isn't whether Keboola is a good tool — it is. The decision we're forced into is cost. We're cancelling Power BI in August (Andy is leaving end of April, so the dashboard layer goes with him) and we're trying to figure out two things: (1) can we ship a smaller dashboard that reads directly from `reporting-v2` by end of April? and (2) for August, what's the cheapest path that preserves your transformation logic? We'd love your help on both."

---

## Section 1 — `reporting-v2` as the new MVP source (Phase 1)

1. **Confirm `reporting-v2` is the right place to read.** The 17 tables in `out.c-reporting-v2` — are these the same tables Power BI consumes via the Snowflake Writer? Or does Power BI read from somewhere else?
2. **For the `job` table** — is this the canonical "active jobs" view? Are there filters Andy applies on top in Power BI (test jobs, archived, specific clients)? What does the `client_id` foreign key resolve to?
3. **For the `candidate` table** — Andy described a wide-form table with horizontal stage timestamps (created, viewed, contacted, screen, actual_screen, ATS, offered, hired). Is `out.c-reporting-v2.candidate` that table, or is it `candidate_stage`? What's the difference between the two? They both have 1.355M rows.
4. **For the `event` table** — 14.6M rows. What are the key columns we should use for sourcer credit and TA assignment? Andy mentioned `who_created_event` and `who_event_created_for` — do those exist with those names?
5. **Are there business rules baked into the reporting-v2 transforms** (e.g., "exclude test jobs", "exclude archived candidates", currency conversion to EUR, dedup logic on talent)? Or are those rules applied downstream in Power BI?
6. **The `job_goal` table only has 40 rows.** What are these — annual hiring targets? Quarterly? Per-client? How do you maintain them?
7. **The `client_cost` table** — what is this used for in the current Power BI dashboards?
8. **Refresh cadence** — `reporting-v2` updates 6×/day. For VBR (leadership review) we probably only need daily or even weekly. Is there a way to read just the daily snapshot?

## Section 2 — Access for the new dashboard

9. **Read access** — what's the simplest way for a Python script to read `reporting-v2` daily? Options:
   - Snowflake credentials directly
   - Keboola Storage API token
   - Scheduled CSV export to S3 / Google Drive
   - Something else you'd recommend
10. **Token role** — current MCP token is `guest`. Can we get a non-guest read-only token for the project, or should we do this with a Snowflake user?
11. **Are any tables in `reporting-v2` linked from another project (read-only aliases)?** The `fullyQualifiedName` is null in the API which sometimes indicates this.

## Section 3 — Andy's 30% (the gap between Keboola output and Power BI dashboards)

12. **Where does Andy's "patching" layer end and yours begin?** He said the Keboola output is ~70% correct and he applies the other 30% in Power BI DAX. From your side, what do you consider "Frantisek-cleaned" vs. "raw with known gotchas"?
13. **The "mistaken stage move" logic** Andy described — moving a candidate to screen, then back to contact, leaves three event rows and the current_stage should win. Is that handled in your transforms or is it left to the consumer?
14. **The "cascading dates" backfill** (if hired, must have had offer/screen/contact dates) — is this in Frantisek's transforms or done downstream?
15. **Sourcer credit logic** — the "first person to contact = credit sourcer" rule. Is this materialized as a column anywhere in `reporting-v2`?
16. **Currency conversion to EUR** — done in Keboola or Power BI?

## Section 4 — Phase 2: cheaper backend (May–August)

17. **The Bubble extractor** — this is what you said is the hardest part to replace ("weeks of work + ongoing maintenance"). How is yours architected? Is it Python? How does it handle pagination, rate limits, incremental updates? Is there any chance we could get the source code or have you run it on a non-Keboola host?
18. **The Snowflake transforms** — are they pure SQL, or do they use Snowflake-specific features (UDFs, stored procs, time travel) that wouldn't port? If pure SQL, would they run on DuckDB or BigQuery with minimal changes?
19. **Hypothetical: if we wanted to keep your transforms running but move off Keboola**, what does that look like? Would you be open to supporting a non-Keboola deployment (paid) for the Phase 2 work?
20. **Negotiation lever** — is there a smaller Keboola plan we'd qualify for? Our actual usage is 275 credits/month and we don't need Power BI refresh anymore. Would a "storage + transforms only" plan exist, and roughly what would it cost?

## Section 5 — Operational handover

21. **The sourcer mapping file** — Andy mentioned a manual file owned by Gustavo / sourcing team that gets ingested. Is this loaded into Keboola via the Google Drive extractor, and if so, where does it live in storage?
22. **The VBR targets spreadsheet** — Andy mentioned this lives separately and isn't from Bubble. Do you know where it lives and how it gets into the pipeline (if at all)?
23. **The 4 Power BI datasets** (TRIBE.XYZ, TRIBE.XYZ_DEV, Client - Alpas, Client - Circula) — once Power BI is cancelled, we can drop the writer / refresh steps. Anything else that depends on those refreshes downstream?
24. **TRIBE.XYZ_DEV failures** — telemetry shows 98 errors over 6 months on that PowerBI refresh. Is that workspace still needed or can we disable it now?
25. **Mikhail** — Andy strongly recommended looping in Mikhail (Bubble designer) for event semantics. Do you work with him directly? Could you make a 3-way intro?

---

## What I want to walk away with

- A clear "yes" or "no" on whether reading `reporting-v2` directly is a viable Phase 1 path
- Read access provisioned (or a clear plan to provision it)
- A list of which transforms are pure SQL vs. Snowflake-specific (sizing for Phase 2 portability)
- Frantisek's honest take on the smallest Keboola plan we could survive on
- An intro to Mikhail
- The Bubble extractor source code, or a path to get it

---

## Status tracker (updated 2026-04-10)

### RESOLVED since these questions were written
- **Event schema:** `event_type = 'Moved to Contacted'` does NOT exist. All stage moves use `event_type = 'Moved to stage'` with `moved_to_stageType` for the canonical stage bucket. Values: Contacted, Recruiter Screen, Positive Response, Offsite, Final Interview, Offer, Hired.
- **Hires gap root cause:** Was `client.test = true` filtering out Wolt (551K candidates). Andy confirmed: do NOT use `client.test`.
- **Candidate table vs events:** Andy explicitly said to always use `candidate_stage` (wide-form with date_* columns) for metrics. Events only for: Positive Response, actual screen note verification, LinkedIn views.
- **Filter rules:** Andy provided definitive written answers (see Q4 above).
- **Per-TA validation:** 95% exact match (73/82 TAs match Power BI week 14). 5 off by 1-2, 4 with larger deltas (Milica/Zelimir multi-client, Meho contacted, Samantha screens).
- **VBR target join:** Confirmed — WBR only shows TAs in the target spreadsheet, joined by (week × client × TA).

### STILL OPEN (priority order)
1. **Stage mapping** (Q0 #1) — Which stageType is Power BI's "Actual Screens"? Recruiter Screen (270 events/wk14) or date_screen_actual in candidate_stage?
2. **Hires source** (Q0 #2) — Power BI shows 11-13 hires week 14. candidate_stage.date_hired gives 13-17 (pre-VBR-join). Need to confirm this is the right column.
3. **Sourced definition** (Q1) — "Candidate created" events vs "first Contacted" per candidate for TS metrics?
4. **Contacted double-count** (Q2) — Events or distinct candidates? Andy counts events (matches DAX).
5. **External recruiter asymmetry** (Q3) — Andy confirmed: NOT excluded from TA weekly. Only from conversion rates.
6. **Team split** — How are TAs split into "Ponies & Unicorns" vs "Dolphins & Whales"? Manager hierarchy? Manual?
7. **TA Weekly Note tab** — Need to export from Google Sheet or set up API access. Structure unknown.
- Frantisek's honest take on the smallest Keboola plan we could survive on
- An intro to Mikhail
- The Bubble extractor source code, or a path to get it
