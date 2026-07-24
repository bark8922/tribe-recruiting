# Recruiting Funnel Stages — Build Plan & Open Questions (DRAFT)

Status: draft for scoping. Nothing here is confirmed. The point is to dig into each part with Jacopo and Mikhail before we build.

Owner: Blake · Backend: Mikhail · Process/rollout: Jacopo · Last updated: 2026-07-15

---

## 1a. Update (2026-07-24)

First candidate movement observed: one candidate moved through Interview 1/2/3 on 2026-07-22 on a real job (Sales Executive Brands, recruiter Milica Mladzic), still on default stage names. Reads like a first trial rather than organic volume. Pipeline adoption is growing: 9 jobs now have the new stages configured (up from 2). Not enough volume yet to build/validate conversion metrics.

Action taken: transform build kicked off. Live SQL pulled from Keboola ([PROD] Data preparation V2, config 375145203). Proposed changes drafted in `stage_transform_changes_DRAFT.sql`, continuity-preserving.

Read-only proof run against live data (2026-07-24) found and fixed three things before any code touched the pipeline:
1. Snowflake syntax: LATERAL FLATTEN can't sit left of a JOIN; the Jobs."stages" explode now lives in a CTE (validated working).
2. Per-rung population works: the test candidate's Interview 1/2/3 dates resolve correctly.
3. Backfill rule refined to a GUARDED backfill (not "no backfill"): rungs are backfilled from offer/hire only when (a) the candidate reached offer/hire, (b) that offer/hire is on/after the 2026-07-14 go-live, and (c) the job defines that rung. This matches how the legacy stages already behave, excludes the 563 pre-go-live candidates from fabricated history, and keeps mid-funnel drop-offs visible (they have no offer/hire, so nothing backfills for them). Legacy cascade untouched.

Also noted: all 24 rung-configured jobs currently carry all three rungs (default template), so the pipeline-aware/variable-round logic is correct but a no-op until recruiters trim rungs.

BUILT + SCHEDULED (2026-07-24): rather than edit the fragile core transform (no dev branch available via MCP), implemented the rung logic as an additive, isolated transformation "Recruiting - candidate stage rungs (Interview 1/2/3)" (config 01ky9xkbr9fzswz7meddr73agn). It reads only published tables (out.c-reporting-v2 event + candidate_stage, Bubble Jobs/stages/stagesType) and writes out.c-Recruiting---candidate-stage-rungs-Interview-123.candidate_stage_rungs (candidate_id, job_id, has_int1/2/3, date_interview_1/2/3). Ran successfully (565 rows); validated: only the 1 real candidate has rung dates, all 564 pre-go-live hires correctly blank, 0 wrongly backfilled. Wired into the "4x daily - NEW" flow (118392817) as a new phase after PROD V2, continueOnFailure=true. Nothing existing was modified. Note for the dashboard phase: Keboola stores NULLs as empty strings, so treat '' as no-date. Dashboard + aggregation layer still held until real volume arrives.

## 1. Where things stand (as of 2026-07-15)

- New stage types are live. Mikhail created `Interview 1`, `Interview 2`, `Interview 3` on 2026-07-14, positioned at 6/7/8, with `Offer` and `Hired` repositioned to 9/10 so they sit after the new rungs.
- Two jobs have the new pipeline set up (both yesterday), still on default names ("Interview 1/2/3"), no client-specific renames yet. Looks like the first/test setups.
- **Zero candidates have moved into the new stages yet** — no events, nobody currently sitting in Interview 1/2/3, in both the raw Bubble data and the reporting layer. So it is plumbed but unused.
- Old duplicate enum rows still exist (Offer@3, Hired@4, Offsite@4, Final Interview@5, one blank-named row@5). Not breaking anything, but worth having Mikhail tidy so the enum isn't confusing.

Implication: no data to rebuild against yet and no rush. Dashboards stay on the current model and keep working. We prep the changes now and build once a handful of real candidates have flowed through, so we validate against known cases instead of guessing.

---

## 2. How the current model works (plain version)

The reporting model tracks each candidate through a fixed set of stage-date columns: `date_screen`, `date_interview`, `date_offer`, `date_hired`.

- Recruiter Screen is its own column (`date_screen`) and stays separate. It is NOT collapsed with anything.
- The collapse is at `date_interview`: today that single column gets filled by BOTH the "Move to ATS" handoff AND any actual interview move, because historically Move-to-ATS/Onsite were the only things in that band so nobody split them. The model even uses `date_interview` as the de-facto "moved to ATS" date.

To show Interview 1/2/3 as distinct rungs, that one field has to be split into separate dates: Move-to-ATS, Interview 1, Interview 2, Interview 3.

---

## 3. Design principles (agreed direction)

1. **Positional, generic stage types.** Interview 1/2/3 = position in the process, not semantic role. The stage TYPE drives the data; the stage NAME is free text the recruiter can set to anything ("Karat", "Coding", "HM Interview") without affecting reporting.
2. **Keep "Move to ATS" as its own stage, same name.** It is the fulcrum all historical WBR/MBR/OKR reporting is built on. Do not rename or delete it. Add the interview rungs after it.
3. **Fixed anchors, variable middle.** Recruiter Screen and Move to ATS at the top, Offer and Hired at the bottom, are shared by every role. The interview rungs in between are variable-depth — a role uses only as many as it has.
4. **Pipeline-aware funnel (critical).** Which rungs a job has is defined in the `stages` table when the job is created. Reporting must respect that per job (see §5.2).

---

## 4. The rebuild, layer by layer (scope to dig into)

Framed as areas to scope, not a final spec. Each needs a closer look before we commit.

**Layer 1 — Transform (Keboola Snowflake SQL). The bulk of the work.**
- Split `date_interview` into a Move-to-ATS date plus `date_interview_1 / _2 / _3` (max 3 rungs).
- Keep the existing "Move to ATS" measure intact and pointed at the same underlying event, so historical numbers don't move.
- Update the hardcoded bucket logic that currently keys on `('Offsite','Interview')`, `'Offer'`, `'Hired'`, etc.
- Make the backfill pipeline-aware (see §5.2).

**Layer 2 — WBR/MBR weekly aggregation tables.**
- Add Interview 1/2/3 counts alongside the existing contacted / screens / ATS / offers / hires columns.
- Decide which of the new rungs actually surface on WBR vs MBR (Jacopo wanted the MBR to hold the full pipeline and the WBR to stay lean).

**Layer 3 — Render-to-JSON (data.json).**
- Expose the new fields so the front end can read them.

**Layer 4 — React dashboards (App.jsx).**
- Add the new rungs to: Overview funnel, per-project dashboards, WBR, MBR.
- Update conversion math to be pipeline-aware.
- Do Jacopo's decluttering at the same time (trim some 12-week metrics on the MBR so it's not too crowded).

---

## 5. The tricky bits (where we need to be careful)

**5.1 Splitting Move-to-ATS from interviews.**
The single most important transform change. Until `date_interview` is split, Interview 1/2/3 can't be distinguished from the ATS handoff. Everything downstream depends on this being right.

**5.2 Pipeline-aware backfill and conversion (variable round counts).**
The existing rule — if someone jumps from screen straight to hire we assume they passed the middle stages and backfill them — still applies. This is also how "Move to ATS" is defined going forward: **reached if any later stage is reached.** If a candidate lands in Interview 1 (or anything after), they are counted as having passed Move to ATS, whether or not the recruiter explicitly moved them through it. This makes Move to ATS behavior-proof — it doesn't matter whether a recruiter parks a candidate there until an interview is scheduled or jumps straight to Interview 1; the data lands the same. Actual recruiter usage (park vs skip) is something we observe from live data, not something we need to decide up front.

The backfill has to become role-aware:
- Backfill on hire/offer fills only the rungs that job actually has. A job with Interview 1 and 2 (no 3) gets 1 and 2 backfilled; Interview 3 stays N/A.
- A rung a job doesn't have is **N/A, not a drop-off**. It is excluded from that rung's denominator entirely.
- So "Interview 3 → Offer" conversion is computed only across jobs that have an Interview 3. A 2-round role is never penalized for a missing third round.
- Source of truth for "which rungs does this job have" = the `stages` rows created for that job.

**5.3 Continuity (no trend snap).**
Whatever `Offsite` / `date_interview` feeds today must keep feeding the same series after the switch, or the trend lines break at the cutover date. Map the old-to-new field mapping explicitly.

---

## 6. Decisions

**Resolved**
- **"Move to ATS" stays, same name, non-negotiable.** It is the historical fulcrum for "passed recruiter screen / moved to next stage." Going forward it is defined by the backfill rule (reached if any later stage is reached — see §5.2), so it is robust to however recruiters actually use it in practice. We learn the real usage pattern from live data rather than pre-deciding it.

**Still open (but NOT blocking the build)**
1. **Does a pre-interview technical test count as its own rung?** e.g. Aviv's Karat before any human interview — is that Interview 1, or just part of the Move-to-ATS handoff? This affects what "Interview 1" means, not what "Move to ATS" means. Can be informed by real usage or a light check with Yoko; does not hold up the build.
2. **Ground rules for the team.** Written definitions of when to move a candidate into each stage, circulated before real volume flows, or the first data we build against is messy.
3. **Enum cleanup.** Mikhail to remove/re-order the old duplicate rows (Offer@3, Hired@4, Offsite@4, Final Interview@5, blank@5).
4. **WBR vs MBR scope.** Which rungs show where.

---

## 7. Sequencing

1. **Now:** dashboards untouched (no data). Move-to-ATS decision is resolved (keep + backfill-defined). Draft and send ground rules. Mikhail tidies the enum. Technical-test question (§6.1) left to observe from live data.
2. **When ~a handful of real candidates have flowed through Interview 1/2/3 (give it a week or two of live use):** build the transform changes and validate against those known candidates.
3. **Then:** wire the aggregations, data.json, and dashboards, keeping pre/post continuity.
4. **Monitor:** re-run the usage check every few days so we know the moment real data starts arriving.

---

## 8. Questions to resolve, in order

- Jacopo: ground rules sign-off and rollout comms (§6.2).
- Mikhail: enum cleanup (§6.3); confirm the `stages` rows reliably capture each job's rung set (needed for §5.2).
- Blake: finalize the transform field design (§5.1) and the old-to-new continuity map (§5.3). Move-to-ATS decision is settled, so this is not blocked.
- Later / non-blocking: whether a pre-interview technical test is its own rung (§6.1), informed by live usage or a light check with Yoko.
