# Spec: Credit screens to the person who ran them (not the job's recruiter)

Status: Draft for sign-off. Do NOT implement until approved.
Author: prepared for Blake, 2026-06-19
Scope owner: recruiting dashboard / WBR-MBR Keboola flow

## 1. Goal

Make the per-person screen metric credit the person who actually ran the screen, while the screen still counts on the correct job and client.

- Per-person (TA) view: a screen is credited to whoever ran it (the evaluator).
- Per-job / per-client funnel: the screen stays on the job it belongs to and its client. These numbers do not move.
- Contacted: unchanged. Stays attributed to the job's recruiter (per Blake, 2026-06-19).
- Everything else (ATS, offers, hires, MBR): unchanged.

Driver: recruiters cover screens for each other (e.g. when someone is out). Today the credit follows the job's recruiter, so a TA who covers a colleague's screen gets no credit and the absent recruiter is over-credited.

## 2. Current behavior

`wbr_weekly` is built in the Keboola transformation "WBR/MBR weekly aggregations"
(keboola.snowflake-transformation, config `01kpr0tr0dt5ryf96a5zk85bx7`, block "Build wbr_weekly").

Every metric (contacted, screened, actual_screens, ats, offers, hires) is attributed to the **job's recruiter** (`job.job_recruiter`), grouped per (client, TA, ISO week). Screens specifically use `candidate_stage.date_screen_actual`, which is derived (in transform `375145203`) from the candidate's `Evaluation` event, with fallbacks to a recruiter screen note, then the interview date.

Result: a screen Lejla ran on Alexandra's req is credited to Alexandra, because Alexandra is the job recruiter.

## 3. Proposed change

In the "Build wbr_weekly" SQL, change only the two screen CTEs (`screened` and `actual_screens`) so the **TA dimension is the evaluator** (the person who ran the screen), while **CLIENT stays the job's client**. Leave `contacted`, `ats`, `offers`, `hires` exactly as they are (job recruiter).

Mechanics:
- The doer = `event.who_created_event` where `event.event_type = 'Evaluation'` (this is the field validated against Lejla's week 25: it returns her true ~19).
- For each screened candidate, keep the screen's client = the candidate's job's client (unchanged), but set TA = normalized evaluator.
- The existing `keys` UNION in the SQL already collects all (client, TA, week) combinations, so rows for a covering TA (e.g. Lejla on AVIV) appear automatically.
- Each (client, TA, week) row will then carry contacted from where that TA is the job recruiter and screens from where that TA ran the screen. Both attributions are correct per their own rule; they coexist on the same row keyed by TA.

Do NOT use `out.c-reporting-v2.screen.user_recruiter` for this. It is "-not available-" for current data (verified 2026-06-19), so it cannot be the doer source. Use the Evaluation event.

### 3a. No role filter — DECIDED 2026-06-19

Decision: credit the screen to whoever logged the Evaluation, regardless of their role. Every Evaluation event is treated as a screen, which is consistent with the current metric (an Evaluation already sets the actual-screen date today). We do NOT exclude sourcers, because a sourcer occasionally runs a real screen and should get credit for it.

Rationale: this is purely an attribution change (who gets credit), not a change to what counts as a screen. A blanket "recruiters only" filter was rejected because it would drop the rare legitimate sourcer screen.

The three would-be new-credit people in the window, for the record (roles not used as a filter, just context):
- Mariam Chkhikvadze — Talent Acquisition Partner L2 (+12).
- Mateja Joković — Talent Acquisition Partner L2 (+2). (Note the diacritic; matched via diacritic-folded lookup.)
- Nare Avetisyan — Sourcer L2 (+1, kept; a sourcer who logged a screen evaluation).

Open data-quality note (separate from this change): if it later turns out some "Evaluation" events are sourcing assessments rather than real screens, that is a metric-definition issue affecting today's totals too (those evals already count as screens under the job recruiter), and would be handled separately.

Population note (decision point): keep the existing `actual_screens` population (candidates with `date_screen_actual` in the week) and just re-attribute the TA to the evaluator where an Evaluation event exists. For the small number of screens whose `date_screen_actual` came only from the screen-note or interview fallback (no Evaluation event, so no clean doer), fall back to the job recruiter. This preserves the total screen count exactly and only moves credit where we genuinely know who ran it. (Alternative, cleaner but changes totals slightly: redefine actual_screens as distinct candidates with an Evaluation event per evaluator. Not recommended unless we want to drop the fallback population.)

## 4. Name normalization — reuse what already exists

The evaluator name (`who_created_event`) and `job_recruiter` are both free-text name strings, so they must be normalized to a single canonical identity before attributing, or the same person splits across spellings.

This is already solved in the stack. Reuse, do not rebuild:
- Canonical user list: `out.c-reporting-v2.user` (282 users: user_id, user_name, user_email, role_current).
- `pipeline/refresh_team_leads.py` already implements diacritic-folded matching plus a manual `NAME_OVERRIDES` map (e.g. `"Lejla Silva" -> "Lejla Dizdarevic"`) for married names and known variants.

Plan: normalize both `who_created_event` and `job_recruiter` through the same canonical mapping (trim + collapse internal whitespace, diacritic fold, then apply the existing override map / join to the `user` table) so they resolve to one identity. This is what catches the "Jelena Lacmanovic" (one space) vs "Jelena  Lacmanovic" (two spaces) case below.

## 5. Impact (sandbox, weeks 22-25 2026)

Most TAs are unaffected (delta 0). Only coverage cases move. Net per-TA change in screens:

| TA | Current (job recruiter) | Proposed (doer) | Change |
|---|---|---|---|
| Mariam Chkhikvadze | 0 | 12 | +12 |
| Ella Darie | 14 | 0 | -14 |
| Alexandra Richiteanu | 81 | 76 | -5 |
| Filip Nogowski | 76 | 72 | -4 |
| Lejla Silva | 40 | 44 | +4 |
| Gustavo Loureiro Castro | 21 | 25 | +4 |
| Kristina Colovic | 73 | 71 | -2 |
| Wladyslaw Gadomski | 43 | 45 | +2 |
| Iryna Dyda | 103 | 104 | +1 |
| Nare Avetisyan | 0 | 1 | +1 |

Total screens are conserved; credit just moves from absent recruiters to coverers.

## 6. Edge cases to handle

- Name variants (the "Jelena" double-space split, +21/-21 in the raw sandbox): handled by Section 4 normalization. This MUST be in place or the change creates phantom swings.
- Ella Darie 14 -> 0: she is the recruiter on 14 screened candidates but ran 0 evaluations herself. Confirm she genuinely does not run her own screens (likely a lead whose reqs are covered) before shipping, since this is a large, fully-attributed-away shift.
- Multiple evaluators on one candidate in a week: pick one doer (latest Evaluation event by timestamp) so a screen is not double-counted.
- Fallback-only screens (no Evaluation event): keep job-recruiter attribution (Section 3 population note).
- Archived candidates / test jobs: keep the existing exclusions.

## 7. What changes vs what stays

Changes:
- `screened` and `actual_screens` CTEs in the "Build wbr_weekly" transform: TA = normalized evaluator; client unchanged.
- Add a normalization step (reuse user table + NAME_OVERRIDES) applied to evaluator and job_recruiter.

Stays exactly the same:
- Job-level and client-level funnels (screens still counted on the job/client).
- Contacted, ATS, offers, hires attribution (job recruiter).
- MBR tables.
- The dashboard front end (it reads the same `wbr_weekly` shape; only the TA on screen rows shifts).

## 8. Validation plan (before enabling in production)

1. Run the modified SQL into a scratch table and diff against current `wbr_weekly` for weeks 22-25.
2. Confirm total screens per (client, week) are unchanged (job/client funnels must not move).
3. Confirm sum of per-TA screens = total screens (no double counting from multi-evaluator candidates).
4. Spot-check the four signal cases: Lejla (+), Mariam (+), Alexandra (-), Ella (- to 0).
5. Confirm no new phantom splits (every name resolves to one canonical identity; re-check Jelena specifically).
6. Confirm contacted numbers are byte-identical to current (we did not touch them).

## 9. Rollback

The change is isolated to two CTEs in one transformation (config `01kpr0tr0dt5ryf96a5zk85bx7`, currently version 42). Revert the config to the prior version to restore the old behavior; the next scheduled run rebuilds `wbr_weekly` and the dashboard follows.

## 10. Open questions for Blake — RESOLVED 2026-06-19

1. Ella Darie going to 0 screens: CONFIRMED OK (leads' covered reqs move to the doer).
2. Fallback rule (screens with no Evaluation event stay on job recruiter): CONFIRMED OK.
3. Screens is the only metric to switch now (contacted, ats, offers, hires stay job-recruiter): CONFIRMED.

## 11. Validation result (read-only, weeks 22-25 2026)

- Total screens: 1,455 before = 1,455 after (conserved).
- Rows reattributed: 27 of 1,455 (~2%).
- Distinct credited TAs: 32 -> 34 (two coverers who get 0 today start getting credit).
- Per-TA deltas match Section 5.

Ready to implement in the transform pending final go-ahead.
