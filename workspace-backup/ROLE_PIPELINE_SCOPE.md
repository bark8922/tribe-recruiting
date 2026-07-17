# Role Pipeline - Build Scope (agreed 2026-07-15, not yet built)

Per-role candidate deep-dive for the recruiting dashboard. Successor to Andy's PBI "Data Download" page and the Rodrigo candidate-list ask that CSV_EXPORT_SCOPE.md parked. Scoped with Blake 2026-07-15; all design decisions below are locked. Sizing queried live from Snowflake (out.c-reporting-v2) on 2026-07-15.

## What it is

A recruiter (Sam being the driving example) picks a client, picks one of her roles, and sees every meaningful candidate on that role: name (LinkedIn link), current title/company, current stage, the date they hit each stage, sourcer, source, and drop reason if they're out. Export CSV of the current view, 5,000-row cap. Own lazy-loaded gz, loads nothing until the tab opens (Candidate Finder pattern).

## Decisions locked (Blake, 2026-07-15)

1. **Candidate floor = positive response or beyond.** Anyone who replied to outreach, or reached any later stage (screen/interview/offer/hire). Raw contacted-no-reply people get NO row; they appear only in the per-role aggregate funnel strip (contacted / positive / screens / ATS / offer / hired counts).
2. **Role window = open roles + roles CLOSED in the last 6 months** (revised 2026-07-15 after Blake correctly rejected the activity-only rule: closed roles get archived, and recently closed ones must stay visible even if their pipeline went quiet earlier). Complication: Bubble stores no archive date, only an `archived` flag and a `Modified_Date` that changes on ANY edit. Validated live: ~580 roles whose activity ended in 2020-2024 were "modified" in the last 6 months (bulk edits), so Modified_Date alone would flood the picker with zombie roles. Two-part rule instead:
   - **Backfill (roles archived before this ships):** an archived role is in scope if its last candidate activity was within 6 months, OR its Modified_Date is within 6 months AND falls within 90 days of its last activity (i.e. the edit plausibly WAS the closure). Catches "closed recently, quiet earlier" without the zombies.
   - **Going forward (exact):** the transform maintains a small state table (`role_pipeline_archive_log`: job_id, first date seen archived), appended each run. From ship date onward the 6-month window runs off the true archive date, no proxies. Recomputed every run (4x/day), self-maintaining; the 6-month horizon and 90-day tolerance are single constants, easy to change.
3. **Visibility = all employees**, behind the normal dashboard login, exactly like the Candidate Finder (which is not role-gated either).
4. **Standalone tab** ("Role Pipeline"), not a drill-down inside another tab.

## Sizing (live, 2026-07-15)

| Population | Rows |
|---|---|
| Jobs in scope (open + closed within 6 months, hybrid rule above) | 866 |
| Candidates at the chosen floor (positive response+) | **40,996** (~2.5-3MB gz, lighter than the Finder's 5.2MB) |
| For reference: everyone contacted on those jobs | ~117k (rejected, bloat with no list value) |
| For reference: recruiter screen+ only | ~23k (rejected, hides the in-conversation pipeline) |
| For reference: naive archived+Modified_Date rule | 1,446 jobs / 84k rows (rejected, ~580 zombie roles from bulk edits) |

## Build plan

**1. New Snowflake transform `Role Pipeline`** (clone the shape of the Candidate Finder transform, config 01kvzgpgwh38awepha7eey08pe):

- Reads candidate, candidate_stage, job, client, talent (+ talent_position/talent_employer for current title/company; reuse the Finder's LinkedIn-artifact title scrubs).
- Also reads the raw Bubble `Jobs` table (in.c-kds-team-ex-bubble-io-122491135, for `archived` + `Modified_Date`) and maintains `role_pipeline_archive_log` (job_id, date first seen archived) as an incremental output fed back as input each run.
- Job scope CTE: non-archived OR closed within 6 months per the two-part rule in decision 2 (tracked archive date once available, backfill heuristic otherwise); test jobs/clients excluded (standard exclusions).
- Candidate floor: is_candidate_reacted = 'true' OR any of date_screen / date_screen_actual / date_interview / date_offer / date_hired non-empty.
- **Duplicate handling (validated 2026-07-16):** exclude rows with `is_candidate_duplicated = 'true'` — Bubble's flag is reliably set on same-talent-same-job duplicate rows (verified on 7 live pairs; note the UNflagged row is canonical even when the flagged row is further progressed — team's own convention). Same-human-different-talent duplicates (accents, "PhD." suffixes, second profiles) have NO flags set (talent-level dedupe fields unused) — do NOT auto-merge; surface a ⚠ near-dup badge via normalized-name match (strip accents/whitespace/suffixes) as a hygiene prompt.
- Carry `is_candidate_archived` — archived counts as dropped (distinct gray chip), per Blake 2026-07-16.
- Output A `role_pipeline_candidates` (~40k rows): candidate_id, job_id, job_title, client, name, linkedin, current_title, company, location, country, stage_current (raw Bubble value), stage_current_type (5-bucket), date_contacted, date_screen, date_screen_actual, date_interview, date_offer, date_hired, sourcer, source, dq_reason, is_disqualified.
- Output B `role_pipeline_jobs` (~833 rows): job_id, job_title, client, recruiter, is_archived, days_open, date_created + aggregate funnel counts (contacted, positive, rec_screen, act_screen, interview/ATS, offer, hired). Powers the role picker and the header funnel strip, and carries the contacted totals the row-level list deliberately drops.

**2. Render wiring** (kds-team.app-custom-python config 01kpr863...): add both tables to the input mapping, add `build_role_pipeline()` to keboola_entry.py producing `role_pipeline.json.gz` ({generated_at, jobs[], candidates[]}) pushed alongside finder_data.json.gz. Best-effort like the Finder build: any failure logs a warning and cannot break the main dashboard refresh. Storage-only config change, encrypted tokens untouched.

**3. Frontend** (App.jsx, new standalone tab "Role Pipeline", registered for all roles): lazy fetch + DecompressionStream gunzip (copy the Finder's loader). **UI locked with Blake 2026-07-16 via the validation-board iterations (see `role_pipeline_validation.html` in this folder — that file IS the design spec):**
- **Board view, not a table.** One column per stage (Responded, Recruiter screen, Moved to ATS, Onsite, Offer, Hired), compact candidate cards (name single-line ellipsized, stage date + sourcer, small type) sorted by latest movement. Hired cards green-accented.
- **Selectors, not tabs:** Recruiter → Client → Role cascading dropdowns (searchable combobox in production, Finder-style), role options labeled "Client — Role (N active)".
- **Dropped/declined/archived collapsed** behind a per-column expander ("N declined / no progress", "N dropped here"), dimmed cards with reason chips; global "show dropped" toggle; search auto-expands matches.
- **Stale badge:** active card with no stage movement for 30+ days gets ◷ Nd (not on Hired). Header stats: in play (N stale 30d+) · dropped/declined/archived · hired · contacted total.
- **"Check a week's numbers" panel** (born from Samantha's WBR-discrepancy Slack question): ISO-week selector (Mon–Sun, same windows as WBR) + metric chips (Contacted / Recruiter screens / Actual screens / Moved to ATS / Offers / Hires) each showing its weekly count; selecting one shows a per-sourcer breakdown and the exact candidates behind the number, including later-dropped ones. Add an **"all my roles" mode** so a recruiter can reconcile their personal WBR weekly total in one view (WBR counts a person across all roles).
- **Weekly CONTACTED must come from the full aggregate, not the floored list** (found 2026-07-17 reconciling Sam's 33-vs-12): the candidate list starts at positive-response, so fresh unanswered outreach has no rows and a list-derived Contacted chip undercounts vs WBR. Ship weekly contacted counts per (job, sourcer, ISO week) in the aggregate table (extend `role_pipeline_jobs` or a small `role_pipeline_weekly` output) and render the chip as e.g. "33 contacted · 12 responded so far (listed)". All other metrics reconcile from the list by definition (a screen/ATS/offer/hire date implies the row passed the floor).
- **Person attribution = both flavors:** the person view aggregates roles they own as recruiter AND candidates they sourced on other roles (Sam's Enam sourcing is credited to her on WBR; role badges make the split visible).
- **"Hired but flagged" review state:** hired candidates who also carry a DQ flag (e.g. Ahmed Jamal / Glovo) sit behind the Hired column's expander labeled for review, never silently counted or dropped.
- Export CSV on the current view, 5k cap, UTF-8 BOM.

**4. Flow wiring**: add a `role-pipeline` task to the build flow's transformations phase (flow 01kpqyq1pz6qpmk7m9s4qx8gmg), same as candidate-finder. Reads only reporting-v2, independent of other transforms, refreshes 4x/day.

## Effort

Roughly 2 days: transform + validation against known roles ~0.5d, render wiring + manual run ~0.25d, frontend tab ~1d (board + week-check panel per the locked UI; the validation HTML is a working reference implementation to port), deploy + verify live ~0.25d. No new infra, no Bubble or extractor changes.

**Running cost (measured from telemetry, 2026-07-15):** the Candidate Finder transform (same input scale, same 4x/day cadence) bills ~0.065 credits/run, ~8 credits/month. Role Pipeline will match that, plus ~1 credit/month of extra render-step runtime: **~10 credits/month total**, ~1.5% of the 600-credit allowance, $0 incremental at current usage (~160-300 credits/month used). If credits ever get tight, the lever is cadence: dropping this one task to 1x/day cuts it to ~2 credits/month at the cost of up-to-a-day staleness on this tab only.

## Risks / notes

1. **PII**: names + LinkedIn per role for positive-response+ people, visible to all employees. Softer than v1 of this scope (no never-engaged people) and consistent with the Finder precedent + 5k export cap. Blake OK'd all-employee visibility.
2. **App.jsx truncation gotcha**: Python search/replace, npx vite build before every push (~5,500 lines now).
3. **Six-month edge**: for roles archived BEFORE ship date the close date is a heuristic (last activity, or a Modified_Date that lands within 90 days of it), so a role archived long after its pipeline died shows its activity-end as the close date. From ship date onward the archive log makes it exact. Also note the state table starts empty: the backfill heuristic governs everything for the first months, then fades out naturally.
4. **Stage display**: show raw stage_current, group by stage_current_type (Finder convention).
5. Unrelated: the 25-table aggregate CSV export (scoped 2026-06-11) remains a separate open decision.

## Validation before calling it done

- Pick 2-3 live roles Sam/team know well and reconcile the tab's counts + candidate lists against Bubble directly.
- Check a role that recently archived (should still show) and a long-dead one (should not).
- Export a CSV with diacritics in names and open in Excel (BOM check).
