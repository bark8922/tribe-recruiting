# Intake Tool — Data Architecture & Future-Proofing Notes

**Date:** June 22, 2026
**Purpose:** Capture how the intake data flows today, where we want it to go (semantic search via TribeBot), and the cheap things to lock in now so the future build is easy. Not a build plan, a decisions-and-prep note.

---

## Current state

The flow today: extension transcribes an intake/kickoff call → auto-populates the Intake Success Guide (a Google Doc in a shared Drive folder) → posts a structured payload to our Supabase Edge Function `intake-ingest` → row lands in `intake_records` (Supabase project `tribe-job-intel`).

The `jobs` table in the same Supabase project is fed separately by a scheduled Bubble → Keboola → Supabase sync (roughly daily). It's the structured list of every role, with TA and sourcer IDs attached.

### Fixed in this session (June 22)
- Added `hiring_manager_email`, `hiring_manager_linkedin`, `hiring_manager_id`, and `bubble_job_id` columns to `intake_records`.
- Redeployed `intake-ingest` (v8) so it actually stores the hiring-manager fields and always preserves the raw Bubble job ID in `bubble_job_id` (even for brand-new roles not yet synced).
- Added `reconcile_intake_job_ids()` + an hourly pg_cron job that backfills `job_id` once a role syncs into `jobs`. No more permanent nulls.
- Verified Rodrigo's two post-fix test intakes (Upvest, Sauce Labs) landed with all fields.

---

## The end goal

Be able to semantic-search across two datasets from Slack via TribeBot (Onyx): the intake calls, and the TA/Sourcer weekly roundup. The result is a searchable knowledge base of "every job we've worked on" and what we learned on each.

---

## Architecture options

The important reframe: there are two different jobs to do, and they want different tools.

- **Conversational search / RAG** (ask TribeBot, "find intakes like this Berlin backend role") → a document-retrieval problem. Onyx is built for it.
- **Structured queries, linking, reporting** (count by TA, join intake → job → pipeline, correlate with the weekly roundup) → a database problem. Supabase is built for it.

These are complementary, not competing.

**Option 1 — Onyx pointed at the existing Google Drive folder.** Native connector, near-zero build, works today. Onyx indexes the actual intake guides; TribeBot answers over them. Cheapest path to the search goal. Limitation: only knows what's written in the docs; no structured filtering or joins.

**Option 2 — Onyx fed from Supabase via its ingestion API.** Only worth it if you want search that filters on structured metadata (e.g. "intakes by Iryna on active Wolt roles"). More build + maintenance, and it largely duplicates the doc text the Drive connector already gives. Not recommended unless filtered search becomes a real need.

**Option 3 — Supabase as the structured backbone, no Onyx for it.** The DB handles linking, reporting, reconciliation, and weekly-roundup correlation. Queried programmatically or from a dashboard, not via chat.

**Recommended: hybrid.** Onyx + Drive for conversational search when you want it. Supabase as the structured system of record. The database is the join table and reporting layer, not the search index.

Note: because Onyx does its own embedding, we do **not** need to build an embedding pipeline in Supabase. The `embedding` column can stay empty. Don't spend time on it.

---

## What to lock in now (cheap future-proofing)

The two foundations are already in place:

1. **Join keys.** `bubble_job_id` on every intake row + hourly reconciliation to `job_id`. This links intake → job → TA/sourcer → pipeline.
2. **The Drive bridge.** Every intake row stores its `google_doc_url`. This connects the structured DB row to the exact Drive doc Onyx will index, so a search hit can always be traced back to structured data and vice versa. Keep this sacred.

The gap is metadata richness (see table below). Capturing it at creation time is nearly free; backfilling later is painful.

---

## Metadata inventory (fill rates across 48 records)

| Field | Filled | Best source | Action |
|---|---|---|---|
| client | 92% | Auto from call | Good |
| position | 96% | Auto from call | Good |
| location | 96% | Auto from call | Good |
| hiring manager name | 60% | Auto from call | Improve prompt/extraction |
| hiring manager email / LinkedIn | now flowing | Bubble (via extension) | Just shipped, monitor |
| responsible recruiter (TA) | 10% | **Derive from `jobs.ta_id`** | Stop relying on manual entry |
| sourcer | not captured | **Derive from `jobs.sourcer_id`** | Add via job join |
| department | 19% | Bubble | Pull from Bubble, not manual |
| employment type | 10% | Bubble / manual | Low priority |
| seniority | not captured | Auto from call / Bubble | Add to doc + row |

The pattern: fields the AI extracts from the call are strong (90%+). Fields that rely on a human typing them, or that live in Bubble, are weak. The fix is to derive them from the join instead of asking people to fill them in.

---

## Easy wins

1. **Derive TA + sourcer from the job, not manual entry.** Responsible recruiter is 10% filled because it's a manual field. We already store `bubble_job_id`; once reconciled, join to `jobs` to get `ta_id` and `sourcer_id` automatically. Kills the weakest field with zero human effort.
2. **Add a clean header block to the Google Doc template.** Client, location, seniority, hiring manager, TA. This makes the Drive→Onyx path fully searchable with no DB involvement, and improves the docs for humans too. The extension controls the template, so this is a small change.
3. **Pull department / employment type / seniority from Bubble** via the same endpoints already feeding the hiring-manager fields, instead of expecting them in the call.
4. **Keep the weekly roundup in the same shape** with the same keys (`bubble_job_id` / `job_id`, TA, week) so the two datasets can be correlated and fed to Onyx the same way later.

---

## Ask for Rodrigo (today's meeting)

- Can the extension/endpoints attach `ta_id` / `sourcer_id` (or let us derive them from the job join) so we stop depending on the manual "responsible recruiter" field?
- Can we add a structured header block (client, location, seniority, HM, TA) to the intake Google Doc template?
- Can department / employment type / seniority come from Bubble alongside the HM fields?
- Confirm the HM fields + `bubble_job_id` now look right on his end after the v8 fix.

---

## What NOT to build yet
- The Supabase `embedding` pipeline (Onyx re-embeds; redundant).
- A custom Supabase→Onyx connector (Drive connector covers search far more cheaply).
- Anything Onyx until the data is clean and the metadata is rich. That prep is the real prerequisite.
