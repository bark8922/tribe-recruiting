# Meeting Prep: Frantisek — Keboola Review
**Date:** April 10, 2026

---

## 1. KEBOOLA SETUP & COST

**Current contract situation:**
- Keboola contract is tied to Power BI — both locked until **August 31, 2026**
- 60-day notice required before Aug 31 to cancel
- That means notice must go out by **~July 1** at the latest
- Combined Power BI + Keboola = ~€96K/year

**What's actually running in Keboola (Project 855, eu-central-1):**
- **Bucket:** `in.c-kds-team-ex-bubble-io-122527414` (ex-bubbleio-incremental)
- **11 tables** being ingested from Bubble: Events (14.6M rows), Position (8M), Nylas_Email_message (2.5M), duxsoup_messages (2.1M), Talent (1.6M), Candidate (1.3M), Analytic (1.1M), Company (967K), Emails (105K), stages (80K), recruiter_screen_notes (77K)
- **SQL Transformations:** "PROD Data preparation V2" — 4 parts (temp tables → Bubble data → Recruitee data → final tables) + a part 4 Andy added
- **Revenue transforms:** 2 additional SQL blocks (unpivoting + data prep)
- **Talent location:** 1 additional transform

**Questions to ask Frantisek on cost reduction:**

- What Keboola tier/plan is Tribe on? What's the actual monthly/annual cost breakdown?
- What's driving the cost — storage, compute (transformation credits), data ingestion frequency, number of connectors?
- Are there tables or transforms we're paying for that nobody uses? (e.g., Nylas_Email_message at 2.5M rows, duxsoup_messages at 2.1M rows — are these feeding anything?)
- Could we reduce ingestion frequency? (Currently incremental — how often does it sync?)
- Are there any quick wins to cut the bill between now and August while we finish the migration?
- Is the Revenue transformation still needed, or has the financial dashboard already replaced it?

---

## 2. LOGIC QUESTIONS FOR FRANTISEK (things he should know)

These are about *his* SQL transforms — the stuff he built ~4 years ago:

**Stage mapping / category logic:**
- In part 0, there's a big CASE statement mapping stage names to categories (candidate → phone_screen → interview → evaluation → offer → hire). Is this still accurate? Have any new stages been added in Bubble that aren't mapped?
- The Recruitee events parsing is extracting stage names from HTML (`message_short_html` with `<strong>` tags) — is this still the format Recruitee uses, or has it changed?

**Candidate deduplication:**
- `is_candidate_duplicated` and `is_talent_duplicated` — what's the logic? How are duplicates identified and which record wins?

**Final tables — UNION of Bubble + Recruitee:**
- The final_candidate_all, final_talent_all, and final_event_all tables UNION Bubble and Recruitee data. Is Recruitee data still relevant? Is anyone still using Recruitee, or is it all Bubble now?
- If Recruitee is dead, can we drop those transforms entirely?

**Sourcer field:**
- `candidate_sourcer` in final_candidate — where does this come from in his transform? Is it the "first to contact" or the "official sourcer on the job"?

**Currency conversion:**
- `hired_salary_eur` — where does the exchange rate come from? Is it hardcoded, a lookup table, or live?

**The "analytic" and "job_ai_filter" tables (part 4 - Andy):**
- Frantisek may or may not know about these — Andy added them. Ask if he's aware of what they feed.

---

## 3. THINGS FRANTISEK PROBABLY WON'T KNOW

Flag these so you don't waste time going in circles. These are Andy's domain:

- **The 30% DAX patch layer** — Andy does ~30% of the data cleanup in Power BI DAX measures *after* Keboola transforms. Frantisek's SQL gets it ~70% correct. Andy's fixes include: backfilling missing stage dates, handling stage-skip cascading, filtering out test jobs/companies, and currency normalization edge cases. There is NO documentation of this layer.

- **"Current stage is source of truth" rule** — When a candidate gets moved forward by mistake and then back, Bubble logs all events. The rule is to trust the current_stage field and ignore any forward-stage timestamps that contradict it. This is applied in Andy's DAX, not in Frantisek's SQL.

- **Sourcer credit = first to contact** — The actual sourcer credit logic (used in WBR/MBR) is "first person to contact the candidate," NOT the official_sourcer field on the job. Andy implements this in Power BI. Frantisek's transforms just pass through the raw fields.

- **VBR target join logic** — The WBR/MBR report only shows TAs who appear in a separate target spreadsheet, joined by (week × client × TA). That spreadsheet is Andy's, lives outside Keboola entirely.

- **Manager hierarchy from BambooHR** — Must use historical `report_to` as-of the relevant date, not just the latest. Andy handles this outside Keboola.

- **External recruiters** — Count toward hire totals and sourcing metrics, but are excluded from time-to-fill / time-to-find. This filtering is in Andy's layer.

- **Positive Response stage** — Not in the candidate table; only exists in the events table. Relatively new stage, Frantisek's original transforms predate it.

---

## 4. SUGGESTED TALKING POINTS / AGENDA

1. **Quick status check** — Does Frantisek still have admin access? Can he still modify transforms if needed?
2. **Cost walkthrough** — Get him to pull up the Keboola billing/usage page and walk through what's costing what
3. **What can we turn off today** — Identify unused tables, connectors, or transforms that are burning credits for nothing
4. **Migration plan** — Explain that you're building a replacement dashboard (Bubble API → DuckDB → static JSON → Cloudflare) and ask what gotchas he sees
5. **His availability** — Can he support ad-hoc questions through July as you validate the migration? He's the only person who understands the SQL transforms
6. **Handoff of his knowledge** — Offer to do a recorded walkthrough of his transforms (similar to what you did with Andy) before the August cutoff

---

## 5. KEY CONTEXT TO HAVE IN YOUR BACK POCKET

- You have **guest access** to Keboola — you can't create API tokens or run transforms yourself
- Andy leaves **end of April** — after that, Frantisek is the only person who understands the Keboola side
- Martin's MVP scope is just **VBR + Project Dashboard** by end of April — everything else is deprioritized
- Mikhail (still at Tribe) designed Bubble's data model and is the backup brain for event table semantics
- The Keboola SQL files have already been extracted and saved locally for reference
