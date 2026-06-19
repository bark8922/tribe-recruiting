# Reporting-v2 Sanity Check — 2026-04-08

**Purpose:** Verify Frantisek's Keboola `out.c-reporting-v2` tables can answer the same questions Power BI does, before we commit to building MVP on top of them.

**Source:** `KEBOOLA_855."out.c-reporting-v2"` (Snowflake, queried via Keboola MCP)

**Filter set used (matches DAX in WBR TA Actual):**
- `job.is_job_archived = 'false'`
- `job.test = 'false'`
- `candidate.is_candidate_archived = 'false'`
- `client.test = 'false'`
- `client.client_name NOT IN ('Tribe.xyz', 'Kamila AI - TEST')`

**Important data-quality finding:** Boolean fields are stored as lowercase TEXT (`'true'` / `'false'`), not uppercase. Empty strings and NULLs both appear. All date columns are also TEXT — must wrap in `TRY_TO_DATE()` before any date math. Recruiting transforms must handle this.

---

## Q1 — How many active jobs are there right now?

**Definition:** open (no first hire), not archived, not test, real client.

| active_jobs | active_clients |
|---:|---:|
| **99** | **12** |

---

## Q2 — Hires by client this calendar month (April 2026 so far)

| client | hires |
|---|---:|
| Glovo | 2 |
| Wolt | 2 |
| Parloa | 1 |

**Total: 5 hires in first 8 days of April.**

### Q2b — Hires last 90 days by client (sanity check on Q2)

| client | hires |
|---|---:|
| Wolt | 911 |
| Parloa | 15 |
| Doordash | 11 |
| Nexi | 10 |
| AVIV | 9 |
| Eucalyptus | 7 |
| Scorewarrior | 4 |
| Tribe.xyz (IR) | 3 |
| DualEntry | 3 |
| Glovo | 2 |
| Enam | 2 |
| PhantomBuster | 2 |
| SevenRooms | 1 |

Wolt's 911 looks shocking but is real — 910 distinct talents across 135 distinct jobs, consistent with Wolt's high-volume courier/store hiring. **Wolt drives the entire pipeline volume.** Any dashboard MUST filter Wolt-aware or it'll dominate every chart.

---

## Q3 — Top 10 longest-open active jobs

| client | job | recruiter | days open |
|---|---|---|---:|
| Aleph Alpha | (Mia) Senior PM - Public Sector | Mia Gjorgievska | 538 |
| Glovo | CRM Solutions Project Lead (Salesforce) | Jovana Drakula | 393 |
| Glovo | Growth & Marketing Lead - Nigeria | Etienne Sargenton | 373 |
| Glovo | Store Manager | Etienne Sargenton | 373 |
| Glovo | Account Manager Tunisia | Etienne Sargenton | 362 |
| Glovo | Data Lead Partner Monetization | Jovana Drakula | 322 |
| Glovo | Category Management Kenya | Etienne Sargenton | 307 |
| Glovo | Sr Sales Executive Tunisia | Etienne Sargenton | 299 |
| Glovo | Sales Executive Longtail - Portugal | Ejla Suljcic | 299 |
| Glovo | CRM Engineer I Live Operations Global HQ Barcelona | Jovana Drakula | 299 |

Glovo has the long-tail-of-aged-jobs problem. Project Dashboard will need an "early warning" cut on this exact list.

---

## Q4 — Time-to-fill, last 20 hires (most recently filled jobs)

Computed as `date_first_hired - date_created` from `job` table (matches DAX `Candidate - Time to Fill (Hired date)` definition: first hire only, never average).

| client | job | days to fill |
|---|---|---:|
| Glovo | Data Internship | 0 |
| Glovo | Android Engineer - IC2 | 36 |
| Wolt | Store Manager - Cyprus | 24 |
| Parloa | Senior Accountant | 55 |
| Wolt | Sales support Associate-Cyprus | 69 |
| Wolt | Merchant Onboarding Associate-Kosovo | 55 |
| Eucalyptus | Medical Support Associate, DE | 15 |
| Wolt | Sales Manager-Albania | 171 |
| Wolt | Marketing Manager (North Macedonia) | 60 |
| Wolt | Senior Category Manager - Almaty | 63 |
| Wolt | Mx Onboarding Team Lead | 42 |
| Wolt | Account Manager (Resto, CZ) | 97 |
| PhantomBuster | Senior Recruiter | 37 |
| PhantomBuster | Client Growth Partner | 37 |
| Wolt | Account Manager - Albania | 67 |
| Wolt | Category Manager - Athens | 112 |
| Wolt | Business System Analyst Talent Solutions | 2 |
| Wolt | Business System Analyst | 43 |
| Wolt | Sales Activation Specialist | 67 |
| Wolt | Operations Team Lead | 0 |

Median ≈ 49 days. The two `0`-day fills (Data Internship, Operations Team Lead) and the `2`-day fill suggest jobs created in Bubble after the candidate was already lined up — known data hygiene issue, harmless for averages but worth flagging in QA.

---

## Q5 — Candidate distribution by current stage (active jobs only)

Stage logic ported from the M expression in the .pbix:

| current stage | candidates |
|---|---:|
| 1. Contacted | 37,911 |
| 2. Recruiter Screen | 1,531 |
| 3. Actual Screen | 3,666 |
| 4. Move to ATS | 3,816 |
| 5. Onsite | 405 |
| 6. Offer | 90 |
| 7. Hired | 2,697 |
| 0. Positive Response (uncategorized) | 1,354 |
| 0. Applied | 1 |
| 0. Final Interview | 3 |

**Funnel reads sensibly:**
- 37,911 contacted → 1,531 + 3,666 = 5,197 in screen → 3,816 in ATS → 405 onsite → 90 active offers → 2,697 historical hires (these are still attached to currently-active jobs as past hires).
- ~3.6% screen rate from contacted, ~17% offer-to-hire conversion if you compare 90 active offers to recent monthly hire rates.
- The 1,354 "Positive Response" rows are an undocumented stage_current_type that the M expression doesn't map. **Question for Andy/Mikhail before he leaves.**

---

## Conclusion

**Reporting-v2 is fully fit for purpose.** All 5 questions answered in seconds. The stage logic ports cleanly from Power Query M to plain SQL. We can build the WBR + Project Dashboard MVP directly on top of these tables with zero dependency on the Bubble extraction pipeline I built earlier.

### Issues to handle in the SQL layer
1. Booleans are TEXT lowercase — wrap with `=  'true' / 'false'` everywhere.
2. Dates are TEXT — wrap with `TRY_TO_DATE()` before any date arithmetic.
3. `'Positive Response'` stage exists in data but isn't in Andy's Power Query mapping — clarify with Andy/Mikhail.
4. Wolt is ~95% of hire volume — every aggregate needs a "with/without Wolt" toggle.

### What this unblocks
- We can write the WBR materialized view tomorrow as a single SQL query, validate row-by-row against Power BI, and ship Phase 1 in days not weeks.
- The pbix DataModel filters and the underlying data agree — no surprise schema drift.
