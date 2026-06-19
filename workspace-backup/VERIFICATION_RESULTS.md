# Recruiting Dashboard — Number Verification vs Power BI
**Date:** 2026-04-10
**Verified by:** Claude (automated queries against Keboola reporting-v2)

---

## WBR Week 14 (Mar 30 - Apr 5, 2026) — VBR-Joined

**Method:** DISTINCT COUNT from `candidate_stage` date columns, joined through
candidate -> job -> client. Filtered to only (client x TA) pairs present in
`wbr_ta_target.csv` for April 2026 (38 pairs, 20 clients).

| Metric | Our Query | Power BI | Delta | % |
|--------|-----------|----------|-------|---|
| Contacted | 1,821 | 1,871 | -50 | **-2.7%** |
| Actual Screens | 206 | 210 | -4 | **-1.9%** |
| Moved to ATS | 128 | 131 | -3 | **-2.3%** |
| Offers | 17 | 13 | +4 | +30.8% |
| Hires | 17 | 13 | +4 | +30.8% |

**Column mapping confirmed:**
- Contacted = `date_contacted`
- Actual Screens = `date_screen_actual` (NOT `date_screen` which gives 302 = +43.8%)
- Moved to ATS = `date_interview`
- Offers = `date_offer`
- Hires = `date_hired`

**Remaining gaps explained:**
- Contacted -50: Missing TAs not in Keboola (Rodrigo Gomez, Adis Prepoljac = 0 jobs found)
- Screens -4 / ATS -3: Same cause + potential data freshness delta
- Offers/Hires +4: Under investigation. Possibly PBI uses `date_hired` from `candidate` table
  (not `candidate_stage`) or has additional month-boundary logic. All 4 extra are from Wolt.

**Name matching issues found and resolved:**
- "Jelena  Lacmanovic" (double space in Keboola)
- "Chene Elliot" -> "Chene Elliot" (accent: e)
- "Dusan Spica" -> "Dusan Spica" (diacritics: s, S)
- "AVIV " (trailing space + uppercase in Keboola) vs "Aviv" in target CSV
- "Doordash" (lowercase d) vs "DoorDash" in target CSV
- "Nexi " (trailing space) vs "Nexi" in target CSV
- ALL Wolt divisions (Wolt Tech, Wolt HQ, Wolt Germany, etc.) -> single "Wolt" in Keboola

---

## Overview (Project Performance) — Apr 6-10, 2026

**Method:** Same query logic, no VBR target join (all TAs), date range = Apr 6-10.
Positive Response from events table.

| Metric | Our Query | Power BI | Delta | % |
|--------|-----------|----------|-------|---|
| Contacted | 1,987 | 2,109 | -122 | -5.8% |
| Positive Response | 401 | 396 | +5 | **+1.3%** |
| Actual Screens | 227 | 219 | +8 | +3.7% |
| Moved to ATS | 128 | 131 | -3 | -2.3% |
| Offered | 5 | 5 | 0 | **0%** |
| Hired | 5 | 5 | 0 | **0%** |

**Note:** The Overview screenshot was likely showing the current partial week
(Apr 6-10) at the time it was captured. Offers and Hires match exactly.
Contacted delta (-5.8%) could be due to external recruiter handling or
data freshness.

---

## Definitive Query Logic (use this, don't re-research)

### Base tables and join path
```sql
FROM "out.c-reporting-v2"."candidate_stage" "cs"
JOIN "out.c-reporting-v2"."candidate" "c"
  ON "cs"."candidate_id" = "c"."candidate_id"
JOIN "out.c-reporting-v2"."job" "j"
  ON "c"."job_id" = "j"."job_id"
JOIN "out.c-reporting-v2"."client" "cl"
  ON "j"."client_id" = "cl"."client_id"
```

### Filters (always apply)
```sql
WHERE LOWER(NULLIF("j"."test", '')) <> 'true'
  AND LOWER(NULLIF("c"."is_candidate_archived", '')) <> 'true'
```

DO NOT filter on:
- `client.test` (unreliable — Wolt has test=true)
- `is_job_archived` (just a status flag)
- External recruiters (NOT excluded from TA weekly; only from conversion rates)

### Metric columns (all DISTINCT COUNT from candidate_stage)
```sql
COUNT(DISTINCT CASE WHEN TRY_TO_DATE("cs"."date_contacted") BETWEEN @start AND @end
  THEN "cs"."candidate_id" END) AS "contacted"

COUNT(DISTINCT CASE WHEN TRY_TO_DATE("cs"."date_screen_actual") BETWEEN @start AND @end
  THEN "cs"."candidate_id" END) AS "actual_screens"

COUNT(DISTINCT CASE WHEN TRY_TO_DATE("cs"."date_interview") BETWEEN @start AND @end
  THEN "cs"."candidate_id" END) AS "moved_to_ats"

COUNT(DISTINCT CASE WHEN TRY_TO_DATE("cs"."date_offer") BETWEEN @start AND @end
  THEN "cs"."candidate_id" END) AS "offers"

COUNT(DISTINCT CASE WHEN TRY_TO_DATE("cs"."date_hired") BETWEEN @start AND @end
  THEN "cs"."candidate_id" END) AS "hires"
```

### Positive Response (ONLY metric from events, not candidate_stage)
```sql
SELECT COUNT(DISTINCT CASE WHEN TRY_TO_DATE("e"."date_created") BETWEEN @start AND @end
  THEN "e"."candidate_id" END)
FROM "out.c-reporting-v2"."event" "e"
JOIN "out.c-reporting-v2"."candidate" "c" ON "e"."candidate_id" = "c"."candidate_id"
JOIN "out.c-reporting-v2"."job" "j" ON "c"."job_id" = "j"."job_id"
WHERE "e"."event_type" = 'Moved to stage'
  AND "e"."moved_to_stageType" = 'Positive Response'
  AND LOWER(NULLIF("j"."test", '')) <> 'true'
  AND LOWER(NULLIF("c"."is_candidate_archived", '')) <> 'true'
```

### Key columns
- TA name: `"j"."job_recruiter"`
- TS name: `"j"."job_sourcer"`
- Client name: `"cl"."client_name"`
- External recruiter flag: `"j"."is_external_recruiter"`
- Event date: `"e"."date_created"` (NOT `"e"."date"` which doesn't exist)

### VBR target join (WBR tab only)
The WBR only shows TAs present in `wbr_ta_target.csv` for the selected
week's month. Join grain: (client x TA). Wolt divisions in the target
all map to Keboola client "Wolt".

Client name mapping (target CSV -> Keboola):
- Aviv -> "AVIV " (uppercase, trailing space)
- DoorDash -> "Doordash" (lowercase d)
- Nexi -> "Nexi " (trailing space)
- Wolt Tech/Market/HQ/Germany/North/Central -> "Wolt"
- All others -> same name

TA name fixes:
- Jelena Lacmanovic -> "Jelena  Lacmanovic" (double space)
- Chene Elliot -> "Chene Elliot" (accent e in Keboola)
- Dusan Spica -> "Dusan Spica" (diacritics in Keboola)

### Snowflake dialect gotchas
- All column names are lowercase in Keboola — MUST double-quote everything
- Booleans stored as TEXT ('true'/'false') — use LOWER(NULLIF(col,'')) <> 'true'
- Dates stored as TEXT — use TRY_TO_DATE() everywhere
- Fully qualified: "out.c-reporting-v2"."table_name"
- Aliases must be double-quoted to preserve case
