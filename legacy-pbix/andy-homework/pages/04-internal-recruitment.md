# Homework: Internal Recruitment

**Time budget:** ~20-30 min + one CSV export.  
**How to do this:** read the reference section first so you're reminded what's on the page, then fill in the homework section at the bottom. Don't explain anything already covered by the DAX below — we have that. We want the *stuff in your head* that the DAX doesn't capture.

**Prefilled from PBIP:** 21 visuals · 8 tables · 17 measures · 3 calc columns

---

# Reference (prefilled — don't edit)

## Tables touched on this page

`Calendar`, `IR Comment`, `LastRefreshedDate`, `Metrics`, `candidate`, `candidate_stage`, `event`, `job`

## Visuals on this page

**1. _(untitled)_** — *slicer*
  - `event` → `who_created_event`

**2. _(untitled)_** — *shape*

**3. _(untitled)_** — *pivotTable*
  - `event` → `who_created_event`
  - `job` → `job_title`, `Jobs Opened`
  - `Metrics` → `# candidates - actual screen (actual screen date)`

**4. _(untitled)_** — *cardVisual*
  - `Metrics` → `Job - Time to Hire`, `Job - Time to Find a Hire`, `Job - Time to Fill`, `Sourced Hired`, `Applicant Hired`

**5. _(untitled)_** — *donutChart*
  - `candidate` → `reason_not_interested`
  - `Metrics` → `# candidates (contacted date) NOT USED`

**6. _(untitled)_** — *slicer*
  - `job` → `is_job_archived`, `job_title`, `date_created`

**7. _(untitled)_** — *pivotTable*
  - `event` → `who_created_event_first`
  - `job` → `job_title`, `Jobs Opened`
  - `Metrics` → `# candidates - contacted (contacted date)`, `# candidates - positive response`, `# candidates - hired (hired date)`

**8. _(untitled)_** — *shape*

**9. _(untitled)_** — *slicer*
  - `candidate` → `source`
  - `job` → `job_title`, `date_created`

**10. _(untitled)_** — *pivotTable*
  - `IR Comment` → `Week`, `Client`, `Headcount`, `Status`, `Outcome`
  - `job` → `job_title`, `Jobs Opened`
  - `event` → `who_event_created_for`

**11. _(untitled)_** — *ChicletSlicer1448559807354*
  - `Calendar` → `is_previous_week`

**12. _(untitled)_** — *funnel*
  - `Metrics` → `# candidates - contacted (contacted date)`, `# candidates - positive response`, `# candidates - screen (screened date)`, `# candidates - actual screen (actual screen date)`, `# candidates - move to ATS (moved date)`, `Onsite`, `Culture Interview`, `Call with Client Interview`, `# candidates - offer (offered date)`, `# candidates - hired (hired date)`

**13. _(untitled)_** — *pivotTable*
  - `job` → `job_title`, `Jobs Opened`
  - `Metrics` → `# candidates - hired (hired date)`
  - `event` → `who_event_created_for`

**14. _(untitled)_** — *slicer*
  - `Calendar` → `Date`

**15. _(untitled)_** — *actionButton*

**16. _(untitled)_** — *pivotTable*
  - `candidate_stage` → `Current Stage`, `date_hired`
  - `job` → `job_title`, `Jobs Opened`
  - `candidate` → `reason_not_interested`

**17. _(untitled)_** — *slicer*
  - `event` → `who_created_event_first`

**18. _(untitled)_** — *card*
  - `LastRefreshedDate` → `Last refreshed date`

**19. _(untitled)_** — *pivotTable*
  - `Calendar` → `Week start end`
  - `job` → `job_title`, `Jobs Opened`
  - `Metrics` → `# candidates - contacted (contacted date)`, `# candidates - positive response`, `# candidates - screen (screened date)`, `# candidates - actual screen (actual screen date)`, `# candidates - move to ATS (moved date)`, `Onsite`, `Culture Interview`, `Call with Client Interview`, `# candidates - offer (offered date)`, `# candidates - hired (hired date)`
  - `event` → `who_event_created_for`

**20. _(untitled)_** — *slicer*
  - `job` → `job_title`, `date_created`

**21. _(untitled)_** — *textbox*

## Measures referenced (with full DAX)

### `# candidates (contacted date) NOT USED` · table: `Metrics`

```dax
CALCULATE(COUNT(candidate_stage[candidate_id]), USERELATIONSHIP('Calendar'[Date],candidate_stage[date_contacted]))
```

### `# candidates - actual screen (actual screen date)` · table: `Metrics`

```dax
var acutalscreened = CALCULATE(COUNT(candidate_stage[candidate_id]), USERELATIONSHIP('Calendar'[Date],candidate_stage[date_screen_actual]),event[event_type]="Evaluation")
			RETURN CALCULATE(acutalscreened, candidate_stage[date_screen_actual]<>BLANK())
```

### `# candidates - contacted (contacted date)` · table: `Metrics`

```dax
var contact = CALCULATE(COUNT(candidate_stage[candidate_id]), USERELATIONSHIP('Calendar'[Date],candidate_stage[date_contacted]))
			RETURN CALCULATE(contact, candidate_stage[date_contacted]<>BLANK())
```

### `# candidates - hired (hired date)` · table: `Metrics`

```dax
var hire = CALCULATE(COUNT(candidate_stage[candidate_id]), USERELATIONSHIP('Calendar'[Date],candidate_stage[date_hired]),event[moved_to_stage]="Hired")
			RETURN CALCULATE(hire, candidate_stage[date_hired]<>BLANK())
```

### `# candidates - move to ATS (moved date)` · table: `Metrics`

```dax
var moveto = CALCULATE(COUNT(candidate_stage[candidate_id]), USERELATIONSHIP('Calendar'[Date],candidate_stage[date_interview]), event[moved_to_stage]="Moved to ATS")
			RETURN CALCULATE(moveto, candidate_stage[date_interview]<>BLANK())
```

### `# candidates - offer (offered date)` · table: `Metrics`

```dax
var offer = CALCULATE(COUNT(candidate_stage[candidate_id]), USERELATIONSHIP('Calendar'[Date],candidate_stage[date_offer]), event[moved_to_stageType]="Offer")
			RETURN CALCULATE(offer, candidate_stage[date_offer]<>BLANK())
```

### `# candidates - positive response` · table: `Metrics`

```dax
CALCULATE(DISTINCTCOUNT(event[candidate_id]), event[moved_to_stageType]="Positive Response", event[date_created]>=DATE(2025,4,14), USERELATIONSHIP(event[date_created], 'Calendar'[Date]))
```

### `# candidates - screen (screened date)` · table: `Metrics`

```dax
var screened = CALCULATE(COUNT(candidate_stage[candidate_id]), USERELATIONSHIP('Calendar'[Date],candidate_stage[date_screen]), event[moved_to_stage]="Recruiter Screen")
			RETURN CALCULATE(screened, candidate_stage[date_screen]<>BLANK())
```

### `Applicant Hired` · table: `Metrics`

```dax
CALCULATE([# candidates - hired (hired date)], candidate[source]="Applicant")
```

### `Call with Client Interview` · table: `Metrics`

```dax
CALCULATE(DISTINCTCOUNT(event[candidate_id]), event[moved_to_stageType]="Final Interview", event[moved_to_stage]="Call with Client", USERELATIONSHIP(event[date_created], 'Calendar'[Date]))
```

### `Culture Interview` · table: `Metrics`

```dax
CALCULATE(DISTINCTCOUNT(event[candidate_id]), event[moved_to_stageType]="Final Interview", event[moved_to_stage]="Culture Interview", USERELATIONSHIP(event[date_created], 'Calendar'[Date]))
```

### `Job - Time to Fill` · table: `Metrics`

```dax
CALCULATE(AVERAGE(job[Diff Hired - Job created]), candidate_stage[Diff Hired - Job created]>0)
```

### `Job - Time to Find a Hire` · table: `Metrics`

```dax
CALCULATE(AVERAGE(job[Diff Contacted - Job created]), candidate_stage[Diff Concated - Job created]>0)
```

### `Job - Time to Hire` · table: `Metrics`

```dax
CALCULATE(AVERAGE(job[Diff Contacted - Hired]), job[Diff Contacted - Hired]>0)
```

### `Last refreshed date` · table: `LastRefreshedDate`

```dax
"Last Updated: " & FORMAT(MAX(LastRefreshedDate[LastRefreshedDate]), "YYYY-MM-DD HH:MM") & " (CET)"
```

### `Onsite` · table: `Metrics`

```dax
CALCULATE(DISTINCTCOUNT(event[candidate_id]), event[moved_to_stageType]="Final Interview", event[moved_to_stage]="Onsite", USERELATIONSHIP(event[date_created], 'Calendar'[Date]))
```

### `Sourced Hired` · table: `Metrics`

```dax
CALCULATE([# candidates - hired (hired date)],  candidate[source]="Sourced")
```

## Calculated columns referenced (with DAX)

### `Jobs Opened` · table: `job`

```dax
IF(job[date_first_hired]<>BLANK(), job[Diff Hired - Job created], job[Job Days Opened w/o hires])
```

### `Week` · table: `analytic_usage`

```dax
WEEKNUM(analytic_usage[created_date], 21)
```

### `is_previous_week` · table: `Calendar`

```dax
if('Calendar'[WeekEnding Current] = 'Calendar'[WeekEnding]+7, TRUE(), FALSE())
```

---

# Homework (Andy — fill in below)

> **Tip:** skip any question you genuinely can't answer — a short "n/a" is fine. More useful to finish all 8 pages at 80% than to perfect one.

## 1. Who uses this page and how often?

> One or two lines. Names/roles + frequency. E.g., "TA leads daily during WBR; Blake reviews monthly."

_Your answer:_


## 2. In plain English, what question does this page answer?

> 2-3 sentences. The *why* of the page — what decision does it inform?

_Your answer:_


## 3. Which measures are the "real answer" on this page?

> Check **up to 3**. Everything else is decorative, diagnostic, or vestigial. If all are equally load-bearing, flag that in the notes below.

- [ ] `# candidates (contacted date) NOT USED`
- [ ] `# candidates - actual screen (actual screen date)`
- [ ] `# candidates - contacted (contacted date)`
- [ ] `# candidates - hired (hired date)`
- [ ] `# candidates - move to ATS (moved date)`
- [ ] `# candidates - offer (offered date)`
- [ ] `# candidates - positive response`
- [ ] `# candidates - screen (screened date)`
- [ ] `Applicant Hired`
- [ ] `Call with Client Interview`
- [ ] `Culture Interview`
- [ ] `Job - Time to Fill`
- [ ] `Job - Time to Find a Hire`
- [ ] `Job - Time to Hire`
- [ ] `Last refreshed date`
- [ ] `Onsite`
- [ ] `Sourced Hired`

_Notes (optional):_


## 4. For each load-bearing measure you checked above — why THIS date relationship (USERELATIONSHIP)?

> The single highest-value field on this homework. The DAX tells us which relationship you activated; it does NOT tell us *why* you chose it vs alternatives. One line each.
> Example: `# candidates - contacted (contacted date)` — *We use date_contacted because we want to attribute candidates to the week they were actually contacted, not the week the job was opened.*

Measure A (`                                  `): _____

Measure B (`                                  `): _____

Measure C (`                                  `): _____

## 5. Default filter rules

> When someone opens this page cold, what gets applied automatically? What does the user toggle? What's the "standard view"?
> E.g., "Default excludes external recruiters. Default date range is last 12 weeks. Default TA slicer is empty (shows all)."

_Your answer:_


## 6. Known outliers

> TAs / clients / roles that *chronically* look weird on this page, and why. Don't list one-off weeks — list patterns.
> E.g., "Eucalyptus always looks low on contacted because their ATS double-counts and we dedupe." / "Andrea's numbers lag 1 week because of how her TS distribution is tracked."

_Your answer:_


## 7. Known bugs or workarounds

> Things you'd fix if you had another month. Edge cases we should be aware of. Stuff you're embarrassed about.

_Your answer:_


## 8. If a stranger had to rebuild this page in SQL tomorrow, what are the top 3 things that would trip them up?

> This is the gotchas field — the stuff not visible in the DAX. Things like: which filter to join first, which relationships to activate, which clients to merge, which null-handling matters, which calc columns are load-bearing.

1. 

2. 

3. 

## 9. Snapshot — export the main table(s) on this page as CSV

> Pick the **primary table visuals** on this page (the ones with the numbers that matter, not the slicers / headers / icons). Export data for **the last 4 full weeks** (or whatever time window makes sense if "last 4 weeks" isn't the default).
> Drop the file(s) into `../snapshots/` named clearly. Examples: `time-to-hire-job-detail-w13-16.csv`, `sourcing-stats-per-sourcer-w13-16.csv`.
> Whatever format Power BI produces is fine. We'll diff against this later as ground truth.

**Files exported:**

- 
