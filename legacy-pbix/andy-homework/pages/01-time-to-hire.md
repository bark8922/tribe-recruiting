# Homework: Time to Hire

**Time budget:** ~20-30 min + one CSV export.  
**How to do this:** read the reference section first so you're reminded what's on the page, then fill in the homework section at the bottom. Don't explain anything already covered by the DAX below — we have that. We want the *stuff in your head* that the DAX doesn't capture.

**Prefilled from PBIP:** 22 visuals · 9 tables · 12 measures · 3 calc columns

---

# Reference (prefilled — don't edit)

## Tables touched on this page

`Calendar`, `LastRefreshedDate`, `Metrics`, `candidate`, `candidate_stage`, `client`, `event`, `job`, `talent`

## Visuals on this page

**1. _(untitled)_** — *slicer*
  - `event` → `who_event_created_for`
  - `job` → `job_title`, `date_created`

**2. _(untitled)_** — *slicer*
  - `client` → `client_name`
  - `job` → `job_title`

**3. _(untitled)_** — *lineChart*
  - `Calendar` → `MON-YYYY`
  - `Metrics` → `Job - Time to Hire`, `Job - Time to Find a Hire`, `Job - Time to Fill`

**4. _(untitled)_** — *shape*

**5. _(untitled)_** — *pivotTable*
  - `client` → `client_name`
  - `job` → `job_title`, `# Job (time to hire)`
  - `Metrics` → `Job - Time to Hire`, `Job - Time to Find a Hire`, `Job - Time to Fill`

**6. _(untitled)_** — *pivotTable*
  - `client` → `client_name`
  - `job` → `job_title`
  - `candidate_stage` → `# Candidate Hired (time to hire)`
  - `Metrics` → `Candidate - Time to Fill`, `Candidate - Time to Find a Hire`, `Candidate - Time to Hire`

**7. _(untitled)_** — *HierarchySlicer1458836712039*
  - `Calendar` → `Year`, `Q number`, `MonthName`

**8. _(untitled)_** — *textbox*

**9. _(untitled)_** — *cardVisual*
  - `job` → `# Job (time to hire)`
  - `Metrics` → `Job - Time to Hire`, `Job - Time to Find a Hire`, `Job - Time to Fill`

**10. _(untitled)_** — *pivotTable*
  - `job` → `job_category`, `job_subcategory`, `# Job (time to hire)`, `job_title`
  - `Metrics` → `Job - Time to Hire`, `Job - Time to Find a Hire`, `Job - Time to Fill`

**11. _(untitled)_** — *slicer*
  - `job` → `job_title`, `date_created`

**12. _(untitled)_** — *actionButton*

**13. _(untitled)_** — *actionButton*

**14. _(untitled)_** — *shape*

**15. _(untitled)_** — *slicer*
  - `job` → `Tech Role`, `job_title`, `date_created`

**16. _(untitled)_** — *textbox*

**17. _(untitled)_** — *image*

**18. _(untitled)_** — *slicer*
  - `candidate` → `source`
  - `job` → `job_title`, `date_created`

**19. _(untitled)_** — *slicer*
  - `job` → `External Recruiter?`, `job_title`, `date_created`

**20. _(untitled)_** — *card*
  - `LastRefreshedDate` → `LastRefreshedDate`

**21. _(untitled)_** — *tableEx*
  - `Metrics` → `# candidates - contacted (contacted date)`, `% Viewed to Contacted`, `% Contacted to Reacted`, `% Reacted to Actual Screen`
  - `client` → `client_name`
  - `job` → `job_title`, `job_category`, `job_subcategory`, `date_created`
  - `candidate_stage` → `date_contacted`, `date_hired`
  - `talent` → `full_name`

**22. _(untitled)_** — *?*

## Measures referenced (with full DAX)

### `# Candidate Hired (time to hire)` · table: `candidate_stage`

```dax
CALCULATE(COUNT(candidate_stage[candidate_id]), candidate_stage[date_hired]<>BLANK())
```

### `# Job (time to hire)` · table: `job`

```dax
CALCULATE(COUNT(job[job_id]), job[date_first_hired]<>BLANK(), job[Diff Hired - Job created]>=0)
```

### `# candidates - contacted (contacted date)` · table: `Metrics`

```dax
var contact = CALCULATE(COUNT(candidate_stage[candidate_id]), USERELATIONSHIP('Calendar'[Date],candidate_stage[date_contacted]))
			RETURN CALCULATE(contact, candidate_stage[date_contacted]<>BLANK())
```

### `% Contacted to Reacted` · table: `Metrics`

```dax
var perc = DIVIDE(Metrics[# candidates - reacted (contacted date)],Metrics[# candidates - contacted (contacted date)],0)
			RETURN IF(perc>1, 1, perc)
```

### `% Reacted to Actual Screen` · table: `Metrics`

```dax
var perc = DIVIDE(Metrics[# candidates - actual screen (actual screen date)],Metrics[# candidates - reacted (contacted date)],0)
			RETURN IF(perc>1, 1, perc)
```

### `% Viewed to Contacted` · table: `Metrics`

```dax
VAR perc = DIVIDE([# candidates - contacted (contacted date)], [# events - LinkedIn visited (date created)],0)
			RETURN IF(perc>1, 1, perc)
```

### `Candidate - Time to Fill` · table: `Metrics`

```dax
CALCULATE(AVERAGE(candidate_stage[Diff Hired - Job created]), candidate_stage[date_hired]<>BLANK())
```

### `Candidate - Time to Find a Hire` · table: `Metrics`

```dax
CALCULATE(AVERAGE(candidate_stage[Diff Concated - Job created]), candidate_stage[date_hired]<>BLANK())
```

### `Candidate - Time to Hire` · table: `Metrics`

```dax
CALCULATE(AVERAGE(candidate_stage[Diff Contacted - Hired]), candidate_stage[date_hired]<>BLANK())
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

## Calculated columns referenced (with DAX)

### `External Recruiter?` · table: `job`

```dax
IF(job[is_external_recruiter]=TRUE(), "Yes", "No")
```

### `MON-YYYY` · table: `Calendar`

```dax
CONCATENATE(LEFT('CALENDAR'[MonthName],3),
			            CONCATENATE(" ",'CALENDAR'[Year]))
```

### `Tech Role` · table: `job`

```dax
IF(job[job_category]="Data Analytics" ||
			            job[job_category]="DevOps" ||
			            job[job_category]="Software Engineering" ||
			            job[job_category]="Software"||
			            job[job_category]="Design" ||
			            job[job_category]="Product Manager" ||
			            job[job_category]="Information Technology" ||
			            job[job_category]="Quality Assurance (QA) " ||
			            job[job_category]="Engineering Management" ||
			            job[job_category]="Data Analytics" ||
			            (job[job_category]="Project Manager" && job[job_subcategory] in {"IT Project Manager", "Technical Program Manager"}),
			            "Yes", "No")
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

- [ ] `# Candidate Hired (time to hire)`
- [ ] `# Job (time to hire)`
- [ ] `# candidates - contacted (contacted date)`
- [ ] `% Contacted to Reacted`
- [ ] `% Reacted to Actual Screen`
- [ ] `% Viewed to Contacted`
- [ ] `Candidate - Time to Fill`
- [ ] `Candidate - Time to Find a Hire`
- [ ] `Candidate - Time to Hire`
- [ ] `Job - Time to Fill`
- [ ] `Job - Time to Find a Hire`
- [ ] `Job - Time to Hire`

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
