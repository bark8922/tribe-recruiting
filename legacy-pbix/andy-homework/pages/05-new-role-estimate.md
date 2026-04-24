# Homework: New Role Estimate

**Time budget:** ~20-30 min + one CSV export.  
**How to do this:** read the reference section first so you're reminded what's on the page, then fill in the homework section at the bottom. Don't explain anything already covered by the DAX below — we have that. We want the *stuff in your head* that the DAX doesn't capture.

**Prefilled from PBIP:** 21 visuals · 5 tables · 12 measures · 1 calc columns

---

# Reference (prefilled — don't edit)

## Tables touched on this page

`LastRefreshedDate`, `Metrics`, `candidate`, `client`, `job`

## Visuals on this page

**1. _(untitled)_** — *funnel*
  - `Metrics` → `Conv rate Visited to Hires`, `Conv rate Contacted to Hires`, `Conv rate Screens to Hires`, `Conv rate Actual screens to Hires`, `Conv rate Moved to ATS to Hires`, `Conv rate Offers to Hires`
  - `job` → `is_external_recruiter`

**2. _(untitled)_** — *slicer*
  - `job` → `Job Country`

**3. _(untitled)_** — *?*

**4. _(untitled)_** — *slicer*
  - `client` → `client_name`

**5. _(untitled)_** — *cardVisual*
  - `candidate` → `job_id`
  - `job` → `Job Creation between`
  - `Metrics` → `# candidates - hired (contacted date)`

**6. _(untitled)_** — *pieChart*
  - `candidate` → `reason_not_interested`, `candidate_id`

**7. _(untitled)_** — *slicer*
  - `job` → `job_title`

**8. _(untitled)_** — *textbox*

**9. _(untitled)_** — *slicer*
  - `job` → `job_subcategory`

**10. _(untitled)_** — *slicer*
  - `job` → `job_category`

**11. _(untitled)_** — *image*

**12. _(untitled)_** — *cardVisual*
  - `Metrics` → `Estimate conversation`

**13. _(untitled)_** — *actionButton*

**14. _(untitled)_** — *textbox*

**15. _(untitled)_** — *card*
  - `LastRefreshedDate` → `LastRefreshedDate`

**16. _(untitled)_** — *cardVisual*
  - `Metrics` → `Job - Time to Hire`, `% Response Rate`
  - `candidate` → `hired_salary_eur`

**17. _(untitled)_** — *shape*

**18. _(untitled)_** — *shape*

**19. _(untitled)_** — *pivotTable*
  - `job` → `Job Country`, `job_title`
  - `candidate` → `job_id`
  - `Metrics` → `Job - Time to Hire`, `Conv rate Visited to Hires`, `Conv rate Contacted to Hires`, `Conv rate Screens to Hires`, `Conv rate Actual screens to Hires`, `Conv rate Moved to ATS to Hires`, `Conv rate Offers to Hires`

**20. _(untitled)_** — *cardVisual*
  - `Metrics` → `Estimate find a time`

**21. _(untitled)_** — *slicer*
  - `job` → `Job Creation Year`

## Measures referenced (with full DAX)

### `# candidates - hired (contacted date)` · table: `Metrics`

```dax
CALCULATE([# candidates - contacted (contacted date)], not isblank(candidate_stage[date_hired]))
```

### `% Response Rate` · table: `Metrics`

```dax
DIVIDE([Candidate Response], [# candidates - contacted (contacted date)])
```

### `Conv rate Actual screens to Hires` · table: `Metrics`

```dax
IF([# candidates - hired (contacted date)]=BLANK(), BLANK(),
			   [# candidates - actual screen (contacted date)]/[# candidates - hired (contacted date)])
```

### `Conv rate Contacted to Hires` · table: `Metrics`

```dax
IF([# candidates - hired (contacted date)]=BLANK(), BLANK(),
			    [# candidates - contacted (contacted date)]/[# candidates - hired (contacted date)])
```

### `Conv rate Moved to ATS to Hires` · table: `Metrics`

```dax
IF([# candidates - hired (contacted date)]=BLANK(), BLANK(),
			    [# candidates - moved to ATS (contacted date)]/[# candidates - hired (contacted date)])
```

### `Conv rate Offers to Hires` · table: `Metrics`

```dax
IF([# candidates - hired (contacted date)]=BLANK(), BLANK(),
			    [# candidates - offer (contacted date)]/[# candidates - hired (contacted date)])
```

### `Conv rate Screens to Hires` · table: `Metrics`

```dax
IF([# candidates - hired (contacted date)]=BLANK(), BLANK(),
			    [# candidates - screen (contacted date)]/[# candidates - hired (contacted date)])
```

### `Conv rate Visited to Hires` · table: `Metrics`

```dax
IF([# candidates - hired (contacted date)]=BLANK(), BLANK(),
			    [# events - LinkedIn visited (date created)]/[# candidates - hired (contacted date)])
```

### `Estimate conversation` · table: `Metrics`

```dax
"You will need to have " & FORMAT([Conv rate Visited to Hires],"0") & " Linkedin profile viewed, " & FORMAT([Conv rate Contacted to Hires],"0") & " candidates contacted and " & FORMAT([Conv rate Actual screens to Hires], "0") & " actual screens to get 1 hire"
```

### `Estimate find a time` · table: `Metrics`

```dax
"This role typically requires " & FORMAT([Job - Time to Hire],"0") & " days to find a hire. If you start to contact candidates today, you will have the 1st hire on " & FORMAT(TODAY()+[Job - Time to Hire], "YYYY-MM-DD")
```

### `Job - Time to Hire` · table: `Metrics`

```dax
CALCULATE(AVERAGE(job[Diff Contacted - Hired]), job[Diff Contacted - Hired]>0)
```

### `Job Creation between` · table: `job`

```dax
```
			"From " & FORMAT(MIN(job[date_created]), "YYYY-MM-DD") & " until " & FORMAT(MAX(job[date_created]), "YYYY-MM-DD") 
			```
```

## Calculated columns referenced (with DAX)

### `Job Creation Year` · table: `job`

```dax
FORMAT(job[date_created], "YYYY")
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

- [ ] `# candidates - hired (contacted date)`
- [ ] `% Response Rate`
- [ ] `Conv rate Actual screens to Hires`
- [ ] `Conv rate Contacted to Hires`
- [ ] `Conv rate Moved to ATS to Hires`
- [ ] `Conv rate Offers to Hires`
- [ ] `Conv rate Screens to Hires`
- [ ] `Conv rate Visited to Hires`
- [ ] `Estimate conversation`
- [ ] `Estimate find a time`
- [ ] `Job - Time to Hire`
- [ ] `Job Creation between`

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
