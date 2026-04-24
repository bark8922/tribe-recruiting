# Homework: KPI - TS Summary

**Time budget:** ~20-30 min + one CSV export.  
**How to do this:** read the reference section first so you're reminded what's on the page, then fill in the homework section at the bottom. Don't explain anything already covered by the DAX below — we have that. We want the *stuff in your head* that the DAX doesn't capture.

**Prefilled from PBIP:** 31 visuals · 8 tables · 23 measures · 3 calc columns

---

# Reference (prefilled — don't edit)

## Tables touched on this page

`Calendar`, `LastRefreshedDate`, `Metrics`, `candidate`, `candidate_stage`, `client`, `event`, `job`

## Visuals on this page

**1. _(untitled)_** — *shape*

**2. _(untitled)_** — *slicer*
  - `client` → `client_name`
  - `job` → `job_title`

**3. _(untitled)_** — *areaChart*
  - `Calendar` → `Year`, `Q number`, `MonthName`
  - `Metrics` → `% Screens to Actual Screen`

**4. _(untitled)_** — *actionButton*

**5. _(untitled)_** — *shape*

**6. _(untitled)_** — *pivotTable*
  - `job` → `job_title`, `job_recruiter`, `Job Days Opened w/o hires`, `> 60 days`, `0-30 days`, `30-60 days`, `job_id`, `date_first_hired`, `job_sourcer`, `Owned by sourcing team (active TS)`
  - `client` → `client_name`
  - `Metrics` → `% Viewed to Contacted`, `% Contacted to Reacted`, `% Reacted to Actual Screen`

**7. _(untitled)_** — *slicer*
  - `event` → `who_created_event_first`

**8. _(untitled)_** — *actionButton*

**9. _(untitled)_** — *actionButton*

**10. _(untitled)_** — *slicer*
  - `job` → `Tech Role`, `job_category`

**11. _(untitled)_** — *card*
  - `LastRefreshedDate` → `LastRefreshedDate`

**12. _(untitled)_** — *slicer*
  - `job` → `is_job_archived`, `job_category`

**13. _(untitled)_** — *textbox*

**14. _(untitled)_** — *areaChart*
  - `Calendar` → `Year`, `Q number`, `MonthName`
  - `Metrics` → `% Contacted to Positive Response`

**15. _(untitled)_** — *pivotTable*
  - `job` → `job_sourcer`, `job_title`, `job_id`, `0-30 days`, `30-60 days`, `60-90 days`, `>90 days`, `> 60 days`, `date_first_hired`, `Owned by sourcing team (active TS)`
  - `Metrics` → `% Viewed to Contacted`, `% Contacted to Reacted`, `% Reacted to Actual Screen`

**16. _(untitled)_** — *pivotTable*
  - `candidate_stage` → `Current Stage`, `stage_current`, `date_hired`
  - `client` → `client_name`
  - `job` → `job_title`
  - `candidate` → `reason_not_interested`

**17. _(untitled)_** — *?*

**18. _(untitled)_** — *textbox*

**19. _(untitled)_** — *areaChart*
  - `Calendar` → `Year`, `Q number`, `MonthName`
  - `Metrics` → `% Screens Actual to ATS`

**20. _(untitled)_** — *slicer*
  - `job` → `job_category`

**21. _(untitled)_** — *pivotTable*
  - `event` → `who_created_event_first`
  - `job` → `job_title`, `job_sourcer`, `job_id`
  - `Metrics` → `% Contacted to Positive Response`, `% Screens to Actual Screen`, `% Screens Actual to ATS`, `# candidates - contacted (contacted date)`, `# candidates - positive response`, `# candidates - screen (screened date)`, `# candidates - actual screen (actual screen date)`, `# candidates - move to ATS (moved date)`, `# candidates - offer (offered date)`, `# candidates - hired (hired date)`, `% Viewed to Contacted`, `% Contacted to Reacted`, `% Reacted to Actual Screen`

**22. _(untitled)_** — *slicer*
  - `job` → `job_title`

**23. _(untitled)_** — *pieChart*
  - `candidate` → `reason_not_interested`
  - `Metrics` → `# candidates (contacted date) NOT USED`, `reason_not_interested`

**24. _(untitled)_** — *shape*

**25. _(untitled)_** — *funnel*
  - `Metrics` → `# events - LinkedIn visited (date created)`, `# candidates - contacted (contacted date)`, `# candidates - reacted (contacted date)`, `# candidates - positive response`, `# candidates - actual screen (actual screen date)`, `# candidates - move to ATS (moved date)`, `# candidates - offer (offered date)`, `# candidates - hired (hired date)`

**26. _(untitled)_** — *cardVisual*
  - `Metrics` → `# candidates - hired (hired date)`, `# Tech Roles Hired`, `Candidate - Time to Find a Hire`
  - `candidate` → `job_id`

**27. _(untitled)_** — *shape*

**28. _(untitled)_** — *shape*

**29. _(untitled)_** — *image*

**30. _(untitled)_** — *HierarchySlicer1458836712039*
  - `Calendar` → `Year`, `Q number`, `MonthName`

**31. _(untitled)_** — *slicer*
  - `job` → `job_sourcer`

## Measures referenced (with full DAX)

### `# Tech Roles Hired` · table: `Metrics`

```dax
```
			CALCULATE(
			    [# events - unique candidates - hired (date created)], 
			            job[job_category]="Data Analytics" ||
			            job[job_category]="DevOps" ||
			            job[job_category]="Software Engineering" ||
			            job[job_category]="Software"||
			            job[job_category]="Design")+0
			```
```

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

### `# candidates - reacted (contacted date)` · table: `Metrics`

```dax
CALCULATE([# candidates - contacted (contacted date)],candidate[is_candidate_reacted]=TRUE())
```

### `# candidates - screen (screened date)` · table: `Metrics`

```dax
var screened = CALCULATE(COUNT(candidate_stage[candidate_id]), USERELATIONSHIP('Calendar'[Date],candidate_stage[date_screen]), event[moved_to_stage]="Recruiter Screen")
			RETURN CALCULATE(screened, candidate_stage[date_screen]<>BLANK())
```

### `# events - LinkedIn visited (date created)` · table: `Metrics`

```dax
CALCULATE(DISTINCTCOUNT(event[talent_id + job_id]),event[event_type]="Linkedin Visited Profile",
			USERELATIONSHIP(job[job_id], event[job_id]), USERELATIONSHIP('Calendar'[Date], event[date_created]))
```

### `% Contacted to Positive Response` · table: `Metrics`

```dax
var con = CALCULATE([# candidates - contacted (contacted date)], candidate_stage[date_contacted]>=DATE(2025,4,14))
			var perc = DIVIDE([# candidates - positive response], con, 0)
			RETURN IF(perc>1, 1, perc)
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

### `% Screens Actual to ATS` · table: `Metrics`

```dax
var perc = DIVIDE([# candidates - move to ATS (moved date)],[# candidates - actual screen (actual screen date)])
			RETURN IF(perc>1, 1, perc)
```

### `% Screens to Actual Screen` · table: `Metrics`

```dax
var perc = DIVIDE(Metrics[# candidates - actual screen (actual screen date)], [# candidates - screen (screened date)],0)
			RETURN IF(perc>1, 1, perc)
```

### `% Viewed to Contacted` · table: `Metrics`

```dax
VAR perc = DIVIDE([# candidates - contacted (contacted date)], [# events - LinkedIn visited (date created)],0)
			RETURN IF(perc>1, 1, perc)
```

### `0-30 days` · table: `job`

```dax
CALCULATE(COUNT(job[job_id]), job[Job Days Opened]<=30)
```

### `30-60 days` · table: `job`

```dax
CALCULATE(COUNT(job[job_id]), job[Job Days Opened]>30 && job[Job Days Opened]<=60)
```

### `60-90 days` · table: `job`

```dax
CALCULATE(COUNT(job[job_id]), job[Job Days Opened]>60 && job[Job Days Opened]<=90)
```

### `> 60 days` · table: `job`

```dax
CALCULATE(COUNT(job[job_id]), job[Job Days Opened]>60)
```

### `>90 days` · table: `job`

```dax
CALCULATE(COUNT(job[job_id]), job[Job Days Opened]>90)
```

### `Candidate - Time to Find a Hire` · table: `Metrics`

```dax
CALCULATE(AVERAGE(candidate_stage[Diff Concated - Job created]), candidate_stage[date_hired]<>BLANK())
```

## Calculated columns referenced (with DAX)

### `Job Days Opened w/o hires` · table: `job`

```dax
IF(job[date_first_hired]=BLANK(), DATEDIFF(job[date_created], TODAY(), DAY), BLANK())
```

### `Owned by sourcing team (active TS)` · table: `job`

```dax
var ts = LOOKUPVALUE(Current_TS[TS], Current_TS[TS], job[job_sourcer])
			RETURN
			IF(ts=BLANK(), "No", "Yes")
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

- [ ] `# Tech Roles Hired`
- [ ] `# candidates (contacted date) NOT USED`
- [ ] `# candidates - actual screen (actual screen date)`
- [ ] `# candidates - contacted (contacted date)`
- [ ] `# candidates - hired (hired date)`
- [ ] `# candidates - move to ATS (moved date)`
- [ ] `# candidates - offer (offered date)`
- [ ] `# candidates - positive response`
- [ ] `# candidates - reacted (contacted date)`
- [ ] `# candidates - screen (screened date)`
- [ ] `# events - LinkedIn visited (date created)`
- [ ] `% Contacted to Positive Response`
- [ ] `% Contacted to Reacted`
- [ ] `% Reacted to Actual Screen`
- [ ] `% Screens Actual to ATS`
- [ ] `% Screens to Actual Screen`
- [ ] `% Viewed to Contacted`
- [ ] `0-30 days`
- [ ] `30-60 days`
- [ ] `60-90 days`
- [ ] `> 60 days`
- [ ] `>90 days`
- [ ] `Candidate - Time to Find a Hire`

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
