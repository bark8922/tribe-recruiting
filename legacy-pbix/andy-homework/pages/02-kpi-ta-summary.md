# Homework: KPI  - TA Summary (Color)

**Time budget:** ~20-30 min + one CSV export.  
**How to do this:** read the reference section first so you're reminded what's on the page, then fill in the homework section at the bottom. Don't explain anything already covered by the DAX below — we have that. We want the *stuff in your head* that the DAX doesn't capture.

**Prefilled from PBIP:** 24 visuals · 7 tables · 15 measures · 2 calc columns

---

# Reference (prefilled — don't edit)

## Tables touched on this page

`Calendar`, `LastRefreshedDate`, `Metrics`, `candidate`, `client`, `event`, `job`

## Visuals on this page

**1. _(untitled)_** — *shape*

**2. _(untitled)_** — *actionButton*

**3. _(untitled)_** — *slicer*
  - `job` → `Tech Role`, `job_category`

**4. _(untitled)_** — *shape*

**5. _(untitled)_** — *slicer*
  - `client` → `client_name`
  - `job` → `job_title`

**6. _(untitled)_** — *slicer*
  - `event` → `who_event_created_for`

**7. _(untitled)_** — *pivotTable*
  - `event` → `who_event_created_for`
  - `Metrics` → `TA linkedin candidate screening time`, `% Screen to ATS`, `% Screens Actual to ATS`, `% Offer to Hire`, `# candidates - actual screen (actual screen date)`, `# candidates - hired (hired date)`, `% Viewed to Contacted`, `% Contacted to Reacted`, `% Reacted to Actual Screen`
  - `job` → `is_external_recruiter`

**8. _(untitled)_** — *areaChart*
  - `Calendar` → `Year`, `Q number`, `MonthName`
  - `Metrics` → `% Move to client / Screen actual (date created)`
  - `job` → `is_external_recruiter`

**9. _(untitled)_** — *textbox*

**10. _(untitled)_** — *cardVisual*
  - `Metrics` → `# candidates - hired (hired date)`, `# Tech Roles Hired`
  - `job` → `Diff Hired - Job created`
  - `candidate` → `job_id`

**11. _(untitled)_** — *tableEx*
  - `event` → `who_event_created_for`
  - `client` → `client_name`
  - `job` → `job_title`, `Tech Role`, `is_external_recruiter`
  - `Metrics` → `# events - LinkedIn visited (date created)`, `# candidates - screen (screened date)`, `# candidates - actual screen (actual screen date)`, `# candidates - move to ATS (moved date)`, `# candidates - offer (offered date)`, `# candidates - hired (hired date)`, `% Viewed to Contacted`, `% Contacted to Reacted`, `% Reacted to Actual Screen`

**12. _(untitled)_** — *card*
  - `LastRefreshedDate` → `LastRefreshedDate`

**13. _(untitled)_** — *clusteredColumnChart*
  - `Calendar` → `Year`, `Q number`, `MonthName`
  - `Metrics` → `# candidates - actual screen (actual screen date)`, `# candidates - offer (offered date)`, `# candidates - hired (hired date)`
  - `job` → `is_external_recruiter`

**14. _(untitled)_** — *areaChart*
  - `Calendar` → `Year`, `Q number`, `MonthName`
  - `Metrics` → `TA linkedin candidate screening time`
  - `job` → `is_external_recruiter`

**15. _(untitled)_** — *funnel*
  - `Metrics` → `# candidates - screen (screened date)`, `# candidates - actual screen (actual screen date)`, `# candidates - move to ATS (moved date)`, `# candidates - offer (offered date)`, `# candidates - hired (hired date)`
  - `job` → `is_external_recruiter`

**16. _(untitled)_** — *image*

**17. _(untitled)_** — *slicer*
  - `job` → `job_category`

**18. _(untitled)_** — *?*

**19. _(untitled)_** — *shape*

**20. _(untitled)_** — *shape*

**21. _(untitled)_** — *HierarchySlicer1458836712039*
  - `Calendar` → `Year`, `Q number`, `MonthName`

**22. _(untitled)_** — *actionButton*

**23. _(untitled)_** — *slicer*
  - `job` → `job_subcategory`, `job_category`

**24. _(untitled)_** — *slicer*
  - `job` → `job_title`

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

### `# candidates - actual screen (actual screen date)` · table: `Metrics`

```dax
var acutalscreened = CALCULATE(COUNT(candidate_stage[candidate_id]), USERELATIONSHIP('Calendar'[Date],candidate_stage[date_screen_actual]),event[event_type]="Evaluation")
			RETURN CALCULATE(acutalscreened, candidate_stage[date_screen_actual]<>BLANK())
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

### `% Contacted to Reacted` · table: `Metrics`

```dax
var perc = DIVIDE(Metrics[# candidates - reacted (contacted date)],Metrics[# candidates - contacted (contacted date)],0)
			RETURN IF(perc>1, 1, perc)
```

### `% Move to client / Screen actual (date created)` · table: `Metrics`

```dax
DIVIDE(Metrics[# candidates - move to ATS (moved date)],Metrics[# candidates - actual screen (actual screen date)],0)
```

### `% Offer to Hire` · table: `Metrics`

```dax
var perc = DIVIDE([# candidates - hired (hired date)],[# candidates - offer (offered date)],0)
			RETURN IF(perc>1, 1, perc)
```

### `% Reacted to Actual Screen` · table: `Metrics`

```dax
var perc = DIVIDE(Metrics[# candidates - actual screen (actual screen date)],Metrics[# candidates - reacted (contacted date)],0)
			RETURN IF(perc>1, 1, perc)
```

### `% Screen to ATS` · table: `Metrics`

```dax
var perc = DIVIDE([# candidates - move to ATS (moved date)],[# candidates - screen (contacted date)],0)
			RETURN IF(perc>1, 1, perc)
```

### `% Screens Actual to ATS` · table: `Metrics`

```dax
var perc = DIVIDE([# candidates - move to ATS (moved date)],[# candidates - actual screen (actual screen date)])
			RETURN IF(perc>1, 1, perc)
```

### `% Viewed to Contacted` · table: `Metrics`

```dax
VAR perc = DIVIDE([# candidates - contacted (contacted date)], [# events - LinkedIn visited (date created)],0)
			RETURN IF(perc>1, 1, perc)
```

### `TA linkedin candidate screening time` · table: `Metrics`

```dax
var linkedin = [# events - LinkedIn visited (date created)]/60
			var screentime = [# candidates - actual screen (actual screen date)]/2
			RETURN IF(linkedin + screentime=0, BLANK(), linkedin + screentime)
```

## Calculated columns referenced (with DAX)

### `Diff Hired - Job created` · table: `job`

```dax
var d = DATEDIFF(job[date_created], job[date_first_hired],DAY)
			RETURN IF(d<0, BLANK(), d)
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
- [ ] `# candidates - actual screen (actual screen date)`
- [ ] `# candidates - hired (hired date)`
- [ ] `# candidates - move to ATS (moved date)`
- [ ] `# candidates - offer (offered date)`
- [ ] `# candidates - screen (screened date)`
- [ ] `# events - LinkedIn visited (date created)`
- [ ] `% Contacted to Reacted`
- [ ] `% Move to client / Screen actual (date created)`
- [ ] `% Offer to Hire`
- [ ] `% Reacted to Actual Screen`
- [ ] `% Screen to ATS`
- [ ] `% Screens Actual to ATS`
- [ ] `% Viewed to Contacted`
- [ ] `TA linkedin candidate screening time`

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
