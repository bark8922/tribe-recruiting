# Homework: Pipelines Health

**Time budget:** ~20-30 min + one CSV export.  
**How to do this:** read the reference section first so you're reminded what's on the page, then fill in the homework section at the bottom. Don't explain anything already covered by the DAX below — we have that. We want the *stuff in your head* that the DAX doesn't capture.

**Prefilled from PBIP:** 16 visuals · 7 tables · 20 measures · 3 calc columns

---

# Reference (prefilled — don't edit)

## Tables touched on this page

`Calendar`, `LastRefreshedDate`, `Metrics`, `WBR Client History`, `client`, `event`, `job`

## Visuals on this page

**1. _(untitled)_** — *actionButton*

**2. _(untitled)_** — *shape*

**3. _(untitled)_** — *tableEx*
  - `client` → `client_name`
  - `job` → `% Problem Jobs`, `# Problem jobs`, `# Total Jobs for calculaing problem jobs`, `Job Days Opened w/o hires`, `job_recruiter`, `date_created`
  - `Metrics` → `% Viewed to Contacted`, `% Contacted to Reacted`, `% Reacted to Actual Screen`

**4. _(untitled)_** — *?*

**5. _(untitled)_** — *pivotTable*
  - `WBR Client History` → `Client`, `Week start end`, `Contacted Target Reached %`, `Screens Target Reached %`
  - `Metrics` → `% Viewed to Contacted`, `% Contacted to Reacted`, `% Reacted to Actual Screen`

**6. _(untitled)_** — *textbox*

**7. _(untitled)_** — *shape*

**8. _(untitled)_** — *card*
  - `LastRefreshedDate` → `LastRefreshedDate`

**9. _(untitled)_** — *barChart*
  - `job` → `job_recruiter`, `job_id`, `Job Days Opened`, `Problem jobs`, `date_created`
  - `client` → `client_name`
  - `Metrics` → `# candidates - contacted (since job created)`

**10. _(untitled)_** — *tableEx*
  - `client` → `client_name`
  - `job` → `user_hiring_manager`, `job_recruiter`, `job_title`, `is_job_archived`, `date_created`, `date_first_hired`, `Job Days Opened w/o hires`, `Job Days Opened`, `Problem jobs`
  - `Metrics` → `# candidates - contacted (since job created)`, `# candidates - screen (since job created)`, `# candidates - actual screen (since job open)`, `# candidates - move to ATS (since job created)`, `# candidates - offer (since job created)`, `# candidates - hired (hired date)`, `% Viewed to Contacted`, `% Contacted to Reacted`, `% Reacted to Actual Screen`

**11. _(untitled)_** — *pivotTable*
  - `event` → `who_created_event_first`
  - `job` → `job_title`, `job_recruiter`, `date_created`, `job_id`, `Job Days Opened`, `is_job_archived`
  - `client` → `client_name`
  - `Metrics` → `% Screens Actual to ATS`, `# candidates - contacted (since job created)`, `# candidates - screen (since job created)`, `# candidates - actual screen (since job open)`, `# candidates - move to ATS (since job created)`, `# candidates - offer (since job created)`, `# candidates - hired (hired date)`, `% Viewed to Contacted`, `% Contacted to Reacted`, `% Reacted to Actual Screen`

**12. _(untitled)_** — *actionButton*

**13. _(untitled)_** — *slicer*
  - `job` → `is_job_archived`

**14. _(untitled)_** — *barChart*
  - `job` → `user_hiring_manager`, `job_id`, `Job Days Opened`, `Problem jobs`, `date_created`
  - `client` → `client_name`
  - `Metrics` → `# candidates - contacted (since job created)`

**15. _(untitled)_** — *pivotTable*
  - `job` → `job_recruiter`, `Problem Pipelines %`, `Total Active Pipelines`, `0-30 days`, `30-60 days`, `> 60 days`, `is_job_archived`, `Problem jobs`
  - `Metrics` → `% Viewed to Contacted`, `% Contacted to Reacted`, `% Reacted to Actual Screen`, `# candidates - actual screen (since job open)`, `# candidates - contacted (since job created)`
  - `client` → `client_name`
  - `Calendar` → `WeekInt`

**16. _(untitled)_** — *image*

## Measures referenced (with full DAX)

### `# Problem jobs` · table: `job`

```dax
CALCULATE(COUNT(job[job_id]), job[Problem jobs]=1)
```

### `# Total Jobs for calculaing problem jobs` · table: `job`

```dax
CALCULATE(COUNT(job[job_id]), job[# ATS]>2)
```

### `# candidates - actual screen (since job open)` · table: `Metrics`

```dax
var acutalscreened = CALCULATE(COUNT(candidate_stage[candidate_id]), event[event_type]="Evaluation", ALL('Calendar'))
			RETURN CALCULATE(acutalscreened, candidate_stage[date_screen_actual]<>BLANK())
```

### `# candidates - contacted (since job created)` · table: `Metrics`

```dax
```
			var contact = CALCULATE(COUNT(candidate_stage[candidate_id]), ALL('Calendar'))
			RETURN CALCULATE(contact, candidate_stage[date_contacted]<>BLANK())

			```
```

### `# candidates - hired (hired date)` · table: `Metrics`

```dax
var hire = CALCULATE(COUNT(candidate_stage[candidate_id]), USERELATIONSHIP('Calendar'[Date],candidate_stage[date_hired]),event[moved_to_stage]="Hired")
			RETURN CALCULATE(hire, candidate_stage[date_hired]<>BLANK())
```

### `# candidates - move to ATS (since job created)` · table: `Metrics`

```dax
var moveto = CALCULATE(COUNT(candidate_stage[candidate_id]), event[moved_to_stage]="Moved to ATS", ALL('Calendar'))
			RETURN CALCULATE(moveto, candidate_stage[date_interview]<>BLANK())
```

### `# candidates - offer (since job created)` · table: `Metrics`

```dax
var offer = CALCULATE(COUNT(candidate_stage[candidate_id]), event[moved_to_stageType]="Offer", ALL('Calendar'))
			RETURN CALCULATE(offer, candidate_stage[date_offer]<>BLANK())
```

### `# candidates - screen (since job created)` · table: `Metrics`

```dax
var screened = CALCULATE(COUNT(candidate_stage[candidate_id]), event[moved_to_stage]="Recruiter Screen", ALL('Calendar'))
			RETURN CALCULATE(screened, candidate_stage[date_screen]<>BLANK())
```

### `% Contacted to Reacted` · table: `Metrics`

```dax
var perc = DIVIDE(Metrics[# candidates - reacted (contacted date)],Metrics[# candidates - contacted (contacted date)],0)
			RETURN IF(perc>1, 1, perc)
```

### `% Problem Jobs` · table: `job`

```dax
DIVIDE([# Problem jobs], [# Total Jobs for calculaing problem jobs])
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

### `> 60 days` · table: `job`

```dax
CALCULATE(COUNT(job[job_id]), job[Job Days Opened]>60)
```

### `Contacted Target Reached %` · table: `WBR Client History`

```dax
```
			var con = CALCULATE(COUNT('WBR Client History'[Week start end]), 'WBR Client History'[Contacted Reached]="Reached")+0
			var wee = DISTINCTCOUNT('WBR Client History'[Week start end]) 
			RETURN
			con/wee
			```
```

### `Problem Pipelines %` · table: `job`

```dax
DIVIDE(
			    CALCULATE(DISTINCTCOUNT(job[job_id]), job[Problem jobs]=1),
			    [Total Active Pipelines])
```

### `Screens Target Reached %` · table: `WBR Client History`

```dax
```
			var scr = CALCULATE(COUNT('WBR Client History'[Week start end]), 'WBR Client History'[Screens Reached]="Reached")+0
			var wee = DISTINCTCOUNT('WBR Client History'[Week start end]) 
			RETURN
			scr/wee
			```
```

### `Total Active Pipelines` · table: `job`

```dax
CALCULATE(DISTINCTCOUNT(job[job_id]), REMOVEFILTERS(job[Problem jobs]))
```

## Calculated columns referenced (with DAX)

### `Job Days Opened` · table: `job`

```dax
DATEDIFF(job[date_created], TODAY(), DAY)
```

### `Job Days Opened w/o hires` · table: `job`

```dax
IF(job[date_first_hired]=BLANK(), DATEDIFF(job[date_created], TODAY(), DAY), BLANK())
```

### `Problem jobs` · table: `job`

```dax
```
			IF(
			    ([# candidates - actual screen (actual screen date)]>=25 && job[date_first_hired]=BLANK() && [# candidates - hired (hired date)]=BLANK()) ||
			    ([# candidates - actual screen (actual screen date)]/[# candidates - hired (hired date)]>=32 && job[date_first_hired]<>BLANK() && [# candidates - hired (hired date)]<>BLANK())
			    , 
			    1, 0)
			```
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

- [ ] `# Problem jobs`
- [ ] `# Total Jobs for calculaing problem jobs`
- [ ] `# candidates - actual screen (since job open)`
- [ ] `# candidates - contacted (since job created)`
- [ ] `# candidates - hired (hired date)`
- [ ] `# candidates - move to ATS (since job created)`
- [ ] `# candidates - offer (since job created)`
- [ ] `# candidates - screen (since job created)`
- [ ] `% Contacted to Reacted`
- [ ] `% Problem Jobs`
- [ ] `% Reacted to Actual Screen`
- [ ] `% Screens Actual to ATS`
- [ ] `% Viewed to Contacted`
- [ ] `0-30 days`
- [ ] `30-60 days`
- [ ] `> 60 days`
- [ ] `Contacted Target Reached %`
- [ ] `Problem Pipelines %`
- [ ] `Screens Target Reached %`
- [ ] `Total Active Pipelines`

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
