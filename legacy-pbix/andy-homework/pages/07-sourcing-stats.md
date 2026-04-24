# Homework: Sourcing Stats

**Time budget:** ~20-30 min + one CSV export.  
**How to do this:** read the reference section first so you're reminded what's on the page, then fill in the homework section at the bottom. Don't explain anything already covered by the DAX below — we have that. We want the *stuff in your head* that the DAX doesn't capture.

**Prefilled from PBIP:** 23 visuals · 8 tables · 9 measures · 6 calc columns

---

# Reference (prefilled — don't edit)

## Tables touched on this page

`Calendar`, `LastRefreshedDate`, `Metrics`, `Sourcing Stats`, `WBR Client History`, `client`, `event`, `job`

## Visuals on this page

**1. _(untitled)_** — *shape*

**2. _(untitled)_** — *lineChart*
  - `Calendar` → `Date`, `DayOfWeekName`
  - `Metrics` → `# events - LinkedIn visited (date created)`, `# candidates - contacted (contacted date)`

**3. _(untitled)_** — *shape*

**4. _(untitled)_** — *textbox*

**5. _(untitled)_** — *?*

**6. _(untitled)_** — *card*
  - `Sourcing Stats` → `Date`

**7. _(untitled)_** — *clusteredColumnChart*
  - `Calendar` → `DayOfWeekName`
  - `Metrics` → `% Viewed to Contacted`, `# events - LinkedIn visited (date created)`, `# candidates - contacted (contacted date)`, `# events - contacted (date created)`
  - `event` → `dayhour`

**8. _(untitled)_** — *card*
  - `Calendar` → `Date`, `DayOfWeekName`, `Before today`, `is_last_6_weeks`

**9. _(untitled)_** — *shape*

**10. _(untitled)_** — *actionButton*

**11. _(untitled)_** — *HierarchySlicer1458836712039*
  - `Calendar` → `Year`, `Q number`, `MonthName`

**12. _(untitled)_** — *pivotTable*
  - `event` → `dayhour`
  - `Calendar` → `Date`
  - `Metrics` → `# events - LinkedIn visited (date created)`
  - `job` → `job_title`

**13. _(untitled)_** — *slicer*
  - `client` → `client_name`
  - `job` → `job_title`

**14. _(untitled)_** — *ChicletSlicer1448559807354*
  - `Calendar` → `is_last_6_weeks`, `is_last_12_weeks`

**15. _(untitled)_** — *shape*

**16. _(untitled)_** — *clusteredColumnChart*
  - `event` → `dayhour`
  - `Metrics` → `% Viewed to Contacted`, `# events - LinkedIn visited (date created)`, `# candidates - contacted (contacted date)`, `# events - contacted (date created)`

**17. _(untitled)_** — *card*
  - `LastRefreshedDate` → `LastRefreshedDate`

**18. _(untitled)_** — *slicer*
  - `event` → `who_created_event_first`

**19. _(untitled)_** — *shape*

**20. _(untitled)_** — *image*

**21. _(untitled)_** — *hundredPercentStackedBarChart*
  - `Sourcing Stats` → `Less than 50`, `50-100`, `100- 200`, `More than 200`

**22. _(untitled)_** — *pivotTable*
  - `WBR Client History` → `Client`, `Week start end`, `Contacted Target Reached %`, `Screens Target Reached %`
  - `Metrics` → `% Viewed to Contacted`, `% Contacted to Reacted`, `% Reacted to Actual Screen`

**23. _(untitled)_** — *pivotTable*
  - `client` → `client_name`
  - `event` → `who_created_event_first`
  - `Metrics` → `Finance sourcer allocation per client`, `% Viewed to Contacted`, `% Contacted to Reacted`, `% Reacted to Actual Screen`

## Measures referenced (with full DAX)

### `# candidates - contacted (contacted date)` · table: `Metrics`

```dax
var contact = CALCULATE(COUNT(candidate_stage[candidate_id]), USERELATIONSHIP('Calendar'[Date],candidate_stage[date_contacted]))
			RETURN CALCULATE(contact, candidate_stage[date_contacted]<>BLANK())
```

### `# events - LinkedIn visited (date created)` · table: `Metrics`

```dax
CALCULATE(DISTINCTCOUNT(event[talent_id + job_id]),event[event_type]="Linkedin Visited Profile",
			USERELATIONSHIP(job[job_id], event[job_id]), USERELATIONSHIP('Calendar'[Date], event[date_created]))
```

### `# events - contacted (date created)` · table: `Metrics`

```dax
CALCULATE(DISTINCTCOUNT(candidate[candidate_id]), event[event_type]="Moved to stage" && event[moved_to_stage]="Contacted")
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

### `Contacted Target Reached %` · table: `WBR Client History`

```dax
```
			var con = CALCULATE(COUNT('WBR Client History'[Week start end]), 'WBR Client History'[Contacted Reached]="Reached")+0
			var wee = DISTINCTCOUNT('WBR Client History'[Week start end]) 
			RETURN
			con/wee
			```
```

### `Finance sourcer allocation per client` · table: `Metrics`

```dax
DIVIDE([# candidates - contacted (contacted date)], CALCULATE([# candidates - contacted (contacted date)], REMOVEFILTERS(client[client_name])))
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

## Calculated columns referenced (with DAX)

### `100- 200` · table: `Sourcing Stats`

```dax
IF('Sourcing Stats'[Viewed]<200 && 'Sourcing Stats'[Viewed]>=100, 1, BLANK())
```

### `50-100` · table: `Sourcing Stats`

```dax
IF('Sourcing Stats'[Viewed]>=50 && 'Sourcing Stats'[Viewed]<100, 1, BLANK())
```

### `Before today` · table: `Calendar`

```dax
IF(DATEDIFF('Calendar'[Date], TODAY(),DAY)>=0, TRUE(), FALSE())
```

### `Less than 50` · table: `Sourcing Stats`

```dax
IF('Sourcing Stats'[Viewed]<50, 1, BLANK())
```

### `More than 200` · table: `Sourcing Stats`

```dax
IF('Sourcing Stats'[Viewed]>=200, 1, BLANK())
```

### `is_last_6_weeks` · table: `Calendar`

```dax
if('CALENDAR'[WeekEnding]>='CALENDAR'[WeekEnding -6w] && 'CALENDAR'[WeekEnding]<='CALENDAR'[WeekEnding Current], "Yes", "No")
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

- [ ] `# candidates - contacted (contacted date)`
- [ ] `# events - LinkedIn visited (date created)`
- [ ] `# events - contacted (date created)`
- [ ] `% Contacted to Reacted`
- [ ] `% Reacted to Actual Screen`
- [ ] `% Viewed to Contacted`
- [ ] `Contacted Target Reached %`
- [ ] `Finance sourcer allocation per client`
- [ ] `Screens Target Reached %`

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
