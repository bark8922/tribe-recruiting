# Homework: TA Actual Screens Target

**Time budget:** ~20-30 min + one CSV export.  
**How to do this:** read the reference section first so you're reminded what's on the page, then fill in the homework section at the bottom. Don't explain anything already covered by the DAX below — we have that. We want the *stuff in your head* that the DAX doesn't capture.

**Prefilled from PBIP:** 16 visuals · 8 tables · 8 measures · 5 calc columns

---

# Reference (prefilled — don't edit)

## Tables touched on this page

`Calendar WBR`, `Historical Manager Structure WBR`, `LastRefreshedDate`, `Metrics`, `WBR Client History`, `WBR Job from Aug 2025`, `WBR TA Actual`, `WBR TS Actual`

## Visuals on this page

**1. _(untitled)_** — *card*
  - `WBR TA Actual` → `% TA Reach Actual Screen Target`

**2. _(untitled)_** — *tableEx*
  - `Historical Manager Structure WBR` → `Report_To`
  - `WBR TA Actual` → `who_event_created_for`, `Client`, `Week start end`, `Actual screens`, `Screens Target`, `Exclude OKR`, `% Actual Screens Target`
  - `Metrics` → `% Viewed to Contacted`, `% Contacted to Reacted`, `% Reacted to Actual Screen`
  - `WBR TS Actual` → `Manager`, `who_created_event_first`, `Week start end`

**3. _(untitled)_** — *shape*

**4. _(untitled)_** — *slicer*
  - `Historical Manager Structure WBR` → `Business Unit`

**5. _(untitled)_** — *tableEx*
  - `WBR TA Actual` → `who_event_created_for`, `Actual screens`, `Hires`, `% Actual Screens to Hired`, `Valid TA for reach actual screens`
  - `WBR Job from Aug 2025` → `# Jobs`
  - `Metrics` → `% Viewed to Contacted`, `% Contacted to Reacted`, `% Reacted to Actual Screen`
  - `Historical Manager Structure WBR` → `Report_To`

**6. _(untitled)_** — *card*
  - `LastRefreshedDate` → `LastRefreshedDate`

**7. _(untitled)_** — *textbox*

**8. _(untitled)_** — *tableEx*
  - `Historical Manager Structure WBR` → `Report_To`
  - `WBR TA Actual` → `Valid TA for reach actual screens`, `Reach Actual Screen Target`, `% TA Reach Actual Screen Target`
  - `Metrics` → `% Viewed to Contacted`, `% Contacted to Reacted`, `% Reacted to Actual Screen`

**9. _(untitled)_** — *clusteredBarChart*
  - `Historical Manager Structure WBR` → `Business Unit`, `Report_To`
  - `WBR TA Actual` → `% TA Reach Actual Screen Target`

**10. _(untitled)_** — *pivotTable*
  - `WBR Client History` → `Client`, `Week start end`, `Contacted Target Reached %`, `Screens Target Reached %`
  - `Metrics` → `% Viewed to Contacted`, `% Contacted to Reacted`, `% Reacted to Actual Screen`

**11. _(untitled)_** — *slicer*
  - `Historical Manager Structure WBR` → `Report_To`

**12. _(untitled)_** — *actionButton*

**13. _(untitled)_** — *image*

**14. _(untitled)_** — *lineChart*
  - `Calendar WBR` → `Week start end`
  - `WBR TA Actual` → `% TA Reach Actual Screen Target`
  - `Historical Manager Structure WBR` → `Report_To`

**15. _(untitled)_** — *?*

**16. _(untitled)_** — *shape*

## Measures referenced (with full DAX)

### `% Actual Screens Target` · table: `WBR TA Actual`

```dax
var target = SUM('WBR TA Actual'[Actual screens])/SUM('WBR TA Actual'[Screens Target])
			RETURN IF(SUM('WBR TA Actual'[Actual screens])=0 || SUM('WBR TA Actual'[Screens Target])=0, BLANK(), target)
```

### `% Actual Screens to Hired` · table: `WBR TA Actual`

```dax
IF(SUM('WBR TA Actual'[Actual screens])=0, BLANK(),
			    SUM('WBR TA Actual'[Hires]) /SUM('WBR TA Actual'[Actual screens]))
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

### `% TA Reach Actual Screen Target` · table: `WBR TA Actual`

```dax
DIVIDE(SUM('WBR TA Actual'[Reach Actual Screen Target]), SUM('WBR TA Actual'[Valid TA for reach actual screens]))
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

### `Business Unit` · table: `Historical Manager Structure WBR`

```dax
```
			SWITCH(TRUE(), 
			'Historical Manager Structure WBR'[Report_To_Next_L2]="Tijana Lazovic", "Ponies and Unicorns",
			'Historical Manager Structure WBR'[Report_To_Next_L2]="Kristjana Thorarinsdottir", "Dolphins and Whales",
			"Other (Internal & Old Org Structure")
			```
```

### `Manager` · table: `WBR TS Actual`

```dax
RELATED('Historical Manager Structure WBR'[Report_To])
```

### `Reach Actual Screen Target` · table: `WBR TA Actual`

```dax
IF('WBR TA Actual'[Screens Target]<>BLANK() && 'WBR TA Actual'[Screens Target]<>0 && 'WBR TA Actual'[Actual screens]>='WBR TA Actual'[Screens Target] && 'WBR TA Actual'[Actual screens]<>BLANK(), 1, 0)
```

### `Screens Target` · table: `WBR TA Actual`

```dax
```
			RELATED('WBR TA Target'[Actual Screens]) 
			```
```

### `Valid TA for reach actual screens` · table: `WBR TA Actual`

```dax
IF('WBR TA Actual'[Screens Target]<>BLANK() && 'WBR TA Actual'[Screens Target]<>0 && 'WBR TA Actual'[Actual screens]<>BLANK() &&
			('WBR TA Actual'[Exclude OKR]<>"Yes" || 'WBR TA Actual'[Actual screens]>='WBR TA Actual'[Screens Target]), 1, 0)
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

- [ ] `% Actual Screens Target`
- [ ] `% Actual Screens to Hired`
- [ ] `% Contacted to Reacted`
- [ ] `% Reacted to Actual Screen`
- [ ] `% TA Reach Actual Screen Target`
- [ ] `% Viewed to Contacted`
- [ ] `Contacted Target Reached %`
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
