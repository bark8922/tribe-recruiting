# Internal Recruitment

**Bucket:** C or D (see `../PAGE_INVENTORY.md`)
**Visuals:** 21
**Tables referenced:** Calendar, IR Comment, LastRefreshedDate, Metrics, candidate, candidate_stage, event, job
**Measures used:** 17
**Calculated columns used:** 3

## What this page answers (fill in from screenshot)

> _TODO: Blake/Andy writes a 2-3 sentence plain-English description of what question this page answers and who uses it._

## Rebuild decision

- [ ] **Keep** (rebuild in new dashboard) — priority: _low / med / high_
- [ ] **Fold in** to existing section: _________
- [ ] **Archive only** (logic preserved here, no live dashboard)
- [ ] **Drop** (nobody uses it)

Notes: _________

## Visuals

### 1. _(untitled)_  _(slicer)_
- `event` → `who_created_event`

### 2. _(untitled)_  _(shape)_
- _no refs extracted_

### 3. Interviewed By  _(pivotTable)_
- `event` → `who_created_event`
- `job` → `job_title`, `Jobs Opened`
- `Metrics` → `# candidates - actual screen (actual screen date)`

### 4. _(untitled)_  _(cardVisual)_
- `Metrics` → `Job - Time to Hire`, `Job - Time to Find a Hire`, `Job - Time to Fill`, `Sourced Hired`, `Applicant Hired`

### 5. Disqualified Reasons  _(donutChart)_
- `candidate` → `reason_not_interested`
- `Metrics` → `# candidates (contacted date) NOT USED`

### 6. _(untitled)_  _(slicer)_
- `job` → `is_job_archived`, `job_title`, `date_created`

### 7. Sourced By  _(pivotTable)_
- `event` → `who_created_event_first`
- `job` → `job_title`, `Jobs Opened`
- `Metrics` → `# candidates - contacted (contacted date)`, `# candidates - positive response`, `# candidates - hired (hired date)`

### 8. _(untitled)_  _(shape)_
- _no refs extracted_

### 9. _(untitled)_  _(slicer)_
- `candidate` → `source`
- `job` → `job_title`, `date_created`

### 10. Missed Opportunities  _(pivotTable)_
- `IR Comment` → `Week`, `Client`, `Headcount`, `Status`, `Outcome`
- `job` → `job_title`, `Jobs Opened`
- `event` → `who_event_created_for`

### 11. Previous Week?  _(ChicletSlicer1448559807354)_
- `Calendar` → `is_previous_week`

### 12. Total Progress  _(funnel)_
- `Metrics` → `# candidates - contacted (contacted date)`, `# candidates - positive response`, `# candidates - screen (screened date)`, `# candidates - actual screen (actual screen date)`, `# candidates - move to ATS (moved date)`, `Onsite`, `Culture Interview`, `Call with Client Interview`, `# candidates - offer (offered date)`, `# candidates - hired (hired date)`

### 13. Active Jobs  _(pivotTable)_
- `job` → `job_title`, `Jobs Opened`
- `Metrics` → `# candidates - hired (hired date)`
- `event` → `who_event_created_for`

### 14. 2. Date Between  _(slicer)_
- `Calendar` → `Date`

### 15. _(untitled)_  _(actionButton)_
- _no refs extracted_

### 16. # of Candidates Disqualified at Each Stage  _(pivotTable)_
- `candidate_stage` → `Current Stage`, `date_hired`
- `job` → `job_title`, `Jobs Opened`
- `candidate` → `reason_not_interested`

### 17. _(untitled)_  _(slicer)_
- `event` → `who_created_event_first`

### 18. _(untitled)_  _(card)_
- `LastRefreshedDate` → `Last refreshed date`

### 19. Weekly Performance  _(pivotTable)_
- `Calendar` → `Week start end`
- `job` → `job_title`, `Jobs Opened`
- `Metrics` → `# candidates - contacted (contacted date)`, `# candidates - positive response`, `# candidates - screen (screened date)`, `# candidates - actual screen (actual screen date)`, `# candidates - move to ATS (moved date)`, `Onsite`, `Culture Interview`, `Call with Client Interview`, `# candidates - offer (offered date)`, `# candidates - hired (hired date)`
- `event` → `who_event_created_for`

### 20. _(untitled)_  _(slicer)_
- `job` → `job_title`, `date_created`

### 21. _(untitled)_  _(textbox)_
- _no refs extracted_

## Measures used (with DAX)

### `# candidates (contacted date) NOT USED` _(table: Metrics)_

```dax
CALCULATE(COUNT(candidate_stage[candidate_id]), USERELATIONSHIP('Calendar'[Date],candidate_stage[date_contacted]))
```

### `# candidates - actual screen (actual screen date)` _(table: Metrics)_

```dax
var acutalscreened = CALCULATE(COUNT(candidate_stage[candidate_id]), USERELATIONSHIP('Calendar'[Date],candidate_stage[date_screen_actual]),event[event_type]="Evaluation")
			RETURN CALCULATE(acutalscreened, candidate_stage[date_screen_actual]<>BLANK())
```

### `# candidates - contacted (contacted date)` _(table: Metrics)_

```dax
var contact = CALCULATE(COUNT(candidate_stage[candidate_id]), USERELATIONSHIP('Calendar'[Date],candidate_stage[date_contacted]))
			RETURN CALCULATE(contact, candidate_stage[date_contacted]<>BLANK())
```

### `# candidates - hired (hired date)` _(table: Metrics)_

```dax
var hire = CALCULATE(COUNT(candidate_stage[candidate_id]), USERELATIONSHIP('Calendar'[Date],candidate_stage[date_hired]),event[moved_to_stage]="Hired")
			RETURN CALCULATE(hire, candidate_stage[date_hired]<>BLANK())
```

### `# candidates - move to ATS (moved date)` _(table: Metrics)_

```dax
var moveto = CALCULATE(COUNT(candidate_stage[candidate_id]), USERELATIONSHIP('Calendar'[Date],candidate_stage[date_interview]), event[moved_to_stage]="Moved to ATS")
			RETURN CALCULATE(moveto, candidate_stage[date_interview]<>BLANK())
```

### `# candidates - offer (offered date)` _(table: Metrics)_

```dax
var offer = CALCULATE(COUNT(candidate_stage[candidate_id]), USERELATIONSHIP('Calendar'[Date],candidate_stage[date_offer]), event[moved_to_stageType]="Offer")
			RETURN CALCULATE(offer, candidate_stage[date_offer]<>BLANK())
```

### `# candidates - positive response` _(table: Metrics)_

```dax
CALCULATE(DISTINCTCOUNT(event[candidate_id]), event[moved_to_stageType]="Positive Response", event[date_created]>=DATE(2025,4,14), USERELATIONSHIP(event[date_created], 'Calendar'[Date]))
```

### `# candidates - screen (screened date)` _(table: Metrics)_

```dax
var screened = CALCULATE(COUNT(candidate_stage[candidate_id]), USERELATIONSHIP('Calendar'[Date],candidate_stage[date_screen]), event[moved_to_stage]="Recruiter Screen")
			RETURN CALCULATE(screened, candidate_stage[date_screen]<>BLANK())
```

### `Applicant Hired` _(table: Metrics)_

```dax
CALCULATE([# candidates - hired (hired date)], candidate[source]="Applicant")
```

### `Call with Client Interview` _(table: Metrics)_

```dax
CALCULATE(DISTINCTCOUNT(event[candidate_id]), event[moved_to_stageType]="Final Interview", event[moved_to_stage]="Call with Client", USERELATIONSHIP(event[date_created], 'Calendar'[Date]))
```

### `Culture Interview` _(table: Metrics)_

```dax
CALCULATE(DISTINCTCOUNT(event[candidate_id]), event[moved_to_stageType]="Final Interview", event[moved_to_stage]="Culture Interview", USERELATIONSHIP(event[date_created], 'Calendar'[Date]))
```

### `Job - Time to Fill` _(table: Metrics)_

```dax
CALCULATE(AVERAGE(job[Diff Hired - Job created]), candidate_stage[Diff Hired - Job created]>0)
```

### `Job - Time to Find a Hire` _(table: Metrics)_

```dax
CALCULATE(AVERAGE(job[Diff Contacted - Job created]), candidate_stage[Diff Concated - Job created]>0)
```

### `Job - Time to Hire` _(table: Metrics)_

```dax
CALCULATE(AVERAGE(job[Diff Contacted - Hired]), job[Diff Contacted - Hired]>0)
```

### `Last refreshed date` _(table: LastRefreshedDate)_

```dax
"Last Updated: " & FORMAT(MAX(LastRefreshedDate[LastRefreshedDate]), "YYYY-MM-DD HH:MM") & " (CET)"
```

### `Onsite` _(table: Metrics)_

```dax
CALCULATE(DISTINCTCOUNT(event[candidate_id]), event[moved_to_stageType]="Final Interview", event[moved_to_stage]="Onsite", USERELATIONSHIP(event[date_created], 'Calendar'[Date]))
```

### `Sourced Hired` _(table: Metrics)_

```dax
CALCULATE([# candidates - hired (hired date)],  candidate[source]="Sourced")
```

## Calculated columns used

### `Jobs Opened` _(table: job)_

```dax
IF(job[date_first_hired]<>BLANK(), job[Diff Hired - Job created], job[Job Days Opened w/o hires])
```

### `Week` _(table: analytic_usage)_

```dax
WEEKNUM(analytic_usage[created_date], 21)
```

### `is_previous_week` _(table: Calendar)_

```dax
if('Calendar'[WeekEnding Current] = 'Calendar'[WeekEnding]+7, TRUE(), FALSE())
```

## Plain columns used

- `Client` (table: WBR TA Target)
- `Current Stage` (table: candidate_stage)
- `Date` (table: Sourcing Stats)
- `Headcount` (table: IR Comment)
- `Outcome` (table: IR Comment)
- `Status` (table: IR Comment)
- `Week start end` (table: WBR TS Actual)
- `date_created` (table: talent_email)
- `date_hired` (table: candidate_stage)
- `is_job_archived` (table: job)
- `job_title` (table: job)
- `reason_not_interested` (table: candidate)
- `source` (table: candidate)
- `who_created_event` (table: event)
- `who_created_event_first` (table: event)
- `who_event_created_for` (table: event)

## Source definitions (Power Query M) for tables used

### `Calendar`

```m
```
				let CreateDateTable = (StartDate as date, EndDate as date, optional Culture as nullable text) as table =>
				  let
					
				    DayCount = Duration.Days(Duration.From(EndDate - StartDate))+1,
				    Source = List.Dates(StartDate,DayCount,#duration(1,0,0,0)),
				    TableFromList = Table.FromList(Source, Splitter.SplitByNothing()),    
				    ChangedType = Table.TransformColumnTypes(TableFromList,{{"Column1", type date}}),
				    RenamedColumns = Table.RenameColumns(ChangedType,{{"Column1", "Date"}}),
				    InsertYear = Table.AddColumn(RenamedColumns, "Year", each Date.Year([Date])),
				    InsertQuarter = Table.AddColumn(InsertYear, "QuarterOfYear", each Date.QuarterOfYear([Date])),
				    InsertMonth = Table.AddColumn(InsertQuarter, "MonthOfYear", each Date.Month([Date])),
				    InsertDay = Table.AddColumn(InsertMonth, "DayOfMonth", each Date.Day([Date])),	
				    InsertDayInt = Table.AddColumn(InsertDay, "DateInt", each [Year] * 10000 + [MonthOfYear] * 100 + [DayOfMonth]),
				    InsertMonthName = Table.AddColumn(InsertDayInt, "MonthName", each Date.ToText([Date], "MMMM", Culture), type text),
				    InsertCalendarMonth = Table.AddColumn(InsertMonthName, "MonthInt", each [Year] * 100 + [MonthOfYear]),
				    InsertCalendarQtr = Table.AddColumn(InsertCalendarMonth, "QuarterInCalendar", each Number.ToText([Year]) & " " & "Q" & Number.ToText([QuarterOfYear]) ),
				    InsertDayWeek = Table.AddColumn(InsertCalendarQtr, "DayInWeek", each (Date.DayOfWeek([Date],Day.Monday)+1)),
				    InsertDayName = Table.AddColumn(InsertDayWeek, "DayOfWeekName", each Date.ToText([Date], "dddd", Culture), type text),
				    InsertWeekEnding = Table.AddColumn(InsertDayName, "WeekEnding", each Date.EndOfWeek([Date], Day.Monday), type date),
				    InsertWeekNum = Table.AddColumn(InsertWeekEnding, "WeekOfYear", each Date.WeekOfYear([Date])),
				    InsertWeekInt = Table.AddColumn(InsertWeekNum, "WeekInt", each [Year] * 100 + [WeekOfYear])
				  in
				    InsertWeekInt,
				    #"Invoked FunctionCreateDateTable" = CreateDateTable(#date(2021,1,1),#date(2026,6,30),"en"),
				    #"Filtered Rows" = Table.SelectRows(#"Invoked FunctionCreateDateTable", each true),
				    #"Change Type" = Table.TransformColumnTypes(#"Filtered Rows",{{"Year", Int64.Type}, {"QuarterOfYear", Int64.Type}, {"MonthOfYear", Int64.Type}, {"DayOfMonth", Int64.Type}, {"DateInt", Int64.Type}, {"DayInWeek", Int64.Type}, {"WeekOfYear", Int64.Type}, {"WeekInt", Int64.Type}}),
				    #
... (truncated)
```

### `IR Comment`

```m
let
				    Source = GoogleSheets.Contents("https://docs.google.com/spreadsheets/d/1Hyb5M244bkh9ygxnssq_wppWxrT0KtGeVFEYvHVoouc/edit?gid=1898344035#gid=1898344035"),
				    IR_Table = Source{[name="IR",ItemKind="Table"]}[Data],
				    #"Promoted Headers" = Table.PromoteHeaders(IR_Table, [PromoteAllScalars=true]),
				    #"Changed Type" = Table.TransformColumnTypes(#"Promoted Headers",{{"Role", type text}, {"Year", Int64.Type}, {"Week", type text}, {"Client", type text}, {"Headcount", Int64.Type}, {"Status", type text}, {"Outcome", type text}}),
				    #"Filtered Rows" = Table.SelectRows(#"Changed Type", each [Job_ID] <> null and [Job_ID] <> ""),
				    #"Filtered Rows1" = Table.SelectRows(#"Filtered Rows", each [Week] <> null and [Week] <> "")
				in
				    #"Filtered Rows1"

	annotation PBI_NavigationStepName = Navigation

	annotation PBI_ResultType = Table
```

### `LastRefreshedDate`

```m
let
				  UTC_DateTimeZone = DateTimeZone.UtcNow(),
				  UTC_Date         = Date.From(UTC_DateTimeZone),
				  StartSummerTime  = Date.StartOfWeek(#date(Date.Year(UTC_Date), 3, 31), Day.Sunday),
				  StartWinterTime  = Date.StartOfWeek(#date(Date.Year(UTC_Date), 10, 31), Day.Sunday),
				  UTC_Offset       = if UTC_Date >= StartSummerTime and UTC_Date < StartWinterTime then 2 else 1,
				  CET_Timezone     = DateTimeZone.SwitchZone(UTC_DateTimeZone, UTC_Offset)
				in
				  CET_Timezone

	annotation PBI_ResultType = DateTimeZone
```

### `Metrics`

```m
let
				    Source = Table.FromRows(Json.Document(Binary.Decompress(Binary.FromText("i45WMlCKjQUA", BinaryEncoding.Base64), Compression.Deflate)), let _t = ((type nullable text) meta [Serialized.Text = true]) in type table [Column1 = _t]),
				    #"Changed Type" = Table.TransformColumnTypes(Source,{{"Column1", Int64.Type}}),
				    #"Removed Columns" = Table.RemoveColumns(#"Changed Type",{"Column1"})
				in
				    #"Removed Columns"

	annotation PBI_ResultType = Table

	annotation PBI_NavigationStepName = Navigation
```

### `candidate`

```m
let
				    Source = Snowflake.Databases("je92638.eu-central-1.snowflakecomputing.com","READER_WH_129",[Implementation="2.0"]),
				    KEBOOLA_855_942138244_Database = Source{[Name="KEBOOLA_855_942138244",Kind="Database"]}[Data],
				    READER_SCHEMA_855_942138244_Schema = KEBOOLA_855_942138244_Database{[Name="READER_SCHEMA_855_942138244",Kind="Schema"]}[Data],
				    v2_candidate_Table = READER_SCHEMA_855_942138244_Schema{[Name="v2_candidate",Kind="Table"]}[Data]
				in
				    v2_candidate_Table

	annotation PBI_ResultType = Exception

	annotation PBI_NavigationStepName = Navigation
```

### `candidate_stage`

```m
```
				let
				    Source = Snowflake.Databases("je92638.eu-central-1.snowflakecomputing.com","READER_WH_129",[Implementation="2.0"]),
				    KEBOOLA_855_942138244_Database = Source{[Name="KEBOOLA_855_942138244",Kind="Database"]}[Data],
				    READER_SCHEMA_855_942138244_Schema = KEBOOLA_855_942138244_Database{[Name="READER_SCHEMA_855_942138244",Kind="Schema"]}[Data],
				    v2_candidate_stage_Table = READER_SCHEMA_855_942138244_Schema{[Name="v2_candidate_stage",Kind="Table"]}[Data],
				    #"Filtered Rows" = Table.SelectRows(v2_candidate_stage_Table, each [date_created] >= #"Start Date"),
				    #"Added Conditional Column" = Table.AddColumn(#"Filtered Rows", "Current Stage", each if [stage_current_type] = "Contacted" then "Contacted"
				
				else if [stage_current_type] = "Recruiter Screen" and [date_screen_actual] is null then "Recruiter Screen"
				
				else if [stage_current_type] = "Recruiter Screen" and [date_screen_actual] <> null then "Actual Screen"
				
				else if [stage_current_type] = "Offsite" then "Move to ATS" 
				else if [stage_current] = "Onsite" then "Onsite" 
				else [stage_current_type]),
				    #"Added Conditional Column1" = Table.AddColumn(#"Added Conditional Column", "Current Stage Num", each if [Current Stage] = "Contacted" then 1 else if [Current Stage] = "Recruiter Screen" then 2 else if [Current Stage] = "Actual Screen" then 3 else if [Current Stage] = "Move to ATS" then 4 else if [Current Stage] = "Onsite" then 5 else if [Current Stage] = "Offer" then 6 else if [Current Stage] = "Hired" then 7 else 10),
				    #"Changed Type" = Table.TransformColumnTypes(#"Added Conditional Column1",{{"Current Stage Num", Int64.Type}})
				in
				    #"Changed Type"
				```

	annotation PBI_ResultType = Table

	annotation PBI_NavigationStepName = Navigation
```

### `event`

```m
```
				let
				    Source = Snowflake.Databases("je92638.eu-central-1.snowflakecomputing.com","READER_WH_129",[Implementation="2.0"]),
				    KEBOOLA_855_942138244_Database = Source{[Name="KEBOOLA_855_942138244",Kind="Database"]}[Data],
				    READER_SCHEMA_855_942138244_Schema = KEBOOLA_855_942138244_Database{[Name="READER_SCHEMA_855_942138244",Kind="Schema"]}[Data],
				    v2_event_Table = READER_SCHEMA_855_942138244_Schema{[Name="v2_event",Kind="Table"]}[Data],
				    #"Extracted Time" = Table.TransformColumns(v2_event_Table,{{"datetime_created", DateTime.Time, type time}}),
				    #"Extracted Hour" = Table.TransformColumns(#"Extracted Time",{{"datetime_created", Time.Hour, Int64.Type}}),
				    #"Renamed Columns" = Table.RenameColumns(#"Extracted Hour",{{"datetime_created", "dayhour"}}),
				    #"Added Custom" = Table.AddColumn(#"Renamed Columns", "Correct", each if [event_type]="Linkedin Visited Profile" and [job_id] <> null then [who_created_event] 
				else
				if [event_type]="Moved to stage" and [moved_to_stage]="Contacted" and [job_id] <> null then [who_created_event] else
				if [event_type]="Moved to stage" and [moved_to_stage]="Prospects" and [job_id] <> null then [who_created_event]
				else
				if [event_type]="Moved to stage" and [moved_to_stageType]="Positive Response" and [job_id] <> null then [who_created_event]
				else
				if [event_type] = "Disqualified" and [job_id] <> null then [who_created_event]
				else
				if [event_type]="Candidate created" and [moved_to_stage]="Contacted" and [job_id] <> null then [who_created_event] else
				if [event_type]="Candidate created" and [moved_to_stage]="Prospects" and [job_id] <> null then [who_created_event] else
				if [event_type]="Candidate created" and [moved_to_stageType]="Prospects" and [job_id] <> null then [who_created_event] else
				if [event_type]="Moved to stage" and [moved_to_stage]="Recruiter Screen" and [job_id] <> null then [who_created_event_first] else
				if ([event_type]="Evaluation" or (Text.Start([event_id], 9)="recruitee" and [moved_to_stageType]="evaluation")) and [job_id] <> null then [who_created_event_first] else 
				if [moved_to_stage]="Moved to ATS" then [who_created_event_first] else
				if [moved_to_stageType]="Offer" then [who_created_event_first] else
				if [moved_to_stage]="Hired" then [who_created_event_first] else null),
				    #"Removed Columns" = Table.RemoveColumns(#"Added Custom",{"who_created_event_first"}),
				    #"Renamed Columns1" = Table.R
... (truncated)
```

### `job`

```m
let
				    Source = Snowflake.Databases("je92638.eu-central-1.snowflakecomputing.com","READER_WH_129",[Implementation="2.0"]),
				    KEBOOLA_855_942138244_Database = Source{[Name="KEBOOLA_855_942138244",Kind="Database"]}[Data],
				    READER_SCHEMA_855_942138244_Schema = KEBOOLA_855_942138244_Database{[Name="READER_SCHEMA_855_942138244",Kind="Schema"]}[Data],
				    v2_job_Table = READER_SCHEMA_855_942138244_Schema{[Name="v2_job",Kind="Table"]}[Data],
				    #"Replaced Value" = Table.ReplaceValue(v2_job_Table,null,false,Replacer.ReplaceValue,{"is_external_recruiter"}),
				    #"Replaced Value1" = Table.ReplaceValue(#"Replaced Value","Ukrainka, Kyiv Oblast, Ukraine, 08720","Ukrainka, Kyiv Oblast, Ukraine",Replacer.ReplaceText,{"job_location"}),
				    #"Replaced Value2" = Table.ReplaceValue(#"Replaced Value1","United Kingdom","UK",Replacer.ReplaceText,{"job_location"}),
				    #"Inserted Text After Delimiter" = Table.AddColumn(#"Replaced Value2", "Job Country", each Text.AfterDelimiter([job_location], ", ", {0, RelativePosition.FromEnd}), type text)
				in
				    #"Inserted Text After Delimiter"

	annotation PBI_ResultType = Table

	annotation PBI_NavigationStepName = Navigation
```
