# Pipelines Health

**Bucket:** C or D (see `../PAGE_INVENTORY.md`)
**Visuals:** 16
**Tables referenced:** Calendar, LastRefreshedDate, Metrics, WBR Client History, client, event, job
**Measures used:** 20
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

### 1. _(untitled)_  _(actionButton)_
- _no refs extracted_

### 2. _(untitled)_  _(shape)_
- _no refs extracted_

### 3. Problem Pipelines by Client  _(tableEx)_
- `client` → `client_name`
- `job` → `% Problem Jobs`, `# Problem jobs`, `# Total Jobs for calculaing problem jobs`, `Job Days Opened w/o hires`, `job_recruiter`, `date_created`
- `Metrics` → `% Viewed to Contacted`, `% Contacted to Reacted`, `% Reacted to Actual Screen`

### 4. _(untitled)_  _(?)_
- _no refs extracted_

### 5. Client''s Target Last 12 Weeks  _(pivotTable)_
- `WBR Client History` → `Client`, `Week start end`, `Contacted Target Reached %`, `Screens Target Reached %`
- `Metrics` → `% Viewed to Contacted`, `% Contacted to Reacted`, `% Reacted to Actual Screen`

### 6. _(untitled)_  _(textbox)_
- _no refs extracted_

### 7. _(untitled)_  _(shape)_
- _no refs extracted_

### 8. _(untitled)_  _(card)_
- `LastRefreshedDate` → `LastRefreshedDate`

### 9. Problem Pipelines by TA  _(barChart)_
- `job` → `job_recruiter`, `job_id`, `Job Days Opened`, `Problem jobs`, `date_created`
- `client` → `client_name`
- `Metrics` → `# candidates - contacted (since job created)`

### 10. Problem Pipelines: jobs that either do not have a first hire with at least 25 actual screens, or required more than a 32:1 actual screens-to-hire ratio  _(tableEx)_
- `client` → `client_name`
- `job` → `user_hiring_manager`, `job_recruiter`, `job_title`, `is_job_archived`, `date_created`, `date_first_hired`, `Job Days Opened w/o hires`, `Job Days Opened`, `Problem jobs`
- `Metrics` → `# candidates - contacted (since job created)`, `# candidates - screen (since job created)`, `# candidates - actual screen (since job open)`, `# candidates - move to ATS (since job created)`, `# candidates - offer (since job created)`, `# candidates - hired (hired date)`, `% Viewed to Contacted`, `% Contacted to Reacted`, `% Reacted to Actual Screen`

### 11. % Actual Screens to ATS is below 50% (Active Pipelines)  _(pivotTable)_
- `event` → `who_created_event_first`
- `job` → `job_title`, `job_recruiter`, `date_created`, `job_id`, `Job Days Opened`, `is_job_archived`
- `client` → `client_name`
- `Metrics` → `% Screens Actual to ATS`, `# candidates - contacted (since job created)`, `# candidates - screen (since job created)`, `# candidates - actual screen (since job open)`, `# candidates - move to ATS (since job created)`, `# candidates - offer (since job created)`, `# candidates - hired (hired date)`, `% Viewed to Contacted`, `% Contacted to Reacted`, `% Reacted to Actual Screen`

### 12. _(untitled)_  _(actionButton)_
- _no refs extracted_

### 13. _(untitled)_  _(slicer)_
- `job` → `is_job_archived`

### 14. Problem Pipelines by Hiring Manager  _(barChart)_
- `job` → `user_hiring_manager`, `job_id`, `Job Days Opened`, `Problem jobs`, `date_created`
- `client` → `client_name`
- `Metrics` → `# candidates - contacted (since job created)`

### 15. Active Problem Pipelines by TA  _(pivotTable)_
- `job` → `job_recruiter`, `Problem Pipelines %`, `Total Active Pipelines`, `0-30 days`, `30-60 days`, `> 60 days`, `is_job_archived`, `Problem jobs`
- `Metrics` → `% Viewed to Contacted`, `% Contacted to Reacted`, `% Reacted to Actual Screen`, `# candidates - actual screen (since job open)`, `# candidates - contacted (since job created)`
- `client` → `client_name`
- `Calendar` → `WeekInt`

### 16. _(untitled)_  _(image)_
- _no refs extracted_

## Measures used (with DAX)

### `# Problem jobs` _(table: job)_

```dax
CALCULATE(COUNT(job[job_id]), job[Problem jobs]=1)
```

### `# Total Jobs for calculaing problem jobs` _(table: job)_

```dax
CALCULATE(COUNT(job[job_id]), job[# ATS]>2)
```

### `# candidates - actual screen (since job open)` _(table: Metrics)_

```dax
var acutalscreened = CALCULATE(COUNT(candidate_stage[candidate_id]), event[event_type]="Evaluation", ALL('Calendar'))
			RETURN CALCULATE(acutalscreened, candidate_stage[date_screen_actual]<>BLANK())
```

### `# candidates - contacted (since job created)` _(table: Metrics)_

```dax
```
			var contact = CALCULATE(COUNT(candidate_stage[candidate_id]), ALL('Calendar'))
			RETURN CALCULATE(contact, candidate_stage[date_contacted]<>BLANK())

			```
```

### `# candidates - hired (hired date)` _(table: Metrics)_

```dax
var hire = CALCULATE(COUNT(candidate_stage[candidate_id]), USERELATIONSHIP('Calendar'[Date],candidate_stage[date_hired]),event[moved_to_stage]="Hired")
			RETURN CALCULATE(hire, candidate_stage[date_hired]<>BLANK())
```

### `# candidates - move to ATS (since job created)` _(table: Metrics)_

```dax
var moveto = CALCULATE(COUNT(candidate_stage[candidate_id]), event[moved_to_stage]="Moved to ATS", ALL('Calendar'))
			RETURN CALCULATE(moveto, candidate_stage[date_interview]<>BLANK())
```

### `# candidates - offer (since job created)` _(table: Metrics)_

```dax
var offer = CALCULATE(COUNT(candidate_stage[candidate_id]), event[moved_to_stageType]="Offer", ALL('Calendar'))
			RETURN CALCULATE(offer, candidate_stage[date_offer]<>BLANK())
```

### `# candidates - screen (since job created)` _(table: Metrics)_

```dax
var screened = CALCULATE(COUNT(candidate_stage[candidate_id]), event[moved_to_stage]="Recruiter Screen", ALL('Calendar'))
			RETURN CALCULATE(screened, candidate_stage[date_screen]<>BLANK())
```

### `% Contacted to Reacted` _(table: Metrics)_

```dax
var perc = DIVIDE(Metrics[# candidates - reacted (contacted date)],Metrics[# candidates - contacted (contacted date)],0)
			RETURN IF(perc>1, 1, perc)
```

### `% Problem Jobs` _(table: job)_

```dax
DIVIDE([# Problem jobs], [# Total Jobs for calculaing problem jobs])
```

### `% Reacted to Actual Screen` _(table: Metrics)_

```dax
var perc = DIVIDE(Metrics[# candidates - actual screen (actual screen date)],Metrics[# candidates - reacted (contacted date)],0)
			RETURN IF(perc>1, 1, perc)
```

### `% Screens Actual to ATS` _(table: Metrics)_

```dax
var perc = DIVIDE([# candidates - move to ATS (moved date)],[# candidates - actual screen (actual screen date)])
			RETURN IF(perc>1, 1, perc)
```

### `% Viewed to Contacted` _(table: Metrics)_

```dax
VAR perc = DIVIDE([# candidates - contacted (contacted date)], [# events - LinkedIn visited (date created)],0)
			RETURN IF(perc>1, 1, perc)
```

### `0-30 days` _(table: job)_

```dax
CALCULATE(COUNT(job[job_id]), job[Job Days Opened]<=30)
```

### `30-60 days` _(table: job)_

```dax
CALCULATE(COUNT(job[job_id]), job[Job Days Opened]>30 && job[Job Days Opened]<=60)
```

### `> 60 days` _(table: job)_

```dax
CALCULATE(COUNT(job[job_id]), job[Job Days Opened]>60)
```

### `Contacted Target Reached %` _(table: WBR Client History)_

```dax
```
			var con = CALCULATE(COUNT('WBR Client History'[Week start end]), 'WBR Client History'[Contacted Reached]="Reached")+0
			var wee = DISTINCTCOUNT('WBR Client History'[Week start end]) 
			RETURN
			con/wee
			```
```

### `Problem Pipelines %` _(table: job)_

```dax
DIVIDE(
			    CALCULATE(DISTINCTCOUNT(job[job_id]), job[Problem jobs]=1),
			    [Total Active Pipelines])
```

### `Screens Target Reached %` _(table: WBR Client History)_

```dax
```
			var scr = CALCULATE(COUNT('WBR Client History'[Week start end]), 'WBR Client History'[Screens Reached]="Reached")+0
			var wee = DISTINCTCOUNT('WBR Client History'[Week start end]) 
			RETURN
			scr/wee
			```
```

### `Total Active Pipelines` _(table: job)_

```dax
CALCULATE(DISTINCTCOUNT(job[job_id]), REMOVEFILTERS(job[Problem jobs]))
```

## Calculated columns used

### `Job Days Opened` _(table: job)_

```dax
DATEDIFF(job[date_created], TODAY(), DAY)
```

### `Job Days Opened w/o hires` _(table: job)_

```dax
IF(job[date_first_hired]=BLANK(), DATEDIFF(job[date_created], TODAY(), DAY), BLANK())
```

### `Problem jobs` _(table: job)_

```dax
```
			IF(
			    ([# candidates - actual screen (actual screen date)]>=25 && job[date_first_hired]=BLANK() && [# candidates - hired (hired date)]=BLANK()) ||
			    ([# candidates - actual screen (actual screen date)]/[# candidates - hired (hired date)]>=32 && job[date_first_hired]<>BLANK() && [# candidates - hired (hired date)]<>BLANK())
			    , 
			    1, 0)
			```
```

## Plain columns used

- `Client` (table: WBR TA Target)
- `LastRefreshedDate` (table: LastRefreshedDate)
- `Week start end` (table: WBR TS Actual)
- `WeekInt` (table: WBR TS Actual)
- `client_name` (table: client)
- `date_created` (table: talent_email)
- `date_first_hired` (table: job)
- `is_job_archived` (table: job)
- `job_id` (table: job)
- `job_recruiter` (table: job)
- `job_title` (table: job)
- `user_hiring_manager` (table: job)
- `who_created_event_first` (table: event)

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

### `WBR Client History`

```m
CALCULATETABLE(SUMMARIZECOLUMNS(
				    'WBR TA Target'[Client],
				    'WBR TA Actual'[Week start end],
				    "Contacted", SUM('WBR TA Actual'[Contacted]),
				    "Screens", SUM('WBR TA Actual'[Actual screens]),
				    "Moved to ATS", SUM('WBR TA Actual'[Moved to ATS]),
				    "Contacted Target", SUM('WBR TA Actual'[Contacted Target]),
				    "Screens Target", SUM('WBR TA Actual'[Moved to ATS Target]),
				    "Moved to ATS Target", SUM('WBR TA Actual'[Moved to ATS Target])),
				    'WBR TA Actual'[Client]<>BLANK(), 'WBR TA Actual'[is_last_12_weeks]="Yes")

	annotation PBI_Id = b57e949f7c9a4d2c9d66bce37093712a
```

### `client`

```m
let
				    Source = Snowflake.Databases("je92638.eu-central-1.snowflakecomputing.com","READER_WH_129",[Implementation="2.0"]),
				    KEBOOLA_855_942138244_Database = Source{[Name="KEBOOLA_855_942138244",Kind="Database"]}[Data],
				    READER_SCHEMA_855_942138244_Schema = KEBOOLA_855_942138244_Database{[Name="READER_SCHEMA_855_942138244",Kind="Schema"]}[Data],
				    v2_client_Table = READER_SCHEMA_855_942138244_Schema{[Name="v2_client",Kind="Table"]}[Data]
				in
				    v2_client_Table

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
