# Weekly Progress

**Bucket:** C or D (see `../PAGE_INVENTORY.md`)
**Visuals:** 19
**Tables referenced:** Calendar, LastRefreshedDate, Metrics, client, event, job
**Measures used:** 16
**Calculated columns used:** 2

## What this page answers (fill in from screenshot)

> _TODO: Blake/Andy writes a 2-3 sentence plain-English description of what question this page answers and who uses it._

## Rebuild decision

- [ ] **Keep** (rebuild in new dashboard) — priority: _low / med / high_
- [ ] **Fold in** to existing section: _________
- [ ] **Archive only** (logic preserved here, no live dashboard)
- [ ] **Drop** (nobody uses it)

Notes: _________

## Visuals

### 1. _(untitled)_  _(card)_
- `LastRefreshedDate` → `LastRefreshedDate`

### 2. _(untitled)_  _(shape)_
- _no refs extracted_

### 3. Weekly Performance  _(pivotTable)_
- `Calendar` → `Week start end`
- `client` → `client_name`
- `Metrics` → `# events - LinkedIn visited (date created)`, `# candidates - contacted (contacted date)`, `Candidate Response`, `# candidates - positive response`, `# candidates - screen (screened date)`, `# candidates - actual screen (actual screen date)`, `# candidates - move to ATS (moved date)`, `# candidates - offer (offered date)`, `# candidates - hired (hired date)`, `% Viewed to Contacted`, `% Contacted to Reacted`, `% Contacted to Positive Response`, `% Positive Response to Screen`, `% Screen to ATS`, `% Screens Actual to ATS`
- `job` → `job_title`
- `event` → `who_event_created_for`

### 4. _(untitled)_  _(slicer)_
- `job` → `is_job_archived`, `job_title`, `date_created`

### 5. _(untitled)_  _(slicer)_
- `client` → `client_name`
- `job` → `job_title`

### 6. _(untitled)_  _(slicer)_
- `event` → `who_created_event_first`

### 7. _(untitled)_  _(textbox)_
- _no refs extracted_

### 8. _(untitled)_  _(actionButton)_
- _no refs extracted_

### 9. Date (Uncheck the above filter)  _(HierarchySlicer1458836712039)_
- `Calendar` → `Year`, `Q number`, `MonthName`

### 10. _(untitled)_  _(shape)_
- _no refs extracted_

### 11. _(untitled)_  _(slicer)_
- `event` → `who_event_created_for`

### 12. Last 12 Weeks?  _(ChicletSlicer1448559807354)_
- `Calendar` → `is_last_12_weeks`

### 13. Total Progress  _(funnel)_
- `Metrics` → `# events - LinkedIn visited (date created)`, `# candidates - contacted (contacted date)`, `# candidates - reacted (contacted date)`, `# candidates - positive response`, `# candidates - screen (screened date)`, `# candidates - actual screen (actual screen date)`, `# candidates - move to ATS (moved date)`, `# candidates - offer (offered date)`, `# candidates - hired (hired date)`

### 14. _(untitled)_  _(image)_
- _no refs extracted_

### 15. Key Metrics by Week/Month  _(lineClusteredColumnComboChart)_
- `Calendar` → `Year/Month`, `Week start end`
- `Metrics` → `# candidates - actual screen (actual screen date)`, `% Positive Response to Screen`

### 16. _(untitled)_  _(slicer)_
- `job` → `External Recruiter?`, `job_title`, `date_created`

### 17. _(untitled)_  _(?)_
- _no refs extracted_

### 18. Monthly Performance  _(pivotTable)_
- `Calendar` → `Year/Month`
- `Metrics` → `# events - LinkedIn visited (date created)`, `# candidates - contacted (contacted date)`, `Candidate Response`, `# candidates - positive response`, `# candidates - screen (screened date)`, `# candidates - actual screen (actual screen date)`, `# candidates - move to ATS (moved date)`, `# candidates - offer (offered date)`, `# candidates - hired (hired date)`, `% Viewed to Contacted`, `% Contacted to Reacted`, `% Contacted to Positive Response`, `% Positive Response to Screen`, `% Screen to ATS`, `% Screens Actual to ATS`
- `job` → `job_title`
- `event` → `who_event_created_for`

### 19. _(untitled)_  _(slicer)_
- `job` → `job_title`, `date_created`

## Measures used (with DAX)

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

### `# candidates - reacted (contacted date)` _(table: Metrics)_

```dax
CALCULATE([# candidates - contacted (contacted date)],candidate[is_candidate_reacted]=TRUE())
```

### `# candidates - screen (screened date)` _(table: Metrics)_

```dax
var screened = CALCULATE(COUNT(candidate_stage[candidate_id]), USERELATIONSHIP('Calendar'[Date],candidate_stage[date_screen]), event[moved_to_stage]="Recruiter Screen")
			RETURN CALCULATE(screened, candidate_stage[date_screen]<>BLANK())
```

### `# events - LinkedIn visited (date created)` _(table: Metrics)_

```dax
CALCULATE(DISTINCTCOUNT(event[talent_id + job_id]),event[event_type]="Linkedin Visited Profile",
			USERELATIONSHIP(job[job_id], event[job_id]), USERELATIONSHIP('Calendar'[Date], event[date_created]))
```

### `% Contacted to Positive Response` _(table: Metrics)_

```dax
var con = CALCULATE([# candidates - contacted (contacted date)], candidate_stage[date_contacted]>=DATE(2025,4,14))
			var perc = DIVIDE([# candidates - positive response], con, 0)
			RETURN IF(perc>1, 1, perc)
```

### `% Contacted to Reacted` _(table: Metrics)_

```dax
var perc = DIVIDE(Metrics[# candidates - reacted (contacted date)],Metrics[# candidates - contacted (contacted date)],0)
			RETURN IF(perc>1, 1, perc)
```

### `% Positive Response to Screen` _(table: Metrics)_

```dax
var perc = DIVIDE([# candidates - screen (screened date)], [# candidates - positive response],0)
			RETURN IF(perc>1, 1, perc)
```

### `% Screen to ATS` _(table: Metrics)_

```dax
var perc = DIVIDE([# candidates - move to ATS (moved date)],[# candidates - screen (contacted date)],0)
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

### `Candidate Response` _(table: Metrics)_

```dax
COUNTROWS (
			    FILTER (
			        candidate,
			            candidate[Response count] > 0
			            ||
			            (
			                NOT ISBLANK ( candidate[reason_not_interested] )
			                && candidate[reason_not_interested] <> "Unresponsive"
			            )
			            ||
			            candidate[is_candidate_reacted] = TRUE ()
			            ||
			            NOT (
			                candidate[Current Stage] IN {
			                    "Contacted",
			                    "Applied",
			                    "Prospects",
			                    BLANK ()
			                }
			            )
			    )
			)
```

## Calculated columns used

### `External Recruiter?` _(table: job)_

```dax
IF(job[is_external_recruiter]=TRUE(), "Yes", "No")
```

### `Year/Month` _(table: analytic_usage)_

```dax
FORMAT(analytic_usage[created_date], "YYYY/MM")
```

## Plain columns used

- `LastRefreshedDate` (table: LastRefreshedDate)
- `MonthName` (table: Calendar)
- `Q number` (table: Calendar)
- `Week start end` (table: WBR TS Actual)
- `Year` (table: WBR TS Comment)
- `client_name` (table: client)
- `date_created` (table: talent_email)
- `is_job_archived` (table: job)
- `is_last_12_weeks` (table: WBR TS Actual)
- `job_title` (table: job)
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
