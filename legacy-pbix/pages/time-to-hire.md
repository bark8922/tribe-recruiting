# Time to Hire

**Bucket:** C or D (see `../PAGE_INVENTORY.md`)
**Visuals:** 22
**Tables referenced:** Calendar, LastRefreshedDate, Metrics, candidate, candidate_stage, client, event, job, talent
**Measures used:** 12
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
- `event` → `who_event_created_for`
- `job` → `job_title`, `date_created`

### 2. _(untitled)_  _(slicer)_
- `client` → `client_name`
- `job` → `job_title`

### 3. Month Trends  _(lineChart)_
- `Calendar` → `MON-YYYY`
- `Metrics` → `Job - Time to Hire`, `Job - Time to Find a Hire`, `Job - Time to Fill`

### 4. _(untitled)_  _(shape)_
- _no refs extracted_

### 5. First Hired per Job by Client/Job Title  _(pivotTable)_
- `client` → `client_name`
- `job` → `job_title`, `# Job (time to hire)`
- `Metrics` → `Job - Time to Hire`, `Job - Time to Find a Hire`, `Job - Time to Fill`

### 6. First Hired per Job by Client/Job Title  _(pivotTable)_
- `client` → `client_name`
- `job` → `job_title`
- `candidate_stage` → `# Candidate Hired (time to hire)`
- `Metrics` → `Candidate - Time to Fill`, `Candidate - Time to Find a Hire`, `Candidate - Time to Hire`

### 7. Date  _(HierarchySlicer1458836712039)_
- `Calendar` → `Year`, `Q number`, `MonthName`

### 8. _(untitled)_  _(textbox)_
- _no refs extracted_

### 9. _(untitled)_  _(cardVisual)_
- `job` → `# Job (time to hire)`
- `Metrics` → `Job - Time to Hire`, `Job - Time to Find a Hire`, `Job - Time to Fill`

### 10. First Hired per Job by Job Category/Subcategory  _(pivotTable)_
- `job` → `job_category`, `job_subcategory`, `# Job (time to hire)`, `job_title`
- `Metrics` → `Job - Time to Hire`, `Job - Time to Find a Hire`, `Job - Time to Fill`

### 11. _(untitled)_  _(slicer)_
- `job` → `job_title`, `date_created`

### 12. _(untitled)_  _(actionButton)_
- _no refs extracted_

### 13. _(untitled)_  _(actionButton)_
- _no refs extracted_

### 14. _(untitled)_  _(shape)_
- _no refs extracted_

### 15. _(untitled)_  _(slicer)_
- `job` → `Tech Role`, `job_title`, `date_created`

### 16. _(untitled)_  _(textbox)_
- _no refs extracted_

### 17. _(untitled)_  _(image)_
- _no refs extracted_

### 18. _(untitled)_  _(slicer)_
- `candidate` → `source`
- `job` → `job_title`, `date_created`

### 19. _(untitled)_  _(slicer)_
- `job` → `External Recruiter?`, `job_title`, `date_created`

### 20. _(untitled)_  _(card)_
- `LastRefreshedDate` → `LastRefreshedDate`

### 21. Candidate Details  _(tableEx)_
- `Metrics` → `# candidates - contacted (contacted date)`, `% Viewed to Contacted`, `% Contacted to Reacted`, `% Reacted to Actual Screen`
- `client` → `client_name`
- `job` → `job_title`, `job_category`, `job_subcategory`, `date_created`
- `candidate_stage` → `date_contacted`, `date_hired`
- `talent` → `full_name`

### 22. _(untitled)_  _(?)_
- _no refs extracted_

## Measures used (with DAX)

### `# Candidate Hired (time to hire)` _(table: candidate_stage)_

```dax
CALCULATE(COUNT(candidate_stage[candidate_id]), candidate_stage[date_hired]<>BLANK())
```

### `# Job (time to hire)` _(table: job)_

```dax
CALCULATE(COUNT(job[job_id]), job[date_first_hired]<>BLANK(), job[Diff Hired - Job created]>=0)
```

### `# candidates - contacted (contacted date)` _(table: Metrics)_

```dax
var contact = CALCULATE(COUNT(candidate_stage[candidate_id]), USERELATIONSHIP('Calendar'[Date],candidate_stage[date_contacted]))
			RETURN CALCULATE(contact, candidate_stage[date_contacted]<>BLANK())
```

### `% Contacted to Reacted` _(table: Metrics)_

```dax
var perc = DIVIDE(Metrics[# candidates - reacted (contacted date)],Metrics[# candidates - contacted (contacted date)],0)
			RETURN IF(perc>1, 1, perc)
```

### `% Reacted to Actual Screen` _(table: Metrics)_

```dax
var perc = DIVIDE(Metrics[# candidates - actual screen (actual screen date)],Metrics[# candidates - reacted (contacted date)],0)
			RETURN IF(perc>1, 1, perc)
```

### `% Viewed to Contacted` _(table: Metrics)_

```dax
VAR perc = DIVIDE([# candidates - contacted (contacted date)], [# events - LinkedIn visited (date created)],0)
			RETURN IF(perc>1, 1, perc)
```

### `Candidate - Time to Fill` _(table: Metrics)_

```dax
CALCULATE(AVERAGE(candidate_stage[Diff Hired - Job created]), candidate_stage[date_hired]<>BLANK())
```

### `Candidate - Time to Find a Hire` _(table: Metrics)_

```dax
CALCULATE(AVERAGE(candidate_stage[Diff Concated - Job created]), candidate_stage[date_hired]<>BLANK())
```

### `Candidate - Time to Hire` _(table: Metrics)_

```dax
CALCULATE(AVERAGE(candidate_stage[Diff Contacted - Hired]), candidate_stage[date_hired]<>BLANK())
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

## Calculated columns used

### `External Recruiter?` _(table: job)_

```dax
IF(job[is_external_recruiter]=TRUE(), "Yes", "No")
```

### `MON-YYYY` _(table: Calendar)_

```dax
CONCATENATE(LEFT('CALENDAR'[MonthName],3),
			            CONCATENATE(" ",'CALENDAR'[Year]))
```

### `Tech Role` _(table: job)_

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

## Plain columns used

- `LastRefreshedDate` (table: LastRefreshedDate)
- `MonthName` (table: Calendar)
- `Q number` (table: Calendar)
- `Year` (table: WBR TS Comment)
- `client_name` (table: client)
- `date_contacted` (table: candidate_stage)
- `date_created` (table: talent_email)
- `date_hired` (table: candidate_stage)
- `full_name` (table: talent)
- `job_category` (table: job)
- `job_subcategory` (table: job)
- `job_title` (table: job)
- `source` (table: candidate)
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

### `talent`

```m
let
				    Source = Snowflake.Databases("je92638.eu-central-1.snowflakecomputing.com","READER_WH_129",[Implementation="2.0"]),
				    KEBOOLA_855_942138244_Database = Source{[Name="KEBOOLA_855_942138244",Kind="Database"]}[Data],
				    READER_SCHEMA_855_942138244_Schema = KEBOOLA_855_942138244_Database{[Name="READER_SCHEMA_855_942138244",Kind="Schema"]}[Data],
				    v2_talent_Table = READER_SCHEMA_855_942138244_Schema{[Name="v2_talent",Kind="Table"]}[Data],
				    #"Filtered Rows" = Table.SelectRows(v2_talent_Table, each [date_created] >= #"Start Date")
				in
				    #"Filtered Rows"

	annotation PBI_ResultType = Exception

	annotation PBI_NavigationStepName = Navigation
```
