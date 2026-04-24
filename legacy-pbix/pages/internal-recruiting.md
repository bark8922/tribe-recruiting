# Internal Recruiting

**Bucket:** C or D (see `../PAGE_INVENTORY.md`)
**Visuals:** 10
**Tables referenced:** Calendar, Calendar recruitee, Metrics, Recruitee, client, job, recruitee_admin, recruitee_candidate, recruitee_candidate_activity, recruitee_interview_result, recruitee_interview_template, recruitee_offer, recruitee_placement, recruitee_stage
**Measures used:** 1
**Calculated columns used:** 1

## What this page answers (fill in from screenshot)

> _TODO: Blake/Andy writes a 2-3 sentence plain-English description of what question this page answers and who uses it._

## Rebuild decision

- [ ] **Keep** (rebuild in new dashboard) — priority: _low / med / high_
- [ ] **Fold in** to existing section: _________
- [ ] **Archive only** (logic preserved here, no live dashboard)
- [ ] **Drop** (nobody uses it)

Notes: _________

## Visuals

### 1. _(untitled)_  _(image)_
- _no refs extracted_

### 2. _(untitled)_  _(shape)_
- _no refs extracted_

### 3. _(untitled)_  _(textbox)_
- _no refs extracted_

### 4. Interview Details  _(tableEx)_
- `recruitee_offer` → `title`
- `recruitee_candidate` → `name`
- `recruitee_interview_result` → `created_at_date`, `interview_template_name`, `rating`
- `recruitee_interview_template` → `interview_template_category`
- `recruitee_admin` → `Name`
- `job` → `job_title`

### 5. Candidates Interview by Category  _(pivotTable)_
- `Calendar recruitee` → `Week`, `Year`
- `recruitee_interview_template` → `interview_template_category`
- `recruitee_interview_result` → `interview_template_name`, `candidate_id`
- `job` → `job_title`
- `Calendar` → `Week start end`, `Week sort`

### 6. Funnel (Candidate created after 2024-04-01)  _(funnel)_
- `recruitee_interview_result` → `Recruiter Screen recruitee`, `Interview WTO`, `Final`
- `recruitee_candidate_activity` → `Offer`, `Hired`
- `Metrics` → `# candidates - contacted (contacted date)`
- `Recruitee` → `Call with Martin`, `Final Interview (TD)`, `Recruiter Screen`, `Work Sample Interview`
- `Calendar` → `Date`
- `client` → `client_name`

### 7. _(untitled)_  _(slicer)_
- `recruitee_offer` → `title`

### 8. # Interviews in the last week  _(tableEx)_
- `recruitee_offer` → `title`, `status`
- `recruitee_interview_result` → `Recruiter Screen recruitee`, `Interview WTO`, `Final`, `rating`, `candidate_id`
- `job` → `job_title`
- `recruitee_stage` → `stage_category`
- `recruitee_interview_template` → `interview_template_category`
- `recruitee_candidate` → `name`
- `recruitee_candidate_activity` → `created_at_date`
- `recruitee_placement` → `Stage name`
- `Calendar recruitee` → `Week`, `WeekNum`

### 9. Current Stage with Qualified Candidates (Active Jobs)  _(pivotTable)_
- `recruitee_stage` → `stage_category`
- `recruitee_offer` → `title`, `status`
- `recruitee_candidate` → `name`, `Disqualified`
- `job` → `job_title`
- `recruitee_interview_result` → `rating`
- `recruitee_candidate_activity` → `created_at_date`, `Latest record`
- `recruitee_placement` → `Stage category`, `Stage name`

### 10. _(untitled)_  _(slicer)_
- `recruitee_offer` → `status`, `title`

## Measures used (with DAX)

### `# candidates - contacted (contacted date)` _(table: Metrics)_

```dax
var contact = CALCULATE(COUNT(candidate_stage[candidate_id]), USERELATIONSHIP('Calendar'[Date],candidate_stage[date_contacted]))
			RETURN CALCULATE(contact, candidate_stage[date_contacted]<>BLANK())
```

## Calculated columns used

### `Week` _(table: analytic_usage)_

```dax
WEEKNUM(analytic_usage[created_date], 21)
```

## Plain columns used

- `Date` (table: Sourcing Stats)
- `Name` (table: Sourcing Team List)
- `Week sort` (table: WBR TS Actual)
- `Week start end` (table: WBR TS Actual)
- `Year` (table: WBR TS Comment)
- `candidate_id` (table: screen)
- `client_name` (table: client)
- `job_title` (table: job)
- `rating` (table: screen)

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
