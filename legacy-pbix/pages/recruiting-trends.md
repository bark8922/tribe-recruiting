# Recruiting Trends & Conversion Rate

**Bucket:** C or D (see `../PAGE_INVENTORY.md`)
**Visuals:** 30
**Tables referenced:** Calendar, LastRefreshedDate, Metrics, candidate, client, event, job
**Measures used:** 21
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

### 2. _(untitled)_  _(slicer)_
- `job` → `job_category`, `job_title`, `date_created`

### 3. Linkedin Profile Viewed and Contacted by Month/Week  _(lineClusteredColumnComboChart)_
- `Calendar` → `Year/Month`, `Week`
- `Metrics` → `# events - LinkedIn visited (date created)`, `# candidates - contacted (contacted date)`, `% Viewed to Contacted`

### 4. _(untitled)_  _(slicer)_
- `client` → `client_name`
- `job` → `job_title`

### 5. _(untitled)_  _(slicer)_
- `event` → `who_created_event_first`

### 6. _(untitled)_  _(card)_
- `LastRefreshedDate` → `LastRefreshedDate`

### 7. # for 1 Hires per Job Cat/Job Subcategory/Job Title (Based on "Contacted Date")  _(pivotTable)_
- `job` → `job_category`, `job_subcategory`, `job_title`, `is_external_recruiter`, `date_first_hired`
- `event` → `job_id`
- `Metrics` → `Conv rate Visited to Hires`, `Conv rate Contacted to Hires`, `Conv rate Screens to Hires`, `Conv rate Actual screens to Hires`, `Conv rate Moved to ATS to Hires`, `Conv rate Offers to Hires`

### 8. % Hired by Source and Client  _(pivotTable)_
- `candidate` → `source`
- `client` → `client_name`
- `Metrics` → `# candidates - hired (hired date)`
- `job` → `job_title`

### 9. _(untitled)_  _(actionButton)_
- _no refs extracted_

### 10. Actual Screens and ATS by Month/Week  _(lineClusteredColumnComboChart)_
- `Calendar` → `Year/Month`, `Week`
- `Metrics` → `# candidates - actual screen (actual screen date)`, `# candidates - move to ATS (moved date)`, `% Screens Actual to ATS`
- `job` → `is_external_recruiter`

### 11. _(untitled)_  _(card)_
- `Metrics` → `Seletced peroid`

### 12. Hired by Source and Job Category  _(pivotTable)_
- `candidate` → `source`
- `job` → `job_category`, `job_subcategory`, `job_title`
- `Metrics` → `# candidates - hired (hired date)`

### 13. Date (Uncheck the above filter)  _(HierarchySlicer1458836712039)_
- `Calendar` → `Year`, `Q number`, `MonthName`

### 14. Source (not work with Viewed)  _(slicer)_
- `candidate` → `source`
- `job` → `job_title`, `date_created`

### 15. Hired by Source and Client  _(pivotTable)_
- `candidate` → `source`
- `client` → `client_name`
- `Metrics` → `# candidates - hired (hired date)`
- `job` → `job_title`

### 16. Offers and Hires by Month/Week  _(clusteredColumnChart)_
- `Calendar` → `Year/Month`, `Week`
- `Metrics` → `# candidates - offer (offered date)`, `# candidates - hired (hired date)`
- `job` → `is_external_recruiter`

### 17. _(untitled)_  _(shape)_
- _no refs extracted_

### 18. % Hired by Source and Job Category  _(pivotTable)_
- `candidate` → `source`
- `job` → `job_category`, `job_subcategory`, `job_title`
- `Metrics` → `# candidates - hired (hired date)`

### 19. _(untitled)_  _(slicer)_
- `job` → `job_title`, `date_created`

### 20. _(untitled)_  _(textbox)_
- _no refs extracted_

### 21. Disqualified Reason (All time data, date filter cannot be applied))  _(pieChart)_
- `candidate` → `reason_not_interested`, `candidate_id`

### 22. _(untitled)_  _(image)_
- _no refs extracted_

### 23. _(untitled)_  _(shape)_
- _no refs extracted_

### 24. _(untitled)_  _(slicer)_
- `event` → `Linkedin Viewed by AI`

### 25. _(untitled)_  _(?)_
- _no refs extracted_

### 26. Last 12 Weeks?  _(ChicletSlicer1448559807354)_
- `Calendar` → `is_last_12_weeks`

### 27. _(untitled)_  _(slicer)_
- `candidate` → `reason_not_interested`
- `job` → `job_title`, `date_created`

### 28. Funnel based on "Contacted Date  _(funnel)_
- `Metrics` → `# events - LinkedIn visited (date created)`, `# candidates - contacted (contacted date)`, `# candidates - reacted (contacted date)`, `# candidates - screen (contacted date)`, `# candidates - actual screen (contacted date)`, `# candidates - moved to ATS (contacted date)`, `# candidates - offer (contacted date)`, `# candidates - hired (contacted date)`
- `job` → `is_external_recruiter`

### 29. _(untitled)_  _(actionButton)_
- _no refs extracted_

### 30. _(untitled)_  _(slicer)_
- `event` → `who_event_created_for`

## Measures used (with DAX)

### `# candidates - actual screen (actual screen date)` _(table: Metrics)_

```dax
var acutalscreened = CALCULATE(COUNT(candidate_stage[candidate_id]), USERELATIONSHIP('Calendar'[Date],candidate_stage[date_screen_actual]),event[event_type]="Evaluation")
			RETURN CALCULATE(acutalscreened, candidate_stage[date_screen_actual]<>BLANK())
```

### `# candidates - actual screen (contacted date)` _(table: Metrics)_

```dax
CALCULATE([# candidates - contacted (contacted date)], not isblank(candidate_stage[date_screen_actual]))
```

### `# candidates - contacted (contacted date)` _(table: Metrics)_

```dax
var contact = CALCULATE(COUNT(candidate_stage[candidate_id]), USERELATIONSHIP('Calendar'[Date],candidate_stage[date_contacted]))
			RETURN CALCULATE(contact, candidate_stage[date_contacted]<>BLANK())
```

### `# candidates - hired (contacted date)` _(table: Metrics)_

```dax
CALCULATE([# candidates - contacted (contacted date)], not isblank(candidate_stage[date_hired]))
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

### `# candidates - moved to ATS (contacted date)` _(table: Metrics)_

```dax
CALCULATE([# candidates - contacted (contacted date)], not isblank(candidate_stage[date_interview]))
```

### `# candidates - offer (contacted date)` _(table: Metrics)_

```dax
CALCULATE([# candidates - contacted (contacted date)], not isblank(candidate_stage[date_offer]))
```

### `# candidates - offer (offered date)` _(table: Metrics)_

```dax
var offer = CALCULATE(COUNT(candidate_stage[candidate_id]), USERELATIONSHIP('Calendar'[Date],candidate_stage[date_offer]), event[moved_to_stageType]="Offer")
			RETURN CALCULATE(offer, candidate_stage[date_offer]<>BLANK())
```

### `# candidates - reacted (contacted date)` _(table: Metrics)_

```dax
CALCULATE([# candidates - contacted (contacted date)],candidate[is_candidate_reacted]=TRUE())
```

### `# candidates - screen (contacted date)` _(table: Metrics)_

```dax
CALCULATE([# candidates - contacted (contacted date)], candidate_stage[date_screen]<>BLANK())
```

### `# events - LinkedIn visited (date created)` _(table: Metrics)_

```dax
CALCULATE(DISTINCTCOUNT(event[talent_id + job_id]),event[event_type]="Linkedin Visited Profile",
			USERELATIONSHIP(job[job_id], event[job_id]), USERELATIONSHIP('Calendar'[Date], event[date_created]))
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

### `Conv rate Actual screens to Hires` _(table: Metrics)_

```dax
IF([# candidates - hired (contacted date)]=BLANK(), BLANK(),
			   [# candidates - actual screen (contacted date)]/[# candidates - hired (contacted date)])
```

### `Conv rate Contacted to Hires` _(table: Metrics)_

```dax
IF([# candidates - hired (contacted date)]=BLANK(), BLANK(),
			    [# candidates - contacted (contacted date)]/[# candidates - hired (contacted date)])
```

### `Conv rate Moved to ATS to Hires` _(table: Metrics)_

```dax
IF([# candidates - hired (contacted date)]=BLANK(), BLANK(),
			    [# candidates - moved to ATS (contacted date)]/[# candidates - hired (contacted date)])
```

### `Conv rate Offers to Hires` _(table: Metrics)_

```dax
IF([# candidates - hired (contacted date)]=BLANK(), BLANK(),
			    [# candidates - offer (contacted date)]/[# candidates - hired (contacted date)])
```

### `Conv rate Screens to Hires` _(table: Metrics)_

```dax
IF([# candidates - hired (contacted date)]=BLANK(), BLANK(),
			    [# candidates - screen (contacted date)]/[# candidates - hired (contacted date)])
```

### `Conv rate Visited to Hires` _(table: Metrics)_

```dax
IF([# candidates - hired (contacted date)]=BLANK(), BLANK(),
			    [# events - LinkedIn visited (date created)]/[# candidates - hired (contacted date)])
```

### `Seletced peroid` _(table: Metrics)_

```dax
"You are selecting date from " & FORMAT(MIN('Calendar'[Date]),"YYYY-MM-DD") & " to " & FORMAT(MIN(MAX('Calendar'[Date]), TODAY()), "YYYY-MM-DD")
```

## Calculated columns used

### `Linkedin Viewed by AI` _(table: event)_

```dax
```
			SWITCH(TRUE(),
			event[event_type]="Linkedin Visited Profile" && event[is_event_createdby_ai]=TRUE(), "Yes", 
			event[event_type]="Linkedin Visited Profile" && event[is_event_createdby_ai]=BLANK(), "No",
			"No")
			```
```

### `Week` _(table: analytic_usage)_

```dax
WEEKNUM(analytic_usage[created_date], 21)
```

### `Year/Month` _(table: analytic_usage)_

```dax
FORMAT(analytic_usage[created_date], "YYYY/MM")
```

## Plain columns used

- `LastRefreshedDate` (table: LastRefreshedDate)
- `MonthName` (table: Calendar)
- `Q number` (table: Calendar)
- `Year` (table: WBR TS Comment)
- `candidate_id` (table: screen)
- `client_name` (table: client)
- `date_created` (table: talent_email)
- `date_first_hired` (table: job)
- `is_external_recruiter` (table: job)
- `is_last_12_weeks` (table: WBR TS Actual)
- `job_category` (table: job)
- `job_id` (table: job)
- `job_subcategory` (table: job)
- `job_title` (table: job)
- `reason_not_interested` (table: candidate)
- `source` (table: candidate)
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
