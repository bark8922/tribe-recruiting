# Sourcing Stats

**Bucket:** C or D (see `../PAGE_INVENTORY.md`)
**Visuals:** 23
**Tables referenced:** Calendar, LastRefreshedDate, Metrics, Sourcing Stats, WBR Client History, client, event, job
**Measures used:** 9
**Calculated columns used:** 6

## What this page answers (fill in from screenshot)

> _TODO: Blake/Andy writes a 2-3 sentence plain-English description of what question this page answers and who uses it._

## Rebuild decision

- [ ] **Keep** (rebuild in new dashboard) — priority: _low / med / high_
- [ ] **Fold in** to existing section: _________
- [ ] **Archive only** (logic preserved here, no live dashboard)
- [ ] **Drop** (nobody uses it)

Notes: _________

## Visuals

### 1. _(untitled)_  _(shape)_
- _no refs extracted_

### 2. Linkedin Profile Viewed & Contacted by Day  _(lineChart)_
- `Calendar` → `Date`, `DayOfWeekName`
- `Metrics` → `# events - LinkedIn visited (date created)`, `# candidates - contacted (contacted date)`

### 3. _(untitled)_  _(shape)_
- _no refs extracted_

### 4. _(untitled)_  _(textbox)_
- _no refs extracted_

### 5. _(untitled)_  _(?)_
- _no refs extracted_

### 6. _(untitled)_  _(card)_
- `Sourcing Stats` → `Date`

### 7. Linkedin Profile Viewed & Contacted by Day  _(clusteredColumnChart)_
- `Calendar` → `DayOfWeekName`
- `Metrics` → `% Viewed to Contacted`, `# events - LinkedIn visited (date created)`, `# candidates - contacted (contacted date)`, `# events - contacted (date created)`
- `event` → `dayhour`

### 8. _(untitled)_  _(card)_
- `Calendar` → `Date`, `DayOfWeekName`, `Before today`, `is_last_6_weeks`

### 9. _(untitled)_  _(shape)_
- _no refs extracted_

### 10. _(untitled)_  _(actionButton)_
- _no refs extracted_

### 11. Date (Uncheck the above filter)  _(HierarchySlicer1458836712039)_
- `Calendar` → `Year`, `Q number`, `MonthName`

### 12. Linkedin Profile Viewed (7am-8pm only)  _(pivotTable)_
- `event` → `dayhour`
- `Calendar` → `Date`
- `Metrics` → `# events - LinkedIn visited (date created)`
- `job` → `job_title`

### 13. _(untitled)_  _(slicer)_
- `client` → `client_name`
- `job` → `job_title`

### 14. Last 6 Weeks?  _(ChicletSlicer1448559807354)_
- `Calendar` → `is_last_6_weeks`, `is_last_12_weeks`

### 15. _(untitled)_  _(shape)_
- _no refs extracted_

### 16. Linkedin Profile Viewed & Contacted by Hour  _(clusteredColumnChart)_
- `event` → `dayhour`
- `Metrics` → `% Viewed to Contacted`, `# events - LinkedIn visited (date created)`, `# candidates - contacted (contacted date)`, `# events - contacted (date created)`

### 17. _(untitled)_  _(card)_
- `LastRefreshedDate` → `LastRefreshedDate`

### 18. _(untitled)_  _(slicer)_
- `event` → `who_created_event_first`

### 19. _(untitled)_  _(shape)_
- _no refs extracted_

### 20. _(untitled)_  _(image)_
- _no refs extracted_

### 21. # Days with viewed (Last 6 weeks, w/o weekend). Can be only filtered by TS  _(hundredPercentStackedBarChart)_
- `Sourcing Stats` → `Less than 50`, `50-100`, `100- 200`, `More than 200`

### 22. Client''s Target Last 12 Weeks  _(pivotTable)_
- `WBR Client History` → `Client`, `Week start end`, `Contacted Target Reached %`, `Screens Target Reached %`
- `Metrics` → `% Viewed to Contacted`, `% Contacted to Reacted`, `% Reacted to Actual Screen`

### 23. Sourcer Time Distribution Across Clients (calculated from candidate sourcing volume)  _(pivotTable)_
- `client` → `client_name`
- `event` → `who_created_event_first`
- `Metrics` → `Finance sourcer allocation per client`, `% Viewed to Contacted`, `% Contacted to Reacted`, `% Reacted to Actual Screen`

## Measures used (with DAX)

### `# candidates - contacted (contacted date)` _(table: Metrics)_

```dax
var contact = CALCULATE(COUNT(candidate_stage[candidate_id]), USERELATIONSHIP('Calendar'[Date],candidate_stage[date_contacted]))
			RETURN CALCULATE(contact, candidate_stage[date_contacted]<>BLANK())
```

### `# events - LinkedIn visited (date created)` _(table: Metrics)_

```dax
CALCULATE(DISTINCTCOUNT(event[talent_id + job_id]),event[event_type]="Linkedin Visited Profile",
			USERELATIONSHIP(job[job_id], event[job_id]), USERELATIONSHIP('Calendar'[Date], event[date_created]))
```

### `# events - contacted (date created)` _(table: Metrics)_

```dax
CALCULATE(DISTINCTCOUNT(candidate[candidate_id]), event[event_type]="Moved to stage" && event[moved_to_stage]="Contacted")
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

### `Contacted Target Reached %` _(table: WBR Client History)_

```dax
```
			var con = CALCULATE(COUNT('WBR Client History'[Week start end]), 'WBR Client History'[Contacted Reached]="Reached")+0
			var wee = DISTINCTCOUNT('WBR Client History'[Week start end]) 
			RETURN
			con/wee
			```
```

### `Finance sourcer allocation per client` _(table: Metrics)_

```dax
DIVIDE([# candidates - contacted (contacted date)], CALCULATE([# candidates - contacted (contacted date)], REMOVEFILTERS(client[client_name])))
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

## Calculated columns used

### `100- 200` _(table: Sourcing Stats)_

```dax
IF('Sourcing Stats'[Viewed]<200 && 'Sourcing Stats'[Viewed]>=100, 1, BLANK())
```

### `50-100` _(table: Sourcing Stats)_

```dax
IF('Sourcing Stats'[Viewed]>=50 && 'Sourcing Stats'[Viewed]<100, 1, BLANK())
```

### `Before today` _(table: Calendar)_

```dax
IF(DATEDIFF('Calendar'[Date], TODAY(),DAY)>=0, TRUE(), FALSE())
```

### `Less than 50` _(table: Sourcing Stats)_

```dax
IF('Sourcing Stats'[Viewed]<50, 1, BLANK())
```

### `More than 200` _(table: Sourcing Stats)_

```dax
IF('Sourcing Stats'[Viewed]>=200, 1, BLANK())
```

### `is_last_6_weeks` _(table: Calendar)_

```dax
if('CALENDAR'[WeekEnding]>='CALENDAR'[WeekEnding -6w] && 'CALENDAR'[WeekEnding]<='CALENDAR'[WeekEnding Current], "Yes", "No")
```

## Plain columns used

- `Client` (table: WBR TA Target)
- `Date` (table: Sourcing Stats)
- `DayOfWeekName` (table: Calendar)
- `LastRefreshedDate` (table: LastRefreshedDate)
- `MonthName` (table: Calendar)
- `Q number` (table: Calendar)
- `Week start end` (table: WBR TS Actual)
- `Year` (table: WBR TS Comment)
- `client_name` (table: client)
- `dayhour` (table: event)
- `is_last_12_weeks` (table: WBR TS Actual)
- `job_title` (table: job)
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

### `Sourcing Stats`

```m
```
				CALCULATETABLE(SUMMARIZECOLUMNS(
				    event[who_created_event_first],
				    'Calendar'[Date],
				    "Viewed", [# events - LinkedIn visited (date created)]),
				    'Calendar'[is_last_6_weeks]="Yes",    
				    job[job_title]<>BLANK(), client[client_name]<>BLANK(), SEARCH("Test", client[client_name],1, BLANK())=BLANK(),
				    'Calendar'[DayOfWeekName]<>"Saturday", 'Calendar'[DayOfWeekName]<>"Sunday")
				```

	annotation PBI_Id = 0eff1784e57b4309928bb60d96ae555f
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
