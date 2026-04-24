# Viewed / Contacted Health

**Bucket:** C or D (see `../PAGE_INVENTORY.md`)
**Visuals:** 18
**Tables referenced:** Calendar, LastRefreshedDate, Metrics, candidate_stage, client, event, job
**Measures used:** 5
**Calculated columns used:** 4

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

### 2. Job Created Date  _(HierarchySlicer1458836712039)_
- `job` → `Job Creation Year`, `Job Creation Month`

### 3. Viewed to Contacted % Cumulative by Month/Week (Job Cat/Job/Open days)  _(pivotTable)_
- `Calendar` → `MonthName`, `WeekInt`, `Week start end`, `Before today`
- `job` → `job_category`, `job_title`, `date_created`, `Job Days Opened`
- `Metrics` → `Cumulative view to contact %`
- `candidate_stage` → `date_hired`

### 4. _(untitled)_  _(actionButton)_
- _no refs extracted_

### 5. _(untitled)_  _(shape)_
- _no refs extracted_

### 6. Viewed to Contacted % Cumulative by Week (Sourcer)  _(pivotTable)_
- `Calendar` → `MonthName`, `WeekInt`, `Week start end`, `Before today`
- `event` → `who_created_event`
- `Metrics` → `Cumulative view to contact %`
- `job` → `job_title`, `job_category`, `date_created`
- `candidate_stage` → `date_hired`

### 7. _(untitled)_  _(?)_
- _no refs extracted_

### 8. _(untitled)_  _(card)_
- `LastRefreshedDate` → `LastRefreshedDate`

### 9. _(untitled)_  _(slicer)_
- `client` → `client_name`
- `job` → `job_title`

### 10. Cumulative % by Week (Sourcer)  _(tableEx)_
- `event` → `who_created_event`
- `Metrics` → `% Viewed to Contacted`, `Cumulative viewed contacted trend line`
- `job` → `job_title`, `job_category`, `date_created`
- `Calendar` → `Week start end`, `WeekInt`, `Before today`
- `candidate_stage` → `date_hired`

### 11. Event Created Date  _(HierarchySlicer1458836712039)_
- `Calendar` → `Year`, `Q number`, `MonthName`

### 12. _(untitled)_  _(shape)_
- _no refs extracted_

### 13. Viewed to Contacted % by Week  (Not Cumulative)  _(pivotTable)_
- `Calendar` → `WeekInt`, `Week start end`, `Before today`
- `event` → `who_created_event`
- `Metrics` → `% Viewed to Contacted`
- `job` → `job_title`, `job_category`, `date_created`
- `candidate_stage` → `date_hired`

### 14. _(untitled)_  _(image)_
- _no refs extracted_

### 15. _(untitled)_  _(textbox)_
- _no refs extracted_

### 16. _(untitled)_  _(slicer)_
- `job` → `job_title`, `date_created`

### 17. _(untitled)_  _(slicer)_
- `job` → `is_job_archived`, `job_title`, `date_created`

### 18. # Viewed/Contacted by Week (Not Cumulative)  _(tableEx)_
- `Calendar` → `Week start end`, `WeekInt`
- `Metrics` → `# events - LinkedIn visited (date created)`, `# candidates - contacted (contacted date)`, `% Viewed to Contacted`
- `job` → `job_title`, `job_category`

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

### `% Viewed to Contacted` _(table: Metrics)_

```dax
VAR perc = DIVIDE([# candidates - contacted (contacted date)], [# events - LinkedIn visited (date created)],0)
			RETURN IF(perc>1, 1, perc)
```

### `Cumulative view to contact %` _(table: Metrics)_

```dax
VAR view = CALCULATE([# events - LinkedIn visited (date created)], FILTER(ALLSELECTED('Calendar'),'Calendar'[Date]<=MAX('Calendar'[Date])))
			VAR end_date = CALCULATE(MAX(candidate_stage[date_contacted]), ALL('Calendar'[Date]))
			VAR contact = CALCULATE([# candidates - contacted (contacted date)], FILTER(ALLSELECTED('Calendar'),'Calendar'[Date]<=MAX('Calendar'[Date])))
			RETURN
			IF(end_date < MIN('Calendar'[Date]) || contact=0 || view=0, BLANK(),
			    contact / view)
```

### `Cumulative viewed contacted trend line` _(table: Metrics)_

```dax
```

			// Static line color - use %23 instead of # for Firefox compatibility (Measure Derived from Eldersveld Modified by Kolosko)
			VAR LineColour = "%23118DFF"
			VAR PointColour = "White"
			VAR Defs = "<defs>
			    <linearGradient id='grad' x1='0' y1='25' x2='0' y2='50' gradientUnits='userSpaceOnUse'>
			        <stop stop-color='#118DFF' offset='0' />
			        <stop stop-color='#118DFF' offset='0.3' />
			        <stop stop-color='white' offset='1' />
			    </linearGradient>
			</defs>"
			// "Date" field used in this example along the X axis
			VAR XMinDate = MIN('Calendar'[WeekInt])
			VAR XMaxDate = MAX('Calendar'[WeekInt])

			// Obtain overall min and overall max measure values when evaluated for each date
			VAR YMinValue = MINX(Values('Calendar'[WeekInt]),CALCULATE([Cumulative view to contact %]))
			VAR YMaxValue = MAXX(Values('Calendar'[WeekInt]),CALCULATE([Cumulative view to contact %]))

			// Build table of X & Y coordinates and fit to 50 x 150 viewbox
			VAR SparklineTable = ADDCOLUMNS(
			    SUMMARIZE('Calendar','Calendar'[WeekInt]),
			        "X",INT(150 * DIVIDE('Calendar'[WeekInt] - XMinDate, XMaxDate - XMinDate)),
			        "Y",INT(50 * DIVIDE([Cumulative view to contact %] - YMinValue,YMaxValue - YMinValue)))

			// Concatenate X & Y coordinates to build the sparkline
			VAR Lines = CONCATENATEX(SparklineTable,[X] & "," & 50-[Y]," ", 'Calendar'[WeekInt])

			// Last data points on the line
			VAR LastSparkYValue = MAXX( FILTER(SparklineTable, 'Calendar'[WeekInt] = XMaxDate), [Y])
			VAR LastSparkXValue = MAXX( FILTER(SparklineTable, 'Calendar'[WeekInt] = XMaxDate), [X])

			// Add to SVG, and verify Data Category is set to Image URL for this measure
			VAR SVGImageURL =
			    "data:image/svg+xml;utf8," &
			    --- gradient---
			    "<svg xmlns='http://www.w3.org/2000/svg' x='0px' y='0px' viewBox='-7 -7 164 64'>" & Defs &
			     "<polyline fill='url(#grad)' fill-opacity='0.3' stroke='transparent'
			      stroke-width='0' points=' 0 50 " & Lines &
			      " 150 150 Z '/>" &
			    --- Lines---
			    "<polyline
			        fill='transparent' stroke='" & LineColour & "'
			        stroke-linecap='round' stroke-linejoin='round'
			        stroke-width='3' points=' " & Lines &
			      " '/>" &
			    --- Last Point---
			        "<circle cx='"& LastSparkXValue & "' cy='" & 50 - LastSparkYValue & "' r='4' stroke='" & LineColour & "' stroke-width='3' fill='" & PointColour & "' />" &
			        "</svg>"
			RETURN SVGImageURL

			```
		dataCategory: ImageUrl
```

## Calculated columns used

### `Before today` _(table: Calendar)_

```dax
IF(DATEDIFF('Calendar'[Date], TODAY(),DAY)>=0, TRUE(), FALSE())
```

### `Job Creation Month` _(table: job)_

```dax
FORMAT(job[date_created], "mmmm")
```

### `Job Creation Year` _(table: job)_

```dax
FORMAT(job[date_created], "YYYY")
```

### `Job Days Opened` _(table: job)_

```dax
DATEDIFF(job[date_created], TODAY(), DAY)
```

## Plain columns used

- `LastRefreshedDate` (table: LastRefreshedDate)
- `MonthName` (table: Calendar)
- `Q number` (table: Calendar)
- `Week start end` (table: WBR TS Actual)
- `WeekInt` (table: WBR TS Actual)
- `Year` (table: WBR TS Comment)
- `client_name` (table: client)
- `date_created` (table: talent_email)
- `date_hired` (table: candidate_stage)
- `is_job_archived` (table: job)
- `job_category` (table: job)
- `job_title` (table: job)
- `who_created_event` (table: event)

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
