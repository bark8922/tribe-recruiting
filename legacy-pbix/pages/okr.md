# OKR

**Bucket:** C or D (see `../PAGE_INVENTORY.md`)
**Visuals:** 14
**Tables referenced:** LastRefreshedDate, Metrics, OKR TA, Org, WBR Client History, job
**Measures used:** 6
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

### 1. Avg Score by Contacted, Actual Screen, ATS  _(areaChart)_
- `OKR TA` → `WeekInt`, `Contacted OKR`, `ATS OKR`, `Actual Screen`
- `job` → `is_external_recruiter`

### 2. _(untitled)_  _(?)_
- _no refs extracted_

### 3. TA''s Score  _(pivotTable)_
- `OKR TA` → `TA`, `Average OKR`, `Contacted OKR`, `Actual Screen`, `ATS OKR`
- `Metrics` → `% Viewed to Contacted`, `% Contacted to Reacted`, `% Reacted to Actual Screen`

### 4. Client''s Target Last 12 Weeks  _(pivotTable)_
- `WBR Client History` → `Client`, `Week start end`, `Contacted Target Reached %`, `Screens Target Reached %`
- `Metrics` → `% Viewed to Contacted`, `% Contacted to Reacted`, `% Reacted to Actual Screen`

### 5. _(untitled)_  _(cardVisual)_
- `OKR TA` → `Average OKR`, `Contacted OKR`, `Actual Screen`, `ATS OKR`

### 6. _(untitled)_  _(actionButton)_
- _no refs extracted_

### 7. _(untitled)_  _(image)_
- _no refs extracted_

### 8. _(untitled)_  _(textbox)_
- _no refs extracted_

### 9. Avg Score  _(areaChart)_
- `OKR TA` → `WeekInt`, `Average OKR`
- `job` → `is_external_recruiter`

### 10. Date  _(HierarchySlicer1458836712039)_
- `OKR TA` → `Year Q`, `WeekInt`

### 11. _(untitled)_  _(slicer)_
- `Org` → `Manager`

### 12. _(untitled)_  _(card)_
- `LastRefreshedDate` → `LastRefreshedDate`

### 13. _(untitled)_  _(textbox)_
- _no refs extracted_

### 14. _(untitled)_  _(shape)_
- _no refs extracted_

## Measures used (with DAX)

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

### `Average OKR` _(table: OKR TA)_

```dax
DIVIDE(SUM('OKR TA'[Contacted OKR])+SUM('OKR TA'[Actual Screen])+SUM('OKR TA'[ATS OKR]), SUM('OKR TA'[Count]))
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

### `Manager` _(table: WBR TS Actual)_

```dax
RELATED('Historical Manager Structure WBR'[Report_To])
```

## Plain columns used

- `ATS OKR` (table: OKR TA)
- `Actual Screen` (table: OKR TA)
- `Client` (table: WBR TA Target)
- `Contacted OKR` (table: OKR TA)
- `LastRefreshedDate` (table: LastRefreshedDate)
- `TA` (table: WBR TS Actual)
- `Week start end` (table: WBR TS Actual)
- `WeekInt` (table: WBR TS Actual)
- `Year Q` (table: OKR TA)
- `is_external_recruiter` (table: job)

## Source definitions (Power Query M) for tables used

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

### `OKR TA`

```m
```
				CALCULATETABLE(SELECTCOLUMNS('WBR TA Actual',    
				    "TA", 'WBR TA Actual'[who_event_created_for],
				    "WeekInt", 'WBR TA Actual'[WeekInt],
				    "Week start end", 'WBR TA Actual'[Week start end],
				    "Year Q", 'WBR TA Actual'[Year]& 'WBR TA Actual'[Q],
				    "Contacted OKR", [Contacted OKR number],
				    "Actual Screen", [Screens OKR number],
				    "ATS OKR", [Moved to ATS OKR number]),
				    'WBR TA Actual'[Client]<>BLANK(), 'WBR TA Actual'[WeekInt]>=202341, 'WBR TA Actual'[WeekInt]<>202352,'WBR TA Actual'[WeekInt]<>202353, 'WBR TA Actual'[WeekInt]<>202401, 'WBR TA Actual'[WeekInt]<>202402, 'WBR TA Comment'[Exclude OKR]<>"Yes")
				```

	annotation PBI_Id = 252f6f158ce74fdaa3db47ee499e368a
```

### `Org`

```m
let
				    Source = Csv.Document(Web.Contents("https://api.bamboohr.com/api/gateway.php/tribexyz/v1/reports/127?format=csv"),[Delimiter=",", Columns=10, Encoding=65001, QuoteStyle=QuoteStyle.None]),
				    #"Changed Type" = Table.TransformColumnTypes(Source,{{"Column1", type text}, {"Column2", type text}, {"Column3", type text}, {"Column4", type text}, {"Column5", type text}}),
				    #"Promoted Headers" = Table.PromoteHeaders(#"Changed Type", [PromoteAllScalars=true]),
				    #"Split Column by Delimiter" = Table.SplitColumn(#"Promoted Headers", "Supervisor name", Splitter.SplitTextByDelimiter(", ", QuoteStyle.Csv), {"Supervisor name.1", "Supervisor name.2"}),
				    #"Added Custom" = Table.AddColumn(#"Split Column by Delimiter", "Name", each [First Name] & " " & [Last Name]),
				    #"Added Custom1" = Table.AddColumn(#"Added Custom", "Supervisor", each [Supervisor name.2] & " " & [Supervisor name.1]),
				    #"Renamed Columns" = Table.RenameColumns(#"Added Custom1",{{"Work Email", "Email"}}),
				    #"Filtered Rows1" = Table.SelectRows(#"Renamed Columns", each ([Employment Status] <> "Terminated")),
				    #"Added Conditional Column" = Table.AddColumn(#"Filtered Rows1", "Manager", each if [#"Employee #"] = "1" then "Martin Bernard" else [Supervisor]),
				    #"Removed Other Columns" = Table.SelectColumns(#"Added Conditional Column",{"Location", "Employee #", "Employment Status", "Name", "Manager"}),
				    #"Filtered Rows" = Table.SelectRows(#"Removed Other Columns", each [Manager] <> null and [Manager] <> "")
				in
				    #"Filtered Rows"

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
