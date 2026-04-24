# Active Pipelines

**Bucket:** C or D (see `../PAGE_INVENTORY.md`)
**Visuals:** 14
**Tables referenced:** LastRefreshedDate, Metrics, client, job
**Measures used:** 3
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

### 1. _(untitled)_  _(slicer)_
- `job` → `job_recruiter`

### 2. _(untitled)_  _(slicer)_
- `job` → `job_sourcer`

### 3. _(untitled)_  _(textbox)_
- _no refs extracted_

### 4. _(untitled)_  _(?)_
- _no refs extracted_

### 5. _(untitled)_  _(slicer)_
- `job` → `External Recruiter?`

### 6. _(untitled)_  _(slicer)_
- `job` → `is_job_archived`, `External Recruiter?`

### 7. _(untitled)_  _(shape)_
- _no refs extracted_

### 8. _(untitled)_  _(card)_
- `LastRefreshedDate` → `LastRefreshedDate`

### 9. Pipeline Details  _(tableEx)_
- `job` → `job_id`, `job_title`, `job_recruiter`, `job_sourcer`, `date_created`, `External Recruiter?`, `date_first_hired`, `Job Country`, `is_job_archived`
- `client` → `client_name`
- `Metrics` → `% Viewed to Contacted`, `% Contacted to Reacted`, `% Reacted to Actual Screen`

### 10. _(untitled)_  _(slicer)_
- `client` → `client_name`
- `job` → `job_title`

### 11. _(untitled)_  _(image)_
- _no refs extracted_

### 12. _(untitled)_  _(slicer)_
- `job` → `job_title`, `date_created`

### 13. _(untitled)_  _(actionButton)_
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

## Calculated columns used

### `External Recruiter?` _(table: job)_

```dax
IF(job[is_external_recruiter]=TRUE(), "Yes", "No")
```

## Plain columns used

- `Job Country` (table: job)
- `LastRefreshedDate` (table: LastRefreshedDate)
- `client_name` (table: client)
- `date_created` (table: talent_email)
- `date_first_hired` (table: job)
- `is_job_archived` (table: job)
- `job_id` (table: job)
- `job_recruiter` (table: job)
- `job_sourcer` (table: job)
- `job_title` (table: job)

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
