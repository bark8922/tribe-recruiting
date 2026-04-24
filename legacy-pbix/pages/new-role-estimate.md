# New Role Estimate

**Bucket:** C or D (see `../PAGE_INVENTORY.md`)
**Visuals:** 21
**Tables referenced:** LastRefreshedDate, Metrics, candidate, client, job
**Measures used:** 12
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

### 1. To get 1 Hire  _(funnel)_
- `Metrics` → `Conv rate Visited to Hires`, `Conv rate Contacted to Hires`, `Conv rate Screens to Hires`, `Conv rate Actual screens to Hires`, `Conv rate Moved to ATS to Hires`, `Conv rate Offers to Hires`
- `job` → `is_external_recruiter`

### 2. _(untitled)_  _(slicer)_
- `job` → `Job Country`

### 3. _(untitled)_  _(?)_
- _no refs extracted_

### 4. _(untitled)_  _(slicer)_
- `client` → `client_name`

### 5. The result you are selecting is based on the following historical data  _(cardVisual)_
- `candidate` → `job_id`
- `job` → `Job Creation between`
- `Metrics` → `# candidates - hired (contacted date)`

### 6. Reason Candidates Declined  _(pieChart)_
- `candidate` → `reason_not_interested`, `candidate_id`

### 7. _(untitled)_  _(slicer)_
- `job` → `job_title`

### 8. _(untitled)_  _(textbox)_
- _no refs extracted_

### 9. _(untitled)_  _(slicer)_
- `job` → `job_subcategory`

### 10. _(untitled)_  _(slicer)_
- `job` → `job_category`

### 11. _(untitled)_  _(image)_
- _no refs extracted_

### 12. _(untitled)_  _(cardVisual)_
- `Metrics` → `Estimate conversation`

### 13. _(untitled)_  _(actionButton)_
- _no refs extracted_

### 14. _(untitled)_  _(textbox)_
- _no refs extracted_

### 15. _(untitled)_  _(card)_
- `LastRefreshedDate` → `LastRefreshedDate`

### 16. _(untitled)_  _(cardVisual)_
- `Metrics` → `Job - Time to Hire`, `% Response Rate`
- `candidate` → `hired_salary_eur`

### 17. _(untitled)_  _(shape)_
- _no refs extracted_

### 18. _(untitled)_  _(shape)_
- _no refs extracted_

### 19. # for 1 Hires per Country  _(pivotTable)_
- `job` → `Job Country`, `job_title`
- `candidate` → `job_id`
- `Metrics` → `Job - Time to Hire`, `Conv rate Visited to Hires`, `Conv rate Contacted to Hires`, `Conv rate Screens to Hires`, `Conv rate Actual screens to Hires`, `Conv rate Moved to ATS to Hires`, `Conv rate Offers to Hires`

### 20. _(untitled)_  _(cardVisual)_
- `Metrics` → `Estimate find a time`

### 21. Job Creation  _(slicer)_
- `job` → `Job Creation Year`

## Measures used (with DAX)

### `# candidates - hired (contacted date)` _(table: Metrics)_

```dax
CALCULATE([# candidates - contacted (contacted date)], not isblank(candidate_stage[date_hired]))
```

### `% Response Rate` _(table: Metrics)_

```dax
DIVIDE([Candidate Response], [# candidates - contacted (contacted date)])
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

### `Estimate conversation` _(table: Metrics)_

```dax
"You will need to have " & FORMAT([Conv rate Visited to Hires],"0") & " Linkedin profile viewed, " & FORMAT([Conv rate Contacted to Hires],"0") & " candidates contacted and " & FORMAT([Conv rate Actual screens to Hires], "0") & " actual screens to get 1 hire"
```

### `Estimate find a time` _(table: Metrics)_

```dax
"This role typically requires " & FORMAT([Job - Time to Hire],"0") & " days to find a hire. If you start to contact candidates today, you will have the 1st hire on " & FORMAT(TODAY()+[Job - Time to Hire], "YYYY-MM-DD")
```

### `Job - Time to Hire` _(table: Metrics)_

```dax
CALCULATE(AVERAGE(job[Diff Contacted - Hired]), job[Diff Contacted - Hired]>0)
```

### `Job Creation between` _(table: job)_

```dax
```
			"From " & FORMAT(MIN(job[date_created]), "YYYY-MM-DD") & " until " & FORMAT(MAX(job[date_created]), "YYYY-MM-DD") 
			```
```

## Calculated columns used

### `Job Creation Year` _(table: job)_

```dax
FORMAT(job[date_created], "YYYY")
```

## Plain columns used

- `Job Country` (table: job)
- `LastRefreshedDate` (table: LastRefreshedDate)
- `candidate_id` (table: screen)
- `client_name` (table: client)
- `hired_salary_eur` (table: candidate)
- `is_external_recruiter` (table: job)
- `job_category` (table: job)
- `job_id` (table: job)
- `job_subcategory` (table: job)
- `job_title` (table: job)
- `reason_not_interested` (table: candidate)

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
