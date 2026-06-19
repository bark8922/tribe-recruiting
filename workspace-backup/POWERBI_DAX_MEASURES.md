# Power BI DAX Extraction — KPI Dashboard (Leadership)

**Source:** `KPI Dashboard (Leadership).pbix` (Andy's leadership Power BI file)  
**Extracted:** 2026-04-08 (before Andy's last day)  
**Tool:** pbixray  
**Why this exists:** This is Andy's brain — the ~30% of business logic that lives in DAX on top of Frantisek's Keboola `reporting-v2` tables. Once Andy is gone, this doc is the only record of how the WBR/MBR/Project numbers are actually computed.

## Contents

- **32 tables** loaded into the model
- **25 Power Query (M) expressions** — how each table is sourced from Snowflake / files
- **200 DAX measures** — all metrics shown on dashboards
- **123 DAX calculated columns** — derived columns added on top of raw data
- **9 DAX calculated tables** — the WBR/MBR materialized views

---

## 1. Tables in the model

- `Calendar`
- `Current_TA`
- `Current_TS`
- `Historical Manager Structure WBR`
- `IR Comment`
- `LastRefreshedDate`
- `OKR TA`
- `Org`
- `Org_WBR`
- `Sourcing Stats`
- `Sourcing Team List`
- `Temp_Inactive_Jobs_Sourcers_WBR`
- `User`
- `WBR Client History`
- `WBR Job from Aug 2025`
- `WBR Job open 60d`
- `WBR TA Actual`
- `WBR TA Comment`
- `WBR TA Job`
- `WBR TA Target`
- `WBR TS Actual`
- `WBR TS Comment`
- `analytic_usage`
- `candidate`
- `candidate_stage`
- `client`
- `event`
- `job`
- `screen`
- `talent`
- `talent_email`
- `talent_employer`

---

## 2. Power Query (M) expressions — how tables are loaded

These are the source definitions. Most read from Snowflake schema `KEBOOLA_855_942138244.READER_SCHEMA_855_942138244` (Frantisek's reporting-v2 tables exposed via Keboola Snowflake reader).

### `candidate`

```m
let
    Source = Snowflake.Databases("je92638.eu-central-1.snowflakecomputing.com","READER_WH_129",[Implementation="2.0"]),
    KEBOOLA_855_942138244_Database = Source{[Name="KEBOOLA_855_942138244",Kind="Database"]}[Data],
    READER_SCHEMA_855_942138244_Schema = KEBOOLA_855_942138244_Database{[Name="READER_SCHEMA_855_942138244",Kind="Schema"]}[Data],
    v2_candidate_Table = READER_SCHEMA_855_942138244_Schema{[Name="v2_candidate",Kind="Table"]}[Data]
in
    v2_candidate_Table
```

### `candidate_stage`

```m
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

### `client`

```m
let
    Source = Snowflake.Databases("je92638.eu-central-1.snowflakecomputing.com","READER_WH_129",[Implementation="2.0"]),
    KEBOOLA_855_942138244_Database = Source{[Name="KEBOOLA_855_942138244",Kind="Database"]}[Data],
    READER_SCHEMA_855_942138244_Schema = KEBOOLA_855_942138244_Database{[Name="READER_SCHEMA_855_942138244",Kind="Schema"]}[Data],
    v2_client_Table = READER_SCHEMA_855_942138244_Schema{[Name="v2_client",Kind="Table"]}[Data]
in
    v2_client_Table
```

### `event`

```m
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
    #"Renamed Columns1" = Table.RenameColumns(#"Removed Columns",{{"Correct", "who_created_event_first"}}),
    #"Filtered Rows1" = Table.SelectRows(#"Renamed Columns1", each [date_created] >= #"Start Date")
in
    #"Filtered Rows1"
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
```

### `screen`

```m
let
    Source = Snowflake.Databases("je92638.eu-central-1.snowflakecomputing.com","READER_WH_129",[Implementation="2.0"]),
    KEBOOLA_855_942138244_Database = Source{[Name="KEBOOLA_855_942138244",Kind="Database"]}[Data],
    READER_SCHEMA_855_942138244_Schema = KEBOOLA_855_942138244_Database{[Name="READER_SCHEMA_855_942138244",Kind="Schema"]}[Data],
    v2_screen_Table = READER_SCHEMA_855_942138244_Schema{[Name="v2_screen",Kind="Table"]}[Data],
    #"Filtered Rows" = Table.SelectRows(v2_screen_Table, each [date_created] >= #"Start Date")
in
    #"Filtered Rows"
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
```

### `talent_email`

```m
let
    Source = Snowflake.Databases("je92638.eu-central-1.snowflakecomputing.com","READER_WH_129",[Implementation="2.0"]),
    KEBOOLA_855_942138244_Database = Source{[Name="KEBOOLA_855_942138244",Kind="Database"]}[Data],
    READER_SCHEMA_855_942138244_Schema = KEBOOLA_855_942138244_Database{[Name="READER_SCHEMA_855_942138244",Kind="Schema"]}[Data],
    v2_talent_email_Table = READER_SCHEMA_855_942138244_Schema{[Name="v2_talent_email",Kind="Table"]}[Data],
    #"Filtered Rows" = Table.SelectRows(v2_talent_email_Table, each [date_created] >= #"Start Date")
in
    #"Filtered Rows"
```

### `talent_employer`

```m
let
    Source = Snowflake.Databases("je92638.eu-central-1.snowflakecomputing.com","READER_WH_129",[Implementation="2.0"]),
    KEBOOLA_855_942138244_Database = Source{[Name="KEBOOLA_855_942138244",Kind="Database"]}[Data],
    READER_SCHEMA_855_942138244_Schema = KEBOOLA_855_942138244_Database{[Name="READER_SCHEMA_855_942138244",Kind="Schema"]}[Data],
    v2_talent_employer_Table = READER_SCHEMA_855_942138244_Schema{[Name="v2_talent_employer",Kind="Table"]}[Data],
    #"Removed Other Columns" = Table.SelectColumns(v2_talent_employer_Table,{"employer_id", "employer_name"})
in
    #"Removed Other Columns"
```

### `Calendar`

```m
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
    #"Fiscal Year" = Table.AddColumn(#"Change Type", "Fiscal Year", each if [MonthOfYear]<4 then [Year]-1 else [Year]),
    #"current eow" = Table.AddColumn(#"Fiscal Year", "WeekEnding Current", each Date.EndOfWeek(DateTime.Date(DateTime.LocalNow()),Day.Monday)),
    #"Changed Type" = Table.TransformColumnTypes(#"current eow",{{"WeekEnding Current", type date}}),
    #"Current Week -6w" = Table.AddColumn(#"Changed Type", "WeekEnding -6w", each Date.AddDays([WeekEnding Current],-42)),
    #"Current Week -4w" = Table.AddColumn(#"Current Week -6w", "WeekEnding -4w", each Date.AddDays([WeekEnding Current],-28)),
    #"Added Custom" = Table.AddColumn(#"Current Week -4w", "WeekEnding -12w", each Date.AddDays([WeekEnding Current],-84)),
    #"Changed Type1" = Table.TransformColumnTypes(#"Added Custom",{{"WeekEnding Current", type date}, {"WeekEnding -6w", type date}, {"WeekEnding -12w", type date}, {"WeekEnding -4w", type date}}),
    #"Added Custom2" = Table.AddColumn(#"Changed Type1", "MonthEnding Current", each Date.EndOfMonth(DateTime.Date(DateTime.LocalNow()))),
    #"Added Custom3" = Table.AddColumn(#"Added Custom2", "MonthEnding", each Date.EndOfMonth([Date])),
    #"Added Custom4" = Table.AddColumn(#"Added Custom3", "MonthEnding -6m", each Date.EndOfMonth(Date.AddMonths([MonthEnding Current],-6))),
    #"Reordered Columns" = Table.ReorderColumns(#"Added Custom4",{"Date", "Year", "QuarterOfYear", "MonthOfYear", "DayOfMonth", "DateInt", "MonthName", "MonthInt", "QuarterInCalendar", "DayInWeek", "DayOfWeekName", "WeekEnding", "WeekOfYear", "WeekInt", "Fiscal Year", "WeekEnding Current", "WeekEnding -6w", "MonthEnding Current", "MonthEnding -6m", "MonthEnding"}),
    #"Sorted Rows" = Table.Sort(#"Reordered Columns",{{"Date", Order.Descending}}),
    #"Changed Type2" = Table.TransformColumnTypes(#"Sorted Rows",{{"MonthEnding Current", type date}, {"MonthEnding -6m", type date}, {"MonthEnding", type date}}),
    #"Added Custom5" = Table.AddColumn(#"Changed Type2", "Q number", each "Q" & Number.ToText([QuarterOfYear])),
    #"Added Custom6" = Table.AddColumn(#"Added Custom5", "MonthNumber", each Date.Month([Date])),
    #"Changed Type3" = Table.TransformColumnTypes(#"Added Custom6",{{"MonthNumber", Int64.Type}})
in
    #"Changed Type3"
```

### `Metrics`

```m
let
    Source = Table.FromRows(Json.Document(Binary.Decompress(Binary.FromText("i45WMlCKjQUA", BinaryEncoding.Base64), Compression.Deflate)), let _t = ((type nullable text) meta [Serialized.Text = true]) in type table [Column1 = _t]),
    #"Changed Type" = Table.TransformColumnTypes(Source,{{"Column1", Int64.Type}}),
    #"Removed Columns" = Table.RemoveColumns(#"Changed Type",{"Column1"})
in
    #"Removed Columns"
```

### `Current_TS`

```m
let
    Source = GoogleSheets.Contents("https://docs.google.com/spreadsheets/d/1Hyb5M244bkh9ygxnssq_wppWxrT0KtGeVFEYvHVoouc/edit#gid=1802525365"),
    #"TS Weekly Note (not used)_Table" = Source{[name="TS Weekly Note",ItemKind="Table"]}[Data],
    #"Promoted Headers" = Table.PromoteHeaders(#"TS Weekly Note (not used)_Table", [PromoteAllScalars=true]),
    #"Filtered Rows2" = Table.SelectRows(#"Promoted Headers", each [Year] <> 2023 and [Year] <> 2024),
    #"Inserted Text Before Delimiter" = Table.AddColumn(#"Filtered Rows2", "Text Before Delimiter", each Text.BeforeDelimiter([#"Week"], " "), type text),
    #"Extracted Text After Delimiter" = Table.TransformColumns(#"Inserted Text Before Delimiter", {{"Text Before Delimiter", each Text.AfterDelimiter(_, "W"), type text}}),
    #"Changed Type" = Table.TransformColumnTypes(#"Extracted Text After Delimiter",{{"Text Before Delimiter", Int64.Type}}),
    #"Added Custom1" = Table.AddColumn(#"Changed Type", "YearWeek", each [Year]*100 + [Text Before Delimiter]),
    #"Added Custom" = Table.AddColumn(#"Added Custom1", "Max", each List.Max(#"Added Custom1"[YearWeek])),
    #"Added Conditional Column" = Table.AddColumn(#"Added Custom", "Keep", each if [YearWeek] = [Max] then 1 else 0),
    #"Filtered Rows1" = Table.SelectRows(#"Added Conditional Column", each [Keep] = 1),
    #"Filtered Rows" = Table.SelectRows(#"Filtered Rows1", each [TS] <> null and [TS] <> ""),
    #"Trimmed Text" = Table.TransformColumns(#"Filtered Rows",{{"TS", Text.Trim, type text}}),
    #"Removed Other Columns" = Table.SelectColumns(#"Trimmed Text",{"TS", "Week"})
in
    #"Removed Other Columns"
```

### `User`

```m
let
    Source = Snowflake.Databases("je92638.eu-central-1.snowflakecomputing.com","READER_WH_129",[Implementation="2.0"]),
    KEBOOLA_855_942138244_Database = Source{[Name="KEBOOLA_855_942138244",Kind="Database"]}[Data],
    READER_SCHEMA_855_942138244_Schema = KEBOOLA_855_942138244_Database{[Name="READER_SCHEMA_855_942138244",Kind="Schema"]}[Data],
    v2_user_Table = READER_SCHEMA_855_942138244_Schema{[Name="v2_user",Kind="Table"]}[Data],
    #"Filtered Rows" = Table.SelectRows(v2_user_Table, each ([employee_number] <> null)),
    #"Replaced Value" = Table.ReplaceValue(#"Filtered Rows","Jelena  Lacmanovic","Jelena Lacmanovic",Replacer.ReplaceText,{"user_name"}),
    #"Replaced Value1" = Table.ReplaceValue(#"Replaced Value","Simon  Siew","Simon Siew",Replacer.ReplaceText,{"user_name"})
in
    #"Replaced Value1"
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
```

### `Current_TA`

```m
let
    Source = GoogleSheets.Contents("https://docs.google.com/spreadsheets/d/1Hyb5M244bkh9ygxnssq_wppWxrT0KtGeVFEYvHVoouc/edit#gid=1802525365"),
    #"TA Target_Table" = Source{[name="TA Target",ItemKind="Table"]}[Data],
    #"Promoted Headers" = Table.PromoteHeaders(#"TA Target_Table", [PromoteAllScalars=true]),
    #"Changed Type" = Table.TransformColumnTypes(#"Promoted Headers",{{"Client", type text}, {"TA", type text}, {"Year", Int64.Type}, {"Month", Int64.Type}, {"Contacted", type number}, {"Actual Screens", type number}, {"Moved to ATS", type number}}),
    #"Filtered Rows1" = Table.SelectRows(#"Changed Type", each [Year] <> 2023 and [Year] <> 2024),
    #"Filtered Rows" = Table.SelectRows(#"Filtered Rows1", each ([TA] <> "")),
    #"Added Custom" = Table.AddColumn(#"Filtered Rows", "YearMonth", each [Year]*100 + [Month]),
    #"Added Custom1" = Table.AddColumn(#"Added Custom", "Max Month", each List.Max(#"Added Custom"[YearMonth])),
    #"Added Conditional Column" = Table.AddColumn(#"Added Custom1", "Keep", each if [YearMonth] = [Max Month] then 1 else 0),
    #"Filtered Rows2" = Table.SelectRows(#"Added Conditional Column", each ([Keep] = 1) and ([Actual Screens] <> null and [Actual Screens] <> 0)),
    #"Trimmed Text" = Table.TransformColumns(#"Filtered Rows2",{{"TA", Text.Trim, type text}}),
    #"Grouped Rows" = Table.Group(#"Trimmed Text", {"TA", "YearMonth"}, {{"Count", each Table.RowCount(_), Int64.Type}}),
    #"Removed Columns" = Table.RemoveColumns(#"Grouped Rows",{"Count"})
in
    #"Removed Columns"
```

### `WBR TA Target`

```m
let
    Source = GoogleSheets.Contents("https://docs.google.com/spreadsheets/d/1Hyb5M244bkh9ygxnssq_wppWxrT0KtGeVFEYvHVoouc/edit#gid=1802525365"),
    #"TA Target_Table" = Source{[name="TA Target",ItemKind="Table"]}[Data],
    #"Promoted Headers" = Table.PromoteHeaders(#"TA Target_Table", [PromoteAllScalars=true]),
    #"Removed Other Columns" = Table.SelectColumns(#"Promoted Headers",{"Client", "TA", "Year", "Month", "Contacted", "Actual Screens", "Moved to ATS", "Hires"}),
    #"Changed Type" = Table.TransformColumnTypes(#"Removed Other Columns",{{"Client", type text}, {"TA", type text}, {"Year", Int64.Type}, {"Month", Int64.Type}, {"Contacted", type number}, {"Actual Screens", type number}, {"Moved to ATS", type number}, {"Hires", type number}}),
    #"Filtered Rows" = Table.SelectRows(#"Changed Type", each ([TA] <> "")),
    #"Trimmed Text" = Table.TransformColumns(#"Filtered Rows",{{"Client", Text.Trim, type text}, {"TA", Text.Trim, type text}}),
    #"Replaced Value" = Table.ReplaceValue(#"Trimmed Text","DoorDash","Wolt HQ",Replacer.ReplaceText,{"Client"}),
    #"Replaced Value1" = Table.ReplaceValue(#"Replaced Value","SevenRooms","Wolt HQ",Replacer.ReplaceText,{"Client"})
in
    #"Replaced Value1"
```

### `WBR TA Comment`

```m
let
    Source = GoogleSheets.Contents("https://docs.google.com/spreadsheets/d/1Hyb5M244bkh9ygxnssq_wppWxrT0KtGeVFEYvHVoouc/edit#gid=1802525365"),
    #"TS Weekly Note (not used)_Table" = Source{[name="TA Weekly Note",ItemKind="Table"]}[Data],
    #"Promoted Headers" = Table.PromoteHeaders(#"TS Weekly Note (not used)_Table", [PromoteAllScalars=true]),
    #"Changed Type" = Table.TransformColumnTypes(#"Promoted Headers",{{"Client", type text}, {"TA", type text}, {"Year", Int64.Type}, {"Week", type text}, {"Comment", type text}}),
    #"Added Custom" = Table.AddColumn(#"Changed Type", "Concat", each [Week]&[TA]&[Client]),
    #"Trimmed Text" = Table.TransformColumns(#"Added Custom",{{"Client", Text.Trim, type text}, {"TA", Text.Trim, type text}, {"Week", Text.Trim, type text}}),
    #"Filtered Rows" = Table.SelectRows(#"Trimmed Text", each [Client] <> null and [Client] <> ""),
    #"Replaced Value" = Table.ReplaceValue(#"Filtered Rows","No",null,Replacer.ReplaceValue,{"Exclude OKR"}),
    #"Replaced Value1" = Table.ReplaceValue(#"Replaced Value","SevenRooms","Wolt HQ",Replacer.ReplaceText,{"Client"}),
    #"Replaced Value2" = Table.ReplaceValue(#"Replaced Value1","Doordash","Wolt HQ",Replacer.ReplaceText,{"Client"})
in
    #"Replaced Value2"
```

### `WBR TS Comment`

```m
let
    Source = GoogleSheets.Contents("https://docs.google.com/spreadsheets/d/1Hyb5M244bkh9ygxnssq_wppWxrT0KtGeVFEYvHVoouc/edit#gid=1802525365"),
    #"TS Weekly Note (not used)_Table" = Source{[name="TS Weekly Note",ItemKind="Table"]}[Data],
    #"Promoted Headers" = Table.PromoteHeaders(#"TS Weekly Note (not used)_Table", [PromoteAllScalars=true]),
    #"Changed Type" = Table.TransformColumnTypes(#"Promoted Headers",{{"TS", type text}, {"Year", Int64.Type}, {"Week", type text}, {"Comment", type text}, {"Contacted Target", Int64.Type}}),
    #"Added Custom" = Table.AddColumn(#"Changed Type", "Concat", each [Week]&[TS]),
    #"Filtered Rows" = Table.SelectRows(#"Added Custom", each [TS] <> null and [TS] <> ""),
    #"Trimmed Text" = Table.TransformColumns(#"Filtered Rows",{{"TS", Text.Trim, type text}})
in
    #"Trimmed Text"
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
```

### `analytic_usage`

```m
let
    Source = Snowflake.Databases("je92638.eu-central-1.snowflakecomputing.com","READER_WH_129",[Implementation="2.0"]),
    KEBOOLA_855_942138244_Database = Source{[Name="KEBOOLA_855_942138244",Kind="Database"]}[Data],
    READER_SCHEMA_855_942138244_Schema = KEBOOLA_855_942138244_Database{[Name="READER_SCHEMA_855_942138244",Kind="Schema"]}[Data],
    v2_analytic_Table = READER_SCHEMA_855_942138244_Schema{[Name="v2_analytic",Kind="Table"]}[Data]
in
    v2_analytic_Table
```

### `Org_WBR`

```m
let
    Source = Org,
    #"Appended Query" = Table.Combine({Source, Org_manager})
in
    #"Appended Query"
```

### `Temp_Inactive_Jobs_Sourcers_WBR`

```m
let
    Source = GoogleSheets.Contents("https://docs.google.com/spreadsheets/d/1WApKTTxsXuwMgK5UBuEyTqzCgiXajLIX54RN3FHsNcA/edit?gid=0#gid=0"),
    Sheet1_Table = Source{[name="Sheet1",ItemKind="Table"]}[Data],
    #"Promoted Headers" = Table.PromoteHeaders(Sheet1_Table, [PromoteAllScalars=true]),
    #"Added Conditional Column" = Table.AddColumn(#"Promoted Headers", "Filter", each if [Sourcer] = [Official Sourcer] then 1 else 0),
    #"Filtered Rows2" = Table.SelectRows(#"Added Conditional Column", each ([Filter] = 1)),
    #"Removed Columns" = Table.RemoveColumns(#"Filtered Rows2",{"Filter"}),
    #"Filtered Rows" = Table.SelectRows(#"Removed Columns", each ([No longer working with the job] = "No longer working")),
    #"Removed Duplicates" = Table.Distinct(#"Filtered Rows", {"job_id"}),
    #"Filtered Rows1" = Table.SelectRows(#"Removed Duplicates", each [job_id] <> null and [job_id] <> "")
in
    #"Filtered Rows1"
```

### `Historical Manager Structure WBR`

```m
let
    Source = GoogleSheets.Contents("https://docs.google.com/spreadsheets/d/1IQMe6ryE_L_BryONqeiCKfvlvc0hpyIPrkayl1gI4bI/edit?gid=0#gid=0"),
    Sheet1_Table = Source{[name="Sheet1",ItemKind="Table"]}[Data],
    #"Promoted Headers" = Table.PromoteHeaders(Sheet1_Table, [PromoteAllScalars=true]),
    #"Changed Type" = Table.TransformColumnTypes(#"Promoted Headers",{{"Employee Number", type text}, {"Tribster Name", type text}, {"Report_To", type text}, {"WeekInt", Int64.Type}, {"Manager Key",  type text}})
in
    #"Changed Type"
```

### `Sourcing Team List`

```m
let
    Source = GoogleSheets.Contents("https://docs.google.com/spreadsheets/d/1jhLTNYC4EoSq9Wd7kXWF13qCnLKObnS1Gf9ZfY46Jh8/edit?gid=1774878141#gid=1774878141"),
    #"Sheet 1_Table" = Source{[name="Sheet 1",ItemKind="Table"]}[Data],
    #"Promoted Headers" = Table.PromoteHeaders(#"Sheet 1_Table", [PromoteAllScalars=true]),
    #"Changed Type" = Table.TransformColumnTypes(#"Promoted Headers",{{"Name", type text}, {"Employee Number", type text}, {"Moved to TA Position Date", type date}}),
    #"Filtered Rows" = Table.SelectRows(#"Changed Type", each [Employee Number] <> null and [Employee Number] <> "")
in
    #"Filtered Rows"
```

### `IR Comment`

```m
let
    Source = GoogleSheets.Contents("https://docs.google.com/spreadsheets/d/1Hyb5M244bkh9ygxnssq_wppWxrT0KtGeVFEYvHVoouc/edit?gid=1898344035#gid=1898344035"),
    IR_Table = Source{[name="IR",ItemKind="Table"]}[Data],
    #"Promoted Headers" = Table.PromoteHeaders(IR_Table, [PromoteAllScalars=true]),
    #"Changed Type" = Table.TransformColumnTypes(#"Promoted Headers",{{"Role", type text}, {"Year", Int64.Type}, {"Week", type text}, {"Client", type text}, {"Headcount", Int64.Type}, {"Status", type text}, {"Outcome", type text}}),
    #"Filtered Rows" = Table.SelectRows(#"Changed Type", each [Job_ID] <> null and [Job_ID] <> ""),
    #"Filtered Rows1" = Table.SelectRows(#"Filtered Rows", each [Week] <> null and [Week] <> "")
in
    #"Filtered Rows1"
```

---

## 3. DAX calculated tables (WBR / MBR materialized views)

These are the most important objects in the file. Each one is the materialized view backing a leadership dashboard — they encode the join logic, filters, and metric set in one place.

### `WBR TA Actual`

```dax
CALCULATETABLE(SUMMARIZECOLUMNS(
    'Calendar'[Year], 
    'Calendar'[Week sort],
    'Calendar'[Week start end],
    'Calendar'[WeekInt],
    'Calendar'[is_last_12_weeks],
    client[client_wolt_group],
    client[client_id],
    event[who_event_created_for],
    "Sourcer", CONCATENATEX(
        FILTER(
            SUMMARIZE(event, event[who_event_created_for], event[who_created_event]),
            event[who_event_created_for]<>event[who_created_event] && event[who_created_event]<>BLANK()),
        event[who_created_event], ", ", event[who_created_event]
        ),
    "# Jobs", CALCULATE(DISTINCTCOUNT(event[job_id]), USERELATIONSHIP('Calendar'[Date], event[date_created])),
    "Early warning", SUM(job[Early warning]),
    "Time to Fill", [Candidate - Time to Fill (Hired date)],
    "Contacted", [# events - contacted (date created)],
    "Actual screens", [# candidates - actual screen (actual screen date)], 
    "Moved to ATS", [# candidates - move to ATS (moved date)],
    "Offers", [# candidates - offer (offered date)],
    "Hires", [# candidates - hired (hired date)]),
    job[job_title]<>BLANK(), candidate[is_candidate_archived]=FALSE(), client[client_name]<>BLANK(), NOT(client[client_name] IN {"Tribe.xyz", "Kamila AI - TEST"}), job[test]<>TRUE(), event[who_event_created_for]<>BLANK(), 'Calendar'[Year]>=2024)
```

### `WBR Client History`

```dax
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
```

### `WBR TS Actual`

```dax
CALCULATETABLE(SUMMARIZECOLUMNS(
    'Calendar'[Year],    
    'Calendar'[Week sort],
    'Calendar'[Week start end],
    'Calendar'[WeekInt],
    'Calendar'[is_last_12_weeks],
    'Calendar'[is_last_4_weeks],
    event[who_created_event_first],
    "TA",
        CONCATENATEX(
        FILTER(
            VALUES(event[who_event_created_for]),
            event[who_event_created_for] <> SELECTEDVALUE(event[who_created_event_first]) &&
            NOT ISBLANK(event[who_event_created_for])
        ),
        event[who_event_created_for],
        ", ",
        event[who_event_created_for],
        ASC
    ),
    "# Jobs", DISTINCTCOUNT(event[job_id]),
    "Viewed", [# events - LinkedIn visited (date created)],
    --"AI Viewed", [Kamila Talent]+0,
    --"Non-AI Viewed", CALCULATE([# events - LinkedIn visited (date created)], event[is_event_createdby_ai]=FALSE())+0,
    "Sourced", [# events - sourced (sourced date)],
    "Contacted", [# candidates - contacted (contacted date)],
    "Positive Response", [# candidates - positive response],
    "Screens", [# candidates - screen (screened date)],
    "Actual Screens", [# candidates - actual screen (actual screen date)],
    "ATS", [# candidates - move to ATS (moved date)],
    "Hires", [# candidates - hired (hired date)]),
    job[job_title]<>BLANK(), event[who_created_event_first]<>BLANK(), candidate[is_candidate_archived]=FALSE(), job[test]<>TRUE(), 'Calendar'[Year]>=2024)
```

### `WBR Job open 60d`

```dax
CALCULATETABLE(SUMMARIZECOLUMNS(
    client[client_name],
    job[job_recruiter],
    "# Jobs 60d", DISTINCTCOUNT(job[job_id])
    ),
    client[client_name]<>"Tribe.xyz", client[client_name]<>BLANK(), job[is_job_archived]<>TRUE(), job[date_first_hired]=BLANK(), job[Job Days Opened]>=60, candidate[is_candidate_archived]=FALSE())
```

### `OKR TA`

```dax
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

### `Calendar WBR`

```dax
CALCULATETABLE(SUMMARIZECOLUMNS('Calendar'[Week start end], 'Calendar'[WeekInt], 'Calendar'[is_current_week], 'Calendar'[is_last_12_weeks], 'Calendar'[is_last_4_weeks]), 'Calendar'[Week start end]<>BLANK(), 'Calendar'[Date]<=TODAY())
```

### `Sourcing Stats`

```dax
CALCULATETABLE(SUMMARIZECOLUMNS(
    event[who_created_event_first],
    'Calendar'[Date],
    "Viewed", [# events - LinkedIn visited (date created)]),
    'Calendar'[is_last_6_weeks]="Yes",    
    job[job_title]<>BLANK(), client[client_name]<>BLANK(), SEARCH("Test", client[client_name],1, BLANK())=BLANK(),
    'Calendar'[DayOfWeekName]<>"Saturday", 'Calendar'[DayOfWeekName]<>"Sunday")
```

### `WBR Job from Aug 2025`

```dax
CALCULATETABLE(SUMMARIZECOLUMNS(
    client[client_name],
    event[who_event_created_for],
    "# Jobs", DISTINCTCOUNT(job[job_id])
    ),
    client[client_name]<>"Tribe.xyz", client[client_name]<>BLANK(), event[date_created]>=DATE(2025,7,28), candidate[is_candidate_archived]=FALSE())
```

### `WBR TA Job`

```dax
CALCULATETABLE(SUMMARIZECOLUMNS(
    'Calendar'[Year], 
    'Calendar'[Week sort],
    'Calendar'[Week start end],
    'Calendar'[WeekInt],
    'Calendar'[is_last_12_weeks],
    client[client_name],
    client[client_id],
    event[who_event_created_for],
    job[job_title],
    "# Jobs", CALCULATE(DISTINCTCOUNT(event[job_id]), USERELATIONSHIP('Calendar'[Date], event[date_created])),
    "Time to Fill", [Candidate - Time to Fill (Hired date)],
    "Contacted", [# events - contacted (date created)],
    "Screens", [# candidates - screen (screened date)],
    "Actual screens", [# candidates - actual screen (actual screen date)], 
    "Moved to ATS", [# candidates - move to ATS (moved date)],
    "Offers", [# candidates - offer (offered date)],
    "Hires", [# candidates - hired (hired date)]),
    job[job_title]<>BLANK(), candidate[is_candidate_archived]=FALSE(), client[client_name]<>BLANK(), NOT(client[client_name] IN {"Tribe.xyz", "Kamila AI - TEST"}), event[who_event_created_for]<>BLANK(), 'Calendar'[Year]>=2023)
```

---

## 4. DAX measures (all 200)

Grouped by source table.

### Table: `LastRefreshedDate` (1 measures)

**`Last refreshed date`**

```dax
"Last Updated: " & FORMAT(MAX(LastRefreshedDate[LastRefreshedDate]), "YYYY-MM-DD HH:MM") & " (CET)"
```

### Table: `Metrics` (124 measures)

**`# candidates (contacted date) NOT USED`**

```dax
CALCULATE(COUNT(candidate_stage[candidate_id]), USERELATIONSHIP('Calendar'[Date],candidate_stage[date_contacted]))
```

**`# candidates - screen (contacted date)`**

```dax
CALCULATE([# candidates - contacted (contacted date)], candidate_stage[date_screen]<>BLANK())
```

**`# candidates - actual screen (contacted date)`**

```dax
CALCULATE([# candidates - contacted (contacted date)], not isblank(candidate_stage[date_screen_actual]))
```

**`# candidates - moved to ATS (contacted date)`**

```dax
CALCULATE([# candidates - contacted (contacted date)], not isblank(candidate_stage[date_interview]))
```

**`# candidates - offer (contacted date)`**

```dax
CALCULATE([# candidates - contacted (contacted date)], not isblank(candidate_stage[date_offer]))
```

**`# candidates - hired (contacted date)`**

```dax
CALCULATE([# candidates - contacted (contacted date)], not isblank(candidate_stage[date_hired]))
```

**`# events (date created)`**

```dax
CALCULATE(COUNT(event[event_id]), USERELATIONSHIP('Calendar'[Date], event[date_created]))
```

**`# events - LinkedIn visited (date created)`**

```dax
CALCULATE(DISTINCTCOUNT(event[talent_id + job_id]),event[event_type]="Linkedin Visited Profile",
USERELATIONSHIP(job[job_id], event[job_id]), USERELATIONSHIP('Calendar'[Date], event[date_created]))
```

**`# events - unique candidates (date created)`**

```dax
CALCULATE(DISTINCTCOUNT(event[candidate_id]), USERELATIONSHIP('Calendar'[Date], event[date_created]))
```

**`% Viewed to Contacted`**

```dax
VAR perc = DIVIDE([# candidates - contacted (contacted date)], [# events - LinkedIn visited (date created)],0)
RETURN IF(perc>1, 1, perc)
```

**`# events - unique candidates - offer (date created)`**

```dax
CALCULATE([# events - unique candidates (date created)],event[moved_to_stageType]="Offer", USERELATIONSHIP(job[job_id], event[job_id]))
```

**`# events - unique candidates - hired (date created)`**

```dax
CALCULATE([# events - unique candidates (date created)],event[moved_to_stage]="Hired", USERELATIONSHIP(job[job_id], event[job_id]))
```

**`# events - unique candidates - Moved to ATS (date created)`**

```dax
CALCULATE([# events - unique candidates (date created)],event[moved_to_stage]="Moved to ATS", USERELATIONSHIP(job[job_id], event[job_id]))
```

**`# events - contacted (date created)`**

```dax
CALCULATE(DISTINCTCOUNT(candidate[candidate_id]), event[event_type]="Moved to stage" && event[moved_to_stage]="Contacted")
```

**`# events - unique candidates - phone screens (date created)`**

```dax


CALCULATE([# events - unique candidates (date created)],
    event[event_type]="Evaluation" || AND(left(event[event_id],9)="recruitee", event[moved_to_stageType]="evaluation"))
```

**`% LinkedIn visited / Contacted (unique)`**

```dax
DIVIDE([# events - LinkedIn visited (date created)],[# events - contacted (date created)],0)
```

**`# events - automation_linkedin_inmail_sent`**

```dax
CALCULATE([# events - unique talents (date created)], event[event_type] = "Linkedin inMail sent")
```

**`# events - automation_linkedin_inmail_received`**

```dax
CALCULATE([# events - unique talents (date created)], event[event_type] = "Linkedin inMail received")
```

**`# events - LinkeIn inMail  Rate`**

```dax
DIVIDE([# events - automation_linkedin_inmail_received],[# events - automation_linkedin_inmail_sent],0)
```

**`# events - unique talents (date created)`**

```dax
CALCULATE(DISTINCTCOUNT(event[talent_id]),USERELATIONSHIP('Calendar'[Date],event[date_created]))
```

**`# candidates - move to ATS (moved date)`**

```dax
var moveto = CALCULATE(COUNT(candidate_stage[candidate_id]), USERELATIONSHIP('Calendar'[Date],candidate_stage[date_interview]), event[moved_to_stage]="Moved to ATS")
RETURN CALCULATE(moveto, candidate_stage[date_interview]<>BLANK())
```

**`# candidates (date created)`**

```dax
CALCULATE(COUNT(candidate_stage[candidate_id]), USERELATIONSHIP('Calendar'[Date],candidate_stage[date_created]))
```

**`# candidates - actual screen (actual screen date)`**

```dax
var acutalscreened = CALCULATE(COUNT(candidate_stage[candidate_id]), USERELATIONSHIP('Calendar'[Date],candidate_stage[date_screen_actual]),event[event_type]="Evaluation")
RETURN CALCULATE(acutalscreened, candidate_stage[date_screen_actual]<>BLANK())
```

**`% Move to client / Screen actual (date created)`**

```dax
DIVIDE(Metrics[# candidates - move to ATS (moved date)],Metrics[# candidates - actual screen (actual screen date)],0)
```

**`# candidates - offer (offered date)`**

```dax
var offer = CALCULATE(COUNT(candidate_stage[candidate_id]), USERELATIONSHIP('Calendar'[Date],candidate_stage[date_offer]), event[moved_to_stageType]="Offer")
RETURN CALCULATE(offer, candidate_stage[date_offer]<>BLANK())
```

**`# candidates - hired (hired date)`**

```dax
var hire = CALCULATE(COUNT(candidate_stage[candidate_id]), USERELATIONSHIP('Calendar'[Date],candidate_stage[date_hired]),event[moved_to_stage]="Hired")
RETURN CALCULATE(hire, candidate_stage[date_hired]<>BLANK())
```

**`% Offer / Screen actual`**

```dax
DIVIDE([# candidates - offer (offered date)],Metrics[# candidates - actual screen (actual screen date)],0)
```

**`% Offer to Hire`**

```dax
var perc = DIVIDE([# candidates - hired (hired date)],[# candidates - offer (offered date)],0)
RETURN IF(perc>1, 1, perc)
```

**`# candidates - contacted (contacted date)`**

```dax
var contact = CALCULATE(COUNT(candidate_stage[candidate_id]), USERELATIONSHIP('Calendar'[Date],candidate_stage[date_contacted]))
RETURN CALCULATE(contact, candidate_stage[date_contacted]<>BLANK())
```

**`% Contacted to Reacted`**

```dax
var perc = DIVIDE(Metrics[# candidates - reacted (contacted date)],Metrics[# candidates - contacted (contacted date)],0)
RETURN IF(perc>1, 1, perc)
```

**`% Reacted to Actual Screen`**

```dax
var perc = DIVIDE(Metrics[# candidates - actual screen (actual screen date)],Metrics[# candidates - reacted (contacted date)],0)
RETURN IF(perc>1, 1, perc)
```

**`% Moved to ATS / phone screens (date created)`**

```dax
DIVIDE(Metrics[# events - unique candidates - Moved to ATS (date created)],[# events - unique candidates - phone screens (date created)],0)
```

**`# candidates - screen (screened date)`**

```dax
var screened = CALCULATE(COUNT(candidate_stage[candidate_id]), USERELATIONSHIP('Calendar'[Date],candidate_stage[date_screen]), event[moved_to_stage]="Recruiter Screen")
RETURN CALCULATE(screened, candidate_stage[date_screen]<>BLANK())
```

**`% Screen to ATS`**

```dax
var perc = DIVIDE([# candidates - move to ATS (moved date)],[# candidates - screen (contacted date)],0)
RETURN IF(perc>1, 1, perc)
```

**`Candidate - Avg time to hire (date contacted)`**

```dax
CALCULATE(AVERAGE(candidate_stage[Diff Contacted - Hired]),USERELATIONSHIP('Calendar'[Date],candidate_stage[date_contacted]))
```

**`Job - Time to Find a Hire`**

```dax
CALCULATE(AVERAGE(job[Diff Contacted - Job created]), candidate_stage[Diff Concated - Job created]>0)
```

**`Job - Time to Fill`**

```dax
CALCULATE(AVERAGE(job[Diff Hired - Job created]), candidate_stage[Diff Hired - Job created]>0)
```

**`# events - automation_step_email_open`**

```dax
CALCULATE(Metrics[# events -  automation_step_sent_email],event[automation_is_message_read]=TRUE())
```

**`# events - automation_step_replied`**

```dax
CALCULATE(Metrics[# events (date created)],event[automation_is_message_replied]=TRUE())
```

**`% events - automation_step_email_open_rate`**

```dax
DIVIDE(Metrics[# events - automation_step_email_open], Metrics[# events -  automation_step_sent_email],0)
```

**`% events - automation_step_reply_rate`**

```dax
DIVIDE(Metrics[# events - automation_step_replied], Metrics[# events (date created)],0)
```

**`# events - automation_step_connection_accepted`**

```dax
CALCULATE(Metrics[# events -  automation_step_sent_connection],event[automation_is_message_read]=TRUE())
```

**`% events - automation_step_accepted_rate`**

```dax
DIVIDE(Metrics[# events - automation_step_connection_accepted], [# events -  automation_step_sent_connection],0)
```

**`# events -  automation_step_sent_connection`**

```dax
CALCULATE(Metrics[# events (date created)], event[event_type]="Linkedin Sent Contact") 
```

**`# events -  automation_step_sent_email`**

```dax
CALCULATE(Metrics[# events (date created)], event[event_type] ="Email Sent")
```

**`# events - automation_step_email_replied`**

```dax
CALCULATE(Metrics[# events -  automation_step_sent_email],event[automation_is_message_replied]=TRUE())
```

**`% events - automation_step_email_reply_rate`**

```dax
DIVIDE(Metrics[# events - automation_step_email_replied], Metrics[# events -  automation_step_sent_email],0)
```

**`# candidates - reacted (contacted date)`**

```dax
CALCULATE([# candidates - contacted (contacted date)],candidate[is_candidate_reacted]=TRUE())
```

**`Candidate - Avg time to hire (date hired)`**

```dax
CALCULATE(AVERAGE(candidate_stage[Diff Contacted - Hired]),USERELATIONSHIP('Calendar'[Date],candidate_stage[date_hired]))
```

**`Candidate - Time to Find a Hire`**

```dax
CALCULATE(AVERAGE(candidate_stage[Diff Concated - Job created]), candidate_stage[date_hired]<>BLANK())
```

**`Candidate - Time to Fill`**

```dax
CALCULATE(AVERAGE(candidate_stage[Diff Hired - Job created]), candidate_stage[date_hired]<>BLANK())
```

**`# talents (date created)`**

```dax
CALCULATE(COUNT(talent[talent_id]),USERELATIONSHIP('Calendar'[Date], talent[date_created]))
```

**`revenue €`**

```dax
CALCULATE(sum(client_cost[cost]), USERELATIONSHIP('Calendar'[Date], client_cost[month]))
```

**`revenue - hired count`**

```dax
CALCULATE(sum(client_cost[hired_count]), USERELATIONSHIP('Calendar'[Date], client_cost[month]))
```

**`revenue - salary €`**

```dax
CALCULATE(sum(client_cost[hired_salary_sum]), USERELATIONSHIP('Calendar'[Date], client_cost[month]))
```

**`revenue - cost_to_salary`**

```dax
DIVIDE([revenue €], [revenue - salary €],BLANK())
```

**`revenue - cost_per_hire`**

```dax
DIVIDE([revenue €],[revenue - hired count],BLANK())
```

**`# Tech Roles Hired`**

```dax
CALCULATE(
    [# events - unique candidates - hired (date created)], 
            job[job_category]="Data Analytics" ||
            job[job_category]="DevOps" ||
            job[job_category]="Software Engineering" ||
            job[job_category]="Software"||
            job[job_category]="Design")+0
```

**`% Vistied to Screen`**

```dax
DIVIDE([# candidates - actual screen (actual screen date)], Metrics[# events - LinkedIn visited (date created)],0)
```

**`% Screens Actual to ATS`**

```dax
var perc = DIVIDE([# candidates - move to ATS (moved date)],[# candidates - actual screen (actual screen date)])
RETURN IF(perc>1, 1, perc)
```

**`TA linkedin candidate screening time`**

```dax
var linkedin = [# events - LinkedIn visited (date created)]/60
var screentime = [# candidates - actual screen (actual screen date)]/2
RETURN IF(linkedin + screentime=0, BLANK(), linkedin + screentime)
```

**`Conv rate Offers to Hires`**

```dax
IF([# candidates - hired (contacted date)]=BLANK(), BLANK(),
    [# candidates - offer (contacted date)]/[# candidates - hired (contacted date)])
```

**`Conv rate Moved to ATS to Hires`**

```dax
IF([# candidates - hired (contacted date)]=BLANK(), BLANK(),
    [# candidates - moved to ATS (contacted date)]/[# candidates - hired (contacted date)])
```

**`Conv rate Actual screens to Hires`**

```dax
IF([# candidates - hired (contacted date)]=BLANK(), BLANK(),
   [# candidates - actual screen (contacted date)]/[# candidates - hired (contacted date)])
```

**`Conv rate Screens to Hires`**

```dax
IF([# candidates - hired (contacted date)]=BLANK(), BLANK(),
    [# candidates - screen (contacted date)]/[# candidates - hired (contacted date)])
```

**`Conv rate Contacted to Hires`**

```dax
IF([# candidates - hired (contacted date)]=BLANK(), BLANK(),
    [# candidates - contacted (contacted date)]/[# candidates - hired (contacted date)])
```

**`Conv rate Visited to Hires`**

```dax
IF([# candidates - hired (contacted date)]=BLANK(), BLANK(),
    [# events - LinkedIn visited (date created)]/[# candidates - hired (contacted date)])
```

**`Separator`**

```dax
blank()
```

**`# Jobs Sequence`**

```dax
CALCULATE(DISTINCTCOUNT(event[job_id]), event[automation_flow_name]<>BLANK())
```

**`% Job Sequence`**

```dax
[# Jobs Sequence]/DISTINCTCOUNT(event[job_id])
```

**`# Job Auto-Respond`**

```dax
CALCULATE(DISTINCTCOUNT(event[job_id]), event[event_type] in {"Linkedin Sent Contact", "Message sent", "Linkedin inMail sent"})
```

**`% Job Auto-Respond`**

```dax
[# Job Auto-Respond]/DISTINCTCOUNT(event[job_id])
```

**`# Users Auto-Read`**

```dax
CALCULATE(DISTINCTCOUNT(event[who_created_event]), event[event_type]="Linkedin Responded")
```

**`% User Auro-Read`**

```dax
[# Users Auto-Read]/DISTINCTCOUNT(event[who_created_event])
```

**`Candidate Sequence reach out`**

```dax
CALCULATE(DISTINCTCOUNT(event[candidate_id]), event[event_type]="Candidate created" && event[automation_flow_name]<>BLANK())
```

**`Read Message + categorization (candidate)`**

```dax
CALCULATE(DISTINCTCOUNT(event[candidate_id]), event[event_type] in {"Linkedin Responded", "Message Categorized", "Message Read"})
```

**`Seletced peroid`**

```dax
"You are selecting date from " & FORMAT(MIN('Calendar'[Date]),"YYYY-MM-DD") & " to " & FORMAT(MIN(MAX('Calendar'[Date]), TODAY()), "YYYY-MM-DD")
```

**`Read Message + categorization (event)`**

```dax
CALCULATE(COUNT(event[event_id]), event[is_event_duplicated]=FALSE(), event[event_type] in {"Linkedin Responded", "Message Categorized", "Message Read"})
```

**`Reach Out %`**

```dax
[Candidate Sequence reach out]/[# events - contacted (date created)]
```

**`Break line`**

```dax
IF(SUM('WBR TA Actual'[Contacted])<>BLANK(), "  ", BLANK())
```

**`% ATS to Offers`**

```dax
var perc = DIVIDE([# candidates - offer (offered date)],[# candidates - move to ATS (moved date)],0)
RETURN IF(perc>1, 1, perc)
```

**`Cumulative view to contact %`**

```dax
VAR view = CALCULATE([# events - LinkedIn visited (date created)], FILTER(ALLSELECTED('Calendar'),'Calendar'[Date]<=MAX('Calendar'[Date])))
VAR end_date = CALCULATE(MAX(candidate_stage[date_contacted]), ALL('Calendar'[Date]))
VAR contact = CALCULATE([# candidates - contacted (contacted date)], FILTER(ALLSELECTED('Calendar'),'Calendar'[Date]<=MAX('Calendar'[Date])))
RETURN
IF(end_date < MIN('Calendar'[Date]) || contact=0 || view=0, BLANK(),
    contact / view)
```

**`Cumulative viewed contacted trend line`**

```dax

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

**`Candidate Response`**

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

**`% Response Rate`**

```dax
DIVIDE([Candidate Response], [# candidates - contacted (contacted date)])
```

**`% Linkedin connect accepted`**

```dax
var accept = CALCULATE(DISTINCTCOUNT(candidate[candidate_id]), event[event_type]="Linkedin Connected")
var sent = CALCULATE(DISTINCTCOUNT(candidate[candidate_id]), event[event_type]="Linkedin Sent Contact")
var link = DIVIDE(accept, sent)
RETURN IF(link<>BLANK() && link>1, 1, link)
```

**`% Positive Response Rate`**

```dax
DIVIDE([# candidates - screen (contacted date)] , [# candidates - contacted (contacted date)])
```

**`Job - Time to Hire`**

```dax
CALCULATE(AVERAGE(job[Diff Contacted - Hired]), job[Diff Contacted - Hired]>0)
```

**`Candidate - Time to Hire`**

```dax
CALCULATE(AVERAGE(candidate_stage[Diff Contacted - Hired]), candidate_stage[date_hired]<>BLANK())
```

**`Estimate find a time`**

```dax
"This role typically requires " & FORMAT([Job - Time to Hire],"0") & " days to find a hire. If you start to contact candidates today, you will have the 1st hire on " & FORMAT(TODAY()+[Job - Time to Hire], "YYYY-MM-DD")
```

**`Estimate conversation`**

```dax
"You will need to have " & FORMAT([Conv rate Visited to Hires],"0") & " Linkedin profile viewed, " & FORMAT([Conv rate Contacted to Hires],"0") & " candidates contacted and " & FORMAT([Conv rate Actual screens to Hires], "0") & " actual screens to get 1 hire"
```

**`% Reacted to Screen`**

```dax
var perc = DIVIDE([# candidates - screen (screened date)],Metrics[# candidates - reacted (contacted date)],0)
RETURN IF(perc>1, 1, perc)
```

**`Job - Time to Find a Hire (Hired date)`**

```dax
CALCULATE(AVERAGE(job[Diff Contacted - Job created]), USERELATIONSHIP('Calendar'[Date], candidate_stage[date_hired]))
```

**`Job - Time to Hire (WBR)`**

```dax
CALCULATE(AVERAGE(job[Diff Contacted - Hired]), ALL(job[date_first_hired]), job[Job Days Opened], ALL('Calendar'))
```

**`Candidate - Time to Fill (Hired date)`**

```dax
CALCULATE(AVERAGE(candidate_stage[Diff Hired - Job created]), candidate_stage[date_hired]<>BLANK(), USERELATIONSHIP('Calendar'[Date], candidate_stage[date_hired]))
```

**`Candidate - Time to Find a Hire (Hired date)`**

```dax
CALCULATE(AVERAGE(candidate_stage[Diff Concated - Job created]), candidate_stage[date_hired]<>BLANK(), USERELATIONSHIP('Calendar'[Date], candidate_stage[date_hired]))
```

**`Candidate - Time to Hire (Hired date)`**

```dax
CALCULATE(AVERAGE(candidate_stage[Diff Contacted - Hired]), candidate_stage[date_hired]<>BLANK(), USERELATIONSHIP('Calendar'[Date], candidate_stage[date_hired]))
```

**`# candidates - contacted (since job created)`**

```dax
var contact = CALCULATE(COUNT(candidate_stage[candidate_id]), ALL('Calendar'))
RETURN CALCULATE(contact, candidate_stage[date_contacted]<>BLANK())

```

**`# candidates - actual screen (since job open)`**

```dax
var acutalscreened = CALCULATE(COUNT(candidate_stage[candidate_id]), event[event_type]="Evaluation", ALL('Calendar'))
RETURN CALCULATE(acutalscreened, candidate_stage[date_screen_actual]<>BLANK())
```

**`# candidates - move to ATS (since job created)`**

```dax
var moveto = CALCULATE(COUNT(candidate_stage[candidate_id]), event[moved_to_stage]="Moved to ATS", ALL('Calendar'))
RETURN CALCULATE(moveto, candidate_stage[date_interview]<>BLANK())
```

**`# candidates - offer (since job created)`**

```dax
var offer = CALCULATE(COUNT(candidate_stage[candidate_id]), event[moved_to_stageType]="Offer", ALL('Calendar'))
RETURN CALCULATE(offer, candidate_stage[date_offer]<>BLANK())
```

**`# candidates - screen (since job created)`**

```dax
var screened = CALCULATE(COUNT(candidate_stage[candidate_id]), event[moved_to_stage]="Recruiter Screen", ALL('Calendar'))
RETURN CALCULATE(screened, candidate_stage[date_screen]<>BLANK())
```

**`TS Client-facing roles`**

```dax
CALCULATE(DISTINCTCOUNT(event[job_id]), job[client_id]<>"1644871631616x132418487146250240", USERELATIONSHIP(event[job_id], job[job_id]))
```

**`TS Client-facing %`**

```dax
IF([TS Client-facing roles]=BLANK() && DISTINCTCOUNT(event[job_id])<>0, 0, DIVIDE([TS Client-facing roles], DISTINCTCOUNT(event[job_id])))
```

**`# events - sourced (sourced date)`**

```dax
CALCULATE(DISTINCTCOUNT(event[candidate_id]), USERELATIONSHIP('Calendar'[Date], event[date_created]), (event[event_type]="Candidate created" && event[moved_to_stageType]="Prospects"))
```

**`Linkedin Viewed per day`**

```dax
CALCULATE(
    DISTINCTCOUNT(event[talent_id + job_id]), event[event_type] = "Linkedin Visited Profile", USERELATIONSHIP(job[job_id], event[job_id]), USERELATIONSHIP('Calendar'[Date], event[date_created]), 
    ALLEXCEPT(event, event[date_created], event[who_created_event_first]),
    FILTER(ALL('Calendar'[Date]), 'Calendar'[Date]=MAX(event[date_created])
))
```

**`Sales - hired (hired date)`**

```dax
var hire = CALCULATE(COUNT(candidate_stage[candidate_id]), USERELATIONSHIP('Calendar'[Date],candidate_stage[date_hired]),event[moved_to_stage]="Hired", job[job_category]="Sales", job[job_subcategory]<>"Account Manager")
RETURN CALCULATE(hire, candidate_stage[date_hired]<>BLANK())
```

**`# candidates - contacted (contacted date) from Prospects stage`**

```dax
var contact = CALCULATE(COUNT(candidate_stage[candidate_id]), candidate_stage[Prospects]<>BLANK() && candidate_stage[date_contacted]<>BLANK())
RETURN CALCULATE(contact, USERELATIONSHIP('Calendar'[Date],candidate_stage[date_contacted]))
```

**`# candidates - Prospects stage no contacted yet`**

```dax
var prospects = CALCULATE(COUNT(candidate_stage[candidate_id]), candidate_stage[stage_current]="Prospects", ALLSELECTED('Calendar'[Date]))
RETURN CALCULATE(prospects, USERELATIONSHIP('Calendar'[Date],candidate_stage[date_created]))
```

**`% Sourced to Contacted`**

```dax
DIVIDE([# candidates - contacted (contacted date) from Prospects stage], [# events - sourced (sourced date)])
```

**`1st Client Interview`**

```dax
CALCULATE(DISTINCTCOUNT(event[candidate_id]), event[moved_to_stageType]="Final Interview", LEFT(event[moved_to_stage],1)="1", USERELATIONSHIP(event[date_created], 'Calendar'[Date]))
```

**`2nd Client Interview`**

```dax
CALCULATE(DISTINCTCOUNT(event[candidate_id]), event[moved_to_stageType]="Final Interview", LEFT(event[moved_to_stage],1)="2", USERELATIONSHIP(event[date_created], 'Calendar'[Date]))
```

**`3rd Client Interview`**

```dax
CALCULATE(DISTINCTCOUNT(event[candidate_id]), event[moved_to_stageType]="Final Interview", LEFT(event[moved_to_stage],1)="3", USERELATIONSHIP(event[date_created], 'Calendar'[Date]))
```

**`# candidates - positive response`**

```dax
CALCULATE(DISTINCTCOUNT(event[candidate_id]), event[moved_to_stageType]="Positive Response", event[date_created]>=DATE(2025,4,14), USERELATIONSHIP(event[date_created], 'Calendar'[Date]))
```

**`% Screens to Actual Screen`**

```dax
var perc = DIVIDE(Metrics[# candidates - actual screen (actual screen date)], [# candidates - screen (screened date)],0)
RETURN IF(perc>1, 1, perc)
```

**`% Contacted to Positive Response`**

```dax
var con = CALCULATE([# candidates - contacted (contacted date)], candidate_stage[date_contacted]>=DATE(2025,4,14))
var perc = DIVIDE([# candidates - positive response], con, 0)
RETURN IF(perc>1, 1, perc)
```

**`Finance sourcer allocation per client`**

```dax
DIVIDE([# candidates - contacted (contacted date)], CALCULATE([# candidates - contacted (contacted date)], REMOVEFILTERS(client[client_name])))
```

**`% Positive Response to Screen`**

```dax
var perc = DIVIDE([# candidates - screen (screened date)], [# candidates - positive response],0)
RETURN IF(perc>1, 1, perc)
```

**`Sourced Hired`**

```dax
CALCULATE([# candidates - hired (hired date)],  candidate[source]="Sourced")
```

**`Applicant Hired`**

```dax
CALCULATE([# candidates - hired (hired date)], candidate[source]="Applicant")
```

**`Onsite`**

```dax
CALCULATE(DISTINCTCOUNT(event[candidate_id]), event[moved_to_stageType]="Final Interview", event[moved_to_stage]="Onsite", USERELATIONSHIP(event[date_created], 'Calendar'[Date]))
```

**`Culture Interview`**

```dax
CALCULATE(DISTINCTCOUNT(event[candidate_id]), event[moved_to_stageType]="Final Interview", event[moved_to_stage]="Culture Interview", USERELATIONSHIP(event[date_created], 'Calendar'[Date]))
```

**`# candidates - rejected (rejected date)`**

```dax
var rejected = CALCULATE(DISTINCTCOUNT(event[candidate_id]), event[event_type]="Disqualified", USERELATIONSHIP(event[date_created], 'Calendar'[Date]))
RETURN CALCULATE(rejected, candidate[is_candidate_disqualified]<>BLANK())
```

**`Call with Client Interview`**

```dax
CALCULATE(DISTINCTCOUNT(event[candidate_id]), event[moved_to_stageType]="Final Interview", event[moved_to_stage]="Call with Client", USERELATIONSHIP(event[date_created], 'Calendar'[Date]))
```

### Table: `OKR TA` (1 measures)

**`Average OKR`**

```dax
DIVIDE(SUM('OKR TA'[Contacted OKR])+SUM('OKR TA'[Actual Screen])+SUM('OKR TA'[ATS OKR]), SUM('OKR TA'[Count]))
```

### Table: `Org` (2 measures)

**`B2B : FTE`**

```dax

VAR b2b = CALCULATE(COUNT(Org[Employee #]), Org[FTE/B2B] = "B2B", ALL('WBR TA Actual'), ALL(Org), ALL('Calendar WBR'), ALL(candidate))
VAR fte = CALCULATE(COUNT(Org[Employee #]), Org[FTE/B2B] = "FTE", ALL('WBR TA Actual'), ALL(Org), ALL('Calendar WBR'), ALL(candidate))
VAR total = b2b + fte

VAR b2bPct = ROUND(DIVIDE(b2b, total, 0) * 100, 0)
VAR ftePct = 100 - b2bPct

RETURN 
b2bPct & "% : " & ftePct & "%"

--& "  (" & b2b & ":" & fte & ")"
```

**`B2B : FTE Number`**

```dax

VAR b2b = CALCULATE(COUNT(Org[Employee #]), Org[FTE/B2B] = "B2B", ALL('WBR TA Actual'), ALL(Org), ALL('Calendar WBR'), ALL(candidate))
VAR fte = CALCULATE(COUNT(Org[Employee #]), Org[FTE/B2B] = "FTE", ALL('WBR TA Actual'), ALL(Org), ALL('Calendar WBR'), ALL(candidate))

RETURN 

"Number " & b2b & ":" & fte 
```

### Table: `WBR Client History` (2 measures)

**`Contacted Target Reached %`**

```dax
var con = CALCULATE(COUNT('WBR Client History'[Week start end]), 'WBR Client History'[Contacted Reached]="Reached")+0
var wee = DISTINCTCOUNT('WBR Client History'[Week start end]) 
RETURN
con/wee
```

**`Screens Target Reached %`**

```dax
var scr = CALCULATE(COUNT('WBR Client History'[Week start end]), 'WBR Client History'[Screens Reached]="Reached")+0
var wee = DISTINCTCOUNT('WBR Client History'[Week start end]) 
RETURN
scr/wee
```

### Table: `WBR TA Actual` (33 measures)

**`Screens color`**

```dax
IF(SELECTEDVALUE('WBR TA Actual'[TA Reasoning]) IN {"Internal-related: Out of Office", "Client-related: Client Delay"}, -1,
IF([% Actual Screens Target]>=1.2 && MAX('WBR TA Actual'[Screens Target])<>0, 120, //>120%
IF([% Actual Screens Target]>=1 && [% Actual Screens Target]<1.2 && MAX('WBR TA Actual'[Screens Target])<>0, 100, //100-150%
IF([% Actual Screens Target]>=0.75 && [% Actual Screens Target]<1 && MAX('WBR TA Actual'[Screens Target])<>0, 75,
IF([% Actual Screens Target]>=0.5 && [% Actual Screens Target]<0.75 && MAX('WBR TA Actual'[Screens Target])<>0, 50,
IF([% Actual Screens Target]>0 && [% Actual Screens Target]<0.5 && MAX('WBR TA Actual'[Screens Target])<>0, 0, //<50%
IF(MAX('WBR TA Actual'[Actual screens])=0, -2)))))))
```

**`Contacted color`**

```dax
IF(SELECTEDVALUE('WBR TA Actual'[TA Reasoning]) IN {"Internal-related: Out of Office", "Client-related: Client Delay"}, -1,
IF([% Contacted Target]>=1.2 && MAX('WBR TA Actual'[Contacted Target])<>0, 120, //>120%
IF([% Contacted Target]>=1 && [% Contacted Target]<1.2 && MAX('WBR TA Actual'[Contacted Target])<>0, 100, //100-150%
IF([% Contacted Target]>=0.75 && [% Contacted Target]<1 && MAX('WBR TA Actual'[Contacted Target])<>0, 75,
IF([% Contacted Target]>=0.5 && [% Contacted Target]<0.75 && MAX('WBR TA Actual'[Contacted Target])<>0, 50,
IF([% Contacted Target]>0 && [% Contacted Target]<0.5 && MAX('WBR TA Actual'[Contacted Target])<>0, 0, //<50%
IF(MAX('WBR TA Actual'[Contacted])=0, -2)))))))
```

**`Moved to ATS color`**

```dax
IF(SELECTEDVALUE('WBR TA Actual'[TA Reasoning]) IN {"Internal-related: Out of Office", "Client-related: Client Delay"}, -1,
IF([% Moved to ATS Target]>=1.2 && MAX('WBR TA Actual'[Moved to ATS Target])<>0, 120, //>120%
IF([% Moved to ATS Target]>=1 && [% Moved to ATS Target]<1.2 && MAX('WBR TA Actual'[Moved to ATS Target])<>0, 100, //100-120%
IF([% Moved to ATS Target]>=0.75 && [% Moved to ATS Target]<1 && MAX('WBR TA Actual'[Moved to ATS Target])<>0, 75,
IF([% Moved to ATS Target]>=0.5 && [% Moved to ATS Target]<0.75 && MAX('WBR TA Actual'[Moved to ATS Target])<>0, 50,
IF([% Moved to ATS Target]>0 && [% Moved to ATS Target]<0.5 && MAX('WBR TA Actual'[Moved to ATS Target])<>0, 0, //<50%
IF(MAX('WBR TA Actual'[Moved to ATS])=0, -2)))))))
```

**`Contacted percentage`**

```dax
var perc = SUM('WBR TA Actual'[Contacted]) / MIN('WBR TA Actual'[Contacted Target])
RETURN
IF(perc>1, 1.1, perc)
```

**`Last 12w Hires`**

```dax
IF(SUM('WBR TA Actual'[Contacted])+SUM('WBR TA Actual'[Actual screens])+SUM('WBR TA Actual'[Moved to ATS])+SUM('WBR TA Actual'[Hires])<>0,
    CALCULATE(SUM('WBR TA Actual'[Hires]), 'WBR TA Actual'[is_last_12_weeks]="Yes", ALL('Calendar WBR')))
```

**`Last 12w Wolt number`**

```dax
//IF(SUM('WBR TA Actual'[Contacted])+SUM('WBR TA Actual'[Actual screens])+SUM('WBR TA Actual'[Moved to ATS])<>0 && (SELECTEDVALUE('WBR TA Actual'[Client])IN {"Wolt Expansion", "Wolt Market", "Wolt North, Baltics & Benelux", "Wolt Central & South", "Wolt HQ", "Wolt Germany"} ||
 IF(SELECTEDVALUE('WBR TA Actual'[Client])="Wolt Volume",
    CALCULATE(SUM('WBR TA Actual'[Hires]), 'WBR TA Actual'[is_last_12_weeks]="Yes", ALL('Calendar WBR')))
```

**`Wolt last 12w TAs`**

```dax

// var woltB =  CALCULATE(COUNTROWS('WBR TA Actual'), 'WBR TA Actual'[Client] IN {"Wolt Expansion", "Wolt Market", "Wolt North, Baltics & Benelux", "Wolt Central & South", "Wolt HQ", "Wolt Germany"} && 'WBR TA Actual'[Client]<>"Wolt Volume", 'WBR TA Actual'[is_last_12_weeks]="Yes", ALL('Calendar WBR'))*1.25
var woltV =  CALCULATE(COUNTROWS('WBR TA Actual'), 'WBR TA Actual'[Client]="Wolt Volume", 'WBR TA Actual'[is_last_12_weeks]="Yes", ALL('Calendar WBR'))*1.75
// var toge = IF(SELECTEDVALUE('WBR TA Actual'[Client]) IN {"Wolt Expansion", "Wolt Market", "Wolt North, Baltics & Benelux", "Wolt Central & South", "Wolt HQ", "Wolt Germany"}, woltB, IF(SELECTEDVALUE('WBR TA Actual'[Client])="Wolt Volume", woltV))
//var target = DIVIDE([Last 12w Wolt number], toge)
RETURN SWITCH(TRUE(),
[Last 12w Wolt number]=0, -1,
woltV>=1.2, 1.2,
woltV<1.2 && woltV>=1, 1,
woltV<1 && woltV>-0.75, 0.75,
woltV<0.75 && woltV>=0.5, 0.5,
woltV<0.5, 0, -1)

//target>=1.5, 1.5,
//target<1.5 && target>=1, 1,
//target<1 && target>-0.75, 0.75,
//target<0.75 && target>=0.5, 0.5,
//target<0.5, 0, -1)
```

**`% Moved to ATS Target`**

```dax
var target = SUM('WBR TA Actual'[Moved to ATS])/SUM('WBR TA Actual'[Moved to ATS Target])
RETURN IF(SUM('WBR TA Actual'[Moved to ATS])=0 || SUM('WBR TA Actual'[Moved to ATS Target])=0, BLANK(), target)
```

**`% Contacted Target`**

```dax
var target = SUM('WBR TA Actual'[Contacted])/SUM('WBR TA Actual'[Contacted Target])
RETURN IF(SUM('WBR TA Actual'[Contacted])=0 || SUM('WBR TA Actual'[Contacted Target])=0, BLANK(), target)
```

**`% Actual Screens Target`**

```dax
var target = SUM('WBR TA Actual'[Actual screens])/SUM('WBR TA Actual'[Screens Target])
RETURN IF(SUM('WBR TA Actual'[Actual screens])=0 || SUM('WBR TA Actual'[Screens Target])=0, BLANK(), target)
```

**`Last 12w Time to Fill`**

```dax
IF([Last 12w Hires]<>0, 
CALCULATE(AVERAGE('WBR TA Actual'[Time to Fill]), 'WBR TA Actual'[is_last_12_weeks]="Yes", ALL('Calendar WBR')))
```

**`Contacted OKR number`**

```dax
SWITCH(TRUE(),
[% Contacted Target]>=1.5 && MAX('WBR TA Actual'[Contacted Target])<>0, 5, //>150%
[% Contacted Target]>=1 && [% Contacted Target]<1.5 && MAX('WBR TA Actual'[Contacted Target])<>0, 4, //100-150%
[% Contacted Target]>=0.75 && [% Contacted Target]<1 && MAX('WBR TA Actual'[Contacted Target])<>0, 3,
[% Contacted Target]>=0.5 && [% Contacted Target]<0.75 && MAX('WBR TA Actual'[Contacted Target])<>0, 2,
[% Contacted Target]>0 && [% Contacted Target]<0.5 && MAX('WBR TA Actual'[Contacted Target])<>0, 1, //<50%
MAX('WBR TA Actual'[Contacted])=0, BLANK())
```

**`Screens OKR number`**

```dax
SWITCH(TRUE(),
[% Actual Screens Target]>=1.5 && MAX('WBR TA Actual'[Screens Target])<>0, 5, //>150%
[% Actual Screens Target]>=1 && [% Actual Screens Target]<1.5 && MAX('WBR TA Actual'[Screens Target])<>0, 4, //100-150%
[% Actual Screens Target]>=0.75 && [% Actual Screens Target]<1 && MAX('WBR TA Actual'[Screens Target])<>0, 3,
[% Actual Screens Target]>=0.5 && [% Actual Screens Target]<0.75 && MAX('WBR TA Actual'[Screens Target])<>0, 2,
[% Actual Screens Target]>0 && [% Actual Screens Target]<0.5 && MAX('WBR TA Actual'[Screens Target])<>0, 1, //<50%
MAX('WBR TA Actual'[Actual screens])=0, BLANK())
```

**`Moved to ATS OKR number`**

```dax
SWITCH(TRUE(),
[% Moved to ATS Target]>=1.5 && MAX('WBR TA Actual'[Moved to ATS Target])<>0, 5, //>150%
[% Moved to ATS Target]>=1 && [% Moved to ATS Target]<1.5 && MAX('WBR TA Actual'[Moved to ATS Target])<>0, 4, //100-150%
[% Moved to ATS Target]>=0.75 && [% Moved to ATS Target]<1 && MAX('WBR TA Actual'[Moved to ATS Target])<>0, 3,
[% Moved to ATS Target]>=0.5 && [% Moved to ATS Target]<0.75 && MAX('WBR TA Actual'[Moved to ATS Target])<>0, 2,
[% Moved to ATS Target]>0 && [% Moved to ATS Target]<0.5 && MAX('WBR TA Actual'[Moved to ATS Target])<>0, 1, //<50%
MAX('WBR TA Actual'[Moved to ATS])=0, BLANK())
```

**`OKR average`**

```dax
([Contacted OKR number]+[Moved to ATS OKR number]+[Screens OKR number])/3
```

**`Last 12w Wolt %`**

```dax
SWITCH(TRUE(),
SUM('WBR TA Actual'[Contacted])+SUM('WBR TA Actual'[Actual screens])+SUM('WBR TA Actual'[Moved to ATS])+SUM('WBR TA Actual'[Hires])<>0 && SELECTEDVALUE('WBR TA Actual'[Client]) IN {"Wolt Expansion", "Wolt Market", "Wolt North, Baltics & Benelux", "Wolt Central & South", "Wolt Germany"}, 
    [Last 12w Hires]/15,

SUM('WBR TA Actual'[Contacted])+SUM('WBR TA Actual'[Actual screens])+SUM('WBR TA Actual'[Moved to ATS])+SUM('WBR TA Actual'[Hires])<>0 && SELECTEDVALUE('WBR TA Actual'[Client])="Wolt HQ", 
    [Last 12w Hires]/12,

SUM('WBR TA Actual'[Contacted])+SUM('WBR TA Actual'[Actual screens])+SUM('WBR TA Actual'[Moved to ATS])+SUM('WBR TA Actual'[Hires])<>0 && SELECTEDVALUE('WBR TA Actual'[Client])="Wolt Tech",
    [Last 12w Hires]/7,    

SUM('WBR TA Actual'[Contacted])+SUM('WBR TA Actual'[Actual screens])+SUM('WBR TA Actual'[Moved to ATS])+SUM('WBR TA Actual'[Hires])<>0 && SELECTEDVALUE('WBR TA Actual'[Client])="Wolt Volume",
     [Last 12w Hires]/42
)
```

**`Total Missing comment`**

```dax
SUM('WBR TA Actual'[Missing comment]) + SUM('WBR TS Actual'[Missing comment])
```

**`Last 12w ATS`**

```dax
IF(SUM('WBR TA Actual'[Contacted])+SUM('WBR TA Actual'[Actual screens])+SUM('WBR TA Actual'[Moved to ATS])+SUM('WBR TA Actual'[Hires])<>0,
    CALCULATE(SUM('WBR TA Actual'[Moved to ATS]), 'WBR TA Actual'[is_last_12_weeks]="Yes", ALL('Calendar WBR')))
```

**`Last 12w Actual Screens`**

```dax
IF(SUM('WBR TA Actual'[Contacted])+SUM('WBR TA Actual'[Actual screens])+SUM('WBR TA Actual'[Moved to ATS])+SUM('WBR TA Actual'[Hires])<>0,
    CALCULATE(SUM('WBR TA Actual'[Actual screens]), 'WBR TA Actual'[is_last_12_weeks]="Yes", ALL('Calendar WBR')))
```

**`Total TA - OKR comment`**

```dax
CALCULATE(COUNTROWS('WBR TA Actual'), FILTER('WBR TA Actual', 'WBR TA Actual'[Client]<>BLANK()))
```

**`Total TA + TS - OKR comment`**

```dax
[Total TA - OKR comment]+[Total TS - OKR comment]
```

**`% Missing comment`**

```dax
1 - DIVIDE([Total Missing comment], [Total TA + TS - OKR comment])
```

**`Latest comment MBR`**

```dax

VAR TA = SELECTEDVALUE('WBR TA Actual'[who_event_created_for])
VAR LatestWeek =
    CALCULATE(
        MAX('WBR TA Actual'[WeekInt]),
        FILTER(
            'WBR TA Actual',
            'WBR TA Actual'[who_event_created_for] = TA &&
            (NOT(ISBLANK('WBR TA Actual'[Comment]) || 'WBR TA Actual'[Comment]=""))
        )
    )
RETURN
    CALCULATE(
        MAX('WBR TA Actual'[Comment]),
        FILTER(
            'WBR TA Actual',
            'WBR TA Actual'[who_event_created_for] = TA &&
            'WBR TA Actual'[WeekInt] = LatestWeek
        )
    )
```

**`Last 12w ATS Target`**

```dax
IF(SUM('WBR TA Actual'[Contacted])+SUM('WBR TA Actual'[Actual screens])+SUM('WBR TA Actual'[Moved to ATS])+SUM('WBR TA Actual'[Hires])<>0,
    CALCULATE(SUM('WBR TA Actual'[Moved to ATS Target]), 'WBR TA Actual'[is_last_12_weeks]="Yes", ALL('Calendar WBR')))
```

**`Last 12w ATS % target`**

```dax
DIVIDE([Last 12w ATS], [Last 12w ATS Target], 0)
```

**`Last 12w Actual Screens Target`**

```dax
IF(SUM('WBR TA Actual'[Contacted])+SUM('WBR TA Actual'[Actual screens])+SUM('WBR TA Actual'[Moved to ATS])+SUM('WBR TA Actual'[Hires])<>0,
    CALCULATE(SUM('WBR TA Actual'[Screens Target]), 'WBR TA Actual'[is_last_12_weeks]="Yes", ALL('Calendar WBR')))
```

**`Last 12w Actual Screens % Target`**

```dax
DIVIDE([Last 12w Actual Screens], [Last 12w Actual Screens Target], 0)
```

**`# Data Cleanliness/Hygiene`**

```dax
CALCULATE(COUNT('WBR TA Actual'[who_event_created_for]), FILTER('WBR TA Actual', 'WBR TA Actual'[Data Cleanliness/Hygiene]="No"))
```

**`Last 12w % Actual Screens to Hires`**

```dax
DIVIDE([Last 12w Hires], [Last 12w Actual Screens], 0)
```

**`% Actual Screens to ATS`**

```dax
IF(SUM('WBR TA Actual'[Actual screens])=0, BLANK(),
    SUM('WBR TA Actual'[Moved to ATS]) /SUM('WBR TA Actual'[Actual screens]))
```

**`% TA Reach Actual Screen Target`**

```dax
DIVIDE(SUM('WBR TA Actual'[Reach Actual Screen Target]), SUM('WBR TA Actual'[Valid TA for reach actual screens]))
```

**`% Actual Screens to Hired`**

```dax
IF(SUM('WBR TA Actual'[Actual screens])=0, BLANK(),
    SUM('WBR TA Actual'[Hires]) /SUM('WBR TA Actual'[Actual screens]))
```

**`% Data Cleanliness/Hygiene`**

```dax
DIVIDE([# Data Cleanliness/Hygiene], CALCULATE(COUNT('WBR TA Actual'[who_event_created_for]), 'WBR TA Actual'[WeekInt]>=11))
```

### Table: `WBR TA Comment` (1 measures)

**`Comment week`**

```dax
SELECTEDVALUE('WBR TA Comment'[Comment])
```

### Table: `WBR TA Job` (1 measures)

**`Actual screen to ATS %`**

```dax
DIVIDE(SUM('WBR TA Job'[Moved to ATS]), SUM('WBR TA Job'[Actual screens]))
```

### Table: `WBR TS Actual` (15 measures)

**`Last 12w Hires TS`**

```dax
IF(SUM('WBR TS Actual'[Contacted])+SUM('WBR TS Actual'[Viewed])<>0,
    CALCULATE(SUM('WBR TS Actual'[Hires]), 'WBR TS Actual'[is_last_12_weeks]="Yes", ALL('Calendar WBR')))
```

**`% Contacted Target TS`**

```dax
var target = SUM('WBR TS Actual'[Contacted])/SUM('WBR TS Actual'[Target])
RETURN IF(SELECTEDVALUE('WBR TS Actual'[TS Reasoning]) IN {"Internal-related: Out of Office", "Client-related: Client Delay"}, -1,
IF(SUM('WBR TS Actual'[Contacted])=0, BLANK(), 
target))
```

**`Last 12w % Contacted to Screens`**

```dax
IF(SUM('WBR TS Actual'[Contacted])+SUM('WBR TS Actual'[Viewed])<>0, CALCULATE(SUM('WBR TS Actual'[Screens])/SUM('WBR TS Actual'[Contacted]), 'WBR TS Actual'[is_last_12_weeks]="Yes", ALL('Calendar WBR')))
```

**`Last 12w % Actual Screens to ATS`**

```dax
IF(SUM('WBR TS Actual'[Actual Screens])=0, BLANK(),
    CALCULATE(SUM('WBR TS Actual'[ATS])/SUM('WBR TS Actual'[Actual Screens]), 'WBR TS Actual'[is_last_12_weeks]="Yes", ALL('Calendar WBR')))
```

**`Total TS - OKR comment`**

```dax
CALCULATE(COUNTROWS('WBR TS Actual'), FILTER('WBR TS Comment', 'WBR TS Comment'[Concat]<>BLANK()))
```

**`Rolling 12 weeks Actual Screens`**

```dax

VAR ThisWeek = YEAR(TODAY()) * 100 + WEEKNUM(TODAY(), 2) -- Get current week YYYYWW
VAR CurrentWeek = MAX('WBR TS Actual'[weekint]) -- Get the week being evaluated in the row
VAR CurrentUser = SELECTEDVALUE('WBR TS Actual'[who_created_event_first]) -- Get the current user in the row
VAR Last12Weeks = 
    FILTER(
        ALL('WBR TS Actual'), 
        'WBR TS Actual'[weekint] <= CurrentWeek && -- Only past and current weeks
        'WBR TS Actual'[weekint] >= CurrentWeek - 3 && -- Last 3 weeks from the row's week
        'WBR TS Actual'[who_created_event_first] = CurrentUser -- Ensure it's for the same user
    )
RETURN 
    IF(
        CurrentWeek > ThisWeek, 
        BLANK(), -- Ignore future weeks
        SUMX(Last12Weeks, 'WBR TS Actual'[Actual Screens])
    )

```

**`Last 12w % Sreens to Actual Screens`**

```dax
IF(SUM('WBR TS Actual'[Screens])=0, BLANK(),
    CALCULATE(SUM('WBR TS Actual'[Actual Screens])/SUM('WBR TS Actual'[Screens]), 'WBR TS Actual'[is_last_12_weeks]="Yes", ALL('Calendar WBR')))
```

**`% Positive Response / Contacted`**

```dax
IF(SUM('WBR TS Actual'[Positive Response])=0, BLANK(),
    DIVIDE(SUM('WBR TS Actual'[Positive Response]), SUM('WBR TS Actual'[Contacted])))
```

**`Last 4w % Actual Screens to ATS`**

```dax
IF(SUM('WBR TS Actual'[Actual Screens])=0, BLANK(),
    CALCULATE(SUM('WBR TS Actual'[ATS])/SUM('WBR TS Actual'[Actual Screens]), 'WBR TS Actual'[is_last_4_weeks]="Yes", ALL('Calendar WBR')))
```

**`Last 4w % Sreens to Actual Screens`**

```dax
IF(SUM('WBR TS Actual'[Screens])=0, BLANK(),
    CALCULATE(SUM('WBR TS Actual'[Actual Screens])/SUM('WBR TS Actual'[Screens]), 'WBR TS Actual'[is_last_4_weeks]="Yes", ALL('Calendar WBR')))
```

**`Screens target %`**

```dax
var avge = DIVIDE(SUM('WBR TS Actual'[Screens]), DISTINCTCOUNT('WBR TS Actual'[Week start end]))
RETURN avge / 10
-- weekly screens target is 10
```

**`ATS target %`**

```dax
var avge = DIVIDE(SUM('WBR TS Actual'[ATS]), DISTINCTCOUNT('WBR TS Actual'[Week start end]))
RETURN IF(SELECTEDVALUE('WBR TS Actual'[TS Reasoning]) IN {"Internal-related: Out of Office", "Client-related: Client Delay"}, -1,
avge / 4)
-- weekly ATS target is 4
```

**`Latest comment MBR TS`**

```dax

VAR TS = SELECTEDVALUE('WBR TS Actual'[who_created_event_first])
VAR LatestWeek =
    CALCULATE(
        MAX('WBR TS Actual'[WeekInt]),
        FILTER(
            'WBR TS Actual',
            'WBR TS Actual'[who_created_event_first] = TS &&
            (NOT(ISBLANK('WBR TS Actual'[Comment]) || 'WBR TS Actual'[Comment]=""))
        )
    )
RETURN
    CALCULATE(
        MAX('WBR TS Actual'[Comment]),
        FILTER(
            'WBR TS Actual',
            'WBR TS Actual'[who_created_event_first] = TS &&
            'WBR TS Actual'[WeekInt] = LatestWeek
        )
    )
```

**`Actual Sreens target %`**

```dax
var avge = DIVIDE(SUM('WBR TS Actual'[Actual Screens]), DISTINCTCOUNT('WBR TS Actual'[Week start end]))
RETURN IF(SELECTEDVALUE('WBR TS Actual'[TS Reasoning]) IN {"Internal-related: Out of Office", "Client-related: Client Delay"}, -1,
avge / 7)
-- weekly Actual Sreens target is 7
```

**`Recruiter Screens %`**

```dax
var avge = DIVIDE(SUM('WBR TS Actual'[Screens]), DISTINCTCOUNT('WBR TS Actual'[Week start end]))
RETURN IF(SELECTEDVALUE('WBR TS Actual'[TS Reasoning]) IN {"Internal-related: Out of Office", "Client-related: Client Delay"}, -1,
avge / 10)
-- weeklyRecuiter Screen target is 10
```

### Table: `analytic_usage` (2 measures)

**`7 days moving avg`**

```dax
VAR CurrDate = MAX(analytic_usage[created_date])
VAR Days =
    CALCULATETABLE (
        VALUES(analytic_usage[created_date]),
        analytic_usage[created_date] <= CurrDate,
        analytic_usage[created_date] > CurrDate - 7
    )
RETURN
    AVERAGEX(Days, CALCULATE( DISTINCTCOUNT(analytic_usage[user])))
```

**`7 days moving avg limited`**

```dax
VAR CurrDate = MAX(analytic_usage[created_date])
VAR Days =
    CALCULATETABLE (
        VALUES(analytic_usage[created_date]),
        analytic_usage[created_date] <= CurrDate,
        analytic_usage[created_date] > CurrDate - 7
    )
RETURN
    AVERAGEX(Days, CALCULATE(DISTINCTCOUNT(analytic_usage[user]), analytic_usage[page] in {"Board_view"}))
```

### Table: `candidate_stage` (1 measures)

**`# Candidate Hired (time to hire)`**

```dax
CALCULATE(COUNT(candidate_stage[candidate_id]), candidate_stage[date_hired]<>BLANK())
```

### Table: `event` (5 measures)

**`Created date peroid`**

```dax
"You are viewing events created from " & FORMAT(MIN(event[date_created]),"YYYY-MM-DD") & " to " & FORMAT(MAX(event[date_created]),"YYYY-MM-DD")
```

**`# User overwrite to not fit`**

```dax
CALCULATE(COUNT(event[event_id]), event[event_type]="User overwrite to not fit", USERELATIONSHIP('Calendar'[Date], event[date_created]))
```

**`# User overwrite to fit`**

```dax
CALCULATE(COUNT(event[event_id]), event[event_type]="User overwrite to fit", USERELATIONSHIP('Calendar'[Date], event[date_created]))
```

**`# Session cookie expired`**

```dax
CALCULATE(COUNT(event[event_id]), event[event_type]="Session cookie expired")
```

**`# Profile was scraped`**

```dax
CALCULATE(COUNT(event[event_id]), event[event_type]="Profile was scraped")
```

### Table: `job` (12 measures)

**`# Job (time to hire)`**

```dax
CALCULATE(COUNT(job[job_id]), job[date_first_hired]<>BLANK(), job[Diff Hired - Job created]>=0)
```

**`Job Creation between`**

```dax
"From " & FORMAT(MIN(job[date_created]), "YYYY-MM-DD") & " until " & FORMAT(MAX(job[date_created]), "YYYY-MM-DD") 
```

**`0-30 days`**

```dax
CALCULATE(COUNT(job[job_id]), job[Job Days Opened]<=30)
```

**`30-60 days`**

```dax
CALCULATE(COUNT(job[job_id]), job[Job Days Opened]>30 && job[Job Days Opened]<=60)
```

**`> 60 days`**

```dax
CALCULATE(COUNT(job[job_id]), job[Job Days Opened]>60)
```

**`Total Active Pipelines`**

```dax
CALCULATE(DISTINCTCOUNT(job[job_id]), REMOVEFILTERS(job[Problem jobs]))
```

**`Problem Pipelines %`**

```dax
DIVIDE(
    CALCULATE(DISTINCTCOUNT(job[job_id]), job[Problem jobs]=1),
    [Total Active Pipelines])
```

**`# Problem jobs`**

```dax
CALCULATE(COUNT(job[job_id]), job[Problem jobs]=1)
```

**`# Total Jobs for calculaing problem jobs`**

```dax
CALCULATE(COUNT(job[job_id]), job[# ATS]>2)
```

**`% Problem Jobs`**

```dax
DIVIDE([# Problem jobs], [# Total Jobs for calculaing problem jobs])
```

**`60-90 days`**

```dax
CALCULATE(COUNT(job[job_id]), job[Job Days Opened]>60 && job[Job Days Opened]<=90)
```

**`>90 days`**

```dax
CALCULATE(COUNT(job[job_id]), job[Job Days Opened]>90)
```

---

## 5. DAX calculated columns (123)

Columns added to base tables via DAX. Often these are the bridge between raw Bubble/Keboola fields and the cleaned values used in measures.

### Table: `Calendar` (13 columns)

**`MON-YYYY`**

```dax
CONCATENATE(LEFT('CALENDAR'[MonthName],3),
            CONCATENATE(" ",'CALENDAR'[Year]))
```

**`is_last_6_weeks`**

```dax
if('CALENDAR'[WeekEnding]>='CALENDAR'[WeekEnding -6w] && 'CALENDAR'[WeekEnding]<='CALENDAR'[WeekEnding Current], "Yes", "No")
```

**`is_last_6_months`**

```dax
if('Calendar'[MonthEnding]>'Calendar'[MonthEnding -6m] && 'Calendar'[MonthEnding]<='CALENDAR'[MonthEnding Current], TRUE(), FALSE())
```

**`is_current_week`**

```dax
if('Calendar'[WeekEnding Current] = 'Calendar'[WeekEnding], "Yes", "No")
```

**`Week start end`**

```dax
var weekstart = 'Calendar'[Date] - WEEKDAY('Calendar'[Date], 3)
var weekend = weekstart + 6
RETURN 
'Calendar'[Year] & "W" & 'Calendar'[WeekOfYear] & " (" & FORMAT(weekstart, "d/m") & "-" & FORMAT(weekend, "d/m") & ")" 
```

**`Week sort`**

```dax
- 'Calendar'[WeekInt]
```

**`is_previous_week`**

```dax
if('Calendar'[WeekEnding Current] = 'Calendar'[WeekEnding]+7, TRUE(), FALSE())
```

**`is_last_12_weeks`**

```dax
if('CALENDAR'[WeekEnding]>='CALENDAR'[WeekEnding -12w] && 'CALENDAR'[WeekEnding]<='CALENDAR'[WeekEnding Current], "Yes", "No")
```

**`Year/Month`**

```dax
FORMAT('Calendar'[Date], "YYYY/MM")
```

**`Week`**

```dax
"W" & 'Calendar'[WeekOfYear]
```

**`Before today`**

```dax
IF(DATEDIFF('Calendar'[Date], TODAY(),DAY)>=0, TRUE(), FALSE())
```

**`Week Z-A`**

```dax
'Calendar'[WeekInt]
```

**`is_last_4_weeks`**

```dax
if('CALENDAR'[WeekEnding]>='CALENDAR'[WeekEnding -4w] && 'CALENDAR'[WeekEnding]<='CALENDAR'[WeekEnding Current], "Yes", "No")
```

### Table: `Historical Manager Structure WBR` (1 columns)

**`Business Unit`**

```dax
SWITCH(TRUE(), 
'Historical Manager Structure WBR'[Report_To_Next_L2]="Tijana Lazovic", "Ponies and Unicorns",
'Historical Manager Structure WBR'[Report_To_Next_L2]="Kristjana Thorarinsdottir", "Dolphins and Whales",
"Other (Internal & Old Org Structure")
```

### Table: `OKR TA` (1 columns)

**`Count`**

```dax
var contact = IF('OKR TA'[Contacted OKR]<>BLANK(), 1, 0)
var screens = IF('OKR TA'[Actual Screen]<>BLANK(), 1, 0)
var ats = IF('OKR TA'[ATS OKR]<>BLANK(), 1, 0)
RETURN contact+screens+ats
```

### Table: `Org` (2 columns)

**`Business Unit current`**

```dax
var manager = PATH(Org[Name], Org[Manager])
RETURN
SWITCH(TRUE(),
SEARCH("Tijana Lazovic", manager,,0)>0, "Ponies and Unicorns",
SEARCH("Kristjana Thorarinsdottir", manager,,0)>0, "Wolt",
"Internal")
```

**`FTE/B2B`**

```dax
IF(Org[Location]="Remote", "B2B", "FTE")
```

### Table: `Sourcing Stats` (4 columns)

**`More than 200`**

```dax
IF('Sourcing Stats'[Viewed]>=200, 1, BLANK())
```

**`100- 200`**

```dax
IF('Sourcing Stats'[Viewed]<200 && 'Sourcing Stats'[Viewed]>=100, 1, BLANK())
```

**`Less than 50`**

```dax
IF('Sourcing Stats'[Viewed]<50, 1, BLANK())
```

**`50-100`**

```dax
IF('Sourcing Stats'[Viewed]>=50 && 'Sourcing Stats'[Viewed]<100, 1, BLANK())
```

### Table: `WBR Client History` (2 columns)

**`Contacted Reached`**

```dax
IF('WBR Client History'[Contacted]>='WBR Client History'[Contacted Target], "Reached", "No")

```

**`Screens Reached`**

```dax
IF('WBR Client History'[Screens]>='WBR Client History'[Screens Target], "Reached", BLANK())

```

### Table: `WBR Job from Aug 2025` (1 columns)

**`Contact`**

```dax
'WBR Job from Aug 2025'[who_event_created_for] & 'WBR Job from Aug 2025'[client_name]
```

### Table: `WBR Job open 60d` (1 columns)

**`Contacy`**

```dax
'WBR Job open 60d'[job_recruiter]&'WBR Job open 60d'[client_name]
```

### Table: `WBR TA Actual` (28 columns)

**`Screens Target`**

```dax
RELATED('WBR TA Target'[Actual Screens]) 
```

**`Contacted Target`**

```dax
RELATED('WBR TA Target'[Contacted])
```

**`Moved to ATS Target`**

```dax
RELATED('WBR TA Target'[Moved to ATS])
```

**`TA Client Key`**

```dax
'WBR TA Actual'[client_name]&'WBR TA Actual'[who_event_created_for]&'WBR TA Actual'[Year]&'WBR TA Actual'[Month]
```

**`Client`**

```dax
var valid = IF(RELATED('WBR TA Target'[Client])<>BLANK(), 1, 0)
RETURN
IF('WBR TA Actual'[client_name]<>"Wolt" && valid=1, 'WBR TA Actual'[client_name], 
IF('WBR TA Actual'[client_name]="Wolt" && valid=1, RELATED('WBR TA Target'[Client])))
```

**`Concat_comment`**

```dax
'WBR TA Actual'[Week start end]&'WBR TA Actual'[who_event_created_for]&'WBR TA Actual'[Client]
```

**`Comment`**

```dax
RELATED('WBR TA Comment'[Comment])
```

**`Month`**

```dax
var num = LEFT(RIGHT('WBR TA Actual'[Week start end], 3),2)
RETURN
IF(LEFT(num, 1)="/", RIGHT(num, 1), num)
```

**`Q`**

```dax
SWITCH(TRUE(),
'WBR TA Actual'[Month] IN {"1","2","3"}, "Q1",
'WBR TA Actual'[Month] IN {"4","5","6"}, "Q2",
'WBR TA Actual'[Month] IN {"7","8","9"}, "Q3",
'WBR TA Actual'[Month] IN {"10","11","12"}, "Q4")
```

**`Week rank`**

```dax
var weekrank = RANKX('WBR TA Actual', 'WBR TA Actual'[Week sort],,ASC,Dense)
RETURN
SWITCH(TRUE(),
weekrank>=1 && weekrank<12, "12w",
weekrank>=12 && weekrank<24, "24w",
weekrank>=24 && weekrank<36, "36w",
weekrank>=36 && weekrank<48, "48w", 
BLANK())
```

**`Test`**

```dax
RANKX('WBR TA Actual', 'WBR TA Actual'[Week sort],,ASC,Dense)
```

**`# Sourcer`**

```dax
SWITCH(TRUE(),
'WBR TA Actual'[Sourcer]=BLANK(), 0, 
LEN(SUBSTITUTE('WBR TA Actual'[Sourcer]," ",""))-LEN(SUBSTITUTE(SUBSTITUTE('WBR TA Actual'[Sourcer]," ",""), ",",""))+1)
```

**`Job 60 days`**

```dax
RELATED('WBR Job open 60d'[# Jobs 60d])
```

**`Manager`**

```dax
RELATED('Historical Manager Structure WBR'[Report_To])
```

**`Missing comment`**

```dax
SWITCH(TRUE(),
'WBR TA Actual'[Contacted]=BLANK() && 'WBR TA Actual'[Actual screens]=BLANK() && 'WBR TA Actual'[Moved to ATS]=BLANK(), 0,
('WBR TA Actual'[Contacted]<'WBR TA Actual'[Contacted Target] || 'WBR TA Actual'[Actual screens]<'WBR TA Actual'[Screens Target] || 'WBR TA Actual'[Moved to ATS]<'WBR TA Actual'[Moved to ATS Target]) && 'WBR TA Actual'[Comment]=BLANK(), 1
, 
0)
```

**`TA& Cient jobs 60d`**

```dax
'WBR TA Actual'[who_event_created_for]&'WBR TA Actual'[client_name]
```

**`Client Wrap tooltip`**

```dax
LEFT('WBR TA Actual'[Client], 13)
```

**`Exclude OKR`**

```dax
RELATED('WBR TA Comment'[Exclude OKR])
```

**`Data Cleanliness/Hygiene`**

```dax
RELATED('WBR TA Comment'[Data Cleanliness/Hygiene])
```

**`Employee number`**

```dax
RELATED(User[employee_number])
```

**`Historical Manager Key`**

```dax
'WBR TA Actual'[Employee number] & "-" & 'WBR TA Actual'[WeekInt]
```

**`Reach Actual Screen Target`**

```dax
IF('WBR TA Actual'[Screens Target]<>BLANK() && 'WBR TA Actual'[Screens Target]<>0 && 'WBR TA Actual'[Actual screens]>='WBR TA Actual'[Screens Target] && 'WBR TA Actual'[Actual screens]<>BLANK(), 1, 0)
```

**`Valid TA for reach actual screens`**

```dax
IF('WBR TA Actual'[Screens Target]<>BLANK() && 'WBR TA Actual'[Screens Target]<>0 && 'WBR TA Actual'[Actual screens]<>BLANK() &&
('WBR TA Actual'[Exclude OKR]<>"Yes" || 'WBR TA Actual'[Actual screens]>='WBR TA Actual'[Screens Target]), 1, 0)
```

**`Key Job concat`**

```dax
'WBR TA Actual'[Week start end] & " " & 'WBR TA Actual'[client_id] & " " & 'WBR TA Actual'[who_event_created_for]
```

**`TA Reasoning`**

```dax
RELATED('WBR TA Comment'[Reasoning])
```

**`Not TA`**

```dax
IF('WBR TA Actual'[Screens Target]=BLANK() && 'WBR TA Actual'[Moved to ATS Target]=BLANK(), "Not TA")
```

**`Hires Target`**

```dax
RELATED('WBR TA Target'[Hires])
```

**`client_name`**

```dax
'WBR TA Actual'[client_wolt_group]
```

### Table: `WBR TA Job` (3 columns)

**`Key Job concat`**

```dax
'WBR TA Job'[Week start end] & " " & 'WBR TA Job'[client_id] & " " & 'WBR TA Job'[who_event_created_for]
```

**`Current week`**

```dax
LOOKUPVALUE('Calendar WBR'[is_current_week], 'Calendar WBR'[Week start end], 'WBR TA Job'[Week start end])
```

**`TA Client`**

```dax
RELATED('WBR TA Actual'[Client])
```

### Table: `WBR TA Target` (3 columns)

**`TA Client Key`**

```dax
IF(LEFT('WBR TA Target'[Client], 4)="Wolt", "Wolt"&'WBR TA Target'[TA]&'WBR TA Target'[Year]&'WBR TA Target'[Month],
    'WBR TA Target'[Client]&'WBR TA Target'[TA]&'WBR TA Target'[Year]&'WBR TA Target'[Month])
```

**`Year Month`**

```dax
'WBR TA Target'[Year]*100 + 'WBR TA Target'[Month]

```

**`Latest month`**

```dax
IF('WBR TA Target'[Year Month]=MAX('WBR TA Target'[Year Month]), 1, 0)
```

### Table: `WBR TS Actual` (9 columns)

**`Concat_comment`**

```dax
'WBR TS Actual'[Week start end]&'WBR TS Actual'[who_created_event_first]
```

**`Comment`**

```dax
RELATED('WBR TS Comment'[Comment])
```

**`# TA`**

```dax
SWITCH(TRUE(),
'WBR TS Actual'[TA]=BLANK(), 0, 
LEN(SUBSTITUTE('WBR TS Actual'[TA]," ",""))-LEN(SUBSTITUTE(SUBSTITUTE('WBR TS Actual'[TA], ",","")," ", ""))+1)
```

**`Manager`**

```dax
RELATED('Historical Manager Structure WBR'[Report_To])
```

**`Missing comment`**

```dax
SWITCH(TRUE(),
[% Contacted Target TS]<1 && 'WBR TS Actual'[Comment]=BLANK() && RELATED('WBR TS Comment'[Concat])<>BLANK() && 'WBR TS Actual'[WeekInt]<=202515, 1,
[% Contacted Target TS]<1 && 'WBR TS Actual'[Screens]<10 && 'WBR TS Actual'[Comment]=BLANK() && RELATED('WBR TS Comment'[Concat])<>BLANK() && 'WBR TS Actual'[WeekInt]>=202516, 1,
0)
```

**`Target`**

```dax
var target = RELATED('WBR TS Comment'[Contacted Target])
RETURN IF(target=BLANK(), 200, target)
```

**`Historical Manager Key TS`**

```dax
RELATED(User[employee_number]) & "-" & 'WBR TS Actual'[WeekInt]
```

**`TS Reasoning`**

```dax
RELATED('WBR TS Comment'[Reasoning])
```

**`Not TS`**

```dax
IF(RELATED('WBR TS Comment'[TS])=BLANK(), "Not TS")
```

### Table: `analytic_usage` (3 columns)

**`User Name`**

```dax
LOOKUPVALUE(User[user_name], User[user_id], analytic_usage[user])
```

**`Week`**

```dax
WEEKNUM(analytic_usage[created_date], 21)
```

**`Year/Month`**

```dax
FORMAT(analytic_usage[created_date], "YYYY/MM")
```

### Table: `candidate` (8 columns)

**`Response count`**

```dax
CALCULATE(COUNTROWS(RELATEDTABLE(event)), event[event_type] IN {"Linkedin Responded", "Linkedin inMail received", "Email Replied", "Disqualified"})
```

**`Candidate Responded?`**

```dax
IF(candidate[Response count]>0, TRUE, FALSE)
```

**`Linkedin connect sent`**

```dax
var num = CALCULATE(COUNTROWS(RELATEDTABLE(event)), event[event_type]="Linkedin Sent Contact")
RETURN IF(num>0, TRUE, FALSE)
```

**`Linkedin Connected`**

```dax
var num = CALCULATE(COUNTROWS(RELATEDTABLE(event)), event[event_type]="Linkedin Connected")
RETURN IF(num>0, TRUE, FALSE)
```

**`Hired Salary cal`**

```dax
candidate[hired_salary] * candidate[Exchange rate]
```

**`Exchange rate`**

```dax
SWITCH(TRUE(),
candidate[hired_salary_currency]="Croatian kuna", 0.13,
candidate[hired_salary_currency]="Czech koruna", 0.04,
candidate[hired_salary_currency]="Danish krone", 0.13,
candidate[hired_salary_currency]="Dollar", 0.93,
candidate[hired_salary_currency]="Hungarian forint", 0.0026,
candidate[hired_salary_currency]="Norwegian Krone", 0.09,
candidate[hired_salary_currency]="Polish złoty", 0.22,
candidate[hired_salary_currency]="Pound", 1.16, 
candidate[hired_salary_currency]="Rupee", 0.011,
candidate[hired_salary_currency]="Swedish krona", 0.09,
candidate[hired_salary_currency]="Euro", 1
)
```

**`Prospects`**

```dax
CALCULATE(COUNTROWS(RELATEDTABLE(event)), event[event_type]="Candidate created" && event[moved_to_stage]="Prospects")
```

**`Current Stage`**

```dax
RELATED(candidate_stage[Current Stage])
```

### Table: `candidate_stage` (9 columns)

**`Diff Contacted - Hired`**

```dax
var d = DATEDIFF(candidate_stage[date_contacted], candidate_stage[date_hired], DAY)
RETURN IF(d<0, BLANK(), d)
```

**`automation_steps`**

```dax
IF(ISBLANK(candidate_stage[automation_connections]), 
                        "0",
                        IF((candidate_stage[automation_connections]+candidate_stage[automation_emails]+candidate_stage[automation_inmails]+candidate_stage[automation_messages])<=5,
                            CONVERT(candidate_stage[automation_connections]+candidate_stage[automation_emails]+candidate_stage[automation_inmails]+candidate_stage[automation_messages], STRING),
                            "6+"))


```

**`is_automation`**

```dax
IF(candidate_stage[automation_steps] = "0", "No automation", "With automation")


```

**`Diff Concated - Job created`**

```dax
var d = DATEDIFF(RELATED(job[date_created]), candidate_stage[date_contacted], DAY)
RETURN IF(d<0, BLANK(), d)
```

**`Diff Hired - Job created`**

```dax
var d = DATEDIFF(RELATED(job[date_created]), candidate_stage[date_hired], DAY)
RETURN IF(d<0, BLANK(), d)
```

**`Contacted created diff`**

```dax
IF(candidate_stage[date_contacted]=candidate_stage[date_created], TRUE(), FALSE())
```

**`Last update`**

```dax
MAX(MAX(MAX(MAX(MAX(MAX(MAX(candidate_stage[date_created], candidate_stage[date_lnkdin_viewed]), candidate_stage[date_contacted]), candidate_stage[date_screen]), candidate_stage[date_screen_actual]), candidate_stage[date_interview]), candidate_stage[date_offer]), candidate_stage[date_hired])
```

**`Is First Hired?`**

```dax
SWITCH(TRUE(),
candidate_stage[hired_order]=1, "Yes",
candidate_stage[hired_order]<>1 && MOD(candidate_stage[hired_order],10)=1, CONVERT(candidate_stage[hired_order],STRING)&"st",
candidate_stage[hired_order]<>1 && MOD(candidate_stage[hired_order],10)=2, CONVERT(candidate_stage[hired_order],STRING)&"nd",
candidate_stage[hired_order]<>1 && MOD(candidate_stage[hired_order],10)=3, CONVERT(candidate_stage[hired_order],STRING)&"rd",
CONVERT(candidate_stage[hired_order],STRING)&"th")
```

**`Prospects`**

```dax
RELATED(candidate[Prospects])
```

### Table: `client` (1 columns)

**`client_wolt_group`**

```dax

IF (
    client[client_name] IN { "SevenRooms", "Doordash" },
    "Wolt",
    client[client_name]
)
```

### Table: `event` (11 columns)

**`is_automation_step_sent`**

```dax
IF(  (event[event_type]="Email Sent" && event[automation_step_type]="Email") 
                               || (event[event_type]="Linkedin Sent Contact" && event[automation_step_type]="Connection")
                               || (event[event_type]="Message sent" && event[automation_step_type]="LN message")
                               || (event[event_type]="Linkedin inMail sent" && event[automation_step_type]="Inmail")
                               , TRUE(), FALSE())
```

**`automation_step_consubcon`**

```dax
CONCATENATE(CONCATENATE(event[automation_step_subcon], " - "), event[automation_step_con])
```

**`Candidate Job ID`**

```dax
IF(event[moved_to_stageType]="Contacted" && event[event_type]="Moved to stage" && event[job_id]<>BLANK() && event[candidate_id]<>BLANK(), CONCATENATE(event[candidate_id], event[job_id]), BLANK())
```

**`Earliest contacted`**

```dax
VAR  earliest_contacted = CALCULATE(MIN(event[date_created]), FILTER(event, event[Candidate Job ID]=EARLIER(event[Candidate Job ID])), event[Candidate Job ID]<>BLANK())
RETURN
IF(event[date_created]=earliest_contacted, event[who_created_event], BLANK())
```

**`Sourcer credit contacted`**

```dax
VAR Sourcer = CONCATENATEX(
    FILTER(
        SUMMARIZE(event, event[Candidate Job ID], event[Earliest contacted]),
        event[Candidate Job ID]=EARLIER(event[Candidate Job ID]) &&
        event[Earliest contacted]<>BLANK()
    ),
    event[Earliest contacted], ", ", event[Earliest contacted])
RETURN IF(event[Earliest contacted]<>BLANK(), Sourcer, BLANK())
```

**`talent_id + job_id`**

```dax
CONCATENATE(event[talent_id], event[job_id]) 
```

**`Tribe`**

```dax
IF(event[who_created_event]=event[who_event_created_for], 1, 0)
```

**`Linkedin Viewed by AI`**

```dax
SWITCH(TRUE(),
event[event_type]="Linkedin Visited Profile" && event[is_event_createdby_ai]=TRUE(), "Yes", 
event[event_type]="Linkedin Visited Profile" && event[is_event_createdby_ai]=BLANK(), "No",
"No")
```

**`Not Official Sourcer WBR`**

```dax
IF(event[who_created_event_first]=RELATED(job[job_sourcer]), "Same", "Not")
```

**`Sourcer Employee Number`**

```dax
RELATED(User[employee_number])
```

**`Sourcing Team?`**

```dax
SWITCH(TRUE(),
RELATED('Sourcing Team List'[Moved to TA Position Date])=BLANK() && RELATED('Sourcing Team List'[Name])<>BLANK(), "Yes",
RELATED('Sourcing Team List'[Moved to TA Position Date])<>BLANK() && event[date_created]<RELATED('Sourcing Team List'[Moved to TA Position Date]) && RELATED('Sourcing Team List'[Name])<>BLANK(), "Yes",
"No")
```

### Table: `job` (23 columns)

**`Diff Contacted - Job created`**

```dax
var d= DATEDIFF(job[date_created], job[date_first_hired_contacted],DAY)
RETURN IF(d<0, BLANK(), d)
```

**`Diff Hired - Job created`**

```dax
var d = DATEDIFF(job[date_created], job[date_first_hired],DAY)
RETURN IF(d<0, BLANK(), d)
```

**`Tech Role`**

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

**`Job Days Opened`**

```dax
DATEDIFF(job[date_created], TODAY(), DAY)
```

**`Diff Contacted - Hired`**

```dax
var d= DATEDIFF(job[date_first_hired_contacted], job[date_first_hired],DAY)
RETURN IF(d<0, BLANK(), d)
```

**`Job Creation Year`**

```dax
FORMAT(job[date_created], "YYYY")
```

**`External Recruiter?`**

```dax
IF(job[is_external_recruiter]=TRUE(), "Yes", "No")
```

**`Job Country Grouping`**

```dax
SWITCH(TRUE(),
job[Job Country]="Germany", "Central Europe",
job[Job Country]="Austria", "Central Europe",
job[Job Country]="Czech Republic", "Central Europe",
job[Job Country]="Czechia", "Central Europe",
job[Job Country]="Hungary", "Central Europe",
job[Job Country]="Slovakia", "Central Europe",
job[Job Country]="Slovenia", "Central Europe",
job[Job Country]="Poland", "Central Europe",
job[Job Country]="France", "Western Europe",
job[Job Country]="Belgium", "Western Europe",
job[Job Country]="Netherlands", "Western Europe",
job[Job Country]="Luxembourg", "Western Europe",
job[Job Country]="Finland", "Northern Europe",
job[Job Country]="Sweden", "Northern Europe",
job[Job Country]="Norway", "Northern Europe",
job[Job Country]="Denmark", "Northern Europe",
job[Job Country]="Iceland", "Northern Europe",
job[Job Country]="Romania", "Eastern Europe",
job[Job Country]="Belarus", "Eastern Europe",
job[Job Country]="Russia", "Eastern Europe",
job[Job Country]="Ukraine", "Eastern Europe",
job[Job Country]="Moldova", "Eastern Europe",
job[Job Country]="Estonia", "Eastern Europe (Baltic States)",
job[Job Country]="Latvia", "Eastern Europe (Baltic States)",
job[Job Country]="Lithuania", "Eastern Europe (Baltic States)",
job[Job Country]="Baltic States", "Eastern Europe (Baltic States)",
job[Job Country]="Albania", "Southeast Europe",
job[Job Country]="Bosnia and Herzegovina", "Southeast Europe",
job[Job Country]="Bulgaria", "Southeast Europe",
job[Job Country]="Croatia", "Southeast Europe",
job[Job Country]="Montenegro", "Southeast Europe",
job[Job Country]="North Macedonia", "Southeast Europe",
job[Job Country]="Serbia", "Southeast Europe",
job[Job Country]="Italy", "Southern Europe",
job[Job Country]="Greece", "Southern Europe",
job[Job Country]="Spain", "Southern Europe",
job[Job Country]="Portugal", "Southern Europe",
job[Job Country]="Cyprus", "Southern Europe",
job[Job Country]="Malta", "Southern Europe",
"Other")
```

**`Tech Role (text)`**

```dax
IF(job[Tech Role]="Yes", "Tech Role", "Non-Tech Role")
```

**`Job Creation Month`**

```dax
FORMAT(job[date_created], "mmmm")
```

**`Job Creation Date Month Text`**

```dax
FORMAT(job[date_created], "MM")
```

**`Job Creation Week`**

```dax
WEEKNUM(job[date_created], 21)
```

**`Temp hide from WBR TS`**

```dax
RELATED(Temp_Inactive_Jobs_Sourcers_WBR[No longer working with the job])
```

**`Problem jobs`**

```dax
IF(
    ([# candidates - actual screen (actual screen date)]>=25 && job[date_first_hired]=BLANK() && [# candidates - hired (hired date)]=BLANK()) ||
    ([# candidates - actual screen (actual screen date)]/[# candidates - hired (hired date)]>=32 && job[date_first_hired]<>BLANK() && [# candidates - hired (hired date)]<>BLANK())
    , 
    1, 0)
```

**`Job Days Opened w/o hires`**

```dax
IF(job[date_first_hired]=BLANK(), DATEDIFF(job[date_created], TODAY(), DAY), BLANK())
```

**`# ATS`**

```dax
CALCULATE(COUNT(candidate_stage[candidate_id]), candidate_stage[date_interview]<>BLANK())
```

**`Early warning`**

```dax
var ats = IF(job[Job Days Opened]>=28 && job[# ATS]=BLANK(), 1, 0)
var offer = IF(job[# Actual Screens]>=25 && job[# Offers]=BLANK(), 1, 0)
var hire = IF(job[Job Days Opened]>=60 && job[date_first_hired]=BLANK(), 1, 0)
RETURN IF(ats=1 || offer=1 || hire=1, 1, 0)
```

**`# Offers`**

```dax
CALCULATE(COUNT(candidate_stage[candidate_id]), candidate_stage[date_offer]<>BLANK())
```

**`# Actual Screens`**

```dax
CALCULATE(COUNT(candidate_stage[candidate_id]), candidate_stage[date_screen_actual]<>BLANK())
```

**`# Hires`**

```dax
CALCULATE(COUNT(candidate_stage[candidate_id]), candidate_stage[date_hired]<>BLANK())
```

**`# Actual Screens with notes`**

```dax
[# candidates - actual screen (since job open)]
```

**`Jobs Opened`**

```dax
IF(job[date_first_hired]<>BLANK(), job[Diff Hired - Job created], job[Job Days Opened w/o hires])
```

**`Owned by sourcing team (active TS)`**

```dax
var ts = LOOKUPVALUE(Current_TS[TS], Current_TS[TS], job[job_sourcer])
RETURN
IF(ts=BLANK(), "No", "Yes")
```
