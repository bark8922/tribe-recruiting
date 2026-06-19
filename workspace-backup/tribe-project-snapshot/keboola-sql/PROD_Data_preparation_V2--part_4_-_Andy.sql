-- Transformation: [PROD] Data preparation V2
-- Block: code
-- Code: part 4 - Andy
-- Extracted from Keboola on 2026-03-30

create or replace table "analytic" as 
select
    "page"
    ,"user"
    ,to_timestamp("Created_Date")::DATE as "created_date"
    , count("bubbleinternal_id") as "count"
from "bubble_Analytic"
group by all
order by 3, 2, 1
;

create or replace table "job_ai_filter" as 
select
"bubbleinternal_id",
to_timestamp("Created_Date") as "created_date",
"Created_By",
 "job" as "job_id", 
 "name" as "job_name"
from "bubble_JobAiFilter";

