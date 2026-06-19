-- Transformation: Revenue - 2. data prep
-- Block: Block 1
-- Code: code
-- Extracted from Keboola on 2026-03-30

create or replace table "tmp" as 
with a as (
    select 
        "Bubble_ID" as "client_id"
        , iff(left("Company", 4)='Wolt', 'Wolt', "Company") as "client_name"
        , try_to_date("month", 'MONYY') as "month"
        , try_to_decimal(replace(trim("eur_amount", '€'),',',''),20,2) as "eur_amount"
    from "client-invoiced-unpivoted"
)
select 
    "client_id"
    , "client_name"
    , "month"
    , NULL::DATE as "month_moved"
    , zeroifnull(sum("eur_amount")) as "eur_invoiced"
    
    , NULL::INT as "hired_count"
    , NULL::INT as "hired_salary_sum"
from a
group by "client_id", "client_name", "month"
order by "client_id", "client_name", "month";

-- count of hired candidates for specific client/month
update "tmp" as tmp
set tmp."hired_count" = zeroifnull(select count(cs."candidate_id")
                        from "candidate_stage" as cs
                        inner join "candidate" as c
                            on c."candidate_id"=cs."candidate_id"
                            and cs."date_hired" <>''
                        inner join "job" as j
                            on j."job_id"=c."job_id"
                        where 1=1
                            and j."client_id"=tmp."client_id"
                            and last_day(tmp."month"::DATE)=last_day(cs."date_hired"::DATE)
                        );

-- sum of salaries of hired candidates for specific client/month                        
update "tmp" as tmp
set tmp."hired_salary_sum" = zeroifnull(
                            select 
                                sum(try_to_number(c."hired_salary_eur"))
                            from "candidate_stage" as cs
                            inner join "candidate" as c
                                on c."candidate_id"=cs."candidate_id"
                                and cs."date_hired" <>''
                            inner join "job" as j
                                on j."job_id"=c."job_id"
                            where 1=1
                                and j."client_id"=tmp."client_id"
                                and last_day(tmp."month"::DATE)=last_day(cs."date_hired"::DATE)
                            );

-- hired candidates in months when we have NO revenue from the client
-- moving this count and salaries to last invoiced months
update "tmp" as tmp
set tmp."month_moved" = (select 
                            max(x."month")
                        from "tmp" as x
                        where 1=1
                            and x."client_id"=tmp."client_id"
                            and x."month" < tmp."month"
                            and x."eur_invoiced">0
                        ) where 1=1
                            and tmp."hired_count">0
                            and tmp."eur_invoiced"=0;

-- moving this count to the last invoiced month
update "tmp" as tmp
set tmp."hired_count" = tmp."hired_count"+
                        zeroifnull(
                        select 
                            sum(x."hired_count")
                        from "tmp" as x
                        where 1=1
                            and x."client_id"=tmp."client_id"
                            and x."month_moved"=tmp."month"
                        );

-- moving salaries to last invoiced month
update "tmp" as tmp
set tmp."hired_salary_sum" = tmp."hired_salary_sum"+
                        zeroifnull(
                        select 
                            sum(x."hired_salary_sum")
                        from "tmp" as x
                        where 1=1
                            and x."client_id"=tmp."client_id"
                            and x."month_moved"=tmp."month"
                        );

-- set 0 to the months when we have NO revenue from the client
update "tmp" as tmp
set tmp."hired_count" = 0, 
    tmp."hired_salary_sum" = 0 
    where tmp."month_moved" is not NULL;

-- result 
create or replace table "client_cost" as 
select 
      "client_id"
    , "client_name"
    , "month"
    , "eur_invoiced" as "cost"
    , "hired_count"
    , "hired_salary_sum"
    /*
    , CASE WHEN "hired_count" > 0 
        THEN "eur_invoiced"/"hired_count" 
        ELSE NULL
      END as "cost_per_hire"
    , CASE WHEN "hired_salary_sum" > 0
        THEN "eur_invoiced"/"hired_salary_sum"
        ELSE NULL
      END as "cost_to_salary"
    */
from "tmp"
where 1=1
    and "cost" > 0
    and year("month")>=2022
order by 1,3
;

