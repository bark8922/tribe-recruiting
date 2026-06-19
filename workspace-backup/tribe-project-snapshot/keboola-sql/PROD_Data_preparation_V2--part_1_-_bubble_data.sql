-- Transformation: [PROD] Data preparation V2
-- Block: code
-- Code: part 1 - bubble data
-- Extracted from Keboola on 2026-03-30

/* 
    JOB
*/
create or replace table "tmp_job" as 

select 
      j."bubbleinternal_id" as "job_id"
    , left(j."Created_Date",10) as "date_created"
    , j."Title" as "job_title"
    , try_to_boolean(j."archived") as "is_job_archived"
    , try_to_boolean(j."archived") as "is_client_archived"
    , c."bubbleinternal_id" as "client_id"
    , c."Name" as "client_name"
    , ifnull(hm."Name", '-not available-') as "user_hiring_manager"
    , lower(trim(hm."Email"))  as "email_hiring_manager"
    , ifnull(u."First_Name" ||' '||u."Last_Name", '-not available-') as "job_recruiter"
    , ifnull(s."First_Name" ||' '||s."Last_Name", '-not available-') as "job_sourcer"
    , j."atsID" as "ats_id"
    , row_number() over (partition by j."atsID" order by left(j."Created_Date",10) desc) as rown
    , ats."name" as "ats_name"
    , j."campaign_inREPLY" as "email_campaign_id"
    , cat."name" as "job_category"
    , scat."name" as "job_subcategory"
    , j."Location_address" as "job_location"
    , try_to_boolean(j."external_recruiter") as "is_external_recruiter"
    ,CASE WHEN j."test" = 'True' THEN TRUE
         WHEN j."test" = 'False' THEN FALSE
         ELSE NULL END AS "test"
    ,CASE WHEN j."executive_search" = 'True' THEN TRUE
         WHEN j."executive_search" = 'False' THEN FALSE
         ELSE NULL END AS "executive_search"
from "bubble_Jobs" as j
left join "bubble_Company" as c on c."bubbleinternal_id"=j."Company"
left join "bubble_atsOptions" as ats on c."ats"=ats."bubbleinternal_id"
left join "bubble_HiringManager" as hm on j."HiringManager"=hm."bubbleinternal_id"
left join "bubble_User" as u on j."recruiter_responsible"=u."bubbleinternal_id"
left join "bubble_User" as s on j."sourcer_responsible"=s."bubbleinternal_id"
left join "bubble_job_subcategory" as scat on j."sub_category"=scat."bubbleinternal_id"
left join "bubble_job_category" as cat on scat."Job_category"=cat."bubbleinternal_id"
;

create or replace table "final_job" as 
select 
            "job_id"
          , "client_id" -- FK
          , to_date("date_created") as "date_created"
          , NULL::DATE as "date_first_hired"
          , NULL::DATE as "date_first_hired_contacted"
          , "job_title"
          , "job_category"
          , "job_subcategory"
          , "job_location"
          , "user_hiring_manager"
          , "email_hiring_manager"
          , "job_recruiter"
          , "job_sourcer"
          , iff("ats_id"<>'' and rown=1,"ats_id", NULL)  as "job_ats_id"
          , "is_job_archived"
          , "is_external_recruiter"
					, "test"
					, "executive_search"
from "tmp_job"
;

insert into "final_job" ("job_id", "client_id", "job_title") values ('--None--', '--None--', '--None--');

/* 
    USER
*/
create or replace table "final_user" as 

with tmp as (
    select
    	  e."who_event_created_for"
        , r."name" as "role_current"
        , sr."name" as "sub_role_current"
        , e."Created_Date"
        , ROW_NUMBER() OVER (PARTITION BY e."who_event_created_for" ORDER BY to_timestamp(e."Created_Date") desc) AS rown
    from "bubble_Event" as e
    left join "bubble_Roles" as r
    	on e."new_role"=r."bubbleinternal_id"
    left join "bubble_sub_roles" as sr
    	on e."new_sub_role"=sr."bubbleinternal_id"
    where 1=1
    	and e."event_type"='1642420714568x807043530709183200' -- Change positions
        and e."new_role"<>''   
)
select
	    u."bubbleinternal_id" as "user_id"
    , concat(u."First_Name", ' ', u."Last_Name") as "user_name"
    , u."authentication_email_email" as "user_email"
    , u."Employee_number" as "employee_number"
    , tmp."role_current"
    , tmp."sub_role_current"
from "bubble_User" as u
left join tmp
	on tmp."who_event_created_for"=u."bubbleinternal_id"
    and tmp.rown=1 -- latest roles
;

/* 
    Client
*/
create or replace table "final_client" as 
select distinct
     "client_id"
    ,"client_name"
    ,"is_client_archived"
  	, CASE WHEN "test" = 'True' THEN TRUE
       WHEN "test" = 'False' THEN FALSE
       ELSE NULL END AS "test"
from "tmp_job"
where "client_name" is not NULL
;

insert into "final_client" ("client_id","client_name") values ('--None--', '--None--');

/*
	JOB_GOAL
*/
create or replace table "final_job_goals" as
select 
    "bubbleinternal_id" as "goal_id"
    , "Job" as "job_id"
    , "Goal_number" as "goal_number"
    , try_to_timestamp("Created_Date")::DATE as "date_created"
    , try_to_timestamp("Modified_Date")::DATE as "date_modified"
    , try_to_timestamp(replace(get(TRY_PARSE_JSON("date_range"), 0),'"'))::DATE as "date_range_from"
    , try_to_timestamp(replace(get(TRY_PARSE_JSON("date_range"), 1),'"'))::DATE as "date_range_to"
    
from "bubble_Goals"
where "date_range_from">='2021-07-11'
;

/* 
    TALENT
*/
create or replace table "final_talent_bubble" as 

select
    t."bubbleinternal_id" as "talent_id"
  , left(t."Created_Date",10) as "date_created"
  , try_to_timestamp(t."Created_Date") as "timestamp_created"
  , t."Full_name" as "full_name"
  , t."companyName" as "current_company"
  , t."currentTitle" as "current_title"
  , e."email" as "main_email"
  , iff(left(t."linkedin",4)='http', trim(lower(t."linkedin")), 
        iff(trim(lower(t."linkedin"))='' or trim(lower(t."linkedin"))='undefined', NULL,
        'https://' || trim(lower(t."linkedin")))) as "linkedin_link"
  , t."LinkedinMainID"
  , t."linkedin_nick"
  , FALSE as "is_talent_duplicated"
  , ''::VARCHAR as "duplicates"

  -- location
  , t."location_address" as "location"
  , "talent_locations_processed"."country" as "location_country"
  , "talent_locations_processed"."city" as "location_city"
  
from "bubble_Talent" as t
left join "talent_locations_processed" 
    on t."location_address" = "talent_locations_processed"."query"
left join (select *, row_number() over (partition by "talent" order by try_to_timestamp("Created_Date") desc) as rown from "bubble_Emails") as e 
    on t."bubbleinternal_id"=e."talent" 
    and e.rown=1
;

-- Linkedin link
update "final_talent_bubble" as b
set b."is_talent_duplicated" = (select iff(count(x."talent_id")>0, TRUE, FALSE)
                                from "final_talent_bubble" as x
                                where 1=1
                                    and x."talent_id"<>b."talent_id"
                                    and x."timestamp_created"<=b."timestamp_created"
                                    and x."linkedin_link"=b."linkedin_link"
                                ) where b."linkedin_link" is not NULL and b."is_talent_duplicated" = FALSE;

update "final_talent_bubble" as b
set b."duplicates" = b."duplicates"
                        || (select iff(count(x."talent_id")>0, 'link -- '||listagg(x."talent_id",',') within group (order by x."talent_id"), '')
                                from "final_talent_bubble" as x
                                where 1=1
                                    and x."talent_id"<>b."talent_id"
                                    and x."timestamp_created"<=b."timestamp_created"
                                    and x."linkedin_link"=b."linkedin_link"
                                ) where b."linkedin_link" is not NULL --and b."is_talent_duplicated" = FALSE
                                ;

-- Linkedin MainID
update "final_talent_bubble" as b
set b."is_talent_duplicated" = (select iff(count(x."talent_id")>0, TRUE, FALSE)
                                from "final_talent_bubble" as x
                                where 1=1
                                    and x."talent_id"<>b."talent_id"
                                    and x."timestamp_created"<=b."timestamp_created"
                                    and x."LinkedinMainID" = b."LinkedinMainID"
                                ) where b."LinkedinMainID" <>'' and b."is_talent_duplicated" = FALSE;

update "final_talent_bubble" as b
set b."duplicates" = b."duplicates"
                        || (select iff(count(x."talent_id")>0, '; LinkedinMainID -- '||listagg(x."talent_id",',') within group (order by x."talent_id"), '')
                                from "final_talent_bubble" as x
                                where 1=1
                                    and x."talent_id"<>b."talent_id"
                                    and x."timestamp_created"<=b."timestamp_created"
                                    and x."LinkedinMainID" = b."LinkedinMainID"
                                ) where b."LinkedinMainID" <>'' --and b."is_talent_duplicated" = FALSE
                                ;

-- Linkedin Nick
update "final_talent_bubble" as b
set b."is_talent_duplicated" = (select iff(count(x."talent_id")>0, TRUE, FALSE)
                                from "final_talent_bubble" as x
                                where 1=1
                                    and x."talent_id"<>b."talent_id"
                                    and x."timestamp_created"<=b."timestamp_created"
                                    and x."linkedin_nick"=b."linkedin_nick"
                                ) where b."linkedin_nick" <>'' and b."is_talent_duplicated" = FALSE;

update "final_talent_bubble" as b
set b."duplicates" = b."duplicates"
                        || (select iff(count(x."talent_id")>0, '; Linkedin nick -- '||listagg(x."talent_id",',') within group (order by x."talent_id"), '')
                            from "final_talent_bubble" as x
                                where 1=1
                                    and x."talent_id"<>b."talent_id"
                                    and x."timestamp_created"<=b."timestamp_created"
                                    and x."linkedin_nick"=b."linkedin_nick"
                                ) where b."linkedin_nick" <>'' --and b."is_talent_duplicated" = FALSE
                                ;

-- main Email                               
update "final_talent_bubble" as b
set b."is_talent_duplicated" = (select iff(count(x."talent_id")>0, TRUE, FALSE)
                                from "final_talent_bubble" as x
                                where 1=1
                                    and x."talent_id"<>b."talent_id"
                                    and x."timestamp_created"<=b."timestamp_created"
                                    and x."main_email"=b."main_email"
                                )where b."main_email" <>'' and b."is_talent_duplicated" = FALSE;

update "final_talent_bubble" as b
set b."duplicates" = b."duplicates"
                        || (select iff(count(x."talent_id")>0, '; Email -- '||listagg(x."talent_id",',') within group (order by x."talent_id"), '')
                            from "final_talent_bubble" as x
                                where 1=1
                                    and x."talent_id"<>b."talent_id"
                                    and x."timestamp_created"<=b."timestamp_created"
                                    and x."main_email"=b."main_email"
                                )where b."main_email" <>'' --and b."is_talent_duplicated" = FALSE
                                ;

update "final_talent_bubble" as b
set b."duplicates" = trim(b."duplicates",'; ');

/* 
    TALENT_EMAIL
*/
CREATE OR REPLACE TABLE "final_email" as
select 
      m."bubbleinternal_id" as "email_id"
    , m."talent" as "talent_id"
    , left(m."Created_Date",10) as "date_created"
    , m."email"
    --, m."Workflow" as "workflow"
    , iff(m.email_order>1 and m."email"<>'', TRUE, FALSE) as "is_email_duplicated"
from (select *, row_number() over (partition by "email" order by try_to_timestamp("Created_Date") desc) as email_order
from "bubble_Emails") as m;

/* 
    TALENT_POSITION
*/
CREATE OR REPLACE TABLE "final_talent_position" as
select 
      p."Talent" as "talent_id"
    , p."Company" as "employer_id"
    , p."Job_title" as "position_job_title"
    , try_to_date(left(p."Worked_from",10)) as "position_worked_from"
    , try_to_date(left(p."Worked_to",10)) as "position_worked_to"
    , row_number() over (partition by p."Talent" order by try_to_date(left(p."Worked_from",10)) desc) as "position_order_desc"
from "bubble_Positions" p
where 1=1
    and p."Talent"<>''
    and p."Worked_from"<>''
order by "talent_id", "position_worked_from" desc;

/* 
    TALENT_EMPLOYER
*/
CREATE OR REPLACE TABLE "final_talent_employer" as
WITH bd as (
  select 
    *, 
    row_number() over (partition by "company" order by try_to_timestamp("Created_Date") DESC)  as rown
  from "bubble_bd_crunchbase"
)
select 
    c."employer_id", 
    cd."Name" as "employer_name", 
    bd."Company_Type" as "employer_type",
    bd."Organization_Name" as "employer_org_name",
    bd."Estimated_Revenue_Range" as "revenue_range",
    bd."Funding_Status" as "funding_status",
    bd."Last_Equity_Funding_Amount_Currency_in_USD" as "funding_last_equity_usd",
    bd."Last_Funding_Amount_Currency_in_USD" as "funding_last_usd",
    bd."Total_Funding_Amount_Currency_in_USD" as "funding_total_usd",
    try_to_date(left(bd."Last_Funding_Date",10)) as "date_last_funding",
    bd."Number_of_Funding_Rounds" as "funding_rounds_count",
    bd."Website" as "web_url",
    bd."LinkedIn" as "linkedin_url",
    bd."Organization_Name_URL" as "crunchbase_url"
   
from (select distinct "employer_id" from "final_talent_position") as c
left join "bubble_Company" as cd 
    on c."employer_id"=cd."bubbleinternal_id"
left join bd 
    on c."employer_id"=bd."company"
    and bd.rown=1 -- removing duplicities
where c."employer_id"<>''
;

/* 
    EVENT
*/
create or replace table "final_event" as 
select 
	e."bubbleinternal_id" as "event_id",
    iff(e."Candidate"='', NULL, e."Candidate") as "candidate_id", 
    coalesce(iff(e."talent"='', NULL, e."talent") , c."Talent") as "talent_id",
    coalesce(iff(e."job"='', NULL ,e."job"), iff(c."Job"='', '--None--',c."Job"))  as "job_id",
    e."date_created_2" as "date_created", 
    iff(e.event_order>1, TRUE, FALSE) as "is_event_duplicated",
    try_to_boolean("external_recruiter") as "is_external_recruiter",
    
    et."name" as "event_type",
    s."stageName" as "moved_to_stage",
    st."stage_type_name" as "moved_to_stageType",
    
    coalesce(u1."full_name",u1."bubbleinternal_id") as "who_created_event", -- TS - talent sourcer
    NULL as "who_created_event_first", -- TS - first talent sourcer
    coalesce(u2."full_name",u2."bubbleinternal_id")  as "who_event_created_for", -- TA - talent acqusition
    e."who_event_created_for" as "who_event_created_for_id",
    
    -- email automation
    flow."Name" as "automation_flow_name",
    step."Type" as "automation_step_type",
    step."order_number" as "automation_step_order",
    CASE 
        WHEN step."bubbleinternal_id" is NULL THEN NULL
        ELSE CONCAT(--ifnull(pcon."type", '?'),
                    --' - ', 
                    --ifnull(sub."Name", '?'), 
                    --' - ', 
                    step."order_number",
                    ' - ', 
                    step."Type", 
                    ' - ', 
                    et."name"
                    --,' - ',
                    --e."Automation_step" 
        )
    END as "automation_step_name",
    ifnull(sub."Name", '?') as "automation_step_subcon",
    ifnull(pcon."type", '?') as "automation_step_con",
    NULL::BOOLEAN as "automation_is_message_read",
    NULL::BOOLEAN as "automation_is_message_replied",
    NULL::VARCHAR as "automation_message_version_id"
    
    -- tmp
    , e."duxsoupMessage"
    , e."Nylas_email"
    
    ,e."Automation_flow"
    ,e."Automation_step"
    
    ,IFF(try_to_boolean(e."AI")=TRUE, TRUE, NULL) as "is_event_createdby_ai"

    ,e."Ai_Search" as "ai_rearch_id"
    ,e."not_fit"
    ,e."not_fit_reason"
         
from (select 
        x.*, 
        coalesce(try_to_timestamp(x."ats_creation_time"), try_to_timestamp(x."Created_Date")) as "date_created_2", -- ats_creation_time - user changes
        row_number() over (partition by x."talent",x."job", x."event_type", "date_created_2"::DATE order by "date_created_2" asc) as event_order
      from "bubble_Event" as x) as e
left join "bubble_Candidate" as c on e."Candidate" = c."bubbleinternal_id"
left join "bubble_EventType" as et on e."event_type"=et."bubbleinternal_id"
left join "bubble_Stages" as s on e."moved_to_stage"=s."bubbleinternal_id"
left join "bubble_stagesType" as st on s."stagesType"=st."bubbleinternal_id"
left join "bubble_User" as u1 on e."who_created_event" = u1."bubbleinternal_id"
left join "bubble_User" as u2 on e."who_event_created_for" = u2."bubbleinternal_id"
left join "bubble_Automationflow" as flow on e."Automation_flow"=flow."bubbleinternal_id"
left join "bubble_Automationstep" as step on e."Automation_step"=step."bubbleinternal_id"
left join "bubble_Sub_conditional" as sub on step."Sub_conditional"=sub."bubbleinternal_id"
left join "bubble_Conditional" as con on sub."Conditional"=con."bubbleinternal_id"
left join "bubble_Conditional" as pcon on sub."parent_conditional"=pcon."bubbleinternal_id"
where 1=1
    --and NOT ("talent_id" is NULL or "candidate_id" is NULL)
    and "Content" <> 'FrantisekDelete' -- duplicated events excluded
		and e."archived"<>'True'
;

--who_created_event_first
update "final_event" as e
set e."who_created_event_first" = x."who_created_event"
                                    from 
                                    (
                                        select
                                        	  t."candidate_id"
                                        	, t."who_created_event"
                                            , row_number() over (partition by t."candidate_id" order by t."date_created" asc) as rown
                                        from "final_event" as t
                                        where t."who_created_event"<>'' and t."candidate_id"<>''
                                    ) as x 
                                    where 1=1
                                    	and x.rown=1 
                                        and e."candidate_id"=x."candidate_id"
                                    ;

update "final_event" as e
set e."who_created_event_first" = x."who_created_event"
                                    from 
                                    (
                                        select
                                        	  t."talent_id"
                                        	, t."who_created_event"
                                            , row_number() over (partition by t."talent_id" order by t."date_created" asc) as rown
                                        from "final_event" as t
                                        where t."who_created_event"<>'' and t."talent_id"<>''
                                    ) as x 
                                    where 1=1
                                    	and x.rown=1 
                                        and e."who_created_event_first" is NULL
                                        and e."talent_id"=x."talent_id"
                                    ;

-- LN duxsoup version 
update "final_event" as e 
set e."automation_message_version_id" = m."version"
from "bubble_duxsoup_messages" as m 
where e."duxsoupMessage"=m."bubbleinternal_id" AND e."duxsoupMessage"<>'';

-- Email Nylas version + read true/false
update "final_event" as e 
set 
    e."automation_message_version_id" = m."version",
    e."automation_is_message_read" = IFF(m."Read" IN ('yes','true', 'True', '1') 
                                         and e."event_type" IN ('Email Sent'), TRUE, NULL)
    from "bubble_Nylas_Email_message" as m 
    where e."Nylas_email"=m."bubbleinternal_id" AND e."Nylas_email"<>'';

-- email
update "final_event" as e
set e."automation_is_message_replied" = (select IFF(count(x."event_id") > 0, TRUE, NULL)
                                         from "final_event" as x
                                         where 1=1
                                            and x."candidate_id"=e."candidate_id"
                                            and x."Automation_flow"=e."Automation_flow"
                                            and x."Automation_step"=e."Automation_step"
                                            and x."event_type" IN ('Email Replied')
                                         ) where e."event_type" IN ('Email Sent');

update "final_event" as e
set e."automation_is_message_read" = (select IFF(count(x."event_id") > 0, TRUE, NULL)
                                         from "final_event" as x
                                         where 1=1
                                            and x."candidate_id"=e."candidate_id"
                                            and x."Automation_flow"=e."Automation_flow"
                                            and x."Automation_step"=e."Automation_step"
                                            and x."event_type" IN ('Email Read')
                                         ) where e."event_type" IN ('Email Sent');

update "final_event" as e
set e."automation_is_message_read" = TRUE where e."automation_is_message_replied"=TRUE and e."event_type" IN ('Email Sent');

-- LN connection
update "final_event" as e
set e."automation_is_message_read" = (select IFF(count(x."event_id") > 0, TRUE, NULL)
                                         from "final_event" as x
                                         where 1=1
                                            and x."candidate_id"=e."candidate_id"
                                            and x."Automation_flow"=e."Automation_flow"
                                            and x."Automation_step"=e."Automation_step"
                                            and x."event_type" IN ('Linkedin Connected')
                                         ) where e."event_type" IN ('Linkedin Sent Contact');

update "final_event" as e
set e."automation_is_message_replied" = (select IFF(count(x."event_id") > 0, TRUE, NULL)
                                         from "final_event" as x
                                         where 1=1
                                            and x."candidate_id"=e."candidate_id"
                                            and x."Automation_flow"=e."Automation_flow"
                                            and x."Automation_step"=e."Automation_step"
                                            and x."event_type" IN ('Linkedin Responded')
                                         ) where e."event_type" IN ('Linkedin Sent Contact');

update "final_event" as e
set e."automation_is_message_read" = TRUE where e."automation_is_message_replied"=TRUE and e."event_type" IN ('Linkedin Sent Contact');

-- LN message
update "final_event" as e
set e."automation_is_message_replied" = (select IFF(count(x."event_id") > 0, TRUE, NULL)
                                         from "final_event" as x
                                         where 1=1
                                            and x."candidate_id"=e."candidate_id"
                                            and x."Automation_flow"=e."Automation_flow"
                                            and x."Automation_step"=e."Automation_step"
                                            and x."event_type" IN ('Linkedin Responded')
                                         ) where e."event_type" IN ('Message sent');

-- LM Inmail
update "final_event" as e
set e."automation_is_message_replied" = (select IFF(count(x."event_id") > 0, TRUE, NULL)
                                         from "final_event" as x
                                         where 1=1
                                            and x."candidate_id"=e."candidate_id"
                                            and x."Automation_flow"=e."Automation_flow"
                                            and x."Automation_step"=e."Automation_step"
                                            and x."event_type" IN ('Linkedin inMail received')
                                         ) where e."event_type" IN ('Linkedin inMail sent');

alter table "final_event" drop column "duxsoupMessage", "Nylas_email", "Automation_flow", "Automation_step";

/* 
    CANDIDATE
*/
create or replace table "final_candidate_bubble" as 
select     
      c."bubbleinternal_id" as "candidate_id"
    , c."Job" as "job_id"
    , c."Talent" as "talent_id"
    , ifnull(u."First_Name" ||' '||u."Last_Name", '-not available-') as "candidate_sourcer" -- current sourcer
    
    , r."name" as "reason_not_interested"
    , c."hired_salary_euro" as "hired_salary_eur"
    , c."hired_salary" as "hired_salary"
    , iff(sal."bubbleinternal_id" is NULL, c."hired_currency", sal."Name") as "hired_salary_currency"

    
    , iff(c.candidate_order=1 or (c."linkedin"='' or c."linkedin" is NULL), FALSE, TRUE) as "is_candidate_duplicated"
    , try_to_boolean(c."disqualified") as "is_candidate_disqualified" 
	  , iff(c."archived"='True', TRUE, FALSE) as "is_candidate_archived"
    , FALSE::BOOLEAN as "is_candidate_createdby_ai"
    
    -- ATS external ID
    , c."atsID" as "candidate_ats_id"
    , try_to_timestamp(c."Created_Date")::DATE as "date_created"
    
    , s."stageName" as "stage_current"
    , st."stage_type_name" as "stage_current_type"
    
    , ss."Name" as "source"
    
from (select 
      z.*, t."linkedin", row_number() over (partition by z."Job", t."linkedin" order by try_to_timestamp(z."Created_Date") asc) as candidate_order -- duplication based on lnkd link+job
      from "bubble_Candidate" as z
      left join "bubble_Talent" as t on z."Talent" = t."bubbleinternal_id"
     ) as c
left join "bubble_Stages" as s on c."Stage" = s."bubbleinternal_id"
left join "bubble_stagesType" as st on s."stagesType" = st."bubbleinternal_id"
left join "bubble_ReasonNotInterested" as r on c."reason_not_interested" = r."bubbleinternal_id"
left join "bubble_User" as u on c."sourcer"=u."bubbleinternal_id"
left join "bubble_Sourced_source" as ss on c."Sourcedsource"=ss."bubbleinternal_id"
left join "bubble_Jobs" as j on c."Job"=j."bubbleinternal_id"
left join "bubble_Salary_currency" as sal on c."hired_currency"=sal."bubbleinternal_id"
;

update "final_candidate_bubble" as c
set "is_candidate_createdby_ai" = TRUE where c."candidate_id" IN (
    select distinct e."Candidate" 
    from "bubble_Event" as e 
    where e."AI"='True'
        and e."event_type"='1542180373448x729603979969397200' -- Candidate created    
);

/* 
    CANDIDATE_STAGE
*/

create or replace table "final_candidate_stage_bubble" as 
select 
  
  -- TMP
    c."talent_id"
  , c."candidate_ats_id"
  
  , c."candidate_id"
  , CASE
      WHEN "stage_current_type" like 'Offer' THEN 4
      WHEN "stage_current_type" like 'Hired' THEN 5
      WHEN "stage_current" like 'Referred'
            OR "stage_current" like 'Sourced%'
            OR "stage_current" like 'Downloaded'
            OR "stage_current" like 'Prospects'
            OR "stage_current" like 'Applied' THEN 0
      WHEN "stage_current_type" IN ('Contacted', 'Positive Response') THEN 1
      WHEN "stage_current_type" like 'Recruiter Screen' THEN 2
      WHEN lower("stage_current_type") like '%interview%'
           OR "stage_current_type" like 'Reference Check' 
           OR "stage_current_type" like 'Offsite' 
           OR lower("stage_current") like '%interview%'
           THEN 3
      ELSE 0
  END as "stage_current_num",
  "stage_current_type",
  "stage_current",
  
  -- dates  
  c."date_created", 
  NULL::DATE as "date_lnkdin_viewed",
  NULL::DATE as "date_contacted",
  NULL::DATE as "date_screen",
  NULL::DATE as "date_screen_actual",
  NULL::DATE as "date_interview",
  NULL::DATE as "date_offer",
  NULL::DATE as "date_hired",
  
  -- automation flow details
  NULL::INT as "automation_emails",
  NULL::INT as "automation_connections",
  NULL::INT as "automation_inmails",
  NULL::INT as "automation_messages"
from "final_candidate_bubble" as c;

update "final_candidate_stage_bubble" as c
set c."date_lnkdin_viewed" = (select max(t."date_created")::DATE 
                            from "final_event" as t
                           where 1=1
                            and c."talent_id"=t."talent_id"
                            and t."event_type"='Linkedin Visited Profile'
                            and t."date_created" <= c."date_created"
                         );

update "final_candidate_stage_bubble" as c
set c."date_contacted" = (select max(t."date_created")::DATE
                            from "final_event" as t
                           where 1=1
                            and c."candidate_id"=t."candidate_id"
                            and t."moved_to_stageType"='Contacted'
                            and not (t."moved_to_stage"='Responded') -- Responded is not Contacted
                            and c."stage_current_num">=1
                         );

update "final_candidate_stage_bubble" as c
set c."date_screen_actual" =  (select max(t."date_created")::DATE
                                  from "final_event" as t
                                 where 1=1
                                  and c."candidate_id"=t."candidate_id"
                                  and t."event_type"='Evaluation'
                              );

update "final_candidate_stage_bubble" as c
set c."date_screen_actual" =  (select 
                                        max(try_to_timestamp(n."Created_Date"))::DATE
                                    from "bubble_recruiter_screeen_notes" as n
                                    where n."candidate" = c."candidate_id"
                              )
                              where c."date_screen_actual" is NULL;

update "final_candidate_stage_bubble" as c
set c."date_screen" = (select max(t."date_created")::DATE
                                  from "final_event" as t
                                 where 1=1
                                  and c."candidate_id"=t."candidate_id"
                                  and t."moved_to_stageType"='Recruiter Screen'
                                  and c."stage_current_num">=2
                      )
                      ;

update "final_candidate_stage_bubble" as c
set c."date_interview" = (select max(t."date_created") 
                            from "final_event" as t
                           where 1=1
                            and c."candidate_id"=t."candidate_id"
                           and c."stage_current_num">=3
                           and (t."moved_to_stageType" IN ('Offsite', 'Interview') 
                                 or lower(t."moved_to_stage") LIKE '%interview%')
                           );

update "final_candidate_stage_bubble" as c
set c."date_offer" = (select max(t."date_created") 
                            from "final_event" as t
                           where 1=1
                            and c."candidate_id"=t."candidate_id"
                            and t."moved_to_stageType"='Offer'
                            and c."stage_current_num">=4
                           );

update "final_candidate_stage_bubble" as c
set c."date_hired" = (select max(t."date_created") 
                            from "final_event" as t
                           where 1=1
                            and c."candidate_id"=t."candidate_id"
                            and t."moved_to_stageType"='Hired'
                            and c."stage_current_num">=5
                           );

/* Recruitee stages */
update "final_candidate_stage_bubble" as c
set c."date_interview" = (select max(r."dt_created")::DATE 
                            from recruitee_events as r
                           where 1=1
                            and c."candidate_ats_id"=r."talent_id"
                            and lower(r."stage_to_category")='interview'
                           ) where c."date_interview" is NULL;

update "final_candidate_stage_bubble" as c
set c."date_offer" = (select max(r."dt_created")::DATE 
                            from recruitee_events as r
                           where 1=1
                            and c."candidate_ats_id"=r."talent_id"
                            and lower(r."stage_to_category")='offer'
                           ) where c."date_offer" is NULL;

update "final_candidate_stage_bubble" as c
set c."date_hired" = (select max(r."dt_created")::DATE 
                            from recruitee_events as r
                           where 1=1
                            and c."candidate_ats_id"=r."talent_id"
                            and lower(r."stage_to_category")='hire'
                           ) where c."date_hired" is NULL;

--fixing dates
update "final_candidate_stage_bubble" as c set c."date_offer" = iff(c."date_hired" is not NULL and c."date_offer" is NULL, c."date_hired", c."date_offer");

update "final_candidate_stage_bubble" as c set c."date_interview" = iff(c."date_offer" is not NULL and c."date_interview" is NULL, c."date_offer", c."date_interview");

update "final_candidate_stage_bubble" as c set c."date_screen_actual" = iff(c."date_interview" is not NULL and c."date_screen_actual" is NULL, c."date_interview", c."date_screen_actual");

update "final_candidate_stage_bubble" as c set c."date_screen" = iff(c."date_screen_actual" is not NULL and c."date_screen" is NULL, c."date_screen_actual", c."date_screen");

update "final_candidate_stage_bubble" as c set c."date_contacted"  = iff(c."date_screen" is not NULL and c."date_contacted" is NULL, c."date_screen", c."date_contacted");

update "final_candidate_stage_bubble" as c set c."date_lnkdin_viewed"  = iff(	(c."date_contacted" is not NULL and c."date_lnkdin_viewed" is NULL) OR c."date_lnkdin_viewed">c."date_contacted", c."date_contacted", c."date_lnkdin_viewed");

/* linkedin events - does not have to be seen on lnkdin before contacted
    update "final_candidate_stage" as c set c."date_lnkdin_viewed"  = iff(c."date_contacted" is not NULL and c."date_lnkdin_viewed" is NULL, c."date_contacted", c."date_lnkdin_viewed"); 
 */
 
-- automation details
update "final_candidate_stage_bubble" as c
set c."automation_emails" = zeroifnull( select count(t."event_id") 
                                from "final_event" as t
                                where 1=1
                                    and c."candidate_id"=t."candidate_id"
                                    and t."automation_step_order" is not NULL
                                    and t."automation_flow_name" is not NULL
                                    and t."event_type" IN ('Email Sent')
                           );

update "final_candidate_stage_bubble" as c
set c."automation_connections" = zeroifnull( select count(t."event_id") 
                                from "final_event" as t
                                where 1=1
                                    and c."candidate_id"=t."candidate_id"
                                    and t."automation_step_order" is not NULL
                                    and t."automation_flow_name" is not NULL
                                    and t."event_type" IN ('Linkedin Sent Contact')
                           );

update "final_candidate_stage_bubble" as c
set c."automation_inmails" = zeroifnull( select count(t."event_id") 
                                from "final_event" as t
                                where 1=1
                                    and c."candidate_id"=t."candidate_id"
                                    and t."automation_step_order" is not NULL
                                    and t."automation_flow_name" is not NULL
                                    and t."event_type" IN ('Linkedin inMail sent')
                           );

update "final_candidate_stage_bubble" as c
set c."automation_messages" = zeroifnull( select count(t."event_id") 
                                from "final_event" as t
                                where 1=1
                                    and c."candidate_id"=t."candidate_id"
                                    and t."automation_step_order" is not NULL
                                    and t."automation_flow_name" is not NULL
                                    and t."event_type" IN ('Message sent')
                           );

--drop columns
alter table "final_candidate_stage_bubble" drop column "talent_id", "stage_current_num", "candidate_ats_id";

alter table "final_candidate_bubble" drop column "date_created","stage_current", "stage_current_type";

/* 
    SCREEN_NOTE
    tables
*/                           
CREATE OR REPLACE TABLE "final_screen" as
select
      n."bubbleinternal_id" as "screen_id"
    , n."candidate" as "candidate_id"
    , left(n."Created_Date",10) as "date_created"
    
    , n."Current_salary" as "current_salary"
    , sc."Name" as "current_salary_currency"
    
    , n."desired_salary"
    , scc."Name" as "desired_salary_currency"
    
    , n."Location" as "location"
    , n."rating"
    , ifnull(r."First_Name" ||' '||r."Last_Name", '-not available-') as "user_recruiter"
    , rd."name" as "relocation"
    , st."Name" as "salary_type"
    , p."name" as "start_date"
    , v."name" as "visa"
    
    
from "bubble_recruiter_screeen_notes" as n
left join "bubble_User" as r on n."recruiter"=r."bubbleinternal_id"
left join "bubble_recruiter_screen_relocation_dropdown" as rd on n."relocation_dropdown" = rd."bubbleinternal_id"
left join "bubble_SalaryType" as st on n."Salary_type" = st."bubbleinternal_id"
left join "bubble_Salary_currency" as sc on n."Current_salary_currency" = sc."bubbleinternal_id"
left join "bubble_Salary_currency" as scc on n."Desired_salary_currency" = scc."bubbleinternal_id"
left join "bubble_recruiter_screen_notice_period_dropdown" as p on n."start_date_dropdown" = p."bubbleinternal_id"
left join "bubble_recruiter_screen_visa_dropdown" as v on n."visa_dropdown" = v."bubbleinternal_id";

CREATE OR REPLACE TABLE "final_screen_techstack" as
select x."screen_id", ts."Name" as "techstack_name", tst."Name" as "techstack_type" from 
(select "bubbleinternal_id" as "screen_id", replace(value, '"', '') as "techstack_id"
from "bubble_recruiter_screeen_notes",
lateral flatten(input => parse_json("tech_stack"), recursive => true) f1) as x
left join "bubble_TechStack" as ts on x."techstack_id" = ts."bubbleinternal_id"
left join "bubble_TechStackType" as tst on ts."TechStackType" = tst."bubbleinternal_id";

CREATE OR REPLACE TABLE "final_screen_lang" as
select x."screen_id", l."Name" as "lang_name", ll."Name" as "lang_level" from 
(select "bubbleinternal_id" as "screen_id", replace(value, '"', '') as "language_talent_id"
from "bubble_recruiter_screeen_notes",
lateral flatten(input => parse_json("Languages"), recursive => true) f1) as x
left join "bubble_Language_talent" as lt on x."language_talent_id" = lt."bubbleinternal_id"
left join "bubble_Languages_levels" as ll on lt."Language_level" = ll."bubbleinternal_id"
left join "bubble_Languages" as l on lt."Language_name" = l."bubbleinternal_id";

