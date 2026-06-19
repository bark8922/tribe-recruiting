-- Transformation: [PROD] Data preparation V2
-- Block: code
-- Code: part 3 - final tables
-- Extracted from Keboola on 2026-03-30

/* 
    final TALENT 
*/
create or replace table "final_talent_all" as 
select 
      "talent_id"
    , "date_created" 
    , "full_name"
    , "current_company"
    , "current_title"
    , "main_email"
    , "linkedin_link"
    , "is_talent_duplicated"
    , "duplicates"
    , "location"
    , "location_country"
  	, "location_city"
from "final_talent_bubble"

UNION 
select 
      "talent_id"
    , "date_created" 
    , "full_name"
    , "current_company"
    , "current_title"
    , "main_email"
    , "linkedin_link"
    , "is_talent_duplicated"
    , NULL as "duplicates"
    , NULL as "location"
    , NULL as "location_country"
  	, NULL as "location_city"
from "final_talent_recruitee"

;

/* 
    final CANDIDATE
*/
create or replace table "final_candidate_all" as 
select 
    "candidate_id"
    ,"job_id"
    ,"talent_id"
    ,"reason_not_interested"
    ,"hired_salary_eur"
    ,"hired_salary"
    ,"hired_salary_currency"
    ,"source"
    ,"is_candidate_duplicated"
    ,"is_candidate_disqualified"
		,"is_candidate_archived"
    , NULL::BOOLEAN as "is_candidate_reacted"
    , "candidate_sourcer"
    , "is_candidate_createdby_ai"
from "final_candidate_bubble"

UNION 
select 
    "candidate_id"
    ,"job_id"
    ,"talent_id"
    ,"reason_not_interested"
    , NULL as "hired_salary_eur"
    , NULL as "hired_salary"
    , NULL as "hired_salary_currency"
    , "source"
    ,"is_candidate_duplicated"
    ,"is_candidate_disqualified"
	  , FALSE::BOOLEAN as "is_candidate_archived"
    , NULL::BOOLEAN as "is_candidate_reacted"
    , "candidate_sourcer"
    , FALSE::BOOLEAN as "is_candidate_createdby_ai"
from "final_candidate_recruitee";

/* 
    final EVENT
*/
create or replace table "final_event_all" as 
select 
	"event_id",
    "candidate_id", 
    "talent_id",
    "job_id",
    "date_created"::DATE as "date_created", 
    "date_created" as "datetime_created", 
    "is_event_duplicated",
    "is_external_recruiter",
    "event_type",
    "moved_to_stage",
    "moved_to_stageType", 
    "who_created_event",
    "who_created_event_first",
    "who_event_created_for",
    "who_event_created_for_id",
    "automation_flow_name",
    "automation_step_type",
    "automation_step_order",
    "automation_step_name",
    "automation_step_subcon",
    "automation_step_con",
    "automation_is_message_read",
    "automation_is_message_replied",
    "automation_message_version_id",
    "is_event_createdby_ai",
    "ai_rearch_id",
    "not_fit",
    "not_fit_reason"
from "final_event"

UNION
select 
	"event_id",
    "candidate_id", 
    "talent_id",
    "job_id",
    "date_created"::DATE as "date_created", 
    "date_created" as "datetime_created", 
    "is_event_duplicated",
    NULL::BOOLEAN as "is_external_recruiter",
    "event_type",
    "moved_to_stage",
    "moved_to_stageType", 
    "who_created_event",
    "who_created_event" as "who_created_event_first",
    "who_event_created_for",
    NULL as "who_event_created_for_id",
    "automation_flow_name",
    "automation_step_type",
    "automation_step_order",
    NULL as "automation_step_order",
    NULL as "automation_step_subcon",
    NULL as "automation_step_con",
    "automation_is_message_read",
    "automation_is_message_replied",
    "automation_message_version_id",
    NULL as "is_event_createdby_ai",
    NULL as "ai_rearch_id",
    NULL as "not_fit",
    NULL as "not_fit_reason"
from "final_event_recruitee";

/* 
    final CANDIDATE STAGE
*/
create or replace table "final_candidate_stage_tmp" as 
select 
    "candidate_id" 
  , "stage_current_type"
  , "stage_current"
  , "date_created" 
  , "date_lnkdin_viewed"
  , "date_contacted"
  , "date_screen"
  , "date_screen_actual"
  , "date_interview"
  , "date_offer"
  , "date_hired"
  , "automation_emails"
  , "automation_connections"
  , "automation_inmails"
  , "automation_messages"

from "final_candidate_stage_bubble"
UNION
select 
    "candidate_id" 
  , "stage_current_type"
  , "stage_current"
  , "date_created" 
  , "date_lnkdin_viewed"
  , "date_contacted"
  , "date_screen"
  , "date_screen_actual"
  , "date_interview"
  , "date_offer"
  , "date_hired"
  , NULL::INT as "automation_emails"
  , NULL::INT as "automation_connections"
  , NULL::INT as "automation_inmails"
  , NULL::INT as "automation_messages"
from "final_candidate_stage_recruitee";

create or replace table "final_candidate_stage_all" as                             
select
     c."job_id"
    ,cs.*
    ,row_number() over (partition by c."job_id" order by cs."date_hired") as "hired_order"
    ,NULL::INT as "hired_views"
    ,NULL::INT as "hired_contacts"
    ,NULL::INT as "hired_screens"
from "final_candidate_stage_tmp" as cs 
left join "final_candidate_all" as c
    on cs."candidate_id" = c."candidate_id";

update "final_candidate_stage_all"
set "hired_order" = NULL where "date_hired" is NULL;

update "final_candidate_stage_all" as cs
set cs."hired_views" = (select count(distinct x."talent_id")
                        from "final_event_all" as x
                        where 1=1
                            and x."job_id"=cs."job_id"
                            and x."event_type"='Linkedin Visited Profile'
                            and x."date_created" <= cs."date_lnkdin_viewed"
                       ) where cs."hired_order" is not NULL;

update "final_candidate_stage_all" as cs
set cs."hired_contacts" = (select count(x."candidate_id")
                        from "final_candidate_stage_all" as x
                        where 1=1
                            and x."job_id"=cs."job_id"
                            and x."date_contacted" <= cs."date_contacted"
                       ) where cs."hired_order" is not NULL;

update "final_candidate_stage_all" as cs
set cs."hired_screens" = (select count(x."candidate_id")
                        from "final_candidate_stage_all" as x
                        where 1=1
                            and x."job_id"=cs."job_id"
                            and x."date_screen" <= cs."date_screen"
                       ) where cs."hired_order" is not NULL;

alter table "final_candidate_stage_all" drop "job_id";

/* 
    other 
*/
-- updating final_candidate_all
update "final_candidate_all" as c
set "is_candidate_reacted" = CASE 
                                WHEN cs."date_screen" is not NULL THEN TRUE
                                WHEN cs."date_contacted" is not NULL AND c."is_candidate_disqualified"=TRUE THEN TRUE
                                ELSE FALSE
                             END
                            from "final_candidate_stage_all" as cs
                            where c."candidate_id"=cs."candidate_id";

update "final_job" as j
set j."date_first_hired" = (select min(s."date_hired")
                            from "final_candidate_stage_all" as s
                            left join "final_candidate_all" as c 
                                on c."candidate_id"=s."candidate_id"
                            where 1=1
                                and c."job_id"=j."job_id"
                                and s."date_hired" is not NULL
                           );

update "final_job" as j
set j."date_first_hired_contacted" = (select min(s."date_contacted") 
                            from "final_candidate_stage_all" as s
                            left join "final_candidate_all" as c 
                                on c."candidate_id"=s."candidate_id"
                            where 1=1
                                and c."job_id"=j."job_id"
                                and s."date_hired" is not NULL
                           );

