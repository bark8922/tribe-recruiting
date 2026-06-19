-- Transformation: [PROD] Data preparation V2
-- Block: code
-- Code: part 2 - recruitee data
-- Extracted from Keboola on 2026-03-30

create or replace table "final_talent_recruitee" as 
with tmp as (
  select r.*, c."name" as "full_name", c."emails", c."adminapp_url", c."created_at" as "talent_created_at"
  from "recruitee_candidates_placements" as r
  inner join "recruitee_candidates" as c
      on c."id"=r."candidate_id"
      and c."company_id"='41121'
  inner join "final_job" as j 
      on j."job_ats_id"=r."offer_id"
  left join "final_candidate_bubble" as fc
      on fc."candidate_ats_id"=c."id"
  where fc."candidate_ats_id" is NULL
)

select distinct
    'recruitee_'||tmp."candidate_id" as "talent_id"
    , left("talent_created_at",10)::DATE as "date_created" 
    , tmp."full_name"
    , NULL as "current_company"
    , NULL as "current_title"
    , split_part(tmp."emails",'"', 2) as "main_email"
    , tmp."adminapp_url" as "linkedin_link"
    , FALSE as "is_talent_duplicated"
from tmp

;

-- final CANDIDATE table incl. pure Recruitee 
create or replace table "final_candidate_recruitee" as 
with tmp as (
  select r.*, 
    j."job_id" as "bubble_job_id",
    j."job_ats_id", 
    c."name" as "full_name", 
    c."emails", 
    c."adminapp_url", 
    c."created_at" as "talent_created_at", 
    c."source",
    ifnull(a."first_name" ||' '||a."last_name", '-not available-') as "candidate_sourcer"
  from "recruitee_candidates_placements" as r
  left join "recruitee_candidates" as rc
    on r."candidate_id"=rc."id"
  left join "recruitee_admins" as a
    on rc."admin_id"=a."id"
  inner join "recruitee_candidates" as c
      on c."id"=r."candidate_id"
      and c."company_id"='41121'
  inner join "final_job" as j 
      on j."job_ats_id"=r."offer_id"
  left join "final_candidate_bubble" as fc
      on fc."candidate_ats_id"=c."id"
  where fc."candidate_ats_id" is NULL
)

select 
    'recruitee_'||tmp."id" as "candidate_id"
    ,tmp."bubble_job_id" as "job_id"
    ,'recruitee_'||tmp."candidate_id" as "talent_id"
    ,NULL as "reason_not_interested"
    ,FALSE as "is_candidate_duplicated"
    ,iff(tmp."disqualify_reason"<>'', TRUE, FALSE) as "is_candidate_disqualified"
    ,'recruitee_'||"source" as "source"
    , "candidate_sourcer"
    , "candidate_sourcer" as "candidate_sourcer_actual"
from tmp
;

-- CANDIATE Recruitee
create or replace table "final_candidate_stage_recruitee" as 
with tmp as (
  select r.*,s."name", s."category", j."job_ats_id", c."name" as "full_name", c."emails", c."adminapp_url", c."created_at" as "talent_created_at"
  from "recruitee_candidates_placements" as r
  left join "recruitee_offers_stages" s
      on r."stage_id"=s."id"
  inner join "recruitee_candidates" as c
      on c."id"=r."candidate_id"
      and c."company_id"='41121'
  inner join "final_job" as j 
      on j."job_ats_id"=r."offer_id"
  left join "final_candidate_bubble" as fc
      on fc."candidate_ats_id"=c."id"
  where fc."candidate_ats_id" is NULL
)

select 
  -- TMP
    tmp."candidate_id" as "talent_id"
  , tmp."offer_id" as "job_id"
  
  , 'recruitee_'||tmp."id" as "candidate_id" 
  , tmp."name" as "stage_current"
  , tmp."category" as "stage_current_type"
  , left(tmp."created_at",10)::DATE as "date_created" 
  , NULL::DATE as "date_lnkdin_viewed"
  , NULL::DATE as "date_contacted"
  , NULL::DATE as "date_screen"
  , NULL::DATE as "date_screen_actual"
  , NULL::DATE as "date_interview"
  , NULL::DATE as "date_offer"
  , NULL::DATE as "date_hired"
from tmp;

UPDATE "final_candidate_stage_recruitee" as c
set c."date_contacted" = (select min(x."dt_created")::DATE from recruitee_events as x 
                                where 1=1
                                    and c."talent_id"=x."talent_id"
                                    and c."job_id"=x."job_id"
                                    and x."event" IN ('candidate_apply', 'candidate_stage_change', 'candidate_disqualify')
                         );

UPDATE "final_candidate_stage_recruitee" as c
set c."date_screen" = (select min(x."dt_created")::DATE from recruitee_events as x 
                                where 1=1
                                    and c."talent_id"=x."talent_id"
                                    and c."job_id"=x."job_id"
                                    and x."stage_to_category" = 'phone_screen'
                         );

UPDATE "final_candidate_stage_recruitee" as c
set c."date_screen_actual" = (select min(x."dt_created")::DATE from recruitee_events as x 
                                where 1=1
                                    and c."talent_id"=x."talent_id"
                                    --and c."job_id"=x."job_id"
                                    and x."event"='interview_result_add'
                         );

UPDATE "final_candidate_stage_recruitee" as c
set c."date_interview" = (select min(x."dt_created")::DATE from recruitee_events as x 
                                where 1=1
                                    and c."talent_id"=x."talent_id"
                                    and c."job_id"=x."job_id"
                                    and x."stage_to_category" = 'interview'
                         );

UPDATE "final_candidate_stage_recruitee" as c
set c."date_offer" = (select min(x."dt_created")::DATE from recruitee_events as x 
                                where 1=1
                                    and c."talent_id"=x."talent_id"
                                    and c."job_id"=x."job_id"
                                    and x."stage_to_category" = 'offer'
                         );

UPDATE "final_candidate_stage_recruitee" as c
set c."date_hired" = (select min(x."dt_created")::DATE from recruitee_events as x 
                                where 1=1
                                    and c."talent_id"=x."talent_id"
                                    and c."job_id"=x."job_id"
                                    and x."stage_to_category" = 'hire'
                         );

--fixing dates
update "final_candidate_stage_recruitee" as c set c."date_offer" = iff(c."date_hired" is not NULL and c."date_offer" is NULL, c."date_hired", c."date_offer");

update "final_candidate_stage_recruitee" as c set c."date_interview" = iff(c."date_offer" is not NULL and "date_interview" is NULL, c."date_offer", c."date_interview");

update "final_candidate_stage_recruitee" as c set c."date_screen_actual" = iff(c."date_interview" is not NULL and c."date_screen_actual" is NULL, c."date_interview", c."date_screen_actual");

update "final_candidate_stage_recruitee" as c set c."date_screen" = iff(c."date_screen_actual" is not NULL and c."date_screen" is NULL, c."date_screen_actual", c."date_screen");

update "final_candidate_stage_recruitee" as c set c."date_contacted"  = iff(c."date_screen" is not NULL and c."date_contacted" is NULL, c."date_screen", c."date_contacted");

-- EVENT Recruitee
create or replace table "final_event_recruitee" as 
with tmp as (
  select distinct 
    r."id" as "candidate_id_recruitee",
    fc."candidate_id" as "candidate_id_bubble",
    r."candidate_id" as "talent_id_recruitee",
    fc."talent_id" as "talent_id_bubble",
    r."offer_id" as "job_id_recruitee",
    j."job_id" as "job_id_bubble",
    j."job_recruiter" as "recruiter_bubble"
  from "recruitee_candidates_placements" as r
  inner join "recruitee_candidates" as c
      on c."id"=r."candidate_id"
      and c."company_id"='41121'
  inner join "final_job" as j 
      on j."job_ats_id"=r."offer_id"
  left join "final_candidate_bubble" as fc
      on fc."candidate_ats_id"=c."id"
  --where fc."candidate_ats_id" is NULL
)

select
	'recruitee_'||x."activity_id" as "event_id",
    coalesce(tmp."candidate_id_bubble", 'recruitee_'||tmp."candidate_id_recruitee") as "candidate_id", 
    coalesce(tmp."talent_id_bubble", 'recruitee_'||tmp."talent_id_recruitee") as "talent_id",
    coalesce(tmp."job_id_bubble", 'recruitee_'||tmp."job_id_recruitee") as "job_id",
    x."dt_created" as "date_created", 
    FALSE as "is_event_duplicated",
    
    
    x."event" as "event_type",
    x."stage_to" as "moved_to_stage",
    x."stage_to_category" as "moved_to_stageType",
    
    a."first_name"||' '||a."last_name" as "who_created_event",
    tmp."recruiter_bubble"  as  "who_event_created_for",
    
    -- email automation
    NULL::VARCHAR as "automation_flow_name",
    NULL::VARCHAR as "automation_step_type",
    NULL::VARCHAR as "automation_step_order",
    NULL::BOOLEAN as "automation_is_message_read",
    NULL::BOOLEAN as "automation_is_message_replied",
    NULL::VARCHAR as "automation_message_version_id"
    
from tmp
inner join recruitee_events as x 
    on x."talent_id"=tmp."talent_id_recruitee"
    and x."job_id"=tmp."job_id_recruitee"
    and x."event" IN ('candidate_apply', 'candidate_stage_change')
left join "recruitee_admins" as a
    on x."admin_id"=a."id";

