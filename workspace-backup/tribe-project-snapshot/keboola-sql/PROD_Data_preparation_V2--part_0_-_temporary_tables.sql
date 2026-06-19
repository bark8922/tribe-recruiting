-- Transformation: [PROD] Data preparation V2
-- Block: code
-- Code: part 0 - temporary tables
-- Extracted from Keboola on 2026-03-30

create or replace table recruitee_stage as
select 
    "id" as "stage_id"
    , "offers_pk" as "job_id"
    , "name" as "stage_name"
    , CASE 
        WHEN "category"='sourced' OR "category"='referred' OR "category"='apply'
             OR ("category" in ('', 'none') and (lower("name") like 'sourced%' OR lower("name") like 'referred%' OR lower("name") like 'applied%'))
             THEN 'candidate'
        WHEN "category"='phone_screen'
             OR ("category" in ('', 'none') and lower("name") like '%screen%')
             THEN 'phone_screen'
        WHEN "category"='interview'
             OR ("category" in ('', 'none') and (lower("name") like '%home test%' OR lower("name") like 'interview%' OR lower("name") like 'moved to client%')) 
             THEN 'interview'
        WHEN "category"='evaluation'
             OR ("category" in ('', 'none') and lower("name") like 'final interview%')
             THEN 'evaluation'
        WHEN "category"='offer'
             OR ("category" in ('', 'none') and lower("name") like 'offer%') 
             THEN 'offer'
        WHEN "category"='hire'
             OR ("category" in ('', 'none') and lower("name") like 'hired%') 
             THEN 'hire'      
        WHEN "category"='none' -- still no category
             THEN 'candidate'
      END as "stage_category_adjusted"
        
    , CASE 
        WHEN "category"='sourced' OR "category"='referred' OR "category"='apply'
             OR ("category" in ('', 'none') and (lower("name") like 'sourced%' OR lower("name") like 'referred%' OR lower("name") like 'applied%'))
             THEN 0
        WHEN "category"='phone_screen'
             OR ("category" in ('', 'none') and lower("name") like '%screen%')
             THEN 1
        WHEN "category"='interview'
             OR ("category" in ('', 'none') and (lower("name") like '%home test%' OR lower("name") like 'interview%' OR lower("name") like 'moved to client%')) 
             THEN 2
        WHEN "category"='evaluation'
             OR ("category" in ('', 'none') and lower("name") like 'final interview%')
             THEN 3
        WHEN "category"='offer'
             OR ("category" in ('', 'none') and lower("name") like 'offer%') 
             THEN 4
        WHEN "category"='hire'
             OR ("category" in ('', 'none') and lower("name") like 'hired%') 
             THEN 5      
        WHEN "category"='none' -- still no category
             THEN '0'
      END as "stage_position_adjusted"
from "recruitee_offers_stages";

create or replace table recruitee_events as
    select
      a."id" as "activity_id",
      a."admin_id",
      a."candidate_id" as "talent_id",
      a."offer_id" as "job_id",
      a."event",
      try_to_timestamp(a."created_at") as "dt_created",
      
      CASE 
        WHEN a."message_short_html" like '%from the stage <strong>%' -- DE
        THEN SPLIT_PART(SPLIT_PART(a."message_short_html", 'from the stage <strong>', 2), '</strong>', 1) 
        ELSE SPLIT_PART(SPLIT_PART(a."message_short_html", 'aus der Phase <strong>', 2), '</strong>', 1) 
      END as "stage_from",
      
      CASE 
        WHEN a."message_short_html" not like '%Phase <strong>%' 
        THEN    iff(
                    SPLIT_PART(SPLIT_PART(a."message_short_html", '</strong> to <strong>', 2), '</strong>', 1)='',
                    SPLIT_PART(SPLIT_PART(a."message_short_html", 'to the stage <strong>', 2), '</strong>', 1),
                    SPLIT_PART(SPLIT_PART(a."message_short_html", '</strong> to <strong>', 2), '</strong>', 1)
                ) 
        ELSE    iff(
                    SPLIT_PART(SPLIT_PART(a."message_short_html", '</strong> nach <strong>', 2), '</strong>', 1)='',
                    SPLIT_PART(SPLIT_PART(a."message_short_html", 'in die Phase <strong>', 2), '</strong>', 1),
                    SPLIT_PART(SPLIT_PART(a."message_short_html", '</strong> nach <strong>', 2), '</strong>', 1)
                ) 
      END as "stage_to",
      --s."stage_category_adjusted",
      CASE 
           WHEN a."event" in ('candidate_add', 'candidate_apply', 'candidate_offer_assign') 
                THEN 'candidate'
           WHEN s."stage_category_adjusted" is not NULL 
                THEN s."stage_category_adjusted"
           ELSE iff(lower("stage_to") like 'sourced%' or lower("stage_to") like 'apply%' or lower("stage_to") like 'applied%' or lower("stage_to") like 'referred%', 'candidate',
                     iff(lower("stage_to") like '%screen%' OR lower("stage_to") like 'to be rejected%', 'phone_screen',
                        iff(lower("stage_to") like 'final interview%' OR lower("stage_to") like 'evaluation%', 'evaluation',
                             iff(lower("stage_to") like '%test%' OR lower("stage_to") like '%task%' OR lower("stage_to") like '%interview%' OR lower("stage_to")='onsite', 'interview',
                                iff(lower("stage_to") like 'offer%', 'offer',
                                    iff(lower("stage_to") like 'hired%', 'hire','candidate')))))) -- CHEAT: else 'candidate' for cases with weird stages
           END "stage_to_category"

    from "recruitee_candidates_activities" as a
    left join recruitee_stage as s 
        on s."job_id"=a."offer_id"
        and s."stage_name"="stage_to"
    where 1=1
        and a."event" in ('candidate_add', 'candidate_apply', 'candidate_offer_assign', 'candidate_stage_change', 'candidate_disqualify', 'interview_result_add')
        --and a."offer_id" <> ''
;

