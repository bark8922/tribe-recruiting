# Reporting V2 Table Schemas
## Extracted from Keboola on 2026-03-30

### Volume Summary

| Table | Rows | Size | Primary Key |
|-------|------|------|-------------|
| event | 14,560,606 | 810 MB | event_id |
| talent_position | 6,869,763 | 189 MB | talent_id\|position_order_desc |
| talent | 1,606,095 | 115 MB | talent_id |
| candidate | 1,352,058 | 104 MB | candidate_id |
| candidate_stage | 1,352,058 | 36 MB | candidate_id |
| talent_employer | 432,043 | 14 MB | employer_id |
| talent_email | 104,845 | 7 MB | email_id |
| screen | 77,084 | 5 MB | screen_id |
| analytic | 51,672 | 421 KB | - |
| job | 6,388 | 534 KB | job_id |
| screen_lang | 4,509 | 119 KB | screen_id\|lang_name |
| screen_techstack | 1,852 | 45 KB | screen_id\|techstack_name |
| job_ai_filter | 1,300 | 78 KB | - |
| user | 282 | 18 KB | user_id |
| client | 120 | 7 KB | client_id |
| client_cost | 97 | 5 KB | - |
| job_goal | 40 | 5 KB | goal_id |

**Total: ~1.28 GB across 17 tables**

---

### job (6,388 rows)
| Column | Type | Description |
|--------|------|-------------|
| job_id | STRING | PK |
| client_id | STRING | FK to client |
| date_created | DATE | |
| date_first_hired | DATE | |
| date_first_hired_contacted | DATE | |
| job_title | STRING | |
| job_category | STRING | |
| job_subcategory | STRING | |
| job_location | STRING | |
| user_hiring_manager | STRING | |
| email_hiring_manager | STRING | |
| job_recruiter | STRING | Assigned recruiter |
| job_sourcer | STRING | Assigned sourcer |
| job_ats_id | STRING | |
| is_job_archived | BOOLEAN | |
| is_external_recruiter | BOOLEAN | |
| test | BOOLEAN | |
| executive_search | BOOLEAN | |

### candidate (1,352,058 rows)
| Column | Type | Description |
|--------|------|-------------|
| candidate_id | STRING | PK |
| job_id | STRING | FK to job |
| talent_id | STRING | FK to talent |
| reason_not_interested | STRING | Why candidate rejected/dropped |
| hired_salary_eur | STRING | |
| hired_salary | STRING | |
| hired_salary_currency | STRING | |
| source | STRING | Sourcing channel |
| is_candidate_duplicated | BOOLEAN | |
| is_candidate_disqualified | BOOLEAN | |
| is_candidate_archived | BOOLEAN | |
| is_candidate_reacted | BOOLEAN | Responded to outreach |
| candidate_sourcer | STRING | Who sourced this candidate |
| is_candidate_createdby_ai | BOOLEAN | AI-sourced flag |

### candidate_stage (1,352,058 rows)
| Column | Type | Description |
|--------|------|-------------|
| candidate_id | STRING | PK, FK to candidate |
| stage_current_type | STRING | Stage category |
| stage_current | STRING | Current stage name |
| date_created | DATE | When candidate was created |
| date_lnkdin_viewed | DATE | LinkedIn profile viewed |
| date_contacted | DATE | First outreach |
| date_screen | DATE | Screen scheduled |
| date_screen_actual | DATE | Screen actually happened |
| date_interview | DATE | Client interview |
| date_offer | DATE | Offer extended |
| date_hired | DATE | Hired |
| automation_emails | NUMERIC | Automated emails sent |
| automation_connections | NUMERIC | LinkedIn connections sent |
| automation_inmails | NUMERIC | InMails sent |
| automation_messages | NUMERIC | Messages sent |
| hired_order | NUMERIC | How many candidates it took |
| hired_views | NUMERIC | Profile views before hire |
| hired_contacts | NUMERIC | Contacts before hire |
| hired_screens | NUMERIC | Screens before hire |

### event (14,560,606 rows)
| Column | Type | Description |
|--------|------|-------------|
| event_id | STRING | PK |
| candidate_id | STRING | FK |
| talent_id | STRING | FK |
| job_id | STRING | FK |
| date_created | DATE | |
| datetime_created | TIMESTAMP | |
| is_event_duplicated | BOOLEAN | |
| is_external_recruiter | BOOLEAN | |
| event_type | STRING | Type of event |
| moved_to_stage | STRING | Stage transition |
| moved_to_stageType | STRING | Stage type |
| who_created_event | STRING | Event creator |
| who_created_event_first | STRING | |
| who_event_created_for | STRING | Recruiter/sourcer |
| who_event_created_for_id | STRING | |
| automation_flow_name | STRING | |
| automation_step_type | STRING | |
| automation_step_order | STRING | |
| automation_step_name | STRING | |
| automation_step_subcon | STRING | |
| automation_step_con | STRING | |
| automation_is_message_read | BOOLEAN | |
| automation_is_message_replied | BOOLEAN | |
| automation_message_version_id | STRING | |
| is_event_createdby_ai | BOOLEAN | |
| ai_rearch_id | STRING | |
| not_fit | STRING | |
| not_fit_reason | STRING | |

### talent (1,606,095 rows)
| Column | Type | Description |
|--------|------|-------------|
| talent_id | STRING | PK |
| date_created | DATE | |
| full_name | STRING | |
| current_company | STRING | |
| current_title | STRING | |
| main_email | STRING | |
| linkedin_link | STRING | |
| is_talent_duplicated | BOOLEAN | |
| duplicates | STRING | |
| location | STRING | |
| location_country | STRING | From geocoding |
| location_city | STRING | From geocoding |

### screen (77,084 rows)
| Column | Type | Description |
|--------|------|-------------|
| screen_id | STRING | PK |
| candidate_id | STRING | FK |
| date_created | STRING | |
| current_salary | STRING | |
| current_salary_currency | STRING | |
| desired_salary | STRING | |
| desired_salary_currency | STRING | |
| location | STRING | |
| rating | STRING | Screen rating |
| user_recruiter | STRING | Who screened |
| relocation | STRING | |
| salary_type | STRING | |
| start_date | STRING | |
| visa | STRING | |

### client (120 rows)
| Column | Type | Description |
|--------|------|-------------|
| client_id | STRING | PK |
| client_name | STRING | |
| is_client_archived | BOOLEAN | |
| test | BOOLEAN | |
