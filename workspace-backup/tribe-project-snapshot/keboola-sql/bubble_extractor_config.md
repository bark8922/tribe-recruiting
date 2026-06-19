# Bubble.io Extractor Configuration
## Extracted from Keboola on 2026-03-30

### API Base URL
`https://overview.tribe.xyz/api/1.1/obj/`

### Authentication
API Token (stored encrypted in Keboola)

---

## Full Load Tables (28 endpoints)
These are loaded in full each run (stored incrementally in Keboola).

| Endpoint | Key Fields |
|----------|-----------|
| Jobs | _id, Company, Title, HiringManager, Location.address, recruiter_responsible, sourcer_responsible, stages, priority, archived, atsID, executive_search |
| HiringManager | _id, Name, Title, Company, Email, archived |
| User | _id, First Name, Last Name, full_name, Recruiter, Role within platform, Employee number, archived |
| stagesType | _id, point_of_process, showInDashboard, stage_type_name |
| atsOptions | _id, name |
| RoleWithinPlatform | _id, Name, Number, Internal |
| EventType | _id, slug, name, classification |
| ReasonNotInterested | name, number |
| Salary_currency | _id, Name |
| Language_talent | _id, Language_level, Language_name |
| Languages | _id, Name |
| Languages_levels | _id, Name |
| recruiter_screen_relocation_dropdown | _id, name |
| SalaryType | _id, Name |
| recruiter_screen_notice_period_dropdown | _id, name |
| TechStack | _id, Name, TechStackType |
| TechStackType | _id, Name |
| recruiter_screen_visa_dropdown | _id, name |
| Sub_conditional | _id, Name, parent_conditional, Conditional |
| Automationflow | _id, Steps, Schedule, Name, Job, Enabled, Conditional, company |
| Automationstep | _id, Type, Delay - days, Delay - hours, order_number, automation flow, Sub-conditional |
| job_category | name |
| Job_sub_category | name, Job_category |
| Sourced_source | Name, order_number |
| Goals | Job, date range, Goal(number), type |
| Conditional | main, delay, automation_flow, type, sub_conditionals, archived |
| Roles | name |
| sub_roles | name, order |

## Incremental Load Tables (11 endpoints)
These load only recent changes (period_from: "1 day ago").

| Endpoint | Key Fields | Notes |
|----------|-----------|-------|
| Candidate | _id, Stage, Talent, Job, sourcer, disqualified, first_name, last_name, atsID, reason_not_interested, Sourcedsource, hired_salary_euro, hired_salary, archived | **Core pipeline data** |
| Talent | _id, Full name, companyName, current_company_name, currentTitle, Email, linkedin, location.address, LinkedinMainID | **1.6M rows** |
| Events | _id, Candidate, talent, job, event_type, moved_to_stage, who_event_created_for, who_created_event, replied, recruiterScreen, AI, not_fit, not_fit_reason | **14.5M rows — largest table** |
| Emails | _id, email, talent, count | Talent email records |
| Company | _id, Name, CompanyWebsite, client, jobs, users, archived, test | Client companies |
| Position | Job_title, Talent, Company, Worked_from, Worked_to | Work history |
| Nylas_Email_message | A/B Id, version, Read | Email tracking |
| duxsoup_messages | A/B id, version | LinkedIn automation tracking |
| Analytic | page, user | Page analytics |
| stages | _id, Company, Point of process, showDashboard, stageName, stagesType, atsID, clientID | Stage definitions per client |
| recruiter_screeen_notes | _id, candidate, Current_salary, job, Languages, rating, tech_stack, visa_dropdown, desired_salary | **Screen/interview data** |
