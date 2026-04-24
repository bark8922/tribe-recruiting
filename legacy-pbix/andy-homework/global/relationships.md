# Relationship graph (prefilled)

**Source:** `powerbi_export/Tribe-recruiting-WBR-MBR.SemanticModel/definition/relationships.tmdl`  
**Total:** 45 relationships (32 active, 13 inactive)

## How to fill this in

This is the **hidden half** of the logic. The DAX tells us *what* to compute; the relationships tell us *how the filter flows through the model*. Measures that say `USERELATIONSHIP(...)` rely on swapping an inactive relationship in as the active one. If we don't know *why* a given inactive relationship exists, we can't reproduce those measures.

For each **inactive** relationship below (the ones that say `INACTIVE`), write one line: *in which measure do you activate this, and why?*  
Example: `event.date_created → Calendar.Date` — *Used in all the `(date created)` measures so we filter events by the date they occurred, not by the candidate's contacted date.*

Active relationships (no prompt needed unless you want to flag something).

---

## from `'IR Comment'`

- `'IR Comment'.Week` → `Calendar.'Week start end'` — **ACTIVE**, cross-filter: `(single)` _(cardinality: many-to-many)_

- `'IR Comment'.Job_ID` → `job.job_id` — **ACTIVE**, cross-filter: `bothDirections`


## from `'OKR TA'`

- `'OKR TA'.TA` → `'WBR TA Target'.TA` — **ACTIVE**, cross-filter: `bothDirections` _(cardinality: many-to-many)_


## from `'WBR TA Actual'`

- `'WBR TA Actual'.'TA Client Key'` → `'WBR TA Target'.'TA Client Key'` — **ACTIVE**, cross-filter: `(single)`

- `'WBR TA Actual'.Concat_comment` → `'WBR TA Comment'.Concat` — **ACTIVE**, cross-filter: `(single)`

- `'WBR TA Actual'.'TA& Cient jobs 60d'` → `'WBR Job open 60d'.Contacy` — **ACTIVE**, cross-filter: `(single)`

- `'WBR TA Actual'.'Historical Manager Key'` → `'Historical Manager Structure WBR'.'Manager Key'` — **ACTIVE**, cross-filter: `(single)`

- `'WBR TA Actual'.'TA& Cient jobs 60d'` → `'WBR Job from Aug 2025'.Contact` — **ACTIVE**, cross-filter: `(single)`

- `'WBR TA Actual'.'Week start end'` → `'Calendar WBR'.'Week start end'` — **ACTIVE**, cross-filter: `(single)`

- `'WBR TA Actual'.who_event_created_for` → `User.user_name` — **ACTIVE**, cross-filter: `(single)`


## from `'WBR TA Job'`

- `'WBR TA Job'.'Key Job concat'` → `'WBR TA Actual'.'Key Job concat'` — **ACTIVE**, cross-filter: `(single)`


## from `'WBR TS Actual'`

- `'WBR TS Actual'.Concat_comment` → `'WBR TS Comment'.Concat` — **ACTIVE**, cross-filter: `bothDirections` _(cardinality: one-to-one)_

- `'WBR TS Actual'.'Historical Manager Key TS'` → `'Historical Manager Structure WBR'.'Manager Key'` — **ACTIVE**, cross-filter: `(single)`

- `'WBR TS Actual'.'Week start end'` → `'Calendar WBR'.'Week start end'` — **ACTIVE**, cross-filter: `(single)`

- `'WBR TS Actual'.who_created_event_first` → `User.user_name` — **ACTIVE**, cross-filter: `(single)`


## from `Org`

- `Org.'Employee #'` → `User.employee_number` — **ACTIVE**, cross-filter: `bothDirections` _(cardinality: one-to-one)_


## from `Org_WBR`

- `Org_WBR.'Employee #'` → `User.employee_number` — **ACTIVE**, cross-filter: `bothDirections`


## from `Temp_Inactive_Jobs_Sourcers_WBR`

- `Temp_Inactive_Jobs_Sourcers_WBR.job_id` → `job.job_id` — **ACTIVE**, cross-filter: `bothDirections` _(cardinality: one-to-one)_


## from `analytic_usage`

- `analytic_usage.created_date` → `Calendar.Date` — **ACTIVE**, cross-filter: `(single)`


## from `candidate`

- `candidate.job_id` → `job.job_id` — **ACTIVE**, cross-filter: `bothDirections`

- `candidate.talent_id` → `talent.talent_id` — **ACTIVE**, cross-filter: `bothDirections`


## from `candidate_stage`

- `candidate_stage.candidate_id` → `candidate.candidate_id` — **ACTIVE**, cross-filter: `bothDirections` _(cardinality: one-to-one)_

- `candidate_stage.date_contacted` → `Calendar.Date` — **ACTIVE**, cross-filter: `(single)`

- `candidate_stage.date_interview` → `Calendar.Date` — **INACTIVE**, cross-filter: `(single)`
  - **Why does this relationship exist?** _Andy fills in:_ 

- `candidate_stage.date_created` → `Calendar.Date` — **INACTIVE**, cross-filter: `(single)`
  - **Why does this relationship exist?** _Andy fills in:_ 

- `candidate_stage.date_offer` → `Calendar.Date` — **INACTIVE**, cross-filter: `(single)`
  - **Why does this relationship exist?** _Andy fills in:_ 

- `candidate_stage.date_hired` → `Calendar.Date` — **INACTIVE**, cross-filter: `(single)`
  - **Why does this relationship exist?** _Andy fills in:_ 

- `candidate_stage.date_screen_actual` → `Calendar.Date` — **INACTIVE**, cross-filter: `(single)`
  - **Why does this relationship exist?** _Andy fills in:_ 

- `candidate_stage.date_screen` → `Calendar.Date` — **INACTIVE**, cross-filter: `(single)`
  - **Why does this relationship exist?** _Andy fills in:_ 

- `candidate_stage.'Last update'` → `Calendar.Date` — **INACTIVE**, cross-filter: `(single)`
  - **Why does this relationship exist?** _Andy fills in:_ 


## from `event`

- `event.candidate_id` → `candidate.candidate_id` — **ACTIVE**, cross-filter: `bothDirections`

- `event.talent_id` → `talent.talent_id` — **INACTIVE**, cross-filter: `(single)`
  - **Why does this relationship exist?** _Andy fills in:_ 

- `event.date_created` → `Calendar.Date` — **INACTIVE**, cross-filter: `(single)`
  - **Why does this relationship exist?** _Andy fills in:_ 

- `event.job_id` → `job.job_id` — **INACTIVE**, cross-filter: `(single)`
  - **Why does this relationship exist?** _Andy fills in:_ 

- `event.who_created_event_first` → `'Sourcing Stats'.who_created_event_first` — **ACTIVE**, cross-filter: `bothDirections` _(cardinality: many-to-many)_

- `event.who_event_created_for` → `Current_TA.TA` — **ACTIVE**, cross-filter: `(single)`

- `event.who_created_event_first` → `Current_TS.TS` — **ACTIVE**, cross-filter: `(single)`

- `event.who_created_event_first` → `User.user_name` — **ACTIVE**, cross-filter: `bothDirections`

- `event.'Sourcer Employee Number'` → `'Sourcing Team List'.'Employee Number'` — **ACTIVE**, cross-filter: `(single)`


## from `job`

- `job.client_id` → `client.client_id` — **ACTIVE**, cross-filter: `bothDirections`

- `job.date_created` → `Calendar.Date` — **INACTIVE**, cross-filter: `(single)`
  - **Why does this relationship exist?** _Andy fills in:_ 


## from `screen`

- `screen.candidate_id` → `candidate.candidate_id` — **ACTIVE**, cross-filter: `(single)`

- `screen.date_created` → `Calendar.Date` — **INACTIVE**, cross-filter: `(single)`
  - **Why does this relationship exist?** _Andy fills in:_ 


## from `talent`

- `talent.date_created` → `Calendar.Date` — **INACTIVE**, cross-filter: `(single)`
  - **Why does this relationship exist?** _Andy fills in:_ 


## from `talent_email`

- `talent_email.talent_id` → `talent.talent_id` — **ACTIVE**, cross-filter: `(single)`

