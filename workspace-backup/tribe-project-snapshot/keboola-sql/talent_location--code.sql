-- Transformation: talent location
-- Block: Block 1
-- Code: code
-- Extracted from Keboola on 2026-03-30

create or replace view "talent_locations_unique" as 
select distinct 
	"location_address" as "location"
from "Talent"
left join "talent_locations_processed"
	on "Talent"."location_address" = "talent_locations_processed"."query"
where 1=1
	AND ("talent_locations_processed"."query" is NULL
       OR "talent_locations_processed"."country"=''
      )
;

