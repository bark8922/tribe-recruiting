#!/usr/bin/env python3
"""
DuckDB Transformation Pipeline for Tribe Recruiting Dashboard
================================================================
Ports the Keboola/Snowflake SQL transformations to DuckDB.
Reads JSON files from bubble_extract.py and produces recruiting_data.json.

Source SQL files ported:
  - part_0_temporary_tables.sql (stage type mapping)
  - part_1_bubble_data.sql (main transformation)
  - part_3_final_tables.sql (talent merge)

Skipped:
  - part_2_recruitee_data.sql (legacy Recruitee ATS — not needed for 2025+)
  - Revenue/client_cost (handled by finance dashboard)
  - talent_location geocoding (deferred)
"""

import json
import logging
import os
import sys
from datetime import datetime
from pathlib import Path

import duckdb

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("transform")

DATA_DIR = Path(os.environ.get("RECRUIT_DATA_DIR", Path(__file__).parent / "data"))
OUTPUT_DIR = Path(os.environ.get("RECRUIT_OUTPUT_DIR", Path(__file__).parent / "output"))


def load_bubble_tables(con: duckdb.DuckDBPyConnection, data_dir: Path):
    """Load all bubble_*.json files into DuckDB tables."""
    json_files = sorted(data_dir.glob("bubble_*.json"))
    if not json_files:
        log.error(f"No bubble_*.json files found in {data_dir}")
        sys.exit(1)

    for f in json_files:
        table_name = f.stem  # e.g., "bubble_Jobs"
        log.info(f"Loading {table_name} from {f.name}...")
        try:
            con.execute(f"""
                CREATE OR REPLACE TABLE "{table_name}" AS
                SELECT * FROM read_json_auto('{f}', maximum_object_size=100000000, union_by_name=true, ignore_errors=true)
            """)
            # Ensure bubbleinternal_id column exists (Bubble API returns _id,
            # but Keboola-ported SQL references bubbleinternal_id)
            cols = [row[0] for row in con.execute(
                f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table_name}'"
            ).fetchall()]
            if "bubbleinternal_id" not in cols and "_id" in cols:
                con.execute(f'ALTER TABLE "{table_name}" ADD COLUMN bubbleinternal_id VARCHAR')
                con.execute(f'UPDATE "{table_name}" SET bubbleinternal_id = "_id"')
                log.info(f"  Added bubbleinternal_id from _id")
            count = con.execute(f'SELECT count(*) FROM "{table_name}"').fetchone()[0]
            log.info(f"  → {count:,} rows")
        except Exception as e:
            log.warning(f"  Failed to load {table_name}: {e}")


def run_transformations(con: duckdb.DuckDBPyConnection):
    """Run all SQL transformations ported from Keboola/Snowflake."""

    # ===================================================================
    # JOBS
    # ===================================================================
    log.info("Creating final_job...")
    con.execute("""
        CREATE OR REPLACE TABLE tmp_job AS
        SELECT
            j.bubbleinternal_id AS job_id,
            LEFT(j.Created_Date, 10) AS date_created,
            j.Title AS job_title,
            CASE WHEN j.archived IN ('true', 'True', '1') THEN TRUE ELSE FALSE END AS is_job_archived,
            CASE WHEN j.archived IN ('true', 'True', '1') THEN TRUE ELSE FALSE END AS is_client_archived,
            c.bubbleinternal_id AS client_id,
            c.Name AS client_name,
            COALESCE(hm.Name, '-not available-') AS user_hiring_manager,
            LOWER(TRIM(hm.Email)) AS email_hiring_manager,
            COALESCE(u.First_Name || ' ' || u.Last_Name, '-not available-') AS job_recruiter,
            COALESCE(s.First_Name || ' ' || s.Last_Name, '-not available-') AS job_sourcer,
            j.atsID AS ats_id,
            ROW_NUMBER() OVER (PARTITION BY j.atsID ORDER BY LEFT(j.Created_Date, 10) DESC) AS rown,
            ats.name AS ats_name,
            j.campaign_inREPLY AS email_campaign_id,
            cat.name AS job_category,
            scat.name AS job_subcategory,
            j.Location_address AS job_location,
            CASE WHEN j.external_recruiter IN ('true', 'True', '1') THEN TRUE ELSE FALSE END AS is_external_recruiter,
            CASE WHEN j.test = 'True' THEN TRUE WHEN j.test = 'False' THEN FALSE ELSE NULL END AS test,
            CASE WHEN j.executive_search = 'True' THEN TRUE WHEN j.executive_search = 'False' THEN FALSE ELSE NULL END AS executive_search
        FROM bubble_Jobs AS j
        LEFT JOIN bubble_Company AS c ON c.bubbleinternal_id = j.Company
        LEFT JOIN bubble_atsOptions AS ats ON c.ats = ats.bubbleinternal_id
        LEFT JOIN bubble_HiringManager AS hm ON j.HiringManager = hm.bubbleinternal_id
        LEFT JOIN bubble_User AS u ON j.recruiter_responsible = u.bubbleinternal_id
        LEFT JOIN bubble_User AS s ON j.sourcer_responsible = s.bubbleinternal_id
        LEFT JOIN bubble_Job_sub_category AS scat ON j.sub_category = scat.bubbleinternal_id
        LEFT JOIN bubble_job_category AS cat ON scat.Job_category = cat.bubbleinternal_id
    """)

    con.execute("""
        CREATE OR REPLACE TABLE final_job AS
        SELECT
            job_id,
            client_id,
            TRY_CAST(date_created AS DATE) AS date_created,
            NULL::DATE AS date_first_hired,
            NULL::DATE AS date_first_hired_contacted,
            job_title,
            job_category,
            job_subcategory,
            job_location,
            user_hiring_manager,
            email_hiring_manager,
            job_recruiter,
            job_sourcer,
            CASE WHEN ats_id IS NOT NULL AND ats_id != '' AND rown = 1 THEN ats_id ELSE NULL END AS job_ats_id,
            is_job_archived,
            is_external_recruiter,
            test,
            executive_search
        FROM tmp_job
    """)
    _log_count(con, "final_job")

    # ===================================================================
    # USER
    # ===================================================================
    log.info("Creating final_user...")
    con.execute("""
        CREATE OR REPLACE TABLE final_user AS
        WITH role_events AS (
            SELECT
                e.who_event_created_for,
                r.name AS role_current,
                sr.name AS sub_role_current,
                e.Created_Date,
                ROW_NUMBER() OVER (
                    PARTITION BY e.who_event_created_for
                    ORDER BY TRY_CAST(e.Created_Date AS TIMESTAMP) DESC
                ) AS rown
            FROM bubble_Events AS e
            LEFT JOIN bubble_Roles AS r ON e.new_role = r.bubbleinternal_id
            LEFT JOIN bubble_sub_roles AS sr ON e.new_sub_role = sr.bubbleinternal_id
            WHERE e.event_type = '1642420714568x807043530709183200'
              AND e.new_role IS NOT NULL AND e.new_role != ''
        )
        SELECT
            u.bubbleinternal_id AS user_id,
            u.First_Name || ' ' || u.Last_Name AS user_name,
            u.authentication_email_email AS user_email,
            u.Employee_number AS employee_number,
            re.role_current,
            re.sub_role_current
        FROM bubble_User AS u
        LEFT JOIN role_events AS re
            ON re.who_event_created_for = u.bubbleinternal_id
            AND re.rown = 1
    """)
    _log_count(con, "final_user")

    # ===================================================================
    # CLIENT
    # ===================================================================
    log.info("Creating final_client...")
    con.execute("""
        CREATE OR REPLACE TABLE final_client AS
        SELECT DISTINCT
            client_id,
            client_name,
            is_client_archived,
            CASE WHEN test = TRUE THEN TRUE ELSE FALSE END AS test
        FROM tmp_job
        WHERE client_name IS NOT NULL
    """)
    _log_count(con, "final_client")

    # ===================================================================
    # JOB GOALS
    # ===================================================================
    log.info("Creating final_job_goals...")
    try:
        con.execute("""
            CREATE OR REPLACE TABLE final_job_goals AS
            SELECT
                bubbleinternal_id AS goal_id,
                Job AS job_id,
                Goal_number AS goal_number,
                TRY_CAST(Created_Date AS TIMESTAMP)::DATE AS date_created,
                TRY_CAST(Modified_Date AS TIMESTAMP)::DATE AS date_modified
            FROM bubble_Goals
        """)
        _log_count(con, "final_job_goals")
    except Exception as e:
        log.warning(f"  Skipping final_job_goals: {e}")

    # ===================================================================
    # TALENT
    # ===================================================================
    log.info("Creating final_talent...")
    con.execute("""
        CREATE OR REPLACE TABLE final_talent AS
        WITH latest_email AS (
            SELECT *,
                ROW_NUMBER() OVER (PARTITION BY talent ORDER BY TRY_CAST(Created_Date AS TIMESTAMP) DESC) AS rown
            FROM bubble_Emails
        )
        SELECT
            t.bubbleinternal_id AS talent_id,
            LEFT(t.Created_Date, 10) AS date_created,
            TRY_CAST(t.Created_Date AS TIMESTAMP) AS timestamp_created,
            t.Full_name AS full_name,
            t.companyName AS current_company,
            t.currentTitle AS current_title,
            e.email AS main_email,
            CASE
                WHEN LEFT(t.linkedin, 4) = 'http' THEN TRIM(LOWER(t.linkedin))
                WHEN TRIM(LOWER(COALESCE(t.linkedin, ''))) IN ('', 'undefined') THEN NULL
                ELSE 'https://' || TRIM(LOWER(t.linkedin))
            END AS linkedin_link,
            t.LinkedinMainID,
            t.linkedin_nick,
            FALSE AS is_talent_duplicated,
            '' AS duplicates,
            t.location_address AS location,
            NULL AS location_country,
            NULL AS location_city
        FROM bubble_Talent AS t
        LEFT JOIN latest_email AS e
            ON t.bubbleinternal_id = e.talent AND e.rown = 1
    """)

    # Duplicate detection: LinkedIn link
    log.info("  Running duplicate detection...")
    con.execute("""
        UPDATE final_talent AS b
        SET is_talent_duplicated = TRUE
        WHERE b.linkedin_link IS NOT NULL
          AND b.is_talent_duplicated = FALSE
          AND EXISTS (
              SELECT 1 FROM final_talent AS x
              WHERE x.talent_id != b.talent_id
                AND x.timestamp_created <= b.timestamp_created
                AND x.linkedin_link = b.linkedin_link
          )
    """)

    # Duplicate detection: LinkedinMainID
    con.execute("""
        UPDATE final_talent AS b
        SET is_talent_duplicated = TRUE
        WHERE b.LinkedinMainID IS NOT NULL AND b.LinkedinMainID != ''
          AND b.is_talent_duplicated = FALSE
          AND EXISTS (
              SELECT 1 FROM final_talent AS x
              WHERE x.talent_id != b.talent_id
                AND x.timestamp_created <= b.timestamp_created
                AND x.LinkedinMainID = b.LinkedinMainID
          )
    """)

    # Duplicate detection: email
    con.execute("""
        UPDATE final_talent AS b
        SET is_talent_duplicated = TRUE
        WHERE b.main_email IS NOT NULL AND b.main_email != ''
          AND b.is_talent_duplicated = FALSE
          AND EXISTS (
              SELECT 1 FROM final_talent AS x
              WHERE x.talent_id != b.talent_id
                AND x.timestamp_created <= b.timestamp_created
                AND x.main_email = b.main_email
          )
    """)
    _log_count(con, "final_talent")

    # ===================================================================
    # TALENT EMAIL
    # ===================================================================
    log.info("Creating final_email...")
    con.execute("""
        CREATE OR REPLACE TABLE final_email AS
        SELECT
            m.bubbleinternal_id AS email_id,
            m.talent AS talent_id,
            LEFT(m.Created_Date, 10) AS date_created,
            m.email,
            CASE WHEN m.email_order > 1 AND m.email != '' THEN TRUE ELSE FALSE END AS is_email_duplicated
        FROM (
            SELECT *,
                ROW_NUMBER() OVER (PARTITION BY email ORDER BY TRY_CAST(Created_Date AS TIMESTAMP) DESC) AS email_order
            FROM bubble_Emails
        ) AS m
    """)
    _log_count(con, "final_email")

    # ===================================================================
    # EVENT
    # ===================================================================
    log.info("Creating final_event...")
    con.execute("""
        CREATE OR REPLACE TABLE final_event AS
        SELECT
            e.bubbleinternal_id AS event_id,
            CASE WHEN e.Candidate = '' THEN NULL ELSE e.Candidate END AS candidate_id,
            COALESCE(
                CASE WHEN e.talent = '' THEN NULL ELSE e.talent END,
                c.Talent
            ) AS talent_id,
            COALESCE(
                CASE WHEN e.job = '' THEN NULL ELSE e.job END,
                CASE WHEN c.Job = '' THEN '--None--' ELSE c.Job END
            ) AS job_id,
            COALESCE(
                TRY_CAST(e.ats_creation_time AS TIMESTAMP),
                TRY_CAST(e.Created_Date AS TIMESTAMP)
            ) AS date_created,
            CASE WHEN e.event_order > 1 THEN TRUE ELSE FALSE END AS is_event_duplicated,
            CASE WHEN e.external_recruiter IN ('true', 'True', '1') THEN TRUE ELSE FALSE END AS is_external_recruiter,
            et.name AS event_type,
            s.stageName AS moved_to_stage,
            st.stage_type_name AS moved_to_stageType,
            COALESCE(u1.full_name, u1.bubbleinternal_id) AS who_created_event,
            NULL::VARCHAR AS who_created_event_first,
            COALESCE(u2.full_name, u2.bubbleinternal_id) AS who_event_created_for,
            e.who_event_created_for AS who_event_created_for_id,
            flow.Name AS automation_flow_name,
            step.Type AS automation_step_type,
            step.order_number AS automation_step_order,
            CASE
                WHEN step.bubbleinternal_id IS NULL THEN NULL
                ELSE step.order_number || ' - ' || step.Type || ' - ' || et.name
            END AS automation_step_name,
            COALESCE(sub.Name, '?') AS automation_step_subcon,
            COALESCE(pcon.type, '?') AS automation_step_con,
            NULL::BOOLEAN AS automation_is_message_read,
            NULL::BOOLEAN AS automation_is_message_replied,
            NULL::VARCHAR AS automation_message_version_id,
            CASE WHEN e.AI IN ('True', 'true', '1') THEN TRUE ELSE NULL END AS is_event_createdby_ai,
            e.Ai_Search AS ai_rearch_id,
            e.not_fit,
            e.not_fit_reason,
            -- Keep for later updates
            e.duxsoupMessage,
            e.Nylas_email,
            e.Automation_flow AS _automation_flow,
            e.Automation_step AS _automation_step
        FROM (
            SELECT x.*,
                COALESCE(TRY_CAST(x.ats_creation_time AS TIMESTAMP), TRY_CAST(x.Created_Date AS TIMESTAMP)) AS date_created_2,
                ROW_NUMBER() OVER (
                    PARTITION BY x.talent, x.job, x.event_type, CAST(COALESCE(TRY_CAST(x.ats_creation_time AS TIMESTAMP), TRY_CAST(x.Created_Date AS TIMESTAMP)) AS DATE)
                    ORDER BY COALESCE(TRY_CAST(x.ats_creation_time AS TIMESTAMP), TRY_CAST(x.Created_Date AS TIMESTAMP)) ASC
                ) AS event_order
            FROM bubble_Events AS x
        ) AS e
        LEFT JOIN bubble_Candidate AS c ON e.Candidate = c.bubbleinternal_id
        LEFT JOIN bubble_EventType AS et ON e.event_type = et.bubbleinternal_id
        LEFT JOIN bubble_stages AS s ON e.moved_to_stage = s.bubbleinternal_id
        LEFT JOIN bubble_stagesType AS st ON s.stagesType = st.bubbleinternal_id
        LEFT JOIN bubble_User AS u1 ON e.who_created_event = u1.bubbleinternal_id
        LEFT JOIN bubble_User AS u2 ON e.who_event_created_for = u2.bubbleinternal_id
        LEFT JOIN bubble_Automationflow AS flow ON e.Automation_flow = flow.bubbleinternal_id
        LEFT JOIN bubble_Automationstep AS step ON e.Automation_step = step.bubbleinternal_id
        LEFT JOIN bubble_Sub_conditional AS sub ON step.Sub_conditional = sub.bubbleinternal_id
        LEFT JOIN bubble_Conditional AS pcon ON sub.parent_conditional = pcon.bubbleinternal_id
        WHERE e.Content IS DISTINCT FROM 'FrantisekDelete'
          AND e.archived IS DISTINCT FROM 'True'
    """)

    # who_created_event_first — first event creator per candidate
    log.info("  Setting who_created_event_first...")
    con.execute("""
        UPDATE final_event AS e
        SET who_created_event_first = x.who_created_event
        FROM (
            SELECT candidate_id, who_created_event,
                ROW_NUMBER() OVER (PARTITION BY candidate_id ORDER BY date_created ASC) AS rown
            FROM final_event
            WHERE who_created_event IS NOT NULL AND who_created_event != ''
              AND candidate_id IS NOT NULL AND candidate_id != ''
        ) AS x
        WHERE x.rown = 1 AND e.candidate_id = x.candidate_id
    """)

    # Update who_created_event_first for records without candidate_id (use talent_id)
    con.execute("""
        UPDATE final_event AS e
        SET who_created_event_first = x.who_created_event
        FROM (
            SELECT talent_id, who_created_event,
                ROW_NUMBER() OVER (PARTITION BY talent_id ORDER BY date_created ASC) AS rown
            FROM final_event
            WHERE who_created_event IS NOT NULL AND who_created_event != ''
              AND talent_id IS NOT NULL AND talent_id != ''
        ) AS x
        WHERE x.rown = 1
          AND e.who_created_event_first IS NULL
          AND e.talent_id = x.talent_id
    """)

    # Automation message tracking updates
    log.info("  Setting automation message tracking...")
    # LinkedIn duxsoup version
    try:
        con.execute("""
            UPDATE final_event AS e
            SET automation_message_version_id = m.version
            FROM bubble_duxsoup_messages AS m
            WHERE e.duxsoupMessage = m.bubbleinternal_id
              AND e.duxsoupMessage IS NOT NULL AND e.duxsoupMessage != ''
        """)
    except Exception:
        pass

    # Nylas email version + read
    try:
        con.execute("""
            UPDATE final_event AS e
            SET
                automation_message_version_id = m.version,
                automation_is_message_read = CASE
                    WHEN m."Read" IN ('yes', 'true', 'True', '1') AND e.event_type = 'Email Sent'
                    THEN TRUE ELSE NULL END
            FROM bubble_Nylas_Email_message AS m
            WHERE e.Nylas_email = m.bubbleinternal_id
              AND e.Nylas_email IS NOT NULL AND e.Nylas_email != ''
        """)
    except Exception:
        pass

    # Email replied tracking
    con.execute("""
        UPDATE final_event AS e
        SET automation_is_message_replied = CASE
            WHEN EXISTS (
                SELECT 1 FROM final_event x
                WHERE x.candidate_id = e.candidate_id
                  AND x._automation_flow = e._automation_flow
                  AND x._automation_step = e._automation_step
                  AND x.event_type = 'Email Replied'
            ) THEN TRUE ELSE NULL END
        WHERE e.event_type = 'Email Sent'
    """)

    # Email read tracking
    con.execute("""
        UPDATE final_event AS e
        SET automation_is_message_read = CASE
            WHEN EXISTS (
                SELECT 1 FROM final_event x
                WHERE x.candidate_id = e.candidate_id
                  AND x._automation_flow = e._automation_flow
                  AND x._automation_step = e._automation_step
                  AND x.event_type = 'Email Read'
            ) THEN TRUE ELSE NULL END
        WHERE e.event_type = 'Email Sent'
    """)

    # If replied, mark as read
    con.execute("""
        UPDATE final_event SET automation_is_message_read = TRUE
        WHERE automation_is_message_replied = TRUE AND event_type = 'Email Sent'
    """)

    # LinkedIn connection tracking
    con.execute("""
        UPDATE final_event AS e
        SET automation_is_message_read = CASE
            WHEN EXISTS (
                SELECT 1 FROM final_event x
                WHERE x.candidate_id = e.candidate_id
                  AND x._automation_flow = e._automation_flow
                  AND x._automation_step = e._automation_step
                  AND x.event_type = 'Linkedin Connected'
            ) THEN TRUE ELSE NULL END
        WHERE e.event_type = 'Linkedin Sent Contact'
    """)

    con.execute("""
        UPDATE final_event AS e
        SET automation_is_message_replied = CASE
            WHEN EXISTS (
                SELECT 1 FROM final_event x
                WHERE x.candidate_id = e.candidate_id
                  AND x._automation_flow = e._automation_flow
                  AND x._automation_step = e._automation_step
                  AND x.event_type = 'Linkedin Responded'
            ) THEN TRUE ELSE NULL END
        WHERE e.event_type IN ('Linkedin Sent Contact', 'Message sent')
    """)

    con.execute("""
        UPDATE final_event SET automation_is_message_read = TRUE
        WHERE automation_is_message_replied = TRUE AND event_type = 'Linkedin Sent Contact'
    """)

    # LinkedIn InMail tracking
    con.execute("""
        UPDATE final_event AS e
        SET automation_is_message_replied = CASE
            WHEN EXISTS (
                SELECT 1 FROM final_event x
                WHERE x.candidate_id = e.candidate_id
                  AND x._automation_flow = e._automation_flow
                  AND x._automation_step = e._automation_step
                  AND x.event_type = 'Linkedin inMail received'
            ) THEN TRUE ELSE NULL END
        WHERE e.event_type = 'Linkedin inMail sent'
    """)

    # Drop temp columns
    con.execute("""
        ALTER TABLE final_event DROP COLUMN duxsoupMessage;
        ALTER TABLE final_event DROP COLUMN Nylas_email;
        ALTER TABLE final_event DROP COLUMN _automation_flow;
        ALTER TABLE final_event DROP COLUMN _automation_step;
    """)
    _log_count(con, "final_event")

    # ===================================================================
    # CANDIDATE
    # ===================================================================
    log.info("Creating final_candidate...")
    con.execute("""
        CREATE OR REPLACE TABLE final_candidate AS
        SELECT
            c.bubbleinternal_id AS candidate_id,
            c.Job AS job_id,
            c.Talent AS talent_id,
            COALESCE(u.First_Name || ' ' || u.Last_Name, '-not available-') AS candidate_sourcer,
            r.name AS reason_not_interested,
            c.hired_salary_euro AS hired_salary_eur,
            c.hired_salary,
            COALESCE(sal.Name, c.hired_currency) AS hired_salary_currency,
            CASE
                WHEN c.candidate_order = 1 OR COALESCE(c.linkedin, '') = '' THEN FALSE
                ELSE TRUE
            END AS is_candidate_duplicated,
            CASE WHEN c.disqualified IN ('true', 'True', '1') THEN TRUE ELSE FALSE END AS is_candidate_disqualified,
            CASE WHEN c.archived = 'True' THEN TRUE ELSE FALSE END AS is_candidate_archived,
            FALSE AS is_candidate_createdby_ai,
            c.atsID AS candidate_ats_id,
            TRY_CAST(c.Created_Date AS TIMESTAMP)::DATE AS date_created,
            s.stageName AS stage_current,
            st.stage_type_name AS stage_current_type,
            ss.Name AS source
        FROM (
            SELECT z.*,
                t.linkedin,
                ROW_NUMBER() OVER (
                    PARTITION BY z.Job, t.linkedin
                    ORDER BY TRY_CAST(z.Created_Date AS TIMESTAMP) ASC
                ) AS candidate_order
            FROM bubble_Candidate AS z
            LEFT JOIN bubble_Talent AS t ON z.Talent = t.bubbleinternal_id
        ) AS c
        LEFT JOIN bubble_stages AS s ON c.Stage = s.bubbleinternal_id
        LEFT JOIN bubble_stagesType AS st ON s.stagesType = st.bubbleinternal_id
        LEFT JOIN bubble_ReasonNotInterested AS r ON c.reason_not_interested = r.bubbleinternal_id
        LEFT JOIN bubble_User AS u ON c.sourcer = u.bubbleinternal_id
        LEFT JOIN bubble_Sourced_source AS ss ON c.Sourcedsource = ss.bubbleinternal_id
        LEFT JOIN bubble_Salary_currency AS sal ON c.hired_currency = sal.bubbleinternal_id
    """)

    # AI-created candidates
    con.execute("""
        UPDATE final_candidate AS c
        SET is_candidate_createdby_ai = TRUE
        WHERE c.candidate_id IN (
            SELECT DISTINCT e.Candidate
            FROM bubble_Events AS e
            WHERE e.AI IN ('True', 'true', '1')
              AND e.event_type = '1542180373448x729603979969397200'
        )
    """)
    _log_count(con, "final_candidate")

    # ===================================================================
    # CANDIDATE STAGE (funnel dates)
    # ===================================================================
    log.info("Creating final_candidate_stage...")
    con.execute("""
        CREATE OR REPLACE TABLE final_candidate_stage AS
        SELECT
            c.candidate_id,
            c.stage_current_type,
            c.stage_current,
            CASE
                WHEN c.stage_current_type LIKE 'Offer' THEN 4
                WHEN c.stage_current_type LIKE 'Hired' THEN 5
                WHEN c.stage_current IN ('Referred', 'Downloaded', 'Prospects', 'Applied')
                     OR c.stage_current LIKE 'Sourced%' THEN 0
                WHEN c.stage_current_type IN ('Contacted', 'Positive Response') THEN 1
                WHEN c.stage_current_type LIKE 'Recruiter Screen' THEN 2
                WHEN LOWER(c.stage_current_type) LIKE '%interview%'
                     OR c.stage_current_type IN ('Reference Check', 'Offsite')
                     OR LOWER(c.stage_current) LIKE '%interview%' THEN 3
                ELSE 0
            END AS stage_current_num,
            c.date_created,
            NULL::DATE AS date_lnkdin_viewed,
            NULL::DATE AS date_contacted,
            NULL::DATE AS date_screen,
            NULL::DATE AS date_screen_actual,
            NULL::DATE AS date_interview,
            NULL::DATE AS date_offer,
            NULL::DATE AS date_hired,
            0 AS automation_emails,
            0 AS automation_connections,
            0 AS automation_inmails,
            0 AS automation_messages,
            -- Keep for stage date computation
            c.talent_id AS _talent_id,
            c.candidate_ats_id AS _candidate_ats_id
        FROM (
            SELECT fc.*, ft.talent_id, fc.candidate_ats_id
            FROM final_candidate fc
            LEFT JOIN (SELECT candidate_id, talent_id FROM final_candidate) ft
                ON fc.candidate_id = ft.candidate_id
        ) c
    """)

    # Set stage dates from events
    log.info("  Computing stage dates from events...")

    # date_lnkdin_viewed
    con.execute("""
        UPDATE final_candidate_stage AS c
        SET date_lnkdin_viewed = x.max_date
        FROM (
            SELECT t.talent_id, fc.candidate_id, MAX(CAST(t.date_created AS DATE)) AS max_date
            FROM final_event t
            JOIN final_candidate fc ON fc.talent_id = t.talent_id
            WHERE t.event_type = 'Linkedin Visited Profile'
            GROUP BY t.talent_id, fc.candidate_id
        ) x
        WHERE c.candidate_id = x.candidate_id
    """)

    # date_contacted
    con.execute("""
        UPDATE final_candidate_stage AS c
        SET date_contacted = x.max_date
        FROM (
            SELECT candidate_id, MAX(CAST(date_created AS DATE)) AS max_date
            FROM final_event
            WHERE moved_to_stageType = 'Contacted'
              AND moved_to_stage IS DISTINCT FROM 'Responded'
            GROUP BY candidate_id
        ) x
        WHERE c.candidate_id = x.candidate_id AND c.stage_current_num >= 1
    """)

    # date_screen_actual (from Evaluation events)
    con.execute("""
        UPDATE final_candidate_stage AS c
        SET date_screen_actual = x.max_date
        FROM (
            SELECT candidate_id, MAX(CAST(date_created AS DATE)) AS max_date
            FROM final_event WHERE event_type = 'Evaluation'
            GROUP BY candidate_id
        ) x
        WHERE c.candidate_id = x.candidate_id
    """)

    # date_screen_actual fallback (from screen notes)
    try:
        con.execute("""
            UPDATE final_candidate_stage AS c
            SET date_screen_actual = x.max_date
            FROM (
                SELECT candidate, MAX(TRY_CAST(Created_Date AS TIMESTAMP)::DATE) AS max_date
                FROM bubble_recruiter_screeen_notes
                GROUP BY candidate
            ) x
            WHERE c.candidate_id = x.candidate AND c.date_screen_actual IS NULL
        """)
    except Exception:
        pass

    # date_screen
    con.execute("""
        UPDATE final_candidate_stage AS c
        SET date_screen = x.max_date
        FROM (
            SELECT candidate_id, MAX(CAST(date_created AS DATE)) AS max_date
            FROM final_event WHERE moved_to_stageType = 'Recruiter Screen'
            GROUP BY candidate_id
        ) x
        WHERE c.candidate_id = x.candidate_id AND c.stage_current_num >= 2
    """)

    # date_interview
    con.execute("""
        UPDATE final_candidate_stage AS c
        SET date_interview = x.max_date
        FROM (
            SELECT candidate_id, MAX(CAST(date_created AS DATE)) AS max_date
            FROM final_event
            WHERE moved_to_stageType IN ('Offsite', 'Interview')
               OR LOWER(moved_to_stage) LIKE '%interview%'
            GROUP BY candidate_id
        ) x
        WHERE c.candidate_id = x.candidate_id AND c.stage_current_num >= 3
    """)

    # date_offer
    con.execute("""
        UPDATE final_candidate_stage AS c
        SET date_offer = x.max_date
        FROM (
            SELECT candidate_id, MAX(CAST(date_created AS DATE)) AS max_date
            FROM final_event WHERE moved_to_stageType = 'Offer'
            GROUP BY candidate_id
        ) x
        WHERE c.candidate_id = x.candidate_id AND c.stage_current_num >= 4
    """)

    # date_hired
    con.execute("""
        UPDATE final_candidate_stage AS c
        SET date_hired = x.max_date
        FROM (
            SELECT candidate_id, MAX(CAST(date_created AS DATE)) AS max_date
            FROM final_event WHERE moved_to_stageType = 'Hired'
            GROUP BY candidate_id
        ) x
        WHERE c.candidate_id = x.candidate_id AND c.stage_current_num >= 5
    """)

    # Fix cascading dates (if hired but no offer date, use hired date, etc.)
    log.info("  Fixing cascading stage dates...")
    con.execute("UPDATE final_candidate_stage SET date_offer = date_hired WHERE date_hired IS NOT NULL AND date_offer IS NULL")
    con.execute("UPDATE final_candidate_stage SET date_interview = date_offer WHERE date_offer IS NOT NULL AND date_interview IS NULL")
    con.execute("UPDATE final_candidate_stage SET date_screen_actual = date_interview WHERE date_interview IS NOT NULL AND date_screen_actual IS NULL")
    con.execute("UPDATE final_candidate_stage SET date_screen = date_screen_actual WHERE date_screen_actual IS NOT NULL AND date_screen IS NULL")
    con.execute("UPDATE final_candidate_stage SET date_contacted = date_screen WHERE date_screen IS NOT NULL AND date_contacted IS NULL")
    con.execute("""
        UPDATE final_candidate_stage
        SET date_lnkdin_viewed = date_contacted
        WHERE (date_contacted IS NOT NULL AND date_lnkdin_viewed IS NULL)
           OR (date_lnkdin_viewed > date_contacted)
    """)

    # Automation counts
    log.info("  Computing automation counts...")
    for event_type, col in [
        ("Email Sent", "automation_emails"),
        ("Linkedin Sent Contact", "automation_connections"),
        ("Linkedin inMail sent", "automation_inmails"),
        ("Message sent", "automation_messages"),
    ]:
        con.execute(f"""
            UPDATE final_candidate_stage AS c
            SET {col} = x.cnt
            FROM (
                SELECT candidate_id, COUNT(*) AS cnt
                FROM final_event
                WHERE automation_step_order IS NOT NULL
                  AND automation_flow_name IS NOT NULL
                  AND event_type = '{event_type}'
                GROUP BY candidate_id
            ) x
            WHERE c.candidate_id = x.candidate_id
        """)

    # Drop temp columns
    con.execute("ALTER TABLE final_candidate_stage DROP COLUMN stage_current_num")
    con.execute("ALTER TABLE final_candidate_stage DROP COLUMN _talent_id")
    con.execute("ALTER TABLE final_candidate_stage DROP COLUMN _candidate_ats_id")

    # Also drop from final_candidate
    con.execute("ALTER TABLE final_candidate DROP COLUMN date_created")
    con.execute("ALTER TABLE final_candidate DROP COLUMN stage_current")
    con.execute("ALTER TABLE final_candidate DROP COLUMN stage_current_type")

    _log_count(con, "final_candidate_stage")

    # ===================================================================
    # SCREEN NOTES
    # ===================================================================
    log.info("Creating final_screen...")
    try:
        con.execute("""
            CREATE OR REPLACE TABLE final_screen AS
            SELECT
                n.bubbleinternal_id AS screen_id,
                n.candidate AS candidate_id,
                LEFT(n.Created_Date, 10) AS date_created,
                n.Current_salary AS current_salary,
                sc.Name AS current_salary_currency,
                n.desired_salary,
                scc.Name AS desired_salary_currency,
                n.Location AS location,
                n.rating,
                COALESCE(r.First_Name || ' ' || r.Last_Name, '-not available-') AS user_recruiter,
                rd.name AS relocation,
                st.Name AS salary_type,
                p.name AS start_date,
                v.name AS visa
            FROM bubble_recruiter_screeen_notes AS n
            LEFT JOIN bubble_User AS r ON n.recruiter = r.bubbleinternal_id
            LEFT JOIN bubble_recruiter_screen_relocation_dropdown AS rd ON n.relocation_dropdown = rd.bubbleinternal_id
            LEFT JOIN bubble_SalaryType AS st ON n.Salary_type = st.bubbleinternal_id
            LEFT JOIN bubble_Salary_currency AS sc ON n.Current_salary_currency = sc.bubbleinternal_id
            LEFT JOIN bubble_Salary_currency AS scc ON n.Desired_salary_currency = scc.bubbleinternal_id
            LEFT JOIN bubble_recruiter_screen_notice_period_dropdown AS p ON n.start_date_dropdown = p.bubbleinternal_id
            LEFT JOIN bubble_recruiter_screen_visa_dropdown AS v ON n.visa_dropdown = v.bubbleinternal_id
        """)
        _log_count(con, "final_screen")
    except Exception as e:
        log.warning(f"  Skipping final_screen: {e}")

    log.info("All transformations complete!")


def _log_count(con, table_name):
    count = con.execute(f'SELECT count(*) FROM "{table_name}"').fetchone()[0]
    log.info(f"  → {table_name}: {count:,} rows")


# ---------------------------------------------------------------------------
# JSON Export
# ---------------------------------------------------------------------------

def export_dashboard_json(con: duckdb.DuckDBPyConnection, output_dir: Path):
    """Export transformed data to JSON for the React dashboard."""
    output_dir.mkdir(parents=True, exist_ok=True)

    log.info("Exporting dashboard JSON...")

    # Helper to run query and get list of dicts
    def query_to_list(sql):
        result = con.execute(sql).fetchdf()
        # Replace NaT/NaN with None for clean JSON
        result = result.where(result.notna(), None)
        return result.to_dict(orient="records")

    dashboard = {}

    # --- Jobs ---
    dashboard["jobs"] = query_to_list("""
        SELECT
            j.job_id, j.client_id, j.date_created, j.job_title,
            j.job_category, j.job_subcategory, j.job_location,
            j.user_hiring_manager, j.job_recruiter, j.job_sourcer,
            j.is_job_archived, j.is_external_recruiter, j.test, j.executive_search,
            cl.client_name
        FROM final_job j
        LEFT JOIN final_client cl ON j.client_id = cl.client_id
        WHERE j.date_created >= '2025-01-01' OR j.job_id IN (
            SELECT DISTINCT job_id FROM final_candidate_stage
            WHERE date_created >= '2025-01-01'
        )
    """)

    # --- Candidates with stage info ---
    dashboard["candidates"] = query_to_list("""
        SELECT
            c.candidate_id, c.job_id, c.talent_id,
            c.candidate_sourcer, c.reason_not_interested,
            c.hired_salary_eur, c.hired_salary_currency,
            c.is_candidate_duplicated, c.is_candidate_disqualified,
            c.is_candidate_archived, c.is_candidate_createdby_ai,
            c.source,
            cs.stage_current_type, cs.stage_current,
            cs.date_created,
            cs.date_lnkdin_viewed, cs.date_contacted,
            cs.date_screen, cs.date_screen_actual,
            cs.date_interview, cs.date_offer, cs.date_hired,
            cs.automation_emails, cs.automation_connections,
            cs.automation_inmails, cs.automation_messages,
            j.client_id,
            cl.client_name,
            j.job_title
        FROM final_candidate c
        JOIN final_candidate_stage cs ON c.candidate_id = cs.candidate_id
        LEFT JOIN final_job j ON c.job_id = j.job_id
        LEFT JOIN final_client cl ON j.client_id = cl.client_id
        WHERE cs.date_created >= '2025-01-01'
    """)

    # --- Events (aggregated by type per candidate per month) ---
    # Instead of shipping all 3-5M events, we aggregate into useful metrics
    # but keep enough detail for drill-down
    dashboard["events_monthly"] = query_to_list("""
        SELECT
            STRFTIME(CAST(date_created AS DATE), '%Y-%m') AS period,
            who_event_created_for AS recruiter,
            event_type,
            job_id,
            COUNT(*) AS count,
            COUNT(CASE WHEN is_event_duplicated THEN 1 END) AS duplicates,
            COUNT(CASE WHEN automation_is_message_read THEN 1 END) AS reads,
            COUNT(CASE WHEN automation_is_message_replied THEN 1 END) AS replies
        FROM final_event
        WHERE CAST(date_created AS DATE) >= '2025-01-01'
          AND is_event_duplicated = FALSE
        GROUP BY period, who_event_created_for, event_type, job_id
    """)

    # --- Events detail (recent 90 days only, for drill-down) ---
    dashboard["events_recent"] = query_to_list("""
        SELECT
            event_id, candidate_id, talent_id, job_id,
            CAST(date_created AS VARCHAR) AS date_created,
            event_type, moved_to_stage, moved_to_stageType,
            who_created_event, who_event_created_for,
            automation_flow_name, automation_step_name,
            automation_is_message_read, automation_is_message_replied,
            is_event_createdby_ai, is_event_duplicated
        FROM final_event
        WHERE CAST(date_created AS DATE) >= CURRENT_DATE - INTERVAL 90 DAY
          AND is_event_duplicated = FALSE
    """)

    # --- Users (recruiters/sourcers) ---
    dashboard["users"] = query_to_list("""
        SELECT user_id, user_name, user_email, employee_number,
               role_current, sub_role_current
        FROM final_user
    """)

    # --- Clients ---
    dashboard["clients"] = query_to_list("""
        SELECT client_id, client_name, is_client_archived, test
        FROM final_client
        WHERE client_name IS NOT NULL AND client_name != '--None--'
    """)

    # --- Screens (2025+) ---
    try:
        dashboard["screens"] = query_to_list("""
            SELECT screen_id, candidate_id, date_created,
                   current_salary, current_salary_currency,
                   desired_salary, desired_salary_currency,
                   location, rating, user_recruiter,
                   relocation, salary_type, start_date, visa
            FROM final_screen
            WHERE date_created >= '2025-01-01'
        """)
    except Exception:
        dashboard["screens"] = []

    # --- Job Goals ---
    try:
        dashboard["job_goals"] = query_to_list("""
            SELECT goal_id, job_id, goal_number, date_created, date_modified
            FROM final_job_goals
        """)
    except Exception:
        dashboard["job_goals"] = []

    # --- Summary stats for quick loading ---
    dashboard["summary"] = {
        "generated_at": datetime.utcnow().isoformat(),
        "period_from": "2025-01",
        "period_to": datetime.utcnow().strftime("%Y-%m"),
        "total_jobs": len(dashboard["jobs"]),
        "total_candidates": len(dashboard["candidates"]),
        "total_events_monthly": len(dashboard["events_monthly"]),
        "total_users": len(dashboard["users"]),
        "total_clients": len(dashboard["clients"]),
    }

    # Write the JSON
    out_path = output_dir / "recruiting_data.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(dashboard, f, separators=(",", ":"), default=str)

    size_mb = out_path.stat().st_size / (1024 * 1024)
    log.info(f"Dashboard JSON: {out_path} ({size_mb:.1f} MB)")
    log.info(f"  jobs: {len(dashboard['jobs']):,}")
    log.info(f"  candidates: {len(dashboard['candidates']):,}")
    log.info(f"  events_monthly: {len(dashboard['events_monthly']):,}")
    log.info(f"  events_recent: {len(dashboard['events_recent']):,}")
    log.info(f"  users: {len(dashboard['users']):,}")
    log.info(f"  clients: {len(dashboard['clients']):,}")

    return out_path


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    log.info("=" * 60)
    log.info("Tribe Recruiting Dashboard — DuckDB Transform")
    log.info("=" * 60)

    # Use in-memory DuckDB (data fits in RAM after 2025 filter)
    con = duckdb.connect(":memory:")

    # Load raw Bubble data
    load_bubble_tables(con, DATA_DIR)

    # Run transformations
    run_transformations(con)

    # Export dashboard JSON
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    export_dashboard_json(con, OUTPUT_DIR)

    # Also save DuckDB file for ad-hoc querying
    db_path = OUTPUT_DIR / "recruiting.duckdb"
    con.execute(f"EXPORT DATABASE '{OUTPUT_DIR / 'recruiting_export'}' (FORMAT PARQUET)")
    log.info(f"Parquet export saved to {OUTPUT_DIR / 'recruiting_export'}")

    con.close()
    log.info("Done!")


if __name__ == "__main__":
    main()
