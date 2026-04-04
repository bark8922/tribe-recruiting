"""
Parquet export function for Tribe Recruiting Dashboard.

Add this function to transform.py and call it from main() after export_dashboard_json().

Usage in main():
    export_dashboard_json(con, OUTPUT_DIR)
    export_parquet_files(con, OUTPUT_DIR / 'parquet')  # Add this line
"""

import logging
from pathlib import Path
import duckdb

log = logging.getLogger("transform")


def export_parquet_files(con: duckdb.DuckDBPyConnection, output_dir: Path):
    """Export transformed data to Parquet files for DuckDB-WASM frontend."""
    output_dir.mkdir(parents=True, exist_ok=True)

    log.info("Exporting Parquet files for DuckDB-WASM...")

    # Helper to export a single table
    def export_table(table_name: str, sql: str):
        try:
            con.execute(f"""
                COPY (
                    {sql}
                ) TO '{output_dir / f"{table_name}.parquet"}' (FORMAT PARQUET, COMPRESSION ZSTD)
            """)
            size_bytes = (output_dir / f"{table_name}.parquet").stat().st_size
            size_mb = size_bytes / (1024 * 1024)
            log.info(f"  {table_name}.parquet: {size_mb:.1f} MB")
        except Exception as e:
            log.error(f"  Failed to export {table_name}: {e}")
            raise

    # 1. Candidates (main candidate data with stage info)
    export_table("candidates", """
        SELECT
            c.candidate_id, c.job_id, c.talent_id,
            c.candidate_sourcer, c.reason_not_interested,
            c.hired_salary_eur, c.hired_salary_currency,
            c.is_candidate_duplicated, c.is_candidate_disqualified,
            c.is_candidate_archived, c.is_candidate_createdby_ai,
            c.source,
            cs.stage_current_type, cs.stage_current,
            CAST(cs.date_created AS VARCHAR) AS date_created,
            CAST(cs.date_lnkdin_viewed AS VARCHAR) AS date_lnkdin_viewed,
            CAST(cs.date_contacted AS VARCHAR) AS date_contacted,
            CAST(cs.date_screen AS VARCHAR) AS date_screen,
            CAST(cs.date_screen_actual AS VARCHAR) AS date_screen_actual,
            CAST(cs.date_interview AS VARCHAR) AS date_interview,
            CAST(cs.date_offer AS VARCHAR) AS date_offer,
            CAST(cs.date_hired AS VARCHAR) AS date_hired,
            cs.automation_emails, cs.automation_connections,
            cs.automation_inmails, cs.automation_messages,
            j.client_id, j.job_recruiter, j.job_sourcer,
            cl.client_name, j.job_title
        FROM final_candidate c
        JOIN final_candidate_stage cs ON c.candidate_id = cs.candidate_id
        LEFT JOIN final_job j ON c.job_id = j.job_id
        LEFT JOIN final_client cl ON j.client_id = cl.client_id
        ORDER BY cs.date_created DESC
    """)

    # 2. Events monthly (aggregated by period, recruiter, event type, job)
    export_table("events_monthly", """
        SELECT
            STRFTIME(CAST(date_created AS DATE), '%Y-%m') AS period,
            who_event_created_for AS recruiter,
            event_type,
            job_id,
            COUNT(*)::INTEGER AS count,
            COUNT(CASE WHEN is_event_duplicated THEN 1 END)::INTEGER AS duplicates,
            COUNT(CASE WHEN automation_is_message_read THEN 1 END)::INTEGER AS reads,
            COUNT(CASE WHEN automation_is_message_replied THEN 1 END)::INTEGER AS replies
        FROM final_event
        WHERE is_event_duplicated = FALSE
        GROUP BY period, who_event_created_for, event_type, job_id
        ORDER BY period DESC
    """)

    # 3. Events detail (individual events, 2025+)
    export_table("events_detail", """
        SELECT
            event_id, candidate_id, talent_id, job_id,
            CAST(date_created AS VARCHAR) AS date_created,
            event_type, moved_to_stage, moved_to_stageType,
            who_created_event, who_event_created_for,
            automation_flow_name, automation_step_name,
            automation_is_message_read, automation_is_message_replied,
            is_event_createdby_ai, is_event_duplicated
        FROM final_event
        WHERE CAST(date_created AS DATE) >= '2025-01-01'
          AND is_event_duplicated = FALSE
        ORDER BY date_created DESC
    """)

    # 4. Jobs
    export_table("jobs", """
        SELECT
            j.job_id, j.job_title, j.client_id, cl.client_name,
            j.job_recruiter, j.job_sourcer,
            CAST(j.date_created AS VARCHAR) AS date_created,
            j.is_job_archived, j.test
        FROM final_job j
        LEFT JOIN final_client cl ON j.client_id = cl.client_id
    """)

    # 5. Users (recruiters, sourcers)
    export_table("users", """
        SELECT user_id, user_name, user_email, employee_number, role_current, sub_role_current
        FROM final_user
    """)

    # 6. Clients
    export_table("clients", """
        SELECT client_id, client_name, is_client_archived, test
        FROM final_client
        WHERE client_name IS NOT NULL AND client_name != '--None--'
    """)

    # 7. Screens
    export_table("screens", """
        SELECT screen_id, candidate_id, date_created,
               current_salary, current_salary_currency,
               desired_salary, desired_salary_currency,
               location, rating, user_recruiter,
               relocation, salary_type, start_date, visa
        FROM final_screen
    """)

    # 8. Job goals
    export_table("job_goals", """
        SELECT goal_id, job_id, goal_number, date_created, date_modified
        FROM final_job_goals
    """)

    log.info(f"All Parquet files exported to {output_dir}")


# ============================================================================
# HOW TO INTEGRATE INTO transform.py
# ============================================================================
#
# 1. Copy the export_parquet_files() function above into transform.py
#    (anywhere after the imports, before main())
#
# 2. In main(), after the line:
#       export_dashboard_json(con, OUTPUT_DIR)
#
#    Add:
#       export_parquet_files(con, OUTPUT_DIR / 'parquet')
#
# 3. The full main() should look like:
#
#    def main():
#        log.info("=" * 60)
#        log.info("Tribe Recruiting Dashboard — DuckDB Transform")
#        log.info("=" * 60)
#
#        con = duckdb.connect(":memory:")
#        load_bubble_tables(con, DATA_DIR)
#        run_transformations(con)
#
#        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
#        export_dashboard_json(con, OUTPUT_DIR)
#        export_parquet_files(con, OUTPUT_DIR / 'parquet')  # <-- ADD THIS
#
#        db_path = OUTPUT_DIR / "recruiting.duckdb"
#        con.execute(f"EXPORT DATABASE '{OUTPUT_DIR / 'recruiting_export'}' (FORMAT PARQUET)")
#        log.info(f"Parquet export saved to {OUTPUT_DIR / 'recruiting_export'}")
#
#        con.close()
#        log.info("Done!")
