"""
Patient programs transformation - comprehensive flattened patient program with workflow states
"""
import dlt


def create_flattened_patient_program(pipeline):
    """Create comprehensive flattened patient program table with workflow states"""

    # If no pipeline provided, create one
    if pipeline is None:
        pipeline = dlt.pipeline(
            pipeline_name="openmrs_etl",
            destination=dlt.destinations.duckdb("/opt/airflow/data/openmrs_etl.duckdb"),
            dataset_name="openmrs_analytics"
        )
    flatten_sql = """
    CREATE OR REPLACE TABLE openmrs_analytics.flattened_patient_program AS
    SELECT
        -- Patient Program identifiers
        pp.patient_program_id,
        pp.uuid as patient_program_uuid,

        -- Patient information
        pp.patient_id,
        CONCAT(pn.given_name, ' ', pn.family_name) as patient_name,
        pn.given_name as patient_given_name,
        pn.middle_name as patient_middle_name,
        pn.family_name as patient_family_name,
        person.gender as patient_gender,
        person.birthdate as patient_birthdate,
        date_diff('year', CAST(person.birthdate AS DATE), CURRENT_DATE) as patient_age,
        person.dead as patient_dead,
        person.death_date as patient_death_date,

        -- Program information
        prog.program_id,
        prog.name as program_name,
        prog.description as program_description,
        prog.uuid as program_uuid,

        -- Enrollment details
        pp.date_enrolled,
        pp.date_completed,
        CASE WHEN pp.date_completed IS NULL THEN 1 ELSE 0 END as is_active,
        CASE
            WHEN pp.date_completed IS NOT NULL
            THEN date_diff('day', CAST(pp.date_enrolled AS DATE), CAST(pp.date_completed AS DATE))
            ELSE date_diff('day', CAST(pp.date_enrolled AS DATE), CURRENT_DATE)
        END as days_in_program,

        -- Outcome information
        pp.outcome_concept_id,
        outcome_concept_name.name as outcome_name,
        outcome_concept_name.uuid as outcome_uuid,

        -- Location information
        pp.location_id as enrollment_location_id,
        location.name as enrollment_location_name,
        location.city_village as enrollment_location_city,
        location.state_province as enrollment_location_state,
        location.uuid as enrollment_location_uuid,

        -- Current/Latest state information
        ps.patient_state_id as current_state_id,
        ps.start_date as current_state_start_date,
        ps.end_date as current_state_end_date,
        ps.uuid as current_state_uuid,

        -- Workflow state details
        pws.program_workflow_state_id as current_workflow_state_id,
        state_concept_name.name as current_state_name,
        state_concept_name.uuid as current_state_concept_uuid,
        pws.initial as is_initial_state,
        pws.terminal as is_terminal_state,

        -- Workflow information
        pw.program_workflow_id,
        workflow_concept_name.name as workflow_name,
        workflow_concept_name.uuid as workflow_concept_uuid,

        -- Audit fields
        pp.date_created,
        pp.date_changed,
        pp.voided,
        pp.void_reason

    FROM openmrs_analytics.patient_program pp

    -- Join patient information
    LEFT JOIN openmrs_analytics.person person ON pp.patient_id = person.person_id
    LEFT JOIN openmrs_analytics.person_name pn ON pp.patient_id = pn.person_id
        AND pn.preferred = true
        AND pn.voided = 0

    -- Join program information
    LEFT JOIN openmrs_analytics.program prog ON pp.program_id = prog.program_id
        AND prog.retired = 0

    -- Join outcome information
    LEFT JOIN openmrs_analytics.concept_name outcome_concept_name
        ON pp.outcome_concept_id = outcome_concept_name.concept_id
        AND outcome_concept_name.locale_preferred = true
        AND outcome_concept_name.locale = 'en'

    -- Join location
    LEFT JOIN openmrs_analytics.location location ON pp.location_id = location.location_id
        AND location.retired = 0

    -- Join current/most recent patient state (if exists)
    LEFT JOIN LATERAL (
        SELECT * FROM openmrs_analytics.patient_state
        WHERE patient_program_id = pp.patient_program_id
        AND voided = 0
        ORDER BY start_date DESC, date_created DESC
        LIMIT 1
    ) ps ON true

    -- Join workflow state information
    LEFT JOIN openmrs_analytics.program_workflow_state pws
        ON ps.state = pws.program_workflow_state_id
        AND pws.retired = 0
    LEFT JOIN openmrs_analytics.concept_name state_concept_name
        ON pws.concept_id = state_concept_name.concept_id
        AND state_concept_name.locale_preferred = true
        AND state_concept_name.locale = 'en'

    -- Join workflow information
    LEFT JOIN openmrs_analytics.program_workflow pw
        ON pws.program_workflow_id = pw.program_workflow_id
        AND pw.retired = 0
    LEFT JOIN openmrs_analytics.concept_name workflow_concept_name
        ON pw.concept_id = workflow_concept_name.concept_id
        AND workflow_concept_name.locale_preferred = true
        AND workflow_concept_name.locale = 'en'

    WHERE pp.voided = 0
    ORDER BY pp.date_enrolled DESC
    """

    with pipeline.sql_client() as client:
        client.execute(flatten_sql)
    print("Flattened patient program table created successfully!")
