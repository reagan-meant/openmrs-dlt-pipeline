"""
Transform appointments data - flatten and enrich with related metadata
"""
import dlt


def create_flattened_appointments(pipeline):
    """
    Create flattened appointments table with all related metadata.
    Joins patient_appointment with service, service_type, location, provider, and patient info.
    """

    # If no pipeline provided, create one
    if pipeline is None:
        pipeline = dlt.pipeline(
            pipeline_name="openmrs_etl",
            destination=dlt.destinations.duckdb("/opt/airflow/data/openmrs_etl.duckdb"),
            dataset_name="openmrs_analytics"
        )

    flatten_sql = """
    CREATE OR REPLACE TABLE openmrs_analytics.flattened_appointments AS
    SELECT
        -- Appointment identifiers
        pa.patient_appointment_id,
        pa.appointment_number,
        pa.uuid as appointment_uuid,

        -- Patient information
        pa.patient_id,
        CONCAT(pn.given_name, ' ', pn.family_name) as patient_name,
        person.gender as patient_gender,
        date_diff('year', person.birthdate, CURRENT_DATE) as patient_age,

        -- Appointment timing
        pa.start_date_time,
        pa.end_date_time,
        date_diff('minute', pa.start_date_time, pa.end_date_time) as duration_minutes,
        pa.date_appointment_scheduled,

        -- Appointment details
        pa.status,
        pa.appointment_kind,
        pa.priority,
        pa.comments,

        -- Service information
        pa.appointment_service_id,
        aps.name as service_name,
        aps.description as service_description,
        aps.duration_mins as service_default_duration,
        aps.start_time as service_start_time,
        aps.end_time as service_end_time,

        -- Service type information
        pa.appointment_service_type_id,
        apst.name as service_type_name,
        apst.duration_mins as service_type_duration,

        -- Location information
        pa.location_id,
        l.name as location_name,
        l.description as location_description,

        -- Provider information
        pa.provider_id,
        CONCAT(provider_pn.given_name, ' ', provider_pn.family_name) as provider_name,

        -- Related appointment (for rescheduled/cancelled appointments)
        pa.related_appointment_id,

        -- Telehealth
        pa.tele_health_video_link,
        CASE WHEN pa.tele_health_video_link IS NOT NULL THEN 1 ELSE 0 END as is_telehealth,

        -- Audit fields
        pa.date_created,
        pa.date_changed,
        pa.voided,
        pa.void_reason

    FROM openmrs_analytics.patient_appointment pa

    -- Join patient information
    LEFT JOIN openmrs_analytics.person person ON pa.patient_id = person.person_id
    LEFT JOIN openmrs_analytics.person_name pn ON pa.patient_id = pn.person_id
        AND pn.preferred = 1
        AND pn.voided = 0

    -- Join service information
    LEFT JOIN openmrs_analytics.appointment_service aps ON pa.appointment_service_id = aps.appointment_service_id
        AND aps.voided = 0

    -- Join service type information
    LEFT JOIN openmrs_analytics.appointment_service_type apst ON pa.appointment_service_type_id = apst.appointment_service_type_id
        AND apst.voided = 0

    -- Join location
    LEFT JOIN openmrs_analytics.location l ON pa.location_id = l.location_id
        AND l.retired = 0

    -- Join provider information
    LEFT JOIN openmrs_analytics.provider prov ON pa.provider_id = prov.provider_id
        AND prov.retired = 0
    LEFT JOIN openmrs_analytics.person_name provider_pn ON prov.person_id = provider_pn.person_id
        AND provider_pn.preferred = 1
        AND provider_pn.voided = 0

    WHERE pa.voided = 0
    ORDER BY pa.start_date_time DESC
    """

    with pipeline.sql_client() as client:
        client.execute(flatten_sql)
    print("Flattened appointments table created successfully!")


def incremental_flattened_appointments(pipeline, start_date=None, end_date=None):
    """
    Incrementally update flattened appointments based on date_changed or date_created.
    Uses DELETE + INSERT pattern to handle updates.
    """
    if pipeline is None:
        pipeline = dlt.pipeline(
            pipeline_name="openmrs_etl",
            destination=dlt.destinations.duckdb("/opt/airflow/data/openmrs_etl.duckdb"),
            dataset_name="openmrs_analytics"
        )

    # Check if dates are provided
    if start_date is None and end_date is None:
        # No dates provided - get latest data from destination
        with pipeline.sql_client() as client:
            result = client.execute_sql("""
                SELECT MAX(date_changed) as last_date
                FROM openmrs_analytics.flattened_appointments
            """)
            last_date = result[0][0] if result and result[0][0] else None

        if last_date:
            # Incremental update from last date
            start_date = last_date
            end_date = None
            print(f"Auto: Incremental update since last date: {last_date}")

    where_clause = ""
    if start_date and end_date:
        where_clause = f"WHERE (pa.date_changed BETWEEN '{start_date}' AND '{end_date}' OR pa.date_created BETWEEN '{start_date}' AND '{end_date}')"
    elif start_date:
        where_clause = f"WHERE (pa.date_changed >= '{start_date}' OR pa.date_created >= '{start_date}')"

    # First delete existing records for the date range
    delete_sql = f"""
    DELETE FROM openmrs_analytics.flattened_appointments
    WHERE patient_appointment_id IN (
        SELECT patient_appointment_id FROM openmrs_analytics.patient_appointment
        {where_clause.replace('pa.', '')}
    )
    """

    # Then insert new/updated records
    insert_sql = f"""
    INSERT INTO openmrs_analytics.flattened_appointments
    SELECT
        -- Appointment identifiers
        pa.patient_appointment_id,
        pa.appointment_number,
        pa.uuid as appointment_uuid,

        -- Patient information
        pa.patient_id,
        CONCAT(pn.given_name, ' ', pn.family_name) as patient_name,
        person.gender as patient_gender,
        date_diff('year', person.birthdate, CURRENT_DATE) as patient_age,

        -- Appointment timing
        pa.start_date_time,
        pa.end_date_time,
        date_diff('minute', pa.start_date_time, pa.end_date_time) as duration_minutes,
        pa.date_appointment_scheduled,

        -- Appointment details
        pa.status,
        pa.appointment_kind,
        pa.priority,
        pa.comments,

        -- Service information
        pa.appointment_service_id,
        aps.name as service_name,
        aps.description as service_description,
        aps.duration_mins as service_default_duration,
        aps.start_time as service_start_time,
        aps.end_time as service_end_time,

        -- Service type information
        pa.appointment_service_type_id,
        apst.name as service_type_name,
        apst.duration_mins as service_type_duration,

        -- Location information
        pa.location_id,
        l.name as location_name,
        l.description as location_description,

        -- Provider information
        pa.provider_id,
        CONCAT(provider_pn.given_name, ' ', provider_pn.family_name) as provider_name,

        -- Related appointment
        pa.related_appointment_id,

        -- Telehealth
        pa.tele_health_video_link,
        CASE WHEN pa.tele_health_video_link IS NOT NULL THEN 1 ELSE 0 END as is_telehealth,

        -- Audit fields
        pa.date_created,
        pa.date_changed,
        pa.voided,
        pa.void_reason

    FROM openmrs_analytics.patient_appointment pa

    -- Join patient information
    LEFT JOIN openmrs_analytics.person person ON pa.patient_id = person.person_id
    LEFT JOIN openmrs_analytics.person_name pn ON pa.patient_id = pn.person_id
        AND pn.preferred = 1
        AND pn.voided = 0

    -- Join service information
    LEFT JOIN openmrs_analytics.appointment_service aps ON pa.appointment_service_id = aps.appointment_service_id
        AND aps.voided = 0

    -- Join service type information
    LEFT JOIN openmrs_analytics.appointment_service_type apst ON pa.appointment_service_type_id = apst.appointment_service_type_id
        AND apst.voided = 0

    -- Join location
    LEFT JOIN openmrs_analytics.location l ON pa.location_id = l.location_id
        AND l.retired = 0

    -- Join provider information
    LEFT JOIN openmrs_analytics.provider prov ON pa.provider_id = prov.provider_id
        AND prov.retired = 0
    LEFT JOIN openmrs_analytics.person_name provider_pn ON prov.person_id = provider_pn.person_id
        AND provider_pn.preferred = 1
        AND provider_pn.voided = 0

    {where_clause}
    AND pa.voided = 0
    ORDER BY pa.start_date_time DESC
    """

    with pipeline.sql_client() as client:
        client.execute("BEGIN TRANSACTION")
        client.execute(delete_sql)
        client.execute(insert_sql)
        client.execute("COMMIT")

    print(f"Incremental update completed for flattened_appointments: {start_date} to {end_date}")
