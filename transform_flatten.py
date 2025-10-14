import dlt

def create_flattened_observations(pipeline):
    """Create flattened observations table from raw data"""
    
    # If no pipeline provided, create one
    if pipeline is None:
        pipeline = dlt.pipeline(
            pipeline_name="sql_to_duckdb_pipeline",
            destination="duckdb",
            dataset_name="sql_to_duckdb_pipeline_data"
        )
    flatten_sql = """
    CREATE OR REPLACE TABLE sql_to_duckdb_pipeline_data.flat_observations AS
    SELECT
        obs.obs_id AS obs_id,
        obs.person_id AS person_id,
        obs.concept_id AS concept_id,
        concept_concept_name.name AS concept_name,
        concept_concept_name.uuid AS concept_uuid,
        obs.obs_group_id AS obs_group_id,
        obs.accession_number AS accession_number,
        obs.form_namespace_and_path AS form_namespace_and_path,
        obs.value_coded AS value_coded,
        value_concept_name.name AS value_coded_name,
        value_concept_name.uuid AS value_coded_uuid,
        obs.value_coded_name_id AS value_coded_name_id,
        obs.value_drug AS value_drug,
        obs.value_datetime AS value_datetime,
        obs.value_numeric AS value_numeric,
        obs.value_modifier AS value_modifier,
        obs.value_text AS value_text,
        obs.value_complex AS value_complex,
        obs.comments AS comments,
        obs.creator AS creator,
        obs.date_created AS date_created,
        obs.voided AS obs_voided,
        obs.void_reason AS obs_void_reason,
        obs.previous_version AS previous_version,

        encounter.encounter_id AS encounter_id,
        encounter.voided AS encounter_voided,

        encounter_type.name AS encounter_type_name,
        encounter_type.description AS encounter_type_description,
        encounter_type.uuid AS encounter_type_uuid,
        encounter_type.retired AS encounter_type_retired,

        visit.visit_id AS visit_id,
        visit.date_started AS visit_date_started,
        visit.date_stopped AS visit_date_stopped,

        visit_type.name AS visit_type_name,
        visit_type.uuid AS visit_type_uuid,
        visit_type.retired AS visit_type_retired,

        visit.location_id AS location_id,
        location.name AS location_name,
        location.address1 AS location_address1,
        location.address2 AS location_address2,
        location.city_village AS location_city_village,
        location.state_province AS location_state_province,
        location.postal_code AS location_postal_code,
        location.country AS location_country,
        location.retired AS location_retired,
        location.uuid AS location_uuid

    FROM sql_to_duckdb_pipeline_data.obs AS obs
    LEFT JOIN sql_to_duckdb_pipeline_data.concept_name AS value_concept_name
        ON obs.value_coded = value_concept_name.concept_id
        AND obs.value_coded IS NOT NULL
        AND value_concept_name.locale_preferred = true
        AND value_concept_name.locale = 'en'
    LEFT JOIN sql_to_duckdb_pipeline_data.encounter AS encounter
        ON obs.encounter_id = encounter.encounter_id
    LEFT JOIN sql_to_duckdb_pipeline_data.visit AS visit
        ON encounter.visit_id = visit.visit_id
    LEFT JOIN sql_to_duckdb_pipeline_data.encounter_type AS encounter_type
        ON encounter.encounter_type = encounter_type.encounter_type_id
    LEFT JOIN sql_to_duckdb_pipeline_data.visit_type AS visit_type
        ON visit.visit_type_id = visit_type.visit_type_id
    LEFT JOIN sql_to_duckdb_pipeline_data.location AS location
        ON obs.location_id = location.location_id
    LEFT JOIN sql_to_duckdb_pipeline_data.concept_name AS concept_concept_name
        ON obs.concept_id = concept_concept_name.concept_id
        AND concept_concept_name.locale_preferred = true
        AND concept_concept_name.locale = 'en'
    WHERE obs.voided = 0
      AND encounter.voided = 0
    """
    
    with pipeline.sql_client() as client:
        client.execute(flatten_sql)
    print("Flattened observations table created successfully!")

def incremental_flattened_observations(pipeline, start_date=None, end_date=None):
    """Update flattened observations incrementally"""
    
    where_clause = ""
    if start_date and end_date:
        where_clause = f"WHERE obs.date_created BETWEEN '{start_date}' AND '{end_date}'"
    elif start_date:
        where_clause = f"WHERE obs.date_created >= '{start_date}'"
    
    incremental_sql = f"""
    INSERT OR REPLACE INTO sql_to_duckdb_pipeline_data.flat_observations
    SELECT
        obs.obs_id AS obs_id,
        obs.person_id AS person_id,
        obs.concept_id AS concept_id,
        concept_concept_name.name AS concept_name,
        concept_concept_name.uuid AS concept_uuid,
        obs.obs_group_id AS obs_group_id,
        obs.accession_number AS accession_number,
        obs.form_namespace_and_path AS form_namespace_and_path,
        obs.value_coded AS value_coded,
        value_concept_name.name AS value_coded_name,
        value_concept_name.uuid AS value_coded_uuid,
        obs.value_coded_name_id AS value_coded_name_id,
        obs.value_drug AS value_drug,
        obs.value_datetime AS value_datetime,
        obs.value_numeric AS value_numeric,
        obs.value_modifier AS value_modifier,
        obs.value_text AS value_text,
        obs.value_complex AS value_complex,
        obs.comments AS comments,
        obs.creator AS creator,
        obs.date_created AS date_created,
        obs.voided AS obs_voided,
        obs.void_reason AS obs_void_reason,
        obs.previous_version AS previous_version,

        encounter.encounter_id AS encounter_id,
        encounter.voided AS encounter_voided,

        encounter_type.name AS encounter_type_name,
        encounter_type.description AS encounter_type_description,
        encounter_type.uuid AS encounter_type_uuid,
        encounter_type.retired AS encounter_type_retired,

        visit.visit_id AS visit_id,
        visit.date_started AS visit_date_started,
        visit.date_stopped AS visit_date_stopped,

        visit_type.name AS visit_type_name,
        visit_type.uuid AS visit_type_uuid,
        visit_type.retired AS visit_type_retired,

        visit.location_id AS location_id,
        location.name AS location_name,
        location.address1 AS location_address1,
        location.address2 AS location_address2,
        location.city_village AS location_city_village,
        location.state_province AS location_state_province,
        location.postal_code AS location_postal_code,
        location.country AS location_country,
        location.retired AS location_retired,
        location.uuid AS location_uuid

    FROM sql_to_duckdb_pipeline_data.obs AS obs
    LEFT JOIN sql_to_duckdb_pipeline_data.concept_name AS value_concept_name
        ON obs.value_coded = value_concept_name.concept_id
        AND obs.value_coded IS NOT NULL
        AND value_concept_name.locale_preferred = true
        AND value_concept_name.locale = 'en'
    LEFT JOIN sql_to_duckdb_pipeline_data.encounter AS encounter
        ON obs.encounter_id = encounter.encounter_id
    LEFT JOIN sql_to_duckdb_pipeline_data.visit AS visit
        ON encounter.visit_id = visit.visit_id
    LEFT JOIN sql_to_duckdb_pipeline_data.encounter_type AS encounter_type
        ON encounter.encounter_type = encounter_type.encounter_type_id
    LEFT JOIN sql_to_duckdb_pipeline_data.visit_type AS visit_type
        ON visit.visit_type_id = visit_type.visit_type_id
    LEFT JOIN sql_to_duckdb_pipeline_data.location AS location
        ON obs.location_id = location.location_id
    LEFT JOIN sql_to_duckdb_pipeline_data.concept_name AS concept_concept_name
        ON obs.concept_id = concept_concept_name.concept_id
        AND concept_concept_name.locale_preferred = true
        AND concept_concept_name.locale = 'en'
    {where_clause}
    AND obs.voided = 0
    AND encounter.voided = 0
    """
    
    with pipeline.sql_client() as client:
        client.execute(incremental_sql)
    
    print(f"Incremental update completed for date range: {start_date} to {end_date}")

if __name__ == '__main__':
    # For testing this module independently
    pipeline = dlt.pipeline(
        pipeline_name="sql_to_duckdb_pipeline",
        destination="duckdb",
        dataset_name="sql_to_duckdb_pipeline_data"
    )
    create_flattened_observations(pipeline)