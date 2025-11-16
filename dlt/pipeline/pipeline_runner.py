from pipeline.load_raw_tables import load_tables
from pipeline.transform_flatten import create_flattened_observations
from pipeline.transform_pivot import run_pivoting_transformation

import dlt

def run_full_pipeline():
    """Run the complete ETL pipeline: Extract → Transform"""
    print("Starting full ETL pipeline...")

    # Step 1: Extract raw data
    print("Step 1: Extracting raw data...")
    load_tables()

    # Create pipeline object to pass to transform functions
    pipeline = dlt.pipeline(
        pipeline_name="openmrs_etl",
        destination=dlt.destinations.duckdb("/opt/airflow/data/openmrs_etl.duckdb"),
        dataset_name="openmrs_analytics"
    )
    
    # Step 2: Create flattened observations
    print("Step 2: Creating flattened observations...")
    create_flattened_observations(pipeline)
    
     # Step 3: Dynamic pivoting
    print("Step 3: Creating dynamically widened observations...")
    run_pivoting_transformation()
    
    print("Full ETL pipeline completed successfully!")