from extract_raw import load_tables
from transform_flatten import create_flattened_observations, incremental_flattened_observations
from transform_pivot import run_pivoting_transformation, run_incremental_pivoting

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

def run_incremental_pipeline(start_date=None, end_date=None):
    """Run incremental update"""
    print(f"Starting incremental update from {start_date} to {end_date}...")
    
    # Step 1: Extract raw data incrementally
    pipeline = load_tables()
    # Incremental flat observation transform
    incremental_flattened_observations(pipeline, start_date, end_date)
    # Incremental pivoting transform    
    run_incremental_pivoting(pipeline, start_date, end_date)
    
    print("Incremental ETL pipeline completed successfully!")

if __name__ == '__main__':
    # Run full pipeline
    run_full_pipeline()
    
    # Or run incremental (uncomment to use)
    #run_incremental_pipeline("", "")