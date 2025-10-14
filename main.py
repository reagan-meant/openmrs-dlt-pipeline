from extract_raw import load_tables as extract_raw_data
from transform_flatten import create_flattened_observations, incremental_flattened_observations
from transform_pivot import run_pivoting_transformation

import dlt

def run_full_pipeline():
    """Run the complete ETL pipeline: Extract → Transform"""
    print("Starting full ETL pipeline...")
    
    # Step 1: Extract raw data
    print("Step 1: Extracting raw data...")
    pipeline = extract_raw_data()
    
    # Step 2: Create flattened observations
    print("Step 2: Creating flattened observations...")
    create_flattened_observations(pipeline)
    
     # Step 3: Dynamic pivoting (no hardcoded UUIDs)
    print("Step 3: Creating dynamically widened observations...")
    run_pivoting_transformation()
    
    print("ETL pipeline completed successfully!")

def run_incremental_pipeline(start_date, end_date):
    """Run incremental update"""
    print(f"Starting incremental update from {start_date} to {end_date}...")
    
    pipeline = dlt.pipeline(
        pipeline_name="sql_to_duckdb_pipeline",
        destination="duckdb", 
        dataset_name="sql_to_duckdb_pipeline_data"
    )
    
    # Update both raw data and flattened observations incrementally
    from extract_raw import create_source
    source = create_source()
    
    # Incremental extract
    load_info = pipeline.run(source)
    print("Incremental extract:", load_info)
    
    # Incremental transform
    incremental_flattened_observations(pipeline, start_date, end_date)

if __name__ == '__main__':
    # Run full pipeline
    run_full_pipeline()
    
    # Or run incremental (uncomment to use)
    # run_incremental_pipeline("2024-01-01", "2024-01-02")