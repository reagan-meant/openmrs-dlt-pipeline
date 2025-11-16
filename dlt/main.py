from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
import os

# Change working directory to dlt folder so .dlt config can be found
# This is required for dlt to locate the .dlt/ configuration directory
os.chdir('/opt/airflow/dlt')

from pipeline.pipeline_runner import run_full_pipeline
from pipeline.transform_pivot import incremental_widened_observations

default_args = {
    'owner': 'openmrs',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def run_incremental_etl():
    """Wrapper for incremental updates - runs full pipeline on first run"""
    import duckdb
    import os

    # Check if this is the first run by seeing if the database exists
    db_path = "/opt/airflow/data/openmrs_etl.duckdb"
    is_first_run = not os.path.exists(db_path)

    if is_first_run:
        print("First run detected - running full ETL pipeline...")
        run_full_pipeline()
    else:
        # Check if flattened_observations table exists
        try:
            conn = duckdb.connect(db_path)
            conn.execute("SELECT 1 FROM openmrs_analytics.flattened_observations LIMIT 1")
            conn.close()
            print("Tables exist - running incremental update...")
            pipeline = None  # Let the function create its own pipeline
            incremental_widened_observations(pipeline)
        except Exception as e:
            print(f"Tables don't exist - running full ETL pipeline: {e}")
            run_full_pipeline()

def run_full_etl():
    """Force full pipeline execution - useful for reprocessing or schema changes"""
    print("Force running full ETL pipeline...")
    run_full_pipeline()

# =====================================================
# DAG 1: Incremental ETL Pipeline (Scheduled)
# =====================================================
with DAG(
    'openmrs_etl_incremental',
    default_args=default_args,
    description='OpenMRS Incremental ETL Pipeline - Auto-detects first run',
    schedule_interval=timedelta(hours=1),
    catchup=False,
    tags=['openmrs', 'etl', 'healthcare', 'incremental'],
) as incremental_dag:

    start = DummyOperator(task_id='start')

    # Run incremental updates (will handle both initial load and updates)
    incremental_etl = PythonOperator(
        task_id='incremental_etl_update',
        python_callable=run_incremental_etl,
    )

    end = DummyOperator(task_id='end')

    # Workflow: incremental updates (handles both initial and incremental)
    start >> incremental_etl >> end

# =====================================================
# DAG 2: Full ETL Pipeline (Manual Trigger Only)
# =====================================================
with DAG(
    'openmrs_etl_full_reload',
    default_args=default_args,
    description='OpenMRS Full ETL Pipeline - Complete data reload (manual trigger only)',
    schedule_interval=None,  # Manual trigger only
    catchup=False,
    tags=['openmrs', 'etl', 'healthcare', 'full-reload'],
) as full_dag:

    start_full = DummyOperator(task_id='start')

    # Always run full pipeline
    full_etl = PythonOperator(
        task_id='full_etl_reload',
        python_callable=run_full_etl,
    )

    end_full = DummyOperator(task_id='end')

    # Workflow: full reload
    start_full >> full_etl >> end_full