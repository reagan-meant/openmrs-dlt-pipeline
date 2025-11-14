import os

# Configuration settings
DB_PATH = os.getenv('DB_PATH', '/opt/airflow/data/openmrs_etl.duckdb')
DATASET_NAME = 'openmrs_analytics'
PIPELINE_NAME = 'openmrs_etl'