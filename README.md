# OpenMRS DLT Pipeline

A production-ready healthcare data transformation pipeline that converts OpenMRS observation tables from long to wide format using **dlt (Data Load Tool)**. Supports both standalone execution and orchestrated Airflow deployment with incremental updates.

## Overview

This pipeline extracts data from OpenMRS (Open Medical Records System), flattens observations by joining with concepts and encounters, and dynamically pivots them into a wide-format analytics table suitable for reporting and analysis.

**Key Features:**
- Dynamic schema evolution - handles new forms and concepts without code changes
- Incremental processing - efficiently processes only changed data
- Multi-value type support - coded, numeric, text, datetime, and drug observations
- Production orchestration - Airflow DAG with hourly incremental updates
- Flexible deployment - standalone scripts or containerized Airflow

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                        OpenMRS MySQL Database                        │
│           (35+ tables: obs, encounter, visit, concept, etc.)        │
└──────────────────────────────────┬──────────────────────────────────┘
                                   │
                                   │ Extract
                                   ▼
                          ┌─────────────────┐
                          │  extract_raw.py │
                          │  (dlt pipeline) │
                          └────────┬────────┘
                                   │
                                   │ Load to DuckDB
                                   ▼
                          ┌─────────────────┐
                          │   Raw Tables    │
                          │   (staging)     │
                          └────────┬────────┘
                                   │
                                   │ Transform + Join
                                   ▼
                    ┌──────────────────────────────┐
                    │   transform_flatten.py       │
                    │ (join concepts, encounters,  │
                    │  visits, locations)          │
                    └──────────────┬───────────────┘
                                   │
                                   │ Create flattened_observations
                                   ▼
                    ┌──────────────────────────────┐
                    │   Flattened Table            │
                    │ (person_id, concept_name,    │
                    │  values, encounter context)  │
                    └──────────────┬───────────────┘
                                   │
                                   │ Pivot Dynamically
                                   ▼
                    ┌──────────────────────────────┐
                    │   transform_pivot.py         │
                    │ (dynamic column creation     │
                    │  based on concept types)     │
                    └──────────────┬───────────────┘
                                   │
                                   │ Create widened_observations
                                   ▼
                    ┌──────────────────────────────┐
                    │   Analytics Table            │
                    │ (person + encounter +        │
                    │  dynamic concept columns)    │
                    └──────────────────────────────┘
```

### Data Transformation Example

**Input (Long Format):**
```
person_id | concept_name      | value_coded_name | value_numeric
----------|-------------------|------------------|---------------
1         | Scheduled visit   | Yes              | NULL
1         | Weight (kg)       | NULL             | 65.5
1         | Visit type        | Follow-up        | NULL
```

**Output (Wide Format):**
```
person_id | scheduled_visit_yes | weight_kg_value | visit_type_follow_up
----------|---------------------|-----------------|---------------------
1         | 1                   | 65.5            | 1
```

## Project Structure

```
openmrs-dlt-pipeline/
├── dlt/                          # Core ETL pipeline code
│   ├── extract_raw.py            # Extract 35+ OpenMRS tables from MySQL
│   ├── transform_flatten.py      # Join observations with concepts/encounters
│   ├── transform_pivot.py        # Dynamic pivoting to wide format
│   ├── main.py                   # Pipeline orchestration (full + incremental)
│   ├── run_scheduled.py          # Cron-compatible incremental runner
│   ├── requirements.txt          # Python dependencies
│   ├── .dlt/
│   │   ├── config.toml           # dlt runtime configuration
│   │   └── secrets.toml          # Database credentials (gitignored)
│   └── README.md                 # Detailed pipeline documentation
│
├── airflow/                      # Airflow orchestration
│   ├── dags/
│   │   └── openmrs_etl_dag.py    # Hourly incremental ETL DAG
│   ├── include/
│   │   └── config.py             # Airflow-specific configuration
│   └── data/
│       └── openmrs_etl.duckdb    # DuckDB output database
│
├── scripts/                      # Database initialization scripts
│   ├── init-openmrs-db.sql       # OpenMRS schema creation
│   └── load-sample-data.sql      # Sample clinical data
│
├── docker compose.yaml           # Production Airflow deployment (with MySQL)
└── README.md                     # This file
```

## Quick Start

### Option 1: Standalone Execution (Development)

**Prerequisites:**
- Python 3.8+
- Access to OpenMRS MySQL database
- pip

**Setup:**
```bash
# Navigate to the dlt directory
cd dlt

# Create virtual environment
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

**Configure Database Connection:**

Edit `dlt/.dlt/secrets.toml`:
```toml
[sources.mysql_database]
drivername = "mysql+pymysql"
database = "openmrs"
username = "your_username"
password = "your_password"
host = "localhost"
port = 3307
```

**Run the Pipeline:**
```bash
# Full ETL (extract + flatten + pivot)
python main.py

# Or run individual steps
python extract_raw.py           # Extract raw tables
python transform_flatten.py     # Flatten observations
python transform_pivot.py       # Pivot to wide format

# For scheduled incremental updates (cron-friendly)
python run_scheduled.py
```

### Option 2: Airflow Deployment (Production)

**Prerequisites:**
- Docker & Docker Compose
- 4GB+ available RAM

**Setup:**

1. **Start all services (Airflow + OpenMRS MySQL):**
```bash
# From project root
docker compose up -d
```

This will start:
- **OpenMRS MySQL Database** (http://localhost:3307) - Pre-loaded with sample data
- **Airflow Webserver** (http://localhost:8080) - ETL orchestration
- **Apache Superset** (http://localhost:8088) - Data visualization and dashboards
- Airflow Scheduler
- Celery Worker
- PostgreSQL (Airflow metadata)
- PostgreSQL (Superset metadata)
- Redis (Celery broker)

**Initial startup takes 2-3 minutes** as the MySQL database initializes with the OpenMRS schema and sample clinical data.

2. **Verify MySQL is ready:**
```bash
# Check MySQL health
docker compose ps openmrs-mysql

# Or connect to verify data
docker exec -it openmrs-mysql mysql -uopenmrs -popenmrs openmrs -e "SELECT COUNT(*) FROM patient;"
```

Expected output: `5` patients in sample data.

3. **Access Airflow UI:**
- URL: http://localhost:8080
- Username: `admin` (default)
- Password: `admin` (default)

To customize credentials, set environment variables:
```bash
export AIRFLOW_USERNAME=myuser
export AIRFLOW_PASSWORD=mypassword
docker compose up -d
```

4. **Database Connection (already configured for Docker):**

The pipeline is pre-configured to connect to the containerized MySQL database. No additional configuration needed!

If you need to connect to an external OpenMRS database, edit `dlt/.dlt/secrets.toml`:
```toml
[sources.mysql_database]
drivername = "mysql+pymysql"
database = "openmrs"
username = "your_username"
password = "your_password"
host = "your_host"  # Change from "openmrs-mysql" to external host
port = 3306
```

5. **Enable the DAG:**
- Navigate to http://localhost:8080
- Find `openmrs_etl_pipeline` DAG
- Toggle it ON
- The pipeline will run hourly with incremental updates

6. **Monitor the first run:**
- Click on the DAG name
- View the Graph or Grid view
- Check task logs for successful extraction

**Stop all services:**
```bash
docker compose down
```

**Remove all data (including MySQL):**
```bash
docker compose down -v
```

**Restart with fresh data:**
```bash
# Remove volumes and restart
docker compose down -v
docker compose up -d
```

## Sample Data

The Docker deployment includes a pre-configured OpenMRS MySQL database with sample clinical data for testing the ETL pipeline.

### What's Included

**5 Sample Patients** with realistic clinical scenarios:
- Patient demographics (age, gender)
- Medical record numbers (MRN identifiers)
- Multiple visits and encounters

**Clinical Data:**
- **Vital Signs:** Weight, height, blood pressure, heart rate, temperature, respiratory rate, oxygen saturation
- **Lab Results:** CD4 counts, hemoglobin, blood glucose
- **HIV Care Data:** ARV status, WHO staging, HIV test dates
- **Clinical Notes:** Chief complaints, medical history
- **Visit Types:** Outpatient, emergency, follow-up, HIV clinic visits
- **12 Clinical Encounters** across different visit types and dates
- **50+ Observations** spanning numeric, coded, text, and datetime values

### Exploring Sample Data

**Connect to MySQL:**
```bash
# Using docker exec
docker exec -it openmrs-mysql mysql -uopenmrs -popenmrs openmrs

# Using MySQL client on host
mysql -h 127.0.0.1 -P 3307 -uopenmrs -popenmrs openmrs
```

**Sample Queries:**
```sql
-- View patients
SELECT p.person_id, p.gender, p.birthdate, pi.identifier
FROM person p
JOIN patient_identifier pi ON p.person_id = pi.patient_id;

-- View recent encounters
SELECT e.encounter_id, p.person_id, et.name AS encounter_type,
       e.encounter_datetime, l.name AS location
FROM encounter e
JOIN person p ON e.patient_id = p.person_id
JOIN encounter_type et ON e.encounter_type = et.encounter_type_id
JOIN location l ON e.location_id = l.location_id
ORDER BY e.encounter_datetime DESC;

-- View observations with concept names
SELECT o.obs_id, o.person_id, cn.name AS concept_name,
       o.value_numeric, o.value_text,
       c2.name AS coded_value, o.obs_datetime
FROM obs o
JOIN concept_name cn ON o.concept_id = cn.concept_id AND cn.locale_preferred = 1
LEFT JOIN concept_name c2 ON o.value_coded = c2.concept_id AND c2.locale_preferred = 1
ORDER BY o.obs_datetime DESC
LIMIT 20;

-- Count observations by type
SELECT cn.name AS concept_name, COUNT(*) AS count
FROM obs o
JOIN concept_name cn ON o.concept_id = cn.concept_id AND cn.locale_preferred = 1
GROUP BY cn.name
ORDER BY count DESC;
```

### Using Real OpenMRS Data

To connect to a real OpenMRS instance instead of sample data:

1. **Option A:** Keep the containerized MySQL and import real data
   ```bash
   # Export from real OpenMRS
   mysqldump -h real-openmrs-host -u user -p openmrs > openmrs-real-data.sql

   # Import to container
   docker exec -i openmrs-mysql mysql -uopenmrs -popenmrs openmrs < openmrs-real-data.sql
   ```

2. **Option B:** Point directly to external OpenMRS database
   - Update `dlt/.dlt/secrets.toml` with external database credentials
   - Comment out or remove the `openmrs-mysql` service from `docker compose.yaml`

## Configuration

### Database Source

Configure your OpenMRS MySQL connection in `dlt/.dlt/secrets.toml`:

```toml
[sources.mysql_database]
drivername = "mysql+pymysql"
database = "openmrs"
username = "openmrs"
password = "openmrs"
host = "localhost"
port = 3307
```

### Database Destination

Configure output database in `dlt/.dlt/config.toml`:

```toml
[runtime]
log_level = "WARNING"

[pipeline]
pipeline_name = "openmrs_etl"
destination = "duckdb"
dataset_name = "openmrs_analytics"
```

**Supported Destinations:**
- DuckDB (default) - Local analytics database
- PostgreSQL - Production relational database
- BigQuery - Cloud data warehouse
- Snowflake - Enterprise data platform
- Redshift - AWS data warehouse

See [dlt destinations docs](https://dlthub.com/docs/dlt-ecosystem/destinations) for configuration details.

## Pipeline Details

### Step 1: Extract Raw Data (`extract_raw.py`)

Extracts 35+ tables from OpenMRS MySQL database:

**Core Tables:**
- `person`, `patient`, `patient_identifier`
- `encounter`, `encounter_type`, `encounter_provider`
- `obs` (observations - main data table)
- `concept`, `concept_name`, `concept_answer`
- `visit`, `visit_type`
- `location`
- `users`, `provider`
- `orders`, `drug_order`, `drug`
- `patient_program`, `program`, `program_workflow`, `patient_state`
- `form`, `encounter_role`

**Features:**
- Incremental loading using merge strategy
- Tracks changes via `date_created`, `encounter_datetime`, `obs_datetime`
- Preserves data types and relationships

### Step 2: Flatten Observations (`transform_flatten.py`)

Creates `flattened_observations` table by joining observations with metadata:

**Joins:**
- Concept names (preferred English locale)
- Coded value names (for categorical observations)
- Encounter details (type, datetime, form, location)
- Visit details (type, start/stop dates)
- Location information

**Filters:**
- Removes voided (soft-deleted) records
- Supports both full refresh and incremental DELETE+INSERT

**Output Schema:**
```sql
person_id, encounter_id, obs_datetime,
concept_name, value_coded_name, value_numeric, value_text, value_datetime, value_drug,
encounter_type, encounter_datetime, visit_type, location_name, form_name
```

### Step 3: Pivot to Wide Format (`transform_pivot.py`)

Dynamically creates wide-format table based on actual data:

**Column Creation Logic:**

| Value Type | Column Pattern | Example |
|------------|----------------|---------|
| Coded (categorical) | `{concept}_{answer}` | `visit_type_follow_up` = 1/0 |
| Numeric | `{concept}_value` | `weight_kg_value` = 65.5 |
| Text | `{concept}_text` | `symptoms_text` = "fever" |
| Datetime | `{concept}_datetime` | `last_visit_datetime` = "2024-10-15" |
| Drug | `{concept}_drug` | `medication_drug` = 123 |

**Features:**
- Automatic schema discovery from data
- SQL-safe column names (special chars → underscores, max 40 chars)
- Groups by `person_id` + `encounter_id`
- Supports both replace (full) and merge (incremental) modes

## Airflow DAG

**Schedule:** Hourly incremental updates

**Tasks:**
```
start → incremental_etl_update → end
```

**Configuration:**
- Owner: `openmrs`
- Retries: 1 (5-minute delay)
- Catchup: Disabled
- Tags: `openmrs`, `etl`, `healthcare`

**Execution:**
- Runs `incremental_widened_observations()` function
- Processes only changed data since last run
- Updates `airflow/data/openmrs_etl.duckdb`

**Full Refresh:**

The DAG includes a `full_etl_load` task (currently unused) for full refreshes. To use it:

Edit `airflow/dags/openmrs_etl_dag.py`:
```python
# Change from:
start >> incremental_etl >> end

# To:
start >> full_etl >> end
```

## Data Visualization with Apache Superset

Apache Superset is included in the Docker deployment for creating dashboards and visualizations from your transformed OpenMRS data.

### Accessing Superset

1. **Access Superset UI:**
   - URL: http://localhost:8088
   - Username: `admin` (default)
   - Password: `admin` (default)

   To customize credentials, set environment variables in `.env`:
   ```bash
   SUPERSET_ADMIN_USERNAME=myuser
   SUPERSET_ADMIN_PASSWORD=mypassword
   ```

2. **First Time Setup - Connect to DuckDB:**

   Once logged in, you need to connect Superset to your DuckDB database:

   a. Click **Settings** → **Database Connections** → **+ Database**

   b. Select **Other** as the database type

   c. Enter the following SQLAlchemy URI:
   ```
   duckdb:////app/data/openmrs_etl.duckdb
   ```

   d. Test the connection and click **Connect**

   e. In the database settings, enable:
      - **Allow DML** (for queries)
      - **Allow file uploads** (optional)
      - **Expose in SQL Lab**

### Creating Your First Dashboard

1. **Explore the Data:**
   - Go to **SQL Lab** → **SQL Editor**
   - Select your DuckDB database
   - Query the widened observations:
   ```sql
   SELECT * FROM openmrs_analytics.widened_observations LIMIT 100;
   ```

2. **Create a Dataset:**
   - Go to **Datasets** → **+ Dataset**
   - Select Database: `DuckDB`
   - Select Schema: `openmrs_analytics`
   - Select Table: `widened_observations` or `flattened_observations`
   - Click **Create Dataset and Create Chart**

3. **Build Charts:**

   **Example: Patient Vital Signs Trends**
   - Chart Type: **Line Chart**
   - Time Column: `visit_date_started`
   - Metrics: `AVG(weight_kg_value)`, `AVG(systolic_blood_pressure_value)`
   - Dimensions: `person_id`

   **Example: Encounter Type Distribution**
   - Chart Type: **Pie Chart**
   - Metric: `COUNT(*)`
   - Group By: `encounter_type_name`

   **Example: Lab Results by Location**
   - Chart Type: **Bar Chart**
   - Metrics: `COUNT(*)`
   - Group By: `location_name`
   - Filter: `encounter_type_name = 'Lab Results'`

4. **Create a Dashboard:**
   - Go to **Dashboards** → **+ Dashboard**
   - Add your charts by clicking **Edit Dashboard** → **+** button
   - Arrange and resize charts
   - Add filters for interactive exploration
   - Save your dashboard

### Sample Dashboard Ideas

**Clinical Overview Dashboard:**
- Total patients, encounters, and observations
- Encounters by type (pie chart)
- Recent vitals trends (line chart)
- Top locations by visit volume (bar chart)

**Patient Monitoring Dashboard:**
- Weight trends over time
- Blood pressure monitoring
- Lab result history
- Visit frequency by patient

**Operational Dashboard:**
- Daily encounter volume
- Encounter types by location
- Provider productivity metrics
- Data quality metrics (missing values, etc.)

### Superset Features

- **SQL Lab:** Interactive SQL editor with autocomplete
- **Chart Builder:** 50+ visualization types
- **Dashboards:** Interactive, filterable dashboards
- **Alerts & Reports:** Schedule email reports
- **Row-Level Security:** Control data access by user
- **Caching:** Fast query performance

### Troubleshooting Superset

**Cannot connect to DuckDB:**
- Ensure the path `/app/data/openmrs_etl.duckdb` is correct
- Check that the DuckDB file exists: `docker exec superset ls -la /app/data/`
- Verify Superset has read permissions

**Charts not updating:**
- Clear Superset cache: **Data** → **Databases** → click database → **Clear Cache**
- Refresh your dataset metadata
- Check that the Airflow DAG has run successfully

**Container fails to start:**
```bash
# Check Superset logs
docker compose logs superset

# Restart Superset
docker compose restart superset
```

## Monitoring

### Standalone Mode

**View pipeline status:**
```bash
cd dlt
dlt pipeline openmrs_etl show
```

**Check load history:**
```bash
dlt pipeline openmrs_etl trace
```

**Inspect DuckDB output:**
```bash
# Install DuckDB CLI
pip install duckdb

# Query the database
duckdb openmrs_etl.duckdb
```
```sql
-- Show tables
.tables

-- Check row counts
SELECT COUNT(*) FROM openmrs_analytics.widened_observations;

-- Sample data
SELECT * FROM openmrs_analytics.widened_observations LIMIT 10;
```

### Airflow Mode

**Web UI:**
- Access: http://localhost:8080
- View DAG runs, task logs, execution history
- Monitor task duration and success rates

**Logs:**
```bash
# View all container logs
docker compose logs

# Follow specific service
docker compose logs -f airflow-scheduler
docker compose logs -f airflow-worker

# View last 100 lines
docker compose logs --tail=100 airflow-worker
```

**Database:**
```bash
# Access DuckDB from host
duckdb airflow/data/openmrs_etl.duckdb
```

## Troubleshooting

### Connection Issues

**MySQL connection refused:**
```bash
# Test MySQL connectivity
mysql -h localhost -P 3307 -u openmrs -p

# Check if port is correct
netstat -an | grep 3307
```

**Solution:** Verify `dlt/.dlt/secrets.toml` credentials and port.

### Airflow Issues

**DAG not appearing:**
```bash
# Check DAG for syntax errors
docker compose exec airflow-webserver airflow dags list

# View DAG parsing errors
docker compose logs airflow-scheduler | grep ERROR
```

**Dependencies not installed:**
```bash
# Rebuild containers with dependencies
docker compose down
docker compose up -d --build
```

**Check if dlt is installed in Airflow:**
```bash
docker compose exec airflow-worker pip list | grep dlt
```

### Performance Issues

**Slow extraction:**
- Add database indexes on `date_created`, `encounter_datetime`, `obs_datetime`
- Increase incremental batch size
- Use parallel extraction (dlt supports this)

**Pivot operation slow:**
- Reduce number of concepts being pivoted
- Filter concepts in `transform_flatten.py`
- Consider materialized views for frequently queried data

### Data Quality Issues

**Missing columns in wide table:**
- Check if concepts exist in `flattened_observations`
- Verify value types are correctly mapped
- Run full refresh to rebuild schema

**Duplicate records:**
- Check for duplicate primary keys in source data
- Verify incremental merge keys are correct
- Review `date_created` timestamps

## Development

### Running Tests

```bash
cd dlt
pytest tests/  # (tests not yet implemented)
```

### Adding New Transformations

1. Create new Python file in `dlt/`
2. Define dlt resource or transformer
3. Update `main.py` to include in pipeline
4. Update Airflow DAG if needed

### Customizing Pivot Logic

Edit `dlt/transform_pivot.py` to:
- Filter specific concepts
- Change column naming conventions
- Add custom aggregations
- Modify value type mappings

## Security Considerations

1. **Credentials:** Never commit `secrets.toml` to version control
2. **Database Access:** Use read-only MySQL user for extraction
3. **Airflow:** Change default admin credentials in production
4. **Network:** Use VPN/SSH tunnels for remote database access
5. **Data Privacy:** Ensure compliance with HIPAA/GDPR for patient data

## Production Checklist

- [ ] Configure production database credentials
- [ ] Change Airflow admin password
- [ ] Set up external database for Airflow metadata (not SQLite)
- [ ] Configure external storage for DuckDB files (S3, Azure, GCS)
- [ ] Add alerting for pipeline failures (email, Slack, PagerDuty)
- [ ] Set up monitoring and logging aggregation
- [ ] Configure backup strategy for output databases
- [ ] Document data retention policies
- [ ] Add data validation and quality checks
- [ ] Set up CI/CD pipeline for code deployment
- [ ] Review and adjust resource limits (memory, CPU)
- [ ] Enable SSL/TLS for database connections
- [ ] Configure log rotation

## Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Acknowledgments

- [OpenMRS](https://openmrs.org/) - Open source medical records system
- [dlt](https://dlthub.com/) - Data Load Tool for Python
- [Apache Airflow](https://airflow.apache.org/) - Workflow orchestration platform
- [DuckDB](https://duckdb.org/) - In-process analytical database

## Support

For issues, questions, or contributions:
- Open an issue on GitHub
- Review existing documentation in `dlt/README.md`
- Check dlt documentation: https://dlthub.com/docs

---

Built for healthcare data transformation with modern data tools.
