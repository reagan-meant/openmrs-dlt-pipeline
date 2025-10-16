# 🏥 OpenMRS DLT Pipeline

A dynamic data transformation pipeline that converts OpenMRS observation tables from long to wide format using **dlt (Data Load Tool)**. Automatically handles schema evolution for new forms and concepts without hardcoding.

## ✨ Features

- **🔁 Dynamic Pivoting**: Automatically widens OpenMRS observations from long to wide format
- **🔄 Schema Evolution**: Handles new forms and concepts without code changes
- **📊 Multi-Value Support**: Processes coded, numeric, text, datetime, and drug observations
- **⚡ Incremental Processing**: Efficiently processes only new data
- **🎯 Database Agnostic**: Works with DuckDB, PostgreSQL, BigQuery, and more
- **🔍 One-Hot Encoding**: Creates binary indicators for categorical data

## 🏗️ Architecture
Raw OpenMRS Data → Flattened Observations → Widened Analytics Table

↓ ↓ ↓

Extract raw Join concepts Pivot to wide
SQL tables and encounters format with
dynamic columns


## 🚀 Quick Start

### 1. Prerequisites

```bash
# Clone the repository
git clone https://github.com/reagan-meant/openmrs-dlt-pipeline.git
cd openmrs-dlt-pipeline

# Create virtual environment
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
2. Run the Pipeline
bash
# Run the complete ETL pipeline
python main.py
This executes the three-step transformation:

Extract: Loads raw OpenMRS tables

Transform: Flattens observations with joins

Load: Creates widened table with dynamic pivoting

📁 Project Structure
openmrs-dlt-pipeline/
├── extract_raw.py          # Raw data extraction
├── transform_flatten.py    # Observation flattening
├── transform_pivot.py      # Dynamic widening
├── main.py                 # Pipeline orchestration
└── requirements.txt       # Python dependencies
🛠️ Configuration
Database Connection
Configure your source database in dlt.config.toml:

toml
[source]
database_url = "your-database-connection-string"

[destination]
destination = "duckdb"  # or postgres, bigquery, etc.
dataset_name = "openmrs_analytics"
Supported Destinations
DuckDB (default) - Local analytics database

PostgreSQL - Production relational database

BigQuery - Cloud data warehouse

Snowflake - Enterprise data platform

📊 Output Schema
Input (Long Format)
| person_id | concept_name          | value_coded_name | value_numeric |
|-----------|-----------------------|------------------|---------------|
| 1         | "Scheduled visit"     | "Yes"            | NULL          |
| 1         | "Weight"              | NULL             | 65.5          |
Output (Wide Format)

| person_id | scheduled_visit_yes | weight_value | visit_type_follow_up|
|-----------|---------------------|--------------|---------------------|
| 1         | 1                   | 65.5         | 1                   |
| 2         | 0                   | 72.0         | 0                   |

```
📈 Monitoring
View Pipeline Status
bash
# Show pipeline information
dlt pipeline openmrs_etl show

# Check load history
dlt pipeline openmrs_etl trace
Streamlit Dashboard (Optional)
bash
# Launch interactive dashboard
streamlit run dashboard.py

# 🤝 Contributing

Fork the repository

Create a feature branch (git checkout -b feature/amazing-feature)

Commit your changes (git commit -m 'Add amazing feature')

Push to the branch (git push origin feature/amazing-feature)

Open a Pull Request

# 📄 License
This project is licensed under the MIT License - see the LICENSE file for details.

# 🙏 Acknowledgments
OpenMRS - Open source medical records system

dlt - Data Load Tool for Python

DuckDB - In-process analytical database

Built for healthcare data transformation 🏥 + modern data tools ⚡


