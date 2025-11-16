#!/bin/bash
set -e

# Create Superset database and user
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE DATABASE superset;
    CREATE USER superset WITH PASSWORD 'superset';
    GRANT ALL PRIVILEGES ON DATABASE superset TO superset;
EOSQL

echo "Superset database created successfully"
