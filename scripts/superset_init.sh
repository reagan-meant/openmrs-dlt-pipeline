#!/bin/bash
set -e

echo "Initializing Apache Superset..."

# Wait for database to be ready
echo "Waiting for Superset database..."
until PGPASSWORD=$DATABASE_PASSWORD psql -h "$DATABASE_HOST" -U "$DATABASE_USER" -d "$DATABASE_DB" -c '\q'; do
  echo "Postgres is unavailable - sleeping"
  sleep 1
done

echo "Superset database is ready!"

# Initialize Superset database
echo "Upgrading Superset database..."
superset db upgrade

# Create admin user
echo "Creating admin user..."
superset fab create-admin \
  --username "${SUPERSET_ADMIN_USERNAME:-admin}" \
  --firstname Admin \
  --lastname User \
  --email admin@superset.com \
  --password "${SUPERSET_ADMIN_PASSWORD:-admin}" || echo "Admin user already exists"

# Initialize Superset
echo "Initializing Superset..."
superset init

echo "Superset initialization complete!"
