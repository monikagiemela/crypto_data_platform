#!/bin/bash
sleep 10  # Wait for the database to be ready"
superset db upgrade
superset fab create-admin \
  --username "${ADMIN_USERNAME:-admin}" \
  --firstname Admin \
  --lastname User \
  --email "${ADMIN_EMAIL:-admin@superset.com}" \
  --password "${ADMIN_PASSWORD:-admin}" || true
superset init
echo "Creating/Updating database connection: 'Bitcoin Analytics DB'"
superset set_database_uri \
  -d "Bitcoin Analytics DB" \
  -u "postgresql://postgres:postgres@postgres:5432/crypto_db" || true
echo "Database connection set."

echo "Importing custom dashboards..."
superset import-dashboards \
  -p /app/my_dashboard_export.zip \
  -u "${ADMIN_USERNAME:-admin}" || true
echo "Dashboard import complete."
