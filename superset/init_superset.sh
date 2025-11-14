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
