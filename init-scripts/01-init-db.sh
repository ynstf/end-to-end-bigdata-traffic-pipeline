#!/bin/bash
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE DATABASE airflow;
    CREATE DATABASE traffic_data;
    GRANT ALL PRIVILEGES ON DATABASE airflow TO spark;
    GRANT ALL PRIVILEGES ON DATABASE traffic_data TO spark;
EOSQL
