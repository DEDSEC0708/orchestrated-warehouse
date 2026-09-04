#!/usr/bin/env bash
#
# 01_create_roles.sh - create the least-privilege roles for the VoltHive stack.
#
# WHEN THIS RUNS
#   The official postgres image executes every file in /docker-entrypoint-initdb.d
#   in alphabetical order, exactly once, on the FIRST boot of an empty data
#   directory. It never runs again while the pgdata volume survives. Statements
#   below are still written to be idempotent, so a developer who re-runs them by
#   hand gets a clean result rather than a confusing half-failure.
#
# WHY A SHELL SCRIPT RATHER THAN A .sql FILE
#   Role passwords come from the environment. The postgres entrypoint runs .sql
#   files with a bare `psql -f`, and psql does not expand shell environment
#   variables inside SQL. A shell script passes them in with `psql --set`, so no
#   password is ever written into a committed file.
#
# ROLES CREATED (rationale in docs/adr/006)
#   airflow      owns and connects to the Airflow metadata database only
#   wh_etl       owns the warehouse; the pipeline's identity
#   cms_owner    owns the simulated source OLTP database
#   cms_reader   READ-ONLY on the source database - what ingestion actually uses
#   wh_analyst   read-only consumer of the warehouse
#
# The superuser (POSTGRES_USER) is used ONLY by this bootstrap and for manual
# administration. No application component ever connects as the superuser.

set -euo pipefail

echo "[init] 01_create_roles.sh: creating least-privilege roles"

require_var() {
    local name="$1"
    if [ -z "${!name:-}" ]; then
        echo "[init] FATAL: required environment variable ${name} is not set." >&2
        echo "[init] Copy .env.example to .env and run scripts/generate_env.sh." >&2
        exit 1
    fi
}

for var in \
    AIRFLOW_DB_USER AIRFLOW_DB_PASSWORD \
    WH_ETL_USER WH_ETL_PASSWORD \
    CMS_OWNER_USER CMS_OWNER_PASSWORD \
    CMS_READER_USER CMS_READER_PASSWORD \
    WH_ANALYST_USER WH_ANALYST_PASSWORD
do
    require_var "$var"
done

# create_role <role_name> <password>
#
# Implementation note: the CREATE/ALTER statement is *generated* by a SELECT and
# executed with psql's \gexec. The obvious alternative - a DO $$ ... $$ block -
# does NOT work here, because psql does not substitute :'variables' inside
# dollar-quoted strings. Building the statement with format() and %I/%L also
# quotes identifiers and literals correctly, so a password containing quotes
# cannot break out of the statement.
#
# The attribute list is spelled out rather than left to defaults, so the
# intended privilege level is visible in the file itself.
create_role() {
    local role="$1"
    local password="$2"

    psql --username "$POSTGRES_USER" --dbname postgres \
         --set ON_ERROR_STOP=1 --quiet \
         --set role_name="$role" --set role_password="$password" <<'SQL'
SELECT format(
    '%s ROLE %I WITH LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE PASSWORD %L',
    CASE WHEN EXISTS (SELECT 1 FROM pg_roles WHERE rolname = :'role_name')
         THEN 'ALTER' ELSE 'CREATE' END,
    :'role_name',
    :'role_password'
)
\gexec
SQL
    echo "[init]   role ready: ${role}"
}

create_role "$AIRFLOW_DB_USER"   "$AIRFLOW_DB_PASSWORD"
create_role "$WH_ETL_USER"       "$WH_ETL_PASSWORD"
create_role "$CMS_OWNER_USER"    "$CMS_OWNER_PASSWORD"
create_role "$CMS_READER_USER"   "$CMS_READER_PASSWORD"
create_role "$WH_ANALYST_USER"   "$WH_ANALYST_PASSWORD"

echo "[init] 01_create_roles.sh: done"
