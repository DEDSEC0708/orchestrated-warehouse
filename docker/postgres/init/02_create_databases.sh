#!/usr/bin/env bash
#
# 02_create_databases.sh - create the three logical databases and lock them down.
#
# THREE DATABASES, ONE INSTANCE
#   warehouse  the platform itself. Owned by wh_etl.
#   cms        the SIMULATED source OLTP system. Owned by cms_owner.
#   airflow    Airflow metadata. Owned by airflow. Never touched by pipeline code.
#
#   They are separate DATABASES rather than separate schemas on purpose: in
#   PostgreSQL a connection cannot query across databases, so the pipeline is
#   forced to perform a real extract from `cms` instead of quietly writing a
#   cross-database join. The constraint is the point.
#
#   In production these would be separate servers. The README says so explicitly
#   rather than implying that a warehouse should share an instance with an OLTP
#   system.
#
# PRIVILEGE MODEL
#   Every database starts by having PUBLIC access revoked. PostgreSQL grants
#   CONNECT on any new database to PUBLIC by default, which would let cms_reader
#   connect to the warehouse and wh_analyst connect to cms. Revoking first and
#   granting explicitly is what makes "least privilege" true rather than assumed.
#
#   Schema-level grants for the warehouse (raw, stg, core, mart, dq, audit, ctl)
#   are deliberately NOT here - those schemas do not exist yet. They arrive with
#   the DDL in Phase 3 (sql/ddl/60_grants.sql).

set -euo pipefail

echo "[init] 02_create_databases.sh: creating databases"

psql_admin() {
    psql --username "$POSTGRES_USER" --dbname "${1:-postgres}" \
         --set ON_ERROR_STOP=1 --quiet "${@:2}"
}

# create_database <database> <owner_role>
create_database() {
    local database="$1"
    local owner="$2"

    # CREATE DATABASE cannot run inside a transaction block or a DO block, so
    # the statement is generated and executed with \gexec, the same technique
    # used for roles in 01.
    psql_admin postgres --set db_name="$database" --set db_owner="$owner" <<'SQL'
SELECT format('CREATE DATABASE %I OWNER %I ENCODING ''UTF8''', :'db_name', :'db_owner')
WHERE NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = :'db_name')
\gexec
SQL
    echo "[init]   database ready: ${database} (owner: ${owner})"
}

create_database "$WH_DB"      "$WH_ETL_USER"
create_database "$CMS_DB"     "$CMS_OWNER_USER"
create_database "$AIRFLOW_DB" "$AIRFLOW_DB_USER"

echo "[init] 02_create_databases.sh: applying privileges"

# --- warehouse -------------------------------------------------------------
# wh_etl owns it and needs DDL (Phase 3 creates schemas; Phase 9 creates monthly
# partitions at runtime). wh_analyst may connect now; its SELECT grants on the
# mart schema come with the DDL in Phase 3.
psql_admin postgres \
    --set wh_db="$WH_DB" --set etl="$WH_ETL_USER" --set analyst="$WH_ANALYST_USER" <<'SQL'
REVOKE ALL ON DATABASE :"wh_db" FROM PUBLIC;
GRANT ALL PRIVILEGES ON DATABASE :"wh_db" TO :"etl";
GRANT CONNECT ON DATABASE :"wh_db" TO :"analyst";
SQL

# The public schema in the warehouse is owned by wh_etl so it can create
# objects there without superuser help. PostgreSQL 15+ already removes CREATE
# on public from PUBLIC; the REVOKE is repeated for explicitness.
psql_admin "$WH_DB" --set etl="$WH_ETL_USER" <<'SQL'
ALTER SCHEMA public OWNER TO :"etl";
REVOKE ALL ON SCHEMA public FROM PUBLIC;
GRANT ALL ON SCHEMA public TO :"etl";
SQL

# --- cms (simulated source system) ----------------------------------------
# cms_owner writes (the Phase 4 generator populates it). cms_reader may only
# read - this is the identity the ingestion layer uses, and the restriction is
# what makes "the pipeline cannot corrupt its source" a fact rather than a
# convention.
psql_admin postgres \
    --set cms_db="$CMS_DB" --set owner="$CMS_OWNER_USER" --set reader="$CMS_READER_USER" <<'SQL'
REVOKE ALL ON DATABASE :"cms_db" FROM PUBLIC;
GRANT ALL PRIVILEGES ON DATABASE :"cms_db" TO :"owner";
GRANT CONNECT ON DATABASE :"cms_db" TO :"reader";
SQL

psql_admin "$CMS_DB" \
    --set owner="$CMS_OWNER_USER" --set reader="$CMS_READER_USER" <<'SQL'
ALTER SCHEMA public OWNER TO :"owner";
REVOKE ALL ON SCHEMA public FROM PUBLIC;
GRANT ALL ON SCHEMA public TO :"owner";

-- USAGE lets the reader see the schema; SELECT is granted on tables only.
GRANT USAGE ON SCHEMA public TO :"reader";
GRANT SELECT ON ALL TABLES IN SCHEMA public TO :"reader";

-- DEFAULT PRIVILEGES is the load-bearing line: without it, every table the
-- generator creates LATER would be invisible to cms_reader, and ingestion
-- would fail with "permission denied" for tables that did not exist when this
-- script ran. It applies to objects created by cms_owner specifically.
ALTER DEFAULT PRIVILEGES FOR ROLE :"owner" IN SCHEMA public
    GRANT SELECT ON TABLES TO :"reader";
ALTER DEFAULT PRIVILEGES FOR ROLE :"owner" IN SCHEMA public
    GRANT USAGE, SELECT ON SEQUENCES TO :"reader";
SQL

# --- airflow metadata ------------------------------------------------------
# Fully isolated: only the airflow role may connect. Neither wh_etl nor
# cms_reader can reach it, so a bug in pipeline code cannot corrupt the
# scheduler's state.
psql_admin postgres --set af_db="$AIRFLOW_DB" --set af_user="$AIRFLOW_DB_USER" <<'SQL'
REVOKE ALL ON DATABASE :"af_db" FROM PUBLIC;
GRANT ALL PRIVILEGES ON DATABASE :"af_db" TO :"af_user";
SQL

psql_admin "$AIRFLOW_DB" --set af_user="$AIRFLOW_DB_USER" <<'SQL'
ALTER SCHEMA public OWNER TO :"af_user";
REVOKE ALL ON SCHEMA public FROM PUBLIC;
GRANT ALL ON SCHEMA public TO :"af_user";
SQL

echo "[init] 02_create_databases.sh: done"
echo "[init] databases: ${WH_DB}, ${CMS_DB}, ${AIRFLOW_DB}"
