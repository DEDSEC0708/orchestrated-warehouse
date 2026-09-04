#!/usr/bin/env bash
#
# init.sh - one-shot Airflow bootstrap, run by the `airflow-init` service.
#
# WHY A SEPARATE ONE-SHOT SERVICE
#   The scheduler and the webserver must not race each other to migrate the
#   metadata database. Running migrations in a dedicated container that must
#   exit successfully first - and gating both long-running services on
#   `service_completed_successfully` - makes the ordering a fact enforced by
#   Docker rather than a hope based on `sleep`.
#
# EVERY STEP IS IDEMPOTENT
#   This container runs on every `make up`, not only the first. Re-running it
#   must be a no-op, for the same reason every load in this project is
#   idempotent: re-runs are the normal case.

set -euo pipefail

echo "[airflow-init] starting bootstrap"
echo "[airflow-init] airflow version: $(airflow version)"

# ---------------------------------------------------------------------------
# 1. Metadata database migration
#
# `airflow db migrate` is the 2.10 command; `airflow db init` is deprecated.
# It is safe to run repeatedly - on an already-current database it reports that
# there is nothing to do and exits 0.
# ---------------------------------------------------------------------------
echo "[airflow-init] migrating metadata database"
airflow db migrate

# ---------------------------------------------------------------------------
# 2. Admin user for the web UI
#
# Guarded rather than blindly created: `airflow users create` fails on a
# duplicate username, which would abort this script on the second `make up`.
# Credentials come from the environment; there is no default password in code.
# ---------------------------------------------------------------------------
if [ -z "${AIRFLOW_ADMIN_USER:-}" ] || [ -z "${AIRFLOW_ADMIN_PASSWORD:-}" ]; then
    echo "[airflow-init] FATAL: AIRFLOW_ADMIN_USER and AIRFLOW_ADMIN_PASSWORD must be set." >&2
    exit 1
fi

if airflow users list --output plain | awk '{print $2}' | grep -qx "${AIRFLOW_ADMIN_USER}"; then
    echo "[airflow-init] admin user '${AIRFLOW_ADMIN_USER}' already exists, skipping"
else
    echo "[airflow-init] creating admin user '${AIRFLOW_ADMIN_USER}'"
    airflow users create \
        --username "${AIRFLOW_ADMIN_USER}" \
        --password "${AIRFLOW_ADMIN_PASSWORD}" \
        --firstname "${AIRFLOW_ADMIN_FIRSTNAME:-Local}" \
        --lastname "${AIRFLOW_ADMIN_LASTNAME:-Admin}" \
        --role Admin \
        --email "${AIRFLOW_ADMIN_EMAIL:-admin@example.invalid}"
fi

# ---------------------------------------------------------------------------
# 3. Pool used to throttle database-heavy work
#
# `airflow pools set` is an upsert, so this is safe on every run. The pool
# exists from Phase 2 so that later phases can reference it without a
# chicken-and-egg problem; it has no effect until tasks are assigned to it.
#
# Two slots: concurrent bulk loads against a single PostgreSQL container
# contend on I/O, and more parallelism past that point makes a backfill slower,
# not faster.
# ---------------------------------------------------------------------------
echo "[airflow-init] ensuring warehouse_pool exists"
airflow pools set warehouse_pool 2 "Throttles database-heavy tasks and backfills"

# ---------------------------------------------------------------------------
# NOT DONE HERE, on purpose:
#
#   * `airflow connections add` - connections are supplied as AIRFLOW_CONN_*
#     environment variables by docker-compose.yml. Storing them in the metadata
#     database would mean a second copy of every password, encrypted with a
#     Fernet key that is itself in .env. Environment URIs keep exactly one copy.
#
#   * Warehouse DDL - schemas and tables are created in Phase 3 by
#     sql/ddl/**, applied through `make db-init`. Infrastructure bootstrap and
#     schema creation are separate concerns with separate failure modes.
# ---------------------------------------------------------------------------

echo "[airflow-init] bootstrap complete"
