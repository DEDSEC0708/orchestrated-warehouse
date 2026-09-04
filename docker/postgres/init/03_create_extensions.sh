#!/usr/bin/env bash
#
# 03_create_extensions.sh - install the extensions the warehouse schema needs.
#
# WHEN THIS RUNS
#   Third and last of the /docker-entrypoint-initdb.d scripts, on the FIRST
#   boot of an empty data directory only. Statements are idempotent
#   (CREATE EXTENSION IF NOT EXISTS) so running them by hand is harmless.
#
# WHY A SEPARATE SCRIPT
#   CREATE EXTENSION requires superuser, and the databases it targets do not
#   exist until 02_create_databases.sh has run. Neither the pipeline role nor
#   the DDL scripts can install an extension, so it has to happen here, once,
#   as the bootstrap superuser. Putting it in the schema DDL instead would
#   mean every developer needed superuser to apply the schema - which is
#   exactly the privilege level the rest of this stack works to avoid.
#
# WHAT IS INSTALLED AND WHY
#
#   btree_gist  in the WAREHOUSE database.
#
#     The SCD Type 2 dimensions declare
#
#         EXCLUDE USING gist (business_key WITH =,
#                             tstzrange(effective_from_utc,
#                                       effective_to_utc) WITH &&)
#
#     which is the constraint that makes overlapping validity periods
#     IMPOSSIBLE rather than merely unlikely. A partial unique index already
#     guarantees one current row per key; it does NOT stop two historical
#     versions from claiming the same instant, which is the failure mode that
#     silently doubles a point-in-time join.
#
#     A gist index cannot mix an equality operator on a scalar column with an
#     overlap operator on a range without btree_gist supplying the scalar
#     operator class. Hence this extension, and hence a schema that DEGRADES
#     rather than fails when it is missing: sql/ddl/30_core_dims.sql wraps the
#     constraints in a guarded DO block and raises a WARNING instead. That
#     keeps the project runnable on a managed PostgreSQL where the extension
#     is unavailable, at the cost of one layer of protection - a tradeoff
#     stated in the schema file rather than hidden.
#
# WHAT IS DELIBERATELY NOT INSTALLED
#
#   pg_stat_statements  would need shared_preload_libraries and a restart, and
#                       nothing in this project reads it. Adding a dependency
#                       for a feature nobody uses is how a stack becomes
#                       impossible to reproduce.
#   uuid-ossp / pgcrypto  gen_random_uuid() has been in core PostgreSQL since
#                       13 and sha256() since 11. Both extensions would be
#                       pure ceremony on PostgreSQL 16.
#   postgis             the model stores latitude and longitude as plain
#                       NUMERIC and does no spatial querying. See
#                       docs/adr/010 for why the geography type was rejected.

set -euo pipefail

echo "[init] 03_create_extensions.sh: installing extensions"

if [ -z "${WH_DB:-}" ]; then
    echo "[init] FATAL: required environment variable WH_DB is not set." >&2
    echo "[init] Copy .env.example to .env and run scripts/generate_env.sh." >&2
    exit 1
fi

# create_extension_in <database> <extension>
#
# ON_ERROR_STOP makes a missing contrib package a hard failure here rather
# than a warning nobody reads. If the image genuinely cannot provide
# btree_gist the right response is to know at bootstrap, not to discover it
# from a degraded constraint set three phases later.
create_extension_in() {
    local database="$1"
    local extension="$2"

    psql --username "$POSTGRES_USER" --dbname "$database" \
         --set ON_ERROR_STOP=1 --quiet \
         --command "CREATE EXTENSION IF NOT EXISTS ${extension};"
    echo "[init]   ${database}: ${extension} ready"
}

create_extension_in "$WH_DB" "btree_gist"

# Confirm rather than assume. A silent no-op here would leave the exclusion
# constraints quietly absent, which is precisely the class of failure this
# project treats as unacceptable.
psql --username "$POSTGRES_USER" --dbname "$WH_DB" \
     --set ON_ERROR_STOP=1 --quiet --tuples-only --no-align \
     --command "SELECT 'btree_gist version ' || extversion
                FROM pg_extension WHERE extname = 'btree_gist';"

echo "[init] 03_create_extensions.sh: done"
