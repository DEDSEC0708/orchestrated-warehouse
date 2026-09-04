#!/usr/bin/env bash
#
# run_analytics.sh - execute every showcase query against the warehouse.
#
#   bash scripts/run_analytics.sh              # print results
#   bash scripts/run_analytics.sh --check      # run them, print nothing, fail loudly
#
# WHY THIS EXISTS RATHER THAN A README CODE BLOCK
# -----------------------------------------------
# Queries pasted into a README rot silently. A column gets renamed, the README
# still says what it always said, and the first person to copy one out gets an
# error from a document that looked authoritative. Keeping the queries as real
# files that a script executes means "do the examples still work" is a command
# rather than an act of faith - and CI runs the same loop, so the answer is
# known on every commit.
#
# The end-to-end suite additionally asserts that each one returns AT LEAST ONE
# ROW, because a query that runs and returns nothing looks fine in CI and
# produces an empty table for whoever actually opens it.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

CHECK_ONLY=0
if [ "${1:-}" = "--check" ]; then
    CHECK_ONLY=1
fi

# Resolve the connection the same way the pipeline does, so this script and
# the pipeline can never disagree about which database they mean. Falling back
# to libpq's own PG* variables keeps it usable from a bare psql environment.
DB="${WH_DB:-warehouse}"
HOST="${POSTGRES_HOST:-${PGHOST:-127.0.0.1}}"
PORT="${POSTGRES_PORT:-${PGPORT:-5432}}"
USER="${WH_ETL_USER:-${PGUSER:-wh_etl}}"

if [ -n "${WH_ETL_PASSWORD:-}" ]; then
    export PGPASSWORD="$WH_ETL_PASSWORD"
fi

shopt -s nullglob
QUERIES=(sql/analytics/*.sql)
if [ ${#QUERIES[@]} -eq 0 ]; then
    echo "ERROR: no queries found under sql/analytics/" >&2
    exit 1
fi

FAILED=0
for path in "${QUERIES[@]}"; do
    name="$(basename "$path")"
    if [ "$CHECK_ONLY" -eq 1 ]; then
        # -v ON_ERROR_STOP=1 turns a SQL error into a non-zero exit, which is
        # the whole point: without it psql prints the error and exits 0.
        if psql -v ON_ERROR_STOP=1 -h "$HOST" -p "$PORT" -U "$USER" -d "$DB" \
                -qtA -f "$path" > /dev/null 2>/tmp/analytics_err; then
            printf '  ok    %s\n' "$name"
        else
            printf '  FAIL  %s\n' "$name"
            sed 's/^/        /' /tmp/analytics_err >&2
            FAILED=$((FAILED + 1))
        fi
    else
        printf '\n=== %s ===\n' "$name"
        # The leading comment block of each file states what the query answers
        # and why it is interesting; showing it turns the output into a
        # readable report rather than a wall of numbers.
        sed -n '2,/^-- =\{20,\}$/p' "$path" | sed 's/^-- \?//' | head -6
        psql -v ON_ERROR_STOP=1 -h "$HOST" -p "$PORT" -U "$USER" -d "$DB" \
             --pset=pager=off -f "$path"
    fi
done

if [ "$FAILED" -gt 0 ]; then
    echo
    echo "$FAILED of ${#QUERIES[@]} showcase queries failed." >&2
    exit 1
fi

if [ "$CHECK_ONLY" -eq 1 ]; then
    echo "All ${#QUERIES[@]} showcase queries executed successfully."
fi
