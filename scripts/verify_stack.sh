#!/usr/bin/env bash
#
# verify_stack.sh - Phase 2 infrastructure verification.
#
#   make verify          (or)   bash scripts/verify_stack.sh
#
# Asserts that the running stack is what the specification says it should be:
# the right databases exist, the right roles exist, each role can do exactly
# what it is supposed to do, and - just as important - CANNOT do what it is not.
#
# A privilege model that is never tested is a privilege model you are guessing
# about. Every "must be denied" check below fails the script if the operation
# unexpectedly SUCCEEDS.
#
# Windows: run from Git Bash. Every `docker compose exec` uses -T because an
# allocated TTY breaks output capture in a non-interactive shell.

set -uo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

if [ ! -f .env ]; then
    echo "ERROR: .env not found. Run: cp .env.example .env && bash scripts/generate_env.sh" >&2
    exit 1
fi

# shellcheck disable=SC1091
set -a; . ./.env; set +a

PASS=0
FAIL=0

ok()   { printf '  \033[32mPASS\033[0m  %s\n' "$1"; PASS=$((PASS + 1)); }
bad()  { printf '  \033[31mFAIL\033[0m  %s\n' "$1"; FAIL=$((FAIL + 1)); }
head1() { printf '\n== %s ==\n' "$1"; }

# Run SQL as the superuser and echo the single-value result.
sql_admin() {
    docker compose exec -T postgres \
        env PGPASSWORD="$POSTGRES_PASSWORD" \
        psql -h 127.0.0.1 -U "$POSTGRES_USER" -d "$1" -tAc "$2" 2>/dev/null | tr -d '\r'
}

# Run SQL as an arbitrary role; returns the psql exit code.
sql_as() {
    local user="$1" password="$2" database="$3" statement="$4"
    docker compose exec -T postgres \
        env PGPASSWORD="$password" \
        psql -h 127.0.0.1 -U "$user" -d "$database" -tAc "$statement" >/dev/null 2>&1
}

expect_sql() {
    local label="$1" database="$2" statement="$3" expected="$4"
    local actual
    actual="$(sql_admin "$database" "$statement")"
    if [ "$actual" = "$expected" ]; then
        ok "$label"
    else
        bad "$label  (expected '${expected}', got '${actual}')"
    fi
}

expect_allowed() {
    local label="$1"; shift
    if sql_as "$@"; then ok "$label"; else bad "$label  (operation was DENIED but should be allowed)"; fi
}

expect_denied() {
    local label="$1"; shift
    if sql_as "$@"; then bad "$label  (operation SUCCEEDED but must be denied)"; else ok "$label"; fi
}

echo "==========================================================="
echo " ORCHESTRATED WAREHOUSE - Phase 2 infrastructure verification"
echo "==========================================================="

# ---------------------------------------------------------------------------
head1 "1. Container health"
# ---------------------------------------------------------------------------
for service in postgres airflow-scheduler airflow-webserver; do
    state="$(docker compose ps --format '{{.Service}} {{.State}} {{.Health}}' 2>/dev/null \
             | awk -v s="$service" '$1 == s {print $2"/"$3}' | head -1)"
    case "$state" in
        running/healthy) ok "service ${service} is running and healthy" ;;
        "")              bad "service ${service} is not present - is the stack up?" ;;
        *)               bad "service ${service} state is ${state}" ;;
    esac
done

init_exit="$(docker inspect --format '{{.State.ExitCode}}' volthive-airflow-init 2>/dev/null | tr -d '\r')"
if [ "$init_exit" = "0" ]; then
    ok "airflow-init completed successfully, exit code 0"
else
    bad "airflow-init exit code is '${init_exit}', expected 0"
fi

# ---------------------------------------------------------------------------
head1 "2. Databases"
# ---------------------------------------------------------------------------
for database in "$WH_DB" "$CMS_DB" "$AIRFLOW_DB"; do
    expect_sql "database '${database}' exists" postgres \
        "SELECT count(*) FROM pg_database WHERE datname = '${database}'" "1"
done

expect_sql "warehouse is owned by ${WH_ETL_USER}" postgres \
    "SELECT pg_get_userbyid(datdba) FROM pg_database WHERE datname = '${WH_DB}'" "$WH_ETL_USER"
expect_sql "cms is owned by ${CMS_OWNER_USER}" postgres \
    "SELECT pg_get_userbyid(datdba) FROM pg_database WHERE datname = '${CMS_DB}'" "$CMS_OWNER_USER"
expect_sql "airflow metadata is owned by ${AIRFLOW_DB_USER}" postgres \
    "SELECT pg_get_userbyid(datdba) FROM pg_database WHERE datname = '${AIRFLOW_DB}'" "$AIRFLOW_DB_USER"

# ---------------------------------------------------------------------------
head1 "3. Roles exist and are NOT superusers"
# ---------------------------------------------------------------------------
for role in "$WH_ETL_USER" "$CMS_OWNER_USER" "$CMS_READER_USER" "$WH_ANALYST_USER" "$AIRFLOW_DB_USER"; do
    expect_sql "role '${role}' exists" postgres \
        "SELECT count(*) FROM pg_roles WHERE rolname = '${role}'" "1"
    expect_sql "role '${role}' is not a superuser" postgres \
        "SELECT rolsuper FROM pg_roles WHERE rolname = '${role}'" "f"
    expect_sql "role '${role}' cannot create databases" postgres \
        "SELECT rolcreatedb FROM pg_roles WHERE rolname = '${role}'" "f"
    expect_sql "role '${role}' cannot create roles" postgres \
        "SELECT rolcreaterole FROM pg_roles WHERE rolname = '${role}'" "f"
done

# ---------------------------------------------------------------------------
head1 "4. Least privilege - what each role MAY do"
# ---------------------------------------------------------------------------
expect_allowed "wh_etl can connect to the warehouse" \
    "$WH_ETL_USER" "$WH_ETL_PASSWORD" "$WH_DB" "SELECT 1"
expect_allowed "wh_etl can create objects in the warehouse" \
    "$WH_ETL_USER" "$WH_ETL_PASSWORD" "$WH_DB" \
    "CREATE TABLE IF NOT EXISTS public._verify_probe(id int); DROP TABLE public._verify_probe"
expect_allowed "cms_owner can create objects in cms" \
    "$CMS_OWNER_USER" "$CMS_OWNER_PASSWORD" "$CMS_DB" \
    "CREATE TABLE IF NOT EXISTS public._verify_probe(id int); DROP TABLE public._verify_probe"
expect_allowed "cms_reader can connect to cms" \
    "$CMS_READER_USER" "$CMS_READER_PASSWORD" "$CMS_DB" "SELECT 1"
expect_allowed "wh_analyst can connect to the warehouse" \
    "$WH_ANALYST_USER" "$WH_ANALYST_PASSWORD" "$WH_DB" "SELECT 1"

# ---------------------------------------------------------------------------
head1 "5. Least privilege - what each role MUST NOT do"
# ---------------------------------------------------------------------------
expect_denied "cms_reader CANNOT create tables in cms" \
    "$CMS_READER_USER" "$CMS_READER_PASSWORD" "$CMS_DB" \
    "CREATE TABLE public._verify_should_fail(id int)"
expect_denied "cms_reader CANNOT connect to the warehouse" \
    "$CMS_READER_USER" "$CMS_READER_PASSWORD" "$WH_DB" "SELECT 1"
expect_denied "cms_reader CANNOT connect to the Airflow metadata database" \
    "$CMS_READER_USER" "$CMS_READER_PASSWORD" "$AIRFLOW_DB" "SELECT 1"
expect_denied "wh_etl CANNOT connect to the Airflow metadata database" \
    "$WH_ETL_USER" "$WH_ETL_PASSWORD" "$AIRFLOW_DB" "SELECT 1"
expect_denied "wh_analyst CANNOT connect to cms" \
    "$WH_ANALYST_USER" "$WH_ANALYST_PASSWORD" "$CMS_DB" "SELECT 1"
expect_denied "wh_analyst CANNOT create tables in the warehouse" \
    "$WH_ANALYST_USER" "$WH_ANALYST_PASSWORD" "$WH_DB" \
    "CREATE TABLE public._verify_should_fail(id int)"
expect_denied "airflow role CANNOT connect to the warehouse" \
    "$AIRFLOW_DB_USER" "$AIRFLOW_DB_PASSWORD" "$WH_DB" "SELECT 1"

# ---------------------------------------------------------------------------
head1 "6. Airflow"
# ---------------------------------------------------------------------------
if docker compose exec -T airflow-scheduler airflow db check >/dev/null 2>&1; then
    ok "Airflow can reach its metadata database"
else
    bad "Airflow cannot reach its metadata database"
fi

af_version="$(docker compose exec -T airflow-scheduler airflow version 2>/dev/null | tr -d '\r\n')"
if [ "$af_version" = "2.10.5" ]; then
    ok "Airflow version is 2.10.5"
else
    bad "Airflow version is '${af_version}', expected 2.10.5"
fi

if docker compose exec -T airflow-scheduler \
        airflow jobs check --job-type SchedulerJob --hostname "$(docker compose exec -T airflow-scheduler hostname | tr -d '\r\n')" >/dev/null 2>&1; then
    ok "scheduler job is alive and heartbeating"
else
    bad "scheduler job heartbeat check failed"
fi

health="$(docker compose exec -T airflow-webserver curl --fail --silent http://localhost:8080/health 2>/dev/null | tr -d '\r')"
case "$health" in
    *'"status": "healthy"'*|*'"status":"healthy"'*) ok "webserver /health reports healthy" ;;
    "") bad "webserver /health returned nothing" ;;
    *)  bad "webserver /health did not report healthy: ${health}" ;;
esac

example_dags="$(docker compose exec -T airflow-scheduler airflow dags list --output plain 2>/dev/null | grep -c 'example_' || true)"
if [ "${example_dags:-0}" -eq 0 ]; then
    ok "no Airflow example DAGs are loaded"
else
    bad "${example_dags} example DAGs are loaded - set AIRFLOW__CORE__LOAD_EXAMPLES=False"
fi

python_ok="$(docker compose exec -T airflow-scheduler python -c "import volthive; print(volthive.__version__)" 2>/dev/null | tr -d '\r\n')"
if [ "$python_ok" = "0.1.0" ]; then
    ok "the volthive package is importable inside the Airflow container"
else
    bad "volthive import inside Airflow returned '${python_ok}', expected 0.1.0"
fi

# ---------------------------------------------------------------------------
echo
echo "==========================================================="
printf ' PASSED: %s    FAILED: %s\n' "$PASS" "$FAIL"
echo "==========================================================="

if [ "$FAIL" -gt 0 ]; then
    exit 1
fi
echo "Phase 2 infrastructure verification passed."
