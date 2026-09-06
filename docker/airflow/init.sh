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

# ---------------------------------------------------------------------------
# 0. Precondition: the running UID must resolve to a username
#
# Airflow refuses to run as a UID with no passwd entry. `getpass.getuser()`
# raises KeyError, and airflow/utils/platform.py::getuser() converts that into
# an AirflowConfigException - raised by _build_metrics() in the @action_cli
# decorator, so it fires BEFORE any subcommand body, including `db migrate`.
#
# docker-compose.yml runs this container as ${AIRFLOW_UID}:0, and that UID is
# whatever `id -u` returned on the machine that generated .env. It is almost
# never a UID the image knows about. The image's own entrypoint solves this in
# create_system_user_if_missing(), and compose is now arranged so that runs
# first (see the `command:` note on the airflow-init service).
#
# This block is the safety net for the other ways this script can be reached -
# `docker compose run --entrypoint`, a direct `docker run`, a future edit that
# reintroduces an entrypoint override. It is deliberately the same mechanism
# the image uses, not a different one, and it is a no-op in the normal path.
#
# It never escalates: if /etc/passwd is not writable it says so and continues,
# because the next `airflow` call will fail with a clear message anyway and a
# bootstrap script has no business being the thing that demands root.
# ---------------------------------------------------------------------------
if ! whoami >/dev/null 2>&1; then
    if [ -w /etc/passwd ]; then
        echo "[airflow-init] uid $(id -u) has no passwd entry; adding one"
        printf '%s:x:%s:0:%s user:%s:/sbin/nologin\n' \
            "${USER_NAME:-airflow}" "$(id -u)" "${USER_NAME:-airflow}" \
            "${AIRFLOW_USER_HOME_DIR:-/home/airflow}" >> /etc/passwd
        export HOME="${AIRFLOW_USER_HOME_DIR:-/home/airflow}"
    else
        echo "[airflow-init] WARNING: uid $(id -u) has no passwd entry and" >&2
        echo "[airflow-init] /etc/passwd is not writable. Airflow will refuse" >&2
        echo "[airflow-init] to start. Run this container through the image's" >&2
        echo "[airflow-init] entrypoint rather than overriding it." >&2
    fi
fi

echo "[airflow-init] starting bootstrap"
echo "[airflow-init] running as uid $(id -u), user $(whoami 2>/dev/null || echo '<unmapped>')"
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

# An un-generated .env satisfies compose's `${VAR:?}` checks - the placeholder
# is a non-empty string - so without this the stack would boot with a password
# published in .env.example and nothing would say so. Same failure shape as the
# Fernet key placeholder, same treatment: refuse, and name the fix.
case "${AIRFLOW_ADMIN_PASSWORD}" in
    *REPLACE_ME_GENERATED_LOCALLY*)
        echo "[airflow-init] FATAL: AIRFLOW_ADMIN_PASSWORD is still the placeholder." >&2
        echo "[airflow-init] Run: bash scripts/generate_env.sh" >&2
        exit 1
        ;;
esac

# The list is captured into a variable BEFORE it is searched, rather than
# piped straight into `grep -q`. That is not a style preference.
#
# `grep -q` exits the instant it matches. Under `set -o pipefail` the upstream
# `airflow users list` and `awk` are then killed by SIGPIPE and report 141, and
# pipefail returns the rightmost NON-ZERO status - so the pipeline returns 141
# even though grep found the user. The `if` reads that as "not found", takes
# the else branch, and `airflow users create` fails on the duplicate username,
# which `set -e` turns into exit 1.
#
# Net effect: the guard did the exact opposite of its job. The first `make up`
# succeeded and every subsequent one failed, which is the worst shape a bug
# like this can have - it passes the test everyone actually runs.
#
# Capturing first, and matching with here-strings rather than pipes, means
# there is no pipeline for pipefail to misreport: the status being tested is
# grep's own. `NR > 1` drops the header row, whose second column is the
# literal word "username".
#
# The command substitution is deliberately NOT guarded with `|| true`: if
# `airflow users list` genuinely fails, set -e must stop here rather than let
# an empty list be read as "no admin exists yet".
existing_users="$(airflow users list --output plain)"
existing_usernames="$(awk 'NR > 1 { print $2 }' <<<"${existing_users}")"

if grep -qx "${AIRFLOW_ADMIN_USER}" <<<"${existing_usernames}"; then
    # RECONCILE, don't skip.
    #
    # "Create if absent" makes .env authoritative exactly once - on the first
    # boot of an empty metadata volume - and silently advisory ever after.
    # Change AIRFLOW_ADMIN_PASSWORD, run `make up`, and the login is unchanged
    # with no warning anywhere: the config and the database disagree and the
    # database wins. That is how you get "Invalid login" against a stack whose
    # .env plainly shows the password you just typed.
    #
    # Every other load in this project converges on the declared state rather
    # than assuming a previous run got it right, and the admin user is not a
    # special case. `airflow users reset-password` is idempotent, so this is a
    # no-op whenever they already agree.
    echo "[airflow-init] admin user '${AIRFLOW_ADMIN_USER}' exists, reconciling password from .env"
    airflow users reset-password \
        --username "${AIRFLOW_ADMIN_USER}" \
        --password "${AIRFLOW_ADMIN_PASSWORD}"
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

# ---------------------------------------------------------------------------
# 4. Tell the developer how to sign in
#
# The username is printed; the password never is. It is in .env, which is
# git-ignored and readable only by the person who generated it:
#
#     grep AIRFLOW_ADMIN_PASSWORD .env
#
# Printing it here would put a working credential into `docker compose logs`,
# into CI output, and into any screenshot of a terminal - three places nobody
# audits. Naming where it lives costs nothing and leaks nothing.
# ---------------------------------------------------------------------------
echo "[airflow-init] ---------------------------------------------------------"
echo "[airflow-init] Airflow UI:  http://localhost:${AIRFLOW_WEBSERVER_HOST_PORT:-8080}"
echo "[airflow-init] username:    ${AIRFLOW_ADMIN_USER}"
echo "[airflow-init] password:    see AIRFLOW_ADMIN_PASSWORD in .env"
echo "[airflow-init] ---------------------------------------------------------"

echo "[airflow-init] bootstrap complete"
