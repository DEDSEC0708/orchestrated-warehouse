#!/usr/bin/env bash
#
# in-stack.sh - run a command inside the project's Python environment.
#
#   bash scripts/in-stack.sh python scripts/run_pipeline.py --from X --to Y
#   bash scripts/in-stack.sh bash scripts/run_analytics.sh
#
# WHY THIS EXISTS
#
#   The pipeline's dependencies - structlog, psycopg, pydantic, the pinned
#   Airflow set - are installed in ONE place: the volthive/airflow image, built
#   from requirements.txt against Airflow's constraints file. They are
#   deliberately not installed on the host, because pinning them twice is how
#   two environments drift apart.
#
#   `make run` used to invoke the host interpreter anyway:
#
#       run:
#           python scripts/run_pipeline.py --from $(FROM) --to $(TO)
#
#   On a Linux box with `make install-dev` already run, that works, so the
#   assumption went unnoticed. On Windows it fails immediately and correctly:
#
#       ModuleNotFoundError: No module named 'structlog'
#
#   The host was never meant to have those modules. The fix is not to install
#   them there - it is to run the pipeline where they already are.
#
# WHY `run` AND NOT `exec`
#
#   `docker compose exec airflow-scheduler ...` would run inside the LIVE
#   scheduler: it competes with scheduled tasks for that container's resources,
#   and a scheduler restart would kill a backfill halfway through. `run --rm`
#   gets a fresh, isolated container with the same image, environment, network
#   and mounts, and removes it afterwards. `--no-deps` keeps a read-only report
#   from starting services as a side effect.
#
# WHY THIS IS NOT A WINDOWS WORKAROUND
#
#   It is the same command on every platform, and it is the one that matches
#   how this project is built: Docker owns the runtime, the host owns the
#   source. CI is unaffected - it never invokes make, it calls the scripts
#   directly against a service container with its own pip-installed
#   dependencies (see .github/workflows/ci.yml), which is the point of the
#   scripts being plain entry points with no container assumptions in them.
#
#   `-T` disables TTY allocation. That is correct for anything non-interactive,
#   and it also sidesteps Git Bash's TTY handling on Windows, which otherwise
#   needs `winpty` in front of every docker command.
#
# ESCAPE HATCH
#
#   VOLTHIVE_EXEC=host reproduces the old behaviour for anyone who has run
#   `make install-dev` and wants to drive a local PostgreSQL directly - the
#   same shape CI uses. Nothing depends on it; it exists so the container is a
#   default rather than a cage.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
EXTRA_ENV=()

# ---------------------------------------------------------------------------
# --source-writer: additionally pass the cms_owner credential.
#
# Only the generator needs it. It plays the part of the source system's
# operator, so it writes to the `cms` database - and that is the one identity
# the pipeline is deliberately denied. docker-compose.yml gives the Airflow
# services cms_READER only, which is what makes "the pipeline cannot corrupt
# its source" a fact enforced by PostgreSQL rather than a convention.
#
# Putting CMS_OWNER_PASSWORD into x-airflow-common would hand write access to
# the scheduler and the webserver permanently, to fix one developer command.
# So it is granted per invocation instead, and only to the command that needs
# it.
#
# The value is read out of .env and forwarded BY NAME (`-e VAR`, no `=value`),
# which makes Compose take it from this process's environment. It therefore
# never appears in argv, where `ps` would show it to every user on the machine.
# ---------------------------------------------------------------------------
if [ "${1:-}" = "--source-writer" ]; then
    shift
    if [ ! -f "${REPO_ROOT}/.env" ]; then
        echo "ERROR: .env not found. Run: bash scripts/generate_env.sh" >&2
        exit 1
    fi
    # awk, not `source`: .env is data, and sourcing it would execute anything
    # a stray backtick happened to contain.
    _env_value() {
        awk -v key="$1" '
            substr($0, 1, 1) == "#" { next }
            {
                p = index($0, "=")
                if (p > 1 && substr($0, 1, p - 1) == key) { print substr($0, p + 1); exit }
            }
        ' "${REPO_ROOT}/.env"
    }
    CMS_OWNER_USER="$(_env_value CMS_OWNER_USER)"
    CMS_OWNER_PASSWORD="$(_env_value CMS_OWNER_PASSWORD)"
    if [ -z "${CMS_OWNER_PASSWORD}" ]; then
        echo "ERROR: CMS_OWNER_PASSWORD is not set in .env." >&2
        echo "       The generator writes to the simulated source database and" >&2
        echo "       cannot do so as the read-only pipeline role." >&2
        exit 1
    fi
    export CMS_OWNER_USER="${CMS_OWNER_USER:-cms_owner}" CMS_OWNER_PASSWORD
    EXTRA_ENV=(-e CMS_OWNER_USER -e CMS_OWNER_PASSWORD)
fi

if [ "$#" -eq 0 ]; then
    echo "Usage: $0 [--source-writer] <command> [args...]" >&2
    echo "Example: $0 python scripts/run_pipeline.py --from 2026-06-01 --to 2026-06-07" >&2
    exit 2
fi

# Host execution: exec through, arguments untouched.
if [ "${VOLTHIVE_EXEC:-container}" = "host" ]; then
    exec "$@"
fi

if ! command -v docker >/dev/null 2>&1; then
    echo "ERROR: docker not found on PATH." >&2
    echo "       This project runs its pipeline inside the Airflow image." >&2
    echo "       Install Docker, or set VOLTHIVE_EXEC=host if you have run" >&2
    echo "       'make install-dev' and want to use your own interpreter." >&2
    exit 1
fi

cd "${REPO_ROOT}"

# The image is built by `make build` / `make up`. airflow-scheduler declares
# `pull_policy: never` - correctly, since volthive/airflow is not published
# anywhere - so a missing image fails with a registry error that says nothing
# useful. Check first and name the fix.
if ! docker image inspect volthive/airflow:2.10.5-local >/dev/null 2>&1; then
    echo "ERROR: the volthive/airflow:2.10.5-local image does not exist yet." >&2
    echo "       Build it first:  make up" >&2
    exit 1
fi

# airflow-scheduler rather than airflow-init: same image, same mounts, same
# network, but without AIRFLOW_ADMIN_PASSWORD in its environment. There is no
# reason for a data-loading command to be able to read the UI credential.
#
# "$@" is forwarded verbatim, so every flag the scripts accept keeps working
# exactly as documented - including --from / --to.
exec docker compose run --rm --no-deps -T "${EXTRA_ENV[@]}" airflow-scheduler "$@"
