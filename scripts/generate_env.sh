#!/usr/bin/env bash
#
# generate_env.sh - create a local .env from .env.example with real generated keys.
#
#   ./scripts/generate_env.sh            # create .env; refuse if one already
#                                        # has real keys in it
#   ./scripts/generate_env.sh --force    # overwrite an existing .env
#
# THIS SCRIPT CREATES .env ITSELF. Do NOT `cp .env.example .env` first - it
# reads the example and writes the copy in one step, so copying beforehand
# only makes the "do not clobber" guard below trigger on your own copy.
#
# WHY THIS EXISTS
#   Two Airflow settings must be cryptographically random and must never be
#   committed:
#     * AIRFLOW__CORE__FERNET_KEY      encrypts connection passwords stored in
#                                      the Airflow metadata database. A shared
#                                      or published key means anyone with the
#                                      database can decrypt every credential.
#     * AIRFLOW__WEBSERVER__SECRET_KEY signs web session cookies.
#   Committing either - or shipping a default - is a real vulnerability, so
#   .env.example carries the placeholder REPLACE_ME_GENERATED_LOCALLY and this
#   script replaces it on each developer's machine.
#
# REQUIREMENTS
#   openssl and awk. Both ship with Git for Windows and with every Linux
#   distribution, so there is nothing to install.
#
#   This deliberately does NOT need Python. It used to, for
#   cryptography.fernet.Fernet.generate_key() - and on Windows that made the
#   documented first command of this project fail with "Python was not found;
#   run without arguments to install from the Microsoft Store", because the
#   python3 on PATH is usually the Store's alias stub. A Fernet key is just 32
#   random bytes in URL-safe base64, which openssl produces directly.
#
# WINDOWS
#   Run from Git Bash or WSL:  bash scripts/generate_env.sh
#   This file is stored with LF line endings (enforced by .gitattributes); CRLF
#   would make the container fail with "exec format error".

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
EXAMPLE_FILE="${REPO_ROOT}/.env.example"
ENV_FILE="${REPO_ROOT}/.env"
FORCE="no"

if [ "${1:-}" = "--force" ]; then
    FORCE="yes"
elif [ -n "${1:-}" ]; then
    echo "Unknown argument: $1" >&2
    echo "Usage: $0 [--force]" >&2
    exit 2
fi

if [ ! -f "${EXAMPLE_FILE}" ]; then
    echo "ERROR: ${EXAMPLE_FILE} not found. Run this from the repository." >&2
    exit 1
fi

# Never clobber a WORKING .env by accident: it may hold values a developer
# changed by hand, and regenerating the Fernet key makes every credential
# already stored in the Airflow metadata database undecryptable.
#
# But an .env that still carries the REPLACE_ME_GENERATED_LOCALLY placeholders
# has no real keys to protect - it is an un-generated copy of .env.example,
# which is exactly what `cp .env.example .env` produces. Refusing there was
# actively harmful: the copy already satisfies compose's `${VAR:?}` checks
# (the placeholder is a non-empty string), so the stack came up with a
# PLACEHOLDER FERNET KEY and nothing said so. That is the precise
# vulnerability this script exists to prevent.
#
# So the guard is scoped to what it is actually protecting: real generated
# keys. An un-generated copy is upgraded in place, no --force required.
if [ -f "${ENV_FILE}" ] && [ "${FORCE}" != "yes" ]; then
    if grep -q "REPLACE_ME_GENERATED_LOCALLY" "${ENV_FILE}" 2>/dev/null; then
        echo "Found an existing .env with un-generated placeholder keys."
        echo "Nothing real to preserve, so it will be regenerated."
    else
        echo "ERROR: ${ENV_FILE} already exists and has generated keys." >&2
        echo "       Re-run with --force to overwrite." >&2
        echo "       Note: a new Fernet key invalidates credentials already stored" >&2
        echo "       in the Airflow metadata database." >&2
        exit 1
    fi
fi

command -v awk >/dev/null 2>&1 || { echo "ERROR: awk not found on PATH." >&2; exit 1; }

# shellcheck source=scripts/lib/secrets.sh
. "${REPO_ROOT}/scripts/lib/secrets.sh"
require_entropy_source || exit 1

echo "Generating secrets..."

FERNET_KEY="$(random_fernet_key)"
SECRET_KEY="$(random_hex 32)"

# The Airflow UI admin password. Generated rather than shipped, because a
# default password in .env.example is a real credential the moment anyone
# publishes port 8080 - and "change it later" is not a control.
#
# token_urlsafe would be shorter, but its alphabet includes '-' and '_' only:
# this uses letters and digits so the value is safe to paste into a browser,
# a shell, a URL and a .env file without quoting or escaping in any of them.
ADMIN_PASSWORD="$(random_alnum 24)"

# On Linux, bind-mounted files are created with the container's UID. If that
# does not match the host user, ./logs and ./data become root-owned and the
# host user cannot delete them. Docker Desktop on macOS/Windows virtualises
# ownership instead, so there the host UID buys nothing.
#
# Whatever lands here, the containers tolerate it: the Airflow image writes a
# /etc/passwd entry for the running UID at start-up, and docker-compose.yml is
# arranged so that always happens (ADR-017). This choice is about file
# ownership on the host, not about whether the stack can boot.
#
# `id -u` reports the UID of the shell running THIS script, which is not
# necessarily the Docker host's Linux. Git Bash and Cygwin on Windows are the
# clear case - they report a UID that means nothing to Docker Desktop - so
# they are detected and skipped. WSL is deliberately NOT skipped: with the
# Docker Desktop WSL backend, files under the WSL filesystem really are owned
# by that UID.
AIRFLOW_UID_VALUE="50000"
case "$(uname -s 2>/dev/null || echo unknown)" in
    MINGW*|MSYS*|CYGWIN*)
        # Windows shell emulation. Keep the image default.
        ;;
    *)
        if command -v id >/dev/null 2>&1; then
            HOST_UID="$(id -u)"
            case "${HOST_UID}" in
                ''|*[!0-9]*) : ;;
                *)
                    if [ "${HOST_UID}" -ge 1000 ] && [ "${HOST_UID}" -le 60000 ]; then
                        AIRFLOW_UID_VALUE="${HOST_UID}"
                    fi
                    ;;
            esac
        fi
        ;;
esac

# Substitution is done with awk rather than sed: sed's in-place flag and
# escaping rules differ between GNU, BSD and Git Bash, and generated keys can
# contain characters (/ + =) that are special to sed. awk does a literal
# assignment, so no value is ever re-parsed.
#
# Values travel in the environment, not in argv, because argv is world-readable
# through `ps`.
GEN_FERNET_KEY="${FERNET_KEY}" \
GEN_SECRET_KEY="${SECRET_KEY}" \
GEN_ADMIN_PASSWORD="${ADMIN_PASSWORD}" \
GEN_AIRFLOW_UID="${AIRFLOW_UID_VALUE}" \
awk '
{
    if ($0 == "AIRFLOW__CORE__FERNET_KEY=REPLACE_ME_GENERATED_LOCALLY") {
        print "AIRFLOW__CORE__FERNET_KEY=" ENVIRON["GEN_FERNET_KEY"]; seen["fernet"] = 1; next
    }
    if ($0 == "AIRFLOW__WEBSERVER__SECRET_KEY=REPLACE_ME_GENERATED_LOCALLY") {
        print "AIRFLOW__WEBSERVER__SECRET_KEY=" ENVIRON["GEN_SECRET_KEY"]; seen["secret"] = 1; next
    }
    if ($0 == "AIRFLOW_ADMIN_PASSWORD=REPLACE_ME_GENERATED_LOCALLY") {
        print "AIRFLOW_ADMIN_PASSWORD=" ENVIRON["GEN_ADMIN_PASSWORD"]; seen["admin"] = 1; next
    }
    if ($0 == "AIRFLOW_UID=50000") {
        print "AIRFLOW_UID=" ENVIRON["GEN_AIRFLOW_UID"]; seen["uid"] = 1; next
    }
    print $0
}
END {
    split("fernet,secret,admin,uid", need, ",")
    for (i = 1; i <= 4; i++) {
        if (!(need[i] in seen)) {
            print ".env.example is missing the " need[i] " placeholder line" > "/dev/stderr"
            bad = 1
        }
    }
    if (bad) exit 1
}
' "${EXAMPLE_FILE}" > "${ENV_FILE}"

chmod 600 "${ENV_FILE}" 2>/dev/null || true

echo "Created ${ENV_FILE}"
echo "  - Fernet key generated"
echo "  - Webserver secret key generated"
echo "  - Airflow admin password generated"
echo "  - AIRFLOW_UID set to ${AIRFLOW_UID_VALUE}"
echo
# The password is NOT echoed. Printing it here would put a live credential in
# terminal scrollback, in CI logs, and in any screenshot of this command.
echo "Airflow UI sign-in:"
echo "  username: admin"
echo "  password: grep AIRFLOW_ADMIN_PASSWORD .env"
echo
echo "NEXT: review .env and change the placeholder DATABASE passwords before use."
echo "      .env is git-ignored and must never be committed."
