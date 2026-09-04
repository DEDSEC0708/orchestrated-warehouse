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
#   python3 with the `cryptography` package (installed as an Airflow dependency).
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

if ! command -v python3 >/dev/null 2>&1; then
    echo "ERROR: python3 not found on PATH." >&2
    exit 1
fi

echo "Generating secrets..."

FERNET_KEY="$(python3 -c 'from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())')"
SECRET_KEY="$(python3 -c 'import secrets; print(secrets.token_hex(32))')"

# On Linux, bind-mounted files are created with the container's UID. If that
# does not match the host user, ./logs and ./data become root-owned and the
# host user cannot delete them. Docker Desktop on macOS/Windows handles this
# itself, so only adopt the host UID when it looks like a normal Linux user.
AIRFLOW_UID_VALUE="50000"
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

# Substitution is done in Python rather than sed: sed's in-place flag and
# escaping rules differ between GNU, BSD and Git Bash, and the generated keys
# contain characters (/ + =) that are special to sed.
FERNET_KEY="${FERNET_KEY}" \
SECRET_KEY="${SECRET_KEY}" \
AIRFLOW_UID_VALUE="${AIRFLOW_UID_VALUE}" \
EXAMPLE_FILE="${EXAMPLE_FILE}" \
ENV_FILE="${ENV_FILE}" \
python3 - <<'PYTHON'
import os
from pathlib import Path

example = Path(os.environ["EXAMPLE_FILE"]).read_text(encoding="utf-8")

replacements = {
    "AIRFLOW__CORE__FERNET_KEY=REPLACE_ME_GENERATED_LOCALLY":
        f"AIRFLOW__CORE__FERNET_KEY={os.environ['FERNET_KEY']}",
    "AIRFLOW__WEBSERVER__SECRET_KEY=REPLACE_ME_GENERATED_LOCALLY":
        f"AIRFLOW__WEBSERVER__SECRET_KEY={os.environ['SECRET_KEY']}",
    "AIRFLOW_UID=50000":
        f"AIRFLOW_UID={os.environ['AIRFLOW_UID_VALUE']}",
}

missing = [key for key in replacements if key not in example]
if missing:
    raise SystemExit(
        ".env.example does not contain the expected placeholder lines:\n  "
        + "\n  ".join(missing)
    )

content = example
for placeholder, value in replacements.items():
    content = content.replace(placeholder, value)

# newline="\n": always write LF, even when this runs on Windows, because the
# file is read inside a Linux container.
Path(os.environ["ENV_FILE"]).write_text(content, encoding="utf-8", newline="\n")
PYTHON

chmod 600 "${ENV_FILE}" 2>/dev/null || true

echo "Created ${ENV_FILE}"
echo "  - Fernet key generated"
echo "  - Webserver secret key generated"
echo "  - AIRFLOW_UID set to ${AIRFLOW_UID_VALUE}"
echo
echo "NEXT: review .env and change the placeholder passwords before use."
echo "      .env is git-ignored and must never be committed."
