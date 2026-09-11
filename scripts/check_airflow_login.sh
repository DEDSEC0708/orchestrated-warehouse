#!/usr/bin/env bash
#
# check_airflow_login.sh - prove the Airflow UI accepts the credential in .env.
#
#   bash scripts/check_airflow_login.sh
#
# Exits 0 if the admin account in the metadata database authenticates with the
# password currently in .env, non-zero otherwise.
#
# WHY A SCRIPT RATHER THAN "just try logging in"
#   "Invalid login" has several causes that look identical in a browser: the
#   account does not exist, it exists with a different password, the container
#   is running an older environment than the file on disk, or the bootstrap
#   never ran. This separates them and says which one it is.
#
# THE PASSWORD IS NEVER PRINTED and never leaves the container. The check runs
# inside airflow-init, which is the only service that has AIRFLOW_ADMIN_PASSWORD
# in its environment, and it reports a verdict rather than a value. That means
# it is safe to run with someone looking over your shoulder, and safe to paste
# into an issue or a screenshot.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${REPO_ROOT}"

command -v docker >/dev/null 2>&1 || { echo "ERROR: docker not on PATH." >&2; exit 1; }

echo "Checking the Airflow admin credential (no secret is printed)..."

# --no-deps: this must not drag the whole stack up as a side effect of a check.
# --rm: a verification container should not survive its verification.
docker compose run --rm --no-deps -T airflow-init python - <<'PYTHON'
import os
import sys

from airflow.www.app import create_app

username = os.environ.get("AIRFLOW_ADMIN_USER", "")
password = os.environ.get("AIRFLOW_ADMIN_PASSWORD", "")

if not username or not password:
    print("FAIL  AIRFLOW_ADMIN_USER / AIRFLOW_ADMIN_PASSWORD are not set.")
    sys.exit(1)

app = create_app(testing=True)

with app.app_context():
    sm = app.appbuilder.sm
    user = sm.find_user(username=username)

    if user is None:
        print(f"FAIL  no account named {username!r} exists in the metadata database.")
        print("      The bootstrap never created it. Check: docker compose logs airflow-init")
        sys.exit(1)

    roles = [r.name for r in user.roles]
    print(f"  account   : {user.username}")
    print(f"  active    : {user.is_active}")
    print(f"  roles     : {roles}")

    # auth_user_db is the exact code path the login form runs. It rotates the
    # session id on success, which needs a request context - hence the wrapper
    # rather than calling the password hash comparison directly, so this tests
    # what actually happens rather than an approximation of it.
    with app.test_request_context():
        authenticated = sm.auth_user_db(username, password) is not None

if not authenticated:
    print("  password  : REJECTED")
    print()
    print("FAIL  The account exists but does not accept the password in .env.")
    print("      The container may predate your last .env edit - environment is")
    print("      fixed at container creation. Recreate it:")
    print("        docker compose up -d --force-recreate airflow-scheduler airflow-webserver")
    sys.exit(1)

if not user.is_active:
    print("  password  : accepted, but the account is INACTIVE")
    sys.exit(1)

if "Admin" not in roles:
    print("  password  : accepted, but the account is not an Admin")
    sys.exit(1)

print("  password  : ACCEPTED")
print()
print("PASS  The Airflow UI accepts the admin credential currently in .env.")
PYTHON
