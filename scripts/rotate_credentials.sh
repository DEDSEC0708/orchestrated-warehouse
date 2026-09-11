#!/usr/bin/env bash
#
# rotate_credentials.sh - replace every local credential in .env, and the
# PostgreSQL roles that must agree with it, in one operation.
#
#   bash scripts/rotate_credentials.sh --dry-run   # show what would change
#   bash scripts/rotate_credentials.sh --yes       # do it
#   bash scripts/rotate_credentials.sh --yes --keep-fernet
#
# WHY THIS EXISTS - AND WHY EDITING .env BY HAND DOES NOT WORK
#
#   The PostgreSQL roles are created by docker/postgres/init/01_create_roles.sh,
#   which the postgres image runs EXACTLY ONCE: on the first boot of an empty
#   pgdata volume. It never runs again while that volume survives.
#
#   So .env is not the source of truth for role passwords after the first boot -
#   it is only what the CLIENTS send. Rewrite the passwords in .env on their own
#   and every component keeps presenting a credential the server no longer
#   accepts:
#
#       FATAL:  password authentication failed for user "wh_etl"
#
#   The two obvious ways out are both wrong. `make clean` destroys the warehouse
#   to change a password. Leaving the placeholders in place means every install
#   of this project shares credentials that are published in git history.
#
#   The right move is the boring one: ALTER the roles in the running server so
#   they match the new .env, in the same operation, before anything restarts.
#   That keeps the data and changes the credentials.
#
# ORDER OF OPERATIONS - the failure modes drive it
#
#   1. Build the new .env in memory and write it to .env.new. Nothing is live
#      yet, so a generation failure here changes nothing.
#   2. ALTER every role, authenticating with the password still in the running
#      container's environment. This is the step that can fail; it happens while
#      the OLD credentials are still valid, and it is one transaction.
#   3. Only now move .env.new into place. If step 2 failed, .env is untouched
#      and the stack is exactly as it was.
#   4. Recreate the containers so they pick up the new environment, and let the
#      existing healthchecks and verify_stack.sh prove it worked.
#
# NO SECRET IS EVER PRINTED, and none is ever written to a file other than .env
# itself. The ALTER statements are piped straight into psql over stdin, so they
# do not appear in `ps`, in shell history, or in a temp file.
#
# NO HOST PYTHON. Everything outside the container is openssl and awk, both of
# which ship with Git for Windows and with every Linux distribution. This is a
# Docker-first project and its scripts should not need a language runtime on
# the developer's machine to start the stack - especially not on Windows, where
# the `python3` on PATH is often the Microsoft Store alias stub, which exists
# and is executable and does nothing but advertise the Store. See
# scripts/lib/secrets.sh.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ENV_FILE="${REPO_ROOT}/.env"
NEW_FILE="${REPO_ROOT}/.env.new"
MODE="dry-run"
KEEP_FERNET="no"

# ---------------------------------------------------------------------------
# Failure handling
#
# The ALTER transaction is a one-way door. Once it commits, the old passwords
# no longer exist anywhere in PostgreSQL, so "undo" is not available and
# pretending otherwise is worse than saying so.
#
# What IS available is roll-forward: from the moment the transaction commits,
# the new credentials exist on disk in .env.new (and then in .env), and the
# database accepts them. Every failure after that point is recoverable by
# finishing the job, not by reversing it.
#
# So the script tracks which side of that door it is on and prints the recovery
# procedure for the phase it actually failed in. No secret is ever included -
# every instruction names files and commands, never values.
# ---------------------------------------------------------------------------
PHASE="prepare"
BACKUP=""

on_exit() {
    local rc=$?
    [ "${rc}" -eq 0 ] && return 0
    [ "${MODE}" = "apply" ] || return "${rc}"

    echo >&2
    echo "=====================================================================" >&2
    echo " ROTATION FAILED - exit ${rc}" >&2
    echo "=====================================================================" >&2

    case "${PHASE}" in
        prepare)
            # Nothing has been applied. The staged file holds unused secrets,
            # and the backup is a byte-copy of the live .env, so both are
            # litter rather than safety. Removing them keeps a failed run from
            # leaving credential-shaped files lying around - but only after
            # confirming the backup really is identical, never on assumption.
            rm -f "${NEW_FILE}"
            if [ -n "${BACKUP}" ] && cmp -s "${BACKUP}" "${ENV_FILE}"; then
                rm -f "${BACKUP}"
                BACKUP=""
            fi
            echo " Nothing was changed. The database and .env are untouched." >&2
            echo " The staged .env.new has been removed. Safe to re-run." >&2
            ;;
        committed)
            echo " The PostgreSQL roles ALREADY HAVE the new passwords, but" >&2
            echo " .env was not swapped. Nothing else on disk knows them." >&2
            echo >&2
            echo " .env.new is the ONLY copy of the live credentials." >&2
            echo " DO NOT DELETE IT. Finish the swap by hand:" >&2
            echo >&2
            echo "   mv .env.new .env" >&2
            echo "   docker compose up -d --force-recreate postgres airflow-scheduler airflow-webserver" >&2
            echo "   bash scripts/verify_stack.sh" >&2
            ;;
        recreate|verify)
            echo " The roles and .env agree - the credentials are fully rotated." >&2
            echo " Only the container recreation or its verification failed, and" >&2
            echo " that is safe to retry as many times as you like:" >&2
            echo >&2
            echo "   docker compose up -d --force-recreate postgres airflow-scheduler airflow-webserver" >&2
            echo "   docker compose logs airflow-init | tail -30" >&2
            echo "   bash scripts/verify_stack.sh" >&2
            echo "   bash scripts/check_airflow_login.sh" >&2
            echo >&2
            echo " DO NOT restore the backup to 'undo' this. The old passwords" >&2
            echo " no longer exist in PostgreSQL, so putting them back in .env" >&2
            echo " would leave every client presenting a rejected credential." >&2
            ;;
    esac

    if [ -n "${BACKUP}" ] && [ -f "${BACKUP}" ]; then
        echo >&2
        echo " Previous .env kept at: $(basename "${BACKUP}")" >&2
        if [ "${PHASE}" = "prepare" ]; then
            # Nothing was applied, so the backup and the live .env are the same
            # file and the database still accepts what is in them.
            echo " It is identical to your current .env. Safe to delete." >&2
        else
            echo " It holds the PRE-rotation values, which the database no" >&2
            echo " longer accepts. Keep it for reference; it is NOT a" >&2
            echo " drop-in rollback." >&2
        fi
    fi
    echo "=====================================================================" >&2
    return "${rc}"
}
trap on_exit EXIT

while [ $# -gt 0 ]; do
    case "$1" in
        --yes) MODE="apply" ;;
        --dry-run) MODE="dry-run" ;;
        --keep-fernet) KEEP_FERNET="yes" ;;
        *) echo "Unknown argument: $1" >&2
           echo "Usage: $0 [--yes|--dry-run] [--keep-fernet]" >&2
           exit 2 ;;
    esac
    shift
done

die() { echo "ERROR: $*" >&2; exit 1; }

[ -f "${ENV_FILE}" ] || die "${ENV_FILE} not found. Run scripts/generate_env.sh first."
command -v docker  >/dev/null 2>&1 || die "docker not found on PATH."
command -v awk     >/dev/null 2>&1 || die "awk not found on PATH."

# Random value generation. No host Python: see the header of this file for why,
# and scripts/lib/secrets.sh for how.
# shellcheck source=scripts/lib/secrets.sh
. "${REPO_ROOT}/scripts/lib/secrets.sh"

cd "${REPO_ROOT}"

# --- 0. The stack has to be up, because the roles live inside it ------------
if ! docker compose ps --status running --services 2>/dev/null | grep -qx postgres; then
    die "the postgres service is not running. Start it first: make up"
fi

# ---------------------------------------------------------------------------
# 1. Is the Fernet key safe to rotate?
#
# The Fernet key encrypts connection and variable values stored in the Airflow
# metadata database. This project stores NONE - connections are injected as
# AIRFLOW_CONN_* environment variables specifically so there is only ever one
# copy of each password (see docker/airflow/init.sh). If that ever changes,
# rotating the key would silently render those rows undecryptable, so this
# asks the database rather than trusting the design note.
# ---------------------------------------------------------------------------
ENCRYPTED_ROWS="$(docker compose exec -T postgres sh -c '
    PGPASSWORD="$POSTGRES_PASSWORD" psql -h 127.0.0.1 -U "$POSTGRES_USER" \
        -d "$AIRFLOW_DB" -tAc "
        SELECT coalesce((SELECT count(*) FROM connection), 0)
             + coalesce((SELECT count(*) FROM variable), 0)"
' 2>/dev/null | tr -d '[:space:]')"

if [ -z "${ENCRYPTED_ROWS}" ]; then
    # Tables absent means the metadata database has never been migrated, so
    # there is certainly nothing encrypted in it.
    ENCRYPTED_ROWS=0
fi

if [ "${ENCRYPTED_ROWS}" != "0" ] && [ "${KEEP_FERNET}" != "yes" ]; then
    echo "REFUSING to rotate the Fernet key."
    echo "  The Airflow metadata database holds ${ENCRYPTED_ROWS} encrypted row(s)"
    echo "  (connections and/or variables). A new key would make them"
    echo "  permanently undecryptable."
    echo
    echo "  Re-run with --keep-fernet to rotate everything else, or export and"
    echo "  re-import those objects after rotating."
    exit 1
fi

echo "Fernet safety check: ${ENCRYPTED_ROWS} encrypted row(s) in the metadata database."

# ---------------------------------------------------------------------------
# 2. Build the new .env
#
# Only the values below change. Usernames, database names, ports, AIRFLOW_UID,
# every comment and the file's ordering are preserved byte for byte, because
# this rewrites individual values rather than regenerating from .env.example.
# ---------------------------------------------------------------------------
# The values are generated by shell helpers (openssl), and the file is
# rewritten by awk. Neither needs a language runtime on the host - see the
# header of scripts/lib/secrets.sh for why that matters on Windows.
#
# Values reach awk through the environment rather than through argv: argv is
# world-readable via `ps`, environment is not. They are never written anywhere
# but .env.new, which is created mode 600.
require_entropy_source || die "cannot generate credentials safely."

ROTATED_KEYS="POSTGRES_PASSWORD,WH_ETL_PASSWORD,CMS_OWNER_PASSWORD"
ROTATED_KEYS="${ROTATED_KEYS},CMS_READER_PASSWORD,WH_ANALYST_PASSWORD"
ROTATED_KEYS="${ROTATED_KEYS},AIRFLOW_DB_PASSWORD,AIRFLOW_ADMIN_PASSWORD"
ROTATED_KEYS="${ROTATED_KEYS},AIRFLOW__WEBSERVER__SECRET_KEY"

export ROT_POSTGRES_PASSWORD="$(random_alnum 32)"
export ROT_WH_ETL_PASSWORD="$(random_alnum 32)"
export ROT_CMS_OWNER_PASSWORD="$(random_alnum 32)"
export ROT_CMS_READER_PASSWORD="$(random_alnum 32)"
export ROT_WH_ANALYST_PASSWORD="$(random_alnum 32)"
export ROT_AIRFLOW_DB_PASSWORD="$(random_alnum 32)"
export ROT_AIRFLOW_ADMIN_PASSWORD="$(random_alnum 32)"
export ROT_AIRFLOW__WEBSERVER__SECRET_KEY="$(random_hex 32)"

if [ "${KEEP_FERNET}" != "yes" ]; then
    ROTATED_KEYS="${ROTATED_KEYS},AIRFLOW__CORE__FERNET_KEY"
    export ROT_AIRFLOW__CORE__FERNET_KEY="$(random_fernet_key)"
fi

echo "Rotating (values never shown):"
printf '%s\n' "${ROTATED_KEYS}" | tr ',' '\n' | sed 's/^/  - /'
if [ "${KEEP_FERNET}" = "yes" ]; then
    echo "  - AIRFLOW__CORE__FERNET_KEY  [SKIPPED: --keep-fernet]"
fi

if [ "${MODE}" != "apply" ]; then
    echo
    echo "DRY RUN - nothing written. Re-run with --yes to apply."
    exit 0
fi

# awk rewrites value-for-value and passes every other byte through, so
# comments, ordering, blank lines and all non-rotated settings survive intact.
# A key named in ROTATED_KEYS but absent from .env is a hard error: it would
# mean rotating only part of the credential set.
awk -v KEYS="${ROTATED_KEYS}" '
BEGIN {
    n = split(KEYS, want, ",")
    for (i = 1; i <= n; i++) expected[want[i]] = 1
}
{
    if (substr($0, 1, 1) != "#") {
        p = index($0, "=")
        if (p > 1) {
            key = substr($0, 1, p - 1)
            if (key in expected) {
                value = ENVIRON["ROT_" key]
                if (value == "") {
                    print "no generated value for " key > "/dev/stderr"
                    aborted = 1
                    exit 3
                }
                print key "=" value
                seen[key] = 1
                next
            }
        }
    }
    print $0
}
END {
    if (aborted) exit 3
    for (k in expected) {
        if (!(k in seen)) {
            print ".env has no line for: " k > "/dev/stderr"
            missing = 1
        }
    }
    if (missing) exit 4
}
' "${ENV_FILE}" > "${NEW_FILE}" || die "could not build the new .env. Nothing was changed."

chmod 600 "${NEW_FILE}" 2>/dev/null || true

# Flush to disk before the ALTER. The window between this file existing and the
# transaction committing is where a crash would otherwise lose the only copy of
# credentials the database is about to start requiring - and the superuser
# password is among them, so that state has no recovery path. There is no
# fsync(1), but sync(1) is in coreutils and Git Bash alike.
command -v sync >/dev/null 2>&1 && sync || true

[ -f "${NEW_FILE}" ] || die "the new .env was not written."

# ---------------------------------------------------------------------------
# 2b. Take the backup BEFORE the one-way door, not after.
#
# From this line until the end of the run, BOTH sets of values exist on disk:
# the previous ones in the backup, the new ones in .env.new. There is no moment
# where a crash can leave the database holding passwords that exist nowhere
# else. Backing up after the ALTER instead would open exactly that window.
# ---------------------------------------------------------------------------
BACKUP="${ENV_FILE}.backup.$(date +%Y%m%d%H%M%S)"
cp "${ENV_FILE}" "${BACKUP}"
chmod 600 "${BACKUP}" 2>/dev/null || true
echo "Previous .env saved as $(basename "${BACKUP}") (git-ignored)."

# ---------------------------------------------------------------------------
# 2c. Pre-flight: every role named in .env must actually exist.
#
# A role that is missing would abort the transaction anyway - which is safe -
# but the error psql gives is about a role name, not about the fact that the
# rotation set is incomplete. Checking first turns "ERROR: role ... does not
# exist" into a sentence that says what to do, and guarantees the ALTER covers
# the whole set rather than a subset.
# ---------------------------------------------------------------------------
EXPECTED_ROLES=6
FOUND_ROLES="$(awk -F= '
BEGIN {
    split("POSTGRES_USER,WH_ETL_USER,CMS_OWNER_USER,CMS_READER_USER,WH_ANALYST_USER,AIRFLOW_DB_USER", k, ",")
    split("volthive,wh_etl,cms_owner,cms_reader,wh_analyst,airflow", d, ",")
}
/^[A-Za-z_]+=/ { val[$1] = substr($0, index($0, "=") + 1) }
END {
    out = ""
    for (i = 1; i <= 6; i++) {
        name = (k[i] in val && val[k[i]] != "") ? val[k[i]] : d[i]
        out = out (out == "" ? "" : ",") "'"'"'" name "'"'"'"
    }
    print out
}
' "${NEW_FILE}")"

ROLE_COUNT="$(docker compose exec -T postgres sh -c "
    PGPASSWORD=\"\$POSTGRES_PASSWORD\" psql -h 127.0.0.1 -U \"\$POSTGRES_USER\" \
        -d postgres -tAc \"SELECT count(*) FROM pg_roles WHERE rolname IN (${FOUND_ROLES})\"
" 2>/dev/null | tr -d '[:space:]')"

if [ "${ROLE_COUNT}" != "${EXPECTED_ROLES}" ]; then
    die "expected ${EXPECTED_ROLES} roles in PostgreSQL, found ${ROLE_COUNT:-0}.
       .env names a role the database does not have, so a rotation would cover
       only some of them. Nothing has been changed."
fi
echo "Pre-flight: all ${EXPECTED_ROLES} roles present."

# ---------------------------------------------------------------------------
# 3. Bring the roles into line with it, BEFORE .env moves.
#
# The container still holds the OLD superuser password in its environment,
# which is exactly the credential needed to authenticate this change. The SQL
# is generated on stdout and piped straight in: it is never an argv value and
# never a file.
#
# ALTER ROLE is transactional, so either every role changes or none does.
# ---------------------------------------------------------------------------
echo
echo "Applying new passwords to the PostgreSQL roles..."

# Role names and passwords are read out of .env.new by awk, so no secret is
# ever an argument or an environment variable here - the file (mode 600) is the
# only carrier, and the statements go to psql over stdin.
if ! awk -F= '
BEGIN {
    split("POSTGRES_USER,WH_ETL_USER,CMS_OWNER_USER,CMS_READER_USER,WH_ANALYST_USER,AIRFLOW_DB_USER", ukey, ",")
    split("POSTGRES_PASSWORD,WH_ETL_PASSWORD,CMS_OWNER_PASSWORD,CMS_READER_PASSWORD,WH_ANALYST_PASSWORD,AIRFLOW_DB_PASSWORD", pkey, ",")
    split("volthive,wh_etl,cms_owner,cms_reader,wh_analyst,airflow", dflt, ",")
}
/^[A-Za-z_]+=/ { val[$1] = substr($0, index($0, "=") + 1) }
END {
    print "BEGIN;"
    emitted = 0
    for (i = 1; i <= 6; i++) {
        role = (ukey[i] in val && val[ukey[i]] != "") ? val[ukey[i]] : dflt[i]
        pw = val[pkey[i]]
        # The generated alphabet is alphanumeric, so a password cannot break out
        # of the literal - but check rather than rely on it, and refuse a role
        # name that is not a plain identifier.
        if (pw == "")               { print "missing " pkey[i] > "/dev/stderr"; exit 5 }
        if (pw ~ /[^A-Za-z0-9]/)    { print pkey[i] " is not alphanumeric" > "/dev/stderr"; exit 5 }
        if (role ~ /[^A-Za-z0-9_]/) { print ukey[i] " is not a plain identifier" > "/dev/stderr"; exit 5 }
        printf "ALTER ROLE \"%s\" WITH PASSWORD '"'"'%s'"'"';\n", role, pw
        emitted++
    }
    # The transaction must cover the WHOLE set. Emitting five statements and
    # committing would leave one role on its old password with no error
    # anywhere - the stack would then half-work, which is harder to diagnose
    # than a clean failure. Refusing before COMMIT makes the rollback automatic.
    if (emitted != 6) { print "expected 6 ALTER statements, generated " emitted > "/dev/stderr"; exit 5 }
    print "COMMIT;"
}
' "${NEW_FILE}" | docker compose exec -T postgres sh -c '
        PGPASSWORD="$POSTGRES_PASSWORD" exec psql -h 127.0.0.1 \
            -U "$POSTGRES_USER" -d postgres -v ON_ERROR_STOP=1 --quiet'
then
    rm -f "${NEW_FILE}"
    die "role rotation failed. The transaction rolled back, so every role still
       has its previous password. .env is UNCHANGED and the stack is untouched.
       Safe to re-run."
fi

# PAST THE ONE-WAY DOOR. PostgreSQL now holds passwords that exist on disk only
# in .env.new. Every failure from here on is roll-forward, and on_exit says so.
PHASE="committed"
echo "  roles updated (single transaction, all 6)."

# ---------------------------------------------------------------------------
# 4. Only now does .env change.
#
# The backup was taken before the ALTER, so this is a plain move: both sets of
# values are already safely on disk.
# ---------------------------------------------------------------------------
mv "${NEW_FILE}" "${ENV_FILE}"
chmod 600 "${ENV_FILE}" 2>/dev/null || true
PHASE="recreate"
echo "  .env updated."

# ---------------------------------------------------------------------------
# 5. Recreate so the containers actually read it.
#
# Environment is baked in at container CREATION, so `restart` is not enough -
# the containers have to be replaced. Until they are, every running client is
# still presenting the old password to a server that no longer accepts it, so
# this step is not cosmetic: it is what closes the stale-credential window.
#
# The pgdata volume is untouched. `up --force-recreate` replaces containers,
# never volumes, which is the entire reason this script exists instead of
# `make clean`.
#
# Done in stages rather than one command, because each stage proves a different
# credential and a failure should say which one:
#
#   postgres healthy      -> POSTGRES_PASSWORD is right (its healthcheck logs in)
#   airflow-init exit 0   -> AIRFLOW_DB_PASSWORD is right, and the admin
#                            password has been reconciled to the new .env
#   scheduler/webserver   -> the long-running clients hold the new environment
#
# airflow-init is recreated EXPLICITLY rather than left to depends_on. It is a
# one-shot container that has already exited 0, and relying on Compose to
# decide that it needs re-running is exactly the kind of assumption that leaves
# the admin password stale while everything else looks fine.
# ---------------------------------------------------------------------------
echo
echo "Recreating containers so the new environment takes effect..."

docker compose up -d --force-recreate --wait --wait-timeout 180 postgres
echo "  postgres healthy - the new superuser password is in effect."

docker compose up -d --force-recreate --no-deps airflow-init

init_cid="$(docker compose ps -aq airflow-init)"
[ -n "${init_cid}" ] || die "airflow-init container was not created."

deadline=$(( $(date +%s) + 300 ))
init_status="running"
while [ "$(date +%s)" -lt "${deadline}" ]; do
    init_status="$(docker inspect -f '{{.State.Status}}' "${init_cid}" 2>/dev/null || echo missing)"
    [ "${init_status}" = "exited" ] && break
    sleep 2
done

if [ "${init_status}" != "exited" ]; then
    die "airflow-init did not finish within 300s (status: ${init_status}).
       Inspect it with: docker compose logs airflow-init"
fi

init_exit="$(docker inspect -f '{{.State.ExitCode}}' "${init_cid}" | tr -d '[:space:]')"
if [ "${init_exit}" != "0" ]; then
    die "airflow-init exited ${init_exit}. The admin password was NOT reconciled.
       Inspect it with: docker compose logs airflow-init"
fi
echo "  airflow-init exit 0 - metadata database reachable, admin password reconciled."

docker compose up -d --force-recreate --no-deps --wait --wait-timeout 180 \
    airflow-scheduler airflow-webserver
echo "  scheduler and webserver healthy on the new environment."

# ---------------------------------------------------------------------------
# 6. Verify before claiming success.
#
# verify_stack.sh logs in as every one of the six roles using the values now in
# .env, and asserts each can do exactly what it should and nothing more. That
# is the direct proof that no role was left on a stale password - a partial
# rotation cannot pass it.
# ---------------------------------------------------------------------------
PHASE="verify"
echo
echo "Verifying..."
bash "${REPO_ROOT}/scripts/verify_stack.sh"
bash "${REPO_ROOT}/scripts/check_airflow_login.sh"

PHASE="done"
echo
echo "====================================================================="
echo " ROTATION COMPLETE AND VERIFIED"
echo "====================================================================="
echo " All 6 PostgreSQL roles, the Airflow admin password and the session"
echo " keys have been replaced, and every one of them has been proved to"
echo " work against the running stack."
echo
echo " Your new Airflow password:  grep AIRFLOW_ADMIN_PASSWORD .env"
echo
echo " The previous .env is still on disk at:"
echo "   $(basename "${BACKUP}")"
echo " It is git-ignored, but it holds real credentials. Now that the"
echo " rotation is verified, delete it when you are ready:"
echo "   rm $(basename "${BACKUP}")"
echo "====================================================================="
