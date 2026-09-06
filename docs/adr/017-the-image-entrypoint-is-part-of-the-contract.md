# ADR-017: The Airflow image entrypoint is part of the contract

**Status:** Accepted
**Date:** 2026-09-05
**Supersedes:** nothing. Amends the container topology in ADR-006.

## Context

The three Airflow services run as `user: "${AIRFLOW_UID:-50000}:0"`. That value
comes from `.env`, written by `scripts/generate_env.sh`, which uses `id -u` on
whatever machine generated the file — so in practice it is 1000, 1007, 501, or
50000, and the project has no control over which.

None of those UIDs exist in the image's `/etc/passwd`. That is not an oversight
in the image; it is how the official Airflow image is designed to support
arbitrary UIDs, and it works because its entrypoint fixes the gap at start-up:

```bash
# scripts/docker/entrypoint_prod.sh, create_system_user_if_missing()
if ! whoami &> /dev/null; then
  if [[ -w /etc/passwd ]]; then
    echo "${USER_NAME:-default}:x:$(id -u):0:${USER_NAME:-default} user:${AIRFLOW_USER_HOME_DIR}:/sbin/nologin" \
        >> /etc/passwd
  fi
  export HOME="${AIRFLOW_USER_HOME_DIR}"
fi
```

`airflow-init` used to declare `entrypoint: /opt/airflow/init.sh`, which
replaces that script rather than running after it. The bootstrap therefore ran
as a UID with no name, and Airflow refuses to run that way:

```python
# airflow/utils/platform.py
def getuser() -> str:
    try:
        return getpass.getuser()
    except KeyError:
        raise AirflowConfigException(
            "The user that Airflow is running as has no username; ..."
        )
```

`getuser()` is called by `_build_metrics()` in `airflow/utils/cli.py`, which the
`@action_cli` decorator runs *before* the subcommand body. So `airflow db
migrate` failed before opening a connection, `airflow-init` exited 1, and
because both long-running services gate on
`service_completed_successfully`, the whole stack stopped with a perfectly
healthy Postgres and no useful error at the compose layer.

The failure was invisible in review for three reasons. The compose file
validates. The image builds. And the scheduler and webserver — which set only
`command` and so keep the entrypoint — were never affected, so the bug looked
like it could not be about the image.

## Decision

**No service that uses the Airflow image may override `entrypoint`.**

A service that needs to run something other than an `airflow` subcommand passes
it as a command through the image's documented dispatcher:

```yaml
command: ["bash", "/opt/airflow/init.sh"]
```

`exec_to_bash_or_python_command_if_specified()` shifts off the leading `bash`
and `exec`s the rest, after the entrypoint has already run `check_uid_gid`,
`umask 0002`, `create_system_user_if_missing` and `wait_for_airflow_db`.

`docker/airflow/init.sh` additionally fixes its own precondition if it finds
itself running unmapped, because it is reachable by `docker compose run
--entrypoint` and by a plain `docker run`. This is the same principle as
ADR-015: a script guarantees what it depends on rather than assuming an earlier
step provided it. It never escalates privileges — if `/etc/passwd` is not
writable it warns and lets the subsequent `airflow` call produce the real
error.

## Consequences

The bootstrap keeps every property it had. It still runs unprivileged, still
runs once, still exits non-zero on failure, still gates the scheduler and
webserver. What changes is that it no longer discards the image's own start-up
work to get there.

We inherit the entrypoint's `wait_for_airflow_db` retry loop. That is a
readiness check published and maintained by the image, not a `sleep` added to
paper over a race, and it is redundant with `depends_on: service_healthy`
rather than a replacement for it.

The alternative used by the upstream `docker-compose.yaml` — running
`airflow-init` as `user: "0:0"` — was rejected. Root has a passwd entry so it
would also have worked, but it would run migrations as root and seed the shared
log volume with root-owned directories that the non-root scheduler then cannot
write. Fixing a UID problem by removing the UID restriction is not a fix.

`tests/unit/test_compose_config.py` asserts the decision directly:
`test_no_airflow_service_replaces_the_image_entrypoint` and
`test_bootstrap_script_tolerates_an_unmapped_uid`. Both fail against the
pre-fix repository.

## Related: the admin account reconciles, it does not create-if-absent

Once the stack started, the UI returned "Invalid login" against an `.env` that
plainly showed the password being typed. Two defects, both in this file's
neighbourhood:

`.env.example` shipped a working password (`local_dev_only_change_me`) that no
document mentioned, so the only way to know it was to read the example file.
It is now `REPLACE_ME_GENERATED_LOCALLY`, generated per machine by
`scripts/generate_env.sh` exactly like the Fernet key, and `init.sh` refuses to
start while the placeholder survives. A default UI password in a public
repository is a real credential the moment anyone publishes port 8080.

More importantly, the bootstrap only consulted `.env` when the account was
absent. That made the file authoritative for exactly one boot and advisory ever
after: rotate the password, run `make up`, and the metadata database silently
wins. `init.sh` now runs `airflow users reset-password` when the user exists,
so the declared state converges on every run — the same rule every load in this
warehouse already follows. The username is logged; the password never is.

## Related: pipefail

A third defect in the same script was found while confirming the first and is
fixed alongside it. The admin-user guard was written as:

```bash
if airflow users list --output plain | awk '{print $2}' | grep -qx "${USER}"; then
```

`grep -q` exits at the first match, the upstream commands take `SIGPIPE` and
report 141, and `set -o pipefail` returns the rightmost non-zero status — so
the pipeline returned 141 *when the user was found*. The guard inverted itself,
`airflow users create` hit a duplicate username, and `set -e` exited 1. It
would have failed on the second `make up` and every one after it, while passing
the first — the shape of bug that survives review because the fresh-install
path is the one everybody tests. The list is now captured first and matched
with here-strings, so no pipeline status is involved.
