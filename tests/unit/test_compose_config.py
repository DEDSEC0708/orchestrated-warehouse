"""Architecture invariants for the local infrastructure.

These tests parse ``docker-compose.yml`` and the Dockerfile as data. They need
no Docker daemon, so they run in CI in milliseconds, and they encode the
decisions in docs/adr/006 as executable rules rather than as prose that drifts.

What they are really defending against: infrastructure sprawl and accidental
insecurity. Someone - future me included - adding a Redis container, pinning an
image to ``:latest``, dropping a healthcheck, or hardcoding a password will get
a red test with a message explaining why, instead of a silent architecture
change nobody reviews.
"""

from __future__ import annotations

import base64
import re
import shutil
import subprocess
from pathlib import Path
from typing import Any

import pytest
import yaml

pytestmark = pytest.mark.unit

REPO_ROOT = Path(__file__).resolve().parents[2]
COMPOSE_FILE = REPO_ROOT / "docker-compose.yml"
DOCKERFILE = REPO_ROOT / "docker" / "airflow" / "Dockerfile"

#: The complete approved service list. Adding a service here is a decision that
#: belongs in an ADR, which is exactly why this test exists.
EXPECTED_SERVICES = {"postgres", "airflow-init", "airflow-scheduler", "airflow-webserver"}

#: Services that run indefinitely and must therefore report their own health.
LONG_RUNNING_SERVICES = {"postgres", "airflow-scheduler", "airflow-webserver"}

#: Infrastructure explicitly excluded by the specification.
FORBIDDEN_IMAGE_SUBSTRINGS = (
    "redis",
    "kafka",
    "zookeeper",
    "minio",
    "spark",
    "elasticsearch",
    "grafana",
    "prometheus",
    "metabase",
    "pgadmin",
)


@pytest.fixture(scope="module")
def compose() -> dict[str, Any]:
    return yaml.safe_load(COMPOSE_FILE.read_text(encoding="utf-8"))


@pytest.fixture(scope="module")
def services(compose: dict[str, Any]) -> dict[str, Any]:
    return compose["services"]


# --------------------------------------------------------------------------
# Topology
# --------------------------------------------------------------------------


def test_service_list_is_exactly_the_approved_set(services: dict[str, Any]) -> None:
    assert set(services) == EXPECTED_SERVICES


def test_no_forbidden_infrastructure(services: dict[str, Any]) -> None:
    """No broker, no object store, no monitoring stack. See ADR-006."""
    for name, service in services.items():
        image = str(service.get("image", "")).lower()
        for forbidden in FORBIDDEN_IMAGE_SUBSTRINGS:
            assert forbidden not in image, f"service '{name}' uses forbidden image '{image}'"


def test_executor_is_local(services: dict[str, Any]) -> None:
    """LocalExecutor is what makes Redis and a Celery worker unnecessary."""
    executor = services["airflow-scheduler"]["environment"]["AIRFLOW__CORE__EXECUTOR"]
    assert "LocalExecutor" in str(executor)


# --------------------------------------------------------------------------
# Image pinning
# --------------------------------------------------------------------------


def test_images_are_pinned_not_latest(services: dict[str, Any]) -> None:
    for name, service in services.items():
        image = str(service["image"])
        assert ":" in image, f"service '{name}' image '{image}' has no tag"
        assert not image.endswith(":latest"), f"service '{name}' pins ':latest', which is not a pin"


def test_dockerfile_pins_airflow_and_python(services: dict[str, Any]) -> None:
    """The base tag must fix BOTH versions, because constraints.txt is per pair."""
    content = DOCKERFILE.read_text(encoding="utf-8")
    assert "FROM apache/airflow:2.10.5-python3.11" in content

    # The image tag built from it should say the same thing.
    assert "2.10.5" in str(services["airflow-scheduler"]["image"])


def test_dockerfile_installs_with_constraints() -> None:
    """Installing Airflow without its constraints file is how environments break."""
    content = DOCKERFILE.read_text(encoding="utf-8")
    assert re.search(r"pip install .*-r /tmp/requirements\.txt -c /tmp/constraints\.txt", content)


# --------------------------------------------------------------------------
# Healthchecks and startup ordering
# --------------------------------------------------------------------------


@pytest.mark.parametrize("name", sorted(LONG_RUNNING_SERVICES))
def test_long_running_services_have_a_healthcheck(services: dict[str, Any], name: str) -> None:
    healthcheck = services[name].get("healthcheck")
    assert healthcheck, f"service '{name}' has no healthcheck"
    assert healthcheck.get("test"), f"service '{name}' healthcheck has no test command"
    assert healthcheck.get(
        "start_period"
    ), f"service '{name}' has no start_period; without one, slow first boots count as failures"


def test_no_healthcheck_uses_sleep(services: dict[str, Any]) -> None:
    """A sleep is a guess. A healthcheck is an observation."""
    for name, service in services.items():
        test_command = str(service.get("healthcheck", {}).get("test", ""))
        assert "sleep" not in test_command, f"service '{name}' healthcheck uses sleep"


def test_dependencies_are_condition_gated(services: dict[str, Any]) -> None:
    """`depends_on` as a bare list only waits for container start, not readiness."""
    for name, service in services.items():
        depends_on = service.get("depends_on")
        if not depends_on:
            continue
        assert isinstance(
            depends_on, dict
        ), f"service '{name}' uses list-form depends_on, which does not wait for readiness"
        for upstream, spec in depends_on.items():
            condition = spec.get("condition")
            assert condition in {
                "service_healthy",
                "service_completed_successfully",
            }, f"service '{name}' depends on '{upstream}' with condition '{condition}'"


def test_airflow_services_wait_for_the_bootstrap_to_succeed(services: dict[str, Any]) -> None:
    """Migrations must finish before either long-running service starts."""
    for name in ("airflow-scheduler", "airflow-webserver"):
        condition = services[name]["depends_on"]["airflow-init"]["condition"]
        assert condition == "service_completed_successfully"


def test_bootstrap_container_does_not_restart(services: dict[str, Any]) -> None:
    """A failed bootstrap must stay failed and visible, not loop."""
    assert str(services["airflow-init"]["restart"]) == "no"


# --------------------------------------------------------------------------
# The image entrypoint contract
#
# `user: "${AIRFLOW_UID}:0"` is only legal because the Airflow image's
# entrypoint appends a /etc/passwd entry for the running UID before handing
# over. Airflow refuses to run as a UID it cannot name: getpass.getuser()
# raises KeyError, and airflow/utils/platform.py::getuser() turns that into an
# AirflowConfigException from _build_metrics(), which the @action_cli decorator
# calls before every subcommand body. `airflow db migrate` therefore fails
# before opening a connection.
#
# A service that replaces `entrypoint` opts out of that silently - the compose
# file still validates, the image still builds, and the container exits 1 with
# a healthy database. These tests make the opt-out impossible to reintroduce.
# --------------------------------------------------------------------------

INIT_SCRIPT = REPO_ROOT / "docker" / "airflow" / "init.sh"


def _airflow_services(services: dict[str, Any]) -> dict[str, Any]:
    return {name: spec for name, spec in services.items() if name.startswith("airflow-")}


def test_no_airflow_service_replaces_the_image_entrypoint(services: dict[str, Any]) -> None:
    """The entrypoint is what makes an arbitrary AIRFLOW_UID work at all.

    Overriding it drops create_system_user_if_missing, and the container dies
    with "The user that Airflow is running as has no username".
    """
    overriding = {
        name: spec["entrypoint"]
        for name, spec in _airflow_services(services).items()
        if "entrypoint" in spec
    }
    assert not overriding, (
        f"These services replace the image entrypoint: {overriding}. "
        "Pass the script as a command instead - the image dispatches "
        "`bash <args>` via exec_to_bash_or_python_command_if_specified, so "
        'command: ["bash", "/opt/airflow/init.sh"] runs it AFTER the passwd '
        "entry, umask and db-readiness check have been set up."
    )


def test_bootstrap_runs_its_script_through_the_image_entrypoint(
    services: dict[str, Any],
) -> None:
    """`bash` first is the image's documented escape hatch to an arbitrary command."""
    command = services["airflow-init"]["command"]
    assert command == ["bash", "/opt/airflow/init.sh"], command


def test_every_airflow_service_runs_with_group_zero(services: dict[str, Any]) -> None:
    """The passwd entry the image writes hardcodes GID 0.

    A non-zero GID would not be able to write the log volume the image seeds.
    """
    for name, spec in _airflow_services(services).items():
        assert str(spec["user"]).endswith(":0"), f"{name} runs as {spec['user']}"


def test_bootstrap_script_tolerates_an_unmapped_uid() -> None:
    """The script must not assume the entrypoint already ran.

    It is reachable by `docker compose run --entrypoint` and by a plain
    `docker run`, so it fixes its own precondition before calling airflow.
    """
    body = INIT_SCRIPT.read_text(encoding="utf-8")
    assert (
        "whoami" in body and "/etc/passwd" in body
    ), "init.sh must guard against a UID with no passwd entry."

    guard_at = body.index("if ! whoami")
    first_airflow_call = min(
        body.index(needle) for needle in ("airflow db migrate", "airflow users", "airflow pools")
    )
    assert guard_at < first_airflow_call, (
        "The passwd guard must run before the first airflow command, because "
        "getuser() fires in the CLI decorator before any subcommand body."
    )


def test_bootstrap_script_never_pipes_into_grep_q() -> None:
    """`... | grep -q` under `set -o pipefail` reports 141, not 0.

    grep -q exits on first match, the upstream commands take SIGPIPE, and
    pipefail returns the rightmost non-zero status. The guard then reads
    "found" as "not found", `airflow users create` hits a duplicate username,
    and set -e exits 1 - on the SECOND `make up` and every one after it.
    """
    body = INIT_SCRIPT.read_text(encoding="utf-8")
    code = "\n".join(line for line in body.splitlines() if not line.lstrip().startswith("#"))
    offenders = [
        line.strip()
        for line in code.splitlines()
        if re.search(r"\|\s*grep\s+(-\w*q|--quiet)", line)
    ]
    assert not offenders, (
        f"Pipefail-unsafe early-exit pipeline in init.sh: {offenders}. "
        "Capture the output first and match with a here-string."
    )


def test_postgres_healthcheck_goes_over_tcp(services: dict[str, Any]) -> None:
    """During initdb the server listens only on a unix socket.

    Checking over TCP is what distinguishes "container started" from
    "initialisation finished and the real server is accepting connections".
    """
    test_command = str(services["postgres"]["healthcheck"]["test"])
    assert "127.0.0.1" in test_command


# --------------------------------------------------------------------------
# Secrets
# --------------------------------------------------------------------------


def test_exactly_one_service_builds_each_image() -> None:
    """Two services building the same tag is a build-time RACE, not a duplicate.

    Compose delegates builds to buildx bake, which makes every service with a
    `build:` section its own target and runs them concurrently. Targets that
    export the same image name collide in the image store:

        target airflow-init: failed to solve:
        image "docker.io/volthive/airflow:2.10.5-local": already exists

    One service reports CANCELED, the others ERROR, and the winner varies run
    to run. The pre-bake builder deduplicated identical build definitions,
    which is why the upstream Airflow compose shape - `build:` on a shared
    anchor - worked for years and then stopped.
    """
    compose = yaml.safe_load(COMPOSE_FILE.read_text(encoding="utf-8"))
    builders: dict[str, list[str]] = {}
    for name, service in compose["services"].items():
        if service.get("build"):
            builders.setdefault(str(service["image"]), []).append(name)

    contested = {image: names for image, names in builders.items() if len(names) > 1}
    assert not contested, (
        "these images are built by more than one service, which races on export: " f"{contested}"
    )


def test_every_service_image_is_either_built_or_pullable() -> None:
    """A service that neither builds nor can pull its image cannot start.

    The other half of the rule above: once only one service owns the build,
    the consumers must still resolve the same tag locally.
    """
    compose = yaml.safe_load(COMPOSE_FILE.read_text(encoding="utf-8"))
    built = {str(s["image"]) for s in compose["services"].values() if s.get("build")}
    for name, service in compose["services"].items():
        image = str(service["image"])
        if service.get("build"):
            continue
        # Either a registry image (postgres:16-alpine) or one built in-project.
        assert (
            image in built or "/" not in image.split(":")[0].rstrip("/") or ":" in image
        ), f"service '{name}' image '{image}' is neither built here nor a registry image"


def test_locally_built_images_are_never_pulled_from_a_registry() -> None:
    """`volthive/airflow` is not a repository this project publishes.

    Left at the default pull policy, a missing image sends Docker to Docker
    Hub - which either fails confusingly or, worse, succeeds against whatever
    stranger's image occupies that name. Every service using the locally built
    tag must therefore declare an explicit policy.
    """
    compose = yaml.safe_load(COMPOSE_FILE.read_text(encoding="utf-8"))
    local_images = {str(s["image"]) for s in compose["services"].values() if s.get("build")}

    for name, service in compose["services"].items():
        if str(service["image"]) not in local_images:
            continue
        policy = service.get("pull_policy")
        assert policy in {"never", "build"}, (
            f"service '{name}' uses the locally built image "
            f"'{service['image']}' with pull_policy={policy!r}; expected "
            "'never' (consumer) or 'build' (the build owner)"
        )


def test_the_build_owner_runs_before_its_consumers() -> None:
    """The single build owner must be ordered ahead of everything using it.

    This is what makes one-service-builds safe: the consumers cannot be
    created before the image exists, because compose already refuses to start
    them until the owner has completed successfully.
    """
    compose = yaml.safe_load(COMPOSE_FILE.read_text(encoding="utf-8"))
    services = compose["services"]
    owners = {name: str(s["image"]) for name, s in services.items() if s.get("build")}

    for name, service in services.items():
        image = str(service["image"])
        if service.get("build") or image not in owners.values():
            continue
        owner = next(o for o, img in owners.items() if img == image)
        depends_on = service.get("depends_on") or {}
        assert owner in depends_on, (
            f"service '{name}' uses the image built by '{owner}' but does not "
            f"depend on it, so it could be created before the image exists"
        )
        assert depends_on[owner]["condition"] in {
            "service_completed_successfully",
            "service_healthy",
        }


def test_makefile_up_builds_before_starting() -> None:
    """`make up` must not rely on `--build` reaching a dependency-only service.

    Only airflow-init declares a build, and `make up` names the two
    long-running services. Building explicitly removes any dependence on
    whether `--build` covers services pulled in through depends_on.
    """
    makefile = (REPO_ROOT / "Makefile").read_text(encoding="utf-8")
    up_block = re.search(r"^up:.*?(?=^\w[\w-]*:)", makefile, re.M | re.S)
    assert up_block, "no `up:` target found in the Makefile"
    body = up_block.group(0)

    assert "--build" not in body, (
        "`make up` still passes --build; build explicitly instead so the image "
        "is guaranteed to exist regardless of dependency resolution"
    )
    assert re.match(r"^up:\s*build\b", body), "`up` should depend on the `build` target"


def test_no_literal_credentials_in_compose_file() -> None:
    """Every credential must arrive through interpolation, never as a literal."""
    content = COMPOSE_FILE.read_text(encoding="utf-8")
    for line in content.splitlines():
        stripped = line.strip()
        if stripped.startswith("#"):
            continue
        if not any(marker in stripped.upper() for marker in ("PASSWORD", "SECRET_KEY", "FERNET")):
            continue
        # Any line mentioning a credential must reference it through compose
        # interpolation. A literal value would have no "${" anywhere.
        assert "${" in stripped, f"possible hardcoded credential in docker-compose.yml: {stripped}"


def test_required_secrets_fail_fast_when_unset() -> None:
    """`${VAR:?message}` turns a missing secret into an immediate, clear error.

    Without it, compose substitutes an empty string and the stack starts with a
    blank password - which fails much later and much more confusingly.
    """
    content = COMPOSE_FILE.read_text(encoding="utf-8")
    for variable in (
        "POSTGRES_PASSWORD",
        "WH_ETL_PASSWORD",
        "CMS_READER_PASSWORD",
        "AIRFLOW_DB_PASSWORD",
        "AIRFLOW__CORE__FERNET_KEY",
        "AIRFLOW__WEBSERVER__SECRET_KEY",
    ):
        assert f"${{{variable}:?" in content, f"{variable} should use the ${{VAR:?message}} form"


def test_env_example_documents_every_variable_used_by_compose() -> None:
    """A variable compose needs but .env.example omits is a broken first run."""
    compose_text = COMPOSE_FILE.read_text(encoding="utf-8")
    example_text = (REPO_ROOT / ".env.example").read_text(encoding="utf-8")

    documented = {
        line.split("=", 1)[0].strip()
        for line in example_text.splitlines()
        if "=" in line and not line.strip().startswith("#")
    }
    # (?<!\$) skips $${VAR}, which is an escaped literal passed through to the
    # container shell (for example $${HOSTNAME}) rather than a compose variable.
    referenced = set(re.findall(r"(?<!\$)\$\{([A-Z0-9_]+)[:?}-]", compose_text))

    missing = sorted(referenced - documented)
    assert not missing, f"used by docker-compose.yml but absent from .env.example: {missing}"


# --------------------------------------------------------------------------
# The documented first-run workflow.
#
# The tests above check the CONTENTS of .env.example. They all passed while
# the documented setup was still broken, because nothing exercised the
# WORKFLOW: the README said to `cp .env.example .env` and then run
# generate_env.sh, but that script creates .env itself and refused when the
# copy already existed. The keys were never generated, and a user who cleaned
# up after the error got "required variable AIRFLOW_DB_PASSWORD is missing a
# value" from compose.
#
# These run the real script in a temporary copy of the repository.
# --------------------------------------------------------------------------

#: Every variable compose refuses to start without, i.e. every `${VAR:?...}`.
REQUIRED_BY_COMPOSE = sorted(
    set(re.findall(r"(?<!\$)\$\{([A-Z0-9_]+):\?", COMPOSE_FILE.read_text(encoding="utf-8")))
)


def _parse_env(text: str) -> dict[str, str]:
    values = {}
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        values[key.strip()] = value
    return values


@pytest.fixture
def scratch_repo(tmp_path):
    """A throwaway repo holding just what generate_env.sh needs."""
    (tmp_path / "scripts" / "lib").mkdir(parents=True)
    shutil.copy(REPO_ROOT / ".env.example", tmp_path / ".env.example")
    target = tmp_path / "scripts" / "generate_env.sh"
    shutil.copy(REPO_ROOT / "scripts" / "generate_env.sh", target)
    target.chmod(0o755)
    # The generator sources its randomness helpers; without them the scratch
    # repo would exercise a script that cannot run.
    shutil.copy(SECRETS_LIB, tmp_path / "scripts" / "lib" / "secrets.sh")
    return tmp_path


def _run_generator(repo, *args):
    bash = shutil.which("bash")
    if bash is None:
        pytest.skip("bash is required to exercise generate_env.sh")
    # Absolute interpreter from shutil.which; every argument is either a
    # pytest tmp_path or a literal in this file - no external input.
    return subprocess.run(  # noqa: S603
        [bash, str(repo / "scripts" / "generate_env.sh"), *args],
        cwd=repo,
        capture_output=True,
        text=True,
        check=False,
    )


def test_compose_declares_at_least_one_required_variable() -> None:
    """Guard against the two tests below passing over an empty set."""
    assert "AIRFLOW_DB_PASSWORD" in REQUIRED_BY_COMPOSE
    assert len(REQUIRED_BY_COMPOSE) >= 5


def test_the_documented_workflow_satisfies_every_required_variable(scratch_repo) -> None:
    """THE regression test. Run exactly what the README says, then check that
    compose would have a value for every variable it refuses to start without.
    """
    result = _run_generator(scratch_repo)
    assert result.returncode == 0, f"generate_env.sh failed:\n{result.stderr}"

    env_file = scratch_repo / ".env"
    assert env_file.exists(), "the documented workflow did not produce a .env"
    values = _parse_env(env_file.read_text(encoding="utf-8"))

    unset = [v for v in REQUIRED_BY_COMPOSE if not values.get(v, "").strip()]
    assert not unset, (
        "after the documented setup, docker compose would still fail with "
        f"'required variable ... is missing a value' for: {unset}"
    )


def test_the_generated_env_has_real_keys_not_placeholders(scratch_repo) -> None:
    """A placeholder Fernet key satisfies compose's ${VAR:?} check because it
    is a non-empty string, so only an explicit assertion catches it. Shipping
    one means every credential in the Airflow metadata database is encrypted
    with a value that is public in this repository.
    """
    assert _run_generator(scratch_repo).returncode == 0
    values = _parse_env((scratch_repo / ".env").read_text(encoding="utf-8"))

    for key in ("AIRFLOW__CORE__FERNET_KEY", "AIRFLOW__WEBSERVER__SECRET_KEY"):
        assert "REPLACE_ME" not in values[key], f"{key} was never generated"
        assert len(values[key]) >= 32, f"{key} is too short to be a real key"

    # Two runs must not produce the same key.
    first = values["AIRFLOW__CORE__FERNET_KEY"]
    (scratch_repo / ".env").unlink()
    assert _run_generator(scratch_repo).returncode == 0
    second = _parse_env((scratch_repo / ".env").read_text(encoding="utf-8"))[
        "AIRFLOW__CORE__FERNET_KEY"
    ]
    assert first != second, "the Fernet key is not random between runs"


# --------------------------------------------------------------------------
# Airflow UI sign-in
#
# "Invalid login" against a stack whose .env plainly shows the password you
# just typed has two possible causes, and both are project defects rather than
# user error: the example ships a working password nobody documents, or the
# bootstrap only consults .env on the first boot of an empty volume and lets
# the metadata database win ever after.
# --------------------------------------------------------------------------


def test_the_example_ships_no_working_admin_password() -> None:
    """A default UI password in a public repo is a real credential.

    It stops being "only local" the moment anyone publishes port 8080, and
    "change it later" is not a control. It must be generated per machine, the
    same as the Fernet key.
    """
    values = _parse_env((REPO_ROOT / ".env.example").read_text(encoding="utf-8"))
    assert (
        values["AIRFLOW_ADMIN_PASSWORD"] == "REPLACE_ME_GENERATED_LOCALLY"
    ), "'.env.example' must not contain a usable Airflow admin password."


def test_the_generated_admin_password_is_real_and_random(scratch_repo) -> None:
    assert _run_generator(scratch_repo).returncode == 0
    first = _parse_env((scratch_repo / ".env").read_text(encoding="utf-8"))[
        "AIRFLOW_ADMIN_PASSWORD"
    ]
    assert "REPLACE_ME" not in first, "the admin password was never generated"
    assert len(first) >= 16, "the generated admin password is too short"
    assert first.isalnum(), (
        "the admin password must stay alphanumeric so it needs no quoting in "
        "a .env file, a shell, or a browser form"
    )

    (scratch_repo / ".env").unlink()
    assert _run_generator(scratch_repo).returncode == 0
    second = _parse_env((scratch_repo / ".env").read_text(encoding="utf-8"))[
        "AIRFLOW_ADMIN_PASSWORD"
    ]
    assert first != second, "the admin password is not random between runs"


def test_the_bootstrap_refuses_an_ungenerated_admin_password() -> None:
    """The placeholder is a non-empty string, so `${VAR:?}` accepts it.

    Without an explicit check the stack boots on a password published in this
    repository and nothing anywhere says so.
    """
    body = INIT_SCRIPT.read_text(encoding="utf-8")
    assert (
        "REPLACE_ME_GENERATED_LOCALLY" in body
    ), "init.sh must reject an un-generated AIRFLOW_ADMIN_PASSWORD."


def test_the_bootstrap_reconciles_the_admin_password(services: dict[str, Any]) -> None:
    """.env must be authoritative on every run, not only the first.

    "Create if absent" makes the metadata database win over the config file
    forever after the first boot: rotate the password in .env, run `make up`,
    and the login is silently unchanged.
    """
    body = INIT_SCRIPT.read_text(encoding="utf-8")
    assert (
        "users reset-password" in body
    ), "init.sh must reconcile an existing admin user's password from .env."

    # The reconcile is worthless if the bootstrap does not run every time.
    assert str(services["airflow-init"]["restart"]) == "no"
    for name in ("airflow-scheduler", "airflow-webserver"):
        assert (
            services[name]["depends_on"]["airflow-init"]["condition"]
            == "service_completed_successfully"
        )


def test_the_bootstrap_never_logs_the_admin_password() -> None:
    """The username is public; the password must not reach a log or screenshot."""
    body = INIT_SCRIPT.read_text(encoding="utf-8")
    for line in body.splitlines():
        stripped = line.strip()
        if stripped.startswith("echo") or stripped.startswith("printf"):
            assert (
                "${AIRFLOW_ADMIN_PASSWORD" not in stripped
            ), f"init.sh prints the admin password: {stripped}"


def test_sign_in_is_documented_for_a_fresh_clone() -> None:
    """A credential nobody can find is indistinguishable from a broken login."""
    readme = (REPO_ROOT / "README.md").read_text(encoding="utf-8")
    assert (
        "AIRFLOW_ADMIN_PASSWORD" in readme
    ), "README must tell a developer where the Airflow password lives."


# --------------------------------------------------------------------------
# Where the pipeline actually runs
#
# The project's dependencies live in exactly one place: the volthive/airflow
# image, pinned by requirements.txt against Airflow's constraints file. The
# warehouse-lifecycle targets used to call the HOST interpreter, which works
# only on a machine that has separately pip-installed the same pins - so on
# Windows `make run` died with ModuleNotFoundError: No module named 'structlog'
# against a perfectly healthy stack.
#
# Developer TOOLING (ruff, mypy, pytest) is the opposite case: it needs the dev
# dependencies, which are deliberately not in the runtime image. Those targets
# must keep running on the host.
# --------------------------------------------------------------------------

MAKEFILE = REPO_ROOT / "Makefile"
IN_STACK = REPO_ROOT / "scripts" / "in-stack.sh"

#: Targets that talk to the warehouse. These need the runtime dependencies.
LIFECYCLE_TARGETS = {
    "db-init",
    "generate",
    "run",
    "analytics",
    "dq-report",
    "restate",
    "backfill",
    "rebuild-dims",
}

#: Targets that are developer tooling. These need the DEV dependencies, which
#: are not in the runtime image, so they must stay on the host.
HOST_TOOLING_TARGETS = {
    "lint",
    "format",
    "typecheck",
    "sqlfluff",
    "test",
    "test-unit",
    "test-int",
    "test-e2e",
    "check",
}


def _make_recipes() -> dict[str, list[str]]:
    """Parse `target:` / tab-indented recipe lines out of the Makefile."""
    recipes: dict[str, list[str]] = {}
    current: str | None = None
    for line in MAKEFILE.read_text(encoding="utf-8").splitlines():
        if line.startswith("\t"):
            if current:
                recipes[current].append(line.lstrip("\t"))
            continue
        match = re.match(r"^([A-Za-z][A-Za-z0-9_-]*):", line)
        current = match.group(1) if match else None
        if current:
            recipes.setdefault(current, [])
    return recipes


def test_scripts_are_mounted_into_the_airflow_image(services: dict[str, Any]) -> None:
    """The entry points must exist inside the container that can run them."""
    for name, spec in _airflow_services(services).items():
        mounts = [str(v) for v in spec["volumes"]]
        assert any("./scripts:/opt/airflow/scripts" in m for m in mounts), (
            f"{name} does not mount ./scripts, so `docker compose run "
            f"{name} python scripts/...` cannot find the script"
        )
        assert any(
            "./scripts:/opt/airflow/scripts:ro" in m for m in mounts
        ), "scripts/ must be mounted read-only - it is an input"


@pytest.mark.parametrize("target", sorted(LIFECYCLE_TARGETS))
def test_lifecycle_targets_run_in_the_container(target: str) -> None:
    """A bare `python`/`bash` here assumes host dependencies that do not exist."""
    recipe = _make_recipes()[target]
    assert recipe, f"target {target} has no recipe"
    for line in recipe:
        assert line.startswith("$(IN_STACK)"), (
            f"'{target}' runs '{line}' directly. The project's dependencies are "
            "only in the Airflow image; route it through scripts/in-stack.sh."
        )


@pytest.mark.parametrize("target", sorted(HOST_TOOLING_TARGETS))
def test_developer_tooling_stays_on_the_host(target: str) -> None:
    """ruff, mypy and pytest are dev dependencies, absent from the runtime image."""
    for line in _make_recipes()[target]:
        assert "IN_STACK" not in line, (
            f"'{target}' was routed into the container, which has no dev "
            "dependencies. Only warehouse-lifecycle targets belong there."
        )


def test_only_the_generator_gets_source_write_access() -> None:
    """cms_owner is the identity the pipeline is deliberately denied.

    The generator impersonates the source system's operator and must write to
    `cms`; everything else must not be able to. Granting it per invocation
    keeps "the pipeline cannot corrupt its source" enforced by PostgreSQL
    rather than by convention.
    """
    recipes = _make_recipes()
    for target in LIFECYCLE_TARGETS:
        uses_flag = any("--source-writer" in line for line in recipes[target])
        assert uses_flag == (target == "generate"), (
            f"'{target}' should {'' if target == 'generate' else 'not '}"
            "request the cms_owner credential"
        )


def test_cms_owner_is_never_in_the_shared_airflow_environment(
    services: dict[str, Any],
) -> None:
    """The long-running services must never hold source write access."""
    for name, spec in _airflow_services(services).items():
        env = spec["environment"]
        assert "CMS_OWNER_PASSWORD" not in env, (
            f"{name} carries the source-owner credential; a bug in a DAG could "
            "then write to the database the pipeline is supposed to only read"
        )


def test_in_stack_forwards_arguments_verbatim() -> None:
    """`--from` / `--to` and every other documented flag must survive intact."""
    bash = shutil.which("bash")
    if bash is None:  # pragma: no cover
        pytest.skip("bash not available")
    body = IN_STACK.read_text(encoding="utf-8")
    assert (
        'exec docker compose run --rm --no-deps -T "${EXTRA_ENV[@]}" airflow-scheduler "$@"' in body
    ), 'in-stack.sh must forward "$@" unmodified to a fresh, dependency-free ' "container"
    # `run --rm`, not `exec`: a backfill must not die when the scheduler
    # restarts, and a report must not start services as a side effect.
    # Comments are stripped first - the header explains why `exec` is wrong,
    # and saying so must not trip the check that enforces it.
    code = "\n".join(line for line in body.splitlines() if not line.lstrip().startswith("#"))
    assert "compose exec" not in code


def test_in_stack_never_puts_a_credential_in_argv() -> None:
    """`-e NAME` takes the value from the environment; `-e NAME=value` shows it in ps."""
    code = "\n".join(
        line
        for line in IN_STACK.read_text(encoding="utf-8").splitlines()
        if not line.lstrip().startswith("#")
    )
    assert re.search(r"-e\s+CMS_OWNER_PASSWORD(?!=)", code)
    assert "CMS_OWNER_PASSWORD=" not in code.replace('CMS_OWNER_PASSWORD="', "ASSIGN")


def test_ci_does_not_depend_on_the_makefile() -> None:
    """This is the invariant that makes the change safe for CI.

    CI runs the same scripts directly against a PostgreSQL service container
    with its own pip-installed dependencies. If it ever started calling `make`,
    routing those targets through Docker would break the pipeline.
    """
    workflow = (REPO_ROOT / ".github" / "workflows" / "ci.yml").read_text(encoding="utf-8")
    offenders = [
        line.strip()
        for line in workflow.splitlines()
        if re.search(r"^\s*(run:\s*)?make\s+[a-z]", line)
    ]
    assert not offenders, (
        f"CI now invokes make: {offenders}. The warehouse-lifecycle targets run "
        "inside Docker, which a CI service container cannot provide."
    )


# --------------------------------------------------------------------------
# Credential rotation
#
# The PostgreSQL roles are created once, by docker/postgres/init/01_create_roles.sh,
# on the first boot of an empty pgdata volume. After that .env is only what the
# CLIENTS send - rewriting a password there without ALTERing the role in the
# running server breaks every connection in the stack.
# --------------------------------------------------------------------------

ROTATE_SCRIPT = REPO_ROOT / "scripts" / "rotate_credentials.sh"
LOGIN_CHECK_SCRIPT = REPO_ROOT / "scripts" / "check_airflow_login.sh"

#: Every credential the rotation must replace. Adding one to .env without
#: adding it here means it silently keeps its shipped value forever.
ROTATABLE = {
    "POSTGRES_PASSWORD",
    "WH_ETL_PASSWORD",
    "CMS_OWNER_PASSWORD",
    "CMS_READER_PASSWORD",
    "WH_ANALYST_PASSWORD",
    "AIRFLOW_DB_PASSWORD",
    "AIRFLOW_ADMIN_PASSWORD",
    "AIRFLOW__WEBSERVER__SECRET_KEY",
    "AIRFLOW__CORE__FERNET_KEY",
}


def test_rotation_covers_every_credential_in_the_example() -> None:
    """A password the rotation forgets is one that keeps its published value."""
    example = _parse_env((REPO_ROOT / ".env.example").read_text(encoding="utf-8"))
    body = ROTATE_SCRIPT.read_text(encoding="utf-8")

    secretish = {
        key
        for key in example
        if key.endswith(("_PASSWORD", "SECRET_KEY", "FERNET_KEY"))
        # The partner API token addresses a local mock with no auth behind it.
        and "PARTNER" not in key
    }
    uncovered = sorted(key for key in secretish if key not in body)
    assert not uncovered, f"rotate_credentials.sh never rotates: {uncovered}"
    assert secretish == ROTATABLE, f"credential set drifted: {secretish ^ ROTATABLE}"


def test_rotation_alters_roles_before_replacing_the_env_file() -> None:
    """Ordering is the safety property.

    ALTER first, authenticating with the credential still live in the running
    container; move .env only once that succeeded. Reverse the two and a failed
    ALTER leaves .env pointing at passwords the server never accepted.
    """
    body = ROTATE_SCRIPT.read_text(encoding="utf-8")
    alter_at = body.index("ALTER ROLE")
    move_at = body.index('mv "${NEW_FILE}" "${ENV_FILE}"')
    assert alter_at < move_at, "the .env swap must come after the role rotation"

    # And a failed ALTER must abandon the new file rather than leave it lying
    # around to be picked up by a later run.
    assert 'rm -f "${NEW_FILE}"' in body


def test_rotation_never_writes_secrets_anywhere_but_the_env_file() -> None:
    """No temp SQL file, and nothing secret in argv where `ps` would show it."""
    body = ROTATE_SCRIPT.read_text(encoding="utf-8")
    code = "\n".join(line for line in body.splitlines() if not line.lstrip().startswith("#"))
    # psql must receive the statements on stdin, not as -c/--command.
    assert "psql" in code
    assert (
        "-c " not in code.split("ALTER ROLE")[0].split("psql")[-1][:80]
    ), "the ALTER statements must be piped to psql, not passed as arguments"
    # The superuser password is read inside the container from its own
    # environment, so it is never interpolated by the host shell.
    assert 'PGPASSWORD="$POSTGRES_PASSWORD"' in code


@pytest.mark.parametrize("script", [ROTATE_SCRIPT, LOGIN_CHECK_SCRIPT])
def test_credential_scripts_never_echo_a_secret(script: Path) -> None:
    """These are run over someone's shoulder and pasted into issues."""
    secret_markers = (
        "AIRFLOW_ADMIN_PASSWORD",
        "POSTGRES_PASSWORD",
        "WH_ETL_PASSWORD",
        "FERNET_KEY",
        "SECRET_KEY",
    )
    for number, line in enumerate(script.read_text(encoding="utf-8").splitlines(), 1):
        stripped = line.strip()
        if stripped.startswith("#") or not stripped.startswith(("echo", "print(")):
            continue
        for marker in secret_markers:
            # Naming the variable is fine ("see AIRFLOW_ADMIN_PASSWORD in .env");
            # expanding it is not.
            assert (
                f"${{{marker}" not in stripped and f"${marker}" not in stripped
            ), f"{script.name}:{number} expands a secret into output: {stripped}"


def test_rotation_backs_up_before_the_one_way_door() -> None:
    """From just before the ALTER onward, BOTH value sets must exist on disk.

    Backing up after the commit instead opens a window where the database
    holds passwords that exist nowhere else - and the superuser password is
    among them, so that state is not recoverable by any means.
    """
    body = ROTATE_SCRIPT.read_text(encoding="utf-8")
    backup_at = body.index('cp "${ENV_FILE}" "${BACKUP}"')
    alter_at = body.index("ALTER ROLE")
    assert backup_at < alter_at, "the backup must be taken before the ALTER"


def test_rotation_recovers_from_failures_after_the_commit() -> None:
    """A failure past the commit must explain itself, not exit silently.

    `set -e` alone aborts with no output at all, leaving an operator with new
    passwords in the database, a half-recreated stack and no idea what to do.
    """
    body = ROTATE_SCRIPT.read_text(encoding="utf-8")
    assert "trap on_exit EXIT" in body, "no failure handler is installed"

    # Branches may be combined (`recreate|verify)`), so match the phase name
    # inside a case pattern rather than insisting on its own arm.
    handler = body.split("on_exit() {", 1)[1].split("trap on_exit EXIT", 1)[0]
    patterns = re.findall(r"^\s*([a-z|]+)\)\s*$", handler, re.MULTILINE)
    covered = {phase for pattern in patterns for phase in pattern.split("|")}
    for phase in ("prepare", "committed", "recreate", "verify"):
        assert phase in covered, f"the handler has no branch for the {phase} phase"

    # Every phase the script actually sets must be one the handler handles.
    assigned = set(re.findall(r'PHASE="([a-z]+)"', body)) - {"done"}
    assert assigned <= covered, f"phases set but unhandled: {assigned - covered}"

    # The committed branch must protect the only copy of the live credentials.
    committed = body.split("committed)", 1)[1].split(";;", 1)[0]
    assert "DO NOT DELETE" in committed
    assert 'rm -f "${NEW_FILE}"' not in committed, (
        "the post-commit branch must never delete .env.new - after the "
        "transaction it is the only record of the passwords now in force"
    )


def test_rotation_never_auto_deletes_the_backup() -> None:
    """The backup must outlive the run unless nothing was applied."""
    body = ROTATE_SCRIPT.read_text(encoding="utf-8")
    # The only permitted removal is inside the prepare branch, where the backup
    # is provably identical to the live file.
    for line_number, line in enumerate(body.splitlines(), 1):
        if 'rm -f "${BACKUP}"' in line:
            preceding = "\n".join(body.splitlines()[: line_number - 1])
            assert 'cmp -s "${BACKUP}" "${ENV_FILE}"' in preceding, (
                f"line {line_number} deletes the backup without proving it is "
                "identical to the current .env"
            )
    # And success must only ever SUGGEST deleting it.
    assert "rm $(basename" in body, "the success path should tell the user how"
    tail = body.split('PHASE="done"', 1)[1]
    assert 'rm -f "${BACKUP}"' not in tail, "success must not delete the backup"


def test_rotation_never_destroys_the_data_volume() -> None:
    """Rotating a password must never cost the warehouse.

    `docker compose down -v` and `make clean` both "work" and both delete
    every row in the database to change a credential.
    """
    code = "\n".join(
        line
        for line in ROTATE_SCRIPT.read_text(encoding="utf-8").splitlines()
        if not line.lstrip().startswith("#")
    )
    for forbidden in ("compose down", "--volumes", "volume rm", "make clean"):
        assert forbidden not in code, f"rotation must never run '{forbidden}'"
    assert "--force-recreate" in code, "containers must be replaced, not restarted"


def test_rotation_verifies_before_reporting_success() -> None:
    """Every credential must be proved against the running stack.

    verify_stack.sh logs in as all six roles; check_airflow_login.sh proves the
    admin password. A partial rotation cannot pass both, which is what makes
    "cannot leave clients on stale credentials" a checked claim.
    """
    body = ROTATE_SCRIPT.read_text(encoding="utf-8")
    verify_at = body.index("scripts/verify_stack.sh")
    login_at = body.index("scripts/check_airflow_login.sh")
    success_at = body.index("ROTATION COMPLETE AND VERIFIED")
    assert verify_at < success_at and login_at < success_at

    # The one-shot bootstrap is what reconciles the admin password, so a
    # non-zero exit from it must stop the run rather than be assumed away.
    assert "State.ExitCode" in body
    assert 'init_exit}" != "0"' in body


# --------------------------------------------------------------------------
# Git Bash on Windows
#
# Windows ships an "App Execution Alias" at
# %LOCALAPPDATA%\\Microsoft\\WindowsApps\\python3.exe: a zero-byte reparse point
# that exists, sits on PATH, and is marked executable. `command -v python3`
# therefore SUCCEEDS, and the interpreter then fails at the point of use with
#
#   Python was not found; run without arguments to install from the Microsoft
#   Store, or disable this shortcut from Settings > Apps > ...
#
# Existence is not executability. The fix is not a better probe - it is not
# needing the interpreter, since openssl and awk ship with Git for Windows.
# --------------------------------------------------------------------------

SECRETS_LIB = REPO_ROOT / "scripts" / "lib" / "secrets.sh"

#: Host-side scripts a developer runs before/around the stack. These must work
#: on a machine that has Docker and Git Bash and nothing else.
HOST_SCRIPTS = (
    REPO_ROOT / "scripts" / "generate_env.sh",
    ROTATE_SCRIPT,
    SECRETS_LIB,
)


@pytest.mark.parametrize("script", HOST_SCRIPTS, ids=lambda p: p.name)
def test_host_scripts_do_not_invoke_python(script: Path) -> None:
    """A language runtime must not stand between a clone and a running stack.

    Requiring one is bad enough on its own; on Windows it fails through the
    Store alias, which passes every existence check and then refuses to run.
    """
    for number, line in enumerate(script.read_text(encoding="utf-8").splitlines(), 1):
        stripped = line.strip()
        if stripped.startswith("#"):
            continue
        assert not re.search(
            r"\bpython3?\b\s", stripped
        ), f"{script.name}:{number} invokes a host Python: {stripped}"


def test_no_script_probes_for_a_command_without_running_it() -> None:
    """`command -v python3` is exactly the check the Store alias defeats.

    Comments are stripped first: secrets.sh quotes the broken probe verbatim to
    explain why it is broken, and documenting a trap must not trip the test
    that guards against it.
    """
    for script in HOST_SCRIPTS:
        code = "\n".join(
            line
            for line in script.read_text(encoding="utf-8").splitlines()
            if not line.lstrip().startswith("#")
        )
        assert "command -v python" not in code, (
            f"{script.name} probes for python by existence, which the Windows "
            "App Execution Alias satisfies without being able to run"
        )


def test_secret_helpers_use_only_git_bash_builtins() -> None:
    """openssl and awk ship with Git for Windows; nothing else may be assumed."""
    body = SECRETS_LIB.read_text(encoding="utf-8")
    for helper in (
        "require_entropy_source",
        "random_alnum",
        "random_hex",
        "random_fernet_key",
    ):
        assert f"{helper}()" in body, f"scripts/lib/secrets.sh lacks {helper}"

    # A weak fallback is worse than a hard failure: it produces a password that
    # looks fine and is guessable.
    code = "\n".join(line for line in body.splitlines() if not line.lstrip().startswith("#"))
    for weak in ("$RANDOM", "date +%s", "$$"):
        assert weak not in code, f"secrets.sh falls back to a weak source: {weak}"


def test_generated_fernet_key_is_a_real_fernet_key() -> None:
    """openssl replaces cryptography.fernet here, so prove they agree.

    Fernet.generate_key() is urlsafe_b64encode(os.urandom(32)); the helper must
    produce something the library itself accepts, not merely something that
    looks like a key.
    """
    bash = shutil.which("bash")
    if bash is None:  # pragma: no cover - bash is present everywhere this runs
        pytest.skip("bash not available")

    result = subprocess.run(  # noqa: S603 - fixed argv, absolute interpreter
        [bash, "-c", f". {SECRETS_LIB}; random_fernet_key"],
        capture_output=True,
        text=True,
        check=True,
    )
    key = result.stdout.strip()

    pytest.importorskip("cryptography")
    from cryptography.fernet import Fernet

    fernet = Fernet(key.encode())  # raises on a malformed key
    assert fernet.decrypt(fernet.encrypt(b"probe")) == b"probe"
    assert len(base64.urlsafe_b64decode(key)) == 32
    assert re.fullmatch(r"[A-Za-z0-9_-]+=*", key), "key must be URL-safe base64"


def test_generated_password_is_alphanumeric_and_long_enough() -> None:
    bash = shutil.which("bash")
    if bash is None:  # pragma: no cover
        pytest.skip("bash not available")

    result = subprocess.run(  # noqa: S603 - fixed argv, absolute interpreter
        [bash, "-c", f". {SECRETS_LIB}; random_alnum 32; echo; random_alnum 32"],
        capture_output=True,
        text=True,
        check=True,
    )
    first, second = result.stdout.strip().splitlines()
    assert len(first) == 32 and first.isalnum()
    assert first != second, "generator is not random between calls"


def test_env_backups_are_git_ignored() -> None:
    """Rotation keeps the previous .env, which is a live credential until deleted."""
    ignore = (REPO_ROOT / ".gitignore").read_text(encoding="utf-8").splitlines()
    assert ".env.*" in ignore, ".gitignore must cover .env.backup.* files"
    assert "!.env.example" in ignore, "the committed template must stay committed"


# --------------------------------------------------------------------------
# UI identity
#
# Branding must stay inside Airflow's documented configuration. A patched
# package or an overridden FAB template is a fork to re-do on every upgrade.
# --------------------------------------------------------------------------

#: Officially supported since 2.1 (instance_name) and 2.8/2.9 (navbar colours).
SUPPORTED_UI_SETTINGS = {
    "AIRFLOW__WEBSERVER__INSTANCE_NAME",
    "AIRFLOW__WEBSERVER__NAVBAR_COLOR",
    "AIRFLOW__WEBSERVER__NAVBAR_TEXT_COLOR",
    "AIRFLOW__WEBSERVER__NAVBAR_HOVER_COLOR",
    "AIRFLOW__WEBSERVER__NAVBAR_TEXT_HOVER_COLOR",
    "AIRFLOW__WEBSERVER__NAVBAR_LOGO_TEXT_COLOR",
}


def test_ui_identity_uses_only_supported_settings(services: dict[str, Any]) -> None:
    env = services["airflow-webserver"]["environment"]
    present = {k for k in env if "NAVBAR" in k or k.endswith("INSTANCE_NAME")}
    assert (
        present == SUPPORTED_UI_SETTINGS
    ), f"unexpected UI settings: {present ^ SUPPORTED_UI_SETTINGS}"


def test_ui_customisation_does_not_patch_airflow() -> None:
    """No overridden templates, no static-asset shadowing, no patched package.

    Airflow 2.10 has no supported hook for the login page, so the project does
    not customise it. A stable stock UI beats a fragile branded one.
    """
    forbidden = [
        REPO_ROOT / "plugins",
        REPO_ROOT / "docker" / "airflow" / "templates",
        REPO_ROOT / "docker" / "airflow" / "static",
    ]
    existing = [p.name for p in forbidden if p.exists()]
    assert not existing, f"UI override directories present: {existing}"

    dockerfile = DOCKERFILE.read_text(encoding="utf-8")
    for marker in ("login_db.html", "site-packages/airflow", "appbuilder"):
        assert marker not in dockerfile, f"Dockerfile appears to patch the Airflow UI ({marker})."


def test_navbar_colours_are_valid_hex(services: dict[str, Any]) -> None:
    """`#` survives YAML and dotenv here, but only unquoted and unspaced."""
    env = services["airflow-webserver"]["environment"]
    for key in SUPPORTED_UI_SETTINGS - {"AIRFLOW__WEBSERVER__INSTANCE_NAME"}:
        raw = str(env[key])
        default = raw.split(":-", 1)[1].rstrip("}") if ":-" in raw else raw
        assert re.fullmatch(r"#[0-9A-Fa-f]{6}", default), f"{key} = {default!r}"


def test_a_stale_copy_of_the_example_is_upgraded_not_refused(scratch_repo) -> None:
    """`cp .env.example .env` is muscle memory and was in older docs.

    An .env still carrying the placeholders has no real keys to protect, so
    the generator must fix it in place rather than refuse and leave the stack
    running on a published Fernet key.
    """
    shutil.copy(scratch_repo / ".env.example", scratch_repo / ".env")
    result = _run_generator(scratch_repo)

    assert result.returncode == 0, f"refused to upgrade a placeholder .env:\n{result.stderr}"
    values = _parse_env((scratch_repo / ".env").read_text(encoding="utf-8"))
    assert "REPLACE_ME" not in values["AIRFLOW__CORE__FERNET_KEY"]


def test_an_env_with_real_keys_is_still_protected(scratch_repo) -> None:
    """The other half: regenerating the Fernet key makes every credential
    already stored in the Airflow database undecryptable, so a real .env must
    never be overwritten without --force.
    """
    assert _run_generator(scratch_repo).returncode == 0
    generated = (scratch_repo / ".env").read_text(encoding="utf-8")

    refused = _run_generator(scratch_repo)
    assert refused.returncode != 0, "an .env with real keys was silently overwritten"
    assert "--force" in refused.stderr
    assert (scratch_repo / ".env").read_text(encoding="utf-8") == generated

    assert _run_generator(scratch_repo, "--force").returncode == 0
    assert (scratch_repo / ".env").read_text(encoding="utf-8") != generated


# --------------------------------------------------------------------------
# Mounts and ports
# --------------------------------------------------------------------------


def test_code_mounts_are_read_only(services: dict[str, Any]) -> None:
    """Airflow has no business writing to the source tree."""
    read_only_expected = ("/opt/airflow/dags", "/opt/airflow/src", "/opt/airflow/sql")
    for name in ("airflow-scheduler", "airflow-webserver", "airflow-init"):
        for mount in services[name]["volumes"]:
            for target in read_only_expected:
                if f":{target}" in mount:
                    assert mount.endswith(":ro"), f"{name}: {mount} should be read-only"


def test_postgres_init_directory_is_read_only(services: dict[str, Any]) -> None:
    mounts = services["postgres"]["volumes"]
    init_mounts = [m for m in mounts if "docker-entrypoint-initdb.d" in m]
    assert init_mounts, "the postgres init directory is not mounted"
    assert all(m.endswith(":ro") for m in init_mounts)


def test_only_two_ports_are_published(services: dict[str, Any]) -> None:
    """Minimal host exposure: PostgreSQL and the Airflow UI, nothing else."""
    published = {
        name: service["ports"] for name, service in services.items() if service.get("ports")
    }
    assert set(published) == {"postgres", "airflow-webserver"}
    assert len(published["postgres"]) == 1
    assert len(published["airflow-webserver"]) == 1


def test_data_volume_is_named_and_survives_down(compose: dict[str, Any]) -> None:
    """The warehouse must not live in an anonymous volume."""
    assert "pgdata" in compose["volumes"]
    assert compose["volumes"]["pgdata"]["name"] == "volthive_pgdata"


def test_postgres_superuser_is_not_used_by_airflow(services: dict[str, Any]) -> None:
    """Airflow connects as least-privilege roles, never as the superuser."""
    environment = services["airflow-scheduler"]["environment"]
    for key in ("AIRFLOW__DATABASE__SQL_ALCHEMY_CONN", "AIRFLOW_CONN_WAREHOUSE_DB"):
        assert "POSTGRES_USER" not in str(environment[key])
    assert "POSTGRES_PASSWORD" not in str(environment)
