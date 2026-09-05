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
    (tmp_path / "scripts").mkdir()
    shutil.copy(REPO_ROOT / ".env.example", tmp_path / ".env.example")
    target = tmp_path / "scripts" / "generate_env.sh"
    shutil.copy(REPO_ROOT / "scripts" / "generate_env.sh", target)
    target.chmod(0o755)
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
