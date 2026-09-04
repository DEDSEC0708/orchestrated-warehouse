"""Shared pytest fixtures.

The overriding concern is **determinism**. A test that reads the developer's
real ``.env``, or that inherits logging configuration from a test that ran
before it, passes or fails for reasons unrelated to the code under test. Both
are prevented below.

The second concern is **honest test doubles**. PostgreSQL is never mocked: the
entire value of the integration suite is whether the SQL is correct - the
merge, the point-in-time join, the window function, the constraints - and
mocking PostgreSQL tests the mock. The filesystem is exercised through
``tmp_path`` rather than a mocked ``open``, for the same reason. Only the clock
and the partner API's HTTP transport are faked, because determinism genuinely
requires it.
"""

from __future__ import annotations

import os
import shutil
from collections.abc import Iterator
from datetime import date
from pathlib import Path

import pytest
import structlog

from volthive.config.settings import reset_settings_cache

#: The minimum environment a valid Settings instance needs: the two required
#: secrets. Everything else has a safe default by design.
MINIMAL_ENV: dict[str, str] = {
    "WH_ETL_PASSWORD": "test_only_not_a_real_password",
    "CMS_READER_PASSWORD": "test_only_not_a_real_password",
}

#: Environment variables this project reads. Cleared before each test so the
#: developer's shell cannot influence an assertion.
_OWNED_PREFIXES = ("VOLTHIVE_", "WH_", "CMS_", "POSTGRES_", "ALERT_")

#: Set by the integration environment (docker compose, or the CI service
#: container) to say "a real PostgreSQL is reachable". Integration tests skip
#: cleanly rather than failing when it is absent, so `pytest` on a laptop with
#: no database still runs the unit and DAG suites to completion.
INTEGRATION_ENV_MARKER = "VOLTHIVE_TEST_DB"

REPO_ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture(autouse=True)
def _isolated_environment(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    """Remove every project environment variable and clear the settings cache."""
    for key in list(os.environ):
        if key.startswith(_OWNED_PREFIXES):
            monkeypatch.delenv(key, raising=False)
    reset_settings_cache()
    yield
    reset_settings_cache()


@pytest.fixture(autouse=True)
def _no_ambient_dotenv(
    monkeypatch: pytest.MonkeyPatch, tmp_path_factory: pytest.TempPathFactory
) -> None:
    """Run every test from an empty directory.

    ``Settings`` reads ``.env`` relative to the working directory. Without
    this, a developer with a populated ``.env`` at the repository root would
    see different test results from CI - and the "missing required setting"
    test would pass locally for the wrong reason. Paths inside the package are
    derived from ``__file__``, never from the working directory, so nothing
    else is affected.
    """
    monkeypatch.chdir(tmp_path_factory.mktemp("cwd"))


@pytest.fixture(autouse=True)
def _reset_structlog() -> Iterator[None]:
    """Reset structlog configuration and context between tests."""
    structlog.contextvars.clear_contextvars()
    yield
    structlog.reset_defaults()
    structlog.contextvars.clear_contextvars()


@pytest.fixture
def minimal_env(monkeypatch: pytest.MonkeyPatch) -> dict[str, str]:
    """Populate the minimum valid environment and return it."""
    for key, value in MINIMAL_ENV.items():
        monkeypatch.setenv(key, value)
    return dict(MINIMAL_ENV)


# ---------------------------------------------------------------------------
# Integration fixtures
# ---------------------------------------------------------------------------


def _database_available() -> bool:
    """Whether a real PostgreSQL is reachable for the integration suite."""
    if not os.environ.get(INTEGRATION_ENV_MARKER):
        return False
    try:
        import psycopg

        with psycopg.connect(os.environ[INTEGRATION_ENV_MARKER], connect_timeout=5):
            return True
    except Exception:  # - any failure means "not available"
        return False


requires_database = pytest.mark.skipif(
    not _database_available(),
    reason=(
        f"no PostgreSQL reachable; set {INTEGRATION_ENV_MARKER} to a warehouse DSN. "
        "`make up` starts one locally."
    ),
)


@pytest.fixture(scope="session")
def integration_env() -> dict[str, str]:
    """The environment the integration suite runs the pipeline under.

    Derived from the DSN in ``VOLTHIVE_TEST_DB`` so that one variable
    configures the whole suite, whether it points at the compose stack, a CI
    service container, or a throwaway local cluster.
    """
    dsn = os.environ.get(INTEGRATION_ENV_MARKER)
    if not dsn:
        pytest.skip(f"{INTEGRATION_ENV_MARKER} is not set")

    from urllib.parse import unquote, urlparse

    parsed = urlparse(dsn)
    return {
        "POSTGRES_HOST": parsed.hostname or "localhost",
        "POSTGRES_PORT": str(parsed.port or 5432),
        "WH_DB": (parsed.path or "/warehouse").lstrip("/"),
        "CMS_DB": os.environ.get("VOLTHIVE_TEST_CMS_DB", "cms"),
        "WH_ETL_USER": unquote(parsed.username or "wh_etl"),
        "WH_ETL_PASSWORD": unquote(parsed.password or ""),
        "CMS_READER_USER": os.environ.get("CMS_READER_USER", "cms_reader"),
        "CMS_READER_PASSWORD": os.environ.get(
            "CMS_READER_PASSWORD", unquote(parsed.password or "")
        ),
        "CMS_OWNER_USER": os.environ.get("CMS_OWNER_USER", "cms_owner"),
        "CMS_OWNER_PASSWORD": os.environ.get("CMS_OWNER_PASSWORD", unquote(parsed.password or "")),
        "VOLTHIVE_PARTNER_MODE": "file",
        "VOLTHIVE_GENERATOR_PROFILE": "tiny",
        "VOLTHIVE_GENERATOR_SEED": "42",
        "VOLTHIVE_LOG_LEVEL": "WARNING",
        "VOLTHIVE_LOG_JSON": "false",
        "VOLTHIVE_CONFIG_DIR": str(REPO_ROOT / "configs"),
    }


@pytest.fixture(scope="session")
def built_warehouse(
    integration_env: dict[str, str], tmp_path_factory: pytest.TempPathFactory
) -> Iterator[dict[str, object]]:
    """A fully built warehouse, from an empty schema, using the tiny profile.

    SESSION-SCOPED, deliberately. Building it takes a few seconds, and forty
    tests each rebuilding it would turn a six-second suite into a four-minute
    one - which is how integration tests come to be skipped, and then to rot.

    Tests that only READ share this. Tests that need to mutate use
    :func:`isolated_warehouse`, which gives them their own.

    Yields the window, the landing directory and the ground-truth manifest, so
    a test can assert quarantine counts against the numbers the GENERATOR
    recorded rather than against the pipeline's own output.
    """
    data_dir = tmp_path_factory.mktemp("volthive_data")
    env = {**integration_env, "VOLTHIVE_DATA_DIR": str(data_dir)}
    previous = {key: os.environ.get(key) for key in env}
    os.environ.update(env)
    reset_settings_cache()

    try:
        from volthive.db.connection import warehouse_connection
        from volthive.db.sqlfiles import run_sql_dir
        from volthive.dq.rules import sync_rules_to_database
        from volthive.generator import generate_all

        with warehouse_connection(application_name="pytest.setup") as conn:
            # Drop and rebuild, so a leftover schema from an earlier run - or a
            # developer's half-finished experiment - cannot make a test pass or
            # fail for reasons that have nothing to do with the code.
            with conn.cursor() as cur:
                for schema in ("mart", "core", "stg", "dq", "audit", "ctl", "raw"):
                    cur.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
            run_sql_dir(conn, "ddl")
            run_sql_dir(conn, "seed")
            sync_rules_to_database(conn)

        result = generate_all("tiny", data_dir=data_dir, load_cms=True)

        import sys

        sys.path.insert(0, str(REPO_ROOT / "scripts"))
        from run_pipeline import run as run_pipeline

        summary = run_pipeline(
            date_from=date(2026, 6, 1),
            date_to=date(2026, 6, 7),
            stages=["ingest", "stage", "core", "dq", "mart"],
            lookback_days=3,
            skip_dq_gate=False,
            is_backfill=False,
        )

        yield {
            "data_dir": data_dir,
            "date_from": date(2026, 6, 1),
            "date_to": date(2026, 6, 7),
            "batch_lo": date(2026, 5, 29),
            "batch_hi": date(2026, 6, 7),
            "generation": result,
            "truth": result.ledger.as_dict(),
            "summary": summary,
            "env": env,
        }
    finally:
        for key, value in previous.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value
        reset_settings_cache()


@pytest.fixture
def warehouse_conn(built_warehouse: dict[str, object]) -> Iterator[object]:
    """A connection to the built warehouse, for read-only assertions."""
    os.environ.update(built_warehouse["env"])  # type: ignore[arg-type]
    reset_settings_cache()
    from volthive.db.connection import warehouse_connection

    with warehouse_connection(application_name="pytest") as conn:
        yield conn


@pytest.fixture
def landing_copy(built_warehouse: dict[str, object], tmp_path: Path) -> Path:
    """A writable copy of the generated landing zone.

    Tests that simulate a REWRITTEN file - the late-arriving-data case - need
    to modify the landing zone, and doing that in place would corrupt the
    session-scoped fixture for every test that ran afterwards.
    """
    source = Path(str(built_warehouse["data_dir"])) / "landing"
    destination = tmp_path / "landing"
    shutil.copytree(source, destination)
    return destination
