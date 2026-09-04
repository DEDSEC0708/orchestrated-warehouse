"""Tests that the package is importable and that importing it stays cheap."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

REPO_ROOT = Path(__file__).resolve().parents[2]


def _run_python(code: str) -> subprocess.CompletedProcess[str]:
    """Run a snippet in a fresh interpreter with only src/ on the path.

    A subprocess is used rather than an in-process import because the point of
    these tests is what happens on a *cold* import, and the pytest session has
    already imported half the package.
    """
    return subprocess.run(  # noqa: S603 - fixed command, no shell, no user input
        [sys.executable, "-c", code],
        capture_output=True,
        text=True,
        cwd=REPO_ROOT,
        env={"PYTHONPATH": str(REPO_ROOT / "src"), "PATH": ""},
        check=False,
        timeout=60,
    )


def test_package_imports_and_exposes_a_version() -> None:
    import volthive

    assert volthive.__version__ == "0.1.0"


def test_public_modules_import() -> None:
    from volthive import exceptions, logging_setup
    from volthive.config import settings

    assert exceptions.VoltHiveError is not None
    assert callable(logging_setup.configure_logging)
    assert settings.Settings is not None


def test_importing_the_package_does_not_read_configuration() -> None:
    """Importing must not touch the environment.

    The Airflow scheduler re-imports every DAG file - and therefore this
    package - on a short interval. If importing loaded and validated settings,
    a missing environment variable would take down DAG parsing entirely
    instead of failing one task, and the cost would be paid hundreds of times
    an hour. Verified with an empty environment: a cold import must succeed
    even though no required setting is set.
    """
    result = _run_python("import volthive; print(volthive.__version__)")

    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "0.1.0"


def test_importing_the_package_does_not_pull_in_heavy_submodules() -> None:
    """`import volthive` must not drag in settings, yaml or structlog."""
    result = _run_python(
        "import sys, volthive; "
        "print(','.join(sorted(m for m in sys.modules "
        "if m.startswith('volthive') or m in {'yaml', 'structlog', 'pydantic'})))"
    )

    assert result.returncode == 0, result.stderr
    loaded = {name for name in result.stdout.strip().split(",") if name}
    assert loaded == {"volthive"}, f"cold import loaded more than expected: {loaded}"
