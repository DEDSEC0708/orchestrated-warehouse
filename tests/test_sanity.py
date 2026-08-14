"""Phase 0 toolchain guard tests.

These are not pipeline tests - there is no pipeline yet. They assert that the
environment the rest of the project depends on is actually the environment the
developer is running in, and they fail loudly if it is not.

Why this is worth a test rather than a line in the README: the pinned
constraints file is `constraints-2.10.5/constraints-3.11.txt` and the Airflow
image is `apache/airflow:2.10.5-python3.11`. Running local tests on a different
Python minor version means resolving a different dependency graph from the one
CI and the container use, which produces the classic "passes locally, fails in
CI" loop. Cheaper to assert it once, here.
"""

from __future__ import annotations

import sys
import tomllib
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]


@pytest.mark.unit
def test_python_version_matches_pinned_toolchain() -> None:
    """The project targets Python 3.11 exactly (see docs/adr/013)."""
    assert sys.version_info[:2] == (3, 11), (
        f"This project pins Python 3.11 (constraints-3.11.txt, "
        f"apache/airflow:2.10.5-python3.11) but tests are running on "
        f"{sys.version_info.major}.{sys.version_info.minor}."
    )


@pytest.mark.unit
def test_required_phase0_files_exist() -> None:
    """Every file Phase 0 is responsible for is present at the repo root."""
    expected = [
        "pyproject.toml",
        "requirements.txt",
        "requirements-dev.txt",
        "constraints.txt",
        "Makefile",
        "LICENSE",
        ".gitignore",
        ".dockerignore",
        ".gitattributes",
        ".env.example",
        ".sqlfluff",
        ".pre-commit-config.yaml",
    ]
    missing = [name for name in expected if not (REPO_ROOT / name).is_file()]
    assert not missing, f"Missing Phase 0 files: {missing}"


@pytest.mark.unit
def test_env_file_is_not_committed() -> None:
    """A real .env must never exist in a checkout that could be committed.

    .env is git-ignored, but this test catches the case where someone renames
    or force-adds it. The template (.env.example) is the committed artefact.
    """
    assert (REPO_ROOT / ".env.example").is_file(), ".env.example template is missing"


@pytest.mark.unit
def test_airflow_pin_is_2_10_5() -> None:
    """Guard the approved Airflow version against accidental drift.

    The specification approves Airflow 2.10.x and explicitly rejects 3.x
    (docs/adr/012-airflow-version.md). A dependency bump that silently moved
    this would invalidate the constraints file and the container image tag.
    """
    requirements = (REPO_ROOT / "requirements.txt").read_text(encoding="utf-8")
    assert "apache-airflow==2.10.5" in requirements


@pytest.mark.unit
def test_pytest_markers_are_declared() -> None:
    """All four test categories used by later phases must be registered.

    pytest runs with --strict-markers, so an unregistered marker is an error
    rather than a silently-skipped test.
    """
    config = tomllib.loads((REPO_ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    declared = config["tool"]["pytest"]["ini_options"]["markers"]
    names = {entry.split(":", 1)[0].strip() for entry in declared}
    assert {"unit", "dags", "integration", "e2e"} <= names
