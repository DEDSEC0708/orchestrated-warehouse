"""The DAGs must import under the CONTAINER's sys.path, not pytest's.

WHY tests/dags/test_dag_integrity.py could not catch this
--------------------------------------------------------
It builds a ``DagBag`` in-process, so it inherits whatever ``sys.path`` pytest
was started with - and that always includes the repository root, because the
rootdir is inserted automatically and ``PYTHONPATH=src:.`` is how the suite is
invoked. Every DAG then imports ``dags.common.datasets`` happily.

The scheduler has no such luxury. Airflow puts the DAGs FOLDER on ``sys.path``
(``/opt/airflow/dags``), and ``PYTHONPATH`` supplies the rest. Nothing puts the
PARENT of dags/ there unless the deployment says so - so ``import dags.common``
raised ``ModuleNotFoundError: No module named 'dags'`` for all four DAGs and for
``dags/common/default_args.py``: five import errors, zero DAGs, in a stack whose
webserver and scheduler were both perfectly healthy.

So this test does not build a DagBag in-process. It starts a SUBPROCESS whose
``sys.path`` is exactly what the container gives Airflow, derived from
``docker-compose.yml`` rather than copied from it - so changing PYTHONPATH there
without thinking breaks this test rather than the stack.
"""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest
import yaml

pytestmark = pytest.mark.dags

REPO_ROOT = Path(__file__).resolve().parents[2]
COMPOSE_FILE = REPO_ROOT / "docker-compose.yml"

#: Where the compose file mounts each host directory. Used to translate the
#: container's PYTHONPATH back into paths that exist on this machine.
CONTAINER_PREFIX = "/opt/airflow"

#: What the DAG folder must yield. Four DAGs, no more and no fewer - a fifth
#: appearing silently is as much a regression as one disappearing.
EXPECTED_DAG_IDS = {
    "volthive_ingest_master",
    "volthive_ingest_sessions",
    "volthive_build_warehouse",
    "volthive_maintenance",
}

_PROBE = """
import json, os, sys

# Airflow adds the DAGs folder itself; PYTHONPATH supplies the rest. Nothing
# else is on the path - in particular NOT the repository root, unless the
# deployment's PYTHONPATH puts it there.
sys.path.insert(0, os.environ["PROBE_DAGS_FOLDER"])

from airflow.models.dagbag import DagBag

bag = DagBag(dag_folder=os.environ["PROBE_DAGS_FOLDER"], include_examples=False)
print("PROBE_RESULT " + json.dumps({
    "dag_ids": sorted(bag.dags),
    "errors": {os.path.basename(k): v.strip().splitlines()[-1]
               for k, v in bag.import_errors.items()},
}))
"""


def _container_pythonpath() -> list[str]:
    """The PYTHONPATH the Airflow services actually run with, from compose."""
    compose = yaml.safe_load(COMPOSE_FILE.read_text(encoding="utf-8"))
    raw = str(compose["services"]["airflow-scheduler"]["environment"]["PYTHONPATH"])
    return [entry for entry in raw.split(":") if entry]


def _as_host_paths(entries: list[str]) -> list[str]:
    """Translate /opt/airflow/... to the equivalent path in this checkout."""
    host: list[str] = []
    for entry in entries:
        if entry == CONTAINER_PREFIX:
            host.append(str(REPO_ROOT))
        elif entry.startswith(CONTAINER_PREFIX + "/"):
            host.append(str(REPO_ROOT / entry[len(CONTAINER_PREFIX) + 1 :]))
        else:  # pragma: no cover - an absolute path outside the mount
            host.append(entry)
    return host


def _run_probe(pythonpath: list[str], tmp_path: Path) -> dict:
    env = {
        "PATH": "/usr/bin:/bin",
        "HOME": str(tmp_path),
        "AIRFLOW_HOME": str(tmp_path / "airflow"),
        "AIRFLOW__CORE__LOAD_EXAMPLES": "False",
        "AIRFLOW__CORE__UNIT_TEST_MODE": "True",
        "PROBE_DAGS_FOLDER": str(REPO_ROOT / "dags"),
        "PYTHONPATH": ":".join(pythonpath),
        # cwd is a temp dir and -c would otherwise put it on sys.path; keeping
        # the probe honest means nothing but PYTHONPATH decides what imports.
        "PYTHONDONTWRITEBYTECODE": "1",
    }
    result = subprocess.run(  # noqa: S603 - fixed argv, interpreter from sys.executable
        [sys.executable, "-c", _PROBE],
        env=env,
        cwd=tmp_path,
        capture_output=True,
        text=True,
        timeout=300,
    )
    marker = "PROBE_RESULT "
    for line in result.stdout.splitlines():
        if line.startswith(marker):
            return json.loads(line[len(marker) :])
    raise AssertionError(
        f"the probe produced no result (exit {result.returncode}).\n"
        f"stdout:\n{result.stdout[-2000:]}\nstderr:\n{result.stderr[-2000:]}"
    )


def test_every_dag_imports_under_the_containers_pythonpath(tmp_path: Path) -> None:
    """The real check: parse the DAGs the way the scheduler does."""
    outcome = _run_probe(_as_host_paths(_container_pythonpath()), tmp_path)

    assert not outcome["errors"], (
        "the scheduler would report these as DAG Import Errors:\n  "
        + "\n  ".join(f"{name}: {err}" for name, err in outcome["errors"].items())
    )
    assert set(outcome["dag_ids"]) == EXPECTED_DAG_IDS


def test_the_probe_fails_without_the_repository_root(tmp_path: Path) -> None:
    """Proves the probe is meaningful rather than passing by accident.

    Drop ``/opt/airflow`` from PYTHONPATH - the exact state that produced
    "DAG Import Errors (5)" - and every DAG must fail to import. If this ever
    passes, the probe has stopped reproducing the container.
    """
    without_root = [p for p in _as_host_paths(_container_pythonpath()) if p != str(REPO_ROOT)]
    outcome = _run_probe(without_root, tmp_path)

    assert outcome["dag_ids"] == []
    assert outcome["errors"], "removing the repository root should break every DAG"
    assert all("No module named 'dags'" in err for err in outcome["errors"].values())


def test_compose_puts_the_repository_root_on_the_path() -> None:
    """Both entries are required, and for different reasons."""
    entries = _container_pythonpath()
    assert f"{CONTAINER_PREFIX}/src" in entries, "`import volthive` needs /opt/airflow/src"
    assert CONTAINER_PREFIX in entries, (
        "`import dags.common...` needs /opt/airflow - Airflow only puts the "
        "DAGs folder on sys.path, not its parent"
    )


def test_the_image_and_compose_agree_on_pythonpath() -> None:
    """A bare `docker run` of the image must behave like the compose service."""
    dockerfile = (REPO_ROOT / "docker" / "airflow" / "Dockerfile").read_text(encoding="utf-8")
    expected = ":".join(_container_pythonpath())
    assert (
        f"ENV PYTHONPATH={expected}" in dockerfile
    ), f"the Dockerfile's PYTHONPATH does not match compose ({expected})"
