"""No task callable may declare a parameter that Airflow reserves.

Airflow builds a task's arguments with ``KeywordParameters.determine``. When a
callable accepts ``**kwargs``, Airflow intends to hand it the whole task
context - so a parameter whose name is also a context key is ambiguous, and
Airflow refuses rather than guessing:

    ValueError: The key 'run_id' in args is a part of kwargs and therefore
    reserved.

This is raised while resolving arguments, BEFORE the function body runs, so no
amount of correctness inside the task can save it. Every task in this project
that carried the pipeline run id was written as

    def rebuild_staging(run_id: str, **context) -> int:

and ``run_id`` is one of Airflow's 48 context keys. The result in the UI was a
Graph view where start and the advisory-lock tasks were green and every task
doing real work was red - while ``make run-clean`` stayed green, because the
CLI calls the same underlying functions directly and never goes through
Airflow's argument binding.

``volthive_maintenance`` was the only healthy DAG, and only because it is the
one DAG whose tasks take no run id.

The reserved set is read from Airflow itself, so an upgrade that reserves a new
name fails here rather than in a scheduler log.
"""

from __future__ import annotations

import inspect
from pathlib import Path

import pytest
from airflow.models.dagbag import DagBag
from airflow.utils.context import KNOWN_CONTEXT_KEYS

pytestmark = pytest.mark.dags

DAGS_FOLDER = Path(__file__).resolve().parents[2] / "dags"


@pytest.fixture(scope="module")
def dagbag() -> DagBag:
    return DagBag(dag_folder=str(DAGS_FOLDER), include_examples=False)


def _python_callables(dagbag: DagBag):
    """Every (dag_id, task_id, callable) pair Airflow will have to bind."""
    for dag_id, dag in sorted(dagbag.dags.items()):
        for task in dag.tasks:
            func = getattr(task, "python_callable", None)
            if func is not None:
                yield dag_id, task.task_id, func


def test_the_dagbag_is_not_empty(dagbag: DagBag) -> None:
    """Guard against this whole file passing because it inspected nothing."""
    assert not dagbag.import_errors, dagbag.import_errors
    assert len(list(_python_callables(dagbag))) >= 10


def test_no_task_parameter_collides_with_an_airflow_context_key(dagbag: DagBag) -> None:
    """The exact check Airflow performs, run at test time instead of run time."""
    offenders: list[str] = []

    for dag_id, task_id, func in _python_callables(dagbag):
        signature = inspect.signature(func)
        takes_kwargs = any(
            p.kind is inspect.Parameter.VAR_KEYWORD for p in signature.parameters.values()
        )
        if not takes_kwargs:
            # Without **kwargs Airflow passes only what is declared, so a
            # context-shaped name is merely unusual, not fatal.
            continue
        for name, param in signature.parameters.items():
            if param.kind is inspect.Parameter.VAR_KEYWORD:
                continue
            if name in KNOWN_CONTEXT_KEYS:
                offenders.append(f"{dag_id}.{task_id}({name}=..., **kwargs)")

    assert not offenders, (
        "these tasks will fail with \"The key '<name>' in args is a part of "
        'kwargs and therefore reserved" before their body runs:\n  '
        + "\n  ".join(offenders)
        + "\nRename the parameter - `pipeline_run_id` rather than `run_id`."
    )


def test_run_id_specifically_is_reserved() -> None:
    """Pin the assumption this whole file rests on.

    If a future Airflow stops reserving `run_id`, the guard above silently
    stops guarding the thing that actually broke.
    """
    assert "run_id" in KNOWN_CONTEXT_KEYS
    assert "dag_run" in KNOWN_CONTEXT_KEYS


def test_tasks_that_need_the_run_id_still_receive_one(dagbag: DagBag) -> None:
    """Renaming must not have quietly dropped the correlation id.

    Every task that does warehouse work needs the pipeline run id - it is what
    stamps `dw_run_id` on every row it writes.
    """
    expected = {
        ("volthive_build_warehouse", "staging.rebuild_staging"),
        ("volthive_build_warehouse", "dimensions.merge_dimensions"),
        ("volthive_build_warehouse", "facts.load_facts"),
        ("volthive_build_warehouse", "dq_publish_gate"),
        ("volthive_build_warehouse", "mart.rebuild_mart"),
        ("volthive_build_warehouse", "close_pipeline_run"),
    }
    seen = {
        (dag_id, task_id)
        for dag_id, task_id, func in _python_callables(dagbag)
        if "pipeline_run_id" in inspect.signature(func).parameters
    }
    assert expected <= seen, f"these tasks lost the run id: {sorted(expected - seen)}"
