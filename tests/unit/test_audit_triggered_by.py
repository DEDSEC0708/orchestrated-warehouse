"""Airflow's run-type vocabulary must reach the audit table translated.

``audit.pipeline_run.triggered_by`` is a closed domain: five words, enforced by
``ck_pipeline_run_triggered_by`` and sized by a VARCHAR(16) column. Airflow's
``DagRun.run_type`` uses different words, and one of them is 17 characters.

Passing run_type through verbatim broke every DAG that opens a run, in three
ways depending on how the run started:

    scheduled          -> CheckViolation on ck_pipeline_run_triggered_by
    dataset_triggered  -> StringDataRightTruncation (before any CHECK runs)
    manual             -> fine

Only ``volthive_maintenance`` stayed green in the UI, and only because it is
the one DAG that never calls ``open_run``. The CLI stayed green because
``run_pipeline.py`` passes the literal ``"manual"`` - so neither the test suite
nor a real ``make run-clean`` could reveal it.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

from volthive.audit import (
    TRIGGERED_BY_DOMAIN,
    normalise_triggered_by,
)
from volthive.exceptions import ContractError

pytestmark = pytest.mark.unit

REPO_ROOT = Path(__file__).resolve().parents[2]
AUDIT_DDL = REPO_ROOT / "sql" / "ddl" / "02_audit.sql"

#: The column width the mapped value has to fit into.
TRIGGERED_BY_MAX_LENGTH = 16


def test_every_airflow_run_type_maps_into_the_domain() -> None:
    """The early-warning test.

    If an Airflow upgrade adds a run type, this fails - rather than a DAG
    failing in production with a constraint violation nobody can read.
    """
    airflow_types = pytest.importorskip("airflow.utils.types")
    for run_type in airflow_types.DagRunType:
        mapped = normalise_triggered_by(run_type.value)
        assert mapped in TRIGGERED_BY_DOMAIN, (
            f"Airflow run_type {run_type.value!r} maps to {mapped!r}, which "
            f"ck_pipeline_run_triggered_by will reject"
        )
        assert (
            len(mapped) <= TRIGGERED_BY_MAX_LENGTH
        ), f"{mapped!r} does not fit VARCHAR({TRIGGERED_BY_MAX_LENGTH})"


@pytest.mark.parametrize(
    ("run_type", "expected"),
    [
        ("scheduled", "schedule"),
        ("dataset_triggered", "dataset"),
        ("asset_triggered", "dataset"),
        ("manual", "manual"),
        ("backfill", "backfill"),
        ("test", "test"),
        (None, "schedule"),
        ("", "schedule"),
        ("  manual  ", "manual"),
    ],
)
def test_translation(run_type: str | None, expected: str) -> None:
    assert normalise_triggered_by(run_type) == expected


def test_an_unknown_run_type_fails_loudly_and_names_itself() -> None:
    """Better here, naming the value, than in the database naming a constraint."""
    with pytest.raises(ContractError) as caught:
        normalise_triggered_by("something_new_in_airflow_4")
    message = str(caught.value)
    assert "something_new_in_airflow_4" in message
    assert "_AIRFLOW_RUN_TYPES" in message, "the error must say where to add the mapping"
    assert caught.value.is_retryable is False


def test_the_domain_matches_the_check_constraint_in_the_ddl() -> None:
    """The Python domain and the SQL CHECK must not drift apart."""
    ddl = AUDIT_DDL.read_text(encoding="utf-8")
    match = re.search(
        r"ck_pipeline_run_triggered_by CHECK \(\s*triggered_by IN \(([^)]*)\)",
        ddl,
        re.DOTALL,
    )
    assert match, "could not find ck_pipeline_run_triggered_by in the DDL"
    in_sql = set(re.findall(r"'([a-z_]+)'", match.group(1)))
    assert in_sql == set(
        TRIGGERED_BY_DOMAIN
    ), f"DDL allows {sorted(in_sql)}, Python allows {sorted(TRIGGERED_BY_DOMAIN)}"


def test_the_interval_constraint_admits_a_point_in_time_run() -> None:
    """A dataset-scheduled DAG has no interval - Airflow gives it an instant.

    ``data_interval_start == data_interval_end`` is the normal, correct state
    for such a run, so requiring a strictly positive interval rejected every
    single one. An INVERTED interval must still be refused.
    """
    ddl = AUDIT_DDL.read_text(encoding="utf-8")
    match = re.search(
        r"CONSTRAINT ck_pipeline_run_interval CHECK \(([^)]*)\)",
        ddl,
    )
    assert match, "could not find ck_pipeline_run_interval in the DDL"
    assert ">=" in match.group(1), (
        "ck_pipeline_run_interval must accept a zero-length interval; a "
        "dataset-triggered run always has one"
    )


def test_the_ddl_migrates_an_existing_warehouse() -> None:
    """`CREATE TABLE IF NOT EXISTS` cannot relax a constraint on a live table.

    Without an explicit ALTER, `make db-init` leaves an existing warehouse on
    the old constraint and the DAG keeps failing - while a fresh clone works,
    which is the worst way to ship a fix.
    """
    ddl = AUDIT_DDL.read_text(encoding="utf-8")
    assert "DROP CONSTRAINT IF EXISTS ck_pipeline_run_interval" in ddl
    assert "ADD CONSTRAINT ck_pipeline_run_interval" in ddl


def test_the_dags_do_not_translate_run_type_themselves() -> None:
    """Translation belongs at the audit boundary, in exactly one place.

    Each DAG passing its own mapping is how three call sites drift apart.
    """
    for dag_file in (REPO_ROOT / "dags").glob("volthive_*.py"):
        body = dag_file.read_text(encoding="utf-8")
        assert "dataset_triggered" not in body, (
            f"{dag_file.name} hardcodes an Airflow run-type spelling; "
            "volthive.audit.normalise_triggered_by owns that translation"
        )
