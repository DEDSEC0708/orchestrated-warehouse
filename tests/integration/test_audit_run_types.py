"""``open_pipeline_run`` must accept what Airflow actually sends it.

This is the behavioural counterpart to tests/unit/test_audit_triggered_by.py.
The unit tests prove the translation function is correct; these prove
``open_pipeline_run`` actually applies it, and that the interval constraint
admits the point-in-time interval a dataset-scheduled DAG has. Deleting the
normalisation call, or tightening the constraint back to ``>``, fails here -
neither is visible to a test that only exercises the helper.

Each case below is a real Airflow run shape that produced a red task in the
Graph view while ``make run-clean`` stayed green.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
from tests.conftest import requires_database

from volthive.audit import TRIGGERED_BY_DOMAIN, open_pipeline_run
from volthive.db.connection import transaction

pytestmark = [pytest.mark.integration, requires_database]

_INSTANT = datetime(2026, 6, 7, 20, 0, tzinfo=UTC)

#: (label, Airflow run_type, interval start, interval end, expected stored value)
AIRFLOW_RUN_SHAPES = [
    # A Dataset trigger: Airflow's own spelling is 17 characters, and the run
    # has no interval at all. This is volthive_build_warehouse's normal path.
    ("dataset trigger", "dataset_triggered", _INSTANT, _INSTANT, "dataset"),
    # Trigger-DAG button on the same DAG: still a point-in-time interval.
    ("manual trigger of a dataset DAG", "manual", _INSTANT, _INSTANT, "manual"),
    # The cron-scheduled ingestion DAGs.
    ("cron schedule", "scheduled", _INSTANT, _INSTANT + timedelta(days=1), "schedule"),
    ("backfill", "backfill", _INSTANT, _INSTANT + timedelta(days=1), "backfill"),
]


@pytest.mark.parametrize(
    ("label", "run_type", "start", "end", "expected"),
    AIRFLOW_RUN_SHAPES,
    ids=[shape[0] for shape in AIRFLOW_RUN_SHAPES],
)
def test_airflow_run_shapes_are_accepted(
    warehouse_conn,
    label: str,
    run_type: str,
    start: datetime,
    end: datetime,
    expected: str,
) -> None:
    with transaction(warehouse_conn):
        run_id = open_pipeline_run(
            warehouse_conn,
            dag_id="volthive_build_warehouse",
            airflow_run_id=f"pytest__{run_type}__{start.isoformat()}",
            data_interval_start_utc=start,
            data_interval_end_utc=end,
            triggered_by=run_type,
        )

    with warehouse_conn.cursor() as cur:
        cur.execute(
            "SELECT triggered_by, data_interval_start_utc, data_interval_end_utc "
            "FROM audit.pipeline_run WHERE pipeline_run_id = %(id)s",
            {"id": run_id},
        )
        row = cur.fetchone()

    assert row is not None, f"{label}: no audit row was written"
    assert row["triggered_by"] == expected
    assert row["triggered_by"] in TRIGGERED_BY_DOMAIN
    assert row["data_interval_end_utc"] >= row["data_interval_start_utc"]

    # Leave the audit table as we found it - these are synthetic runs.
    with transaction(warehouse_conn), warehouse_conn.cursor() as cur:
        cur.execute(
            "DELETE FROM audit.pipeline_run WHERE pipeline_run_id = %(id)s",
            {"id": run_id},
        )


def test_an_inverted_interval_is_still_refused(warehouse_conn) -> None:
    """Relaxing `>` to `>=` must not have relaxed it to "anything goes"."""
    import psycopg

    with pytest.raises(psycopg.errors.CheckViolation), transaction(warehouse_conn):
        open_pipeline_run(
            warehouse_conn,
            dag_id="volthive_build_warehouse",
            airflow_run_id="pytest__inverted",
            data_interval_start_utc=_INSTANT,
            data_interval_end_utc=_INSTANT - timedelta(hours=1),
            triggered_by="manual",
        )
