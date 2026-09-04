"""Writers for the ``audit`` schema: pipeline runs, task attempts, load stats.

These tables are a first-class output of the platform, not debug residue. They
are written by the SAME TRANSACTION as the data they describe, which is what
makes it impossible for the audit trail and the warehouse to disagree: either
both the rows and their row count committed, or neither did.

What each table answers:

``audit.pipeline_run``
    What ran, when, over which logical window, triggered how, on which commit.
    Its primary key is the correlation ID stamped on every warehouse row as
    ``dw_run_id``.

``audit.task_run``
    One row per task ATTEMPT. Written from the Airflow failure and retry
    callbacks, so "it passed on the third try" stays visible instead of being
    flattened into a green tick.

``audit.load_stat``
    Rows read, inserted, updated, deleted, quarantined and deduplicated, per
    target table per batch. This is the table that answers "is today's volume
    normal?" and it is what the row-count anomaly rule reads.

The load-stat counters are separate on purpose: ``rows_read`` minus
``rows_inserted`` should be exactly ``rows_quarantined`` plus
``rows_duplicate_skipped``, and that identity is asserted by a data-quality
rule. A single "rows processed" counter could not express it.
"""

from __future__ import annotations

import os
import subprocess
import uuid
from dataclasses import asdict, dataclass, field
from datetime import date, datetime
from functools import lru_cache
from typing import Any

import psycopg

from volthive.logging_setup import get_logger

__all__ = [
    "LoadStat",
    "close_pipeline_run",
    "current_git_sha",
    "open_pipeline_run",
    "record_task_run",
    "write_load_stat",
]

log = get_logger(__name__)


@lru_cache(maxsize=1)
def current_git_sha() -> str | None:
    """Best-effort short commit SHA of the running code.

    Read from ``GIT_SHA`` when the environment supplies one (CI, and any future
    image build that bakes it in), otherwise from ``git rev-parse``. Returns
    None rather than raising when neither is available - the container image
    has no ``.git`` directory, and a missing provenance field must never fail a
    pipeline run.
    """
    from_env = os.environ.get("GIT_SHA") or os.environ.get("GITHUB_SHA")
    if from_env:
        return from_env[:40]
    try:
        result = subprocess.run(  # noqa: S603
            ["git", "rev-parse", "HEAD"],  # noqa: S607
            capture_output=True,
            text=True,
            timeout=5,
            check=False,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    return result.stdout.strip()[:40] or None if result.returncode == 0 else None


@dataclass(slots=True)
class LoadStat:
    """One row of ``audit.load_stat``.

    A dataclass rather than a bag of keyword arguments so that a caller who
    forgets ``rows_quarantined`` gets a zero with a name attached, and so the
    reconciliation identity can be asserted against a typed object in tests.
    """

    task_id: str
    target_table: str
    dw_batch_key: date
    pipeline_run_id: str | None = None
    dag_id: str | None = None
    source_system: str | None = None
    entity: str | None = None
    rows_read: int = 0
    rows_inserted: int = 0
    rows_updated: int = 0
    rows_deleted: int = 0
    rows_quarantined: int = 0
    rows_duplicate_skipped: int = 0
    files_seen: int = 0
    files_loaded: int = 0
    files_skipped: int = 0
    bytes_read: int = 0
    duration_ms: int | None = None
    extra: dict[str, Any] = field(default_factory=dict)

    def reconciles(self) -> bool:
        """Whether rows_read is fully accounted for by the outcome counters.

        ``read = inserted + quarantined + duplicates_skipped``

        The accounting identity of the pipeline: if it holds, no row was lost
        between the source and the target, and that can be SAID WITH PROOF
        rather than hoped. Loads that legitimately do not conserve rows (an
        aggregate, or the interval derivation which turns n samples into n-1
        intervals) report ``rows_read = 0`` and are therefore not claimed to
        reconcile.
        """
        if self.rows_read == 0:
            return True
        return (
            self.rows_read
            == self.rows_inserted + self.rows_quarantined + self.rows_duplicate_skipped
        )


def open_pipeline_run(
    conn: psycopg.Connection,
    *,
    dag_id: str,
    airflow_run_id: str,
    data_interval_start_utc: datetime,
    data_interval_end_utc: datetime,
    triggered_by: str = "schedule",
    pipeline_run_id: str | None = None,
) -> str:
    """Insert a RUNNING row and return the pipeline run ID.

    The returned UUID is the correlation ID for everything that follows: it is
    bound into the structured logging context, passed as a SQL parameter to
    every load, and stamped on every row written as ``dw_run_id``. From one
    fact row you can therefore reach the run, the task, the source file and the
    git commit that produced it.
    """
    run_id = pipeline_run_id or str(uuid.uuid4())
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO audit.pipeline_run (
                pipeline_run_id, dag_id, airflow_run_id,
                data_interval_start_utc, data_interval_end_utc,
                triggered_by, status, git_sha
            )
            VALUES (%s, %s, %s, %s, %s, %s, 'RUNNING', %s)
            ON CONFLICT (pipeline_run_id) DO NOTHING
            """,
            (
                run_id,
                dag_id,
                airflow_run_id,
                data_interval_start_utc,
                data_interval_end_utc,
                triggered_by,
                current_git_sha(),
            ),
        )
    log.info(
        "pipeline_run_opened",
        pipeline_run_id=run_id,
        dag_id=dag_id,
        triggered_by=triggered_by,
    )
    return run_id


def close_pipeline_run(
    conn: psycopg.Connection,
    pipeline_run_id: str,
    *,
    status: str,
    error_summary: str | None = None,
) -> None:
    """Close a pipeline run, filling in its totals from ``audit.load_stat``.

    Totals are DERIVED here rather than accumulated in XCom across tasks. XCom
    should carry identifiers and small counters, never state that has to be
    kept consistent - and a task that dies mid-run would leave an accumulator
    permanently wrong, whereas the load-stat rows it did write are still there
    to be summed.

    Called with ``trigger_rule='all_done'`` so a run is closed out even when it
    failed. A run left in RUNNING for ever is not just untidy: the maintenance
    DAG's stale-run sweep and the concurrency guard both key off that status.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE audit.pipeline_run AS r
            SET status = %s,
                ended_at_utc = now(),
                error_summary = %s,
                rows_ingested = COALESCE(t.rows_ingested, 0),
                rows_quarantined = COALESCE(t.rows_quarantined, 0),
                rows_loaded_core = COALESCE(t.rows_loaded_core, 0)
            FROM (
                SELECT
                    SUM(rows_inserted) FILTER (WHERE target_table LIKE 'raw.%%')  AS rows_ingested,
                    SUM(rows_quarantined)                                         AS rows_quarantined,
                    SUM(rows_inserted) FILTER (WHERE target_table LIKE 'core.%%') AS rows_loaded_core
                FROM audit.load_stat
                WHERE pipeline_run_id = %s
            ) AS t
            WHERE r.pipeline_run_id = %s
            """,
            (status, error_summary, pipeline_run_id, pipeline_run_id),
        )
    log.info("pipeline_run_closed", pipeline_run_id=pipeline_run_id, status=status)


def record_task_run(
    conn: psycopg.Connection,
    *,
    dag_id: str,
    task_id: str,
    status: str,
    pipeline_run_id: str | None = None,
    airflow_run_id: str | None = None,
    try_number: int = 1,
    duration_ms: int | None = None,
    error_type: str | None = None,
    error_message: str | None = None,
) -> None:
    """Record one task attempt.

    ``error_message`` is truncated to 4000 characters. A full traceback belongs
    in the structured log, where it is searchable; a database column holding
    an unbounded string is how an audit table becomes the largest table in the
    warehouse.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO audit.task_run (
                pipeline_run_id, dag_id, task_id, airflow_run_id, try_number,
                status, duration_ms, error_type, error_message
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                pipeline_run_id,
                dag_id,
                task_id,
                airflow_run_id,
                try_number,
                status,
                duration_ms,
                error_type,
                (error_message or "")[:4000] or None,
            ),
        )


def write_load_stat(conn: psycopg.Connection, stat: LoadStat) -> None:
    """Append one ``audit.load_stat`` row.

    Called INSIDE the load's own transaction, so the statistics and the data
    they describe commit or roll back together.
    """
    payload = asdict(stat)
    payload.pop("extra", None)
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO audit.load_stat (
                pipeline_run_id, dag_id, task_id, source_system, entity,
                target_table, dw_batch_key,
                rows_read, rows_inserted, rows_updated, rows_deleted,
                rows_quarantined, rows_duplicate_skipped,
                files_seen, files_loaded, files_skipped,
                bytes_read, duration_ms
            )
            VALUES (
                %(pipeline_run_id)s, %(dag_id)s, %(task_id)s, %(source_system)s, %(entity)s,
                %(target_table)s, %(dw_batch_key)s,
                %(rows_read)s, %(rows_inserted)s, %(rows_updated)s, %(rows_deleted)s,
                %(rows_quarantined)s, %(rows_duplicate_skipped)s,
                %(files_seen)s, %(files_loaded)s, %(files_skipped)s,
                %(bytes_read)s, %(duration_ms)s
            )
            """,
            payload,
        )

    if not stat.reconciles():
        # A WARNING, not an exception. The reconciliation rule in the quality
        # gate is what decides whether a discrepancy blocks the publish; this
        # log line is there so the discrepancy is visible in the task's own
        # output at the moment it happens, rather than only in a check result
        # nobody reads until tomorrow.
        log.warning(
            "load_stat_does_not_reconcile",
            target_table=stat.target_table,
            rows_read=stat.rows_read,
            rows_inserted=stat.rows_inserted,
            rows_quarantined=stat.rows_quarantined,
            rows_duplicate_skipped=stat.rows_duplicate_skipped,
        )
