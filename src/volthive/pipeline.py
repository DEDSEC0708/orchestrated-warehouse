"""Task bodies for the Airflow DAGs.

Every function here is what one Airflow task actually does. The DAG files
declare orchestration - dependencies, retries, timeouts, schedules - and call
into this module; they contain no business logic at all.

That split is not cosmetic. Business logic in a DAG file:

* cannot be unit-tested without an Airflow runtime;
* is re-parsed by the scheduler every few seconds, so an expensive import
  becomes a permanent tax on the whole deployment;
* turns a one-line bug fix into a DAG redeploy.

It is the most common mistake in junior Airflow code, and a DAG integrity test
in ``tests/dags/`` enforces the split by failing if any DAG file exceeds a
length that only business logic could produce.

Every function takes the Airflow context values it needs as ORDINARY
ARGUMENTS - ``run_id``, ``data_interval_start``, ``data_interval_end`` - rather
than reaching into a global context. So each one can be called directly from a
test, from a script, or from the end-to-end suite, with no Airflow anywhere.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Any

from volthive.audit import LoadStat, close_pipeline_run, open_pipeline_run, record_task_run
from volthive.db.connection import fetch_value, transaction, warehouse_connection
from volthive.db.preflight import require_initialised_warehouse
from volthive.dq.engine import GateDecision, evaluate_gate, run_dataset_checks
from volthive.ingest.cms import CMS_ENTITIES, ingest_cms_entity
from volthive.ingest.files import ingest_file_source, ingest_seed_file
from volthive.ingest.partner_api import ingest_partner_cdrs
from volthive.logging_setup import bind_run_context, get_logger
from volthive.transform.core import run_dimensions, run_facts, run_mart
from volthive.transform.staging import run_staging

__all__ = [
    "ADVISORY_LOCK_KEY",
    "acquire_build_lock",
    "batch_window",
    "close_run",
    "ingest_all_cms",
    "ingest_partner",
    "ingest_seed",
    "ingest_sessions_files",
    "open_run",
    "release_build_lock",
    "run_dq_gate",
    "stage_all",
    "transform_dimensions",
    "transform_facts",
    "transform_mart",
]

log = get_logger(__name__)

#: Advisory lock key for the warehouse build. An arbitrary constant, but a
#: STABLE one: every process that takes this lock must use the same number, so
#: it lives here rather than being typed at each call site.
ADVISORY_LOCK_KEY = 8_102_026


def batch_window(
    data_interval_start: datetime,
    data_interval_end: datetime,
    *,
    lookback_days: int = 3,
) -> tuple[date, date]:
    """The restatement window a run rebuilds, as inclusive dates.

    Derived from AIRFLOW'S DATA INTERVAL, never from ``now()``. That is what
    makes a rerun reproducible and a backfill correct: the same logical run
    always processes the same window, whenever it happens to execute.

    The lower bound extends back by the lookback so late-arriving records are
    picked up; the upper bound is the interval end minus a microsecond, so a
    daily run for the 14th has a batch key of the 14th rather than the 15th.
    """
    hi = (data_interval_end - timedelta(microseconds=1)).date()
    lo = (data_interval_start - timedelta(days=lookback_days)).date()
    return lo, hi


def open_run(
    *,
    dag_id: str,
    airflow_run_id: str,
    data_interval_start: datetime,
    data_interval_end: datetime,
    triggered_by: str = "schedule",
) -> str:
    """Open ``audit.pipeline_run`` and return the correlation ID.

    Pushed to XCom by the DAG and pulled by every downstream task. XCom carries
    ONLY this UUID and small counters - never data. "Did they push a dataframe
    through XCom?" is a real thing interviewers check, and the answer here is
    visible in the type signature.
    """
    with warehouse_connection(application_name=f"{dag_id}.open_run") as conn, transaction(conn):
        # Every run starts here - the scripts and all three DAGs - which makes
        # this the one place worth asserting that the warehouse actually
        # exists. Without it the next statement fails with
        # `relation "audit.pipeline_run" does not exist`, which is true but
        # tells the reader nothing about the missing `make db-init`.
        require_initialised_warehouse(conn)
        run_id = open_pipeline_run(
            conn,
            dag_id=dag_id,
            airflow_run_id=airflow_run_id,
            data_interval_start_utc=data_interval_start,
            data_interval_end_utc=data_interval_end,
            triggered_by=triggered_by,
        )
    bind_run_context(pipeline_run_id=run_id, dag_id=dag_id)
    return run_id


def close_run(*, run_id: str, dag_id: str, status: str, error_summary: str | None = None) -> None:
    """Close ``audit.pipeline_run``, totalling from ``audit.load_stat``.

    Called with ``trigger_rule='all_done'`` so it runs even when the DAG
    failed. A run left in RUNNING for ever is not merely untidy: the stale-run
    sweep and the concurrency guard both key off that status.
    """
    with warehouse_connection(application_name=f"{dag_id}.close_run") as conn, transaction(conn):
        close_pipeline_run(conn, run_id, status=status, error_summary=error_summary)


def acquire_build_lock(*, dag_id: str = "volthive_build_warehouse") -> bool:
    """Take a session-level advisory lock for the warehouse build.

    ``max_active_runs=1`` stops the SCHEDULER from starting two runs of the
    same DAG, but it cannot stop a manually-triggered run, a backfill or a
    developer's script from interleaving with a scheduled one. Two concurrent
    warehouse builds would race on the restatement window and on watermark
    advancement.

    An advisory lock costs about five lines and closes that gap. It is
    session-scoped, so it releases automatically if the process dies - which is
    exactly what you want from a lock nobody will remember to clean up.

    Returns True if acquired. The caller raises if not: refusing to start is
    correct, because the other run is presumably doing the work.
    """
    with warehouse_connection(application_name=f"{dag_id}.lock") as conn:
        acquired = bool(
            fetch_value(conn, "SELECT pg_try_advisory_lock(:key)", {"key": ADVISORY_LOCK_KEY})
        )
    if acquired:
        log.info("advisory_lock_acquired", key=ADVISORY_LOCK_KEY)
    else:
        log.warning("advisory_lock_unavailable", key=ADVISORY_LOCK_KEY)
    return acquired


def release_build_lock(*, dag_id: str = "volthive_build_warehouse") -> None:
    """Release the advisory lock. Idempotent and never raises.

    Called with ``trigger_rule='all_done'``. A lock that outlives a failed run
    blocks every subsequent one, so releasing it must not itself be able to
    fail the task that releases it.
    """
    try:
        with warehouse_connection(application_name=f"{dag_id}.unlock") as conn:
            fetch_value(conn, "SELECT pg_advisory_unlock(:key)", {"key": ADVISORY_LOCK_KEY})
        log.info("advisory_lock_released", key=ADVISORY_LOCK_KEY)
    except Exception as exc:  # - releasing must never fail the task
        log.warning("advisory_lock_release_failed", error=str(exc))


# --------------------------------------------------------------------------
# Ingestion
# --------------------------------------------------------------------------


def ingest_all_cms(
    *,
    run_id: str,
    data_interval_start: datetime,
    data_interval_end: datetime,
    dag_id: str | None = None,
    entities: list[str] | None = None,
) -> dict[str, int]:
    """Extract the CMS entities for one window."""
    counts: dict[str, int] = {}
    with warehouse_connection(application_name=f"{dag_id or 'volthive'}.ingest_cms") as conn:
        for entity in entities or CMS_ENTITIES:
            stat = ingest_cms_entity(
                conn,
                entity,
                run_id=run_id,
                data_interval_start=data_interval_start,
                data_interval_end=data_interval_end,
                dag_id=dag_id,
            )
            counts[entity] = stat.rows_inserted
    return counts


def ingest_sessions_files(
    *,
    run_id: str,
    data_interval_start: datetime,
    data_interval_end: datetime,
    sources: list[str] | None = None,
    dag_id: str | None = None,
) -> dict[str, int]:
    """Ingest the partitioned landing files for one window."""
    counts: dict[str, int] = {}
    with warehouse_connection(application_name=f"{dag_id or 'volthive'}.ingest_files") as conn:
        for source in sources or ["ocpp_cdr", "meter_value"]:
            stat = ingest_file_source(
                conn,
                source,
                run_id=run_id,
                data_interval_start=data_interval_start,
                data_interval_end=data_interval_end,
                dag_id=dag_id,
            )
            counts[source] = stat.rows_inserted
    return counts


def ingest_partner(
    *,
    run_id: str,
    data_interval_start: datetime,
    data_interval_end: datetime,
    dag_id: str | None = None,
) -> int:
    """Extract roaming partner records for the cursor window."""
    with warehouse_connection(application_name=f"{dag_id or 'volthive'}.ingest_partner") as conn:
        return ingest_partner_cdrs(
            conn,
            run_id=run_id,
            data_interval_start=data_interval_start,
            data_interval_end=data_interval_end,
            dag_id=dag_id,
        ).rows_inserted


def ingest_seed(*, run_id: str, dag_id: str | None = None) -> int:
    """Reload the static grid-tariff reference in full."""
    with warehouse_connection(application_name=f"{dag_id or 'volthive'}.ingest_seed") as conn:
        return ingest_seed_file(conn, run_id=run_id, dag_id=dag_id).rows_inserted


# --------------------------------------------------------------------------
# Transformation
# --------------------------------------------------------------------------


def stage_all(
    *,
    run_id: str,
    batch_lo: date,
    batch_hi: date,
    dag_id: str | None = None,
    only: list[str] | None = None,
) -> list[LoadStat]:
    """Rebuild staging for the restatement window."""
    with warehouse_connection(application_name=f"{dag_id or 'volthive'}.staging") as conn:
        return run_staging(
            conn, run_id=run_id, batch_lo=batch_lo, batch_hi=batch_hi, dag_id=dag_id, only=only
        )


def transform_dimensions(
    *,
    run_id: str,
    batch_lo: date,
    batch_hi: date,
    dag_id: str | None = None,
    is_backfill: bool = False,
) -> list[LoadStat]:
    """Merge the dimensions, then create inferred members."""
    with warehouse_connection(application_name=f"{dag_id or 'volthive'}.dimensions") as conn:
        return run_dimensions(
            conn,
            run_id=run_id,
            batch_lo=batch_lo,
            batch_hi=batch_hi,
            dag_id=dag_id,
            is_backfill=is_backfill,
        )


def transform_facts(
    *, run_id: str, batch_lo: date, batch_hi: date, dag_id: str | None = None
) -> list[LoadStat]:
    """Load the three facts over the restatement window."""
    with warehouse_connection(application_name=f"{dag_id or 'volthive'}.facts") as conn:
        return run_facts(conn, run_id=run_id, batch_lo=batch_lo, batch_hi=batch_hi, dag_id=dag_id)


def transform_mart(
    *, run_id: str, batch_lo: date, batch_hi: date, dag_id: str | None = None
) -> list[LoadStat]:
    """Rebuild the mart aggregates in full."""
    with warehouse_connection(application_name=f"{dag_id or 'volthive'}.mart") as conn:
        return run_mart(conn, run_id=run_id, batch_lo=batch_lo, batch_hi=batch_hi, dag_id=dag_id)


# --------------------------------------------------------------------------
# Data quality
# --------------------------------------------------------------------------


def run_dq_gate(
    *,
    run_id: str,
    batch_lo: date,
    batch_hi: date,
    skip_gate: bool = False,
    layers: list[str] | None = None,
) -> GateDecision:
    """Run the dataset checks and decide whether the mart may publish.

    ``skip_gate`` is the emergency override, and it is deliberately LOUD: the
    bypass is logged at WARNING with the rules it bypassed and recorded in
    ``audit.pipeline_run.error_summary``. Overriding quality is sometimes
    necessary at 3 a.m.; doing it invisibly never is.
    """
    with warehouse_connection(application_name="volthive.dq_gate") as conn:
        results = run_dataset_checks(
            conn, run_id=run_id, batch_lo=batch_lo, batch_hi=batch_hi, layers=layers
        )
        decision = evaluate_gate(results, skip_gate=skip_gate)

        if skip_gate and decision.blocking_rules:
            log.warning("dq_gate_bypassed", rules=decision.blocking_rules)
            with transaction(conn), conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE audit.pipeline_run
                    SET error_summary = %s
                    WHERE pipeline_run_id = %s
                    """,
                    (decision.message, run_id),
                )

    if decision.passed:
        log.info("dq_gate_passed", checks=decision.checks_run, warnings=decision.warned_rules)
    else:
        log.error("dq_gate_blocked", rules=decision.blocking_rules)
    return decision


def record_failure(
    *,
    dag_id: str,
    task_id: str,
    run_id: str | None,
    airflow_run_id: str | None,
    try_number: int,
    error_type: str | None,
    error_message: str | None,
    status: str = "FAILED",
) -> None:
    """Write one ``audit.task_run`` row. Never raises.

    Called from the Airflow failure and retry callbacks. A callback that can
    itself fail turns one failed task into two, and the second one hides the
    first - so every exception here is swallowed and logged.
    """
    try:
        with warehouse_connection(application_name=f"{dag_id}.callback") as conn, transaction(conn):
            record_task_run(
                conn,
                dag_id=dag_id,
                task_id=task_id,
                status=status,
                pipeline_run_id=run_id,
                airflow_run_id=airflow_run_id,
                try_number=try_number,
                error_type=error_type,
                error_message=error_message,
            )
    except Exception as exc:  # - an audit write must never mask a failure
        log.warning("task_run_audit_write_failed", task_id=task_id, error=str(exc))


_ = Any  # re-exported implicitly by the DAG callables' signatures
