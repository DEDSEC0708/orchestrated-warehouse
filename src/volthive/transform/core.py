"""Run the dimension and fact loads.

Like :mod:`volthive.transform.staging`, this is a thin runner: SQL file,
parameters, transaction boundary, statistics. Every join, hash and arithmetic
decision lives in ``sql/core/``.

**Order is correctness here, not preference.** Three dependencies are real:

1. **Inferred members before facts.** A fact whose charge point is not yet in
   the dimension needs a placeholder to point at, or its foreign key cannot
   resolve.
2. **Dimensions before facts.** Facts resolve surrogate keys point-in-time, so
   the versions have to exist first.
3. **Session fact before interval fact.** The interval inherits
   ``charging_session_sk`` from the session it belongs to.

Everything else is independent, which is why the DAG can run the four SCD2
merges in parallel.

**Dimension merges are skipped during a backfill**, and that is the single
most important operational note in this module - see :func:`run_dimensions`.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import date
from typing import Any

import psycopg

from volthive.audit import LoadStat, write_load_stat
from volthive.db.connection import fetch_value, transaction
from volthive.db.sqlfiles import run_sql_file
from volthive.logging_setup import get_logger

__all__ = [
    "DIMENSION_STEPS",
    "FACT_STEPS",
    "MART_STEPS",
    "CoreStep",
    "run_core_step",
    "run_dimensions",
    "run_facts",
    "run_mart",
]

log = get_logger(__name__)


@dataclass(frozen=True, slots=True)
class CoreStep:
    """One core or mart transformation."""

    name: str
    sql_file: str
    target_table: str
    count_sql: str | None = None
    #: Windowed steps take :batch_lo/:batch_hi; full rebuilds take only :run_id.
    windowed: bool = True


#: The four SCD2 merges plus the two Type 1 dimension loads.
#:
#: FK order matters for readability rather than for correctness - the merges do
#: not reference each other - but station before charge point matches the
#: dependency in the model and makes the DAG graph tell the truth about it.
DIMENSION_STEPS: list[CoreStep] = [
    CoreStep(
        "merge_dim_customer",
        "core/scd2_merge_customer.sql",
        "core.dim_customer",
        "SELECT count(*) FROM core.dim_customer WHERE customer_sk > 0",
    ),
    CoreStep(
        "merge_dim_station",
        "core/scd2_merge_station.sql",
        "core.dim_station",
        "SELECT count(*) FROM core.dim_station WHERE station_sk > 0",
    ),
    CoreStep(
        "merge_dim_tariff_plan",
        "core/scd2_merge_tariff_plan.sql",
        "core.dim_tariff_plan",
        "SELECT count(*) FROM core.dim_tariff_plan WHERE tariff_plan_sk > 0",
    ),
    CoreStep(
        "merge_dim_charge_point",
        "core/scd2_merge_charge_point.sql",
        "core.dim_charge_point",
        "SELECT count(*) FROM core.dim_charge_point WHERE charge_point_sk > 0",
    ),
    CoreStep(
        "load_dim_vehicle",
        "core/load_dim_vehicle.sql",
        "core.dim_vehicle",
        "SELECT count(*) FROM core.dim_vehicle WHERE vehicle_sk > 0",
    ),
    CoreStep(
        "load_dim_session_outcome",
        "core/load_dim_session_outcome.sql",
        "core.dim_session_outcome",
        "SELECT count(*) FROM core.dim_session_outcome",
    ),
]

#: Runs after the dimension merges and before the facts.
INFERRED_MEMBER_STEP = CoreStep(
    "create_inferred_members",
    "core/create_inferred_members.sql",
    "core.dim_charge_point",
    "SELECT count(*) FROM core.dim_charge_point WHERE is_inferred",
)

FACT_STEPS: list[CoreStep] = [
    CoreStep(
        "load_fact_charging_session",
        "core/load_fact_charging_session.sql",
        "core.fact_charging_session",
        "SELECT count(*) FROM core.fact_charging_session "
        "WHERE dw_batch_key BETWEEN :batch_lo AND :batch_hi",
    ),
    CoreStep(
        "load_fact_meter_interval",
        "core/load_fact_meter_interval.sql",
        "core.fact_meter_interval",
        "SELECT count(*) FROM core.fact_meter_interval "
        "WHERE dw_batch_key BETWEEN :batch_lo AND :batch_hi",
    ),
    CoreStep(
        "load_fact_station_daily_utilization",
        "core/load_fact_station_daily_utilization.sql",
        "core.fact_station_daily_utilization",
        "SELECT count(*) FROM core.fact_station_daily_utilization "
        "WHERE dw_batch_key BETWEEN :batch_lo AND :batch_hi",
    ),
]

MART_STEPS: list[CoreStep] = [
    CoreStep(
        "mart_station_month_kpi",
        "mart/00_station_month_kpi.sql",
        "mart.mart_station_month_kpi",
        "SELECT count(*) FROM mart.mart_station_month_kpi",
        windowed=False,
    ),
    CoreStep(
        "mart_customer_month_kpi",
        "mart/01_customer_month_kpi.sql",
        "mart.mart_customer_month_kpi",
        "SELECT count(*) FROM mart.mart_customer_month_kpi",
        windowed=False,
    ),
    CoreStep(
        "mart_charge_point_daily",
        "mart/02_charge_point_daily.sql",
        "mart.mart_charge_point_daily",
        "SELECT count(*) FROM mart.mart_charge_point_daily",
        windowed=False,
    ),
]


def run_core_step(
    conn: psycopg.Connection,
    step: CoreStep,
    *,
    run_id: str,
    batch_lo: date,
    batch_hi: date,
    dag_id: str | None = None,
) -> LoadStat:
    """Execute one core or mart transformation in its own transaction."""
    started = time.monotonic()
    params: dict[str, Any] = {"run_id": run_id}
    if step.windowed:
        params.update({"batch_lo": batch_lo, "batch_hi": batch_hi})
    window = {"batch_lo": batch_lo, "batch_hi": batch_hi}

    with transaction(conn):
        affected = run_sql_file(conn, step.sql_file, params)
        rows = int(fetch_value(conn, step.count_sql, window) or 0) if step.count_sql else affected
        stat = LoadStat(
            task_id=step.name,
            dag_id=dag_id,
            pipeline_run_id=run_id,
            target_table=step.target_table,
            dw_batch_key=batch_hi,
            rows_inserted=rows,
            duration_ms=int((time.monotonic() - started) * 1000),
        )
        write_load_stat(conn, stat)

    log.info(
        "core_step_completed",
        step=step.name,
        target_table=step.target_table,
        rows=rows,
        duration_ms=stat.duration_ms,
    )
    return stat


def run_dimensions(
    conn: psycopg.Connection,
    *,
    run_id: str,
    batch_lo: date,
    batch_hi: date,
    dag_id: str | None = None,
    is_backfill: bool = False,
) -> list[LoadStat]:
    """Run the dimension merges, then create inferred members.

    ``is_backfill`` SKIPS THE SCD2 MERGES, and the reason is the most important
    operational caveat in this project.

    Backfilling facts is safe by design: each run's window comes from the
    Airflow data interval, and the load is a delete-insert over that window.

    Backfilling DIMENSIONS is not, and it is not a bug. The source exposes only
    current state plus ``updated_at``. Re-running a dimension merge for a date
    in the past would apply TODAY'S attributes with a HISTORICAL
    effective_from, corrupting the very history the dimension exists to
    preserve. So a fact backfill re-resolves its keys against the EXISTING
    dimension history - which is correct - and rebuilding true dimension
    history is a separate, explicit operation that replays raw in updated_at
    order (``scripts/rebuild_dimension_history.py``).

    "Backfilling my facts is safe, and here is precisely why backfilling my
    dimensions is not automatic" is the honest answer, and it is a far stronger
    one than pretending the problem does not exist.
    """
    if is_backfill:
        log.warning(
            "dimension_merges_skipped_backfill",
            reason=(
                "A backfill re-resolves facts against existing dimension history. "
                "Re-running the merge would apply current attributes with a historical "
                "effective_from and corrupt that history. Use "
                "scripts/rebuild_dimension_history.py for a genuine history rebuild."
            ),
        )
        return [
            run_core_step(
                conn,
                INFERRED_MEMBER_STEP,
                run_id=run_id,
                batch_lo=batch_lo,
                batch_hi=batch_hi,
                dag_id=dag_id,
            )
        ]

    stats = [
        run_core_step(
            conn, step, run_id=run_id, batch_lo=batch_lo, batch_hi=batch_hi, dag_id=dag_id
        )
        for step in DIMENSION_STEPS
    ]
    stats.append(
        run_core_step(
            conn,
            INFERRED_MEMBER_STEP,
            run_id=run_id,
            batch_lo=batch_lo,
            batch_hi=batch_hi,
            dag_id=dag_id,
        )
    )
    return stats


def run_facts(
    conn: psycopg.Connection,
    *,
    run_id: str,
    batch_lo: date,
    batch_hi: date,
    dag_id: str | None = None,
) -> list[LoadStat]:
    """Run the three fact loads in dependency order."""
    return [
        run_core_step(
            conn, step, run_id=run_id, batch_lo=batch_lo, batch_hi=batch_hi, dag_id=dag_id
        )
        for step in FACT_STEPS
    ]


def run_mart(
    conn: psycopg.Connection,
    *,
    run_id: str,
    batch_lo: date,
    batch_hi: date,
    dag_id: str | None = None,
) -> list[LoadStat]:
    """Rebuild the mart aggregates in full."""
    return [
        run_core_step(
            conn, step, run_id=run_id, batch_lo=batch_lo, batch_hi=batch_hi, dag_id=dag_id
        )
        for step in MART_STEPS
    ]
