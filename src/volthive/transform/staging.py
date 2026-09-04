"""Run the staging transformations.

Thin by design. Every one of these functions does the same three things -
choose a SQL file, supply parameters, own the transaction - and contains no
business logic whatsoever. The logic is in ``sql/stg/``, where it can be
linted, diffed, and pasted straight into ``psql`` when a load misbehaves.

That split is not stylistic. Business logic embedded in Python strings cannot
be linted by sqlfluff, is painful to read in a code review, and cannot be run
by hand against the database when something looks wrong at 2 a.m.

**Each entity is its own transaction.** The alternative - one transaction for
all of staging - would mean a failure in the meter load discards the perfectly
good session load that preceded it. Per-entity transactions mean a rerun skips
what already succeeded, and because every load rebuilds its own window,
re-running a successful one is a no-op rather than a duplication.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from datetime import date
from typing import Any

import psycopg

from volthive.audit import LoadStat, write_load_stat
from volthive.db.connection import fetch_value, transaction
from volthive.db.sqlfiles import run_sql_file
from volthive.logging_setup import get_logger

__all__ = ["STAGING_STEPS", "StagingStep", "run_staging", "run_staging_step"]

log = get_logger(__name__)


@dataclass(frozen=True, slots=True)
class StagingStep:
    """One staging transformation: a SQL file and the table it fills."""

    name: str
    sql_file: str
    target_table: str
    source_system: str
    entity: str
    #: Counts rows that reached the target. Used for audit.load_stat.
    count_sql: str | None = None
    #: Counts rows that were rejected, for the reconciliation identity.
    quarantine_sql: str | None = None
    #: Counts records collapsed as duplicates, so read = staged + rejected +
    #: deduplicated holds exactly.
    duplicate_sql: str | None = None
    extra_params: dict[str, Any] = field(default_factory=dict)


#: The staging pipeline, IN DEPENDENCY ORDER.
#:
#: Order is load-bearing in two places and incidental everywhere else:
#:   * sessions must precede partner conformance, which reads stg.session to
#:     decide which partner records are cross-source duplicates;
#:   * sessions must precede meter intervals, because an interval is only
#:     derived for a sample whose session is present.
#: The master-data loads are independent of each other and of these.
STAGING_STEPS: list[StagingStep] = [
    StagingStep(
        name="stg_customer",
        sql_file="stg/00_customer.sql",
        target_table="stg.customer",
        source_system="CMS",
        entity="customers",
        count_sql="SELECT count(*) FROM stg.customer",
        quarantine_sql=(
            "SELECT count(*) FROM dq.quarantine_cms_entity "
            "WHERE entity = 'customers' AND dw_batch_key BETWEEN :batch_lo AND :batch_hi"
        ),
    ),
    StagingStep(
        name="stg_station",
        sql_file="stg/01_station.sql",
        target_table="stg.station",
        source_system="CMS",
        entity="stations",
        count_sql="SELECT count(*) FROM stg.station",
        quarantine_sql=(
            "SELECT count(*) FROM dq.quarantine_cms_entity "
            "WHERE entity = 'stations' AND dw_batch_key BETWEEN :batch_lo AND :batch_hi"
        ),
    ),
    StagingStep(
        name="stg_tariff_plan",
        sql_file="stg/02_tariff_plan.sql",
        target_table="stg.tariff_plan",
        source_system="CMS",
        entity="tariff_plans",
        count_sql="SELECT count(*) FROM stg.tariff_plan",
        quarantine_sql=(
            "SELECT count(*) FROM dq.quarantine_cms_entity "
            "WHERE entity = 'tariff_plans' AND dw_batch_key BETWEEN :batch_lo AND :batch_hi"
        ),
    ),
    StagingStep(
        name="stg_charge_point",
        sql_file="stg/03_charge_point.sql",
        target_table="stg.charge_point",
        source_system="CMS",
        entity="charge_points",
        count_sql="SELECT count(*) FROM stg.charge_point",
        quarantine_sql=(
            "SELECT count(*) FROM dq.quarantine_cms_entity "
            "WHERE entity = 'charge_points' AND dw_batch_key BETWEEN :batch_lo AND :batch_hi"
        ),
    ),
    StagingStep(
        name="stg_vehicle",
        sql_file="stg/04_vehicle.sql",
        target_table="stg.vehicle",
        source_system="CMS",
        entity="vehicles",
        count_sql="SELECT count(*) FROM stg.vehicle",
    ),
    StagingStep(
        name="stg_grid_tariff",
        sql_file="stg/05_grid_tariff_slab.sql",
        target_table="stg.grid_tariff_slab",
        source_system="SEED",
        entity="grid_tariff_slab",
        count_sql="SELECT count(*) FROM stg.grid_tariff_slab",
    ),
    StagingStep(
        name="stg_session",
        sql_file="stg/10_session.sql",
        target_table="stg.session",
        source_system="OCPP",
        entity="ocpp_cdr",
        count_sql=(
            "SELECT count(*) FROM stg.session "
            "WHERE dw_batch_key BETWEEN :batch_lo AND :batch_hi AND source_system = 'OCPP'"
        ),
        quarantine_sql=(
            "SELECT count(*) FROM dq.quarantine_ocpp_cdr "
            "WHERE dw_batch_key BETWEEN :batch_lo AND :batch_hi"
        ),
        # Records landed minus DISTINCT transaction ids: exactly the retry-storm
        # re-deliveries and superseded corrections that the dedupe collapsed.
        duplicate_sql=(
            "SELECT count(*) - count(DISTINCT payload ->> 'transaction_id') "
            "FROM raw.ocpp_cdr WHERE dw_batch_key BETWEEN :batch_lo AND :batch_hi"
        ),
    ),
    StagingStep(
        name="stg_partner_session",
        sql_file="stg/11_partner_session.sql",
        target_table="stg.partner_session",
        source_system="PARTNER",
        entity="partner_cdr",
        count_sql=(
            "SELECT count(*) FROM stg.partner_session "
            "WHERE dw_batch_key BETWEEN :batch_lo AND :batch_hi"
        ),
        quarantine_sql=(
            "SELECT count(*) FROM dq.quarantine_partner_cdr "
            "WHERE dw_batch_key BETWEEN :batch_lo AND :batch_hi"
        ),
        duplicate_sql=(
            "SELECT count(*) - count(DISTINCT payload ->> 'cdr_id') "
            "FROM raw.partner_cdr WHERE dw_batch_key BETWEEN :batch_lo AND :batch_hi"
        ),
    ),
    StagingStep(
        name="stg_session_conform_partner",
        sql_file="stg/12_session_partner_conform.sql",
        target_table="stg.session",
        source_system="PARTNER",
        entity="partner_cdr",
        count_sql=(
            "SELECT count(*) FROM stg.session "
            "WHERE dw_batch_key BETWEEN :batch_lo AND :batch_hi AND source_system = 'PARTNER'"
        ),
    ),
    StagingStep(
        name="stg_meter_sample",
        sql_file="stg/20_meter_sample.sql",
        target_table="stg.meter_sample",
        source_system="METER",
        entity="meter_value",
        count_sql=(
            "SELECT count(*) FROM stg.meter_sample "
            "WHERE dw_batch_key BETWEEN :batch_lo AND :batch_hi"
        ),
        quarantine_sql=(
            "SELECT count(*) FROM dq.quarantine_meter_value "
            "WHERE dw_batch_key BETWEEN :batch_lo AND :batch_hi"
        ),
        duplicate_sql=(
            "SELECT count(*) - count(DISTINCT sample_id) FROM raw.meter_value "
            "WHERE dw_batch_key BETWEEN :batch_lo AND :batch_hi"
        ),
    ),
    StagingStep(
        name="stg_meter_interval",
        sql_file="stg/21_meter_interval.sql",
        target_table="stg.meter_interval",
        source_system="METER",
        entity="meter_value",
        count_sql=(
            "SELECT count(*) FROM stg.meter_interval "
            "WHERE dw_batch_key BETWEEN :batch_lo AND :batch_hi"
        ),
    ),
    StagingStep(
        name="stg_meter_orphan_expiry",
        sql_file="stg/22_meter_orphan_expiry.sql",
        target_table="dq.quarantine_meter_value",
        source_system="METER",
        entity="meter_value",
        extra_params={"lookback_days": 3},
    ),
]


def run_staging_step(
    conn: psycopg.Connection,
    step: StagingStep,
    *,
    run_id: str,
    batch_lo: date,
    batch_hi: date,
    dag_id: str | None = None,
) -> LoadStat:
    """Execute one staging transformation and record its statistics."""
    started = time.monotonic()
    params: dict[str, Any] = {
        "run_id": run_id,
        "batch_lo": batch_lo,
        "batch_hi": batch_hi,
        **step.extra_params,
    }
    window = {"batch_lo": batch_lo, "batch_hi": batch_hi}

    with transaction(conn):
        affected = run_sql_file(conn, step.sql_file, params)
        rows_inserted = (
            int(fetch_value(conn, step.count_sql, window) or 0) if step.count_sql else affected
        )
        rows_quarantined = (
            int(fetch_value(conn, step.quarantine_sql, window) or 0) if step.quarantine_sql else 0
        )
        rows_duplicate = (
            max(int(fetch_value(conn, step.duplicate_sql, window) or 0), 0)
            if step.duplicate_sql
            else 0
        )

        stat = LoadStat(
            task_id=step.name,
            dag_id=dag_id,
            pipeline_run_id=run_id,
            source_system=step.source_system,
            entity=step.entity,
            target_table=step.target_table,
            dw_batch_key=batch_hi,
            # rows_read is only claimed where the identity read = staged +
            # quarantined + deduplicated actually applies. A derivation that
            # legitimately does not conserve rows - the interval load turns n
            # samples into n-1 intervals - reports zero rather than pretending.
            rows_read=(
                rows_inserted + rows_quarantined + rows_duplicate if step.duplicate_sql else 0
            ),
            rows_inserted=rows_inserted,
            rows_quarantined=rows_quarantined,
            rows_duplicate_skipped=rows_duplicate,
            duration_ms=int((time.monotonic() - started) * 1000),
        )
        write_load_stat(conn, stat)

    log.info(
        "staging_step_completed",
        step=step.name,
        rows_inserted=rows_inserted,
        rows_quarantined=rows_quarantined,
        rows_duplicate_skipped=rows_duplicate,
        duration_ms=stat.duration_ms,
    )
    return stat


def run_staging(
    conn: psycopg.Connection,
    *,
    run_id: str,
    batch_lo: date,
    batch_hi: date,
    dag_id: str | None = None,
    only: list[str] | None = None,
) -> list[LoadStat]:
    """Run the whole staging pipeline in dependency order."""
    steps = STAGING_STEPS
    if only:
        wanted = set(only)
        steps = [s for s in steps if s.name in wanted]
    return [
        run_staging_step(
            conn, step, run_id=run_id, batch_lo=batch_lo, batch_hi=batch_hi, dag_id=dag_id
        )
        for step in steps
    ]
