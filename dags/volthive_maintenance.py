"""Weekly housekeeping: partitions, retention, statistics, reconciliation.

Schedule: 21:00 UTC on Sunday, which is 02:30 IST on Monday. Off-peak for a
business whose evening peak is IST, and after the nightly build has finished.

The five jobs here are the ones that are invisible until they are not done:

**Partition pre-creation.** A missing partition is a classic production
incident - everything works until midnight on the 1st, when the first insert of
the new month fails. Creating three months ahead means a missed maintenance run
costs nothing.

**Raw retention.** The raw layer is evidence, and evidence has a retention
period. Pruning past 180 days keeps the replay window honest rather than
letting it grow until someone notices the disk.

**VACUUM ANALYZE.** Delete-insert restatement generates dead tuples every
single day, and the planner making a bad choice on stale statistics right after
a bulk load is a real and commonly-missed problem.

**Hard-delete reconciliation.** An incremental ``updated_at`` extract CANNOT
SEE a row that was deleted outright - there is no update to notice. This is a
genuine limitation of watermark-based change capture, not an oversight, and the
weekly key comparison is the mitigation. A production system would use logical
replication or a CDC tool instead.

**Stale-run sweep.** A hard crash leaves audit.pipeline_run stuck at RUNNING,
which misreports the health view for ever. Closing anything older than a day is
a two-line fix for a permanent lie.
"""

from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow.decorators import task
from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator

from dags.common.default_args import DEFAULT_ARGS, TAGS
from volthive.db.connection import fetch_all, fetch_value, transaction, warehouse_connection
from volthive.logging_setup import get_logger

log = get_logger("volthive.maintenance")

#: Raw is the replay tape, and 180 days is how far back a replay is supported.
#: Stated as a constant rather than buried in SQL so the policy is one edit.
RAW_RETENTION_DAYS = 180

#: Quality check results and quarantine rows are the quality TREND, which is
#: itself data. Kept four times longer than raw.
DQ_RETENTION_DAYS = 365

with DAG(
    dag_id="volthive_maintenance",
    description="Partition pre-creation, retention, VACUUM, key reconciliation, stale-run sweep",
    default_args={**DEFAULT_ARGS, "execution_timeout": timedelta(minutes=60)},
    schedule="0 21 * * 0",
    start_date=pendulum.datetime(2026, 6, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=[*TAGS, "maintenance"],
    doc_md=__doc__,
    params={"retention_days": RAW_RETENTION_DAYS, "dry_run": False},
) as dag:
    start = EmptyOperator(task_id="start")

    @task(task_id="create_future_partitions")
    def create_future_partitions() -> int:
        """Pre-create monthly partitions three months ahead.

        Three months rather than one, so a missed maintenance run - a holiday
        week, a broken scheduler - cannot turn into a failed insert at
        midnight on the 1st.
        """
        with warehouse_connection(application_name="maintenance.partitions") as conn:
            with transaction(conn):
                created = fetch_value(
                    conn,
                    """
                    SELECT core.ensure_meter_interval_partitions(
                        CURRENT_DATE, (CURRENT_DATE + INTERVAL '3 months')::DATE
                    )
                    """,
                )
        log.info("partitions_ensured", created=created)
        return int(created or 0)

    @task(task_id="prune_raw_retention")
    def prune_raw_retention(**context) -> dict[str, int]:
        """Delete raw rows older than the retention window.

        Deletes are per table and per batch key, which the (dw_batch_key) index
        makes cheap. The dry_run parameter reports what WOULD be deleted -
        because the first time anyone runs a retention job in anger, they want
        to see the number before they see the deletion.
        """
        days = int(context["params"]["retention_days"])
        dry_run = bool(context["params"]["dry_run"])
        tables = [
            "raw.cms_customers",
            "raw.cms_vehicles",
            "raw.cms_stations",
            "raw.cms_charge_points",
            "raw.cms_tariff_plans",
            "raw.ocpp_cdr",
            "raw.meter_value",
            "raw.partner_cdr",
        ]
        removed: dict[str, int] = {}
        with warehouse_connection(application_name="maintenance.retention") as conn:
            for table in tables:
                with transaction(conn), conn.cursor() as cur:
                    verb = "SELECT count(*) FROM" if dry_run else "DELETE FROM"
                    cur.execute(  # - table names come from the list above
                        f"{verb} {table} WHERE dw_batch_key < CURRENT_DATE - %s::INT",
                        (days,),
                    )
                    removed[table] = (
                        int((cur.fetchone() or [0])[0]) if dry_run else (cur.rowcount or 0)
                    )
        log.info("raw_retention_pruned", dry_run=dry_run, days=days, removed=removed)
        return removed

    @task(task_id="vacuum_analyze")
    def vacuum_analyze() -> list[str]:
        """VACUUM ANALYZE the tables that churn.

        Delete-insert restatement produces dead tuples daily, and the planner
        works from statistics that a bulk load leaves stale. Runs with
        autocommit because VACUUM cannot run inside a transaction block - which
        is exactly the kind of detail that turns a maintenance DAG red on its
        first Sunday.
        """
        tables = [
            "core.fact_charging_session",
            "core.fact_meter_interval",
            "core.fact_station_daily_utilization",
            "core.dim_customer",
            "core.dim_charge_point",
            "raw.ocpp_cdr",
            "raw.meter_value",
        ]
        with warehouse_connection(application_name="maintenance.vacuum") as conn:
            conn.autocommit = True
            for table in tables:
                with conn.cursor() as cur:
                    cur.execute(f"VACUUM (ANALYZE) {table}")  # - fixed list
        log.info("vacuum_analyze_completed", tables=tables)
        return tables

    @task(task_id="reconcile_source_keys")
    def reconcile_source_keys() -> dict[str, int]:
        """Detect entities present in the warehouse but gone from the source.

        THE HONEST LIMITATION, and the reason this task exists. An incremental
        extract watches ``updated_at``; a HARD DELETE produces no update, so the
        extract cannot see it and the dimension keeps a member that no longer
        exists.

        The mitigation is a weekly comparison of KEYS ONLY - cheap, because it
        moves no attributes - reporting anything the warehouse still believes
        in that the source has forgotten. It reports rather than deletes:
        automatically expiring a dimension member because one weekly query did
        not return it is exactly how a network blip becomes data loss.

        A production system would use logical replication or a CDC tool and
        would not need this at all. Saying so is better than pretending the
        problem does not exist.
        """
        from volthive.db.connection import cms_connection

        findings: dict[str, int] = {}
        pairs = [
            ("customers", "customer_id", "core.dim_customer"),
            ("stations", "station_id", "core.dim_station"),
            ("charge_points", "charge_point_id", "core.dim_charge_point"),
            ("tariff_plans", "tariff_plan_id", "core.dim_tariff_plan"),
        ]
        with (
            cms_connection(application_name="maintenance.reconcile") as source_conn,
            warehouse_connection(application_name="maintenance.reconcile") as wh_conn,
        ):
            for table, key, dimension in pairs:
                source_keys = {
                    row[key]
                    for row in fetch_all(source_conn, f"SELECT {key} FROM {table}")  # noqa: S608
                }
                warehouse_keys = {
                    row[key]
                    for row in fetch_all(
                        wh_conn,
                        f"SELECT {key} FROM {dimension} "  # noqa: S608
                        f"WHERE is_current AND NOT is_inferred",
                    )
                    if not str(row[key]).startswith(("UNKNOWN", "NOT_APPLICABLE"))
                }
                orphaned = warehouse_keys - source_keys
                findings[dimension] = len(orphaned)
                if orphaned:
                    log.warning(
                        "dim_orphaned_keys",
                        dimension=dimension,
                        count=len(orphaned),
                        sample=sorted(orphaned)[:10],
                        note=(
                            "present in the warehouse, absent from the source. "
                            "Watermark-based extracts cannot see hard deletes; "
                            "review before expiring."
                        ),
                    )
        return findings

    @task(task_id="sweep_stale_runs")
    def sweep_stale_runs() -> int:
        """Close pipeline runs left RUNNING by a hard crash.

        A run stuck at RUNNING misreports the health view for ever and, because
        it can never be closed by the process that opened it, will never fix
        itself. Anything still open after a day was not merely slow.
        """
        with warehouse_connection(application_name="maintenance.sweep") as conn:
            with transaction(conn), conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE audit.pipeline_run
                    SET status = 'FAILED',
                        ended_at_utc = now(),
                        error_summary = COALESCE(error_summary, '')
                            || ' [closed by the maintenance stale-run sweep: '
                            || 'left RUNNING for over 24 hours]'
                    WHERE status = 'RUNNING'
                      AND started_at_utc < now() - INTERVAL '1 day'
                    """
                )
                swept = cur.rowcount or 0
        if swept:
            log.warning("stale_runs_swept", count=swept)
        return swept

    @task(task_id="prune_dq_history")
    def prune_dq_history() -> dict[str, int]:
        """Prune old check results and RESOLVED quarantine rows.

        Only rows already triaged to REQUEUED or WONTFIX are removed. An
        untriaged quarantine row is unfinished work, and deleting unfinished
        work on a schedule is how a quality process quietly stops being one.
        """
        removed: dict[str, int] = {}
        with warehouse_connection(application_name="maintenance.dq_retention") as conn:
            with transaction(conn), conn.cursor() as cur:
                cur.execute(
                    "DELETE FROM dq.check_result WHERE checked_at_utc < now() - %s::INT * INTERVAL '1 day'",
                    (DQ_RETENTION_DAYS,),
                )
                removed["dq.check_result"] = cur.rowcount or 0
                for table in (
                    "dq.quarantine_ocpp_cdr",
                    "dq.quarantine_meter_value",
                    "dq.quarantine_partner_cdr",
                    "dq.quarantine_cms_entity",
                ):
                    cur.execute(
                        f"DELETE FROM {table} "  # noqa: S608 - fixed list
                        "WHERE quarantined_at_utc < now() - %s::INT * INTERVAL '1 day' "
                        "AND status IN ('REQUEUED', 'WONTFIX')",
                        (DQ_RETENTION_DAYS,),
                    )
                    removed[table] = cur.rowcount or 0
        log.info("dq_history_pruned", removed=removed)
        return removed

    end = EmptyOperator(task_id="end", trigger_rule="all_done")

    (
        start
        >> create_future_partitions()
        >> prune_raw_retention()
        >> prune_dq_history()
        >> vacuum_analyze()
        >> reconcile_source_keys()
        >> sweep_stale_runs()
        >> end
    )
