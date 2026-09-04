"""Ingest one IST business day of session-shaped sources into ``raw``.

Sources: OCPP charge detail records (JSONL), meter telemetry (gzipped CSV) and
the roaming partner API. Schedule 20:15 UTC, which is 01:45 IST the next
morning - fifteen minutes after the master extract, so dimension data is
already landing when the sessions arrive.

CATCHUP IS TRUE here, unlike volthive_ingest_master. Each logical day maps to
specific ``dt=`` partitions on disk, so replaying a missed day means reading
that day's files - which is exactly what a backfill should do. Master data is
watermark-based and needs no replay; the two settings are opposite because the
two load patterns are.

Backfilling this DAG is SAFE by design: every window comes from the Airflow
data interval, the file registry skips what has already been loaded, and
max_active_runs=1 serialises runs so watermarks cannot race.
"""

from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow.decorators import task
from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator
from airflow.sensors.filesystem import FileSensor

from dags.common.datasets import RAW_SESSIONS
from dags.common.default_args import DEFAULT_ARGS, INGESTION_ARGS, TAGS
from volthive.pipeline import close_run, ingest_partner, ingest_sessions_files, open_run

with DAG(
    dag_id="volthive_ingest_sessions",
    description="OCPP charge detail records, meter telemetry and roaming partner data into raw",
    default_args=DEFAULT_ARGS,
    schedule="15 20 * * *",
    start_date=pendulum.datetime(2026, 6, 1, tz="UTC"),
    catchup=True,
    max_active_runs=1,
    tags=[*TAGS, "ingest"],
    doc_md=__doc__,
    params={
        "sources": ["ocpp_cdr", "meter_value"],
        "lookback_days_override": None,
    },
) as dag:
    start = EmptyOperator(task_id="start")

    @task(task_id="open_pipeline_run")
    def open_pipeline_run(**context) -> str:
        dag_run = context["dag_run"]
        return open_run(
            dag_id="volthive_ingest_sessions",
            airflow_run_id=dag_run.run_id,
            data_interval_start=context["data_interval_start"],
            data_interval_end=context["data_interval_end"],
            triggered_by=dag_run.run_type or "schedule",
        )

    # The ONE genuinely-needed sensor in the project.
    #
    # mode='reschedule' frees the worker slot between pokes instead of blocking
    # it for forty-five minutes. Under LocalExecutor with a parallelism of
    # eight, a poke-mode sensor waiting on a late file would hold an eighth of
    # the capacity doing nothing.
    #
    # soft_fail=True marks the sensor SKIPPED rather than FAILED when the file
    # never appears, so downstream ingest tasks skip too. A missing upstream
    # file is an UPSTREAM incident, not a pipeline bug, and the DAG's status
    # should say which it was - a red DAG would send someone looking for a bug
    # in code that is working perfectly.
    wait_for_cdr_partition = FileSensor(
        task_id="wait_for_cdr_partition",
        filepath="{{ var.value.get('volthive_data_dir', '/opt/airflow/data') }}"
        "/landing/cdr/dt={{ data_interval_start | ds }}",
        fs_conn_id="fs_default",
        poke_interval=60,
        timeout=45 * 60,
        mode="reschedule",
        soft_fail=True,
        execution_timeout=timedelta(minutes=50),
    )

    @task(task_id="ingest_session_files", **INGESTION_ARGS)
    def ingest_session_files(run_id: str, **context) -> dict[str, int]:
        """Load unprocessed files in the three-day lookback window.

        The lookback exists because a ``dt=`` partition is occasionally
        REWRITTEN with corrections a day or two later. Every file is hashed and
        checked against ctl.ingested_file, so an unchanged re-delivery costs one
        hash and a rewritten file is reprocessed automatically.
        """
        return ingest_sessions_files(
            run_id=run_id,
            data_interval_start=context["data_interval_start"],
            data_interval_end=context["data_interval_end"],
            sources=context["params"].get("sources"),
            dag_id="volthive_ingest_sessions",
        )

    @task(
        task_id="ingest_partner_cdrs",
        **{**INGESTION_ARGS, "execution_timeout": timedelta(minutes=10)},
    )
    def ingest_partner_cdrs_task(run_id: str, **context) -> int:
        """Page through the roaming partner's cursor window.

        DELIBERATELY NOT DOWNSTREAM OF THE SENSOR. A missing OCPP file must not
        block an unrelated source - roaming revenue is real revenue, and it
        arrives over an API that knows nothing about VoltHive's file drops.
        """
        return ingest_partner(
            run_id=run_id,
            data_interval_start=context["data_interval_start"],
            data_interval_end=context["data_interval_end"],
            dag_id="volthive_ingest_sessions",
        )

    @task(task_id="publish_raw_sessions", outlets=[RAW_SESSIONS], trigger_rule="none_failed")
    def publish_raw_sessions(file_counts: dict[str, int] | None, partner_rows: int) -> dict:
        """Publish the dataset that lets the warehouse build trigger.

        trigger_rule='none_failed' rather than 'all_success': if the sensor
        soft-failed and the file ingest was SKIPPED, the partner data still
        arrived and the warehouse should still build with what exists. A
        genuine FAILURE upstream still blocks it.
        """
        return {"files": file_counts or {}, "partner_rows": partner_rows}

    @task(task_id="close_pipeline_run", trigger_rule="all_done")
    def close_pipeline_run(run_id: str, **context) -> None:
        task_instance = context["task_instance"]
        failed = [
            t.task_id
            for t in context["dag_run"].get_task_instances()
            if t.state == "failed" and t.task_id != task_instance.task_id
        ]
        close_run(
            run_id=run_id,
            dag_id="volthive_ingest_sessions",
            status="FAILED" if failed else "SUCCESS",
            error_summary=f"failed tasks: {', '.join(failed)}" if failed else None,
        )

    end = EmptyOperator(task_id="end", trigger_rule="all_done")

    run = open_pipeline_run()
    files = ingest_session_files(run)
    partner = ingest_partner_cdrs_task(run)
    published = publish_raw_sessions(files, partner)
    closed = close_pipeline_run(run)

    start >> run
    run >> wait_for_cdr_partition >> files
    run >> partner
    published >> closed >> end
