"""Extract CMS master data and the static grid-tariff seed into ``raw``.

Schedule: 20:00 UTC daily, which is 01:30 IST the following morning. The DAG
runs in UTC and the business day is IST; the offset is written down here rather
than being folded into the cron expression and forgotten.

CATCHUP IS FALSE, and the contrast with volthive_ingest_sessions is the
interesting part. Master data is extracted BY WATERMARK, so a missed day is
automatically covered by the next run's predicate - the window simply widens.
Replaying thirty dated runs would do the same work thirty times and end in
exactly the same place.

Session ingest is PARTITION-oriented: each logical day maps to specific files,
so catchup is exactly right there. Two DAGs in one project with opposite
catchup settings, each correct for its own reason.
"""

from __future__ import annotations

import pendulum
from airflow.decorators import task
from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator

from dags.common.datasets import RAW_CMS_MASTER
from dags.common.default_args import DEFAULT_ARGS, INGESTION_ARGS, TAGS
from volthive.pipeline import close_run, ingest_all_cms, ingest_seed, open_run

with DAG(
    dag_id="volthive_ingest_master",
    description="CMS master data and the static grid-tariff seed into raw",
    default_args=DEFAULT_ARGS,
    schedule="0 20 * * *",
    start_date=pendulum.datetime(2026, 6, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=[*TAGS, "ingest"],
    doc_md=__doc__,
) as dag:
    start = EmptyOperator(task_id="start")

    @task(task_id="open_pipeline_run")
    def open_pipeline_run(**context) -> str:
        """Generate the correlation ID stamped on every row this run writes."""
        dag_run = context["dag_run"]
        return open_run(
            dag_id="volthive_ingest_master",
            airflow_run_id=dag_run.run_id,
            data_interval_start=context["data_interval_start"],
            data_interval_end=context["data_interval_end"],
            triggered_by=dag_run.run_type or "schedule",
        )

    @task(task_id="ingest_cms_entities", **INGESTION_ARGS)
    def ingest_cms_entities(pipeline_run_id: str, **context) -> dict[str, int]:
        """Extract all five CMS entities on a bounded updated_at window.

        The upper bound is the DATA INTERVAL END, never now(). With now(), the
        same logical run would extract a different set depending on when it
        executed, so a rerun would not be reproducible and a backfill would
        pull current data into a historical partition.
        """
        return ingest_all_cms(
            run_id=pipeline_run_id,
            data_interval_start=context["data_interval_start"],
            data_interval_end=context["data_interval_end"],
            dag_id="volthive_ingest_master",
        )

    @task(task_id="ingest_grid_tariff_seed", **INGESTION_ARGS)
    def ingest_grid_tariff_seed(pipeline_run_id: str) -> int:
        """Reload source S5 in full - sixty rows, truncate and replace.

        Deliberately NOT incremental. Slowly-changing reference data this small
        is cheaper and safer to reload wholesale, and having one source that is
        not incremental is what makes "incremental everywhere" visibly a choice
        rather than a reflex.
        """
        return ingest_seed(run_id=pipeline_run_id, dag_id="volthive_ingest_master")

    @task(task_id="publish_raw_cms_master", outlets=[RAW_CMS_MASTER])
    def publish_raw_cms_master(cms_counts: dict[str, int], seed_rows: int) -> dict[str, int]:
        """Publish the dataset that lets the warehouse build trigger.

        Publishing is a SEPARATE TASK downstream of both extracts, so the
        signal means "master data is complete", not "one extract finished".
        """
        return {**cms_counts, "grid_tariff_slab": seed_rows}

    @task(task_id="close_pipeline_run", trigger_rule="all_done")
    def close_pipeline_run(pipeline_run_id: str, **context) -> None:
        """Close the run, whatever happened.

        trigger_rule='all_done' so a failed run is still closed out. A run left
        RUNNING for ever blocks the stale-run sweep and misreports the health
        view.
        """
        task_instance = context["task_instance"]
        failed = [
            t.task_id
            for t in context["dag_run"].get_task_instances()
            if t.state == "failed" and t.task_id != task_instance.task_id
        ]
        close_run(
            run_id=pipeline_run_id,
            dag_id="volthive_ingest_master",
            status="FAILED" if failed else "SUCCESS",
            error_summary=f"failed tasks: {', '.join(failed)}" if failed else None,
        )

    end = EmptyOperator(task_id="end", trigger_rule="all_done")

    run = open_pipeline_run()
    cms = ingest_cms_entities(run)
    seed = ingest_grid_tariff_seed(run)
    published = publish_raw_cms_master(cms, seed)
    closed = close_pipeline_run(run)

    start >> run
    published >> closed >> end
