"""Build the warehouse: staging, dimensions, facts, quality gate, mart.

**Dataset-scheduled**, not cron-scheduled. This DAG runs when BOTH
``raw.cms_master`` and ``raw.sessions`` have been published, which is the real
dependency: facts cannot resolve dimension keys that have not landed. Coupling
it to a clock instead would mean guessing how long the ingests take and being
wrong on the day one of them is slow.

**Task order is correctness, not preference.**

* Staging first, because everything downstream reads conformed data.
* Dimensions before facts, so facts always find their keys.
* Inferred members between them, so a device that reported a session before the
  CMS knew about it still has something to point at.
* The session fact before the interval fact, which inherits its surrogate key.
* The quality gate before the mart, so consumers only ever see a complete,
  checked state.

**The gate's trade-off is deliberate: stale but correct beats fresh but
wrong.** If an error-severity rule fails, the mart tasks do not run and
consumers keep yesterday's correct data. Core stays loaded and inspectable so
the failure can be diagnosed against the actual rows.

**Concurrency** is guarded twice. ``max_active_runs=1`` stops the scheduler
starting two runs; a PostgreSQL advisory lock additionally stops a
manually-triggered run, a backfill or a developer's script interleaving with a
scheduled one. The second guard is five lines and closes a real gap.
"""

from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow.decorators import task
from airflow.exceptions import AirflowFailException
from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup

from dags.common.datasets import RAW_CMS_MASTER, RAW_SESSIONS
from dags.common.default_args import DEFAULT_ARGS, TAGS, TRANSFORM_ARGS
from volthive.pipeline import (
    acquire_build_lock,
    batch_window,
    close_run,
    open_run,
    release_build_lock,
    run_dq_gate,
    stage_all,
    transform_dimensions,
    transform_facts,
    transform_mart,
)

with DAG(
    dag_id="volthive_build_warehouse",
    description="stg -> dimensions -> facts -> data quality gate -> mart",
    default_args=DEFAULT_ARGS,
    # Both datasets, so the build waits for master data AND session data.
    schedule=[RAW_CMS_MASTER, RAW_SESSIONS],
    start_date=pendulum.datetime(2026, 6, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=[*TAGS, "transform"],
    doc_md=__doc__,
    params={
        # Widen the restatement window for a one-off repair without editing
        # code or redeploying a DAG.
        "lookback_days": 3,
        # THE EMERGENCY ESCAPE HATCH. Publishing past a failed quality gate is
        # sometimes the right call at 3 a.m. Doing it invisibly never is, so
        # the bypass is logged at WARNING and written to
        # audit.pipeline_run.error_summary.
        "skip_dq_gate": False,
        "is_backfill": False,
    },
) as dag:
    start = EmptyOperator(task_id="start")

    @task(task_id="acquire_advisory_lock")
    def acquire_advisory_lock() -> bool:
        """Refuse to start if another build already holds the lock.

        Refusing is correct: the other run is presumably doing this work, and
        two concurrent builds would race on the same restatement window.
        """
        if not acquire_build_lock():
            raise AirflowFailException(
                "Another volthive_build_warehouse run holds the advisory lock. "
                "Refusing to start a concurrent build over the same restatement window."
            )
        return True

    @task(task_id="open_pipeline_run")
    def open_pipeline_run(**context) -> str:
        dag_run = context["dag_run"]
        return open_run(
            dag_id="volthive_build_warehouse",
            airflow_run_id=dag_run.run_id,
            data_interval_start=context["data_interval_start"],
            data_interval_end=context["data_interval_end"],
            triggered_by=dag_run.run_type or "dataset",
        )

    with TaskGroup(group_id="staging") as staging_group:

        @task(task_id="rebuild_staging", **TRANSFORM_ARGS)
        def rebuild_staging(pipeline_run_id: str, **context) -> int:
            """Cast, conform, deduplicate, validate and split valid/invalid.

            One task rather than twelve. The steps share a restatement window
            and run in seconds; splitting them would add eleven task overheads
            and eleven places for the window parameter to drift, in exchange
            for a prettier graph. The per-step statistics are still in
            audit.load_stat, which is where anyone actually looks.
            """
            lo, hi = batch_window(
                context["data_interval_start"],
                context["data_interval_end"],
                lookback_days=int(context["params"]["lookback_days"]),
            )
            stats = stage_all(
                run_id=pipeline_run_id, batch_lo=lo, batch_hi=hi, dag_id="volthive_build_warehouse"
            )
            return sum(s.rows_inserted for s in stats)

        staged = rebuild_staging("{{ ti.xcom_pull(task_ids='open_pipeline_run') }}")

    with TaskGroup(group_id="dimensions") as dimension_group:

        @task(task_id="merge_dimensions", **TRANSFORM_ARGS)
        def merge_dimensions(pipeline_run_id: str, **context) -> int:
            """SCD2 merges, Type 1 loads, then inferred members.

            Each dimension merges in its OWN transaction. A crash mid-merge
            would otherwise leave a business key with zero current rows - which
            the partial unique index cannot catch, because it prevents two
            currents rather than none.

            Skipped during a backfill: see run_dimensions() for why rebuilding
            dimension history is a separate, explicit operation.
            """
            lo, hi = batch_window(
                context["data_interval_start"],
                context["data_interval_end"],
                lookback_days=int(context["params"]["lookback_days"]),
            )
            stats = transform_dimensions(
                run_id=pipeline_run_id,
                batch_lo=lo,
                batch_hi=hi,
                dag_id="volthive_build_warehouse",
                is_backfill=bool(context["params"]["is_backfill"]),
            )
            return len(stats)

        merged = merge_dimensions("{{ ti.xcom_pull(task_ids='open_pipeline_run') }}")

    with TaskGroup(group_id="facts") as fact_group:

        @task(task_id="load_facts", **TRANSFORM_ARGS)
        def load_facts(pipeline_run_id: str, **context) -> int:
            """Point-in-time key resolution and delete-insert restatement.

            The three facts load in dependency order inside this task: the
            interval fact needs charging_session_sk from the session fact.
            """
            lo, hi = batch_window(
                context["data_interval_start"],
                context["data_interval_end"],
                lookback_days=int(context["params"]["lookback_days"]),
            )
            stats = transform_facts(
                run_id=pipeline_run_id, batch_lo=lo, batch_hi=hi, dag_id="volthive_build_warehouse"
            )
            return sum(s.rows_inserted for s in stats)

        loaded = load_facts("{{ ti.xcom_pull(task_ids='open_pipeline_run') }}")

    @task(task_id="dq_publish_gate", **TRANSFORM_ARGS)
    def dq_publish_gate(pipeline_run_id: str, **context) -> dict:
        """Run every dataset rule and BLOCK the mart on an error-severity fail.

        Raising is what stops the mart. Downstream tasks use the default
        all_success trigger rule, so a failure here leaves consumers on
        yesterday's correct data rather than giving them today's wrong data.
        """
        lo, hi = batch_window(
            context["data_interval_start"],
            context["data_interval_end"],
            lookback_days=int(context["params"]["lookback_days"]),
        )
        decision = run_dq_gate(
            run_id=pipeline_run_id,
            batch_lo=lo,
            batch_hi=hi,
            skip_gate=bool(context["params"]["skip_dq_gate"]),
        )
        if not decision.passed:
            raise AirflowFailException(decision.message)
        return {
            "checks_run": decision.checks_run,
            "warnings": decision.warned_rules,
        }

    with TaskGroup(group_id="mart") as mart_group:

        @task(task_id="rebuild_mart", **TRANSFORM_ARGS)
        def rebuild_mart(pipeline_run_id: str, **context) -> int:
            """Rebuild the aggregates in full. Trivially idempotent."""
            lo, hi = batch_window(
                context["data_interval_start"],
                context["data_interval_end"],
                lookback_days=int(context["params"]["lookback_days"]),
            )
            stats = transform_mart(
                run_id=pipeline_run_id, batch_lo=lo, batch_hi=hi, dag_id="volthive_build_warehouse"
            )
            return sum(s.rows_inserted for s in stats)

        marts = rebuild_mart("{{ ti.xcom_pull(task_ids='open_pipeline_run') }}")

    @task(task_id="close_pipeline_run", trigger_rule="all_done")
    def close_pipeline_run(pipeline_run_id: str, **context) -> None:
        """Close the run out. Runs whatever happened upstream."""
        task_instance = context["task_instance"]
        failed = [
            t.task_id
            for t in context["dag_run"].get_task_instances()
            if t.state == "failed" and t.task_id != task_instance.task_id
        ]
        close_run(
            run_id=pipeline_run_id,
            dag_id="volthive_build_warehouse",
            status="FAILED" if failed else "SUCCESS",
            error_summary=f"failed tasks: {', '.join(failed)}" if failed else None,
        )

    @task(
        task_id="release_advisory_lock",
        trigger_rule="all_done",
        execution_timeout=timedelta(minutes=5),
    )
    def release_advisory_lock() -> None:
        """Always release, even on failure.

        A lock that outlives a failed run blocks every subsequent one - a
        pipeline that fails once and then never runs again is far worse than
        one that fails once.
        """
        release_build_lock()

    end = EmptyOperator(task_id="end", trigger_rule="all_done")

    locked = acquire_advisory_lock()
    run = open_pipeline_run()
    gate = dq_publish_gate("{{ ti.xcom_pull(task_ids='open_pipeline_run') }}")
    closed = close_pipeline_run("{{ ti.xcom_pull(task_ids='open_pipeline_run') }}")
    unlocked = release_advisory_lock()

    start >> locked >> run
    run >> staging_group >> dimension_group >> fact_group >> gate >> mart_group
    mart_group >> closed >> unlocked >> end
