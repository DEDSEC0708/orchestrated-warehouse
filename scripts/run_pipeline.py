#!/usr/bin/env python
"""Run the whole pipeline for a date range, without Airflow.

    python scripts/run_pipeline.py --from 2026-06-01 --to 2026-06-07
    python scripts/run_pipeline.py --from 2026-06-01 --to 2026-06-07 --stages stage,core

Airflow orchestrates this in production. This script exists for three things
that Airflow is a poor fit for:

* the END-TO-END TESTS, which need the pipeline to run in seconds rather than
  waiting on a scheduler;
* the INITIAL BOOTSTRAP, where iterating month by month in one process is far
  simpler than triggering hundreds of DAG runs;
* DEBUGGING, where a stack trace in the terminal beats hunting through task
  logs in a browser.

It calls exactly the same functions the DAG tasks call - ``volthive.pipeline``
- so it cannot drift from what Airflow actually runs. That is the point of
keeping the DAG files thin.
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "src"))

from volthive.logging_setup import configure_logging, get_logger
from volthive.pipeline import (
    close_run,
    ingest_all_cms,
    ingest_partner,
    ingest_seed,
    ingest_sessions_files,
    open_run,
    run_dq_gate,
    stage_all,
    transform_dimensions,
    transform_facts,
    transform_mart,
)

log = get_logger("run_pipeline")

ALL_STAGES = ["ingest", "stage", "core", "dq", "mart"]


def run(
    *,
    date_from: date,
    date_to: date,
    stages: list[str],
    lookback_days: int,
    skip_dq_gate: bool,
    is_backfill: bool,
) -> dict:
    """Execute the selected stages over one window and return a summary."""
    interval_start = datetime.combine(date_from, time.min, tzinfo=UTC)
    interval_end = datetime.combine(date_to + timedelta(days=1), time.min, tzinfo=UTC)
    batch_lo = date_from - timedelta(days=lookback_days)
    batch_hi = date_to

    run_id = open_run(
        dag_id="volthive_run_pipeline",
        airflow_run_id=f"manual__{datetime.now(tz=UTC).isoformat()}",
        data_interval_start=interval_start,
        data_interval_end=interval_end,
        triggered_by="manual",
    )
    summary: dict = {"run_id": run_id, "window": f"{batch_lo} .. {batch_hi}"}
    status = "SUCCESS"
    error: str | None = None

    try:
        if "ingest" in stages:
            summary["cms"] = ingest_all_cms(
                run_id=run_id,
                data_interval_start=interval_start,
                data_interval_end=interval_end,
                dag_id="volthive_run_pipeline",
            )
            summary["files"] = ingest_sessions_files(
                run_id=run_id,
                data_interval_start=interval_start,
                data_interval_end=interval_end,
                dag_id="volthive_run_pipeline",
            )
            summary["partner_rows"] = ingest_partner(
                run_id=run_id,
                data_interval_start=interval_start,
                data_interval_end=interval_end,
                dag_id="volthive_run_pipeline",
            )
            summary["seed_rows"] = ingest_seed(run_id=run_id, dag_id="volthive_run_pipeline")

        if "stage" in stages:
            stats = stage_all(
                run_id=run_id,
                batch_lo=batch_lo,
                batch_hi=batch_hi,
                dag_id="volthive_run_pipeline",
            )
            summary["staged"] = {s.task_id: s.rows_inserted for s in stats}
            summary["quarantined"] = sum(s.rows_quarantined for s in stats)

        if "core" in stages:
            transform_dimensions(
                run_id=run_id,
                batch_lo=batch_lo,
                batch_hi=batch_hi,
                dag_id="volthive_run_pipeline",
                is_backfill=is_backfill,
            )
            fact_stats = transform_facts(
                run_id=run_id,
                batch_lo=batch_lo,
                batch_hi=batch_hi,
                dag_id="volthive_run_pipeline",
            )
            summary["facts"] = {s.task_id: s.rows_inserted for s in fact_stats}

        gate_passed = True
        if "dq" in stages:
            decision = run_dq_gate(
                run_id=run_id, batch_lo=batch_lo, batch_hi=batch_hi, skip_gate=skip_dq_gate
            )
            gate_passed = decision.passed
            summary["dq"] = {
                "passed": decision.passed,
                "checks_run": decision.checks_run,
                "blocking": decision.blocking_rules,
                "warnings": decision.warned_rules,
            }

        # The gate is what stops the mart. Publishing anyway would defeat the
        # entire point of having one: consumers keep yesterday's correct data
        # rather than receiving today's wrong data.
        if "mart" in stages and gate_passed:
            mart_stats = transform_mart(
                run_id=run_id,
                batch_lo=batch_lo,
                batch_hi=batch_hi,
                dag_id="volthive_run_pipeline",
            )
            summary["mart"] = {s.task_id: s.rows_inserted for s in mart_stats}
        elif "mart" in stages:
            summary["mart"] = "SKIPPED - the data quality gate blocked the publish"
            status = "FAILED"
            error = "DQ gate blocked the mart publish"
    except Exception as exc:
        status, error = "FAILED", str(exc)
        raise
    finally:
        close_run(run_id=run_id, dag_id="volthive_run_pipeline", status=status, error_summary=error)

    summary["status"] = status
    return summary


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--from", dest="date_from", required=True, help="YYYY-MM-DD")
    parser.add_argument("--to", dest="date_to", required=True, help="YYYY-MM-DD")
    parser.add_argument(
        "--stages",
        default=",".join(ALL_STAGES),
        help=f"comma-separated subset of {ALL_STAGES}",
    )
    parser.add_argument("--lookback-days", type=int, default=3)
    parser.add_argument(
        "--skip-dq-gate",
        action="store_true",
        help="publish the mart even if error-severity rules failed. Logged loudly.",
    )
    parser.add_argument(
        "--backfill",
        action="store_true",
        help="skip the SCD2 merges - facts re-resolve against existing dimension history",
    )
    parser.add_argument("--json", action="store_true", help="print the summary as JSON only")
    args = parser.parse_args()

    configure_logging(json_logs=args.json)
    stages = [s.strip() for s in args.stages.split(",") if s.strip()]
    unknown = [s for s in stages if s not in ALL_STAGES]
    if unknown:
        parser.error(f"unknown stage(s) {unknown}; choose from {ALL_STAGES}")

    summary = run(
        date_from=date.fromisoformat(args.date_from),
        date_to=date.fromisoformat(args.date_to),
        stages=stages,
        lookback_days=args.lookback_days,
        skip_dq_gate=args.skip_dq_gate,
        is_backfill=args.backfill,
    )
    print(json.dumps(summary, indent=2, sort_keys=True, default=str))
    return 0 if summary["status"] == "SUCCESS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
