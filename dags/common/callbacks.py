"""Failure and retry callbacks.

Both write a row to ``audit.task_run``, so a retry that eventually succeeded is
still VISIBLE. Airflow shows the final green tick; the audit table remembers
that it took three attempts, which is the difference between "the pipeline is
fine" and "the pipeline is fine but something upstream is flapping".

Alerting is deliberately minimal: a structured ERROR log, an audit row, and -
only if ALERT_WEBHOOK_URL is set - one compact JSON POST. Empty by default, so
the project has no external dependency to run.

No Prometheus, no Grafana, no Slack integration. A monitoring stack for a
laptop pipeline is decoration: it would double the container count to
visualise four metrics that a SQL view already answers. In a team environment
these logs would ship to a central store and the audit views would sit behind a
dashboard - but adding that here would be complexity without a consumer.
"""

from __future__ import annotations

import json
from typing import Any

from volthive.config.settings import get_settings
from volthive.logging_setup import get_logger
from volthive.pipeline import record_failure

log = get_logger("volthive.callbacks")

__all__ = ["record_task_failure", "record_task_retry"]


def _context_fields(context: dict[str, Any]) -> dict[str, Any]:
    """Pull the identifying fields out of an Airflow task context."""
    task_instance = context.get("task_instance")
    dag_run = context.get("dag_run")
    exception = context.get("exception")
    return {
        "dag_id": getattr(task_instance, "dag_id", "unknown"),
        "task_id": getattr(task_instance, "task_id", "unknown"),
        "airflow_run_id": getattr(dag_run, "run_id", None),
        "try_number": getattr(task_instance, "try_number", 1),
        "error_type": type(exception).__name__ if exception else None,
        "error_message": str(exception) if exception else None,
    }


def _post_webhook(payload: dict[str, Any]) -> None:
    """Best-effort alert POST. Never raises.

    An alerting failure must not fail the task that was already failing - that
    would replace a comprehensible error with a confusing one.
    """
    settings = get_settings()
    if not settings.alert_webhook_url:
        return
    try:
        import httpx

        httpx.post(settings.alert_webhook_url, json=payload, timeout=5.0)
    except Exception as exc:
        log.warning("alert_webhook_failed", error=str(exc))


def record_task_failure(context: dict[str, Any]) -> None:
    """on_failure_callback: audit row, structured ERROR, optional webhook."""
    fields = _context_fields(context)
    run_id = None
    task_instance = context.get("task_instance")
    if task_instance is not None:
        try:
            run_id = task_instance.xcom_pull(task_ids="open_pipeline_run", key="return_value")
        except Exception:  # - a missing XCom must not break the callback
            run_id = None

    log.error("task_failed", **fields)
    record_failure(run_id=run_id, status="FAILED", **fields)
    _post_webhook({"event": "task_failed", **{k: str(v) for k, v in fields.items()}})


def record_task_retry(context: dict[str, Any]) -> None:
    """on_retry_callback: record the attempt so retries are not invisible."""
    fields = _context_fields(context)
    log.warning("task_retrying", **fields)
    record_failure(run_id=None, status="RETRY", **fields)


_ = json  # imported for the webhook payload shape documented above
