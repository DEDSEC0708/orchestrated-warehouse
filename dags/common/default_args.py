"""One source of truth for retry, timeout and callback policy.

**Retry counts differ by task TYPE, and the reason is the whole point.**

Ingestion failures are usually TRANSIENT - a connection reset, an HTTP 503, a
database that is restarting. The input was fine; the world was briefly
unavailable. Retrying with backoff genuinely fixes those, so ingestion gets
three attempts.

Transform failures are usually DETERMINISTIC - bad SQL, a constraint violation,
a null where none was expected. Retrying a deterministic failure produces the
same failure three times and delays the real error message by six minutes while
holding a worker slot. So transforms get one retry: enough to survive a
genuinely transient database blip, not enough to waste anyone's evening.

Being able to explain why two task types in the same DAG have different retry
counts is a better answer than any particular number.

**Every task has an execution_timeout**, without exception. A hung connection
with no timeout occupies a worker slot until someone notices, and under
LocalExecutor with a parallelism of eight, three hung tasks is most of the
capacity gone.

**SLAs are deliberately unused.** Airflow 2.x SLA semantics are confusing (they
are measured from the DAG run's start, not the task's, and the implementation
is being replaced), so freshness is enforced by a data-quality rule instead -
which is more honest, more portable, and reports in the same place as every
other quality signal.

A DAG integrity test enforces this policy: every task must have at least one
retry, a non-null execution_timeout, and a failure callback. Policy as a TEST
rather than policy as documentation is what makes it still true in six months.
"""

from __future__ import annotations

from datetime import timedelta
from typing import Any

from dags.common.callbacks import record_task_failure, record_task_retry

__all__ = ["DEFAULT_ARGS", "INGESTION_ARGS", "TRANSFORM_ARGS", "OWNER", "TAGS"]

OWNER = "data-eng"

TAGS = ["volthive", "ev-charging", "batch"]

#: Applied to every task in every DAG.
DEFAULT_ARGS: dict[str, Any] = {
    "owner": OWNER,
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    "execution_timeout": timedelta(minutes=30),
    # No SMTP dependency anywhere in this project. Alerting goes through the
    # callback, which writes an audit row and optionally POSTs a webhook.
    "email_on_failure": False,
    "email_on_retry": False,
    "on_failure_callback": record_task_failure,
    "on_retry_callback": record_task_retry,
}

#: Overrides for tasks that talk to a source system.
INGESTION_ARGS: dict[str, Any] = {
    **DEFAULT_ARGS,
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=20),
    "execution_timeout": timedelta(minutes=20),
}

#: Overrides for set-based transformations inside the warehouse.
TRANSFORM_ARGS: dict[str, Any] = {
    **DEFAULT_ARGS,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    "execution_timeout": timedelta(minutes=45),
}
