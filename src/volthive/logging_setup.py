"""Structured logging for the VoltHive warehouse platform.

Airflow's own logs answer *"did the task run?"*. These logs answer the
questions you are actually asked when a dashboard looks wrong: which source,
which batch, how many rows, how long, and - critically - **which pipeline run
produced this row in the warehouse?**

Three deliberate choices:

**1. JSON to stdout.** Airflow captures a task's stdout, so the same line is
readable in the Airflow UI *and* parseable by any log tool. No file handlers,
no log shipper, no extra container. Set ``VOLTHIVE_LOG_JSON=false`` for
human-readable console output while developing.

**2. A run ID bound to the context, not passed around.** ``structlog``
contextvars mean every log line emitted anywhere inside a task automatically
carries ``pipeline_run_id``, ``dag_id`` and ``task_id`` without threading them
through every function signature. The same ``pipeline_run_id`` is written to
``audit.pipeline_run`` and stamped on every warehouse row as ``dw_run_id``, so
a single fact row traces back to the run, the task, the source file and the
git commit that produced it.

**3. PII redaction as a processor, not as caller discipline.** Redaction runs
inside the logging pipeline, so a careless ``log.info("row", **record)`` cannot
leak a customer's email. Relying on every call site to remember is how leaks
happen.

Deliberately NOT built: log shipping, a metrics exporter, sampling, log
rotation. Those need a consumer, and this project's consumers are the Airflow
UI and the ``audit`` tables.
"""

from __future__ import annotations

import logging
import sys
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

import structlog
from structlog.typing import EventDict, WrappedLogger

__all__ = [
    "configure_logging",
    "get_logger",
    "new_pipeline_run_id",
    "bind_run_context",
    "clear_run_context",
    "run_context",
    "REDACTED",
    "REDACT_KEYS",
]

#: Replacement value written in place of a sensitive field.
REDACTED = "***REDACTED***"

#: Field names whose values never appear in a log line.
#:
#: Matching is by EXACT key, never by substring. That is not an optimisation -
#: it is correctness: ``email_domain`` is deliberately warehoused (see
#: ``core.dim_customer``) and a substring rule would redact it, destroying a
#: legitimate analytical attribute while giving a false sense of safety
#: elsewhere. Add new keys here rather than loosening the rule.
REDACT_KEYS: frozenset[str] = frozenset(
    {
        # Customer PII
        "email",
        "email_address",
        "phone",
        "phone_number",
        "full_name",
        "customer_name",
        "address",
        "address_line",
        # Authentication identifiers - an RFID id_tag identifies a person
        "id_tag",
        "auth_token",
        # Credentials
        "password",
        "passwd",
        "secret",
        "token",
        "api_key",
        "apikey",
        "authorization",
        "dsn",
        "connection_string",
    }
)

#: How deep to walk nested structures when redacting. Raw source payloads are
#: shallow; the limit exists so a pathological or cyclic structure cannot turn
#: a log call into a hang.
_MAX_REDACT_DEPTH = 4

_configured = False


def _redact_value(value: Any, depth: int) -> Any:
    """Recursively redact sensitive keys inside nested containers."""
    if depth > _MAX_REDACT_DEPTH:
        return value
    if isinstance(value, dict):
        return {
            key: (REDACTED if key.lower() in REDACT_KEYS else _redact_value(item, depth + 1))
            for key, item in value.items()
        }
    if isinstance(value, list | tuple):
        redacted = [_redact_value(item, depth + 1) for item in value]
        return tuple(redacted) if isinstance(value, tuple) else redacted
    return value


def redact_pii(
    _logger: WrappedLogger,
    _method_name: str,
    event_dict: EventDict,
) -> EventDict:
    """structlog processor: replace sensitive values with :data:`REDACTED`.

    Runs late in the processor chain so that it also covers keys bound to the
    context (via :func:`bind_run_context`) and keys added by earlier
    processors - not just the ones passed to this particular log call.

    Scope, stated honestly: this protects *structured fields*, which is the
    boundary the platform controls. A caller who interpolates an email address
    into the free-text event message defeats it. That is why pipeline code
    logs ``log.info("row_rejected", email=...)`` and never
    ``log.info(f"rejected {email}")``.
    """
    return {
        key: (REDACTED if key.lower() in REDACT_KEYS else _redact_value(value, 1))
        for key, value in event_dict.items()
    }


def configure_logging(
    *,
    level: str = "INFO",
    json_logs: bool = True,
    force: bool = False,
) -> None:
    """Configure structlog once per process.

    Args:
        level: One of DEBUG/INFO/WARNING/ERROR/CRITICAL.
        json_logs: JSON when True (containers, CI), coloured console when
            False (interactive development).
        force: Reconfigure even if already configured. Used by tests.
    """
    # One module-level flag guards repeated configuration in a single process.
    global _configured
    if _configured and not force:
        return

    numeric_level = logging.getLevelName(level.upper())
    if not isinstance(numeric_level, int):
        numeric_level = logging.INFO

    # Keep the stdlib root logger in step so noisy third-party libraries
    # respect the same level.
    logging.basicConfig(format="%(message)s", stream=sys.stdout, level=numeric_level, force=True)

    renderer: Any = (
        structlog.processors.JSONRenderer()
        if json_logs
        else structlog.dev.ConsoleRenderer(colors=False)
    )

    structlog.configure(
        processors=[
            # 1. Pull in run_id / dag_id / task_id bound for this task.
            structlog.contextvars.merge_contextvars,
            # 2. Standard fields.
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso", utc=True, key="timestamp"),
            # 3. Exception rendering, before redaction so tracebacks are covered.
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.UnicodeDecoder(),
            # 4. Redaction last, so nothing added above can slip past it.
            redact_pii,
            # 5. Output.
            renderer,
        ],
        wrapper_class=structlog.make_filtering_bound_logger(numeric_level),
        logger_factory=structlog.PrintLoggerFactory(file=sys.stdout),
        cache_logger_on_first_use=True,
    )
    _configured = True


def get_logger(name: str | None = None) -> Any:
    """Return a bound structlog logger, configuring logging on first use."""
    if not _configured:
        configure_logging()
    return structlog.get_logger(name) if name else structlog.get_logger()


def new_pipeline_run_id() -> str:
    """Generate a correlation ID for one pipeline run.

    UUID4 rather than a timestamp or a counter: it is unique without
    coordination, safe when a backfill runs several logical days, and stable
    to store as a ``uuid`` column in ``audit.pipeline_run``.
    """
    return str(uuid.uuid4())


def bind_run_context(**kwargs: Any) -> None:
    """Bind key/values onto every subsequent log line in this context.

    Typically called once per Airflow task with ``pipeline_run_id``,
    ``dag_id``, ``task_id`` and ``dw_batch_key``.
    """
    structlog.contextvars.bind_contextvars(**kwargs)


def clear_run_context() -> None:
    """Remove all bound context values."""
    structlog.contextvars.clear_contextvars()


@contextmanager
def run_context(**kwargs: Any) -> Iterator[None]:
    """Scoped version of :func:`bind_run_context`.

    Guarantees the context is cleared even if the body raises, so a failed
    task cannot leak its run ID into whatever executes next in the same
    worker process.
    """
    bind_run_context(**kwargs)
    try:
        yield
    finally:
        clear_run_context()
