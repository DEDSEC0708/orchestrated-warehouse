"""Exception taxonomy for the VoltHive warehouse platform.

The point of this module is not to have custom exceptions for their own sake.
It is to make ONE decision explicit and testable: **what should happen when
this goes wrong?**

Data pipelines fail in three fundamentally different ways, and treating them
identically is the most common reliability mistake in a batch platform:

===========================================================================
 Category         Example                        Correct response
===========================================================================
 TransientError   Connection reset, HTTP 503,    RETRY with backoff. The
                  database restarting, a         input is fine; the world
                  timeout on the partner API.    was briefly unavailable.

 DataError        A CDR with meter_stop <        QUARANTINE the record and
                  meter_start, an unparseable    CONTINUE. One bad row out of
                  timestamp, SoC of 140%.        thousands must not fail a run,
                                                 and it must not be dropped.

 ContractError    An expected source column      FAIL LOUDLY and stop. The
                  disappeared, a required        assumption the pipeline is
                  setting is missing, a table    built on is no longer true.
                  is not what the code expects.  Continuing produces silently
                                                 wrong data.
===========================================================================

Retrying a malformed record three times is not error handling - it is three
times the same failure. Quarantining a network timeout loses data that was
never broken. The classification *is* the engineering.

Each class carries ``is_retryable`` so a generic handler (or an Airflow task
wrapper in a later phase) can branch on behaviour rather than on isinstance
chains scattered through the codebase.
"""

from __future__ import annotations

from typing import Any

__all__ = [
    "VoltHiveError",
    "TransientError",
    "DataError",
    "ContractError",
    "ConfigurationError",
]


class VoltHiveError(Exception):
    """Base class for every error raised by this platform.

    Catching ``VoltHiveError`` catches "something our code decided was wrong",
    as opposed to an arbitrary library exception. Never raised directly.
    """

    #: Whether a retry could plausibly succeed with the same input.
    is_retryable: bool = False

    #: Short, stable label used in logs and audit rows.
    category: str = "unknown"

    def __init__(self, message: str, **context: Any) -> None:
        super().__init__(message)
        self.message = message
        #: Arbitrary structured context, merged into the structured log entry.
        self.context: dict[str, Any] = context

    def as_dict(self) -> dict[str, Any]:
        """Render as a flat dict suitable for structured logging or audit rows."""
        return {
            "error_type": type(self).__name__,
            "error_category": self.category,
            "error_message": self.message,
            "is_retryable": self.is_retryable,
            **self.context,
        }

    def __repr__(self) -> str:
        return f"{type(self).__name__}({self.message!r}, {self.context!r})"


class TransientError(VoltHiveError):
    """A failure that is expected to succeed on a later attempt.

    Raise when the *input was fine* and the environment was not: network
    resets, connection timeouts, HTTP 429/5xx, a database that is restarting,
    a lock that could not be acquired in time.

    Handling: retried by the Airflow task retry policy with exponential
    backoff (Phase 9/11). If retries are exhausted, the task fails and
    downstream tasks do not run - deliberately, because a partially-ingested
    window must never reach the warehouse.

    Trade-off: retrying costs wall-clock time. That is why transform tasks
    get fewer retries than ingestion tasks - a deterministic SQL failure will
    fail identically three times and only delay the real error message.
    """

    is_retryable = True
    category = "transient"

    def __init__(
        self,
        message: str,
        *,
        source_system: str | None = None,
        attempt: int | None = None,
        **context: Any,
    ) -> None:
        super().__init__(
            message,
            source_system=source_system,
            attempt=attempt,
            **context,
        )


class DataError(VoltHiveError):
    """A single record violates a data-quality rule.

    Raise when *this row* is wrong but the pipeline and the source contract
    are both fine: a negative energy delta, a timestamp that will not parse,
    a state-of-charge outside 0-100.

    Handling: the record is written to ``dq.quarantine_*`` with its rule code,
    a human-readable detail, and its complete original payload, and processing
    continues (Phase 7/10). It is never silently dropped, and it can be
    requeued once the underlying problem is fixed.

    Escalation: an individual DataError is not a pipeline failure, but a
    *ratio* of them is. When the quarantine rate for a window crosses the
    configured threshold, the data-quality gate blocks the mart publish -
    at that point it is no longer a bad record, it is a bad feed.

    Trade-off: quarantining keeps the pipeline running at the cost of
    completeness for that window. That is the right trade for analytics, and
    the wrong one for a billing system of record - which this is not, and the
    README says so.
    """

    is_retryable = False
    category = "data"

    def __init__(
        self,
        message: str,
        *,
        rule_code: str | None = None,
        natural_key: str | None = None,
        source_file: str | None = None,
        **context: Any,
    ) -> None:
        super().__init__(
            message,
            rule_code=rule_code,
            natural_key=natural_key,
            source_file=source_file,
            **context,
        )


class ContractError(VoltHiveError):
    """An assumption the pipeline is built on is no longer true.

    Raise when the *shape* of the world changed: a source column that the
    extract selects no longer exists, a required setting is absent, a JSON
    payload is missing a field the staging layer depends on, a table does not
    have the structure the code expects.

    Handling: fail immediately and loudly. Do not retry (the next attempt
    fails identically), do not quarantine (the problem is not one row), do not
    continue (downstream casts would silently produce NULLs, which is far
    worse than a red task - a failed run is visible, wrong data is not).

    This is the category most often missing from junior pipelines, and it is
    the one that prevents the worst failure mode: a pipeline that keeps
    reporting success while quietly producing garbage.
    """

    is_retryable = False
    category = "contract"

    def __init__(
        self,
        message: str,
        *,
        entity: str | None = None,
        expected: Any = None,
        actual: Any = None,
        **context: Any,
    ) -> None:
        super().__init__(
            message,
            entity=entity,
            expected=expected,
            actual=actual,
            **context,
        )


class ConfigurationError(ContractError):
    """Configuration is missing or invalid.

    A subclass of :class:`ContractError` rather than a fourth top-level
    category, because it behaves identically: it is a broken assumption about
    the deployment, it cannot be retried into working, and continuing without
    it would mean guessing. Keeping the taxonomy at three categories is
    deliberate - a taxonomy nobody can recite is a taxonomy nobody applies.
    """

    category = "configuration"
