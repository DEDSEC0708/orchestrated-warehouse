"""Watermarks: how the pipeline knows what is new, and how it remembers.

This is the smallest module in the ingestion layer and the one most worth
getting exactly right. Three properties, each of which is a specific bug
avoided:

**1. The upper bound comes from Airflow's data interval, never from
``now()``.** With ``now()`` the same DAG run extracts a different set depending
on when it happens to execute, so a rerun is not reproducible and a backfill
quietly pulls *current* data into a *historical* partition. This one line is
the difference between a backfillable pipeline and a broken one.

**2. The new watermark is derived from the data that was loaded, not from the
clock.** If the source's newest row is three hours old, setting the watermark
to ``now()`` skips every row that arrives later carrying an older timestamp.

**3. The interval is half-open: ``(lower, upper]``.** Consecutive windows
therefore neither skip nor duplicate the boundary instant. Inclusive on both
ends double-counts it; exclusive on both loses it.

And one property that is about *when* rather than *what*: the watermark is
advanced **inside the same transaction that commits the data**, at the end.
Either both moved or neither did. That is what makes "the job died mid-load"
a non-event rather than an incident.

Advancement uses ``GREATEST``, so an out-of-order rerun - a backfill of an old
window executing after a current one - can never move the watermark backwards
and cause the pipeline to re-extract months of data.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from typing import Any

import psycopg

from volthive.db.connection import fetch_one
from volthive.exceptions import ContractError
from volthive.logging_setup import get_logger

__all__ = ["Watermark", "ExtractWindow", "read_watermark", "compute_window", "advance_watermark"]

log = get_logger(__name__)

EPOCH = datetime(1970, 1, 1, tzinfo=UTC)


@dataclass(frozen=True, slots=True)
class Watermark:
    """One row of ``ctl.watermark``."""

    source_system: str
    entity: str
    watermark_type: str
    watermark_ts_utc: datetime | None
    watermark_str: str | None
    lookback: timedelta


@dataclass(frozen=True, slots=True)
class ExtractWindow:
    """The bounded window one extract will read.

    ``lower_exclusive`` and ``upper_inclusive`` are named for their SEMANTICS,
    not their values, because "is the boundary in or out?" is exactly the
    question that gets answered wrong in a code review and then loses a row a
    month later.
    """

    lower_exclusive: datetime
    upper_inclusive: datetime
    lookback: timedelta
    partitions: list[date]

    @property
    def batch_key(self) -> date:
        """The logical partition this window belongs to.

        Derived from the END of the interval minus one microsecond: an Airflow
        daily run for 2026-06-14 has a data interval of
        ``[2026-06-14 00:00, 2026-06-15 00:00)``, and its batch key must be the
        14th, not the 15th.
        """
        return (self.upper_inclusive - timedelta(microseconds=1)).date()


def read_watermark(conn: psycopg.Connection, source_system: str, entity: str) -> Watermark:
    """Read one watermark row.

    Raises:
        ContractError: no row exists. Deliberately fatal rather than defaulting
            to the epoch: a missing watermark means the seed did not run, and
            silently re-extracting all history would look like a very slow
            success rather than the misconfiguration it is.
    """
    row = fetch_one(
        conn,
        """
        SELECT source_system, entity, watermark_type, watermark_ts_utc,
               watermark_str, lookback_interval
        FROM ctl.watermark
        WHERE source_system = :source_system AND entity = :entity
        """,
        {"source_system": source_system, "entity": entity},
    )
    if row is None:
        raise ContractError(
            f"No watermark row for ({source_system}, {entity}). "
            "Run scripts/apply_schema.py to seed ctl.watermark.",
            entity=f"{source_system}.{entity}",
        )
    return Watermark(
        source_system=row["source_system"],
        entity=row["entity"],
        watermark_type=row["watermark_type"],
        watermark_ts_utc=row["watermark_ts_utc"],
        watermark_str=row["watermark_str"],
        lookback=row["lookback_interval"],
    )


def compute_window(
    watermark: Watermark,
    *,
    data_interval_start: datetime,
    data_interval_end: datetime,
    lookback_override: timedelta | None = None,
) -> ExtractWindow:
    """Compute the bounded extract window for one run.

    Args:
        watermark: The current watermark for this entity.
        data_interval_start: Airflow's logical interval start.
        data_interval_end: Airflow's logical interval end. THIS, never
            ``now()``, is the upper bound.
        lookback_override: Widen the window for a one-off repair, from the
            DAG's ``lookback_days_override`` parameter.

    Returns:
        The window, including the list of ``dt=`` partitions to scan for
        partition-oriented sources.
    """
    lookback = lookback_override if lookback_override is not None else watermark.lookback

    if watermark.watermark_type == "date_partition":
        # Partition sources look back from the INTERVAL START, not from the
        # stored watermark. The two differ during a backfill: the watermark may
        # already be far ahead (a later day ran first), and looking back from
        # it would scan the wrong days entirely.
        first_day = (data_interval_start - lookback).date()
        last_day = (data_interval_end - timedelta(microseconds=1)).date()
        partitions = [
            first_day + timedelta(days=offset) for offset in range((last_day - first_day).days + 1)
        ]
        lower = datetime.combine(first_day, datetime.min.time(), tzinfo=UTC)
        return ExtractWindow(lower, data_interval_end, lookback, partitions)

    base = watermark.watermark_ts_utc or EPOCH
    lower = base - lookback
    # A rerun of an OLD window must not re-extract everything just because the
    # watermark has since advanced past it. Clamping the lower bound to the
    # interval start keeps a backfill reading only its own window.
    lower = min(lower, data_interval_start - lookback)
    return ExtractWindow(lower, data_interval_end, lookback, [])


def advance_watermark(
    conn: psycopg.Connection,
    source_system: str,
    entity: str,
    *,
    new_value: datetime | str | None,
    upper_bound: datetime | None = None,
    run_id: str | None = None,
) -> None:
    """Advance a watermark, never backwards, inside the caller's transaction.

    Args:
        new_value: The maximum value actually observed in the loaded rows. None
            means nothing was loaded, in which case the watermark is left alone
            - advancing on an empty extract would skip rows that arrive later
            with timestamps inside the window just read.
        upper_bound: Caps the new value at the window's upper bound. A source
            row carrying a future timestamp (clock skew, a bad test record)
            would otherwise push the watermark past the interval and silently
            skip everything in between.
        run_id: Recorded as ``last_success_run_id``.

    MUST be called inside the same transaction as the data load. That is not a
    style preference: it is the entire crash-safety argument.
    """
    if new_value is None:
        log.info(
            "watermark_unchanged_no_rows",
            source_system=source_system,
            entity=entity,
        )
        return

    if isinstance(new_value, datetime):
        capped: Any = min(new_value, upper_bound) if upper_bound else new_value
        column, value = "watermark_ts_utc", capped
    else:
        column, value = "watermark_str", new_value

    # GREATEST is what makes an out-of-order rerun harmless. A backfill of May
    # executing after June's scheduled run must not rewind the watermark to
    # May and cause the whole of June to be re-extracted.
    statement = f"""
        UPDATE ctl.watermark
        SET {column} = GREATEST({column}, %(value)s),
            last_success_run_id = %(run_id)s,
            last_success_at_utc = now(),
            updated_at_utc = now()
        WHERE source_system = %(source_system)s AND entity = %(entity)s
    """  # noqa: S608 - `column` is chosen from two literals above, never from input
    with conn.cursor() as cur:
        cur.execute(
            statement,
            {
                "value": value,
                "run_id": run_id,
                "source_system": source_system,
                "entity": entity,
            },
        )

    log.info(
        "watermark_advanced",
        source_system=source_system,
        entity=entity,
        watermark_after=str(value),
    )
