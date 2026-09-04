"""Bulk loading with ``COPY``, and nothing else.

``COPY ... FROM STDIN`` rather than executemany, everywhere, without exception.
The difference is one to two orders of magnitude on the volumes this project
handles, and it is the single easiest large win in a batch loader: ``COPY``
sends one statement and streams rows, while ``INSERT`` per row pays statement
parse, plan and round-trip costs on every one of them.

Rows are consumed from an ITERATOR, never from a materialised list. An
eighteen-month bootstrap chunk must not have to fit in memory, and the whole
ingestion path - JSONL reader, gzipped CSV reader, server-side cursor over the
CMS - is written as generators so that this holds end to end.

Chunking exists for two reasons that are easy to conflate: it bounds memory,
and it produces progress you can watch. It does *not* create transaction
boundaries. Chunks are flushed inside whatever transaction the caller opened,
so a failure half way through rolls the whole load back - which is exactly what
"the watermark advances with the data" requires.
"""

from __future__ import annotations

from collections.abc import Iterable, Iterator, Sequence
from typing import Any

import psycopg
from psycopg import sql as pgsql

from volthive.logging_setup import get_logger

__all__ = ["copy_rows", "DEFAULT_CHUNK_SIZE"]

log = get_logger(__name__)

#: Rows per progress checkpoint. Large enough to amortise the per-chunk
#: bookkeeping, small enough to bound peak memory and to log progress often
#: enough to be useful on a long backfill.
DEFAULT_CHUNK_SIZE = 10_000


def _chunked(rows: Iterable[Sequence[Any]], size: int) -> Iterator[list[Sequence[Any]]]:
    """Yield lists of at most ``size`` rows from an iterable, lazily."""
    batch: list[Sequence[Any]] = []
    for row in rows:
        batch.append(row)
        if len(batch) >= size:
            yield batch
            batch = []
    if batch:
        yield batch


def copy_rows(
    conn: psycopg.Connection,
    table: str,
    columns: Sequence[str],
    rows: Iterable[Sequence[Any]],
    *,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
    log_context: dict[str, Any] | None = None,
) -> int:
    """Bulk-load rows into ``table`` and return how many were written.

    Args:
        conn: An open connection. The caller owns the transaction.
        table: Target as ``schema.table``. Composed with :mod:`psycopg.sql`
            identifiers rather than string-formatted, so a table name can never
            be interpreted as SQL even though it arrives as a string.
        columns: Target column names, in the order ``rows`` supplies values.
        rows: An iterable of row sequences. Consumed lazily - pass a generator.
        chunk_size: Rows per progress checkpoint.
        log_context: Extra fields to bind onto the completion log line.

    Returns:
        The number of rows written.
    """
    schema_name, _, table_name = table.partition(".")
    if not schema_name or not table_name:
        msg = f"copy_rows expects a qualified 'schema.table', got {table!r}"
        raise ValueError(msg)

    statement = pgsql.SQL("COPY {}.{} ({}) FROM STDIN").format(
        pgsql.Identifier(schema_name),
        pgsql.Identifier(table_name),
        pgsql.SQL(", ").join(pgsql.Identifier(column) for column in columns),
    )

    total = 0
    with conn.cursor() as cur, cur.copy(statement) as copy:
        for batch in _chunked(rows, chunk_size):
            for row in batch:
                copy.write_row(row)
            total += len(batch)
            log.debug(
                "copy_progress",
                target_table=table,
                rows_written=total,
                **(log_context or {}),
            )

    log.info("copy_completed", target_table=table, rows_written=total, **(log_context or {}))
    return total
