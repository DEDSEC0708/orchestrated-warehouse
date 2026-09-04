"""Extract master data from the simulated CMS into ``raw``.

The shape of a real incremental database extract, with nothing simulated about
the mechanics:

* a **server-side cursor**, so an eighteen-month bootstrap chunk streams rather
  than materialising in Python memory;
* an **explicit column list** from ``configs/sources.yml``, so a new upstream
  column is ignored until declared and a dropped one fails loudly at extract
  time;
* a **bounded window** whose upper edge is the Airflow data interval, never
  ``now()``;
* **chunked COPY** into the raw table;
* the **watermark advanced in the same transaction as the data**.

Ingestion moves bytes and does not interpret them. It does not cast, does not
filter "bad" rows and does not derive columns. Every value lands as TEXT. If
ingestion decided what was valid, the evidence of what actually arrived would
be destroyed - and a transformation bug would then be unfixable without
re-reading a source that may no longer hold the data.
"""

from __future__ import annotations

import time
from collections.abc import Iterator
from datetime import datetime
from typing import Any

import psycopg
from psycopg import sql as pgsql

from volthive.audit import LoadStat, write_load_stat
from volthive.config.settings import load_yaml_config
from volthive.db.bulk import copy_rows
from volthive.db.connection import cms_connection, transaction
from volthive.exceptions import ContractError
from volthive.ingest.lineage import row_hash
from volthive.ingest.watermark import (
    ExtractWindow,
    advance_watermark,
    compute_window,
    read_watermark,
)
from volthive.logging_setup import get_logger

__all__ = ["CMS_ENTITIES", "ingest_cms_entity"]

log = get_logger(__name__)

#: The five CMS entities, in the order the dimension merges need them. Kept as
#: a module constant so a DAG can iterate it without re-reading YAML at import
#: time - the Airflow scheduler re-parses DAG files constantly, and file I/O at
#: import is how a config typo becomes a scheduler-wide outage.
CMS_ENTITIES = ["customers", "vehicles", "stations", "charge_points", "tariff_plans"]

#: Audit columns prepended to every raw row, in the order copy_rows writes them.
_AUDIT_COLUMNS = [
    "dw_run_id",
    "dw_source_system",
    "dw_source_file",
    "dw_source_row_seq",
    "dw_batch_key",
]


def _entity_config(entity: str) -> dict[str, Any]:
    document = load_yaml_config("sources.yml")
    entities = document.get("cms", {}).get("entities", {})
    if entity not in entities:
        raise ContractError(
            f"CMS entity '{entity}' is not declared in configs/sources.yml",
            entity=entity,
            expected=sorted(entities),
        )
    return entities[entity]


def _stream_rows(
    source_conn: psycopg.Connection,
    config: dict[str, Any],
    window: ExtractWindow,
    *,
    run_id: str,
    entity: str,
) -> Iterator[tuple[Any, ...]]:
    """Yield raw-shaped tuples from a server-side cursor over the source.

    The identifiers are composed with :mod:`psycopg.sql`, never formatted into
    a string. They come from a version-controlled config file rather than from
    user input, so this is belt and braces - but a codebase in which SQL is
    NEVER assembled by string formatting has no place for the exception to hide.
    """
    columns: list[str] = config["columns"]
    watermark_column: str = config["watermark_column"]

    query = pgsql.SQL(
        "SELECT {columns} FROM {table} "
        "WHERE {watermark} > %(lower)s AND {watermark} <= %(upper)s "
        "ORDER BY {watermark}, {first_column}"
    ).format(
        columns=pgsql.SQL(", ").join(pgsql.Identifier(c) for c in columns),
        table=pgsql.Identifier(config["source_table"]),
        watermark=pgsql.Identifier(watermark_column),
        first_column=pgsql.Identifier(columns[0]),
    )

    # A NAMED cursor is a server-side cursor: rows are fetched in batches
    # instead of the whole result set being sent at once. On a bootstrap chunk
    # that is the difference between steady memory and an OOM kill.
    with source_conn.cursor(name=f"extract_{entity}", row_factory=psycopg.rows.tuple_row) as cur:
        cur.itersize = 5000
        cur.execute(
            query,
            {"lower": window.lower_exclusive, "upper": window.upper_inclusive},
        )
        for sequence, row in enumerate(cur, start=1):
            business_values = ["" if value is None else str(value) for value in row]
            yield (
                run_id,
                "CMS",
                f"{config['source_table']}",
                sequence,
                window.batch_key,
                *business_values,
                row_hash(list(row)),
            )


def ingest_cms_entity(
    warehouse_conn: psycopg.Connection,
    entity: str,
    *,
    run_id: str,
    data_interval_start: datetime,
    data_interval_end: datetime,
    dag_id: str | None = None,
    task_id: str | None = None,
    lookback_override: Any = None,
) -> LoadStat:
    """Extract one CMS entity for one window and land it in ``raw``.

    Returns the load statistics, which are also written to
    ``audit.load_stat`` inside the same transaction as the data.
    """
    started = time.monotonic()
    config = _entity_config(entity)
    watermark = read_watermark(warehouse_conn, "CMS", entity)
    window = compute_window(
        watermark,
        data_interval_start=data_interval_start,
        data_interval_end=data_interval_end,
        lookback_override=lookback_override,
    )

    target_columns = _AUDIT_COLUMNS + list(config["columns"]) + ["src_row_hash"]
    max_watermark: datetime | None = None
    rows_written = 0

    log.info(
        "cms_extract_started",
        entity=entity,
        source_table=config["source_table"],
        window_lower=str(window.lower_exclusive),
        window_upper=str(window.upper_inclusive),
        lookback=str(window.lookback),
    )

    # autocommit=False on the SOURCE connection because a server-side (named)
    # cursor can only be declared inside a transaction block. The connection is
    # read-only by grant, so the transaction is a read snapshot rather than a
    # write - which is also the correct semantics: the extract sees one
    # consistent view of the source even if it is being written to while we
    # stream from it.
    with cms_connection(
        application_name=f"volthive-ingest-{entity}", autocommit=False
    ) as source_conn:
        # The whole load and the watermark update share ONE transaction. If
        # anything raises, the rows and the watermark roll back together, and
        # the next run re-extracts exactly this window. There is no state in
        # which the data landed but the pipeline forgot that it did.
        with transaction(warehouse_conn):
            watermark_index = len(_AUDIT_COLUMNS) + config["columns"].index(
                config["watermark_column"]
            )

            def tracked(iterator: Iterator[tuple[Any, ...]]) -> Iterator[tuple[Any, ...]]:
                nonlocal max_watermark
                for row in iterator:
                    value = row[watermark_index]
                    if value:
                        parsed = datetime.fromisoformat(str(value))
                        if max_watermark is None or parsed > max_watermark:
                            max_watermark = parsed
                    yield row

            rows_written = copy_rows(
                warehouse_conn,
                config["target_table"],
                target_columns,
                tracked(_stream_rows(source_conn, config, window, run_id=run_id, entity=entity)),
                log_context={"entity": entity, "source_system": "CMS"},
            )

            stat = LoadStat(
                task_id=task_id or f"ingest_cms_{entity}",
                dag_id=dag_id,
                pipeline_run_id=run_id,
                source_system="CMS",
                entity=entity,
                target_table=config["target_table"],
                dw_batch_key=window.batch_key,
                rows_read=rows_written,
                rows_inserted=rows_written,
                duration_ms=int((time.monotonic() - started) * 1000),
            )
            write_load_stat(warehouse_conn, stat)

            # Last statement in the transaction, and only reached on success.
            advance_watermark(
                warehouse_conn,
                "CMS",
                entity,
                new_value=max_watermark,
                upper_bound=window.upper_inclusive,
                run_id=run_id,
            )

    log.info(
        "cms_extract_completed",
        entity=entity,
        rows_inserted=rows_written,
        watermark_after=str(max_watermark) if max_watermark else None,
        duration_ms=stat.duration_ms,
    )
    return stat
