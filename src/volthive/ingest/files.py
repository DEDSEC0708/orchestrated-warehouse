"""Ingest the partitioned landing files: OCPP charge detail records and meter
telemetry.

The pattern for both is the same and is worth stating plainly:

    scan the ``dt=`` partitions in the lookback window
      -> hash each file
        -> skip it if that (path, hash) pair is already registered
          -> otherwise stream it into ``raw`` and register it

That is the whole of file-level idempotency. A file re-delivered unchanged is
skipped; a file rewritten with corrections has a new hash and is reprocessed;
neither case needs a special code path.

Files are **streamed**, never read whole. A JSONL file is consumed line by
line and a gzipped CSV through a decompressing reader, so memory stays flat
regardless of file size. That matters less at the default profile than the
habit does.

Malformed input is handled at the right layer: a JSON line that will not parse
is landed as a quarantine row here rather than crashing the extract, because
ingestion's job is to move bytes and a single unparseable line is not a reason
to lose the other two thousand records in the file.
"""

from __future__ import annotations

import csv
import gzip
import json
import time
from collections.abc import Iterator
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import psycopg

from volthive.audit import LoadStat, write_load_stat
from volthive.config.settings import get_settings, load_yaml_config
from volthive.db.bulk import copy_rows
from volthive.db.connection import transaction
from volthive.exceptions import ContractError
from volthive.ingest.lineage import payload_hash, row_hash
from volthive.ingest.registry import (
    DiscoveredFile,
    discover_partition_files,
    is_already_ingested,
    register_file,
)
from volthive.ingest.watermark import advance_watermark, compute_window, read_watermark
from volthive.logging_setup import get_logger

__all__ = ["ingest_file_source"]

log = get_logger(__name__)

_JSON_AUDIT_COLUMNS = [
    "dw_run_id",
    "dw_source_system",
    "dw_source_file",
    "dw_source_row_seq",
    "dw_batch_key",
]

#: A line that does not parse as JSON. Landed with the raw text preserved in
#: the payload under a reserved key, so the byte sequence that broke the parser
#: is still recoverable - which is the entire promise of an immutable raw layer.
_UNPARSEABLE_KEY = "_unparseable_raw_line"


def _source_config(source: str) -> dict[str, Any]:
    document = load_yaml_config("sources.yml")
    sources = document.get("files", {})
    if source not in sources:
        raise ContractError(
            f"File source '{source}' is not declared in configs/sources.yml",
            entity=source,
            expected=sorted(sources),
        )
    return sources[source]


def _read_jsonl(path: Path) -> Iterator[tuple[int, str, dict[str, Any] | None]]:
    """Yield ``(line_number, raw_line, parsed_or_None)`` from a JSONL file."""
    with path.open("r", encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, start=1):
            stripped = line.rstrip("\n")
            if not stripped.strip():
                continue
            try:
                yield line_number, stripped, json.loads(stripped)
            except json.JSONDecodeError:
                yield line_number, stripped, None


def _read_csv_gz(path: Path) -> Iterator[tuple[int, dict[str, str]]]:
    """Yield ``(line_number, row)`` from a gzipped CSV file."""
    with gzip.open(path, "rt", encoding="utf-8", newline="") as handle:
        yield from enumerate(csv.DictReader(handle), start=1)


def _json_rows(
    discovered: DiscoveredFile,
    config: dict[str, Any],
    *,
    run_id: str,
) -> Iterator[tuple[Any, ...]]:
    """Render a JSONL file as raw-table tuples."""
    promoted: dict[str, str] = config.get("promoted", {})
    for line_number, raw_line, parsed in _read_jsonl(discovered.path):
        if parsed is None:
            payload: dict[str, Any] = {_UNPARSEABLE_KEY: raw_line}
            promoted_values: list[str | None] = [None] * len(promoted)
        else:
            payload = parsed
            promoted_values = [
                None if parsed.get(source_key) is None else str(parsed[source_key])
                for source_key in promoted.values()
            ]
        yield (
            run_id,
            config["source_system"],
            discovered.registry_path,
            line_number,
            discovered.batch_key,
            *promoted_values,
            False,
            json.dumps(payload, sort_keys=True),
            payload_hash(raw_line),
        )


def _csv_rows(
    discovered: DiscoveredFile,
    config: dict[str, Any],
    *,
    run_id: str,
) -> Iterator[tuple[Any, ...]]:
    """Render a gzipped CSV file as raw-table tuples."""
    columns: list[str] = config["columns"]
    for line_number, row in _read_csv_gz(discovered.path):
        values = [row.get(column) or None for column in columns]
        yield (
            run_id,
            config["source_system"],
            discovered.registry_path,
            line_number,
            discovered.batch_key,
            *values,
            row_hash(values),
        )


def ingest_file_source(
    conn: psycopg.Connection,
    source: str,
    *,
    run_id: str,
    data_interval_start: datetime,
    data_interval_end: datetime,
    dag_id: str | None = None,
    task_id: str | None = None,
    landing_root: Path | None = None,
    lookback_override: Any = None,
) -> LoadStat:
    """Ingest every unprocessed file in the lookback window for one source.

    Each file is loaded and registered in ITS OWN transaction, deliberately.
    One transaction for the whole batch would mean a failure on the eighth of
    ten files discards the seven that already succeeded; per-file transactions
    mean a rerun skips those seven by hash and resumes at the eighth. The
    watermark still advances once, at the end, only if every file succeeded.
    """
    started = time.monotonic()
    config = _source_config(source)
    settings = get_settings()
    root = landing_root or (settings.data_dir / "landing")

    watermark = read_watermark(conn, config["source_system"], source)
    window = compute_window(
        watermark,
        data_interval_start=data_interval_start,
        data_interval_end=data_interval_end,
        lookback_override=lookback_override,
    )

    discovered = discover_partition_files(
        root, config["landing_subdir"], window.partitions, pattern=config["file_glob"]
    )

    files_seen = len(discovered)
    files_loaded = files_skipped = 0
    rows_inserted = 0
    bytes_read = 0
    max_partition: date | None = None

    if config["format"] == "jsonl":
        target_columns = (
            _JSON_AUDIT_COLUMNS
            + list(config.get("promoted", {}).keys())
            + ["is_requeued", "payload", "payload_hash"]
        )
        row_builder = _json_rows
    elif config["format"] == "csv_gz":
        target_columns = _JSON_AUDIT_COLUMNS + list(config["columns"]) + ["src_row_hash"]
        row_builder = _csv_rows
    else:
        raise ContractError(
            f"Unsupported file format '{config['format']}' for source '{source}'",
            entity=source,
            expected=["jsonl", "csv_gz"],
        )

    log.info(
        "file_scan_started",
        source=source,
        partitions=[p.isoformat() for p in window.partitions],
        files_seen=files_seen,
    )

    for found in discovered:
        if is_already_ingested(conn, found):
            files_skipped += 1
            log.debug("file_skipped_duplicate", file=found.registry_path)
            continue

        with transaction(conn):
            written = copy_rows(
                conn,
                config["target_table"],
                target_columns,
                row_builder(found, config, run_id=run_id),
                log_context={"source": source, "file": found.path.name},
            )
            register_file(
                conn,
                found,
                source_system=config["source_system"],
                row_count=written,
                run_id=run_id,
            )
        rows_inserted += written
        bytes_read += found.size_bytes
        files_loaded += 1
        max_partition = (
            found.batch_key if max_partition is None else max(max_partition, found.batch_key)
        )

    with transaction(conn):
        stat = LoadStat(
            task_id=task_id or f"ingest_{source}",
            dag_id=dag_id,
            pipeline_run_id=run_id,
            source_system=config["source_system"],
            entity=source,
            target_table=config["target_table"],
            dw_batch_key=window.batch_key,
            rows_read=rows_inserted,
            rows_inserted=rows_inserted,
            files_seen=files_seen,
            files_loaded=files_loaded,
            files_skipped=files_skipped,
            bytes_read=bytes_read,
            duration_ms=int((time.monotonic() - started) * 1000),
        )
        write_load_stat(conn, stat)
        advance_watermark(
            conn,
            config["source_system"],
            source,
            new_value=max_partition.isoformat() if max_partition else None,
            run_id=run_id,
        )

    log.info(
        "file_ingest_completed",
        source=source,
        files_seen=files_seen,
        files_loaded=files_loaded,
        files_skipped_duplicate=files_skipped,
        rows_inserted=rows_inserted,
        duration_ms=stat.duration_ms,
    )
    return stat


def ingest_seed_file(
    conn: psycopg.Connection,
    *,
    run_id: str,
    dag_id: str | None = None,
    task_id: str | None = None,
    batch_key: date | None = None,
) -> LoadStat:
    """Load source S5, the static grid-tariff reference, as a FULL SNAPSHOT.

    Truncate and reload, and the choice is deliberate rather than lazy. Sixty
    rows of slowly-changing reference data are cheaper and safer to reload
    wholesale than to reason about incrementally, and having one source that
    is NOT incremental demonstrates that "incremental everywhere" is dogma
    rather than engineering.
    """
    started = time.monotonic()
    document = load_yaml_config("sources.yml")
    config = document["seed"]["grid_tariff_slab"]
    from volthive.config.settings import repo_root

    path = repo_root() / config["path"]
    if not path.is_file():
        raise ContractError(
            f"Grid tariff seed file not found: {path}. Run `make generate` to write it.",
            entity="grid_tariff_slab",
            expected=str(path),
        )

    columns: list[str] = config["columns"]
    effective_batch = batch_key or datetime.now(tz=UTC).date()

    def rows() -> Iterator[tuple[Any, ...]]:
        with path.open("r", encoding="utf-8", newline="") as handle:
            for line_number, record in enumerate(csv.DictReader(handle), start=1):
                values = [record.get(column) or None for column in columns]
                yield (
                    run_id,
                    "SEED",
                    config["path"],
                    line_number,
                    effective_batch,
                    *values,
                    row_hash(values),
                )

    with transaction(conn):
        with conn.cursor() as cur:
            cur.execute("TRUNCATE TABLE raw.grid_tariff_slab")
        written = copy_rows(
            conn,
            config["target_table"],
            _JSON_AUDIT_COLUMNS + columns + ["src_row_hash"],
            rows(),
            log_context={"source": "grid_tariff_slab"},
        )
        stat = LoadStat(
            task_id=task_id or "ingest_grid_tariff_seed",
            dag_id=dag_id,
            pipeline_run_id=run_id,
            source_system="SEED",
            entity="grid_tariff_slab",
            target_table=config["target_table"],
            dw_batch_key=effective_batch,
            rows_read=written,
            rows_inserted=written,
            files_seen=1,
            files_loaded=1,
            bytes_read=path.stat().st_size,
            duration_ms=int((time.monotonic() - started) * 1000),
        )
        write_load_stat(conn, stat)

    log.info("seed_ingest_completed", rows_inserted=written)
    return stat
