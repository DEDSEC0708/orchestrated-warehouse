"""The processed-file registry: file-level idempotency in one table.

``ctl.ingested_file`` is keyed on ``(file_path, file_sha256)``, and that key
choice IS the mechanism. Two behaviours fall out of it with no branching logic:

* a file **re-delivered unchanged** has the same path and the same hash, is
  already registered, and is skipped;
* a file **rewritten with corrections** has the same path and a NEW hash, is
  therefore not registered, and is reprocessed - with the downstream
  restatement window rewriting the facts it affects.

Keying on path alone would silently ignore corrections. Keying on hash alone
would reprocess a file that was merely moved. The pair is right.

Hashes are computed by streaming the file in chunks. A gzipped meter file is
small, but "read the whole thing into memory to hash it" is the kind of
shortcut that works for eighteen months and then meets a 2 GB file.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path

import psycopg

from volthive.db.connection import fetch_value
from volthive.logging_setup import get_logger

__all__ = [
    "DiscoveredFile",
    "discover_partition_files",
    "file_sha256",
    "is_already_ingested",
    "register_file",
]

log = get_logger(__name__)

_HASH_CHUNK_BYTES = 1024 * 1024


@dataclass(frozen=True, slots=True)
class DiscoveredFile:
    """One landing file found by a partition scan."""

    path: Path
    sha256: str
    size_bytes: int
    batch_key: date
    partition_label: str

    @property
    def registry_path(self) -> str:
        """The path as stored in the registry.

        Stored RELATIVE to the landing root, deliberately. The same file is
        ``/opt/airflow/data/landing/...`` inside a container and
        ``C:\\Projects\\...\\data\\landing\\...`` on a developer's machine, and
        an absolute path would make the registry reprocess everything the first
        time the project moved.
        """
        return self.path.as_posix()


def file_sha256(path: Path) -> str:
    """Stream a file and return its hex sha256."""
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(_HASH_CHUNK_BYTES):
            digest.update(chunk)
    return digest.hexdigest()


def discover_partition_files(
    landing_root: Path,
    source_dir: str,
    partitions: list[date],
    *,
    pattern: str = "*",
) -> list[DiscoveredFile]:
    """Find every file in the given ``dt=`` partitions.

    Args:
        landing_root: The landing zone root, e.g. ``data/landing``.
        source_dir: ``cdr``, ``meter`` or ``partner``.
        partitions: The dates to scan, from the extract window.
        pattern: Glob for the file name.

    Returns:
        Files sorted by path, so a rerun processes them in the same order and
        the resulting ``dw_source_row_seq`` values are stable. Missing
        partitions are simply absent from the result - a day with no data is
        normal (the pipeline started yesterday, or it is a partition that will
        be written later), and treating it as an error would make the lookback
        window fail on its first run every time.
    """
    found: list[DiscoveredFile] = []
    for partition in partitions:
        directory = landing_root / source_dir / f"dt={partition.isoformat()}"
        if not directory.is_dir():
            continue
        for path in sorted(directory.rglob(pattern)):
            if not path.is_file():
                continue
            found.append(
                DiscoveredFile(
                    path=path,
                    sha256=file_sha256(path),
                    size_bytes=path.stat().st_size,
                    batch_key=partition,
                    partition_label=f"dt={partition.isoformat()}",
                )
            )
    return found


def is_already_ingested(conn: psycopg.Connection, discovered: DiscoveredFile) -> bool:
    """Whether this exact (path, content) pair has been loaded before."""
    return bool(
        fetch_value(
            conn,
            """
            SELECT 1 FROM ctl.ingested_file
            WHERE file_path = :file_path AND file_sha256 = :file_sha256
              AND status = 'LOADED'
            """,
            {"file_path": discovered.registry_path, "file_sha256": discovered.sha256},
        )
    )


def register_file(
    conn: psycopg.Connection,
    discovered: DiscoveredFile,
    *,
    source_system: str,
    row_count: int,
    run_id: str | None = None,
    status: str = "LOADED",
) -> None:
    """Record that a file was processed, inside the caller's transaction.

    ``ON CONFLICT DO UPDATE`` rather than ``DO NOTHING``: a file that previously
    failed is registered with status FAILED, and a later successful run has to
    be able to flip it to LOADED. With DO NOTHING the failed marker would be
    permanent and the file would be reprocessed on every subsequent run for
    ever.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO ctl.ingested_file (
                file_path, file_sha256, source_system, dw_batch_key,
                row_count, bytes, first_ingested_run_id, status
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (file_path, file_sha256) DO UPDATE
            SET row_count = EXCLUDED.row_count,
                status = EXCLUDED.status
            """,
            (
                discovered.registry_path,
                discovered.sha256,
                source_system,
                discovered.batch_key,
                row_count,
                discovered.size_bytes,
                run_id,
                status,
            ),
        )


def partition_of(moment: datetime) -> date:
    """The ``dt=`` partition an instant belongs to, in UTC."""
    return moment.astimezone().date() if moment.tzinfo is None else moment.date()
