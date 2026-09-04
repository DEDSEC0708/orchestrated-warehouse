"""The roaming-partner source: an HTTP client, a file client, and one contract.

**The fragility rule.** A portfolio project that depends on a live external API
is a project that stops working. So the partner source is served locally - by a
small FastAPI app in :mod:`volthive.mock_partner_api` - and the ingestion code
depends on an INTERFACE rather than on HTTP:

.. code-block:: text

    PartnerCdrClient (Protocol)
    |-- HttpPartnerCdrClient   VOLTHIVE_PARTNER_MODE=http
    +-- FilePartnerCdrClient   VOLTHIVE_PARTNER_MODE=file  (default, and CI)

Both read the SAME deterministic dataset, so results are identical either way.
Three consequences, all of them practical: the project runs with the API
container switched off; CI needs no network and cannot flake; and a unit test
can assert that the two clients produce byte-identical output, which is the
test that makes "dependency inversion" a checked property rather than a claim.

**Retries are honest.** The mock API deliberately misbehaves - 503s, 429s with
``Retry-After``, occasional truncated pages - because retry logic that has
never met a failure is decoration. The client honours ``Retry-After`` rather
than blindly backing off, since a server that told you when to come back has
given you better information than your own exponential curve.

**Pagination is bounded.** ``max_pages`` caps the loop. Without it, a server
that fails to advance its cursor - or that misreports ``total`` - turns a
five-minute task into an infinite loop holding a worker slot until someone
notices.
"""

from __future__ import annotations

import json
import time
from collections.abc import Iterator
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Protocol

import psycopg

from volthive.audit import LoadStat, write_load_stat
from volthive.config.settings import get_settings, load_yaml_config
from volthive.db.bulk import copy_rows
from volthive.db.connection import transaction
from volthive.exceptions import ContractError, TransientError
from volthive.ingest.lineage import payload_hash
from volthive.ingest.watermark import advance_watermark, compute_window, read_watermark
from volthive.logging_setup import get_logger

__all__ = [
    "FilePartnerCdrClient",
    "HttpPartnerCdrClient",
    "PartnerCdrClient",
    "build_partner_client",
    "ingest_partner_cdrs",
]

log = get_logger(__name__)


class PartnerCdrClient(Protocol):
    """What the ingestion layer needs from the roaming partner.

    Deliberately narrow. A wider interface - "give me a session", "give me a
    location" - would couple ingestion to the partner's data model; this one
    couples it only to "records changed in a time range", which is the only
    thing an incremental extract actually needs.
    """

    def fetch(self, date_from: datetime, date_to: datetime) -> Iterator[dict[str, Any]]:
        """Yield partner records whose ``last_updated`` falls in the range."""
        ...


def _parse_instant(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


class FilePartnerCdrClient:
    """Reads the deterministic partner dataset straight off disk.

    The default, and what CI uses. No network, no container, no flakiness -
    and the same records the HTTP client would return, because both read the
    files the generator wrote.
    """

    def __init__(self, landing_root: Path) -> None:
        self.landing_root = landing_root

    def fetch(self, date_from: datetime, date_to: datetime) -> Iterator[dict[str, Any]]:
        partner_dir = self.landing_root / "partner"
        if not partner_dir.is_dir():
            return

        # Files are partitioned by last_updated DATE, so a whole day can be
        # skipped without opening it. The per-record filter below is still
        # needed for the partial days at each end of the range.
        first_day = date_from.date()
        last_day = date_to.date()
        for directory in sorted(partner_dir.glob("dt=*")):
            try:
                partition_day = date.fromisoformat(directory.name.removeprefix("dt="))
            except ValueError:
                continue
            if partition_day < first_day or partition_day > last_day:
                continue
            for path in sorted(directory.glob("*.jsonl")):
                with path.open("r", encoding="utf-8") as handle:
                    for line in handle:
                        if not line.strip():
                            continue
                        record = json.loads(line)
                        updated = _parse_instant(record["last_updated"])
                        if date_from < updated <= date_to:
                            yield record


class HttpPartnerCdrClient:
    """Paginated HTTP client for the OCPI-style partner API.

    Retries only what is worth retrying. A 503 or a 429 means "the world was
    briefly unavailable" and is raised as a :class:`TransientError`; a 400 or a
    404 means the request itself is wrong, and retrying it three times only
    delays the real error message by six minutes.
    """

    def __init__(
        self,
        base_url: str,
        token: str | None,
        *,
        base_path: str = "/ocpi/2.2/cdrs",
        page_size: int = 500,
        max_pages: int = 200,
        timeout_seconds: float = 15.0,
        max_retries: int = 4,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.token = token
        self.base_path = base_path
        self.page_size = page_size
        self.max_pages = max_pages
        self.timeout_seconds = timeout_seconds
        self.max_retries = max_retries

    def _get_page(self, client: Any, params: dict[str, Any]) -> dict[str, Any]:
        import httpx

        for attempt in range(1, self.max_retries + 1):
            try:
                response = client.get(self.base_path, params=params)
            except httpx.TransportError as exc:
                if attempt >= self.max_retries:
                    raise TransientError(
                        f"Partner API unreachable after {attempt} attempts: {exc}",
                        source_system="PARTNER",
                        attempt=attempt,
                    ) from exc
                time.sleep(min(2**attempt, 20))
                continue

            if response.status_code == 429:
                # The server told us when to come back. Believe it - an
                # exponential curve that ignores Retry-After is guessing at
                # information the server already supplied.
                wait = float(response.headers.get("Retry-After", 2))
                log.warning("partner_api_rate_limited", retry_after=wait, attempt=attempt)
                if attempt >= self.max_retries:
                    raise TransientError(
                        "Partner API rate limit not cleared within the retry budget",
                        source_system="PARTNER",
                        attempt=attempt,
                    )
                time.sleep(min(wait, 30))
                continue

            if 500 <= response.status_code < 600:
                log.warning(
                    "partner_api_server_error",
                    status=response.status_code,
                    attempt=attempt,
                )
                if attempt >= self.max_retries:
                    raise TransientError(
                        f"Partner API returned {response.status_code} after {attempt} attempts",
                        source_system="PARTNER",
                        attempt=attempt,
                    )
                time.sleep(min(2**attempt, 20))
                continue

            if response.status_code >= 400:
                # NOT retryable: the request is wrong, and it will be wrong
                # again in two minutes.
                raise ContractError(
                    f"Partner API rejected the request with {response.status_code}",
                    entity="partner_cdr",
                    actual=response.text[:500],
                )

            return dict(response.json())

        raise TransientError("Partner API retry budget exhausted", source_system="PARTNER")

    def fetch(self, date_from: datetime, date_to: datetime) -> Iterator[dict[str, Any]]:
        import httpx

        headers = {"Authorization": f"Token {self.token}"} if self.token else {}
        offset = 0
        with httpx.Client(
            base_url=self.base_url, headers=headers, timeout=self.timeout_seconds
        ) as client:
            for page in range(self.max_pages):
                body = self._get_page(
                    client,
                    {
                        "date_from": date_from.isoformat().replace("+00:00", "Z"),
                        "date_to": date_to.isoformat().replace("+00:00", "Z"),
                        "offset": offset,
                        "limit": self.page_size,
                    },
                )
                records = body.get("data", [])
                yield from records

                total = int(body.get("total", 0))
                offset += len(records)
                # Stop on a short page as well as on reaching `total`. The mock
                # API deliberately returns pages whose length disagrees with
                # its own `total`, and trusting `total` alone would loop for
                # ever against a server that is simply wrong about itself.
                if not records or offset >= total or len(records) < self.page_size:
                    log.info(
                        "partner_api_pagination_completed",
                        pages=page + 1,
                        records=offset,
                        reported_total=total,
                    )
                    return
            log.warning("partner_api_max_pages_reached", max_pages=self.max_pages)


def build_partner_client(landing_root: Path | None = None) -> PartnerCdrClient:
    """Return the client selected by ``VOLTHIVE_PARTNER_MODE``."""
    settings = get_settings()
    root = landing_root or (settings.data_dir / "landing")
    if settings.partner_mode == "http":
        api_config = load_yaml_config("sources.yml").get("partner_api", {})
        return HttpPartnerCdrClient(
            settings.partner_api_url or "",
            settings.partner_api_token.get_secret_value() if settings.partner_api_token else None,
            base_path=api_config.get("base_path", "/ocpi/2.2/cdrs"),
            page_size=int(api_config.get("page_size", 500)),
            max_pages=int(api_config.get("max_pages", 200)),
            timeout_seconds=float(api_config.get("request_timeout_seconds", 15)),
            max_retries=int(api_config.get("max_retries", 4)),
        )
    return FilePartnerCdrClient(root)


def ingest_partner_cdrs(
    conn: psycopg.Connection,
    *,
    run_id: str,
    data_interval_start: datetime,
    data_interval_end: datetime,
    dag_id: str | None = None,
    task_id: str | None = None,
    client: PartnerCdrClient | None = None,
    landing_root: Path | None = None,
    lookback_override: timedelta | None = None,
) -> LoadStat:
    """Extract roaming partner records for the cursor window and land them.

    The seven-day lookback on this source is not a copy-paste of the file
    sources' three days: the partner revises records for up to a week while
    billing disputes settle, so the window has to be wide enough to catch a
    restatement. Lookback is a property of how a SOURCE behaves.
    """
    started = time.monotonic()
    watermark = read_watermark(conn, "PARTNER", "partner_cdr")
    window = compute_window(
        watermark,
        data_interval_start=data_interval_start,
        data_interval_end=data_interval_end,
        lookback_override=lookback_override,
    )
    active_client = client or build_partner_client(landing_root)

    max_updated: datetime | None = None
    row_count = 0

    def rows() -> Iterator[tuple[Any, ...]]:
        nonlocal max_updated, row_count
        for sequence, record in enumerate(
            active_client.fetch(window.lower_exclusive, window.upper_inclusive), start=1
        ):
            updated = _parse_instant(record["last_updated"])
            if max_updated is None or updated > max_updated:
                max_updated = updated
            row_count += 1
            line = json.dumps(record, sort_keys=True)
            yield (
                run_id,
                "PARTNER",
                f"partner_api:offset={sequence}",
                sequence,
                window.batch_key,
                record.get("cdr_id"),
                record.get("partner_code"),
                record.get("last_updated"),
                False,
                line,
                payload_hash(line),
            )

    log.info(
        "partner_extract_started",
        window_lower=str(window.lower_exclusive),
        window_upper=str(window.upper_inclusive),
        lookback=str(window.lookback),
        mode=get_settings().partner_mode,
    )

    with transaction(conn):
        inserted = copy_rows(
            conn,
            "raw.partner_cdr",
            [
                "dw_run_id",
                "dw_source_system",
                "dw_source_file",
                "dw_source_row_seq",
                "dw_batch_key",
                "cdr_id",
                "partner_code",
                "last_updated_txt",
                "is_requeued",
                "payload",
                "payload_hash",
            ],
            rows(),
            log_context={"source_system": "PARTNER"},
        )
        stat = LoadStat(
            task_id=task_id or "ingest_partner_cdrs",
            dag_id=dag_id,
            pipeline_run_id=run_id,
            source_system="PARTNER",
            entity="partner_cdr",
            target_table="raw.partner_cdr",
            dw_batch_key=window.batch_key,
            rows_read=inserted,
            rows_inserted=inserted,
            duration_ms=int((time.monotonic() - started) * 1000),
        )
        write_load_stat(conn, stat)
        advance_watermark(
            conn,
            "PARTNER",
            "partner_cdr",
            new_value=max_updated,
            upper_bound=window.upper_inclusive,
            run_id=run_id,
        )

    log.info("partner_extract_completed", rows_inserted=inserted, duration_ms=stat.duration_ms)
    return stat


_ = timezone  # re-exported implicitly by callers building UTC intervals
