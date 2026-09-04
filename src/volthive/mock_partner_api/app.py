"""A local stand-in for the roaming partner's OCPI API.

About 150 lines of FastAPI serving the same deterministic dataset the file
client reads, so that ``VOLTHIVE_PARTNER_MODE=http`` and
``VOLTHIVE_PARTNER_MODE=file`` return identical records. Running it is
optional: ``docker compose --profile with-api up``.

**It misbehaves on purpose.** Rates come from the environment and default to
zero, so ordinary use and CI are deterministic; the resilience test turns them
up and asserts the client copes:

===========================  =====================================
 ``MOCK_API_FAIL_503_PCT``    service unavailable
 ``MOCK_API_FAIL_429_PCT``    rate limited, WITH a ``Retry-After``
 ``MOCK_API_TRUNCATE_PCT``    a page whose length disagrees with
                              its own reported ``total``
 ``MOCK_API_SLOW_PCT``        an eight-second response
===========================  =====================================

Retry logic that has never actually met a 503 is decoration. The truncated
page is the subtlest of the four and the most valuable: a client that trusts
``total`` and ignores the number of rows it actually received will loop for
ever against a server that is merely wrong about itself.

Failures are seeded from the request parameters rather than drawn at random, so
the same request fails the same way every time. A flaky mock produces a flaky
test suite, which is worse than no mock at all.
"""

from __future__ import annotations

import hashlib
import json
import os
import time
from datetime import date, datetime
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import JSONResponse

__all__ = ["app", "create_app", "load_dataset"]


def _rate(name: str) -> float:
    try:
        return float(os.environ.get(name, "0")) / 100.0
    except ValueError:
        return 0.0


def _deterministic_roll(*parts: Any) -> float:
    """A stable pseudo-random value in [0, 1) derived from the request.

    Deterministic rather than random so a given request always behaves the same
    way. That is what makes a resilience test assert an exact number of retries
    instead of "roughly this many, usually".
    """
    digest = hashlib.sha256("|".join(str(p) for p in parts).encode()).hexdigest()
    return int(digest[:8], 16) / 0xFFFFFFFF


def load_dataset(landing_root: Path) -> list[dict[str, Any]]:
    """Read every partner record from the landing zone, sorted by cursor.

    Sorted by ``(last_updated, cdr_id)``, which is the order pagination must
    walk: an unstable sort would let a record slip between pages, and that is
    exactly the class of bug that leaves one missing row in a million and no
    explanation.
    """
    records: list[dict[str, Any]] = []
    partner_dir = landing_root / "partner"
    if partner_dir.is_dir():
        for path in sorted(partner_dir.rglob("*.jsonl")):
            with path.open("r", encoding="utf-8") as handle:
                records.extend(json.loads(line) for line in handle if line.strip())
    records.sort(key=lambda r: (r["last_updated"], r["cdr_id"]))
    return records


def create_app(landing_root: Path | None = None) -> FastAPI:
    """Build the FastAPI application."""
    root = landing_root or Path(os.environ.get("VOLTHIVE_DATA_DIR", "data")) / "landing"
    application = FastAPI(
        title="VoltHive roaming partner (mock)",
        description=(
            "A local stand-in for a roaming partner's OCPI CDR endpoint. "
            "All data is synthetic. Not a real partner API."
        ),
        version="2.2.0",
    )

    # Loaded once at startup rather than per request: the dataset is static,
    # and re-reading it on every page would make the pagination test measure
    # disk speed instead of pagination.
    cache: dict[str, list[dict[str, Any]]] = {}

    def dataset() -> list[dict[str, Any]]:
        if "records" not in cache:
            cache["records"] = load_dataset(root)
        return cache["records"]

    @application.get("/health")
    def health() -> dict[str, Any]:
        return {"status": "ok", "records": len(dataset())}

    @application.get("/ocpi/2.2/cdrs")
    def list_cdrs(
        request: Request,
        date_from: str = Query(...),
        date_to: str = Query(...),
        offset: int = Query(0, ge=0),
        limit: int = Query(500, ge=1, le=2000),
    ) -> JSONResponse:
        # Authorization is checked but not enforced against a real secret: this
        # is a local mock, and pretending otherwise would be security theatre.
        # It exists so the client's header handling is exercised.
        if "authorization" not in {k.lower() for k in request.headers}:
            raise HTTPException(status_code=401, detail="Missing Authorization header")

        roll = _deterministic_roll(date_from, date_to, offset, limit)

        if roll < _rate("MOCK_API_FAIL_503_PCT"):
            raise HTTPException(status_code=503, detail="Service temporarily unavailable")

        if roll < _rate("MOCK_API_FAIL_503_PCT") + _rate("MOCK_API_FAIL_429_PCT"):
            return JSONResponse(
                status_code=429,
                content={"detail": "Rate limit exceeded"},
                headers={"Retry-After": "2"},
            )

        if roll < _rate("MOCK_API_SLOW_PCT"):
            time.sleep(8)

        try:
            lower = datetime.fromisoformat(date_from.replace("Z", "+00:00"))
            upper = datetime.fromisoformat(date_to.replace("Z", "+00:00"))
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=f"Bad timestamp: {exc}") from exc

        matching = [
            record
            for record in dataset()
            if lower
            < datetime.fromisoformat(record["last_updated"].replace("Z", "+00:00"))
            <= upper
        ]
        page = matching[offset : offset + limit]

        # The truncated page: `total` says one thing, the rows say another. A
        # client that trusts `total` and ignores len(rows) loops for ever here.
        if page and roll < _rate("MOCK_API_TRUNCATE_PCT"):
            page = page[: max(1, len(page) // 2)]

        next_offset = offset + len(page)
        return JSONResponse(
            content={
                "data": page,
                "total": len(matching),
                "limit": limit,
                "offset": offset,
                "next": (
                    f"/ocpi/2.2/cdrs?date_from={date_from}&date_to={date_to}"
                    f"&offset={next_offset}&limit={limit}"
                    if next_offset < len(matching)
                    else None
                ),
            }
        )

    return application


app = create_app()

_ = date  # imported for the type of the partition labels this server reads
