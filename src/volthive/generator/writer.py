"""Write generated data to its destinations: the CMS database and the landing
zone.

Two destinations, two shapes, because the sources really are heterogeneous:

* **The CMS database** receives master data through ``COPY``, as
  ``cms_owner``. Both the current-state tables and the ``_history`` tables that
  stand in for a change-data feed.
* **The landing zone** receives partitioned files - JSONL for charge detail
  records, gzipped CSV for meter telemetry, JSONL for the roaming partner
  dataset the file-mode client reads.

**Byte-level determinism is a requirement here, not a nicety.** Two people
running this project must get identical warehouses, and a test asserts it by
comparing file hashes. Three things would break that if left to defaults:

1. ``gzip`` writes the current time into its header, so every regeneration
   would differ. Written with ``mtime=0``.
2. JSON key order follows insertion order, so dict construction order would
   leak into the bytes. Written with ``sort_keys=True``.
3. Line endings differ by platform. Written explicitly as ``\\n``.

Each of those is a one-line fix and each would otherwise produce a test that
fails on someone else's machine for reasons that take an afternoon to find.
"""

from __future__ import annotations

import csv
import gzip
import io
import json
from datetime import date
from pathlib import Path
from typing import Any

import psycopg

from volthive.db.bulk import copy_rows
from volthive.logging_setup import get_logger

__all__ = [
    "MARKER_FILENAME",
    "apply_cms_schema",
    "write_cdr_files",
    "write_marker",
    "write_meter_file",
    "write_partner_files",
    "write_master_data",
]

log = get_logger(__name__)

MARKER_FILENAME = "GENERATED_SYNTHETIC_DATA.txt"

_MARKER_TEXT = """\
ALL DATA IN THIS DIRECTORY IS SYNTHETIC.

It was produced by the deterministic generator in src/volthive/generator/ from
a fixed seed. VoltHive Energy Pvt. Ltd. is a fictional company. No real
customer, vehicle, station or company data is used anywhere in this project.

Email addresses use the RFC 2606 reserved .invalid top-level domain, which can
never resolve. Phone numbers use a reserved prefix. Grid tariff values are
illustrative and are not audited real tariffs.

Regenerate with:  make generate
"""

_METER_COLUMNS = [
    "sample_id",
    "transaction_id",
    "charge_point_id",
    "sample_timestamp",
    "energy_register_wh",
    "power_kw",
    "soc_percent",
    "voltage_v",
    "current_a",
    "temperature_c",
]

#: Columns written to each CMS table, in order. Explicit rather than derived
#: from the dicts, because the generator carries private ``_``-prefixed keys
#: (city code, version number) that are working state and must not leak into
#: the simulated source system.
CMS_COLUMNS: dict[str, list[str]] = {
    "customers": [
        "customer_id",
        "full_name",
        "email",
        "phone",
        "city",
        "state",
        "customer_segment",
        "subscription_plan",
        "kyc_status",
        "signup_date",
        "is_active",
        "created_at",
        "updated_at",
    ],
    "vehicles": [
        "vehicle_id",
        "customer_id",
        "make",
        "model",
        "model_year",
        "battery_capacity_kwh",
        "connector_type",
        "registration_state",
        "created_at",
        "updated_at",
    ],
    "stations": [
        "station_id",
        "station_name",
        "address_line",
        "city",
        "state",
        "pincode",
        "latitude",
        "longitude",
        "site_type",
        "commissioned_date",
        "num_bays",
        "operator_name",
        "is_active",
        "created_at",
        "updated_at",
    ],
    "charge_points": [
        "charge_point_id",
        "station_id",
        "oem_vendor",
        "model",
        "current_type",
        "rated_power_kw",
        "connector_type",
        "tariff_plan_id",
        "firmware_version",
        "status",
        "commissioned_date",
        "created_at",
        "updated_at",
    ],
    "tariff_plans": [
        "tariff_plan_id",
        "plan_name",
        "price_per_kwh_inr",
        "price_per_minute_inr",
        "idle_fee_per_minute_inr",
        "min_billable_kwh",
        "gst_rate_pct",
        "valid_from_date",
        "is_active",
        "created_at",
        "updated_at",
    ],
}


def apply_cms_schema(conn: psycopg.Connection) -> None:
    """Create the simulated source schema, then empty it.

    TRUNCATE rather than DROP: the tables are owned by ``cms_owner`` and the
    ``ALTER DEFAULT PRIVILEGES`` grant that lets ``cms_reader`` see them was
    applied at database bootstrap. Dropping and recreating would work, but
    truncating keeps the privilege story simple and makes regeneration fast.
    """
    from volthive.db.sqlfiles import load_sql, split_statements

    with conn.cursor() as cur:
        for statement in split_statements(load_sql("source/00_cms_ddl.sql")):
            cur.execute(statement)
        for entity in CMS_COLUMNS:
            cur.execute(f"TRUNCATE TABLE {entity}, {entity}_history")


def write_master_data(
    conn: psycopg.Connection,
    entity: str,
    versions: list[dict[str, Any]],
    current: list[dict[str, Any]],
) -> tuple[int, int]:
    """Load one entity's history and current state into the CMS.

    Returns ``(history_rows, current_rows)``.
    """
    columns = CMS_COLUMNS[entity]

    def project(rows: list[dict[str, Any]]) -> list[tuple[Any, ...]]:
        return [tuple(row.get(column) for column in columns) for row in rows]

    history_rows = copy_rows(
        conn,
        f"public.{entity}_history",
        columns,
        project(versions),
        log_context={"entity": entity},
    )
    current_rows = copy_rows(
        conn,
        f"public.{entity}",
        columns,
        project(current),
        log_context={"entity": entity},
    )
    return history_rows, current_rows


def write_marker(data_dir: Path) -> Path:
    """Write the synthetic-data marker into the landing zone.

    Present so that anyone who stumbles on these files - in a container, in a
    backup, in a screenshot - can tell immediately that they are not real
    records about real people. Costs nothing; removes any ambiguity.
    """
    data_dir.mkdir(parents=True, exist_ok=True)
    path = data_dir / MARKER_FILENAME
    path.write_text(_MARKER_TEXT, encoding="utf-8")
    return path


def _write_jsonl(path: Path, records: list[dict[str, Any]]) -> int:
    """Write records as newline-delimited JSON, deterministically."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="\n") as handle:
        for record in records:
            payload = {k: v for k, v in record.items() if not k.startswith("_")}
            handle.write(json.dumps(payload, sort_keys=True, default=str))
            handle.write("\n")
    return len(records)


def write_cdr_files(
    landing_dir: Path, day: date, cdrs_by_city: dict[str, list[dict[str, Any]]]
) -> list[Path]:
    """Write one charge-detail-record file per city for one day.

    ``data/landing/cdr/dt=YYYY-MM-DD/city=XXX/cdr_XXX_YYYYMMDD.jsonl``

    Hive-style ``key=value`` partition directories, which is what a real
    landing zone on object storage looks like and what makes the partition
    scan in the ingestion layer a directory glob rather than a filename parse.
    """
    written: list[Path] = []
    for city_code in sorted(cdrs_by_city):
        path = (
            landing_dir
            / "cdr"
            / f"dt={day.isoformat()}"
            / f"city={city_code}"
            / f"cdr_{city_code}_{day.strftime('%Y%m%d')}.jsonl"
        )
        _write_jsonl(path, cdrs_by_city[city_code])
        written.append(path)
    return written


def write_meter_file(landing_dir: Path, day: date, samples: list[dict[str, Any]]) -> Path:
    """Write one day of meter telemetry as gzipped CSV.

    ``mtime=0`` on the gzip header, because the default writes the current time
    into the file and every regeneration would then differ byte-for-byte -
    breaking the determinism test for a reason that has nothing to do with the
    data.
    """
    path = (
        landing_dir
        / "meter"
        / f"dt={day.isoformat()}"
        / f"meter_values_{day.strftime('%Y%m%d')}.csv.gz"
    )
    path.parent.mkdir(parents=True, exist_ok=True)

    buffer = io.StringIO(newline="")
    writer = csv.DictWriter(buffer, fieldnames=_METER_COLUMNS, lineterminator="\n")
    writer.writeheader()
    for sample in samples:
        writer.writerow({column: sample.get(column) for column in _METER_COLUMNS})

    with path.open("wb") as raw, gzip.GzipFile(fileobj=raw, mode="wb", mtime=0, filename="") as gz:
        gz.write(buffer.getvalue().encode("utf-8"))
    return path


def write_partner_files(
    landing_dir: Path, records_by_day: dict[date, list[dict[str, Any]]]
) -> list[Path]:
    """Write the roaming-partner dataset, partitioned by ``last_updated`` date.

    Partitioned on ``last_updated`` rather than on session date, because that
    is the column the cursor extract advances on. A record revised five days
    after the session lands in the LATER partition, which is precisely what
    makes the seven-day lookback necessary and what the lookback test exercises.
    """
    written: list[Path] = []
    for day in sorted(records_by_day):
        path = (
            landing_dir
            / "partner"
            / f"dt={day.isoformat()}"
            / f"partner_cdrs_{day.strftime('%Y%m%d')}.jsonl"
        )
        records = sorted(records_by_day[day], key=lambda r: (r["last_updated"], r["cdr_id"]))
        _write_jsonl(path, records)
        written.append(path)
    return written
