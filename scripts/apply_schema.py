#!/usr/bin/env python
"""Apply the warehouse schema and its seed data.

    python scripts/apply_schema.py            # DDL + seeds + data-quality rules
    python scripts/apply_schema.py --ddl-only
    python scripts/apply_schema.py --check    # verify, change nothing

Everything this runs is idempotent, so it is safe on every ``make up``. That is
not a convenience: the whole platform is built on the premise that re-running
is the normal case, and the schema bootstrap has to hold to the same standard
as the loads do.

Order is filename order within each directory - ``00_schemas`` before
``01_ctl`` before ``10_raw`` - because the dependencies between them are real
and encoding them in filenames keeps the order visible in a directory listing.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "src"))

from volthive.db import fetch_value, run_sql_dir, transaction, warehouse_connection
from volthive.logging_setup import configure_logging, get_logger

log = get_logger("apply_schema")

#: Objects whose absence means the schema is not usable. Checked by --check and
#: after an apply, so a partially-applied schema is reported as a failure
#: rather than discovered later as a confusing "relation does not exist".
REQUIRED_OBJECTS = [
    "ctl.watermark",
    "ctl.ingested_file",
    "ctl.source_registry",
    "audit.pipeline_run",
    "audit.load_stat",
    "dq.rule",
    "dq.check_result",
    "raw.ocpp_cdr",
    "stg.session",
    "core.dim_charge_point",
    "core.fact_charging_session",
    "core.fact_meter_interval",
    "mart.mart_station_month_kpi",
]


def verify(conn) -> list[str]:
    """Return the list of required objects that are missing."""
    missing = []
    for qualified in REQUIRED_OBJECTS:
        exists = fetch_value(conn, "SELECT to_regclass(:name) IS NOT NULL", {"name": qualified})
        if not exists:
            missing.append(qualified)
    return missing


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ddl-only", action="store_true", help="skip the seed data")
    parser.add_argument("--check", action="store_true", help="verify only, change nothing")
    parser.add_argument(
        "--skip-dq-rules",
        action="store_true",
        help="do not sync configs/dq/*.yml into dq.rule",
    )
    args = parser.parse_args()

    configure_logging(json_logs=False)

    with warehouse_connection(application_name="apply_schema") as conn:
        if args.check:
            missing = verify(conn)
            if missing:
                log.error("schema_incomplete", missing=missing)
                return 1
            log.info("schema_verified", objects_checked=len(REQUIRED_OBJECTS))
            return 0

        # DDL runs statement-by-statement outside one big transaction: several
        # statements (CREATE INDEX on a partitioned parent, and the partition
        # loop) are cheaper without holding a single long-lived lock, and each
        # file is independently idempotent so a partial apply is recoverable by
        # simply running it again.
        for path, _ in run_sql_dir(conn, "ddl"):
            log.info("ddl_applied", file=path)

        if not args.ddl_only:
            # Seeds DO run in one transaction: dimension unknown members and
            # the watermark rows are a consistent starting state, and a
            # half-seeded warehouse is worse than an unseeded one.
            with transaction(conn):
                for path, rows in run_sql_dir(conn, "seed"):
                    log.info("seed_applied", file=path, rows=rows)

            if not args.skip_dq_rules:
                from volthive.dq.rules import sync_rules_to_database

                synced = sync_rules_to_database(conn)
                log.info("dq_rules_synced", rules=synced)

        missing = verify(conn)
        if missing:
            log.error("schema_incomplete_after_apply", missing=missing)
            return 1

    log.info("schema_ready")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
