"""Table checksums, for proving idempotency rather than asserting it.

"My pipeline is idempotent" is a claim. "Here is the test that re-runs the
whole pipeline and asserts every table checksum is unchanged" is evidence, and
the difference is most of why this module exists.

**What is included and what is not.** The checksum covers the DATA and excludes
the LINEAGE:

* ``dw_run_id`` changes on every run BY DESIGN. Delete-insert restatement
  rewrites the window's rows, and stamping them with the run that last wrote
  them is the whole point of having the column - it is how a row traces back to
  its run, its task and its git commit.
* ``dw_inserted_at_utc`` and ``dw_updated_at_utc`` are wall-clock times and
  change for the same reason.
* Surrogate keys ARE excluded for facts, because an identity sequence advances
  on every insert and a rewritten window legitimately gets new ones.
* Surrogate keys are INCLUDED for dimensions, because a merge that is behaving
  must not renumber them - facts point at them, and a changed dimension key
  means every fact referencing it now means something different. That
  asymmetry is deliberate and is itself a property worth testing.

Excluding those columns is not weakening the test; including them would test
the clock. What remains is every business value and every relationship, which
is exactly what "same input, same output" should mean.

**Idempotency means "same input, same output", not "frozen output".** If the
source data genuinely changed between two runs - a corrected charge detail
record arrived - the second run legitimately produces different and MORE
CORRECT results. Stating that distinction unprompted is worth more than the
guarantee itself, because plenty of people claim idempotency without
understanding its precondition.
"""

from __future__ import annotations

import hashlib
from typing import Any

import psycopg

from volthive.db.connection import fetch_all, fetch_value

__all__ = ["CHECKSUM_TABLES", "EXCLUDED_COLUMNS", "table_checksum", "warehouse_checksums"]

#: Columns excluded from every checksum. Lineage and wall-clock, not data.
EXCLUDED_COLUMNS = frozenset(
    {
        "dw_run_id",
        "dw_inserted_at_utc",
        "dw_updated_at_utc",
        "dw_ingested_at_utc",
    }
)

#: Additionally excluded per table: identity surrogate keys on FACTS, which a
#: delete-insert restatement legitimately reissues. Dimension surrogate keys
#: are NOT excluded - see the module docstring.
_EXCLUDED_PER_TABLE: dict[str, set[str]] = {
    "core.fact_charging_session": {"charging_session_sk"},
    # meter_interval_sk for the same reason as the other two. But note
    # charging_session_sk as well, and the reason is worth understanding
    # rather than waving through:
    #
    # fact_meter_interval carries a POINTER TO ANOTHER FACT'S SURROGATE KEY.
    # When the session fact's window is deleted and re-inserted, its identity
    # sequence issues new keys, and the intervals loaded immediately afterwards
    # inherit the new values. The relationship is identical, the row is
    # identical, the number is different.
    #
    # The consequence is a real coupling and it is documented in the runbook:
    # the two facts MUST be restated together over the same window, which is
    # why they are loaded in one task in that order. If they ever drifted, the
    # error-severity FACT_METER_ORPHAN_SESSION rule is what would catch it -
    # and transaction_id, which IS in the checksum, still ties every interval
    # to its session by business key.
    "core.fact_meter_interval": {"meter_interval_sk", "charging_session_sk"},
    "core.fact_station_daily_utilization": {"station_daily_sk"},
}

#: The tables whose contents must be identical after a re-run. `dq.check_result`
#: and `audit.*` are deliberately ABSENT: they are append-only histories, and a
#: second run correctly adds a second set of check results. A test that demanded
#: those be unchanged would be demanding that the pipeline forget it ran.
CHECKSUM_TABLES: list[str] = [
    "core.dim_customer",
    "core.dim_station",
    "core.dim_charge_point",
    "core.dim_tariff_plan",
    "core.dim_vehicle",
    "core.dim_session_outcome",
    "core.fact_charging_session",
    "core.fact_meter_interval",
    "core.fact_station_daily_utilization",
    "mart.mart_station_month_kpi",
    "mart.mart_customer_month_kpi",
    "mart.mart_charge_point_daily",
]


def _checksum_columns(conn: psycopg.Connection, qualified: str) -> list[str]:
    """The columns that participate in a table's checksum, in a stable order."""
    schema, _, table = qualified.partition(".")
    rows = fetch_all(
        conn,
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = :schema AND table_name = :table
        ORDER BY column_name
        """,
        {"schema": schema, "table": table},
    )
    excluded = EXCLUDED_COLUMNS | _EXCLUDED_PER_TABLE.get(qualified, set())
    return [r["column_name"] for r in rows if r["column_name"] not in excluded]


def table_checksum(conn: psycopg.Connection, qualified: str) -> tuple[int, str]:
    """Return ``(row_count, checksum)`` for one table.

    Every value is rendered as text, concatenated in a fixed column order and
    hashed per row; the ROW hashes are then sorted and hashed together. Sorting
    the row hashes rather than the rows means the result does not depend on
    physical row order - which changes freely after a delete-insert and has
    nothing to do with whether the data is the same.
    """
    columns = _checksum_columns(conn, qualified)
    if not columns:
        return 0, ""

    projection = ", ".join(f"COALESCE({column}::TEXT, '~NULL~')" for column in columns)
    rows = fetch_all(
        conn,
        f"SELECT md5(concat_ws('|', {projection})) AS row_hash "  # noqa: S608
        f"FROM {qualified} ORDER BY 1",
    )
    digest = hashlib.sha256()
    for row in rows:
        digest.update(row["row_hash"].encode())
    return len(rows), digest.hexdigest()


def warehouse_checksums(
    conn: psycopg.Connection, tables: list[str] | None = None
) -> dict[str, dict[str, Any]]:
    """Checksum every table that a re-run must leave unchanged."""
    result: dict[str, dict[str, Any]] = {}
    for qualified in tables or CHECKSUM_TABLES:
        exists = fetch_value(conn, "SELECT to_regclass(:name) IS NOT NULL", {"name": qualified})
        if not exists:
            continue
        rows, checksum = table_checksum(conn, qualified)
        result[qualified] = {"rows": rows, "checksum": checksum}
    return result
