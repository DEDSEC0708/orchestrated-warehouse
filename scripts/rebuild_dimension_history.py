#!/usr/bin/env python
"""Rebuild one dimension's SCD Type 2 history from scratch, out of ``raw``.

    python scripts/rebuild_dimension_history.py --dim charge_point --dry-run
    python scripts/rebuild_dimension_history.py --dim charge_point --yes
    python scripts/rebuild_dimension_history.py --dim all --yes

THIS IS THE SCRIPT ``restate.py`` AND ``transform/core.py`` BOTH POINT AT, and
it is deliberately the most guarded operation in the project.

WHY IT EXISTS
-------------
A fact backfill is safe: each run's window comes from the Airflow data
interval and the load is a delete-insert over that window, so replaying it
re-resolves keys against the dimension history that already exists.

A dimension backfill is NOT safe, and that is not a bug. The source exposes
current state plus ``updated_at``, so re-running a merge for a past date would
stamp TODAY'S attributes with a HISTORICAL ``effective_from`` and corrupt the
very history the dimension exists to preserve. ``run_dimensions`` therefore
refuses to merge when ``is_backfill`` is set, and points here.

The legitimate reason to rebuild is different: the MERGE ITSELF was wrong.
A hash column list that omitted an attribute, a validity chain built with the
wrong comparison, a Type 1 column that should have been Type 2. In that case
the existing chains are not history worth preserving - they are a bug's
output - and the correct response is to replay every raw version in
``updated_at`` order through the corrected merge.

WHY IT REBUILDS THE FACTS TOO
-----------------------------
Surrogate keys are assigned by an identity sequence. Rebuilding a dimension
issues NEW keys, so every fact row pointing at an old key becomes wrong -
not orphaned, which would at least be detectable, but silently pointing at
somebody else's row.

The database will not let that happen quietly: the facts declare foreign keys
to the dimensions, so deleting dimension rows fails outright until the facts
are gone. That refusal is a feature, and this script works WITH it rather than
disabling it. The facts are deleted and rebuilt in the same transaction.

The honest consequence is that a "surgical" dimension rebuild does not exist.
Rebuilding one dimension costs a full fact and mart rebuild, and this script
says so up front rather than discovering it halfway through.

ATOMICITY
---------
Everything runs inside ONE transaction. Either the warehouse ends up fully
rebuilt or completely untouched; there is no window in which the dimensions
are new and the facts still point at the old keys. On a large warehouse that
is a long transaction holding a lot of locks - which is the real cost of this
operation and the reason it is not something a DAG does on a schedule.
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "src"))

from volthive.db.connection import (
    fetch_all,
    fetch_value,
    transaction,
    warehouse_connection,
)
from volthive.logging_setup import configure_logging, get_logger
from volthive.pipeline import close_run, open_run
from volthive.transform.core import (
    DIMENSION_STEPS,
    FACT_STEPS,
    INFERRED_MEMBER_STEP,
    MART_STEPS,
    run_core_step,
)
from volthive.transform.staging import run_staging

log = get_logger("rebuild_dimension_history")

#: The four Type 2 dimensions, and everything needed to rebuild each one.
#:
#: ``staging_step`` and ``merge_step`` name existing pipeline steps rather than
#: duplicating their SQL. A rebuild that used its own copy of the merge would
#: be rebuilding history with logic that is not the logic in production, which
#: defeats the purpose entirely.
REBUILDABLE: dict[str, dict[str, str]] = {
    "customer": {
        "table": "core.dim_customer",
        "key_column": "customer_sk",
        "business_key": "customer_id",
        "staging_step": "stg_customer",
        "merge_step": "merge_dim_customer",
    },
    "station": {
        "table": "core.dim_station",
        "key_column": "station_sk",
        "business_key": "station_id",
        "staging_step": "stg_station",
        "merge_step": "merge_dim_station",
    },
    "tariff_plan": {
        "table": "core.dim_tariff_plan",
        "key_column": "tariff_plan_sk",
        "business_key": "tariff_plan_id",
        "staging_step": "stg_tariff_plan",
        "merge_step": "merge_dim_tariff_plan",
    },
    "charge_point": {
        "table": "core.dim_charge_point",
        "key_column": "charge_point_sk",
        "business_key": "charge_point_id",
        "staging_step": "stg_charge_point",
        "merge_step": "merge_dim_charge_point",
    },
}

#: Raw tables whose ``dw_batch_key`` range contributes to "all of history".
RAW_HISTORY_TABLES = [
    "raw.cms_customers",
    "raw.cms_stations",
    "raw.cms_charge_points",
    "raw.cms_tariff_plans",
    "raw.ocpp_cdr",
    "raw.meter_value",
    "raw.partner_cdr",
]

#: Fact tables whose existing coverage the rebuild must not shrink.
COVERAGE_TABLES = [
    "core.fact_charging_session",
    "core.fact_meter_interval",
    "core.fact_station_daily_utilization",
]


def full_history_window(conn) -> tuple[date, date]:
    """The widest batch window the warehouse has ever covered.

    A rebuild is not a windowed operation: replaying only part of the history
    would produce a chain whose first version begins wherever the window
    started, which is worse than the bug being fixed.

    The two bounds come from deliberately different places, and swapping
    either one breaks the rebuild in a way that reports success:

    LOWER BOUND - the earliest of raw AND the existing facts.
        Every pipeline run stages from ``date_from - lookback_days``, and the
        dense station-day snapshot writes a row for every station on every
        date in that window, including dates earlier than the first raw batch
        key. Taking raw alone therefore DELETES those rows and never puts them
        back. The first version of this script did exactly that: a rebuild
        turned 66 station-day rows into 48 and exited zero.

    UPPER BOUND - raw only, never the facts.
        ``load_fact_station_daily_utilization`` deliberately covers
        ``batch_hi + 1``, because a session starting late in UTC belongs to
        the next IST business date. Those rows carry
        ``dw_batch_key = batch_hi + 1``, so reading the upper bound back from
        the facts extends the window by a day on EVERY rebuild - six more
        empty station-days each time, growing without limit and never
        converging. Raw is the honest authority for how far the data goes.

    The asymmetry is the point: coverage must never shrink, and must never
    grow on its own either. A rebuild run twice in a row has to produce the
    same window both times, which is what makes it safe to re-run after a
    failure.
    """
    raw_spans = " UNION ALL ".join(
        f"SELECT min(dw_batch_key) AS lo, max(dw_batch_key) AS hi FROM {table}"
        for table in RAW_HISTORY_TABLES
    )
    raw = fetch_all(conn, f"SELECT min(lo) AS lo, max(hi) AS hi FROM ({raw_spans}) AS s")[0]
    if raw["lo"] is None:
        raise SystemExit("raw is empty - there is no history to rebuild from.")

    covered_spans = " UNION ALL ".join(
        f"SELECT min(dw_batch_key) AS lo FROM {table}" for table in COVERAGE_TABLES
    )
    covered_lo = fetch_value(conn, f"SELECT min(lo) FROM ({covered_spans}) AS s")

    lo = min(raw["lo"], covered_lo) if covered_lo is not None else raw["lo"]
    return lo, raw["hi"]


def preview(conn, dims: list[str]) -> dict:
    """Report exactly what would be destroyed and rebuilt, before doing it."""
    report: dict = {"dimensions": {}, "facts": {}}
    for dim in dims:
        spec = REBUILDABLE[dim]
        report["dimensions"][dim] = fetch_all(
            conn,
            f"""
            SELECT
                count(*)                                       AS version_rows,
                count(DISTINCT {spec["business_key"]})         AS business_keys,
                count(*) FILTER (WHERE is_current)             AS current_rows,
                count(*) FILTER (WHERE is_inferred)            AS inferred_rows,
                min(effective_from_utc)                        AS earliest_version,
                max(effective_from_utc) FILTER (WHERE NOT is_current) AS latest_expiry
            FROM {spec["table"]}
            WHERE {spec["key_column"]} > 0
            """,
        )[0]

    for step in FACT_STEPS:
        report["facts"][step.target_table] = int(
            fetch_value(conn, f"SELECT count(*) FROM {step.target_table}") or 0
        )
    return report


def rebuild(conn, dims: list[str], *, run_id: str, lo: date, hi: date) -> dict:
    """Delete and rebuild the selected dimensions, then every fact and mart.

    Ordering is forced by the foreign keys and is not negotiable:

      1. facts out       - they reference the surrogate keys about to vanish
      2. dimension rows out, EXCEPT the -1 / -2 members, which are seed data
         and are referenced by facts in other layers
      3. staging rebuilt over the FULL raw history rather than a window
      4. the production merge replayed over that staging
      5. inferred members recreated
      6. facts rebuilt over the full window, re-resolving every key
      7. mart rebuilt, because it aggregates the facts just replaced
    """
    summary: dict = {"deleted": {}, "rebuilt": {}}

    with transaction(conn):
        # 1. Facts first. Reversed so the dependent interval fact goes before
        #    the session fact it references.
        for step in reversed(FACT_STEPS):
            with conn.cursor() as cur:
                cur.execute(f"DELETE FROM {step.target_table}")
                summary["deleted"][step.target_table] = cur.rowcount

        # 2. Dimension versions, sparing the unknown members. Deleting those
        #    would break every fact in every layer that resolves to -1 or -2,
        #    and they carry no history to rebuild.
        for dim in dims:
            spec = REBUILDABLE[dim]
            with conn.cursor() as cur:
                cur.execute(f"DELETE FROM {spec['table']} WHERE {spec['key_column']} > 0")
                summary["deleted"][spec["table"]] = cur.rowcount

        # 3. Staging over ALL of raw. The staging tables are UNLOGGED scratch
        #    rebuilt on every run, so widening the window costs nothing but
        #    time and is the only way to see every historical version.
        staging_steps = [REBUILDABLE[dim]["staging_step"] for dim in dims]
        staged = run_staging(
            conn,
            run_id=run_id,
            batch_lo=lo,
            batch_hi=hi,
            dag_id="rebuild_dimension_history",
            only=staging_steps,
        )
        summary["rebuilt"]["staging"] = {s.task_id: s.rows_inserted for s in staged}

        # 4. The production merge, unmodified. Its set-based design already
        #    handles N versions per key in a single pass, so replaying the
        #    whole history is one statement rather than a loop over months.
        merge_names = {REBUILDABLE[dim]["merge_step"] for dim in dims}
        for step in DIMENSION_STEPS:
            if step.name in merge_names:
                stat = run_core_step(
                    conn,
                    step,
                    run_id=run_id,
                    batch_lo=lo,
                    batch_hi=hi,
                    dag_id="rebuild_dimension_history",
                )
                summary["rebuilt"][step.name] = stat.rows_inserted

        # 5-7. Inferred members, facts, mart.
        for step in [INFERRED_MEMBER_STEP, *FACT_STEPS, *MART_STEPS]:
            stat = run_core_step(
                conn,
                step,
                run_id=run_id,
                batch_lo=lo,
                batch_hi=hi,
                dag_id="rebuild_dimension_history",
            )
            summary["rebuilt"][step.name] = stat.rows_inserted

    return summary


def main() -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--dim",
        action="append",
        required=True,
        help=f"one of {sorted(REBUILDABLE)}, repeatable, or 'all'",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="report what would be rebuilt and exit without changing anything",
    )
    parser.add_argument(
        "--yes",
        action="store_true",
        help="required to actually rebuild; without it the script only previews",
    )
    parser.add_argument("--json", action="store_true", help="print the summary as JSON only")
    args = parser.parse_args()

    requested = ["all"] if "all" in args.dim else args.dim
    if requested == ["all"]:
        dims = sorted(REBUILDABLE)
    else:
        unknown = sorted(set(requested) - set(REBUILDABLE))
        if unknown:
            parser.error(f"unknown dimension(s) {unknown}; choose from {sorted(REBUILDABLE)}")
        dims = sorted(set(requested))

    configure_logging(json_logs=args.json, level="WARNING" if args.json else "INFO")

    with warehouse_connection(application_name="rebuild_dimension_history") as conn:
        lo, hi = full_history_window(conn)
        before = preview(conn, dims)

    if not args.json:
        print(f"\nRaw history spans {lo} .. {hi}\n")
        print("Dimensions to be REBUILT (every version row deleted and replayed):")
        for dim, stats in before["dimensions"].items():
            print(
                f"  {dim:14s} {stats['version_rows']:>7} versions across "
                f"{stats['business_keys']} keys, {stats['inferred_rows']} inferred"
            )
        print("\nFacts to be DELETED AND REBUILT (surrogate keys are reissued):")
        for table, count in before["facts"].items():
            print(f"  {table:42s} {count:>9} rows")
        print(
            "\nThis is not a surgical operation. Rebuilding a dimension reissues\n"
            "its surrogate keys, so every fact referencing them is rebuilt too.\n"
        )

    if args.dry_run or not args.yes:
        if not args.json:
            print("Nothing was changed. Re-run with --yes to perform the rebuild.\n")
        else:
            print(json.dumps({"preview": before, "applied": False}, indent=2, default=str))
        return 0

    now = datetime.now(tz=UTC)
    run_id = open_run(
        dag_id="rebuild_dimension_history",
        airflow_run_id=f"manual__{now.isoformat()}",
        data_interval_start=datetime.combine(lo, time.min, tzinfo=UTC),
        data_interval_end=datetime.combine(hi + timedelta(days=1), time.min, tzinfo=UTC),
        triggered_by="manual",
    )
    status, error = "SUCCESS", None
    try:
        with warehouse_connection(
            application_name="rebuild_dimension_history",
            # A history replay touches every version of every key. The default
            # per-statement timeout is sized for incremental windows and would
            # abort this halfway through - which is safe, because of the outer
            # transaction, but wastes the whole run.
            statement_timeout_ms=30 * 60 * 1000,
        ) as conn:
            summary = rebuild(conn, dims, run_id=run_id, lo=lo, hi=hi)
            after = preview(conn, dims)
    except Exception as exc:
        status, error = "FAILED", str(exc)
        log.error("rebuild_failed", error=str(exc))
        raise
    finally:
        close_run(
            run_id=run_id,
            dag_id="rebuild_dimension_history",
            status=status,
            error_summary=error,
        )

    result = {"run_id": run_id, "dimensions": dims, "window": f"{lo} .. {hi}", **summary}
    if args.json:
        print(
            json.dumps({"preview": before, "result": result, "after": after}, indent=2, default=str)
        )
    else:
        print(f"\nRebuild complete. run_id={run_id}\n")
        for dim, stats in after["dimensions"].items():
            was = before["dimensions"][dim]["version_rows"]
            print(
                f"  {dim:14s} {was:>7} versions -> {stats['version_rows']:>7} "
                f"({stats['current_rows']} current)"
            )
        print()
        for table, count in after["facts"].items():
            print(f"  {table:42s} {count:>9} rows")
        print(
            "\nRun the data-quality report before publishing anything downstream:\n"
            "  python scripts/dq_report.py\n"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
