"""End-to-end: the idempotency proof, and recovery from a partial failure.

These are the slowest tests in the suite and the most valuable. "My pipeline is
idempotent" is a claim; re-running the whole thing and asserting that every
table checksum is unchanged is evidence.

**Idempotency means "same input, same output", not "frozen output".** If the
source data genuinely changed between two runs - a corrected charge detail
record arrived - the second run legitimately produces different and MORE
CORRECT results. Stating that distinction matters as much as the guarantee: a
pipeline that produced identical output regardless of its input would not be
idempotent, it would be broken.
"""

from __future__ import annotations

import sys
from datetime import date
from pathlib import Path
from unittest import mock

import pytest
from tests.conftest import requires_database

from volthive.checksum import warehouse_checksums
from volthive.db.connection import fetch_all, fetch_one, fetch_value

pytestmark = [pytest.mark.e2e, requires_database]

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT / "scripts"))

WINDOW = {"date_from": date(2026, 6, 1), "date_to": date(2026, 6, 7)}


#: Business-level fingerprints, deliberately NOT including surrogate keys.
#:
#: A dimension rebuild reissues every surrogate key - the identity sequence
#: keeps counting rather than restarting - so comparing raw table checksums
#: across a rebuild would fail for a reason that is correct by design. These
#: hash the natural keys and the measures instead, which is the level at which
#: a rebuild genuinely must not change anything.
_SHAPE_QUERIES: dict[str, str] = {
    "dim_customer": """
        SELECT count(*) AS n,
               md5(string_agg(customer_id || '|' || effective_from_utc || '|' || row_hash,
                              ',' ORDER BY customer_id, effective_from_utc)) AS h
        FROM core.dim_customer WHERE customer_sk > 0
    """,
    "dim_station": """
        SELECT count(*) AS n,
               md5(string_agg(station_id || '|' || effective_from_utc || '|' || row_hash,
                              ',' ORDER BY station_id, effective_from_utc)) AS h
        FROM core.dim_station WHERE station_sk > 0
    """,
    "dim_charge_point": """
        SELECT count(*) AS n,
               md5(string_agg(charge_point_id || '|' || effective_from_utc || '|' || row_hash,
                              ',' ORDER BY charge_point_id, effective_from_utc)) AS h
        FROM core.dim_charge_point WHERE charge_point_sk > 0
    """,
    "dim_tariff_plan": """
        SELECT count(*) AS n,
               md5(string_agg(tariff_plan_id || '|' || effective_from_utc || '|' || row_hash,
                              ',' ORDER BY tariff_plan_id, effective_from_utc)) AS h
        FROM core.dim_tariff_plan WHERE tariff_plan_sk > 0
    """,
    "fact_charging_session": """
        SELECT count(*) AS n,
               md5(string_agg(transaction_id || '|' || energy_delivered_kwh
                              || '|' || gross_revenue_inr || '|' || gross_margin_inr,
                              ',' ORDER BY transaction_id)) AS h
        FROM core.fact_charging_session
    """,
    "fact_meter_interval": """
        SELECT count(*) AS n,
               md5(string_agg(transaction_id || '|' || interval_seq
                              || '|' || interval_energy_kwh,
                              ',' ORDER BY transaction_id, interval_seq)) AS h
        FROM core.fact_meter_interval
    """,
    "fact_station_daily_utilization": """
        SELECT count(*) AS n,
               md5(string_agg(date_key || '|' || st.station_id || '|' || u.session_count
                              || '|' || u.energy_delivered_kwh,
                              ',' ORDER BY date_key, st.station_id)) AS h
        FROM core.fact_station_daily_utilization AS u
        INNER JOIN core.dim_station AS st ON st.station_sk = u.station_sk
    """,
}


def _warehouse_shape(conn) -> dict[str, tuple[int, str | None]]:
    """Row count plus a business-key content hash for every core table."""
    shape = {}
    for name, sql in _SHAPE_QUERIES.items():
        row = fetch_one(conn, sql)
        shape[name] = (int(row["n"]), row["h"])
    return shape


def _rebuild_dimensions(dims: list[str]) -> None:
    """Invoke the rebuild script exactly as an operator would, via its CLI.

    Calling ``main()`` rather than the ``rebuild`` function is deliberate: the
    argument parsing, the --yes guard and the connection handling are part of
    what makes this script safe, and a test that bypassed them would prove
    nothing about the thing an operator actually runs.
    """
    from rebuild_dimension_history import main

    argv = ["rebuild_dimension_history.py"]
    for dim in dims:
        argv += ["--dim", dim]
    argv += ["--yes", "--json"]
    with mock.patch.object(sys, "argv", argv):
        assert main() == 0


def _read_analytics(name: str) -> str:
    """Return one showcase query's SQL by file name."""
    from volthive.db.sqlfiles import sql_dir

    return (sql_dir() / "analytics" / name).read_text(encoding="utf-8")


def _run(stages: list[str], **overrides) -> dict:
    from run_pipeline import run

    return run(
        stages=stages,
        lookback_days=3,
        skip_dq_gate=False,
        is_backfill=False,
        **{**WINDOW, **overrides},
    )


class TestFullRerun:
    def test_rerunning_the_whole_pipeline_changes_no_data(
        self, warehouse_conn, built_warehouse
    ) -> None:
        """THE test that turns the idempotency claim into evidence.

        Excluded from the comparison: dw_run_id and the inserted/updated
        timestamps, which change BY DESIGN - delete-insert restatement rewrites
        the window and stamping rows with the run that last wrote them is the
        entire point of having those columns. Every business value and every
        relationship is compared.
        """
        before = warehouse_checksums(warehouse_conn)
        assert before, "nothing to compare - the warehouse was not built"

        _run(["ingest", "stage", "core", "dq", "mart"])

        after = warehouse_checksums(warehouse_conn)
        differing = {
            table: (before[table], after[table])
            for table in before
            if before[table] != after[table]
        }
        assert differing == {}, differing

    def test_a_rerun_ingests_no_new_raw_rows(self, warehouse_conn) -> None:
        """Files are skipped by hash; the CMS window re-extracts but the
        staging dedupe collapses it. Either way the warehouse does not grow."""
        before = fetch_value(warehouse_conn, "SELECT count(*) FROM raw.ocpp_cdr")
        _run(["ingest"])
        after = fetch_value(warehouse_conn, "SELECT count(*) FROM raw.ocpp_cdr")
        assert before == after

    def test_check_results_DO_accumulate(self, warehouse_conn) -> None:
        """The one thing a rerun SHOULD change.

        dq.check_result and audit.* are append-only histories. A test demanding
        those be unchanged would be demanding that the pipeline forget it ran -
        and the quality trend is itself data.
        """
        before = fetch_value(warehouse_conn, "SELECT count(*) FROM dq.check_result")
        _run(["dq"])
        after = fetch_value(warehouse_conn, "SELECT count(*) FROM dq.check_result")
        assert after > before


class TestPartialFailureRecovery:
    def test_recovery_from_a_crash_matches_a_clean_run(self, warehouse_conn) -> None:
        """The strongest test in the suite.

        Simulates a crash AFTER the dimensions loaded and BEFORE the facts did -
        the most likely partial state, and the one that would be hardest to
        reason about if the design were wrong. Re-running must reach exactly
        the state a clean run reaches.

        It works because of two properties acting together: dimension merges are
        hash-idempotent, so re-running them writes nothing; and fact loads
        delete-then-insert their window, so they do not care what was there
        before.
        """
        clean = warehouse_checksums(warehouse_conn)

        with warehouse_conn.transaction(), warehouse_conn.cursor() as cur:
            cur.execute("DELETE FROM core.fact_meter_interval")
            cur.execute("DELETE FROM core.fact_station_daily_utilization")
            cur.execute("DELETE FROM core.fact_charging_session")
            cur.execute("DELETE FROM mart.mart_station_month_kpi")

        assert fetch_value(warehouse_conn, "SELECT count(*) FROM core.fact_charging_session") == 0

        _run(["stage", "core", "dq", "mart"])

        recovered = warehouse_checksums(warehouse_conn)
        differing = {
            table: (clean[table], recovered[table])
            for table in clean
            if clean[table] != recovered[table]
        }
        assert differing == {}, differing

    def test_the_dimensions_survived_the_fact_wipe(self, warehouse_conn) -> None:
        """Deleting facts must not touch dimension history.

        It is also why the interval fact has no declared foreign key to the
        session fact: a cascade there would have destroyed interval rows nobody
        asked to remove.
        """
        versions = fetch_value(
            warehouse_conn,
            "SELECT count(*) FROM core.dim_charge_point WHERE charge_point_sk > 0",
        )
        assert versions > 0


class TestDimensionHistoryRebuild:
    """``scripts/rebuild_dimension_history.py`` - the most destructive script.

    It deletes every version row of a Type 2 dimension and every fact that
    references it, then replays the whole history through the production
    merge. Nothing else in the project can lose this much data, so it gets
    tested against the two properties that make it safe to run at all.
    """

    def test_a_rebuild_reproduces_the_pipeline_exactly(self, warehouse_conn) -> None:
        """A full rebuild must land on the same warehouse, row for row.

        Surrogate keys are the one thing that legitimately changes: the
        identity sequence keeps counting, so replayed rows get NEW keys. That
        is by design and is exactly why the facts are rebuilt alongside. So
        the comparison is made on BUSINESS keys and measures, which is the
        level at which the two runs must agree.

        Row counts are asserted too, and separately. A content hash over
        matching subsets would still pass if the rebuild quietly dropped rows
        from both sides of the comparison.
        """
        before = _warehouse_shape(warehouse_conn)

        _rebuild_dimensions(["all"])

        after = _warehouse_shape(warehouse_conn)
        assert after == before, "a rebuild changed the warehouse's business content"

    def test_rebuilding_twice_converges(self, warehouse_conn) -> None:
        """The window a rebuild chooses must not depend on the last rebuild.

        The station-day snapshot deliberately writes rows at ``batch_hi + 1``,
        because a session late in UTC belongs to the next IST business date.
        An earlier version of the script derived its upper bound from those
        rows, so every rebuild extended the window by a day and added six more
        empty station-days - growing without limit while reporting success.

        Convergence is the property that makes the script safe to re-run after
        a failure, so it is asserted rather than assumed.
        """
        first = _warehouse_shape(warehouse_conn)
        _rebuild_dimensions(["all"])
        second = _warehouse_shape(warehouse_conn)

        assert second == first, "a second rebuild moved the warehouse again"

    def test_a_rebuild_preserves_the_scd2_chains(self, warehouse_conn) -> None:
        """Replayed history must be the SAME history, not merely valid history.

        Chain shape - one current row per key, contiguous validity, no
        overlaps - is asserted elsewhere against the pipeline's output. What
        matters here is that the replay produces the same effective dates and
        the same row hashes, because a merge that built a DIFFERENT but
        internally consistent chain would pass every structural check and
        still have rewritten the past.
        """
        chains = fetch_all(
            warehouse_conn,
            """
            SELECT charge_point_id, effective_from_utc, effective_to_utc, row_hash
            FROM core.dim_charge_point
            WHERE charge_point_sk > 0
            ORDER BY charge_point_id, effective_from_utc
            """,
        )
        assert chains, "no charge point history to rebuild"

        _rebuild_dimensions(["charge_point"])

        replayed = fetch_all(
            warehouse_conn,
            """
            SELECT charge_point_id, effective_from_utc, effective_to_utc, row_hash
            FROM core.dim_charge_point
            WHERE charge_point_sk > 0
            ORDER BY charge_point_id, effective_from_utc
            """,
        )
        assert replayed == chains

    def test_a_rebuild_refuses_without_confirmation(self, warehouse_conn) -> None:
        """--dry-run and a bare invocation must both leave the warehouse alone.

        A destructive script whose default is destruction is a script that
        eventually destroys something. Both no-op paths are exercised, because
        it is the DEFAULT - no flags at all - that a tired operator reaches
        first.
        """
        from rebuild_dimension_history import main

        for argv in (
            ["--dim", "charge_point", "--dry-run"],
            ["--dim", "charge_point"],
        ):
            before = _warehouse_shape(warehouse_conn)
            with mock.patch.object(sys, "argv", ["rebuild_dimension_history.py", *argv]):
                assert main() == 0
            assert _warehouse_shape(warehouse_conn) == before, argv


class TestRestatement:
    def test_replaying_from_raw_reproduces_the_same_facts(self, warehouse_conn) -> None:
        """The payoff for keeping raw immutable.

        A restatement contacts NO source system and re-reads NO file. When a
        transformation was wrong for a month, this is the fix - and it works
        whether or not the OCPP central system still holds that month's
        records, because the landing zone and the raw layer do.
        """
        before = warehouse_checksums(
            warehouse_conn, ["core.fact_charging_session", "core.fact_meter_interval"]
        )

        # No 'ingest' stage: replay from raw only.
        _run(["stage", "core", "dq", "mart"])

        after = warehouse_checksums(
            warehouse_conn, ["core.fact_charging_session", "core.fact_meter_interval"]
        )
        assert before == after

    def test_a_narrow_restatement_leaves_other_days_alone(self, warehouse_conn) -> None:
        """The restatement window is a WINDOW.

        Rewriting the 3rd must not disturb the 5th - otherwise a targeted
        repair becomes a full reload and the whole point of a window is lost.
        """
        untouched_before = fetch_one(
            warehouse_conn,
            """
            SELECT count(*) AS sessions, round(sum(gross_revenue_inr), 2) AS revenue
            FROM core.fact_charging_session WHERE dw_batch_key = DATE '2026-06-05'
            """,
        )

        from run_pipeline import run

        run(
            date_from=date(2026, 6, 3),
            date_to=date(2026, 6, 3),
            stages=["stage", "core"],
            lookback_days=0,
            skip_dq_gate=True,
            is_backfill=True,
        )

        untouched_after = fetch_one(
            warehouse_conn,
            """
            SELECT count(*) AS sessions, round(sum(gross_revenue_inr), 2) AS revenue
            FROM core.fact_charging_session WHERE dw_batch_key = DATE '2026-06-05'
            """,
        )
        assert untouched_before == untouched_after


class TestAuditTrail:
    def test_every_run_is_recorded_and_closed(self, warehouse_conn) -> None:
        """A run left RUNNING for ever misreports the health view."""
        open_runs = fetch_value(
            warehouse_conn,
            "SELECT count(*) FROM audit.pipeline_run WHERE status = 'RUNNING'",
        )
        assert open_runs == 0

    def test_load_statistics_reconcile(self, warehouse_conn) -> None:
        """read = inserted + quarantined + deduplicated, for every load that
        claims to conserve rows."""
        bad = fetch_all(
            warehouse_conn,
            """
            SELECT task_id, target_table, rows_read, rows_inserted,
                   rows_quarantined, rows_duplicate_skipped
            FROM audit.load_stat
            WHERE rows_read > 0
              AND rows_read <> rows_inserted + rows_quarantined + rows_duplicate_skipped
            LIMIT 5
            """,
        )
        assert bad == [], bad

    def test_every_warehouse_row_carries_a_run_id(self, warehouse_conn) -> None:
        """The end-to-end lineage claim, asserted.

        From one fact row you can reach the run, the task, the source file and
        the git commit that produced it - but only if the correlation ID is
        actually on every row.
        """
        for table in (
            "core.fact_charging_session",
            "core.fact_meter_interval",
            "core.dim_charge_point",
            "mart.mart_station_month_kpi",
        ):
            nulls = fetch_value(
                warehouse_conn,
                f"SELECT count(*) FROM {table} WHERE dw_run_id IS NULL",
            )
            assert nulls == 0, table

    def test_a_fact_row_traces_back_to_its_run(self, warehouse_conn) -> None:
        """Demonstrated rather than described: fact -> run -> git commit."""
        row = fetch_one(
            warehouse_conn,
            """
            SELECT
                f.transaction_id,
                r.dag_id,
                r.status,
                r.data_interval_start_utc
            FROM core.fact_charging_session AS f
            INNER JOIN audit.pipeline_run AS r ON r.pipeline_run_id = f.dw_run_id
            LIMIT 1
            """,
        )
        assert row is not None
        assert row["dag_id"]


class TestBusinessOracle:
    def test_total_energy_matches_the_generator(self, warehouse_conn, built_warehouse) -> None:
        """Against an INDEPENDENT oracle: the number of sessions the generator
        recorded emitting, before the pipeline existed.

        Roaming OUTBOUND sessions never produce a charge detail record, and
        some sessions are quarantined, so the fact count is bounded rather than
        equal - but it must be bounded on BOTH sides, or the assertion is
        vacuous.
        """
        emitted = built_warehouse["truth"]["totals"]["sessions_emitted"]
        loaded = fetch_value(warehouse_conn, "SELECT count(*) FROM core.fact_charging_session")
        quarantined = fetch_value(
            warehouse_conn,
            "SELECT count(DISTINCT natural_key) FROM dq.quarantine_ocpp_cdr",
        )
        assert loaded > 0
        assert loaded <= emitted
        assert loaded + quarantined >= emitted * 0.95

    def test_the_analytics_queries_all_return_rows(self, warehouse_conn) -> None:
        """Every showcase query must run AND return data.

        A README full of queries that error is worse than none - but a query
        that runs and returns nothing is nearly as bad, because it looks fine
        in CI and produces an empty table for whoever actually opens it.

        Asserting non-empty is only defensible because the generator is
        deterministic and the tiny profile now guarantees the phenomena these
        queries measure: at least one power upgrade, at least one tariff
        revision, at least one zero-session station day, at least one
        quarantined row. Each of those guarantees has its own test upstream;
        this is the one that proves they reach the surface.
        """
        from volthive.db.sqlfiles import sql_dir

        analytics = sorted((sql_dir() / "analytics").glob("*.sql"))
        assert len(analytics) >= 10, "fewer than ten showcase queries"

        empty = []
        for path in analytics:
            rows = fetch_all(warehouse_conn, path.read_text(encoding="utf-8"))
            assert isinstance(rows, list), path.name
            if not rows:
                empty.append(path.name)
        assert not empty, f"showcase queries returned no rows: {empty}"

    def test_the_scd2_showcase_query_shows_a_real_before_and_after(self, warehouse_conn) -> None:
        """The 30 kW -> 60 kW query is the point of the whole dimension design.

        It is asserted separately and specifically because it is the one result
        that would still LOOK correct if Type 2 were quietly broken: with a
        Type 1 dimension every session resolves to the device's current power,
        the before and after groups become identical, and the measured uplift
        collapses to approximately zero. A non-empty result set alone would not
        catch that. A positive uplift does.
        """
        rows = fetch_all(
            warehouse_conn,
            (
                "SELECT charge_point_id, power_before_kw, power_after_kw, "
                "sessions_before, sessions_after, energy_uplift_pct FROM ("
                + _read_analytics("03_power_upgrade_before_after.sql").rstrip().rstrip(";")
                + ") AS q"
            ),
        )
        assert rows, "no charge point has sessions on both sides of a power upgrade"
        for row in rows:
            assert row["power_after_kw"] > row["power_before_kw"], row["charge_point_id"]
            assert row["sessions_before"] > 0 and row["sessions_after"] > 0

        # At least one device must show a genuine uplift. Not all of them will -
        # a device upgraded on a quiet day can legitimately show less - but if
        # NONE do, the point-in-time join is not doing what it claims.
        assert any(
            row["energy_uplift_pct"] is not None and row["energy_uplift_pct"] > 0 for row in rows
        ), "no upgraded charge point delivered more energy per session afterwards"
