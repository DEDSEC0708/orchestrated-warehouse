"""SCD Type 2: the ten assertions that make the claim credible.

Nothing here is mocked. The entire value of these tests is whether the SQL is
correct - the merge, the hash comparison, the validity chaining, the
constraints - and mocking PostgreSQL would test the mock.

The last two are the ones that matter most. `test_no_key_has_two_current_rows`
and `test_point_in_time_join_resolves_exactly_one_version` are invariants over
the WHOLE dimension rather than assertions about a fixture, so they catch a
merge that is wrong for one key in ten thousand.
"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from tests.conftest import requires_database

from volthive.db.connection import fetch_all, fetch_one, fetch_value

pytestmark = [pytest.mark.integration, requires_database]

SCD2_DIMENSIONS = [
    ("core.dim_customer", "customer_id", "customer_sk"),
    ("core.dim_station", "station_id", "station_sk"),
    ("core.dim_charge_point", "charge_point_id", "charge_point_sk"),
    ("core.dim_tariff_plan", "tariff_plan_id", "tariff_plan_sk"),
]


class TestVersionChaining:
    def test_first_load_creates_version_one(self, warehouse_conn) -> None:
        bad = fetch_value(
            warehouse_conn,
            """
            SELECT count(*) FROM (
                SELECT tariff_plan_id, min(version_no) AS lowest
                FROM core.dim_tariff_plan WHERE tariff_plan_sk > 0
                GROUP BY tariff_plan_id
            ) AS t WHERE t.lowest <> 1
            """,
        )
        assert bad == 0

    def test_version_numbers_are_contiguous(self, warehouse_conn) -> None:
        """Not required for correctness, but a gap means a version vanished."""
        bad = fetch_value(
            warehouse_conn,
            """
            SELECT count(*) FROM (
                SELECT tariff_plan_id, count(*) AS versions, max(version_no) AS highest
                FROM core.dim_tariff_plan WHERE tariff_plan_sk > 0
                GROUP BY tariff_plan_id
            ) AS t WHERE t.versions <> t.highest
            """,
        )
        assert bad == 0

    @pytest.mark.parametrize(("table", "key", "sk"), SCD2_DIMENSIONS)
    def test_validity_ranges_abut_exactly(
        self, warehouse_conn, table: str, key: str, sk: str
    ) -> None:
        """Half-open intervals: no gaps AND no overlaps.

        Each version's effective_to must equal the next version's
        effective_from. A gap means a fact in between resolves to nothing; an
        overlap means it resolves to two rows and every measure doubles.
        """
        gaps = fetch_value(
            warehouse_conn,
            f"""
            SELECT count(*) FROM (
                SELECT
                    effective_to_utc,
                    lead(effective_from_utc) OVER (
                        PARTITION BY {key} ORDER BY effective_from_utc
                    ) AS next_from
                FROM {table} WHERE {sk} > 0
            ) AS chain
            WHERE chain.next_from IS NOT NULL AND chain.effective_to_utc <> chain.next_from
            """,  # - names come from the parametrised list above
        )
        assert gaps == 0, table

    @pytest.mark.parametrize(("table", "key", "sk"), SCD2_DIMENSIONS)
    def test_no_key_has_two_current_rows(
        self, warehouse_conn, table: str, key: str, sk: str
    ) -> None:
        bad = fetch_value(
            warehouse_conn,
            f"""
            SELECT count(*) FROM (
                SELECT {key} FROM {table} WHERE is_current AND {sk} > 0
                GROUP BY {key} HAVING count(*) > 1
            ) AS t
            """,
        )
        assert bad == 0, table

    @pytest.mark.parametrize(("table", "key", "sk"), SCD2_DIMENSIONS)
    def test_every_key_has_exactly_one_current_row(
        self, warehouse_conn, table: str, key: str, sk: str
    ) -> None:
        """The invariant the partial unique index CANNOT enforce.

        An index prevents two current rows. Nothing prevents ZERO - that is the
        absence of a row - except the merge running in one transaction. This is
        the assertion that proves the transaction boundary held.
        """
        bad = fetch_value(
            warehouse_conn,
            f"""
            SELECT count(*) FROM (
                SELECT {key} FROM {table} WHERE {sk} > 0
                GROUP BY {key} HAVING count(*) FILTER (WHERE is_current) <> 1
            ) AS t
            """,
        )
        assert bad == 0, table

    @pytest.mark.parametrize(("table", "key", "sk"), SCD2_DIMENSIONS)
    def test_no_overlapping_validity_ranges(
        self, warehouse_conn, table: str, key: str, sk: str
    ) -> None:
        overlaps = fetch_value(
            warehouse_conn,
            f"""
            SELECT count(*) FROM {table} AS a
            INNER JOIN {table} AS b
                ON a.{key} = b.{key} AND a.{sk} < b.{sk}
                AND tstzrange(a.effective_from_utc, a.effective_to_utc)
                    && tstzrange(b.effective_from_utc, b.effective_to_utc)
            WHERE a.{sk} > 0
            """,
        )
        assert overlaps == 0, table


class TestChangeDetection:
    def test_a_tracked_attribute_change_creates_a_new_version(self, warehouse_conn) -> None:
        """The generator upgrades charge points from 30 kW to 60 kW.

        If those upgrades produced no second version, SCD2 is doing nothing.
        """
        changed = fetch_value(
            warehouse_conn,
            """
            SELECT count(*) FROM (
                SELECT charge_point_id
                FROM core.dim_charge_point WHERE charge_point_sk > 0
                GROUP BY charge_point_id
                HAVING count(DISTINCT rated_power_kw) > 1
                    OR count(DISTINCT tariff_plan_id) > 1
                    OR count(DISTINCT firmware_version) > 1
            ) AS t
            """,
        )
        assert changed > 0, "no tracked attribute ever changed - SCD2 is untested"

    def test_row_hashes_differ_between_versions_of_a_key(self, warehouse_conn) -> None:
        """Two versions with the same hash means the merge versioned needlessly."""
        identical = fetch_value(
            warehouse_conn,
            """
            SELECT count(*) FROM (
                SELECT charge_point_id, row_hash
                FROM core.dim_charge_point WHERE charge_point_sk > 0
                GROUP BY charge_point_id, row_hash HAVING count(*) > 1
            ) AS t
            """,
        )
        assert identical == 0

    def test_rerunning_the_merge_is_a_no_op(self, warehouse_conn) -> None:
        """Same input, zero writes. The core idempotency property.

        Not merely "no new rows": dw_updated_at_utc must not move either, or a
        rerun would churn the table and generate dead tuples for nothing.
        """
        from volthive.db.sqlfiles import run_sql_file

        before = fetch_one(
            warehouse_conn,
            """
            SELECT count(*) AS versions, max(dw_updated_at_utc) AS newest
            FROM core.dim_charge_point WHERE charge_point_sk > 0
            """,
        )
        with warehouse_conn.transaction():
            run_sql_file(
                warehouse_conn,
                "core/scd2_merge_charge_point.sql",
                {
                    "run_id": "00000000-0000-0000-0000-000000000000",
                    "batch_lo": "2026-05-29",
                    "batch_hi": "2026-06-07",
                },
            )
        after = fetch_one(
            warehouse_conn,
            """
            SELECT count(*) AS versions, max(dw_updated_at_utc) AS newest
            FROM core.dim_charge_point WHERE charge_point_sk > 0
            """,
        )
        assert before == after

    def test_a_rerun_creates_no_false_retro_dated_quarantine_rows(self, warehouse_conn) -> None:
        """The bug this test was written for.

        Historical versions inside the lookback window are re-read on every
        run. Classifying them as retro-dated changes - because they predate the
        current version - fills the quarantine table with hundreds of rows
        describing nothing but the lookback doing its job, and buries the
        genuine retro-dated changes it is meant to surface.
        """
        false_positives = fetch_value(
            warehouse_conn,
            """
            SELECT count(*) FROM dq.quarantine_cms_entity
            WHERE rule_code = 'SCD_RETRO_DATED_CHANGE'
            """,
        )
        assert false_positives == 0


class TestTypeOneBehaviour:
    def test_vehicle_has_exactly_one_row_per_key(self, warehouse_conn) -> None:
        """Type 1 by deliberate decision, not by omission."""
        duplicates = fetch_value(
            warehouse_conn,
            """
            SELECT count(*) FROM (
                SELECT vehicle_id FROM core.dim_vehicle
                GROUP BY vehicle_id HAVING count(*) > 1
            ) AS t
            """,
        )
        assert duplicates == 0

    def test_type_one_columns_are_identical_across_versions(self, warehouse_conn) -> None:
        """A correction is retroactive: it fixes ALL versions of the key.

        plan_name is Type 1 and is deliberately absent from the hash, so
        changing it must update every version rather than create a new one.
        """
        inconsistent = fetch_value(
            warehouse_conn,
            """
            SELECT count(*) FROM (
                SELECT tariff_plan_id FROM core.dim_tariff_plan
                WHERE tariff_plan_sk > 0
                GROUP BY tariff_plan_id HAVING count(DISTINCT plan_name) > 1
            ) AS t
            """,
        )
        assert inconsistent == 0


class TestInferredMembers:
    def test_inferred_members_exist(self, warehouse_conn) -> None:
        """The generator emits sessions on devices the CMS has never reported."""
        inferred = fetch_value(
            warehouse_conn,
            "SELECT count(*) FROM core.dim_charge_point WHERE is_inferred",
        )
        assert inferred > 0

    def test_inferred_members_are_open_ended(self, warehouse_conn) -> None:
        """Validity starts at the beginning of time, so any fact resolves to them.

        Without it, a session that occurred before the placeholder was created
        would fall through to the UNKNOWN member and lose its device entirely.
        """
        bad = fetch_value(
            warehouse_conn,
            """
            SELECT count(*) FROM core.dim_charge_point
            WHERE is_inferred AND effective_from_utc <> TIMESTAMPTZ '0001-01-01 00:00:00+00'
            """,
        )
        assert bad == 0

    def test_promotion_keeps_the_surrogate_key(self, warehouse_conn) -> None:
        """The property that makes inferred members worth having.

        When the real record arrives, its attributes are filled in ON THE SAME
        surrogate key, so every fact already pointing at it becomes correct
        with no fact rewrite at all.

        The whole thing runs inside a transaction that is ROLLED BACK. The
        warehouse fixture is session-scoped and shared, so a test that mutates
        it must undo itself - otherwise the next test's assertions depend on
        which order pytest happened to choose.
        """
        from volthive.db.sqlfiles import run_sql_file

        params = {
            "run_id": "00000000-0000-0000-0000-000000000000",
            "batch_lo": "2026-05-29",
            "batch_hi": "2026-06-07",
        }
        promoted: dict = {}
        original_sk: int | None = None

        try:
            with warehouse_conn.transaction():
                with warehouse_conn.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO core.dim_charge_point (
                            charge_point_id, status, effective_from_utc, effective_to_utc,
                            is_current, version_no, row_hash, is_inferred, dw_run_id
                        )
                        VALUES (
                            'CP-PROMOTE-TEST', 'UNKNOWN', '0001-01-01 00:00:00+00',
                            '9999-12-31 00:00:00+00', TRUE, 1,
                            core.row_hash('INFERRED', 'CP-PROMOTE-TEST'), TRUE,
                            '00000000-0000-0000-0000-000000000000'
                        )
                        """
                    )
                original_sk = fetch_value(
                    warehouse_conn,
                    "SELECT charge_point_sk FROM core.dim_charge_point "
                    "WHERE charge_point_id = 'CP-PROMOTE-TEST'",
                )

                with warehouse_conn.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO stg.charge_point (
                            charge_point_id, station_id, oem_vendor, model, current_type,
                            rated_power_kw, connector_type, tariff_plan_id, firmware_version,
                            status, is_deleted, source_updated_at_utc,
                            dw_run_id, dw_batch_key
                        )
                        VALUES (
                            'CP-PROMOTE-TEST', 'ST-BLR-001', 'Delta', 'VH-60D', 'DC',
                            60.00, 'CCS2', 'TP-DC-PREM', '3.5.0', 'ACTIVE', FALSE,
                            %s, '00000000-0000-0000-0000-000000000000', DATE '2026-06-07'
                        )
                        """,
                        (datetime(2026, 6, 6, tzinfo=UTC),),
                    )
                run_sql_file(warehouse_conn, "core/scd2_merge_charge_point.sql", params)
                promoted = dict(
                    fetch_one(
                        warehouse_conn,
                        """
                        SELECT charge_point_sk, rated_power_kw, is_inferred,
                               effective_from_utc, version_no
                        FROM core.dim_charge_point
                        WHERE charge_point_id = 'CP-PROMOTE-TEST'
                        """,
                    )
                    or {}
                )
                raise _Rollback
        except _Rollback:
            pass

        assert promoted, "the promoted row was not found"
        assert promoted["charge_point_sk"] == original_sk, "the surrogate key changed"
        assert promoted["is_inferred"] is False, "the placeholder was not promoted"
        assert promoted["rated_power_kw"] == 60
        assert promoted["version_no"] == 1, "promotion must fill in, not version"
        # Still open-ended, so facts predating the promotion still resolve to it.
        assert promoted["effective_from_utc"].year == 1

        # The rollback really did undo it.
        assert not fetch_value(
            warehouse_conn,
            "SELECT count(*) FROM core.dim_charge_point "
            "WHERE charge_point_id = 'CP-PROMOTE-TEST'",
        )


class _Rollback(Exception):
    """Sentinel used to roll back a mutating assertion block."""


class TestPointInTimeResolution:
    def test_exactly_one_version_matches_each_session(self, warehouse_conn) -> None:
        """THE test that proves the model works.

        For every session, the point-in-time join must match exactly one
        version of the charge point. Zero means the fact loses its device; two
        means every measure on it doubles.
        """
        rows = fetch_all(
            warehouse_conn,
            """
            SELECT
                s.transaction_id,
                count(cp.charge_point_sk) AS matches
            FROM stg.session AS s
            LEFT JOIN core.dim_charge_point AS cp
                ON cp.charge_point_id = s.charge_point_id
                AND s.session_start_utc >= cp.effective_from_utc
                AND s.session_start_utc < cp.effective_to_utc
            WHERE s.charge_point_id IS NOT NULL
            GROUP BY s.transaction_id
            HAVING count(cp.charge_point_sk) <> 1
            """,
        )
        assert rows == [], rows[:5]

    def test_a_session_resolves_to_the_price_in_effect_at_its_start(self, warehouse_conn) -> None:
        """The business justification, asserted.

        Sessions before and after a tariff revision must carry DIFFERENT
        prices. If they all carry today's price, the dimension is effectively
        Type 1 and February's revenue is silently wrong.
        """
        row = fetch_one(
            warehouse_conn,
            """
            WITH revised AS (
                SELECT tariff_plan_id, effective_from_utc AS revision_at
                FROM core.dim_tariff_plan
                WHERE tariff_plan_sk > 0 AND version_no > 1
                ORDER BY effective_from_utc
                LIMIT 1
            )
            SELECT
                count(*) FILTER (WHERE f.session_start_utc < r.revision_at) AS before_count,
                count(*) FILTER (WHERE f.session_start_utc >= r.revision_at) AS after_count,
                count(DISTINCT tp.price_per_kwh_inr)                        AS distinct_prices
            FROM revised AS r
            INNER JOIN core.dim_tariff_plan AS tp ON tp.tariff_plan_id = r.tariff_plan_id
            INNER JOIN core.fact_charging_session AS f ON f.tariff_plan_sk = tp.tariff_plan_sk
            """,
        )
        assert row["before_count"] > 0, "no sessions before the revision"
        assert row["after_count"] > 0, "no sessions after the revision"
        assert row["distinct_prices"] > 1, "every session resolved to the same price"

    def test_facts_never_reference_a_non_current_version_incorrectly(self, warehouse_conn) -> None:
        """Every fact's session start must fall inside its dimension version."""
        bad = fetch_value(
            warehouse_conn,
            """
            SELECT count(*)
            FROM core.fact_charging_session AS f
            INNER JOIN core.dim_charge_point AS cp ON cp.charge_point_sk = f.charge_point_sk
            WHERE cp.charge_point_sk > 0
              AND NOT (
                  f.session_start_utc >= cp.effective_from_utc
                  AND f.session_start_utc < cp.effective_to_utc
              )
            """,
        )
        assert bad == 0


class TestSoftDeletes:
    def test_a_deactivated_customer_keeps_its_history(self, warehouse_conn) -> None:
        """A soft delete is a CHANGE, not a disappearance.

        The tombstone version means "how many customers churned in May" stays
        answerable, and existing facts keep pointing at the version that was
        current when they occurred.
        """
        row = fetch_one(
            warehouse_conn,
            """
            SELECT
                count(*) FILTER (WHERE NOT is_active AND is_current) AS tombstones,
                count(*) FILTER (
                    WHERE NOT is_active AND is_current AND version_no > 1
                ) AS with_history
            FROM core.dim_customer WHERE customer_sk > 0
            """,
        )
        if row["tombstones"] == 0:
            pytest.skip("the tiny profile produced no deactivations")
        assert row["with_history"] > 0
