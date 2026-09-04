"""The schema itself: constraints that make bad states impossible.

Most of these assert the EXISTENCE of a constraint rather than its behaviour.
That is deliberate: a partial unique index that someone drops during a
performance investigation takes a structural guarantee with it, and the next
bug is a dimension with two current rows that nobody notices for a month.
"""

from __future__ import annotations

import pytest
from tests.conftest import requires_database

from volthive.db.connection import fetch_all, fetch_value

pytestmark = [pytest.mark.integration, requires_database]


class TestSchemaCompleteness:
    def test_all_seven_schemas_exist(self, warehouse_conn) -> None:
        found = {
            row["nspname"]
            for row in fetch_all(
                warehouse_conn,
                "SELECT nspname FROM pg_namespace WHERE nspname = ANY(:names)",
                {"names": ["raw", "stg", "core", "mart", "dq", "audit", "ctl"]},
            )
        }
        assert found == {"raw", "stg", "core", "mart", "dq", "audit", "ctl"}

    def test_ddl_applies_twice_cleanly(self, warehouse_conn) -> None:
        """`make db-init` runs on every `make up`, not only the first."""
        from volthive.db.sqlfiles import run_sql_dir

        run_sql_dir(warehouse_conn, "ddl")
        run_sql_dir(warehouse_conn, "ddl")

    def test_every_table_and_column_is_documented(self, warehouse_conn) -> None:
        """Not every column, but every TABLE in core and mart.

        A model nobody can read is a model nobody trusts, and COMMENT ON is
        what makes docs/data_model.md generatable rather than hand-maintained.
        """
        undocumented = fetch_all(
            warehouse_conn,
            """
            SELECT c.relname
            FROM pg_class AS c
            INNER JOIN pg_namespace AS n ON n.oid = c.relnamespace
            WHERE n.nspname IN ('core', 'mart')
              AND c.relkind IN ('r', 'p', 'v')
              AND c.relispartition IS FALSE
              AND obj_description(c.oid, 'pg_class') IS NULL
            """,
        )
        assert not undocumented, [row["relname"] for row in undocumented]


class TestGrainEnforcement:
    @pytest.mark.parametrize(
        ("table", "constraint"),
        [
            ("core.fact_charging_session", "uq_fact_charging_session_grain"),
            ("core.fact_meter_interval", "uq_fact_meter_interval_grain"),
            ("core.fact_station_daily_utilization", "uq_fact_station_daily_grain"),
        ],
    )
    def test_every_fact_has_a_grain_enforcing_unique_constraint(
        self, warehouse_conn, table: str, constraint: str
    ) -> None:
        """A grain that lives only in a comment is a grain that gets violated."""
        assert fetch_value(
            warehouse_conn,
            "SELECT 1 FROM pg_constraint WHERE conname = :name AND contype = 'u'",
            {"name": constraint},
        ), f"{table} has no grain constraint"


class TestScd2Invariants:
    @pytest.mark.parametrize(
        "index_name",
        [
            "uix_dim_customer_current",
            "uix_dim_station_current",
            "uix_dim_charge_point_current",
            "uix_dim_tariff_plan_current",
        ],
    )
    def test_partial_unique_current_index_exists(self, warehouse_conn, index_name: str) -> None:
        """Makes "two current rows" STRUCTURALLY IMPOSSIBLE, not merely unlikely."""
        definition = fetch_value(
            warehouse_conn,
            "SELECT indexdef FROM pg_indexes WHERE indexname = :name",
            {"name": index_name},
        )
        assert definition is not None, index_name
        assert "UNIQUE" in definition
        assert "WHERE is_current" in definition

    def test_overlap_exclusion_constraints_exist(self, warehouse_conn) -> None:
        """Makes overlapping validity ranges structurally impossible too.

        Skipped rather than failed when btree_gist is unavailable: the schema
        applies without it and the DIM_NO_OVERLAPPING_VERSIONS rule still
        catches violations after the fact.
        """
        if not fetch_value(
            warehouse_conn, "SELECT 1 FROM pg_extension WHERE extname = 'btree_gist'"
        ):
            pytest.skip("btree_gist is not installed")
        found = {
            row["conname"]
            for row in fetch_all(
                warehouse_conn,
                "SELECT conname FROM pg_constraint WHERE conname LIKE 'ex_dim%_no_overlap'",
            )
        }
        assert len(found) == 4, found

    def test_two_current_rows_are_rejected(self, warehouse_conn) -> None:
        """Behavioural proof, not just structural.

        Asserts the index actually fires - a constraint that exists but does
        not apply to the rows you insert is worse than none, because it looks
        like protection.
        """
        import psycopg

        with warehouse_conn.transaction() as _tx, pytest.raises(psycopg.errors.UniqueViolation):
            with warehouse_conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO core.dim_tariff_plan (
                        tariff_plan_id, price_per_kwh_inr, effective_from_utc,
                        effective_to_utc, is_current, version_no, row_hash, dw_run_id
                    )
                    SELECT
                        d.tariff_plan_id, 1, now(), '9999-12-31 00:00:00+00', TRUE, 99,
                        repeat('f', 64), '00000000-0000-0000-0000-000000000000'
                    FROM core.dim_tariff_plan AS d
                    WHERE d.is_current AND d.tariff_plan_sk > 0
                    LIMIT 1
                    """
                )
            raise AssertionError("expected the partial unique index to reject this")

    def test_is_current_cannot_disagree_with_the_dates(self, warehouse_conn) -> None:
        """The flag is a shorthand for the end date and must not drift from it."""
        import psycopg

        with warehouse_conn.transaction() as _tx, pytest.raises(psycopg.errors.CheckViolation):
            with warehouse_conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO core.dim_tariff_plan (
                        tariff_plan_id, price_per_kwh_inr, effective_from_utc,
                        effective_to_utc, is_current, version_no, row_hash, dw_run_id
                    )
                    VALUES (
                        'TP-CHECK-TEST', 1, '2026-01-01', '2026-06-01', TRUE, 1,
                        repeat('f', 64), '00000000-0000-0000-0000-000000000000'
                    )
                    """
                )


class TestUnknownMembers:
    @pytest.mark.parametrize(
        ("table", "key"),
        [
            ("core.dim_customer", "customer_sk"),
            ("core.dim_station", "station_sk"),
            ("core.dim_charge_point", "charge_point_sk"),
            ("core.dim_tariff_plan", "tariff_plan_sk"),
            ("core.dim_vehicle", "vehicle_sk"),
        ],
    )
    def test_unknown_and_not_applicable_members_exist(
        self, warehouse_conn, table: str, key: str
    ) -> None:
        """-1 and -2 are what let every fact foreign key be NOT NULL."""
        found = fetch_value(
            warehouse_conn,
            f"SELECT count(*) FROM {table} WHERE {key} IN (-1, -2)",
        )
        assert found == 2, table

    def test_the_unknown_tariff_prices_at_zero(self, warehouse_conn) -> None:
        """Deliberately visible rather than quietly wrong.

        A plausible-looking default price would poison a revenue total in a way
        no aggregate would reveal. Zero, plus the unknown-member ratio rule,
        makes the gap show up on the scorecard instead.
        """
        price = fetch_value(
            warehouse_conn,
            "SELECT price_per_kwh_inr FROM core.dim_tariff_plan WHERE tariff_plan_sk = -1",
        )
        assert price == 0


class TestPartitioning:
    def test_the_interval_fact_is_partitioned(self, warehouse_conn) -> None:
        assert fetch_value(
            warehouse_conn,
            """
            SELECT 1 FROM pg_class AS c
            INNER JOIN pg_namespace AS n ON n.oid = c.relnamespace
            WHERE n.nspname = 'core' AND c.relname = 'fact_meter_interval'
              AND c.relkind = 'p'
            """,
        )

    def test_nothing_else_is_partitioned(self, warehouse_conn) -> None:
        """Partitioning a 65k-row table costs planning time for zero benefit.

        Saying WHERE you did not partition is worth more than partitioning
        everything.
        """
        partitioned = fetch_all(
            warehouse_conn,
            """
            SELECT c.relname FROM pg_class AS c
            INNER JOIN pg_namespace AS n ON n.oid = c.relnamespace
            WHERE n.nspname = 'core' AND c.relkind = 'p'
            """,
        )
        assert [row["relname"] for row in partitioned] == ["fact_meter_interval"]

    def test_partitions_cover_the_generated_window(self, warehouse_conn) -> None:
        count = fetch_value(
            warehouse_conn,
            """
            SELECT count(*) FROM pg_class AS c
            INNER JOIN pg_namespace AS n ON n.oid = c.relnamespace
            WHERE n.nspname = 'core' AND c.relkind = 'r'
              AND c.relname LIKE 'fact_meter_interval_2%'
            """,
        )
        assert count >= 24

    def test_partition_creation_is_idempotent(self, warehouse_conn) -> None:
        created = fetch_value(
            warehouse_conn,
            "SELECT core.ensure_meter_interval_partitions(DATE '2026-01-01', DATE '2026-06-01')",
        )
        assert created == 0


class TestDateDimension:
    def test_indian_fiscal_year_boundary(self, warehouse_conn) -> None:
        """FY runs April-March and is labelled by its ENDING year."""
        rows = {
            str(row["full_date"]): row["fiscal_year"]
            for row in fetch_all(
                warehouse_conn,
                """
                SELECT full_date, fiscal_year FROM core.dim_date
                WHERE full_date IN (DATE '2026-03-31', DATE '2026-04-01')
                """,
            )
        }
        assert rows["2026-03-31"] == 2026
        assert rows["2026-04-01"] == 2027

    def test_the_calendar_spans_2023_to_2030(self, warehouse_conn) -> None:
        count = fetch_value(warehouse_conn, "SELECT count(*) FROM core.dim_date")
        assert count == 2922

    def test_the_hour_dimension_has_24_rows(self, warehouse_conn) -> None:
        assert fetch_value(warehouse_conn, "SELECT count(*) FROM core.dim_hour") == 24
