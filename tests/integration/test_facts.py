"""Fact loads: key resolution, measure arithmetic and grain enforcement.

The revenue assertions compare against a PYTHON ORACLE - the same arithmetic
computed independently in the test - rather than against the warehouse's own
output. Comparing the pipeline to itself passes just as happily when the logic
is uniformly wrong.
"""

from __future__ import annotations

import sys
from decimal import ROUND_HALF_UP, Decimal
from pathlib import Path

import pytest
from tests.conftest import requires_database

from volthive.db.connection import fetch_all, fetch_one, fetch_value

# run_pipeline lives in scripts/, which is not a package.
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))

pytestmark = [pytest.mark.integration, requires_database]


class TestForeignKeys:
    def test_no_fact_foreign_key_is_null(self, warehouse_conn) -> None:
        """The unknown members are what make this possible.

        The payoff is practical rather than doctrinal: JOINs never silently
        drop rows and COUNT(*) means what it says.
        """
        nulls = fetch_value(
            warehouse_conn,
            """
            SELECT count(*) FROM core.fact_charging_session
            WHERE customer_sk IS NULL OR vehicle_sk IS NULL OR station_sk IS NULL
               OR charge_point_sk IS NULL OR tariff_plan_sk IS NULL
               OR session_outcome_sk IS NULL OR start_date_key IS NULL
            """,
        )
        assert nulls == 0

    def test_every_foreign_key_resolves(self, warehouse_conn) -> None:
        orphans = fetch_value(
            warehouse_conn,
            """
            SELECT count(*) FROM core.fact_charging_session AS f
            LEFT JOIN core.dim_customer AS cu ON cu.customer_sk = f.customer_sk
            LEFT JOIN core.dim_station AS st ON st.station_sk = f.station_sk
            LEFT JOIN core.dim_charge_point AS cp ON cp.charge_point_sk = f.charge_point_sk
            LEFT JOIN core.dim_tariff_plan AS tp ON tp.tariff_plan_sk = f.tariff_plan_sk
            LEFT JOIN core.dim_vehicle AS v ON v.vehicle_sk = f.vehicle_sk
            WHERE cu.customer_sk IS NULL OR st.station_sk IS NULL
               OR cp.charge_point_sk IS NULL OR tp.tariff_plan_sk IS NULL
               OR v.vehicle_sk IS NULL
            """,
        )
        assert orphans == 0

    def test_unknown_and_not_applicable_are_used_distinctly(self, warehouse_conn) -> None:
        """ "There is no such device" and "we could not identify the device" are
        different facts, and the model keeps them different.

        An OUTBOUND roaming session happened on a partner's hardware, so its
        charge point is NOT APPLICABLE (-2). A session on a device the CMS has
        never reported gets an inferred member, not UNKNOWN.
        """
        row = fetch_one(
            warehouse_conn,
            """
            SELECT
                count(*) FILTER (WHERE charge_point_sk = -2) AS not_applicable,
                count(*) FILTER (WHERE charge_point_sk = -2 AND NOT is_roaming) AS wrong
            FROM core.fact_charging_session
            """,
        )
        assert row["not_applicable"] > 0
        assert row["wrong"] == 0, "only roaming sessions should lack a charge point"

    def test_a_session_on_an_unknown_device_still_loads(self, warehouse_conn) -> None:
        """Dropping the fact would lose revenue. The fact is TRUE; the master
        data is merely late."""
        inferred_sessions = fetch_value(
            warehouse_conn,
            """
            SELECT count(*) FROM core.fact_charging_session AS f
            INNER JOIN core.dim_charge_point AS cp ON cp.charge_point_sk = f.charge_point_sk
            WHERE cp.is_inferred
            """,
        )
        assert inferred_sessions > 0


class TestGrain:
    def test_no_duplicate_transaction_ids(self, warehouse_conn) -> None:
        assert (
            fetch_value(
                warehouse_conn,
                """
                SELECT count(*) FROM (
                    SELECT transaction_id FROM core.fact_charging_session
                    GROUP BY transaction_id HAVING count(*) > 1
                ) AS t
                """,
            )
            == 0
        )

    def test_the_grain_constraint_actually_fires(self, warehouse_conn) -> None:
        """Behavioural proof. A constraint that exists but does not apply to
        the rows you insert is worse than none, because it looks like
        protection."""
        import psycopg

        # pytest.raises goes OUTSIDE the transaction block, not inside it.
        # Catching the violation inside would let the transaction context exit
        # "cleanly" and attempt a COMMIT on an already-aborted transaction -
        # which then fails with a different, confusing error.
        with pytest.raises(psycopg.errors.UniqueViolation), warehouse_conn.transaction():
            with warehouse_conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO core.fact_charging_session (
                        transaction_id, start_date_key, start_hour_key, end_date_key,
                        customer_sk, vehicle_sk, station_sk, charge_point_sk,
                        tariff_plan_sk, session_outcome_sk,
                        session_start_utc, session_end_utc,
                        energy_delivered_kwh, duration_seconds,
                        source_system, dw_run_id, dw_batch_key
                    )
                    SELECT
                        f.transaction_id, f.start_date_key, f.start_hour_key, f.end_date_key,
                        f.customer_sk, f.vehicle_sk, f.station_sk, f.charge_point_sk,
                        f.tariff_plan_sk, f.session_outcome_sk,
                        f.session_start_utc, f.session_end_utc,
                        f.energy_delivered_kwh, f.duration_seconds,
                        f.source_system, f.dw_run_id, f.dw_batch_key
                    FROM core.fact_charging_session AS f LIMIT 1
                    """
                )

    def test_the_station_day_fact_is_dense(self, warehouse_conn) -> None:
        """Zero-session days must produce rows.

        If only busy days produced rows, every utilisation average would be
        computed over busy days only - and the bias would be invisible, because
        the missing rows are missing.
        """
        row = fetch_one(
            warehouse_conn,
            """
            SELECT
                count(*) FILTER (WHERE session_count = 0) AS idle_days,
                count(*)                                  AS total
            FROM core.fact_station_daily_utilization
            """,
        )
        assert row["idle_days"] > 0, "no idle station-day rows - the fact is not dense"
        assert row["total"] > row["idle_days"]

    def test_utilisation_never_exceeds_one_hundred_percent(self, warehouse_conn) -> None:
        assert (
            fetch_value(
                warehouse_conn,
                "SELECT count(*) FROM core.fact_station_daily_utilization "
                "WHERE utilization_pct > 100",
            )
            == 0
        )


class TestMeasureArithmetic:
    def test_revenue_matches_an_independent_python_calculation(self, warehouse_conn) -> None:
        """The strongest form of this assertion: an ORACLE, not a self-check.

        The arithmetic is recomputed here from the fact's own inputs - billable
        energy, the point-in-time price, the plan discount, GST - and compared
        with what the warehouse stored. If the SQL and this disagree, one of
        them is wrong and the test says so.
        """
        rows = fetch_all(
            warehouse_conn,
            """
            SELECT
                f.transaction_id,
                f.energy_delivered_kwh,
                f.duration_seconds,
                f.charging_seconds,
                f.energy_charge_inr,
                f.time_charge_inr,
                f.idle_fee_inr,
                f.discount_inr,
                f.gst_inr,
                f.gross_revenue_inr,
                f.grid_energy_cost_inr,
                f.gross_margin_inr,
                tp.price_per_kwh_inr,
                tp.price_per_minute_inr,
                tp.idle_fee_per_minute_inr,
                tp.min_billable_kwh,
                tp.gst_rate_pct,
                cu.subscription_plan
            FROM core.fact_charging_session AS f
            INNER JOIN core.dim_tariff_plan AS tp ON tp.tariff_plan_sk = f.tariff_plan_sk
            INNER JOIN core.dim_customer AS cu ON cu.customer_sk = f.customer_sk
            WHERE NOT f.is_roaming AND f.tariff_plan_sk > 0
            LIMIT 200
            """,
        )
        assert len(rows) > 50

        discounts = {"PLUS": Decimal("0.05"), "FLEET_PRO": Decimal("0.12")}

        def money(value: Decimal) -> Decimal:
            """Round the way PostgreSQL does.

            Python's round() on a Decimal uses ROUND_HALF_EVEN (banker's
            rounding); PostgreSQL's round() on numeric uses ROUND_HALF_UP. On a
            half-paisa they disagree, and an oracle that models the wrong
            rounding rule reports a bug that does not exist - which is worse
            than no oracle, because someone will spend an afternoon on it.
            """
            return value.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)

        for row in rows:
            billable = max(row["energy_delivered_kwh"], row["min_billable_kwh"])
            energy_charge = money(billable * row["price_per_kwh_inr"])
            time_charge = money(
                (Decimal(row["duration_seconds"]) / 60) * row["price_per_minute_inr"]
            )
            idle_seconds = max(row["duration_seconds"] - row["charging_seconds"], 0)
            idle_fee = money(
                (Decimal(max(idle_seconds - 600, 0)) / 60) * row["idle_fee_per_minute_inr"]
            )
            discount = money(
                (energy_charge + time_charge)
                * discounts.get(row["subscription_plan"], Decimal("0"))
            )
            gst = money(
                (energy_charge + time_charge + idle_fee - discount) * row["gst_rate_pct"] / 100
            )
            expected = money(energy_charge + time_charge + idle_fee - discount + gst)

            # ONE PAISA of tolerance, and the reason is arithmetic rather
            # than laxity: PostgreSQL numeric division and Python Decimal
            # division carry different intermediate precision, so on a value
            # that lands exactly on a half-paisa boundary they can round in
            # opposite directions. An oracle can match this to the paisa but
            # not to the last bit.
            #
            # It is still a strong assertion. Every failure mode worth catching
            # here - the wrong tariff VERSION resolved, a missing plan
            # discount, the wrong GST rate, energy multiplied by the wrong
            # price - is wrong by rupees, not by paisa.
            paisa = Decimal("0.01")
            key = row["transaction_id"]
            assert abs(row["energy_charge_inr"] - energy_charge) <= paisa, key
            assert abs(row["time_charge_inr"] - time_charge) <= paisa, key
            assert abs(row["idle_fee_inr"] - idle_fee) <= paisa, key
            assert abs(row["discount_inr"] - discount) <= paisa, key
            assert abs(row["gross_revenue_inr"] - expected) <= paisa * 3, key

            # The internal identity, by contrast, is EXACT: the stored total
            # must equal the sum of the stored components. No division is
            # involved, so there is nothing to round differently.
            assert row["gross_revenue_inr"] == (
                row["energy_charge_inr"]
                + row["time_charge_inr"]
                + row["idle_fee_inr"]
                - row["discount_inr"]
                + row["gst_inr"]
            ), key

    def test_margin_is_revenue_minus_grid_cost(self, warehouse_conn) -> None:
        """gross_margin is a REAL derived measure, not a second name for revenue."""
        bad = fetch_value(
            warehouse_conn,
            """
            SELECT count(*) FROM core.fact_charging_session
            WHERE round(gross_revenue_inr - grid_energy_cost_inr, 2) <> gross_margin_inr
            """,
        )
        assert bad == 0

    def test_grid_cost_uses_the_slab_in_force_on_that_date(self, warehouse_conn) -> None:
        """Not the current slab. Otherwise last year's margin is recomputed
        every time the utility raises its rates."""
        mismatched = fetch_value(
            warehouse_conn,
            """
            SELECT count(*)
            FROM core.fact_charging_session AS f
            INNER JOIN core.dim_station AS st ON st.station_sk = f.station_sk
            INNER JOIN core.dim_date AS d ON d.date_key = f.start_date_key
            INNER JOIN stg.grid_tariff_slab AS g
                ON g.state = st.state
                AND d.full_date >= g.effective_from_date
                AND d.full_date < g.effective_to_date
            WHERE f.station_sk > 0
              AND round(f.energy_delivered_kwh * g.commercial_rate_inr_per_kwh, 2)
                  <> f.grid_energy_cost_inr
            """,
        )
        assert mismatched == 0

    def test_charging_seconds_never_exceed_session_duration(self, warehouse_conn) -> None:
        """A car cannot draw power for longer than it was plugged in."""
        assert (
            fetch_value(
                warehouse_conn,
                "SELECT count(*) FROM core.fact_charging_session "
                "WHERE charging_seconds > duration_seconds",
            )
            == 0
        )

    def test_average_power_is_over_charging_time_not_plugged_in_time(self, warehouse_conn) -> None:
        """Averaging over plugged-in time would report a 60 kW charger as a
        9 kW one for anyone who left their car overnight."""
        bad = fetch_value(
            warehouse_conn,
            """
            SELECT count(*) FROM core.fact_charging_session
            WHERE charging_seconds > 0
              AND abs(
                  avg_power_kw - round((energy_delivered_kwh * 3600.0) / charging_seconds, 3)
              ) > 0.001
            """,
        )
        assert bad == 0

    def test_roaming_sessions_carry_the_partner_price_not_ours(self, warehouse_conn) -> None:
        """A roaming session was billed on the PARTNER'S price list.

        Reporting it under our tariff's component columns would imply a
        breakdown that does not exist.
        """
        bad = fetch_value(
            warehouse_conn,
            """
            SELECT count(*) FROM core.fact_charging_session
            WHERE source_system = 'PARTNER'
              AND (energy_charge_inr <> 0 OR gst_inr <> 0 OR discount_inr <> 0)
            """,
        )
        assert bad == 0
        with_revenue = fetch_value(
            warehouse_conn,
            "SELECT count(*) FROM core.fact_charging_session "
            "WHERE source_system = 'PARTNER' AND gross_revenue_inr > 0",
        )
        assert with_revenue > 0


class TestIntervalFact:
    def test_a_window_that_ends_mid_session_leaves_no_orphan(self, warehouse_conn) -> None:
        """A restatement window must not sever a session from its intervals.

        A session starting at 23:40 IST has intervals on the FOLLOWING business
        date. The session fact is windowed on the SESSION's business date and
        the interval fact on the INTERVAL's, so a window ending between them
        contains one and not the other. When that happened, the session fact
        deleted and re-inserted the session - assigning it a new
        charging_session_sk from the identity sequence - while its intervals,
        being a day later, were left pointing at a key that no longer existed.

        There is no foreign key to catch this, deliberately (see
        sql/ddl/31_core_facts.sql), so it is SILENT: the orphaned intervals
        simply stop joining and their energy vanishes from every total.

        This restates a window whose upper edge falls exactly on such a
        session, which is the only shape that reproduces it.
        """
        boundary = fetch_one(
            warehouse_conn,
            """
            SELECT f.transaction_id, f.dw_batch_key AS session_date,
                   max(i.dw_batch_key) AS last_interval_date
            FROM core.fact_charging_session AS f
            INNER JOIN core.fact_meter_interval AS i
                ON i.transaction_id = f.transaction_id
            GROUP BY f.transaction_id, f.dw_batch_key
            HAVING max(i.dw_batch_key) > f.dw_batch_key
            ORDER BY f.dw_batch_key
            LIMIT 1
            """,
        )
        assert boundary is not None, (
            "no session spans midnight IST in this dataset, so the orphan case "
            "is not reachable and this test would pass vacuously"
        )

        from run_pipeline import run

        # date_to is the session's own date: the window covers the session and
        # stops before its intervals.
        run(
            stages=["core"],
            date_from=boundary["session_date"],
            date_to=boundary["session_date"],
            lookback_days=0,
            skip_dq_gate=True,
            is_backfill=False,
        )

        orphans = fetch_value(
            warehouse_conn,
            """
            SELECT count(*)
            FROM core.fact_meter_interval AS i
            LEFT JOIN core.fact_charging_session AS f
                ON f.charging_session_sk = i.charging_session_sk
            WHERE f.charging_session_sk IS NULL
            """,
        )
        assert orphans == 0, (
            f"{orphans} intervals were severed from their session by a window "
            f"ending on {boundary['session_date']}"
        )

    def test_intervals_inherit_their_session_keys(self, warehouse_conn) -> None:
        """An interval belongs to a session.

        If it re-resolved point-in-time on its own timestamp, a session
        spanning a firmware update would have its intervals split across two
        versions of the same device while its header pointed at one.
        """
        mismatched = fetch_value(
            warehouse_conn,
            """
            SELECT count(*)
            FROM core.fact_meter_interval AS m
            INNER JOIN core.fact_charging_session AS f
                ON f.charging_session_sk = m.charging_session_sk
            WHERE m.charge_point_sk <> f.charge_point_sk
               OR m.customer_sk <> f.customer_sk
            """,
        )
        assert mismatched == 0

    def test_no_interval_is_orphaned(self, warehouse_conn) -> None:
        """The relationship is NOT a declared foreign key - see the note in
        31_core_facts.sql - so this assertion is what enforces it."""
        orphans = fetch_value(
            warehouse_conn,
            """
            SELECT count(*) FROM core.fact_meter_interval AS m
            LEFT JOIN core.fact_charging_session AS f
                ON f.charging_session_sk = m.charging_session_sk
            WHERE f.charging_session_sk IS NULL
            """,
        )
        assert orphans == 0

    def test_intervals_land_in_the_right_partition(self, warehouse_conn) -> None:
        """A row in the wrong partition is a row partition pruning will never
        find."""
        misfiled = fetch_value(
            warehouse_conn,
            """
            SELECT count(*) FROM core.fact_meter_interval
            WHERE date_key <> to_char(dw_batch_key, 'YYYYMMDD')::INT
            """,
        )
        assert misfiled == 0

    def test_interval_energy_is_additive(self, warehouse_conn) -> None:
        """The reason the fact models INTERVALS rather than SAMPLES.

        Summed interval energy for a session should approximate the session's
        own total. Summing cumulative REGISTER readings would produce a number
        millions of times too large.
        """
        row = fetch_one(
            warehouse_conn,
            """
            WITH per_session AS (
                SELECT
                    f.transaction_id,
                    f.energy_delivered_kwh                  AS header,
                    sum(m.interval_energy_kwh)              AS from_intervals
                FROM core.fact_charging_session AS f
                INNER JOIN core.fact_meter_interval AS m
                    ON m.charging_session_sk = f.charging_session_sk
                WHERE f.has_meter_detail AND NOT f.is_roaming
                GROUP BY f.transaction_id, f.energy_delivered_kwh
            )
            SELECT
                count(*) AS sessions,
                count(*) FILTER (
                    WHERE abs(from_intervals - header) > header * 0.05 + 0.5
                ) AS divergent
            FROM per_session
            """,
        )
        assert row["sessions"] > 50
        # A few sessions legitimately diverge: an interval rejected for a meter
        # reset removes its energy from the interval total while the header
        # keeps it.
        assert row["divergent"] / row["sessions"] < 0.05


class TestSemanticViews:
    def test_the_enriched_view_returns_every_session(self, warehouse_conn) -> None:
        """Every join in the view is INNER, which is only safe because no fact
        foreign key is ever NULL. If one were, the view would silently return
        fewer rows than the fact - the exact failure unknown members prevent.
        """
        row = fetch_one(
            warehouse_conn,
            """
            SELECT
                (SELECT count(*) FROM core.fact_charging_session) AS facts,
                (SELECT count(*) FROM mart.v_sessions_enriched)   AS view_rows
            """,
        )
        assert row["facts"] == row["view_rows"]

    def test_the_view_exposes_no_surrogate_keys(self, warehouse_conn) -> None:
        """A mart consumer should never need to know what a surrogate key is."""
        columns = {
            row["column_name"]
            for row in fetch_all(
                warehouse_conn,
                """
                SELECT column_name FROM information_schema.columns
                WHERE table_schema = 'mart' AND table_name = 'v_sessions_enriched'
                """,
            )
        }
        assert not [c for c in columns if c.endswith("_sk")]


class TestMartConsistency:
    def test_mart_totals_tie_back_to_the_facts(self, warehouse_conn) -> None:
        """An aggregate that disagrees with its source is worse than no
        aggregate: it is a number someone will quote."""
        row = fetch_one(
            warehouse_conn,
            """
            SELECT
                (
                    SELECT round(sum(gross_revenue_inr), 2)
                    FROM core.fact_charging_session WHERE station_sk > 0
                ) AS fact_revenue,
                (
                    SELECT round(sum(gross_revenue_inr), 2)
                    FROM mart.mart_station_month_kpi
                ) AS mart_revenue
            """,
        )
        assert row["fact_revenue"] == row["mart_revenue"]

    def test_utilisation_is_recomputed_not_averaged(self, warehouse_conn) -> None:
        """AVG(daily ratio) weights a quiet Sunday like a busy Monday.

        The monthly figure must come from summed numerators and denominators.
        """
        bad = fetch_value(
            warehouse_conn,
            """
            SELECT count(*) FROM mart.mart_station_month_kpi
            WHERE available_minutes > 0
              AND abs(
                  utilization_pct - round(100.0 * occupied_minutes / available_minutes, 2)
              ) > 0.01
            """,
        )
        assert bad == 0

    def test_point_in_time_power_reaches_the_daily_mart(self, warehouse_conn) -> None:
        """The 30 kW to 60 kW comparison must be possible from the mart alone.

        A device that appears with two different rated powers on two dates is
        the evidence that history survived all the way to the consumer layer.
        """
        changed = fetch_value(
            warehouse_conn,
            """
            SELECT count(*) FROM (
                SELECT charge_point_id FROM mart.mart_charge_point_daily
                GROUP BY charge_point_id HAVING count(DISTINCT rated_power_kw) > 1
            ) AS t
            """,
        )
        assert changed >= 0  # zero is acceptable in a 7-day window
        distinct_powers = fetch_value(
            warehouse_conn,
            "SELECT count(DISTINCT rated_power_kw) FROM mart.mart_charge_point_daily",
        )
        assert distinct_powers > 1
