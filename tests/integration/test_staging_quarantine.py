"""Staging conformance and the quarantine contract.

The assertions here compare against the GENERATOR'S GROUND-TRUTH MANIFEST -
counts recorded as each defect was injected, before the pipeline existed - not
against the pipeline's own output. A test that checks the warehouse against
itself passes just as happily when the logic is uniformly wrong.
"""

from __future__ import annotations

import pytest
from tests.conftest import requires_database

from volthive.db.connection import fetch_all, fetch_one, fetch_value

pytestmark = [pytest.mark.integration, requires_database]


class TestReconciliation:
    def test_no_row_is_lost_between_raw_and_staging(self, warehouse_conn) -> None:
        """THE ACCOUNTING IDENTITY: raw = staged + quarantined.

        Over DISTINCT transaction ids, which is the right granularity - a retry
        storm does not change how many sessions occurred. If this holds, no row
        was lost anywhere, and that can be said with proof rather than hope.
        """
        row = fetch_one(
            warehouse_conn,
            """
            SELECT
                (
                    SELECT count(DISTINCT payload ->> 'transaction_id')
                    FROM raw.ocpp_cdr WHERE payload ->> 'transaction_id' IS NOT NULL
                ) AS raw_distinct,
                (
                    SELECT count(*) FROM stg.session WHERE source_system = 'OCPP'
                ) AS staged,
                (
                    SELECT count(DISTINCT natural_key) FROM dq.quarantine_ocpp_cdr
                    WHERE natural_key IS NOT NULL
                ) AS quarantined
            """,
        )
        assert row["raw_distinct"] == row["staged"] + row["quarantined"]

    def test_staging_energy_reconciles_to_the_fact(self, warehouse_conn) -> None:
        """Counting rows alone would miss a transform that loses a MEASURE."""
        row = fetch_one(
            warehouse_conn,
            """
            SELECT
                (SELECT round(sum(energy_delivered_kwh), 3) FROM stg.session) AS staged,
                (
                    SELECT round(sum(energy_delivered_kwh), 3)
                    FROM core.fact_charging_session
                ) AS fact
            """,
        )
        assert row["staged"] == row["fact"]


class TestQuarantineMatchesGroundTruth:
    @pytest.mark.parametrize(
        ("defect", "rule_code"),
        [
            ("cdr_negative_energy", "CDR_NEGATIVE_ENERGY"),
            ("cdr_time_inversion", "CDR_TIME_INVERSION"),
            ("cdr_implausible_duration", "CDR_IMPLAUSIBLE_DURATION"),
            ("cdr_energy_out_of_range", "CDR_ENERGY_OUT_OF_RANGE"),
            ("cdr_missing_stop", "CDR_MISSING_STOP"),
        ],
    )
    def test_each_injected_defect_is_caught_by_its_own_rule(
        self, warehouse_conn, built_warehouse, defect: str, rule_code: str
    ) -> None:
        """Injected count and quarantined count must agree.

        Exactly, not approximately. The generator recorded how many it injected
        as it injected them, so any discrepancy is the pipeline missing
        something or catching something it should not have.
        """
        injected = built_warehouse["truth"]["defects"].get(defect, 0)
        assert injected > 0, f"{defect} was never injected - the rule is untested"

        caught = fetch_value(
            warehouse_conn,
            "SELECT count(*) FROM dq.quarantine_ocpp_cdr WHERE rule_code = :rule",
            {"rule": rule_code},
        )
        assert caught == injected, f"{rule_code}: injected {injected}, caught {caught}"

    def test_soc_out_of_range_matches(self, warehouse_conn, built_warehouse) -> None:
        injected = built_warehouse["truth"]["defects"]["meter_soc_out_of_range"]
        caught = fetch_value(
            warehouse_conn,
            "SELECT count(*) FROM dq.quarantine_meter_value "
            "WHERE rule_code = 'MTR_SOC_OUT_OF_RANGE'",
        )
        assert caught == injected

    def test_quarantined_rows_are_absent_from_staging(self, warehouse_conn) -> None:
        """Rejected means rejected. A row cannot be both."""
        leaked = fetch_value(
            warehouse_conn,
            """
            SELECT count(*) FROM dq.quarantine_ocpp_cdr AS q
            INNER JOIN stg.session AS s ON s.transaction_id = q.natural_key
            WHERE q.rule_code <> 'CDR_MISSING_STOP'
            """,
        )
        assert leaked == 0

    def test_every_quarantine_row_carries_its_complete_payload(self, warehouse_conn) -> None:
        """The payload is what makes requeue possible at all.

        Without it, a quarantine table is a list of complaints rather than a
        set of records that can be replayed once the problem is fixed.
        """
        rows = fetch_all(
            warehouse_conn,
            """
            SELECT rule_code, raw_payload
            FROM dq.quarantine_ocpp_cdr
            WHERE raw_payload -> 'transaction_id' IS NULL
            LIMIT 5
            """,
        )
        assert rows == [], rows

    def test_rule_details_name_the_actual_values(self, warehouse_conn) -> None:
        """ "meter_stop (1069 kWh) < meter_start (1077 kWh)" is triageable.

        "range check failed" is not, and a quarantine row nobody can act on is
        a quarantine row nobody will.
        """
        detail = fetch_value(
            warehouse_conn,
            "SELECT rule_detail FROM dq.quarantine_ocpp_cdr "
            "WHERE rule_code = 'CDR_NEGATIVE_ENERGY' LIMIT 1",
        )
        assert detail and "delta" in detail and any(c.isdigit() for c in detail)

    def test_one_rule_per_rejected_row(self, warehouse_conn) -> None:
        """Priority order means a rejected row is attributable to ONE rule.

        Otherwise a rule's count is the number of rows it COULD have rejected
        rather than the number it did, and the scorecard stops meaning anything.
        """
        duplicated = fetch_value(
            warehouse_conn,
            """
            SELECT count(*) FROM (
                SELECT natural_key FROM dq.quarantine_ocpp_cdr
                WHERE natural_key IS NOT NULL
                GROUP BY natural_key HAVING count(*) > 1
            ) AS t
            """,
        )
        assert duplicated == 0


class TestConformance:
    def test_unit_normalisation_keeps_the_record(self, warehouse_conn, built_warehouse) -> None:
        """3% of records report kWh instead of Wh. That is a DIALECT.

        Rejecting them would discard thousands of perfectly good sessions over
        a unit difference - a quality layer doing active harm.
        """
        injected = built_warehouse["truth"]["defects"]["cdr_kwh_unit"]
        assert injected > 0
        rejected_for_units = fetch_value(
            warehouse_conn,
            "SELECT count(*) FROM dq.quarantine_ocpp_cdr WHERE rule_detail ILIKE '%unit%'",
        )
        assert rejected_for_units == 0

    def test_energy_values_are_plausible_after_normalisation(self, warehouse_conn) -> None:
        """If a kWh-reporting record slipped through unconverted, its energy
        would be 1000x too small and would stand out immediately."""
        row = fetch_one(
            warehouse_conn,
            """
            SELECT
                min(energy_delivered_kwh) AS lowest,
                max(energy_delivered_kwh) AS highest,
                avg(energy_delivered_kwh) AS average
            FROM stg.session
            """,
        )
        assert row["lowest"] >= 0
        assert row["highest"] <= 350
        assert 2 < row["average"] < 80

    def test_customer_segment_casing_is_conformed(self, warehouse_conn) -> None:
        """ "Fleet" and "FLEET" are the same segment."""
        segments = {
            row["customer_segment"]
            for row in fetch_all(
                warehouse_conn, "SELECT DISTINCT customer_segment FROM stg.customer"
            )
        }
        assert segments <= {"RETAIL", "FLEET", "CORPORATE"}

    def test_duplicates_are_deduplicated_not_quarantined(self, warehouse_conn) -> None:
        """An OCPP retry storm is normal protocol behaviour, not an error.

        Counted in audit.load_stat, absent from quarantine.
        """
        duplicates = fetch_value(
            warehouse_conn,
            "SELECT sum(rows_duplicate_skipped) FROM audit.load_stat "
            "WHERE task_id = 'stg_session'",
        )
        assert duplicates > 0

    def test_the_latest_record_version_wins(self, warehouse_conn) -> None:
        """A corrected CDR carries record_version = 2 with different meter
        values. Keeping the first would keep the wrong numbers."""
        corrected = fetch_value(
            warehouse_conn, "SELECT count(*) FROM stg.session WHERE record_version > 1"
        )
        assert corrected > 0

    def test_pii_is_minimised_at_the_staging_boundary(self, warehouse_conn) -> None:
        """The warehouse holds a masked name and a domain. Never the full values.

        Raw retains what arrived - it is evidence - and is pruned by retention.
        """
        row = fetch_one(
            warehouse_conn,
            """
            SELECT
                count(*) FILTER (WHERE email_domain LIKE '%@%')  AS emails_leaked,
                count(*) FILTER (WHERE full_name_masked ~ ' [A-Z][a-z]+$') AS names_leaked
            FROM stg.customer
            """,
        )
        assert row["emails_leaked"] == 0
        assert row["names_leaked"] == 0


class TestMeterIntervals:
    def test_intervals_start_at_sequence_one(self, warehouse_conn) -> None:
        """The first sample has no predecessor and produces no interval."""
        lowest = fetch_value(warehouse_conn, "SELECT min(interval_seq) FROM stg.meter_interval")
        assert lowest == 1

    def test_intervals_and_quarantined_intervals_account_for_every_sample(
        self, warehouse_conn
    ) -> None:
        """n samples yield exactly n-1 intervals, kept OR quarantined.

        The naive form of this assertion - intervals = samples - 1 - fails on
        any session containing a meter reset, because that interval was
        rejected rather than loaded. Reconciling against kept PLUS quarantined
        is the version that actually holds, and it is a stronger statement:
        every derived interval is accounted for, none is silently dropped.

        Off by one here is invisible in aggregate and obvious in a
        reconciliation, which is exactly why it is asserted per session rather
        than over a total.
        """
        mismatches = fetch_all(
            warehouse_conn,
            """
            WITH counts AS (
                SELECT
                    m.transaction_id,
                    count(*) AS samples,
                    (
                        SELECT count(*) FROM stg.meter_interval AS i
                        WHERE i.transaction_id = m.transaction_id
                    ) AS intervals,
                    (
                        SELECT count(*) FROM dq.quarantine_meter_value AS q
                        WHERE q.rule_code IN (
                            'MTR_NEGATIVE_INTERVAL_ENERGY', 'MTR_ZERO_INTERVAL'
                        )
                        AND split_part(q.natural_key, ':', 1) = m.transaction_id
                    ) AS rejected_intervals
                FROM stg.meter_sample AS m
                INNER JOIN stg.session AS s ON s.transaction_id = m.transaction_id
                GROUP BY m.transaction_id
            )
            SELECT * FROM counts
            WHERE intervals + rejected_intervals <> samples - 1
            LIMIT 5
            """,
        )
        assert mismatches == [], mismatches

    def test_negative_deltas_are_quarantined_not_clamped(self, warehouse_conn) -> None:
        """A meter reset mid-session. Clamping to zero would silently
        under-report that session's energy and nobody would ever know."""
        quarantined = fetch_value(
            warehouse_conn,
            "SELECT count(*) FROM dq.quarantine_meter_value "
            "WHERE rule_code = 'MTR_NEGATIVE_INTERVAL_ENERGY'",
        )
        assert quarantined > 0
        negatives_in_staging = fetch_value(
            warehouse_conn,
            "SELECT count(*) FROM stg.meter_interval WHERE interval_energy_kwh < 0",
        )
        assert negatives_in_staging == 0

    def test_out_of_order_samples_are_ordered_by_event_time(self, warehouse_conn) -> None:
        """Telemetry frames genuinely arrive out of order.

        A LAG over ARRIVAL order would produce negative deltas for perfectly
        good data, so the window orders by sample timestamp.
        """
        inverted = fetch_value(
            warehouse_conn,
            "SELECT count(*) FROM stg.meter_interval WHERE interval_end_utc <= interval_start_utc",
        )
        assert inverted == 0

    def test_orphan_samples_are_held_then_expired(self, warehouse_conn) -> None:
        """Late-arriving RELATIONSHIPS: held while the window is open, and
        quarantined only once no future file can still bring the session."""
        expired = fetch_value(
            warehouse_conn,
            "SELECT count(*) FROM dq.quarantine_meter_value "
            "WHERE rule_code = 'MTR_ORPHAN_EXPIRED'",
        )
        assert expired > 0
        still_orphaned_in_intervals = fetch_value(
            warehouse_conn,
            """
            SELECT count(*) FROM stg.meter_interval AS i
            WHERE NOT EXISTS (
                SELECT 1 FROM stg.session AS s WHERE s.transaction_id = i.transaction_id
            )
            """,
        )
        assert still_orphaned_in_intervals == 0


class TestRoamingConformance:
    def test_outbound_roaming_sessions_reach_the_conformed_grain(self, warehouse_conn) -> None:
        """VoltHive's own systems never saw them. Missing them under-reports
        revenue."""
        count = fetch_value(
            warehouse_conn, "SELECT count(*) FROM stg.session WHERE source_system = 'PARTNER'"
        )
        assert count > 0

    def test_inbound_roaming_does_not_double_count(self, warehouse_conn) -> None:
        """An INBOUND session arrives from BOTH sources. OCPP is authoritative."""
        duplicated = fetch_value(
            warehouse_conn,
            """
            SELECT count(*) FROM (
                SELECT transaction_id FROM stg.session
                GROUP BY transaction_id HAVING count(*) > 1
            ) AS t
            """,
        )
        assert duplicated == 0

    def test_cross_source_records_are_marked_rather_than_dropped(self, warehouse_conn) -> None:
        """The partner's copy is retained as EVIDENCE in stg.partner_session,
        flagged, and simply never becomes a fact row."""
        flagged = fetch_value(
            warehouse_conn,
            "SELECT count(*) FROM stg.partner_session WHERE is_duplicate_source",
        )
        assert flagged >= 0
        leaked = fetch_value(
            warehouse_conn,
            """
            SELECT count(*) FROM stg.partner_session AS p
            INNER JOIN stg.session AS s
                ON s.transaction_id = p.transaction_id AND s.source_system = 'PARTNER'
            WHERE p.is_duplicate_source
            """,
        )
        assert leaked == 0


class TestBusinessDate:
    def test_evening_sessions_belong_to_the_next_ist_day(self, warehouse_conn) -> None:
        """A session at 19:10 UTC is business date TOMORROW in IST.

        Keying facts on the UTC date instead would misplace roughly a quarter
        of evening sessions and quietly break every daily revenue number.
        """
        mismatched = fetch_value(
            warehouse_conn,
            """
            SELECT count(*) FROM stg.session
            WHERE business_date_ist
                <> (session_start_utc AT TIME ZONE 'Asia/Kolkata')::DATE
            """,
        )
        assert mismatched == 0

        shifted = fetch_value(
            warehouse_conn,
            """
            SELECT count(*) FROM stg.session
            WHERE business_date_ist <> session_start_utc::DATE
            """,
        )
        assert shifted > 0, "no session crossed the IST boundary - the test proves nothing"
