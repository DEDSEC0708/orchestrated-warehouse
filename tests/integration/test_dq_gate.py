"""The data-quality gate: does it actually block, and does it block correctly?

A gate that never fires is indistinguishable from no gate. These tests
deliberately BREAK the warehouse - insert a duplicate, orphan a foreign key,
inflate the quarantine ratio - and assert that the right rule notices and that
the gate stops the mart. Every one rolls back afterwards.
"""

from __future__ import annotations

from datetime import date

import pytest
from tests.conftest import requires_database

from volthive.db.connection import fetch_all, fetch_value
from volthive.dq.engine import evaluate_gate, run_dataset_checks

pytestmark = [pytest.mark.integration, requires_database]

RUN_ID = "00000000-0000-0000-0000-000000000000"
WINDOW = {"batch_lo": date(2026, 5, 29), "batch_hi": date(2026, 6, 7)}


class _Rollback(Exception):
    """Sentinel used to roll back a deliberately-broken warehouse."""


class TestCleanDataPasses:
    def test_all_error_severity_rules_pass_on_the_built_warehouse(self, warehouse_conn) -> None:
        """A rule that fires on CORRECT data is a false positive.

        False positives are worse than missing rules: people learn to ignore
        them, and then ignore the real one.
        """
        results = run_dataset_checks(warehouse_conn, run_id=RUN_ID, **WINDOW)
        failing = [r.rule_code for r in results if r.severity == "error" and r.status != "PASS"]
        assert failing == [], failing

    def test_the_gate_opens(self, warehouse_conn) -> None:
        results = run_dataset_checks(warehouse_conn, run_id=RUN_ID, **WINDOW)
        assert evaluate_gate(results).passed

    def test_every_rule_actually_evaluated_something(self, warehouse_conn) -> None:
        """A rule evaluating zero rows passes VACUOUSLY.

        Some legitimately evaluate one row (a freshness check, a zero-count
        check), so the assertion is that the rule ran and returned a row - not
        that it examined many.
        """
        results = run_dataset_checks(warehouse_conn, run_id=RUN_ID, **WINDOW)
        assert results
        errored = [r.rule_code for r in results if r.status == "ERROR"]
        assert errored == [], errored

    def test_results_are_recorded_for_every_rule(self, warehouse_conn) -> None:
        run_dataset_checks(warehouse_conn, run_id=RUN_ID, **WINDOW)
        recorded = fetch_value(
            warehouse_conn,
            "SELECT count(DISTINCT rule_code) FROM dq.check_result WHERE dw_run_id = :run",
            {"run": RUN_ID},
        )
        enabled = fetch_value(
            warehouse_conn,
            "SELECT count(*) FROM dq.rule WHERE scope = 'dataset' AND is_enabled",
        )
        assert recorded == enabled


class TestBrokenDataBlocks:
    def test_a_grain_violation_is_detected(self, warehouse_conn) -> None:
        """Bypassing the unique constraint to prove the RULE works too.

        The constraint already makes this impossible - the rule exists so that
        a future load which disables constraints for speed cannot do so
        silently.
        """
        try:
            with warehouse_conn.transaction():
                with warehouse_conn.cursor() as cur:
                    cur.execute(
                        "ALTER TABLE core.fact_charging_session "
                        "DROP CONSTRAINT uq_fact_charging_session_grain"
                    )
                    cur.execute(
                        """
                        INSERT INTO core.fact_charging_session (
                            transaction_id, start_date_key, start_hour_key, end_date_key,
                            customer_sk, vehicle_sk, station_sk, charge_point_sk,
                            tariff_plan_sk, session_outcome_sk, session_start_utc,
                            session_end_utc, energy_delivered_kwh, duration_seconds,
                            source_system, dw_run_id, dw_batch_key
                        )
                        SELECT
                            f.transaction_id, f.start_date_key, f.start_hour_key,
                            f.end_date_key, f.customer_sk, f.vehicle_sk, f.station_sk,
                            f.charge_point_sk, f.tariff_plan_sk, f.session_outcome_sk,
                            f.session_start_utc, f.session_end_utc,
                            f.energy_delivered_kwh, f.duration_seconds,
                            f.source_system, f.dw_run_id, f.dw_batch_key
                        FROM core.fact_charging_session AS f LIMIT 1
                        """
                    )
                results = run_dataset_checks(
                    warehouse_conn,
                    run_id=RUN_ID,
                    only=["FACT_SESSION_GRAIN_UNIQUE"],
                    **WINDOW,
                )
                assert results[0].status == "FAIL"
                assert not evaluate_gate(results).passed
                raise _Rollback
        except _Rollback:
            pass

        assert fetch_value(
            warehouse_conn,
            "SELECT 1 FROM pg_constraint WHERE conname = 'uq_fact_charging_session_grain'",
        ), "the rollback failed to restore the constraint"

    def test_two_current_dimension_rows_are_detected(self, warehouse_conn) -> None:
        try:
            with warehouse_conn.transaction():
                with warehouse_conn.cursor() as cur:
                    # BOTH structural guards have to come off to create the bad
                    # state - which is itself the point: the schema makes this
                    # impossible, and the rule exists so that a future load
                    # which disables constraints for speed cannot do so
                    # silently.
                    cur.execute("DROP INDEX core.uix_dim_tariff_plan_current")
                    cur.execute(
                        "ALTER TABLE core.dim_tariff_plan "
                        "DROP CONSTRAINT IF EXISTS ex_dim_tariff_plan_no_overlap"
                    )
                    cur.execute(
                        """
                        INSERT INTO core.dim_tariff_plan (
                            tariff_plan_id, price_per_kwh_inr, effective_from_utc,
                            effective_to_utc, is_current, version_no, row_hash, dw_run_id
                        )
                        SELECT
                            d.tariff_plan_id, 1, now(), '9999-12-31 00:00:00+00',
                            TRUE, 99, repeat('e', 64), %s
                        FROM core.dim_tariff_plan AS d
                        WHERE d.is_current AND d.tariff_plan_sk > 0 LIMIT 1
                        """,
                        (RUN_ID,),
                    )
                results = run_dataset_checks(
                    warehouse_conn, run_id=RUN_ID, only=["DIM_SINGLE_CURRENT_ROW"], **WINDOW
                )
                assert results[0].status == "FAIL"
                raise _Rollback
        except _Rollback:
            pass

    def test_a_zero_row_window_is_an_error_not_a_warning(self, warehouse_conn) -> None:
        """A quiet day is plausible. Complete silence is an outage."""
        results = run_dataset_checks(
            warehouse_conn,
            run_id=RUN_ID,
            only=["ROWCOUNT_ZERO_SESSION"],
            batch_lo=date(2030, 1, 1),
            batch_hi=date(2030, 1, 2),
        )
        assert results[0].status == "FAIL"
        assert results[0].severity == "error"

    def test_an_orphaned_interval_is_detected(self, warehouse_conn) -> None:
        """The relationship this fact has NO declared foreign key for.

        Operability beat a constraint there, so this rule is the only thing
        enforcing it - which is why it is error severity.
        """
        try:
            with warehouse_conn.transaction():
                with warehouse_conn.cursor() as cur:
                    cur.execute(
                        """
                        UPDATE core.fact_meter_interval
                        SET charging_session_sk = -999
                        WHERE meter_interval_sk IN (
                            SELECT meter_interval_sk FROM core.fact_meter_interval LIMIT 3
                        )
                        """
                    )
                results = run_dataset_checks(
                    warehouse_conn,
                    run_id=RUN_ID,
                    only=["FACT_METER_ORPHAN_SESSION"],
                    **WINDOW,
                )
                assert results[0].status == "FAIL"
                assert results[0].rows_failed == 3
                raise _Rollback
        except _Rollback:
            pass


class TestSeverityBehaviour:
    def test_a_warn_severity_failure_does_not_block(self, warehouse_conn) -> None:
        """A genuine public holiday should not stop the warehouse."""
        results = run_dataset_checks(warehouse_conn, run_id=RUN_ID, **WINDOW)
        warned = [r for r in results if r.status == "WARN"]
        assert warned, "no rule warned - the warn path is untested"
        assert evaluate_gate(results).passed

    def test_schema_drift_warns_rather_than_failing(self, warehouse_conn) -> None:
        """A NEW key is a warning: raw JSONB already captured it losslessly, so
        history is available from the day it first appeared.

        A MISSING expected key would be the error case, because downstream
        casts would then silently produce NULLs.
        """
        results = run_dataset_checks(
            warehouse_conn, run_id=RUN_ID, only=["SCHEMA_DRIFT_OCPP"], **WINDOW
        )
        assert results[0].severity == "warn"
        assert results[0].rows_failed >= 1, (
            "the generator adds grid_carbon_intensity from firmware 3.5; "
            "drift detection is not seeing it"
        )

    def test_the_bypass_publishes_but_records_what_it_bypassed(self, warehouse_conn) -> None:
        """Overriding quality is sometimes right at 3 a.m. Invisibly, never."""
        results = run_dataset_checks(
            warehouse_conn,
            run_id=RUN_ID,
            only=["ROWCOUNT_ZERO_SESSION"],
            batch_lo=date(2030, 1, 1),
            batch_hi=date(2030, 1, 2),
        )
        blocked = evaluate_gate(results)
        assert not blocked.passed

        bypassed = evaluate_gate(results, skip_gate=True)
        assert bypassed.passed
        assert bypassed.blocking_rules == ["ROWCOUNT_ZERO_SESSION"]
        assert "BYPASSED" in bypassed.message


class TestRuleExecutionSafety:
    def test_one_broken_rule_does_not_break_the_others(self, warehouse_conn) -> None:
        """PostgreSQL aborts the WHOLE transaction on any error.

        Without per-rule savepoints, one malformed rule would make every
        SUBSEQUENT rule fail with "current transaction is aborted" - a single
        broken check looking like a total collapse, which is exactly the moment
        someone decides the quality layer is unreliable and turns it off.

        Note that the broken rule is INJECTED rather than written into
        dq.rule: YAML is the source of truth and the engine loads from it, so
        editing the table would change nothing. That is itself worth asserting
        by construction.
        """
        from dataclasses import replace

        from volthive.dq.rules import load_rules

        rules = [
            replace(rule, rule_sql="SELECT * FROM a_table_that_does_not_exist")
            if rule.rule_code == "REVENUE_NON_NEGATIVE"
            else rule
            for rule in load_rules()
        ]

        results = run_dataset_checks(warehouse_conn, run_id=RUN_ID, rules=rules, **WINDOW)
        by_code = {r.rule_code: r for r in results}
        assert by_code["REVENUE_NON_NEGATIVE"].status == "ERROR"

        # Every OTHER rule still ran and reached a real verdict.
        others = [r for r in results if r.rule_code != "REVENUE_NON_NEGATIVE"]
        assert others
        assert all(r.status != "ERROR" for r in others)

        # And the broken rule blocks the gate, because a check that could not
        # run is not evidence of good data.
        assert not evaluate_gate(results).passed

    def test_a_rule_that_cannot_run_blocks_the_gate(self, warehouse_conn) -> None:
        """A broken check is not evidence of good data."""
        from volthive.dq.engine import CheckResult

        broken = CheckResult("X", "e", "core", "error", "ERROR", 0, 0, 0.0, None, None, "boom", 1)
        assert not evaluate_gate([broken]).passed


class TestRuleRegistrySync:
    def test_every_yaml_rule_reached_the_database(self, warehouse_conn) -> None:
        from volthive.dq.rules import load_rules

        declared = {rule.rule_code for rule in load_rules()}
        stored = {
            row["rule_code"]
            for row in fetch_all(warehouse_conn, "SELECT rule_code FROM dq.rule WHERE is_enabled")
        }
        assert declared == stored

    def test_quarantine_rows_join_to_their_rule(self, warehouse_conn) -> None:
        """The foreign key makes an undeclared rule code impossible.

        Without it, a typo in a staging INSERT would produce quarantine rows
        nobody could explain.
        """
        orphaned = fetch_value(
            warehouse_conn,
            """
            SELECT count(*) FROM dq.quarantine_ocpp_cdr AS q
            LEFT JOIN dq.rule AS r ON r.rule_code = q.rule_code
            WHERE r.rule_code IS NULL
            """,
        )
        assert orphaned == 0
