"""The data-quality rule registry and threshold judgement.

The rule loader is deliberately strict, and the tests below are mostly about
that strictness: a rule that fails to load correctly and reports PASS for ever
is the single worst failure mode a quality system can have, because it looks
exactly like a healthy one.
"""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from volthive.dq.engine import CheckResult, _judge, evaluate_gate
from volthive.dq.rules import Rule, load_rules
from volthive.exceptions import ConfigurationError

pytestmark = pytest.mark.unit

REPO_ROOT = Path(__file__).resolve().parents[2]


def _rule(**overrides) -> Rule:
    base = {
        "rule_code": "TEST_RULE",
        "entity": "stg.session",
        "layer": "stg",
        "rule_type": "range",
        "scope": "dataset",
        "severity": "error",
        "description": "test",
        "rule_sql": "SELECT 1 AS rows_evaluated, 0 AS rows_failed",
        "params": {},
    }
    return Rule(**{**base, **overrides})


def _write(directory: Path, name: str, rules: list[dict]) -> None:
    (directory / name).write_text(yaml.safe_dump({"rules": rules}), encoding="utf-8")


class TestRuleLoading:
    def test_project_rules_load(self) -> None:
        rules = load_rules(REPO_ROOT / "configs" / "dq")
        assert len(rules) > 20

    def test_every_dataset_rule_has_sql(self) -> None:
        """A dataset rule without SQL would silently never run and always pass."""
        for rule in load_rules(REPO_ROOT / "configs" / "dq"):
            if rule.scope == "dataset":
                assert rule.rule_sql, rule.rule_code

    def test_unknown_rule_type_is_rejected(self, tmp_path: Path) -> None:
        _write(
            tmp_path,
            "bad.yml",
            [
                {
                    "rule_code": "X",
                    "entity": "e",
                    "layer": "stg",
                    "rule_type": "not_a_real_type",
                    "scope": "row",
                    "severity": "error",
                    "description": "d",
                }
            ],
        )
        with pytest.raises(ConfigurationError, match="invalid rule_type"):
            load_rules(tmp_path)

    def test_unknown_severity_is_rejected(self, tmp_path: Path) -> None:
        _write(
            tmp_path,
            "bad.yml",
            [
                {
                    "rule_code": "X",
                    "entity": "e",
                    "layer": "stg",
                    "rule_type": "range",
                    "scope": "row",
                    "severity": "critical",
                    "description": "d",
                }
            ],
        )
        with pytest.raises(ConfigurationError, match="invalid severity"):
            load_rules(tmp_path)

    def test_dataset_rule_without_sql_is_rejected(self, tmp_path: Path) -> None:
        _write(
            tmp_path,
            "bad.yml",
            [
                {
                    "rule_code": "X",
                    "entity": "e",
                    "layer": "stg",
                    "rule_type": "range",
                    "scope": "dataset",
                    "severity": "error",
                    "description": "d",
                }
            ],
        )
        with pytest.raises(ConfigurationError, match="no rule_sql"):
            load_rules(tmp_path)

    def test_duplicate_rule_code_is_fatal(self, tmp_path: Path) -> None:
        """Last-one-wins would make severity depend on directory order."""
        rule = {
            "rule_code": "DUP",
            "entity": "e",
            "layer": "stg",
            "rule_type": "range",
            "scope": "row",
            "severity": "error",
            "description": "d",
        }
        _write(tmp_path, "a.yml", [rule])
        _write(tmp_path, "b.yml", [rule])
        with pytest.raises(ConfigurationError, match="Duplicate rule code"):
            load_rules(tmp_path)

    def test_empty_rule_set_is_fatal(self, tmp_path: Path) -> None:
        """An empty rule set means every check passes vacuously."""
        with pytest.raises(ConfigurationError, match="No data-quality rules"):
            load_rules(tmp_path)


class TestThresholdJudgement:
    def test_zero_failures_passes(self) -> None:
        status, _, _ = _judge(_rule(), {"rows_evaluated": 100, "rows_failed": 0})
        assert status == "PASS"

    def test_any_failure_fails_a_rule_with_no_threshold(self) -> None:
        status, _, _ = _judge(_rule(), {"rows_evaluated": 100, "rows_failed": 1})
        assert status == "FAIL"

    def test_a_warn_rule_warns_rather_than_failing(self) -> None:
        status, _, _ = _judge(_rule(severity="warn"), {"rows_evaluated": 100, "rows_failed": 1})
        assert status == "WARN"

    def test_a_ratio_rule_is_judged_by_its_threshold_not_by_row_count(self) -> None:
        """The bug this guards: a tolerance behaving as zero tolerance.

        A rule that declares max_value must not also fail on "any failing row",
        or a WARN meant to flag a trend blocks the mart every night.
        """
        status, _, _ = _judge(
            _rule(params={"max_value": 20.0}),
            {"rows_evaluated": 100, "rows_failed": 5, "observed_value": 5.0},
        )
        assert status == "PASS"

    def test_observed_value_above_the_maximum_fails(self) -> None:
        status, threshold, message = _judge(
            _rule(params={"max_value": 20.0}),
            {"rows_evaluated": 100, "rows_failed": 25, "observed_value": 25.0},
        )
        assert status == "FAIL"
        assert threshold == 20.0
        assert "25" in message

    def test_fail_ratio_above_the_maximum_fails(self) -> None:
        status, _, _ = _judge(
            _rule(params={"max_fail_ratio": 0.05}),
            {"rows_evaluated": 100, "rows_failed": 10},
        )
        assert status == "FAIL"

    def test_empty_window_passes(self) -> None:
        """Zero rows is a job for the freshness and zero-row rules.

        Having every other rule also complain would bury the one that matters.
        """
        status, _, _ = _judge(_rule(), {"rows_evaluated": 0, "rows_failed": 0})
        assert status == "PASS"


def _result(code: str, severity: str, status: str) -> CheckResult:
    return CheckResult(code, "e", "core", severity, status, 1, 0, 0.0, None, None, "", 1)


class TestGate:
    def test_all_passing_opens_the_gate(self) -> None:
        decision = evaluate_gate([_result("A", "error", "PASS")])
        assert decision.passed

    def test_an_error_severity_failure_blocks(self) -> None:
        decision = evaluate_gate([_result("A", "error", "FAIL")])
        assert not decision.passed
        assert decision.blocking_rules == ["A"]

    def test_a_warn_severity_failure_does_not_block(self) -> None:
        decision = evaluate_gate([_result("A", "warn", "WARN")])
        assert decision.passed
        assert decision.warned_rules == ["A"]

    def test_a_rule_that_could_not_execute_blocks(self) -> None:
        """A broken check is not evidence of good data."""
        decision = evaluate_gate([_result("A", "error", "ERROR")])
        assert not decision.passed

    def test_the_bypass_records_what_it_bypassed(self) -> None:
        """Overriding quality is sometimes right. Doing it invisibly never is."""
        decision = evaluate_gate([_result("A", "error", "FAIL")], skip_gate=True)
        assert decision.passed
        assert decision.blocking_rules == ["A"]
        assert "BYPASSED" in decision.message


class TestRuleCatalogueCoverage:
    def test_every_rule_code_used_in_sql_is_declared(self) -> None:
        """A quarantine INSERT names a rule_code, and dq.rule has a foreign key.

        An undeclared code would fail the staging load at runtime with a
        constraint violation. Catching it here turns a 2 a.m. incident into a
        red test.
        """
        import re

        declared = {rule.rule_code for rule in load_rules(REPO_ROOT / "configs" / "dq")}
        used: set[str] = set()
        for path in (REPO_ROOT / "sql").rglob("*.sql"):
            text = path.read_text(encoding="utf-8")
            used |= set(re.findall(r"'((?:CDR|MTR|CMS|PTR|SCD|FACT|DIM|RECON)_[A-Z_]+)'", text))
        assert used <= declared, f"used in SQL but not declared: {sorted(used - declared)}"

    def test_every_injected_defect_maps_to_a_declared_rule(self) -> None:
        """A defect the generator injects with no rule to catch it is a blind spot."""
        from volthive.generator.defects import DEFECT_CATALOGUE

        declared = {rule.rule_code for rule in load_rules(REPO_ROOT / "configs" / "dq")}
        for defect, meta in DEFECT_CATALOGUE.items():
            if meta["rule"] != "-":
                assert meta["rule"] in declared, f"{defect} names an undeclared rule"
