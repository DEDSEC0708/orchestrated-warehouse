"""Execute dataset-level quality rules and enforce the publish gate.

Two responsibilities, deliberately separated:

:func:`run_dataset_checks`
    runs every enabled dataset rule, records a row in ``dq.check_result``, and
    returns the results. It never raises on a failing rule - a check that
    crashes the task it is checking is a check nobody keeps.

:func:`evaluate_gate`
    reads those results and decides whether the mart may publish.

Keeping them apart matters operationally: the checks are still recorded when
the gate blocks, so the person triaging at 9 a.m. can see exactly which rules
failed and by how much, rather than only that something did.

**The gate's trade-off, stated plainly: STALE BUT CORRECT BEATS FRESH BUT
WRONG.** When an error-severity rule fails, the mart tasks do not run, so
consumers keep yesterday's correct data instead of receiving today's wrong
data. Core stays loaded and inspectable for debugging. That is a real cost -
someone's dashboard is a day behind - and it is the right cost.

A rule that itself throws is recorded with status ERROR and treated as a
FAILURE, not as a pass. A broken check is not evidence of good data.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import date
from typing import Any

import psycopg

from volthive.db.connection import fetch_one
from volthive.dq.rules import Rule, load_rules
from volthive.exceptions import DataError
from volthive.logging_setup import get_logger

__all__ = ["CheckResult", "GateDecision", "evaluate_gate", "run_dataset_checks"]

log = get_logger(__name__)


@dataclass(slots=True)
class CheckResult:
    """The outcome of one rule for one run."""

    rule_code: str
    entity: str
    layer: str
    severity: str
    status: str
    rows_evaluated: int
    rows_failed: int
    fail_ratio: float
    observed_value: float | None
    threshold_value: float | None
    message: str
    duration_ms: int

    @property
    def blocks(self) -> bool:
        return self.severity == "error" and self.status in {"FAIL", "ERROR"}


@dataclass(slots=True)
class GateDecision:
    """Whether the mart may publish, and why not if not."""

    passed: bool
    blocking_rules: list[str]
    warned_rules: list[str]
    checks_run: int
    message: str


def _judge(rule: Rule, row: dict[str, Any]) -> tuple[str, float | None, str]:
    """Apply a rule's thresholds to its measured result.

    Returns ``(status, threshold, message)``.

    Three independent ways to fail, any of which is enough:

    * ``rows_failed`` above ``max_failed_rows`` (default 0)
    * ``fail_ratio`` above ``max_fail_ratio``
    * ``observed_value`` outside ``[min_value, max_value]``

    ``rows_evaluated = 0`` is a PASS, deliberately. An empty window is a job
    for the freshness and zero-row rules, which say so explicitly; having every
    other rule also complain would bury the one that matters.
    """
    rows_evaluated = int(row.get("rows_evaluated") or 0)
    rows_failed = int(row.get("rows_failed") or 0)
    observed = row.get("observed_value")
    observed_value = float(observed) if observed is not None else None
    params = rule.params

    max_fail_ratio = params.get("max_fail_ratio")
    min_value = params.get("min_value")
    max_value = params.get("max_value")
    # A rule that declares a threshold is judged BY THAT THRESHOLD, not by the
    # default "any failing row is a failure". Getting this wrong makes every
    # ratio rule fire on its first non-zero count and turns a tolerance into a
    # zero-tolerance check - which is how a WARN meant to flag a trend becomes
    # an ERROR that blocks the mart every night.
    has_threshold = max_fail_ratio is not None or min_value is not None or max_value is not None
    max_failed_rows = float(params.get("max_failed_rows", 0))

    ratio = rows_failed / rows_evaluated if rows_evaluated else 0.0
    reasons: list[str] = []
    threshold: float | None = None

    if rows_failed > max_failed_rows and not has_threshold:
        reasons.append(f"{rows_failed} failing rows (limit {max_failed_rows:g})")
        threshold = max_failed_rows
    if max_fail_ratio is not None and ratio > float(max_fail_ratio):
        reasons.append(f"fail ratio {ratio:.4f} above {float(max_fail_ratio):.4f}")
        threshold = float(max_fail_ratio)
    if observed_value is not None and max_value is not None and observed_value > float(max_value):
        reasons.append(f"observed {observed_value:g} above {float(max_value):g}")
        threshold = float(max_value)
    if observed_value is not None and min_value is not None and observed_value < float(min_value):
        reasons.append(f"observed {observed_value:g} below {float(min_value):g}")
        threshold = float(min_value)

    if not reasons:
        return "PASS", threshold, f"{rows_evaluated} rows evaluated, {rows_failed} failed"

    status = "FAIL" if rule.severity == "error" else "WARN"
    return status, threshold, "; ".join(reasons)


def run_dataset_checks(
    conn: psycopg.Connection,
    *,
    run_id: str,
    batch_lo: date,
    batch_hi: date,
    only: list[str] | None = None,
    layers: list[str] | None = None,
    rules: list[Rule] | None = None,
) -> list[CheckResult]:
    """Run every enabled dataset rule and record the results.

    Args:
        only: Run just these rule codes. Used by tests that need to assert one
            rule's behaviour without waiting for the whole suite.
        layers: Restrict to rules on these layers, so the DAG can check staging
            immediately after staging rather than only at the end - a failure
            found early is a failure found before the expensive fact load.
        rules: Override the rule set entirely. Rules are normally loaded from
            YAML - which is the SOURCE OF TRUTH, so editing ``dq.rule`` in the
            database changes nothing - and this parameter is how a test
            exercises a deliberately-malformed rule without having to write one
            into the version-controlled configuration.

    Returns:
        One result per rule executed. Never raises because a rule failed; the
        gate decides what a failure means.
    """
    rules = [
        r
        for r in (rules if rules is not None else load_rules())
        if r.scope == "dataset" and r.is_enabled
    ]
    if only:
        wanted = set(only)
        rules = [r for r in rules if r.rule_code in wanted]
    if layers:
        allowed = set(layers)
        rules = [r for r in rules if r.layer in allowed]

    results: list[CheckResult] = []
    params = {"run_id": run_id, "batch_lo": batch_lo, "batch_hi": batch_hi}

    for rule in rules:
        started = time.monotonic()
        try:
            # Each rule runs inside its OWN SAVEPOINT.
            #
            # PostgreSQL aborts the whole transaction on any error, so a single
            # malformed rule would otherwise make every SUBSEQUENT rule fail
            # with "current transaction is aborted" - one broken check looking
            # like a total collapse. A savepoint confines the damage to the
            # rule that caused it.
            #
            # A plain conn.rollback() would also clear the error, but it would
            # discard the CALLER'S transaction too - including any work done
            # before the checks ran. Nested transactions in psycopg are
            # savepoints, which is exactly the scope needed.
            with conn.transaction():
                row = fetch_one(conn, rule.rule_sql or "", params) or {}
            status, threshold, message = _judge(rule, row)
            rows_evaluated = int(row.get("rows_evaluated") or 0)
            rows_failed = int(row.get("rows_failed") or 0)
            observed = row.get("observed_value")
            observed_value = float(observed) if observed is not None else None
        except psycopg.Error as exc:
            # A rule that cannot execute is recorded as ERROR and treated as a
            # FAILURE. A broken check is not evidence of good data, and
            # swallowing it would turn a schema drift into a silent green tick.
            status, threshold, observed_value = "ERROR", None, None
            rows_evaluated = rows_failed = 0
            message = f"rule failed to execute: {exc}"
            log.error("dq_rule_execution_failed", rule_code=rule.rule_code, error=str(exc))

        duration_ms = int((time.monotonic() - started) * 1000)
        ratio = rows_failed / rows_evaluated if rows_evaluated else 0.0
        result = CheckResult(
            rule_code=rule.rule_code,
            entity=rule.entity,
            layer=rule.layer,
            severity=rule.severity,
            status=status,
            rows_evaluated=rows_evaluated,
            rows_failed=rows_failed,
            fail_ratio=round(ratio, 6),
            observed_value=observed_value,
            threshold_value=threshold,
            message=message,
            duration_ms=duration_ms,
        )
        results.append(result)

        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO dq.check_result (
                    dw_run_id, rule_code, entity, layer, dw_batch_key,
                    rows_evaluated, rows_failed, fail_ratio,
                    threshold_value, observed_value, status, severity,
                    message, duration_ms
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    run_id,
                    rule.rule_code,
                    rule.entity,
                    rule.layer,
                    batch_hi,
                    rows_evaluated,
                    rows_failed,
                    round(ratio, 6),
                    threshold,
                    observed_value,
                    status,
                    rule.severity,
                    message,
                    duration_ms,
                ),
            )

        log_at = log.info if status == "PASS" else log.warning
        log_at(
            "dq_check_completed",
            rule_code=rule.rule_code,
            status=status,
            rows_evaluated=rows_evaluated,
            rows_failed=rows_failed,
            observed_value=observed_value,
            message=message,
        )

    return results


def evaluate_gate(
    results: list[CheckResult],
    *,
    skip_gate: bool = False,
) -> GateDecision:
    """Decide whether the mart may publish.

    Args:
        skip_gate: The emergency override. It does NOT make the gate pass
            silently - the decision records that it was bypassed and which
            rules it was bypassed over, and the caller logs it loudly and
            writes it to ``audit.pipeline_run.error_summary``. Bypassing
            quality is sometimes necessary; doing it invisibly never is.
    """
    blocking = [r.rule_code for r in results if r.blocks]
    warned = [r.rule_code for r in results if r.status == "WARN"]

    if skip_gate and blocking:
        return GateDecision(
            passed=True,
            blocking_rules=blocking,
            warned_rules=warned,
            checks_run=len(results),
            message=(
                "DQ GATE BYPASSED by params.skip_dq_gate. "
                f"Failing error-severity rules: {', '.join(blocking)}. "
                "The mart has published data that did not pass quality checks."
            ),
        )

    if blocking:
        return GateDecision(
            passed=False,
            blocking_rules=blocking,
            warned_rules=warned,
            checks_run=len(results),
            message=(
                f"DQ gate failed on {len(blocking)} error-severity rule(s): "
                f"{', '.join(blocking)}. The mart was NOT refreshed - consumers keep "
                "yesterday's correct data rather than receiving today's wrong data."
            ),
        )

    return GateDecision(
        passed=True,
        blocking_rules=[],
        warned_rules=warned,
        checks_run=len(results),
        message=(f"DQ gate passed: {len(results)} checks, {len(warned)} warning(s)."),
    )


def raise_if_blocked(decision: GateDecision) -> None:
    """Raise when the gate blocked, for use as an Airflow task body."""
    if not decision.passed:
        raise DataError(decision.message, rule_code=",".join(decision.blocking_rules))
