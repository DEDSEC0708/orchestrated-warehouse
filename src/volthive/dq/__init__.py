"""The YAML-driven data-quality engine, rule registry and quarantine writer.

Two scopes, two behaviours, and neither ever deletes a row:

**Row-level** rules run inside the staging loads, where the record is already
parsed and its original payload is still to hand. A failing record goes to a
``dq.quarantine_*`` table with its rule code, a human-readable detail
containing the actual values, and its complete payload - so it can be triaged,
fixed and replayed byte for byte. Valid records continue.

**Dataset-level** rules run after each layer. Results land in
``dq.check_result``; a ``warn`` is recorded, an ``error`` blocks the mart
publish. The trade-off is deliberate and stated in the README: stale but
correct beats fresh but wrong.

The rules themselves are YAML under ``configs/dq/``, so adding a check is a
pull request rather than a code change.
"""

from __future__ import annotations

from volthive.dq.engine import (
    CheckResult,
    GateDecision,
    evaluate_gate,
    raise_if_blocked,
    run_dataset_checks,
)
from volthive.dq.rules import Rule, load_rules, sync_rules_to_database

__all__ = [
    "CheckResult",
    "GateDecision",
    "Rule",
    "evaluate_gate",
    "load_rules",
    "raise_if_blocked",
    "run_dataset_checks",
    "sync_rules_to_database",
]
