"""Load data-quality rules from YAML and sync them into ``dq.rule``.

YAML is the source of truth. The table is a queryable projection of it, so a
check result or a quarantine row can join to the rule that produced it and pick
up its description and severity without the reader having to open a file.

**Why not just put the rules in the table?** Because a rule set that lives only
in a database has no history, no code review and no diff. Adding a check should
be a pull request, not an INSERT somebody ran once and cannot now explain.

**Why a custom engine at all, rather than Great Expectations or Soda?** Three
reasons, and the fourth is the honest one. (1) Explainability: this is a few
hundred lines that can be walked through in an interview, which a framework's
internals cannot. (2) Weight: the alternatives pull a large dependency tree and
slow CI meaningfully at this scale. (3) Fit: the requirement here is row-level
quarantine with payload preservation and a requeue lifecycle, which is not what
those tools model - they validate batches and report. (4) And the honest one:
**for a team pipeline I would evaluate Soda Core rather than maintain this.**
The custom engine exists because the goal is to demonstrate the mechanics and
because the quarantine lifecycle is a first-class requirement here.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import psycopg
import yaml

from volthive.config.settings import get_settings
from volthive.exceptions import ConfigurationError
from volthive.logging_setup import get_logger

__all__ = ["Rule", "load_rules", "sync_rules_to_database"]

log = get_logger(__name__)

_VALID_TYPES = {
    "not_null",
    "unique",
    "range",
    "accepted_values",
    "referential",
    "freshness",
    "row_count_anomaly",
    "reconciliation",
    "schema",
    "ratio",
    "cast",
    "rule",
}
_VALID_SCOPES = {"row", "dataset"}
_VALID_SEVERITIES = {"error", "warn"}
_VALID_LAYERS = {"raw", "stg", "core", "mart"}


@dataclass(frozen=True, slots=True)
class Rule:
    """One data-quality rule, as declared in YAML."""

    rule_code: str
    entity: str
    layer: str
    rule_type: str
    scope: str
    severity: str
    description: str
    rule_sql: str | None = None
    params: dict[str, Any] = field(default_factory=dict)
    is_enabled: bool = True

    @property
    def blocks_publish(self) -> bool:
        """Whether a failure of this rule stops the mart from publishing."""
        return self.severity == "error"


def _validate(rule: dict[str, Any], source: Path) -> Rule:
    """Turn one YAML mapping into a Rule, failing loudly on anything wrong.

    Strict on purpose. A rule with a typo in its ``rule_type`` that loaded as
    a no-op would report PASS for ever, which is the single worst failure mode
    a quality system can have: a check that is not running but looks like it
    is.
    """
    required = {"rule_code", "entity", "layer", "rule_type", "scope", "severity", "description"}
    missing = required - set(rule)
    if missing:
        raise ConfigurationError(
            f"Rule in {source.name} is missing required keys: {sorted(missing)}",
            entity=str(rule.get("rule_code", "<unnamed>")),
        )

    code = str(rule["rule_code"])
    for value, allowed, name in (
        (rule["rule_type"], _VALID_TYPES, "rule_type"),
        (rule["scope"], _VALID_SCOPES, "scope"),
        (rule["severity"], _VALID_SEVERITIES, "severity"),
        (rule["layer"], _VALID_LAYERS, "layer"),
    ):
        if value not in allowed:
            raise ConfigurationError(
                f"Rule {code} in {source.name} has invalid {name} '{value}'",
                entity=code,
                expected=sorted(allowed),
                actual=value,
            )

    if rule["scope"] == "dataset" and not rule.get("rule_sql"):
        raise ConfigurationError(
            f"Dataset-scope rule {code} has no rule_sql. A dataset rule without "
            "SQL would silently never run and would report PASS for ever.",
            entity=code,
        )

    return Rule(
        rule_code=code,
        entity=str(rule["entity"]),
        layer=str(rule["layer"]),
        rule_type=str(rule["rule_type"]),
        scope=str(rule["scope"]),
        severity=str(rule["severity"]),
        description=" ".join(str(rule["description"]).split()),
        rule_sql=rule.get("rule_sql"),
        params=dict(rule.get("params") or {}),
        is_enabled=bool(rule.get("is_enabled", True)),
    )


def load_rules(config_dir: Path | None = None) -> list[Rule]:
    """Load every rule from ``configs/dq/*.yml``.

    Raises:
        ConfigurationError: a rule is malformed, or two files declare the same
            rule code. A duplicate code is fatal rather than last-one-wins,
            because which file won would depend on directory order and the
            resulting severity would be a coin flip.
    """
    directory = config_dir or (get_settings().config_dir / "dq")
    if not directory.is_dir():
        raise ConfigurationError(
            f"Data-quality rule directory not found: {directory}",
            entity="configs/dq",
        )

    rules: dict[str, Rule] = {}
    for path in sorted(directory.glob("*.yml")):
        document = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        for raw_rule in document.get("rules", []):
            rule = _validate(raw_rule, path)
            if rule.rule_code in rules:
                raise ConfigurationError(
                    f"Duplicate rule code '{rule.rule_code}' in {path.name}",
                    entity=rule.rule_code,
                )
            rules[rule.rule_code] = rule

    if not rules:
        raise ConfigurationError(
            f"No data-quality rules found in {directory}. An empty rule set would "
            "mean every check passes vacuously.",
            entity="configs/dq",
        )
    return [rules[code] for code in sorted(rules)]


def sync_rules_to_database(conn: psycopg.Connection, config_dir: Path | None = None) -> int:
    """Upsert the YAML rule set into ``dq.rule`` and return how many.

    Upsert rather than truncate-and-reload: ``dq.check_result`` and every
    quarantine table hold foreign keys to ``dq.rule``, so deleting a rule would
    orphan the history of every row it ever rejected. A rule that disappears
    from YAML is DISABLED here rather than removed - its past verdicts stay
    joinable, and "we stopped checking this in March" remains answerable.
    """
    rules = load_rules(config_dir)
    import json

    with conn.cursor() as cur:
        for rule in rules:
            cur.execute(
                """
                INSERT INTO dq.rule (
                    rule_code, entity, layer, rule_type, scope, severity,
                    rule_sql, params, description, is_enabled, updated_at_utc
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s::JSONB, %s, %s, now())
                ON CONFLICT (rule_code) DO UPDATE SET
                    entity = EXCLUDED.entity,
                    layer = EXCLUDED.layer,
                    rule_type = EXCLUDED.rule_type,
                    scope = EXCLUDED.scope,
                    severity = EXCLUDED.severity,
                    rule_sql = EXCLUDED.rule_sql,
                    params = EXCLUDED.params,
                    description = EXCLUDED.description,
                    is_enabled = EXCLUDED.is_enabled,
                    updated_at_utc = now()
                """,
                (
                    rule.rule_code,
                    rule.entity,
                    rule.layer,
                    rule.rule_type,
                    rule.scope,
                    rule.severity,
                    rule.rule_sql,
                    json.dumps(rule.params),
                    rule.description,
                    rule.is_enabled,
                ),
            )

        codes = [rule.rule_code for rule in rules]
        cur.execute(
            """
            UPDATE dq.rule
            SET is_enabled = FALSE, updated_at_utc = now()
            WHERE rule_code <> ALL(%s) AND is_enabled
            """,
            (codes,),
        )
        retired = cur.rowcount or 0

    if retired:
        log.warning("dq_rules_retired", count=retired)
    log.info("dq_rules_synced", count=len(rules))
    return len(rules)
