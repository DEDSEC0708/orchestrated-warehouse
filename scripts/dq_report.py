#!/usr/bin/env python
"""Render the data-quality scorecard as Markdown.

    python scripts/dq_report.py
    python scripts/dq_report.py --days 7 --output docs/dq_scorecard.md

The output is pasted into the README so a reviewer sees the quality posture
WITHOUT running anything. Someone who will not clone the repository will still
read a table showing which checks ran, which failed, and how many records were
quarantined under which rule.

Every number here is measured from the database. Nothing is estimated, rounded
up, or aspirational - a hiring manager who spots one invented metric discounts
everything else in the project.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "src"))

from volthive.db.connection import fetch_all, warehouse_connection
from volthive.logging_setup import configure_logging


def _table(rows: list[dict], columns: list[tuple[str, str]]) -> str:
    """Render rows as a GitHub-flavoured Markdown table."""
    if not rows:
        return "_No data._\n"
    header = "| " + " | ".join(label for _, label in columns) + " |"
    divider = "|" + "|".join("---" for _ in columns) + "|"
    body = [
        "| "
        + " | ".join(
            str(row.get(key, "") if row.get(key) is not None else "") for key, _ in columns
        )
        + " |"
        for row in rows
    ]
    return "\n".join([header, divider, *body]) + "\n"


def build_report(days: int) -> str:
    with warehouse_connection(application_name="dq_report") as conn:
        checks = fetch_all(
            conn,
            """
            SELECT
                c.rule_code,
                c.severity,
                count(*)                                              AS runs,
                count(*) FILTER (WHERE c.status = 'PASS')             AS passed,
                count(*) FILTER (WHERE c.status = 'WARN')             AS warned,
                count(*) FILTER (WHERE c.status IN ('FAIL', 'ERROR')) AS failed,
                max(c.rows_evaluated)                                 AS max_rows_evaluated,
                max(c.message) FILTER (WHERE c.status <> 'PASS')      AS last_message
            FROM dq.check_result AS c
            WHERE c.checked_at_utc >= now() - :days::INT * INTERVAL '1 day'
            GROUP BY c.rule_code, c.severity
            ORDER BY
                count(*) FILTER (WHERE c.status IN ('FAIL', 'ERROR')) DESC,
                count(*) FILTER (WHERE c.status = 'WARN') DESC,
                c.rule_code
            """,
            {"days": days},
        )
        quarantine = fetch_all(
            conn,
            """
            SELECT
                q.source_entity,
                q.rule_code,
                sum(q.quarantined_rows) AS rows_quarantined,
                sum(q.untriaged_rows)   AS untriaged,
                sum(q.requeued_rows)    AS requeued,
                min(q.oldest_untriaged_at_utc)::DATE AS oldest_untriaged
            FROM mart.v_quarantine_summary AS q
            GROUP BY q.source_entity, q.rule_code
            ORDER BY sum(q.quarantined_rows) DESC
            """,
        )
        volumes = fetch_all(
            conn,
            """
            SELECT
                'raw'   AS layer, 'ocpp_cdr' AS entity, count(*) AS rows FROM raw.ocpp_cdr
            UNION ALL SELECT 'raw', 'meter_value', count(*) FROM raw.meter_value
            UNION ALL SELECT 'raw', 'partner_cdr', count(*) FROM raw.partner_cdr
            UNION ALL SELECT 'stg', 'session', count(*) FROM stg.session
            UNION ALL SELECT 'core', 'fact_charging_session',
                count(*) FROM core.fact_charging_session
            UNION ALL SELECT 'core', 'fact_meter_interval',
                count(*) FROM core.fact_meter_interval
            UNION ALL SELECT 'core', 'fact_station_daily_utilization',
                count(*) FROM core.fact_station_daily_utilization
            ORDER BY 1, 2
            """,
        )

    blocking = sum(1 for row in checks if row["severity"] == "error" and row["failed"])
    verdict = (
        "**Gate status: BLOCKING** - "
        f"{blocking} error-severity rule(s) failed in the last {days} days."
        if blocking
        else f"**Gate status: clear** - no error-severity rule failed in the last {days} days."
    )

    return "\n".join(
        [
            "# Data quality scorecard",
            "",
            f"Measured from `dq.check_result` over the last {days} days. "
            "Every number here comes from the database.",
            "",
            verdict,
            "",
            "## Dataset checks",
            "",
            _table(
                checks,
                [
                    ("rule_code", "Rule"),
                    ("severity", "Severity"),
                    ("runs", "Runs"),
                    ("passed", "Pass"),
                    ("warned", "Warn"),
                    ("failed", "Fail"),
                    ("max_rows_evaluated", "Rows evaluated"),
                    ("last_message", "Last non-pass message"),
                ],
            ),
            "## Quarantine",
            "",
            "Rejected records, preserved with their complete original payloads. "
            "Nothing is ever silently dropped.",
            "",
            _table(
                quarantine,
                [
                    ("source_entity", "Entity"),
                    ("rule_code", "Rule"),
                    ("rows_quarantined", "Rows"),
                    ("untriaged", "Untriaged"),
                    ("requeued", "Requeued"),
                    ("oldest_untriaged", "Oldest untriaged"),
                ],
            ),
            "## Row counts by layer",
            "",
            _table(volumes, [("layer", "Layer"), ("entity", "Entity"), ("rows", "Rows")]),
        ]
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--days", type=int, default=30)
    parser.add_argument("--output", default=None, help="write to a file instead of stdout")
    args = parser.parse_args()
    configure_logging(json_logs=False, level="WARNING")

    report = build_report(args.days)
    if args.output:
        path = Path(args.output)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(report, encoding="utf-8")
        print(f"Wrote {path}")
    else:
        print(report)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
