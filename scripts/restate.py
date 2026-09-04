#!/usr/bin/env python
"""Rebuild a date range from ``raw``, without re-reading any source.

    python scripts/restate.py --from 2026-06-01 --to 2026-06-05
    python scripts/restate.py --from 2026-06-01 --to 2026-06-05 --dry-run

THIS SCRIPT IS THE PAYOFF FOR KEEPING RAW IMMUTABLE.

When a transformation was wrong for the last thirty days, the fix is to replay
from raw over that window. No source system is contacted, no file is
re-fetched, and it does not matter whether the OCPP central system still holds
June's records - the landing zone does, byte for byte, and so does the raw
layer.

That is the single most important architectural consequence of choosing
EL-then-T over ETL, and it is the answer to "what do you do when a
transformation was wrong for a month?".

Deliberately does NOT re-run the dimension merges. A restatement fixes FACTS
by re-resolving them against the dimension history that already exists, which
is correct. Rebuilding dimension history is a different, more dangerous
operation with its own script.
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "src"))

from run_pipeline import run

from volthive.db.connection import fetch_all, warehouse_connection
from volthive.logging_setup import configure_logging, get_logger

log = get_logger("restate")


def preview(date_from: date, date_to: date) -> list[dict]:
    """Report what a restatement WOULD rewrite, before it rewrites it."""
    with warehouse_connection(application_name="restate.preview") as conn:
        return fetch_all(
            conn,
            """
            SELECT
                f.dw_batch_key,
                count(*)                             AS sessions,
                round(sum(f.energy_delivered_kwh), 3) AS energy_kwh,
                round(sum(f.gross_revenue_inr), 2)    AS revenue_inr,
                count(DISTINCT f.dw_run_id)          AS distinct_runs
            FROM core.fact_charging_session AS f
            WHERE f.dw_batch_key BETWEEN :date_from AND :date_to
            GROUP BY f.dw_batch_key
            ORDER BY f.dw_batch_key
            """,
            {"date_from": date_from, "date_to": date_to},
        )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--from", dest="date_from", required=True)
    parser.add_argument("--to", dest="date_to", required=True)
    parser.add_argument("--lookback-days", type=int, default=0)
    parser.add_argument(
        "--dry-run", action="store_true", help="report what would be rewritten and stop"
    )
    args = parser.parse_args()
    configure_logging(json_logs=False)

    date_from = date.fromisoformat(args.date_from)
    date_to = date.fromisoformat(args.date_to)

    current = preview(date_from, date_to)
    print("Currently in the window:")
    print(json.dumps(current, indent=2, default=str))
    if args.dry_run:
        print("\n--dry-run: nothing was changed.")
        return 0

    # No 'ingest' stage: restatement REPLAYS FROM RAW. Re-reading the sources
    # would defeat the purpose and, for a source that no longer holds the data,
    # would simply fail.
    summary = run(
        date_from=date_from,
        date_to=date_to,
        stages=["stage", "core", "dq", "mart"],
        lookback_days=args.lookback_days,
        skip_dq_gate=False,
        is_backfill=True,
    )
    print("\nAfter restatement:")
    print(json.dumps(preview(date_from, date_to), indent=2, default=str))
    print(json.dumps(summary, indent=2, sort_keys=True, default=str))
    return 0 if summary["status"] == "SUCCESS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
