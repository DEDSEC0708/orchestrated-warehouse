#!/usr/bin/env python
"""Re-inject quarantined records into ``raw`` and restate them.

    python scripts/requeue_quarantine.py --rule CDR_MISSING_STOP --since 2026-06-01
    python scripts/requeue_quarantine.py --rule CDR_MISSING_STOP --since 2026-06-01 --dry-run

THIS SCRIPT IS WHAT TURNS "I HAVE A QUARANTINE TABLE" INTO "I HAVE A
QUARANTINE PROCESS".

A quarantine table nobody can act on is a landfill. Each row here holds the
COMPLETE ORIGINAL PAYLOAD, so a record can be replayed byte for byte once the
underlying problem is fixed - a corrected file arrives, a rule threshold is
adjusted, an upstream bug is resolved.

The lifecycle is NEW -> TRIAGED -> REQUEUED | WONTFIX, and this script performs
the REQUEUED transition: the payload goes back into raw with is_requeued=TRUE,
the quarantine row records which run reprocessed it, and a targeted restatement
picks it up.

Requeueing is the RIGHT tool for a rule like CDR_MISSING_STOP, where a later
file usually carries the completed record. It is the WRONG tool for
CDR_NEGATIVE_ENERGY, where the record is genuinely broken and replaying it
would simply quarantine it again - so the script warns rather than pretending.
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "src"))

from volthive.audit import open_pipeline_run
from volthive.db.connection import fetch_all, transaction, warehouse_connection
from volthive.logging_setup import configure_logging, get_logger

log = get_logger("requeue_quarantine")

#: Rules where replaying the SAME payload can plausibly succeed. Every other
#: rule describes a record that is broken on its own terms, and replaying it
#: unchanged would only quarantine it again.
REQUEUEABLE_RULES = {"CDR_MISSING_STOP", "MTR_ORPHAN_EXPIRED", "SCD_RETRO_DATED_CHANGE"}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rule", required=True, help="rule code to requeue")
    parser.add_argument("--since", required=True, help="YYYY-MM-DD")
    parser.add_argument("--limit", type=int, default=10000)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument(
        "--force",
        action="store_true",
        help="requeue a rule that is not in the requeueable set",
    )
    args = parser.parse_args()
    configure_logging(json_logs=False)

    if args.rule not in REQUEUEABLE_RULES and not args.force:
        print(
            f"{args.rule} is not in the requeueable set {sorted(REQUEUEABLE_RULES)}.\n"
            "Records rejected by that rule are broken on their own terms, so replaying\n"
            "the same payload would quarantine them again. Fix the source or the rule\n"
            "first, or pass --force if you know something has changed."
        )
        return 2

    since = date.fromisoformat(args.since)

    with warehouse_connection(application_name="requeue") as conn:
        candidates = fetch_all(
            conn,
            """
            SELECT quarantine_id, natural_key, dw_batch_key, raw_payload
            FROM dq.quarantine_ocpp_cdr
            WHERE rule_code = :rule
              AND dw_batch_key >= :since
              AND status IN ('NEW', 'TRIAGED')
            ORDER BY quarantine_id
            LIMIT :limit
            """,
            {"rule": args.rule, "since": since, "limit": args.limit},
        )

        print(f"{len(candidates)} quarantined record(s) match {args.rule} since {since}.")
        if args.dry_run:
            print(json.dumps(candidates[:5], indent=2, default=str))
            print("--dry-run: nothing was changed.")
            return 0
        if not candidates:
            return 0

        run_id = open_pipeline_run(
            conn,
            dag_id="volthive_requeue_quarantine",
            airflow_run_id=f"requeue__{args.rule}",
            data_interval_start_utc=fetch_all(conn, "SELECT now() AS n")[0]["n"],
            data_interval_end_utc=fetch_all(conn, "SELECT now() + INTERVAL '1 second' AS n")[0][
                "n"
            ],
            triggered_by="manual",
        )

        with transaction(conn), conn.cursor() as cur:
            for record in candidates:
                # is_requeued = TRUE marks the row's provenance for ever: it
                # came back from quarantine rather than from a landing file.
                # Without it, a raw layer that is supposed to be an immutable
                # record of what ARRIVED would quietly contain rows that never
                # did.
                cur.execute(
                    """
                    INSERT INTO raw.ocpp_cdr (
                        dw_run_id, dw_source_system, dw_source_file, dw_source_row_seq,
                        dw_batch_key, transaction_id, charge_point_id,
                        start_timestamp_txt, record_version, is_requeued,
                        payload, payload_hash
                    )
                    VALUES (
                        %(run_id)s, 'OCPP', 'requeue:' || %(quarantine_id)s, NULL,
                        %(batch_key)s,
                        %(payload)s ->> 'transaction_id',
                        %(payload)s ->> 'charge_point_id',
                        %(payload)s ->> 'start_timestamp',
                        %(payload)s ->> 'record_version',
                        TRUE,
                        %(payload)s,
                        encode(sha256(convert_to(%(payload)s::TEXT, 'UTF8')), 'hex')
                    )
                    """,
                    {
                        "run_id": run_id,
                        "quarantine_id": record["quarantine_id"],
                        "batch_key": record["dw_batch_key"],
                        "payload": json.dumps(record["raw_payload"]),
                    },
                )
            cur.execute(
                """
                UPDATE dq.quarantine_ocpp_cdr
                SET status = 'REQUEUED', requeued_run_id = %s
                WHERE quarantine_id = ANY(%s)
                """,
                (run_id, [r["quarantine_id"] for r in candidates]),
            )

    batch_keys = sorted({r["dw_batch_key"] for r in candidates})
    print(
        f"\nRequeued {len(candidates)} record(s) under run {run_id}.\n"
        f"Now restate the affected window so they reach the warehouse:\n\n"
        f"    python scripts/restate.py --from {batch_keys[0]} --to {batch_keys[-1]}\n"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
