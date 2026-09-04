#!/usr/bin/env python
"""Generate the synthetic source dataset.

    python scripts/generate_data.py                    # the configured profile
    python scripts/generate_data.py --profile tiny     # 7 days, for tests
    python scripts/generate_data.py --profile small    # 3 months, 2 cities
    python scripts/generate_data.py --no-cms           # landing files only

Deterministic: the same seed produces byte-identical output, so two people
running this project get identical warehouses and the tests can assert exact
counts rather than tolerances.

Writes a ground-truth manifest to ``data/_truth/expected_defects.json``
recording exactly how many of each defect were injected. The integration tests
assert quarantine counts against THAT FILE - an oracle computed independently
of the pipeline - rather than against the pipeline's own output.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "src"))

from volthive.generator import generate_all
from volthive.logging_setup import configure_logging, get_logger

log = get_logger("generate_data")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", default=None, help="default | small | tiny | clean")
    parser.add_argument("--seed", type=int, default=None, help="override the configured seed")
    parser.add_argument("--data-dir", default=None, help="landing zone root")
    parser.add_argument(
        "--no-cms",
        action="store_true",
        help="write landing files only, skipping the simulated source database",
    )
    args = parser.parse_args()

    configure_logging(json_logs=False)
    result = generate_all(
        args.profile,
        seed=args.seed,
        data_dir=Path(args.data_dir) if args.data_dir else None,
        load_cms=not args.no_cms,
    )

    print(
        json.dumps(
            {
                "profile": result.profile,
                "seed": result.seed,
                "window": f"{result.start_date} .. {result.end_date}",
                "sessions": result.ledger.totals.get("sessions_emitted", 0),
                "cdr_records": result.ledger.totals.get("cdr_records_emitted", 0),
                "meter_samples": result.ledger.totals.get("meter_samples_emitted", 0),
                "partner_records": result.ledger.totals.get("partner_records_emitted", 0),
                "files": {
                    "cdr": result.cdr_files,
                    "meter": result.meter_files,
                    "partner": result.partner_files,
                },
                "master_rows": result.master_rows,
                "defects_injected": dict(result.ledger.counts),
                "duration_seconds": round(result.duration_seconds, 2),
                "truth_manifest": str(result.truth_manifest),
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
