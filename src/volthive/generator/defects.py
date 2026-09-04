"""Defect injection and the ground-truth ledger.

Every defect this generator injects is a REAL, DOCUMENTED failure mode of OCPP
charging telemetry - not a contrived error made up to give the quality layer
something to catch. Meter registers really do reset. Chargers really do lose
connectivity mid-session and never send a stop. Device clocks really do drift.
Older firmware really does report Wh where newer firmware reports kWh. That is
what makes the quarantine story credible rather than decorative.

The ledger below is the other half of the idea. As defects are injected their
counts are recorded, and the totals are written to
``data/_truth/expected_defects.json``. Tests then assert the pipeline's
quarantine counts against THAT FILE - an oracle derived independently of the
pipeline - rather than against the pipeline's own output.

The distinction matters more than it looks. A test that checks the warehouse
against itself passes just as happily when the logic is uniformly wrong. A test
that checks it against a separately-computed expectation does not.
"""

from __future__ import annotations

import json
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

__all__ = ["DefectLedger", "TRUTH_FILENAME"]

TRUTH_FILENAME = "expected_defects.json"


@dataclass(slots=True)
class DefectLedger:
    """Counts of everything the generator deliberately did wrong.

    Also records the honest totals - sessions emitted, files written - so a
    test can assert the reconciliation identity
    ``raw = staged + quarantined + duplicates`` against numbers that were never
    derived from the warehouse.
    """

    counts: Counter[str] = field(default_factory=Counter)
    totals: Counter[str] = field(default_factory=Counter)

    def record(self, defect: str, count: int = 1) -> None:
        """Record that ``count`` instances of ``defect`` were injected."""
        self.counts[defect] += count

    def count_total(self, name: str, count: int = 1) -> None:
        """Record a non-defect total, such as rows emitted or files written."""
        self.totals[name] += count

    def as_dict(self) -> dict[str, Any]:
        """Render as the JSON structure written to the truth manifest."""
        return {
            "defects": dict(sorted(self.counts.items())),
            "totals": dict(sorted(self.totals.items())),
        }

    def write(self, directory: Path, *, metadata: dict[str, Any] | None = None) -> Path:
        """Write the manifest to ``<directory>/expected_defects.json``.

        Sorted keys and a trailing newline, so that regenerating with the same
        seed produces a byte-identical file - which is what the determinism
        test actually compares.
        """
        directory.mkdir(parents=True, exist_ok=True)
        path = directory / TRUTH_FILENAME
        document = {"metadata": metadata or {}, **self.as_dict()}
        path.write_text(
            json.dumps(document, indent=2, sort_keys=True, default=str) + "\n",
            encoding="utf-8",
        )
        return path

    @staticmethod
    def read(directory: Path) -> dict[str, Any]:
        """Read a previously written manifest."""
        return json.loads((directory / TRUTH_FILENAME).read_text(encoding="utf-8"))


# ---------------------------------------------------------------------------
# The catalogue below maps each injected defect to the data-quality rule that
# is expected to catch it, and to what the pipeline should DO about it.
#
# It is deliberately data rather than prose: a test asserts that every defect
# named here has a matching enabled rule in dq.rule, so adding a defect without
# a rule to catch it - or deleting a rule that a defect still produces - fails
# the build instead of quietly leaving a blind spot.
#
# `handling` values:
#   quarantine  the row is rejected, preserved with its payload, and counted
#   conform     the row is REPAIRED in staging and kept (a unit mismatch is not
#               an error, it is a dialect)
#   dedupe      an expected protocol behaviour, counted but never treated as an
#               error - OCPP retry storms are normal
#   infer       a missing dimension member is created rather than the fact
#               being dropped
#   warn        recorded and reported, but the row is kept and nothing blocks
# ---------------------------------------------------------------------------
DEFECT_CATALOGUE: dict[str, dict[str, str]] = {
    "cdr_exact_duplicate": {"rule": "-", "handling": "dedupe"},
    "cdr_corrected_version": {"rule": "-", "handling": "dedupe"},
    "cdr_negative_energy": {"rule": "CDR_NEGATIVE_ENERGY", "handling": "quarantine"},
    "cdr_missing_stop": {"rule": "CDR_MISSING_STOP", "handling": "quarantine"},
    "cdr_time_inversion": {"rule": "CDR_TIME_INVERSION", "handling": "quarantine"},
    "cdr_implausible_duration": {"rule": "CDR_IMPLAUSIBLE_DURATION", "handling": "quarantine"},
    "cdr_energy_out_of_range": {"rule": "CDR_ENERGY_OUT_OF_RANGE", "handling": "quarantine"},
    "cdr_kwh_unit": {"rule": "-", "handling": "conform"},
    "cdr_unknown_charge_point": {"rule": "FACT_UNKNOWN_MEMBER_RATIO", "handling": "infer"},
    "meter_duplicate_sample": {"rule": "-", "handling": "dedupe"},
    "meter_out_of_order": {"rule": "-", "handling": "conform"},
    "meter_non_monotonic": {"rule": "MTR_NEGATIVE_INTERVAL_ENERGY", "handling": "quarantine"},
    "meter_soc_out_of_range": {"rule": "MTR_SOC_OUT_OF_RANGE", "handling": "quarantine"},
    "meter_null_power": {"rule": "-", "handling": "warn"},
    "meter_orphan_sample": {"rule": "MTR_ORPHAN_EXPIRED", "handling": "quarantine"},
    "cms_updated_before_created": {"rule": "CMS_UPDATED_BEFORE_CREATED", "handling": "warn"},
    "cms_null_city": {"rule": "-", "handling": "warn"},
    "cms_segment_casing": {"rule": "-", "handling": "conform"},
    "cms_dangling_tariff_fk": {"rule": "FACT_UNKNOWN_MEMBER_RATIO", "handling": "infer"},
    "partner_revision": {"rule": "-", "handling": "dedupe"},
    "schema_new_field": {"rule": "SCHEMA_DRIFT_OCPP", "handling": "warn"},
}
