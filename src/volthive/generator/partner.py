"""Roaming-partner records: the OCPI-style third source.

Roaming is what happens when a VoltHive customer charges on a partner's network
(OUTBOUND) or a partner's customer charges on a VoltHive charger (INBOUND). The
two directions behave completely differently in a warehouse, and modelling both
is the reason this source is interesting rather than decorative:

**OUTBOUND** - VoltHive's own OCPP system never sees the session, because it
happened on someone else's hardware. The partner API is the ONLY source. Miss
it and you under-report revenue.

**INBOUND** - the session happened on a VoltHive charger, so it arrives TWICE:
once as an OCPP charge detail record and once from the partner. Naively loading
both double-counts it. The conformance rule is that OCPP is authoritative and
the partner copy is marked ``is_duplicate_source``, with the
``SESSION_CROSS_SOURCE_DUP`` rule asserting the fact table never gains a row.

**Revisions.** A partner can restate a record for up to a week while a billing
dispute settles, which is precisely why this source's lookback is seven days
while the file sources' is three. Lookback is a property of how a source
behaves, not a global constant - and this generator produces the revisions that
make that concrete.
"""

from __future__ import annotations

import random
from datetime import timedelta
from typing import Any

from volthive.generator.config import GeneratorConfig
from volthive.generator.defects import DefectLedger

__all__ = ["make_partner_record"]


def make_partner_record(
    session: dict[str, Any],
    direction: str,
    config: GeneratorConfig,
    rng: random.Random,
    ledger: DefectLedger,
) -> list[dict[str, Any]]:
    """Build the partner API record(s) for one roaming session.

    Returns one record normally, or two when a revision is injected: the
    original plus a restated version with a later ``last_updated`` and
    different money. The cursor extract picks both up within its lookback, and
    the staging dedupe keeps the latest.
    """
    partner = rng.choice(config.roaming_partners)
    start = session["_start_utc"]
    end = session["_end_utc"]
    energy_kwh = round(session["_energy_kwh"], 3)
    hours = round((end - start).total_seconds() / 3600.0, 4)
    # The partner bills at its own rate, which is deliberately NOT VoltHive's
    # tariff - a roaming session's cost genuinely comes from someone else's
    # price list, and pretending otherwise would make the roaming-vs-own-network
    # margin comparison meaningless.
    cost = round(energy_kwh * rng.uniform(17.0, 26.0), 2)

    base = {
        "cdr_id": f"CDR-{partner[:3]}-{session['transaction_id'][4:]}",
        "partner_code": partner,
        "direction": direction,
        "customer_id": session.get("customer_id"),
        "partner_location_id": f"LOC-{partner[:3]}-{rng.randrange(1, 400):04d}",
        "partner_evse_id": f"EVSE-{partner[:3]}-{rng.randrange(1, 2000):05d}",
        "transaction_id": session["transaction_id"],
        "start_date_time": start.isoformat().replace("+00:00", "Z"),
        "end_date_time": end.isoformat().replace("+00:00", "Z"),
        "total_energy_kwh": energy_kwh,
        "total_time_hours": hours,
        "total_cost_inr": cost,
        "currency": "INR",
        "last_updated": (end + timedelta(minutes=rng.randrange(5, 240)))
        .isoformat()
        .replace("+00:00", "Z"),
    }
    records = [base]

    if rng.random() < config.defect_rate("partner_revision_pct"):
        revised = dict(base)
        revised["total_cost_inr"] = round(cost * rng.uniform(0.85, 1.15), 2)
        revised["total_energy_kwh"] = round(energy_kwh * rng.uniform(0.97, 1.03), 3)
        revised["last_updated"] = (
            (end + timedelta(days=rng.randrange(1, config.roaming_revision_window_days)))
            .isoformat()
            .replace("+00:00", "Z")
        )
        records.append(revised)
        ledger.record("partner_revision")

    return records
