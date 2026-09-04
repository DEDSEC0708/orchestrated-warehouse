"""Meter-value telemetry derived from a charging session.

Samples are not random noise around a straight line - they follow a **charging
curve**. A lithium-ion pack draws close to the charger's rated power while its
state of charge is low, then tapers sharply above roughly 80%. That shape is
why ``fact_meter_interval`` is worth having at all: it is what makes
"charging-curve analysis" a real question rather than a phrase in a README,
and it means ``peak_power_kw`` and ``avg_power_kw`` genuinely differ, which is
what makes the additive/non-additive distinction on those measures meaningful
rather than academic.

The register is CUMULATIVE, exactly as a real meter reports it, and the
warehouse derives the additive delta from it with a window function. Generating
cumulative values here rather than deltas is a deliberate choice: it means the
staging layer has to do the real work, and the "cumulative register vs additive
delta" lesson is exercised by actual data instead of asserted in a comment.
"""

from __future__ import annotations

import random
from datetime import datetime, timedelta
from typing import Any

from volthive.generator.config import GeneratorConfig
from volthive.generator.defects import DefectLedger

__all__ = ["generate_samples_for_session"]


def _power_fraction(soc_pct: float) -> float:
    """Fraction of rated power drawn at a given state of charge.

    Flat to 55%, a gentle roll-off to 80%, then a steep taper - the shape any
    DC fast-charging curve has, simplified to three segments because the point
    is realistic *shape*, not battery physics.
    """
    if soc_pct < 55:
        return 1.0
    if soc_pct < 80:
        return 1.0 - (soc_pct - 55) * 0.012
    return max(0.18, 0.7 - (soc_pct - 80) * 0.026)


def generate_samples_for_session(
    session: dict[str, Any],
    config: GeneratorConfig,
    rng: random.Random,
    ledger: DefectLedger,
) -> list[dict[str, Any]]:
    """Produce the meter samples for one session, defects included.

    Args:
        session: A session dict as produced by :mod:`volthive.generator.sessions`.
        config: Generator configuration.
        rng: The day's seeded generator.
        ledger: Ground-truth ledger; injected defects are recorded here.

    Returns:
        Sample dicts in emission order - which is deliberately NOT always
        timestamp order, because out-of-order delivery is one of the injected
        defects and the staging layer has to sort by sample time rather than by
        arrival.
    """
    start: datetime = session["_start_utc"]
    end: datetime = session["_end_utc"]
    energy_kwh: float = session["_energy_kwh"]
    register_start_wh: float = session["_meter_start_wh"]
    rated_kw: float = session["_rated_power_kw"]
    battery_kwh: float = session.get("_battery_kwh") or 45.0

    duration_seconds = max(int((end - start).total_seconds()), 1)
    interval = config.meter_sample_interval_seconds
    sample_count = min(max(duration_seconds // interval + 1, 2), config.meter_max_samples)

    # Walk a synthetic state of charge across the session and give each step a
    # share of the total energy proportional to the power the curve allows
    # there. Normalising afterwards guarantees the samples sum EXACTLY to the
    # session's energy - which is what lets an integration test assert that
    # meter detail reconciles to the session header rather than merely
    # approximates it.
    soc_start = rng.uniform(8.0, 45.0)
    soc_end = min(99.0, soc_start + (energy_kwh / battery_kwh) * 100.0)

    weights: list[float] = []
    for step in range(sample_count - 1):
        soc_here = soc_start + (soc_end - soc_start) * (step / max(sample_count - 1, 1))
        weights.append(_power_fraction(soc_here))
    weight_total = sum(weights) or 1.0

    samples: list[dict[str, Any]] = []
    register = register_start_wh
    soc = soc_start
    txn = session["transaction_id"]
    non_monotonic_at = -1
    if rng.random() < config.defect_rate("meter_non_monotonic_pct"):
        # A mid-session meter reset: the register jumps BACKWARDS, which turns
        # one interval delta negative. Placed away from the ends so it always
        # produces a genuine interval to reject.
        non_monotonic_at = rng.randrange(1, max(sample_count - 1, 2))

    for step in range(sample_count):
        moment = start + timedelta(seconds=min(step * interval, duration_seconds))
        gained_this_step = 0.0
        if step > 0:
            share = weights[step - 1] / weight_total
            gained_this_step = energy_kwh * share * 1000.0
            register += gained_this_step
            soc += (soc_end - soc_start) * share

        if step == non_monotonic_at:
            # The reset must EXCEED the energy this interval just added, or the
            # register still ends up higher than the previous reading and the
            # interval is merely small rather than negative.
            #
            # Subtracting a flat 500-5000 Wh looked right and was not: on a
            # fast DC session a five-minute interval can add 5 kWh, so the
            # smaller resets were absorbed entirely. The ground-truth ledger
            # counted a defect that the data did not contain, and
            # MTR_NEGATIVE_INTERVAL_ENERGY had nothing to fire on - a
            # disagreement between the oracle and the warehouse that would
            # surface as an unexplainable off-by-N in reconciliation.
            #
            # Subtracting the step's own gain first makes the backwards jump
            # unconditional, whatever the session's power.
            register -= gained_this_step + rng.uniform(500.0, 5000.0)
            ledger.record("meter_non_monotonic")

        soc_value: float | None = round(soc, 2)
        if rng.random() < config.defect_rate("meter_soc_out_of_range_pct"):
            soc_value = round(rng.choice([-4.0, 104.0, 132.0]), 2)
            ledger.record("meter_soc_out_of_range")

        power: float | None = round(rated_kw * _power_fraction(soc) * rng.uniform(0.9, 1.0), 3)
        if rng.random() < config.defect_rate("meter_null_power_pct"):
            power = None
            ledger.record("meter_null_power")

        samples.append(
            {
                "sample_id": f"SMP-{txn[4:]}-{step:03d}",
                "transaction_id": txn,
                "charge_point_id": session["charge_point_id"],
                "sample_timestamp": moment.isoformat().replace("+00:00", "Z"),
                "energy_register_wh": round(register, 2),
                "power_kw": power,
                "soc_percent": soc_value,
                "voltage_v": round(rng.uniform(380.0, 420.0), 2),
                "current_a": round(rng.uniform(20.0, 190.0), 2),
                "temperature_c": round(rng.uniform(22.0, 48.0), 2),
            }
        )

    # An identical sample delivered twice. Deduplicated on sample_id in
    # staging, counted, and NOT quarantined - a repeated telemetry frame is
    # normal protocol behaviour, not a data error.
    if samples and rng.random() < config.defect_rate("meter_duplicate_sample_pct"):
        samples.append(dict(rng.choice(samples)))
        ledger.record("meter_duplicate_sample")

    # Delivery order is not timestamp order. Staging must sort by
    # sample_timestamp before taking the LAG, and this is what proves it does.
    if len(samples) > 2 and rng.random() < config.defect_rate("meter_out_of_order_pct"):
        i = rng.randrange(0, len(samples) - 1)
        samples[i], samples[i + 1] = samples[i + 1], samples[i]
        ledger.record("meter_out_of_order")

    return samples
