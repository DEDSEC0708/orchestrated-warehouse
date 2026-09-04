"""Charging-session generation: the day-by-day heart of the simulator.

Sessions are not uniform noise. Real charging demand has shape, and the shape
is what makes the analytics questions in the mart worth asking:

* **Hour of day depends on site type.** Office sites fill on arrival, malls in
  the evening, residential overnight, fleet depots in the small hours. A single
  flat hourly distribution would make "utilisation by hour by site type" - the
  most-asked question in charging analytics - return a boring straight line.
* **Weekends differ.** Office and fleet demand collapses; malls and highways
  rise. Which is also why the row-count anomaly rule compares against a
  weekday-aware baseline instead of a flat average.
* **AC and DC sessions are different animals.** A DC session is twenty to sixty
  minutes at high power; an AC session is hours at low power. Averaging them
  without splitting by connector type is a classic way to produce a meaningless
  "average session duration".

Cumulative meter registers are carried ACROSS days per charge point, exactly as
a physical meter does, so a session's ``meter_start_wh`` is genuinely where the
previous session on that device left off.
"""

from __future__ import annotations

import bisect
import random
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, time, timedelta, timezone
from typing import Any

from volthive.generator.config import GeneratorConfig
from volthive.generator.defects import DefectLedger
from volthive.generator.entities import MasterData
from volthive.generator.meter import generate_samples_for_session
from volthive.generator.partner import make_partner_record

__all__ = ["DayOutput", "SessionUniverse", "build_universe", "generate_day"]

#: Relative hourly demand by site type, index 0-23 in IST. These are shapes,
#: not measurements, and the README says so - but they are the RIGHT shapes,
#: which is what makes the hour-of-day mart query show something.
_HOUR_WEIGHTS: dict[str, list[int]] = {
    "OFFICE": [1, 1, 1, 1, 1, 2, 5, 14, 26, 30, 24, 16, 12, 12, 16, 18, 14, 8, 4, 3, 2, 2, 1, 1],
    "MALL": [2, 1, 1, 1, 1, 1, 2, 4, 7, 11, 16, 22, 24, 22, 20, 22, 26, 30, 32, 28, 20, 12, 6, 3],
    "HIGHWAY": [
        6,
        4,
        3,
        3,
        4,
        7,
        14,
        20,
        22,
        22,
        21,
        20,
        20,
        20,
        21,
        22,
        22,
        20,
        17,
        14,
        12,
        10,
        8,
        7,
    ],
    "RESIDENTIAL": [
        14,
        10,
        7,
        5,
        4,
        4,
        6,
        8,
        7,
        6,
        5,
        5,
        5,
        5,
        6,
        7,
        9,
        14,
        22,
        30,
        34,
        30,
        24,
        18,
    ],
    "FLEET_DEPOT": [
        30,
        32,
        30,
        26,
        20,
        12,
        6,
        4,
        3,
        3,
        3,
        4,
        5,
        6,
        6,
        5,
        5,
        6,
        8,
        12,
        18,
        24,
        28,
        30,
    ],
}

#: Weekend multiplier by site type. Offices empty out; malls and highways fill.
_WEEKEND_FACTOR = {
    "OFFICE": 0.30,
    "MALL": 1.35,
    "HIGHWAY": 1.25,
    "RESIDENTIAL": 1.10,
    "FLEET_DEPOT": 0.55,
}

_STOP_REASONS = [
    "Local",
    "Local",
    "Local",
    "Remote",
    "EVDisconnected",
    "PowerLoss",
    "EmergencyStop",
]
_STOP_WEIGHTS = [40, 20, 15, 12, 8, 3, 2]

IST = timezone(timedelta(hours=5, minutes=30))


@dataclass(slots=True)
class SessionUniverse:
    """Pre-computed indexes over master data, plus mutable meter registers.

    Built once and reused for every day. Without it, resolving "which version
    of this charge point was in effect at this instant" would be a linear scan
    over every version for every one of a million sessions; with it, it is a
    bisect over a short sorted list.
    """

    charge_point_versions: dict[str, list[dict[str, Any]]] = field(default_factory=dict)
    charge_point_times: dict[str, list[datetime]] = field(default_factory=dict)
    charge_points_by_city: dict[str, list[str]] = field(default_factory=dict)
    station_site_type: dict[str, str] = field(default_factory=dict)
    customer_ids: list[str] = field(default_factory=list)
    customer_segment: dict[str, str] = field(default_factory=dict)
    vehicles_by_customer: dict[str, list[str]] = field(default_factory=dict)
    vehicle_battery: dict[str, float] = field(default_factory=dict)
    registers: dict[str, float] = field(default_factory=dict)

    def version_at(self, charge_point_id: str, moment: datetime) -> dict[str, Any] | None:
        """The charge-point version in effect at ``moment``, or None."""
        times = self.charge_point_times.get(charge_point_id)
        if not times:
            return None
        index = bisect.bisect_right(times, moment) - 1
        if index < 0:
            return None
        return self.charge_point_versions[charge_point_id][index]


def build_universe(master: MasterData, config: GeneratorConfig) -> SessionUniverse:
    """Index master data for fast point-in-time lookups during generation."""
    universe = SessionUniverse()

    for row in master.charge_points:
        cp_id = row["charge_point_id"]
        universe.charge_point_versions.setdefault(cp_id, []).append(row)
    for cp_id, versions in universe.charge_point_versions.items():
        versions.sort(key=lambda r: r["updated_at"])
        universe.charge_point_times[cp_id] = [r["updated_at"] for r in versions]
        universe.charge_points_by_city.setdefault(versions[0]["_city_code"], []).append(cp_id)
    for city in universe.charge_points_by_city.values():
        city.sort()

    for station in master.current(master.stations, "station_id"):
        universe.station_site_type[station["station_id"]] = station["site_type"]

    current_customers = master.current(master.customers, "customer_id")
    universe.customer_ids = [row["customer_id"] for row in current_customers]
    universe.customer_segment = {
        row["customer_id"]: str(row["customer_segment"]).upper() for row in current_customers
    }

    for vehicle in master.vehicles:
        universe.vehicles_by_customer.setdefault(vehicle["customer_id"], []).append(
            vehicle["vehicle_id"]
        )
        universe.vehicle_battery[vehicle["vehicle_id"]] = float(vehicle["battery_capacity_kwh"])

    # Deterministic starting register per device, so a session's meter_start_wh
    # is a plausible lifetime total rather than zero.
    for index, cp_id in enumerate(sorted(universe.charge_point_versions)):
        universe.registers[cp_id] = 1_000_000.0 + index * 7_777.0

    _ = config  # configuration is not needed here yet; kept for symmetry
    return universe


@dataclass(slots=True)
class DayOutput:
    """Everything generated for one calendar day."""

    business_date: date
    cdrs_by_city: dict[str, list[dict[str, Any]]] = field(default_factory=dict)
    meter_samples: list[dict[str, Any]] = field(default_factory=list)
    partner_records: list[dict[str, Any]] = field(default_factory=list)


def _pick_hour(rng: random.Random, site_type: str, is_weekend: bool) -> int:
    weights = list(_HOUR_WEIGHTS.get(site_type, _HOUR_WEIGHTS["MALL"]))
    if is_weekend and site_type in {"OFFICE", "FLEET_DEPOT"}:
        weights = [max(1, int(w * 0.4)) for w in weights]
    return rng.choices(range(24), weights=weights, k=1)[0]


def _session_shape(
    rng: random.Random, current_type: str, rated_kw: float, battery_kwh: float
) -> tuple[int, float]:
    """Return (duration_seconds, energy_kwh) for one session.

    Energy is derived from power and time and then capped by the battery, so
    the numbers hold together: a 7 kW AC charger cannot deliver 90 kWh in an
    hour no matter what the random draw says.
    """
    if current_type == "DC":
        duration_minutes = rng.triangular(12, 75, 32)
        efficiency = rng.uniform(0.55, 0.85)
    else:
        duration_minutes = rng.triangular(45, 420, 150)
        efficiency = rng.uniform(0.60, 0.92)

    duration_seconds = int(duration_minutes * 60)
    energy = rated_kw * (duration_seconds / 3600.0) * efficiency
    energy = min(energy, battery_kwh * rng.uniform(0.45, 0.85))
    return duration_seconds, round(max(energy, 0.2), 3)


def generate_day(
    day: date,
    config: GeneratorConfig,
    universe: SessionUniverse,
    ledger: DefectLedger,
) -> DayOutput:
    """Generate one IST business day of sessions, telemetry and partner records.

    The day's random generator is seeded from the master seed AND the date, so
    regenerating a single day in isolation produces exactly the same data as
    regenerating the whole window. That property is what makes a targeted
    restatement test meaningful: the "corrected" data really is what the
    generator would have produced.
    """
    rng = random.Random(f"{config.seed}:{day.isoformat()}")
    output = DayOutput(business_date=day)

    is_weekend = day.weekday() >= 5
    # Mild seasonality: a slow annual swing plus day-to-day noise, so the
    # row-count anomaly rule has a baseline that moves without being erratic.
    seasonal = 1.0 + 0.12 * ((day.month - 6) / 6.0)
    daily_target = int(config.sessions_per_day * seasonal * rng.uniform(0.88, 1.12))

    all_cp_ids = sorted(universe.charge_point_versions)
    if not all_cp_ids:
        return output

    counters: dict[str, int] = {}
    for _ in range(daily_target):
        cp_id = rng.choice(all_cp_ids)
        versions = universe.charge_point_versions[cp_id]
        city_code = versions[0]["_city_code"]
        first_version = versions[0]

        if first_version["commissioned_date"] > day:
            continue

        site_type = universe.station_site_type.get(first_version["station_id"], "MALL")
        if is_weekend and rng.random() > _WEEKEND_FACTOR.get(site_type, 1.0):
            continue

        hour_ist = _pick_hour(rng, site_type, is_weekend)
        start_ist = datetime.combine(day, time(hour=hour_ist), tzinfo=IST) + timedelta(
            minutes=rng.randrange(60), seconds=rng.randrange(60)
        )
        start_utc = start_ist.astimezone(UTC)

        version = universe.version_at(cp_id, start_utc) or first_version
        if version["status"] == "DECOMMISSIONED":
            continue

        customer_id = rng.choice(universe.customer_ids)
        vehicle_ids = universe.vehicles_by_customer.get(customer_id, [])
        vehicle_id = rng.choice(vehicle_ids) if vehicle_ids else None
        battery = universe.vehicle_battery.get(vehicle_id or "", 45.0)

        duration_seconds, energy_kwh = _session_shape(
            rng, version["current_type"], float(version["rated_power_kw"]), battery
        )
        end_utc = start_utc + timedelta(seconds=duration_seconds)

        counters[city_code] = counters.get(city_code, 0) + 1
        txn = f"TXN-{day.strftime('%Y%m%d')}-{city_code}-{counters[city_code]:06d}"

        register = universe.registers.get(cp_id, 1_000_000.0)
        session = {
            "transaction_id": txn,
            "charge_point_id": cp_id,
            "connector_no": rng.randrange(1, 3),
            "id_tag": f"IDT-{abs(hash(customer_id)) % 9_999_999:07d}",
            "customer_id": customer_id,
            "vehicle_id": vehicle_id,
            "_start_utc": start_utc,
            "_end_utc": end_utc,
            "_energy_kwh": energy_kwh,
            "_meter_start_wh": register,
            "_rated_power_kw": float(version["rated_power_kw"]),
            "_battery_kwh": battery,
            "_city_code": city_code,
            "_firmware": version["firmware_version"],
        }
        universe.registers[cp_id] = register + energy_kwh * 1000.0

        is_roaming = rng.random() < (config.roaming_session_pct / 100.0)
        # OUTBOUND means the session happened on a PARTNER's hardware, so
        # VoltHive's own OCPP system never sees it and the API is the only
        # source. INBOUND happened here, so it arrives from both - which is the
        # cross-source duplicate the conformance rule exists for.
        direction = "OUTBOUND" if is_roaming and rng.random() < 0.7 else "INBOUND"

        if is_roaming:
            output.partner_records.extend(
                make_partner_record(session, direction, config, rng, ledger)
            )
            if direction == "OUTBOUND":
                continue

        samples = generate_samples_for_session(session, config, rng, ledger)
        output.meter_samples.extend(samples)

        for record in _emit_cdrs(session, is_roaming, config, rng, ledger, day):
            output.cdrs_by_city.setdefault(record["_city_code"], []).append(record)

    _inject_orphan_samples(output, config, rng, ledger, day)
    return output


def _emit_cdrs(
    session: dict[str, Any],
    is_roaming: bool,
    config: GeneratorConfig,
    rng: random.Random,
    ledger: DefectLedger,
    day: date,
) -> list[dict[str, Any]]:
    """Render a session as one or more charge detail records, with defects.

    "One or more" because a retry storm delivers the same record twice and a
    correction delivers a higher ``record_version`` - both of which the staging
    dedupe has to distinguish, since the first is a duplicate to be counted and
    the second is an update to be applied.
    """
    start = session["_start_utc"]
    end = session["_end_utc"]
    energy_wh = session["_energy_kwh"] * 1000.0
    meter_start = session["_meter_start_wh"]
    meter_stop = meter_start + energy_wh
    charge_point_id = session["charge_point_id"]
    energy_unit = "Wh"
    stop_timestamp: str | None = end.isoformat().replace("+00:00", "Z")
    session_status = "COMPLETED"
    stop_reason = rng.choices(_STOP_REASONS, weights=_STOP_WEIGHTS, k=1)[0]
    if stop_reason in {"PowerLoss", "EmergencyStop"}:
        session_status = "FAULTED"

    # --- injected defects, at most one per record so counts stay attributable
    if rng.random() < config.defect_rate("cdr_negative_energy_pct"):
        meter_stop = meter_start - rng.uniform(500.0, 9000.0)
        ledger.record("cdr_negative_energy")
    elif rng.random() < config.defect_rate("cdr_missing_stop_pct"):
        stop_timestamp = None
        ledger.record("cdr_missing_stop")
    elif rng.random() < config.defect_rate("cdr_time_inversion_pct"):
        stop_timestamp = (
            (start - timedelta(minutes=rng.randrange(5, 90))).isoformat().replace("+00:00", "Z")
        )
        ledger.record("cdr_time_inversion")
    elif rng.random() < config.defect_rate("cdr_implausible_duration_pct"):
        stop_timestamp = (
            (start + timedelta(hours=rng.randrange(25, 90))).isoformat().replace("+00:00", "Z")
        )
        ledger.record("cdr_implausible_duration")
    elif rng.random() < config.defect_rate("cdr_energy_out_of_range_pct"):
        meter_stop = meter_start + rng.uniform(360_000.0, 900_000.0)
        ledger.record("cdr_energy_out_of_range")

    # Older firmware reports the register in kWh rather than Wh. This is NOT a
    # rejection: it is a dialect, and staging normalises it. Rejecting 3% of
    # perfectly good records because a field is in different units would be a
    # data-quality layer doing harm.
    if rng.random() < config.defect_rate("cdr_kwh_unit_pct"):
        energy_unit = "kWh"
        meter_start = round(meter_start / 1000.0, 3)
        meter_stop = round(meter_stop / 1000.0, 3)
        ledger.record("cdr_kwh_unit")

    # A device commissioned this morning and not yet in the CMS extract. The
    # fact must still load - dropping it loses revenue - so the fact load
    # creates an INFERRED dimension member instead.
    if rng.random() < config.defect_rate("cdr_unknown_charge_point_pct"):
        charge_point_id = f"CP-{session['_city_code']}-9{rng.randrange(100, 999)}"
        ledger.record("cdr_unknown_charge_point")

    record: dict[str, Any] = {
        "transaction_id": session["transaction_id"],
        "charge_point_id": charge_point_id,
        "connector_no": session["connector_no"],
        "id_tag": session["id_tag"],
        "customer_id": session["customer_id"],
        "vehicle_id": session["vehicle_id"],
        "start_timestamp": start.isoformat().replace("+00:00", "Z"),
        "stop_timestamp": stop_timestamp,
        "meter_start_wh": round(meter_start, 2),
        "meter_stop_wh": round(meter_stop, 2),
        "stop_reason": stop_reason,
        "session_status": session_status,
        "auth_method": "ROAMING" if is_roaming else rng.choice(["APP", "APP", "APP", "RFID"]),
        "energy_unit": energy_unit,
        "firmware_version": session["_firmware"],
        "record_version": 1,
        "emitted_at": (end + timedelta(seconds=rng.randrange(2, 400)))
        .isoformat()
        .replace("+00:00", "Z"),
        "_city_code": session["_city_code"],
    }

    # Schema evolution: from the configured date, firmware 3.5 adds a field the
    # pipeline has never seen. Raw JSONB captures it losslessly, staging
    # ignores it, SCHEMA_DRIFT_OCPP warns. This is the single best argument for
    # the payload-preserving raw design, so the generator exercises it.
    if day >= config.schema_evolution_from and session["_firmware"].startswith("3.5"):
        record[config.schema_evolution_field] = round(rng.uniform(180.0, 720.0), 1)
        ledger.record("schema_new_field")

    records = [record]

    if rng.random() < config.defect_rate("cdr_exact_duplicate_pct"):
        records.append(dict(record))
        ledger.record("cdr_exact_duplicate")

    if rng.random() < config.defect_rate("cdr_corrected_version_pct"):
        corrected = dict(record)
        corrected["record_version"] = 2
        # The correction adjusts the ENERGY DELIVERED, not the register total.
        # Scaling the raw register would subtract tens of thousands of Wh from
        # a lifetime meter reading and drive the delta negative - producing a
        # record that is not "a billing correction" but "a broken meter", and
        # inflating the CDR_NEGATIVE_ENERGY count with defects the ledger never
        # recorded. A real correction restates how much energy was delivered.
        corrected_delta = (
            float(record["meter_stop_wh"]) - float(record["meter_start_wh"])
        ) * rng.uniform(0.94, 1.06)
        corrected["meter_stop_wh"] = round(float(record["meter_start_wh"]) + corrected_delta, 2)
        corrected["emitted_at"] = (
            (end + timedelta(hours=rng.randrange(1, 20))).isoformat().replace("+00:00", "Z")
        )
        records.append(corrected)
        ledger.record("cdr_corrected_version")

    ledger.count_total("cdr_records_emitted", len(records))
    ledger.count_total("sessions_emitted", 1)
    return records


def _inject_orphan_samples(
    output: DayOutput,
    config: GeneratorConfig,
    rng: random.Random,
    ledger: DefectLedger,
    day: date,
) -> None:
    """Add meter samples for transactions that have no charge detail record.

    This is the LATE-ARRIVING RELATIONSHIP case, and it is the one people get
    wrong. The natural instinct is to quarantine an orphan sample immediately -
    but the session is very often still open, or its record is simply in
    tomorrow's file. Quarantining it would reject data that is about to become
    valid.

    So orphans are HELD: the lookback window re-reads the partition on the next
    run, and by then the record has usually arrived. Only samples still
    orphaned after the window has passed are quarantined, as
    ``MTR_ORPHAN_EXPIRED``. The ones generated here belong to transactions that
    will never appear at all, so they eventually expire - which is exactly the
    case that proves the hold has a limit and the limit is handled.
    """
    orphan_rate = config.defect_rate("meter_orphan_sample_pct")
    if orphan_rate <= 0 or not output.meter_samples:
        return

    samples_per_orphan = 3
    orphan_groups = max(int(len(output.meter_samples) * orphan_rate / samples_per_orphan), 0)
    for index in range(orphan_groups):
        txn = f"TXN-{day.strftime('%Y%m%d')}-ORP-{index:06d}"
        first = datetime.combine(day, time(hour=rng.randrange(24)), tzinfo=UTC)
        register = rng.uniform(100_000, 900_000)
        # A real orphan is a whole session's worth of telemetry whose header
        # record never arrived, not a single stray frame - so it gets several
        # samples, as a genuinely open session would.
        for step in range(samples_per_orphan):
            register += rng.uniform(500.0, 4000.0)
            output.meter_samples.append(
                {
                    "sample_id": f"SMP-ORPHAN-{day.strftime('%Y%m%d')}-{index:06d}-{step}",
                    "transaction_id": txn,
                    "charge_point_id": None,
                    "sample_timestamp": (
                        first + timedelta(seconds=step * config.meter_sample_interval_seconds)
                    )
                    .isoformat()
                    .replace("+00:00", "Z"),
                    "energy_register_wh": round(register, 2),
                    "power_kw": round(rng.uniform(3.0, 60.0), 3),
                    "soc_percent": round(rng.uniform(10.0, 90.0), 2),
                    "voltage_v": round(rng.uniform(380.0, 420.0), 2),
                    "current_a": round(rng.uniform(20.0, 190.0), 2),
                    "temperature_c": round(rng.uniform(22.0, 48.0), 2),
                }
            )
            ledger.record("meter_orphan_sample")
