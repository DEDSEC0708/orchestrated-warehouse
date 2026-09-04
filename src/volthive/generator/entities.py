"""Master-data generation with genuine change history.

This module produces the contents of the simulated CMS: customers, vehicles,
stations, charge points and tariff plans - not as a single current snapshot,
but as a **full version history**, because the version history is the thing the
whole SCD Type 2 half of this project exists to handle.

**The honest caveat, stated here and in the README rather than buried.** A real
OLTP system exposes only current state plus an ``updated_at`` column, so an
incremental extract genuinely cannot reconstruct history at bootstrap - that is
a real limitation of watermark-based change capture, and log-based CDC
(Debezium, logical replication) is the production answer. To make the
demonstration honest rather than fabricated, this simulator writes BOTH:

* ``cms.<entity>``          the current state, as a real OLTP would expose it
* ``cms.<entity>_history``  every version, standing in for what CDC would give

Ingestion reads the history table with an ordinary bounded ``updated_at``
predicate, so the extract code is exactly what it would be against a CDC feed.
Nothing is pretended: the substitution is documented, and the current-state
table is what the weekly key reconciliation reads.

**Every change here has an analytical consequence.** A customer relocating
moves revenue between cities from that date forward. A station's bay count
changing moves the utilisation denominator. A charge point going 30 kW to
60 kW changes energy per session. A tariff revision changes what a historical
session cost. That is why these are Type 2 attributes, and it is why the
generator bothers to produce the changes at all.
"""

from __future__ import annotations

import random
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, time, timedelta
from typing import Any

from volthive.generator.config import GeneratorConfig
from volthive.generator.defects import DefectLedger

__all__ = ["MasterData", "generate_master_data"]

_MAKES: list[tuple[str, list[str], str, float]] = [
    ("Tata", ["Nexon EV", "Tiago EV", "Punch EV", "Curvv EV"], "SUV", 40.0),
    ("Mahindra", ["XUV400", "BE 6", "XEV 9e"], "SUV", 55.0),
    ("MG", ["ZS EV", "Comet EV", "Windsor EV"], "SUV", 45.0),
    ("Hyundai", ["Kona Electric", "Ioniq 5", "Creta Electric"], "SUV", 60.0),
    ("BYD", ["Atto 3", "Seal", "eMAX 7"], "SEDAN", 60.0),
    ("Citroen", ["eC3"], "HATCH", 29.0),
    ("Ashok Leyland", ["Switch EiV12"], "COMMERCIAL", 150.0),
    ("Olectra", ["K6"], "COMMERCIAL", 130.0),
]

_FIRST_NAMES = [
    "Aarav",
    "Priya",
    "Rohan",
    "Ananya",
    "Vikram",
    "Meera",
    "Arjun",
    "Divya",
    "Karthik",
    "Sneha",
    "Rahul",
    "Pooja",
    "Aditya",
    "Nisha",
    "Sanjay",
    "Kavya",
    "Imran",
    "Fatima",
    "Joseph",
    "Grace",
    "Manish",
    "Ritu",
    "Sameer",
    "Tanvi",
]
_LAST_INITIALS = list("ABCDGHJKMNPRSTVY")

_OEM_VENDORS = ["Delta", "ABB", "Exicom", "Servotech", "Tata Power EZ", "Kazam"]
_OPERATORS = ["VoltHive Energy", "VoltHive Retail", "VoltHive Fleet Services"]


def _weighted_choice(rng: random.Random, options: list[Any], weights: list[int]) -> Any:
    return rng.choices(options, weights=weights, k=1)[0]


def _random_instant(rng: random.Random, day: date) -> datetime:
    """A uniformly random UTC instant within one calendar day."""
    return datetime.combine(day, time.min, tzinfo=UTC) + timedelta(seconds=rng.randrange(86400))


@dataclass(slots=True)
class MasterData:
    """Every version of every master entity, plus lookups the session
    generator needs.

    ``*_versions`` lists are ordered by (natural key, updated_at), which is the
    order an ``updated_at``-watermarked extract would deliver them in, and the
    order the SCD2 merge expects.
    """

    customers: list[dict[str, Any]] = field(default_factory=list)
    vehicles: list[dict[str, Any]] = field(default_factory=list)
    stations: list[dict[str, Any]] = field(default_factory=list)
    charge_points: list[dict[str, Any]] = field(default_factory=list)
    tariff_plans: list[dict[str, Any]] = field(default_factory=list)

    def current(self, versions: list[dict[str, Any]], key: str) -> list[dict[str, Any]]:
        """Collapse a version list to the latest row per natural key."""
        latest: dict[str, dict[str, Any]] = {}
        for row in versions:
            existing = latest.get(row[key])
            if existing is None or row["updated_at"] >= existing["updated_at"]:
                latest[row[key]] = row
        return [latest[k] for k in sorted(latest)]

    def version_at(
        self, versions: list[dict[str, Any]], key: str, key_value: str, moment: datetime
    ) -> dict[str, Any] | None:
        """The version of an entity that was in effect at ``moment``.

        This is the generator's own point-in-time lookup, and it exists so the
        expected revenue for a session can be computed INDEPENDENTLY of the
        warehouse. A test that compares the pipeline's answer against a
        separately-derived oracle is worth ten tests that compare the pipeline
        against itself.
        """
        best: dict[str, Any] | None = None
        for row in versions:
            if row[key] != key_value or row["updated_at"] > moment:
                continue
            if best is None or row["updated_at"] > best["updated_at"]:
                best = row
        return best


def _generate_tariff_plans(config: GeneratorConfig, rng: random.Random) -> list[dict[str, Any]]:
    """Fourteen plans, each revised a few times across the window.

    Price history IS the point of this dimension, so revisions are the
    interesting part: a plan revised on 1 March means February's sessions must
    resolve to February's price. Revisions are spread across the window rather
    than clustered, so any reasonable test window contains at least one.
    """
    catalogue = [
        ("TP-DC-STD", "DC Standard", 18.50, 60.0),
        ("TP-DC-PREM", "DC Premium Fast", 21.00, 90.0),
        ("TP-DC-HWY", "DC Highway", 22.50, 120.0),
        ("TP-DC-FLEET", "DC Fleet Contract", 15.75, 60.0),
        ("TP-DC-NGT", "DC Night Saver", 15.00, 60.0),
        ("TP-DC-CORP", "DC Corporate", 17.25, 60.0),
        ("TP-DC-T2", "DC Tier-2 City", 16.90, 60.0),
        ("TP-AC-STD", "AC Standard", 11.50, 7.4),
        ("TP-AC-PREM", "AC Premium", 13.00, 22.0),
        ("TP-AC-RES", "AC Residential", 9.75, 7.4),
        ("TP-AC-FLEET", "AC Fleet Depot", 8.90, 22.0),
        ("TP-AC-NGT", "AC Night Saver", 8.50, 7.4),
        ("TP-AC-CORP", "AC Corporate", 10.80, 22.0),
        ("TP-AC-T2", "AC Tier-2 City", 10.25, 7.4),
    ]
    revisions = int(config.change_rates.get("tariff_revisions_per_plan", 3))
    window_days = config.day_count
    created = datetime.combine(config.start_date - timedelta(days=400), time.min, tzinfo=UTC)

    rows: list[dict[str, Any]] = []
    for plan_id, plan_name, base_price, _power in catalogue:
        price = base_price
        # Version 1 predates the window, so every session in the window has a
        # tariff version to resolve against - there is no "before history
        # began" hole for a point-in-time join to fall into.
        moments = [created]
        for index in range(revisions):
            # Evenly spaced with jitter: predictable enough that a short test
            # window still contains a revision, irregular enough not to look
            # synthetic.
            offset = int(window_days * (index + 1) / (revisions + 1))
            moments.append(
                datetime.combine(config.start_date + timedelta(days=offset), time.min, tzinfo=UTC)
                + timedelta(hours=rng.randrange(24))
            )

        for version_no, moment in enumerate(moments, start=1):
            if version_no > 1:
                # Revisions trend upward (regulatory revisions and grid cost
                # pass-through) but are not monotonic, because a night-saver
                # tariff being cut is a real thing that happens.
                price = round(price * rng.uniform(0.97, 1.12), 2)
            rows.append(
                {
                    "tariff_plan_id": plan_id,
                    "plan_name": plan_name,
                    "price_per_kwh_inr": price,
                    "price_per_minute_inr": round(rng.uniform(0.0, 1.5), 2),
                    "idle_fee_per_minute_inr": round(rng.choice([0.0, 2.0, 3.0, 5.0]), 2),
                    "min_billable_kwh": rng.choice([0.0, 0.5, 1.0]),
                    "gst_rate_pct": 18.0,
                    "valid_from_date": moment.date(),
                    "is_active": True,
                    "created_at": created,
                    "updated_at": moment,
                    "_version_no": version_no,
                }
            )
    rows.sort(key=lambda r: (r["tariff_plan_id"], r["updated_at"]))
    return rows


def _generate_stations(config: GeneratorConfig, rng: random.Random) -> list[dict[str, Any]]:
    """Stations across the enabled cities, with reclassifications and
    expansions.

    A station changing site type or bay count is not cosmetic: both are inputs
    to utilisation, and comparing this month to last month using today's values
    silently corrupts the trend.
    """
    codes = sorted(config.cities)
    rows: list[dict[str, Any]] = []
    reclassify_rate = config.change_rate("station_reclassification_pct")
    expansion_rate = config.change_rate("station_bay_expansion_pct")

    for index in range(config.station_count):
        code = codes[index % len(codes)]
        city = config.cities[code]
        station_id = f"ST-{code}-{index // len(codes) + 1:03d}"
        site_type = _weighted_choice(rng, config.site_types, config.site_type_weights)
        num_bays = rng.choice([2, 4, 4, 6, 6, 8, 10, 12])
        commissioned = config.start_date - timedelta(days=rng.randrange(120, 1200))
        created = datetime.combine(commissioned, time.min, tzinfo=UTC)

        base = {
            "station_id": station_id,
            "station_name": f"VoltHive {city.name} {site_type.title().replace('_', ' ')} {index // len(codes) + 1}",
            "address_line": f"{rng.randrange(1, 400)} {rng.choice(['MG Road', 'Ring Road', 'NH-44', 'Sector 21', 'Tech Park Ave'])}",
            "city": city.name,
            "state": city.state,
            "pincode": f"{city.pincode_prefix}{rng.randrange(10, 99)}",
            "latitude": round(rng.uniform(8.0, 30.0), 6),
            "longitude": round(rng.uniform(72.0, 88.0), 6),
            "site_type": site_type,
            "commissioned_date": commissioned,
            "num_bays": num_bays,
            "operator_name": rng.choice(_OPERATORS),
            "is_active": True,
            "created_at": created,
            "updated_at": created,
            "_city_code": code,
            "_version_no": 1,
        }
        rows.append(base)

        version_no = 1
        if rng.random() < reclassify_rate:
            version_no += 1
            changed = dict(base)
            changed["site_type"] = rng.choice([t for t in config.site_types if t != site_type])
            changed["updated_at"] = _random_instant(
                rng, config.start_date + timedelta(days=rng.randrange(config.day_count))
            )
            changed["_version_no"] = version_no
            rows.append(changed)
        if rng.random() < expansion_rate:
            version_no += 1
            latest = dict(rows[-1])
            latest["num_bays"] = num_bays + rng.choice([2, 4])
            latest["updated_at"] = _random_instant(
                rng, config.start_date + timedelta(days=rng.randrange(config.day_count))
            )
            latest["_version_no"] = version_no
            rows.append(latest)

    rows.sort(key=lambda r: (r["station_id"], r["updated_at"]))
    return _renumber(rows, "station_id")


def _generate_charge_points(
    config: GeneratorConfig,
    stations: list[dict[str, Any]],
    tariff_plans: list[dict[str, Any]],
    rng: random.Random,
    ledger: DefectLedger,
) -> list[dict[str, Any]]:
    """Charge points per station, including the headline 30 -> 60 kW upgrade.

    One defect is injected here on purpose: a small fraction of devices point
    at a tariff plan that does not exist, which is what a bad admin edit looks
    like in a real CMS. It resolves to the UNKNOWN tariff member and raises a
    warning rather than failing the load.
    """
    dc_plans = sorted(
        {p["tariff_plan_id"] for p in tariff_plans if p["tariff_plan_id"].startswith("TP-DC")}
    )
    ac_plans = sorted(
        {p["tariff_plan_id"] for p in tariff_plans if p["tariff_plan_id"].startswith("TP-AC")}
    )

    current_stations = {}
    for station in stations:
        current_stations[station["station_id"]] = station

    upgrade_rate = config.change_rate("charge_point_power_upgrade_pct")
    firmware_rate = config.change_rate("charge_point_firmware_pct")
    tariff_move_rate = config.change_rate("charge_point_tariff_move_pct")
    relocation_rate = config.change_rate("charge_point_relocation_pct")
    dangling_fk_rate = config.defect_rate("cms_dangling_tariff_fk_pct")

    rows: list[dict[str, Any]] = []
    counter: dict[str, int] = {}

    for station_id in sorted(current_stations):
        station = current_stations[station_id]
        code = station["_city_code"]
        count = rng.randrange(config.charge_points_min, config.charge_points_max + 1)

        for _ in range(count):
            counter[code] = counter.get(code, 0) + 1
            cp_id = f"CP-{code}-{counter[code]:04d}"
            is_dc = rng.random() < 0.55
            current_type = "DC" if is_dc else "AC"
            rated = (
                rng.choice([30.0, 30.0, 60.0, 120.0]) if is_dc else rng.choice([7.4, 11.0, 22.0])
            )
            plan = rng.choice(dc_plans if is_dc else ac_plans)
            if rng.random() < dangling_fk_rate:
                # The bad admin edit: a plan id that was never in the catalogue.
                plan = f"TP-{'DC' if is_dc else 'AC'}-DELETED{rng.randrange(10, 99)}"
                ledger.record("cms_dangling_tariff_fk")
            commissioned = station["commissioned_date"] + timedelta(days=rng.randrange(0, 90))
            created = datetime.combine(commissioned, time.min, tzinfo=UTC)

            base = {
                "charge_point_id": cp_id,
                "station_id": station_id,
                "oem_vendor": rng.choice(_OEM_VENDORS),
                "model": f"{rng.choice(['VH', 'PX', 'EV'])}-{int(rated)}{'D' if is_dc else 'A'}",
                "current_type": current_type,
                "rated_power_kw": rated,
                "connector_type": (
                    rng.choice(["CCS2", "CCS2", "CHAdeMO"])
                    if is_dc
                    else rng.choice(["TYPE2", "TYPE2", "BHARAT_AC001"])
                ),
                "tariff_plan_id": plan,
                "firmware_version": rng.choice(["3.3.7", "3.4.0", "3.4.1"]),
                "status": "ACTIVE",
                "commissioned_date": commissioned,
                "created_at": created,
                "updated_at": created,
                "_city_code": code,
                "_version_no": 1,
            }
            rows.append(base)

            # The 30 -> 60 kW upgrade. This is the example in the README, the
            # example in the SCD2 tests, and the one an interviewer asks about.
            #
            # It is eligible only for DC devices that shipped at 30 kW - about
            # a quarter of the fleet - so the effective rate is roughly a
            # quarter of the declared one. On the small fleets used by CI that
            # is low enough to produce no upgrade at all in some runs, which
            # would leave the entire Type 2 apparatus unexercised. The tiny
            # profile therefore raises this rate through change_rate_overrides
            # rather than relying on the global value; the test named
            # test_the_power_upgrade_is_always_generated holds that guarantee.
            if is_dc and rated == 30.0 and rng.random() < upgrade_rate:
                upgraded = dict(rows[-1])
                upgraded["rated_power_kw"] = 60.0
                upgraded["tariff_plan_id"] = "TP-DC-PREM"
                upgraded["firmware_version"] = "3.5.0"
                upgraded["updated_at"] = _random_instant(
                    rng,
                    config.start_date
                    + timedelta(days=rng.randrange(config.day_count // 4, config.day_count)),
                )
                rows.append(upgraded)

            if rng.random() < firmware_rate:
                patched = dict(rows[-1])
                patched["firmware_version"] = rng.choice(["3.4.1", "3.5.0", "3.5.1"])
                patched["updated_at"] = _random_instant(
                    rng, config.start_date + timedelta(days=rng.randrange(config.day_count))
                )
                rows.append(patched)

            if rng.random() < tariff_move_rate:
                moved = dict(rows[-1])
                moved["tariff_plan_id"] = rng.choice(dc_plans if is_dc else ac_plans)
                moved["updated_at"] = _random_instant(
                    rng, config.start_date + timedelta(days=rng.randrange(config.day_count))
                )
                rows.append(moved)

            if rng.random() < relocation_rate and len(current_stations) > 1:
                relocated = dict(rows[-1])
                relocated["station_id"] = rng.choice(
                    [s for s in sorted(current_stations) if s != station_id]
                )
                relocated["updated_at"] = _random_instant(
                    rng, config.start_date + timedelta(days=rng.randrange(config.day_count))
                )
                rows.append(relocated)

    rows.sort(key=lambda r: (r["charge_point_id"], r["updated_at"]))
    return _renumber(rows, "charge_point_id")


def _generate_customers(
    config: GeneratorConfig, rng: random.Random, ledger: DefectLedger
) -> list[dict[str, Any]]:
    """Customers with relocations, segment migrations, plan changes and churn.

    Personal data is synthetic by construction, not by promise: emails use the
    RFC 2606 reserved ``.invalid`` TLD, which can never resolve, and phone
    numbers use a reserved prefix.
    """
    codes = sorted(config.cities)
    relocation_rate = config.change_rate("customer_relocation_pct")
    segment_rate = config.change_rate("customer_segment_change_pct")
    plan_rate = config.change_rate("customer_plan_change_pct")
    deactivation_rate = config.change_rate("customer_deactivation_pct")
    null_city_rate = config.defect_rate("cms_null_city_pct")
    casing_rate = config.defect_rate("cms_segment_casing_pct")
    updated_before_created_rate = config.defect_rate("cms_updated_before_created_pct")

    rows: list[dict[str, Any]] = []
    for index in range(1, config.customer_count + 1):
        customer_id = f"CUS-{index:06d}"
        code = rng.choice(codes)
        city = config.cities[code]
        segment = _weighted_choice(rng, config.customer_segments, config.segment_weights)
        plan = {
            "RETAIL": rng.choice(["PAYG", "PLUS"]),
            "CORPORATE": rng.choice(["PLUS", "FLEET_PRO"]),
            "FLEET": "FLEET_PRO",
        }[segment]
        signup = config.start_date - timedelta(days=rng.randrange(30, 900))
        created = datetime.combine(signup, time.min, tzinfo=UTC)
        first = rng.choice(_FIRST_NAMES)

        base = {
            "customer_id": customer_id,
            "full_name": f"{first} {rng.choice(_LAST_INITIALS)}{rng.choice(['harma', 'ao', 'yer', 'atel', 'han', 'eddy'])}",
            "email": f"{first.lower()}.{index}@example.invalid",
            "phone": f"+9199000{index % 100000:05d}",
            "city": city.name,
            "state": city.state,
            "customer_segment": (segment.title() if rng.random() < casing_rate else segment),
            "subscription_plan": plan,
            "kyc_status": rng.choices(
                ["VERIFIED", "PENDING", "REJECTED"], weights=[88, 10, 2], k=1
            )[0],
            "signup_date": signup,
            "is_active": True,
            "created_at": created,
            "updated_at": created,
            "_city_code": code,
            "_version_no": 1,
        }
        # The injected clock defect: updated_at earlier than created_at. It is
        # a WARNING, not a rejection - the row is still usable, and treating a
        # sloppy source timestamp as fatal would be the wrong call.
        if rng.random() < updated_before_created_rate:
            base["created_at"] = created + timedelta(hours=rng.randrange(1, 72))
            ledger.record("cms_updated_before_created")
        if rng.random() < null_city_rate:
            base["city"] = None
            ledger.record("cms_null_city")
        if str(base["customer_segment"]) != str(base["customer_segment"]).upper():
            ledger.record("cms_segment_casing")
        rows.append(base)

        if rng.random() < relocation_rate:
            moved = dict(rows[-1])
            new_code = rng.choice([c for c in codes if c != code] or codes)
            moved["city"] = config.cities[new_code].name
            moved["state"] = config.cities[new_code].state
            moved["_city_code"] = new_code
            moved["updated_at"] = _random_instant(
                rng, config.start_date + timedelta(days=rng.randrange(config.day_count))
            )
            rows.append(moved)

        if rng.random() < segment_rate:
            promoted = dict(rows[-1])
            promoted["customer_segment"] = rng.choice(
                [s for s in config.customer_segments if s != segment]
            )
            promoted["updated_at"] = _random_instant(
                rng, config.start_date + timedelta(days=rng.randrange(config.day_count))
            )
            rows.append(promoted)

        if rng.random() < plan_rate:
            upgraded = dict(rows[-1])
            upgraded["subscription_plan"] = rng.choice(config.subscription_plans)
            upgraded["updated_at"] = _random_instant(
                rng, config.start_date + timedelta(days=rng.randrange(config.day_count))
            )
            rows.append(upgraded)

        # A soft delete is a CHANGE, not a disappearance: the current version
        # is expired and a tombstone version is inserted, so history stays
        # intact and "how many customers churned in May" is answerable.
        if rng.random() < deactivation_rate:
            churned = dict(rows[-1])
            churned["is_active"] = False
            churned["updated_at"] = _random_instant(
                rng,
                config.start_date
                + timedelta(days=rng.randrange(config.day_count // 2, config.day_count)),
            )
            rows.append(churned)

    rows.sort(key=lambda r: (r["customer_id"], r["updated_at"]))
    return _renumber(rows, "customer_id")


def _generate_vehicles(
    config: GeneratorConfig, customers: list[dict[str, Any]], rng: random.Random
) -> list[dict[str, Any]]:
    """Roughly 1.16 vehicles per customer. Type 1 - no version history.

    Deliberately flat: a vehicle's physical attributes do not change, only data
    corrections do, and correcting a battery-capacity typo should fix all
    history rather than invent a change event.
    """
    customer_ids = sorted({row["customer_id"] for row in customers})
    rows: list[dict[str, Any]] = []
    index = 0
    for customer_id in customer_ids:
        for _ in range(1 if rng.random() > 0.16 else 2):
            index += 1
            make, models, vclass, battery = rng.choice(_MAKES)
            created = datetime.combine(
                config.start_date - timedelta(days=rng.randrange(10, 800)),
                time.min,
                tzinfo=UTC,
            )
            rows.append(
                {
                    "vehicle_id": f"VEH-{index:06d}",
                    "customer_id": customer_id,
                    "make": make,
                    "model": rng.choice(models),
                    "model_year": rng.randrange(2021, 2027),
                    "battery_capacity_kwh": round(battery * rng.uniform(0.9, 1.15), 1),
                    "connector_type": _weighted_choice(
                        rng, config.connector_types, config.connector_weights
                    ),
                    "registration_state": rng.choice([c.state for c in config.cities.values()]),
                    "created_at": created,
                    "updated_at": created,
                    "_vehicle_class": vclass,
                    "_version_no": 1,
                }
            )
    rows.sort(key=lambda r: r["vehicle_id"])
    return rows


def _renumber(rows: list[dict[str, Any]], key: str) -> list[dict[str, Any]]:
    """Assign sequential ``_version_no`` per natural key after sorting.

    Version numbers are a readability aid, not a correctness mechanism - the
    SCD2 merge derives its own - but they make a failing test's output
    dramatically easier to read, and that is worth ten lines.
    """
    seen: dict[str, int] = {}
    for row in rows:
        seen[row[key]] = seen.get(row[key], 0) + 1
        row["_version_no"] = seen[row[key]]
    return rows


def generate_master_data(config: GeneratorConfig, ledger: DefectLedger | None = None) -> MasterData:
    """Generate every master entity with its full version history.

    Each entity draws from its OWN seeded generator, offset from the master
    seed. That is not decoration: it means changing the number of customers
    cannot shift the random stream that produces stations, so a profile change
    does not silently invalidate every previously-recorded expectation.
    """
    ledger = ledger if ledger is not None else DefectLedger()
    tariff_plans = _generate_tariff_plans(config, random.Random(config.seed + 101))
    stations = _generate_stations(config, random.Random(config.seed + 102))
    charge_points = _generate_charge_points(
        config, stations, tariff_plans, random.Random(config.seed + 103), ledger
    )
    customers = _generate_customers(config, random.Random(config.seed + 104), ledger)
    vehicles = _generate_vehicles(config, customers, random.Random(config.seed + 105))

    return MasterData(
        customers=customers,
        vehicles=vehicles,
        stations=stations,
        charge_points=charge_points,
        tariff_plans=tariff_plans,
    )
