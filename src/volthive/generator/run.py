"""Orchestration of a full generator run.

One entry point, :func:`generate_all`, that produces everything the platform
ingests: the simulated CMS database, the partitioned landing files, the static
grid-tariff seed, and the ground-truth manifest the tests assert against.

Days are generated and written ONE AT A TIME. At the default profile the window
holds roughly 11.5 million meter samples, and accumulating them before writing
would need several gigabytes of memory for no benefit. The only thing held
across days is the roaming-partner dataset, which is partitioned by
``last_updated`` rather than by session date - a record revised five days later
belongs in a later partition - and at roughly 62,000 records that is a few
megabytes, which is a fair price for getting the partitioning right.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

from volthive.config.settings import default_data_dir, get_settings, repo_root
from volthive.db.connection import connect
from volthive.db.sqlfiles import load_sql
from volthive.generator.config import GeneratorConfig, load_generator_config
from volthive.generator.defects import DefectLedger
from volthive.generator.entities import generate_master_data
from volthive.generator.sessions import build_universe, generate_day
from volthive.generator.writer import (
    apply_cms_schema,
    write_cdr_files,
    write_marker,
    write_master_data,
    write_meter_file,
    write_partner_files,
)
from volthive.logging_setup import get_logger

__all__ = ["GenerationResult", "generate_all", "write_grid_tariff_seed"]

log = get_logger(__name__)

#: Source S5. Illustrative commercial rates per state, NOT audited real
#: tariffs - the source_note column says so on every row, and the README
#: repeats it. Invented numbers presented as real are the fastest way to make a
#: reviewer distrust everything else in a project.
_GRID_SLABS: list[tuple[str, str, str, str, float]] = [
    ("Karnataka", "2024-04-01", "2025-03-31", "HT-2A Commercial", 7.65),
    ("Karnataka", "2025-04-01", "2026-03-31", "HT-2A Commercial", 8.10),
    ("Karnataka", "2026-04-01", "2027-03-31", "HT-2A Commercial", 8.55),
    ("Delhi", "2024-04-01", "2025-03-31", "EV Charging Tariff", 4.90),
    ("Delhi", "2025-04-01", "2026-03-31", "EV Charging Tariff", 5.20),
    ("Delhi", "2026-04-01", "2027-03-31", "EV Charging Tariff", 5.50),
    ("Maharashtra", "2024-04-01", "2025-03-31", "EV-HT Commercial", 7.20),
    ("Maharashtra", "2025-04-01", "2026-03-31", "EV-HT Commercial", 7.55),
    ("Maharashtra", "2026-04-01", "2027-03-31", "EV-HT Commercial", 7.95),
    ("Telangana", "2024-04-01", "2025-03-31", "EV Charging LT-VIII", 6.70),
    ("Telangana", "2025-04-01", "2026-03-31", "EV Charging LT-VIII", 6.95),
    ("Telangana", "2026-04-01", "2027-03-31", "EV Charging LT-VIII", 7.30),
    ("Tamil Nadu", "2024-04-01", "2025-03-31", "HT-Commercial EV", 7.40),
    ("Tamil Nadu", "2025-04-01", "2026-03-31", "HT-Commercial EV", 7.75),
    ("Tamil Nadu", "2026-04-01", "2027-03-31", "HT-Commercial EV", 8.15),
    ("Gujarat", "2024-04-01", "2025-03-31", "EV Charging Station", 6.20),
    ("Gujarat", "2025-04-01", "2026-03-31", "EV Charging Station", 6.55),
    ("Gujarat", "2026-04-01", "2027-03-31", "EV Charging Station", 6.90),
    ("West Bengal", "2024-04-01", "2025-03-31", "Commercial EV", 7.85),
    ("West Bengal", "2025-04-01", "2026-03-31", "Commercial EV", 8.20),
    ("West Bengal", "2026-04-01", "2027-03-31", "Commercial EV", 8.60),
]

_GRID_NOTE = "Illustrative rate for this project. Not an audited real tariff."


@dataclass(slots=True)
class GenerationResult:
    """What a run produced. Returned so the CLI can report it and tests can
    assert on it without re-reading the filesystem."""

    profile: str
    seed: int
    start_date: date
    end_date: date
    master_rows: dict[str, int]
    cdr_files: int
    meter_files: int
    partner_files: int
    ledger: DefectLedger
    duration_seconds: float
    truth_manifest: Path


def write_grid_tariff_seed(seed_dir: Path) -> Path:
    """Write ``data/seed/grid_tariff_slabs.csv``.

    The ONE data file committed to git. It is small, hand-authored reference
    data rather than generated volume, and it is what turns
    ``gross_margin_inr`` into a real derived measure - revenue minus what
    VoltHive paid the grid - instead of a column that just mirrors revenue.
    """
    seed_dir.mkdir(parents=True, exist_ok=True)
    path = seed_dir / "grid_tariff_slabs.csv"
    lines = [
        "state,effective_from_date,effective_to_date,slab_name,commercial_rate_inr_per_kwh,source_note"
    ]
    for state, valid_from, valid_to, slab, rate in _GRID_SLABS:
        lines.append(f"{state},{valid_from},{valid_to},{slab},{rate:.2f},{_GRID_NOTE}")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


def _load_cms(config: GeneratorConfig, master: Any, ledger: DefectLedger) -> dict[str, int]:
    """Create the simulated source schema and load master data into it.

    Connects as ``cms_owner``: the generator is playing the part of the source
    system here, not of the pipeline. The pipeline's own identity,
    ``cms_reader``, has SELECT and nothing else - which is what makes "the
    pipeline cannot corrupt its source" a property of the database rather than
    a promise about the code.
    """
    settings = get_settings()
    import os
    from urllib.parse import quote_plus

    owner = os.environ.get("CMS_OWNER_USER", "cms_owner")
    password = os.environ.get("CMS_OWNER_PASSWORD")
    if not password:
        from volthive.exceptions import ConfigurationError

        raise ConfigurationError(
            "CMS_OWNER_PASSWORD must be set to load the simulated source database. "
            "It is in .env.example; scripts/generate_env.sh fills in the generated keys.",
            entity="CMS_OWNER_PASSWORD",
        )
    dsn = (
        f"postgresql://{quote_plus(owner)}:{quote_plus(password)}"
        f"@{settings.postgres_host}:{settings.postgres_port}/{settings.cms_db}"
    )

    counts: dict[str, int] = {}
    entities = {
        "customers": (master.customers, "customer_id"),
        "vehicles": (master.vehicles, "vehicle_id"),
        "stations": (master.stations, "station_id"),
        "charge_points": (master.charge_points, "charge_point_id"),
        "tariff_plans": (master.tariff_plans, "tariff_plan_id"),
    }

    with connect(dsn, application_name="volthive-generator", autocommit=False) as conn:
        apply_cms_schema(conn)
        for entity, (versions, key) in entities.items():
            current = master.current(versions, key)
            history_rows, current_rows = write_master_data(conn, entity, versions, current)
            counts[f"{entity}_history"] = history_rows
            counts[entity] = current_rows
            ledger.count_total(f"cms_{entity}_versions", history_rows)
            ledger.count_total(f"cms_{entity}_current", current_rows)
        conn.commit()

    _ = config
    return counts


def generate_all(
    profile: str | None = None,
    *,
    seed: int | None = None,
    data_dir: Path | None = None,
    load_cms: bool = True,
) -> GenerationResult:
    """Generate the complete synthetic dataset.

    Args:
        profile: ``default`` / ``small`` / ``tiny``.
        seed: Overrides the configured seed.
        data_dir: Root of the landing zone. Defaults to the configured one.
        load_cms: Whether to populate the CMS database. Tests that only need
            the landing files pass False and skip the database entirely, which
            keeps the file-shape tests runnable without any database at all.

    Returns:
        A :class:`GenerationResult` describing what was produced.
    """
    started = time.monotonic()
    config = load_generator_config(profile, seed=seed)
    root = data_dir or default_data_dir()
    landing = root / "landing"
    ledger = DefectLedger()

    log.info(
        "generation_started",
        profile=config.profile,
        seed=config.seed,
        start_date=str(config.start_date),
        end_date=str(config.end_date),
        days=config.day_count,
    )

    write_marker(root)
    write_marker(landing)
    write_grid_tariff_seed(repo_root() / "data" / "seed")

    master = generate_master_data(config, ledger)
    master_rows: dict[str, int] = {}
    if load_cms:
        master_rows = _load_cms(config, master, ledger)
    else:
        for name, rows, key in (
            ("customers", master.customers, "customer_id"),
            ("vehicles", master.vehicles, "vehicle_id"),
            ("stations", master.stations, "station_id"),
            ("charge_points", master.charge_points, "charge_point_id"),
            ("tariff_plans", master.tariff_plans, "tariff_plan_id"),
        ):
            master_rows[f"{name}_history"] = len(rows)
            master_rows[name] = len(master.current(rows, key))

    universe = build_universe(master, config)

    partner_by_day: dict[date, list[dict[str, Any]]] = {}
    cdr_files = meter_files = 0
    day = config.start_date
    while day <= config.end_date:
        output = generate_day(day, config, universe, ledger)

        cdr_files += len(write_cdr_files(landing, day, output.cdrs_by_city))
        write_meter_file(landing, day, output.meter_samples)
        meter_files += 1
        ledger.count_total("meter_samples_emitted", len(output.meter_samples))

        for record in output.partner_records:
            bucket = datetime.fromisoformat(record["last_updated"].replace("Z", "+00:00")).date()
            partner_by_day.setdefault(bucket, []).append(record)
        ledger.count_total("partner_records_emitted", len(output.partner_records))

        if day.day == 1 or day == config.end_date:
            log.info("generation_progress", day=str(day))
        day += timedelta(days=1)

    partner_files = len(write_partner_files(landing, partner_by_day))

    duration = time.monotonic() - started
    manifest = ledger.write(
        root / "_truth",
        metadata={
            "profile": config.profile,
            "seed": config.seed,
            "start_date": str(config.start_date),
            "end_date": str(config.end_date),
            "cities": sorted(config.cities),
            "cdr_files": cdr_files,
            "meter_files": meter_files,
            "partner_files": partner_files,
            "master_rows": master_rows,
        },
    )

    log.info(
        "generation_completed",
        profile=config.profile,
        duration_seconds=round(duration, 2),
        cdr_files=cdr_files,
        meter_files=meter_files,
        partner_files=partner_files,
        sessions=ledger.totals.get("sessions_emitted", 0),
        meter_samples=ledger.totals.get("meter_samples_emitted", 0),
    )

    _ = load_sql  # imported for symmetry with the schema loader; not used here
    return GenerationResult(
        profile=config.profile,
        seed=config.seed,
        start_date=config.start_date,
        end_date=config.end_date,
        master_rows=master_rows,
        cdr_files=cdr_files,
        meter_files=meter_files,
        partner_files=partner_files,
        ledger=ledger,
        duration_seconds=duration,
        truth_manifest=manifest,
    )
