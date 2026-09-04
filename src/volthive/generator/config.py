"""Typed access to ``configs/generator.yml``.

The generator has a lot of knobs, and the difference between a simulator that
is useful and one that is a liability is whether those knobs are *declared*
somewhere reviewable. They are here, in YAML, so changing a defect rate is a
diff rather than an edit to a literal buried in a loop.

Everything is validated on load. A typo in a profile name, or a defect rate of
1.5 where a fraction was meant, fails immediately with a message naming the
key - rather than silently producing a dataset in which 150% of records are
duplicates and every downstream count is inexplicable.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from volthive.config.settings import default_config_dir, load_yaml_config
from volthive.exceptions import ConfigurationError

__all__ = ["CityConfig", "GeneratorConfig", "load_generator_config"]

#: Half-open validity sentinel shared with the SCD2 dimensions.
OPEN_END_UTC = datetime(9999, 12, 31, tzinfo=UTC)


@dataclass(frozen=True, slots=True)
class CityConfig:
    """One city's fixed attributes."""

    code: str
    name: str
    state: str
    tier: str
    pincode_prefix: str


@dataclass(slots=True)
class GeneratorConfig:
    """Resolved generator configuration for one profile."""

    seed: int
    profile: str
    start_date: date
    end_date: date
    cities: dict[str, CityConfig]
    station_count: int
    customer_count: int
    sessions_per_day: int
    site_types: list[str]
    site_type_weights: list[int]
    charge_points_min: int
    charge_points_max: int
    customer_segments: list[str]
    segment_weights: list[int]
    subscription_plans: list[str]
    connector_types: list[str]
    connector_weights: list[int]
    change_rates: dict[str, float]
    defects: dict[str, float]
    roaming_session_pct: float
    roaming_partners: list[str]
    roaming_revision_window_days: int
    schema_evolution_from: date
    schema_evolution_field: str
    meter_sample_interval_seconds: int
    meter_max_samples: int
    raw: dict[str, Any] = field(default_factory=dict, repr=False)

    @property
    def day_count(self) -> int:
        """Number of days in the simulation window, inclusive of both ends."""
        return (self.end_date - self.start_date).days + 1

    def defect_rate(self, name: str) -> float:
        """Return a defect rate as a FRACTION (the YAML declares percentages).

        Raises:
            ConfigurationError: the rate is not declared. Returning 0.0 for an
                unknown name would silently disable a defect - and therefore
                make the data-quality rule that catches it pass vacuously,
                which is the worst possible failure for a test suite whose
                whole point is proving those rules fire.
        """
        if name not in self.defects:
            raise ConfigurationError(
                f"Unknown defect rate '{name}'. Declared rates: {sorted(self.defects)}",
                entity="generator.yml:defects",
            )
        return self.defects[name] / 100.0

    def change_rate(self, name: str) -> float:
        """Return a change rate as a fraction. See :meth:`defect_rate`."""
        if name not in self.change_rates:
            raise ConfigurationError(
                f"Unknown change rate '{name}'. Declared rates: {sorted(self.change_rates)}",
                entity="generator.yml:change_rates",
            )
        return self.change_rates[name] / 100.0


def _require(mapping: dict[str, Any], key: str, where: str) -> Any:
    if key not in mapping:
        raise ConfigurationError(
            f"generator.yml is missing required key '{key}' in {where}",
            entity="generator.yml",
            expected=key,
        )
    return mapping[key]


def load_generator_config(
    profile: str | None = None,
    *,
    seed: int | None = None,
    config_dir: Path | None = None,
) -> GeneratorConfig:
    """Load and validate the generator configuration for one profile.

    Args:
        profile: ``default`` / ``small`` / ``tiny``. Falls back to
            ``VOLTHIVE_GENERATOR_PROFILE``.
        seed: Overrides the seed in the file. Used by the determinism test,
            which needs to prove that a DIFFERENT seed produces different data
            just as much as that the same seed reproduces identical data.
        config_dir: Overrides the configuration directory, for tests.
    """
    # Deliberately NOT through Settings: choosing a generator profile must not
    # require database credentials, so a unit test - or a developer with no
    # .env at all - can still load and validate the configuration.
    import os

    profile_name: str = profile or os.environ.get("VOLTHIVE_GENERATOR_PROFILE") or "default"
    document = load_yaml_config("generator.yml", config_dir=config_dir or default_config_dir())

    profiles = _require(document, "profiles", "top level")
    if profile_name not in profiles:
        raise ConfigurationError(
            f"Unknown generator profile '{profile_name}'. Available: {sorted(profiles)}",
            entity="generator.yml:profiles",
        )
    selected = profiles[profile_name]

    city_defs = _require(document, "cities", "top level")
    enabled_codes = _require(selected, "cities", f"profiles.{profile_name}")
    unknown = [code for code in enabled_codes if code not in city_defs]
    if unknown:
        raise ConfigurationError(
            f"Profile '{profile_name}' enables undefined cities: {unknown}",
            entity="generator.yml:profiles",
        )

    cities = {
        code: CityConfig(
            code=code,
            name=city_defs[code]["name"],
            state=city_defs[code]["state"],
            tier=city_defs[code]["tier"],
            pincode_prefix=str(city_defs[code]["pincode_prefix"]),
        )
        for code in enabled_codes
    }

    start = date.fromisoformat(str(_require(selected, "start_date", profile_name)))
    end = date.fromisoformat(str(_require(selected, "end_date", profile_name)))
    if end < start:
        raise ConfigurationError(
            f"Profile '{profile_name}' has end_date {end} before start_date {start}",
            entity="generator.yml:profiles",
        )

    # Profile-level defect overrides are merged over the global rates. The tiny
    # profile raises the rarest defects so a seven-day window still contains
    # every one of them, and the clean profile zeroes them all. Without the
    # first, a 0.05%-rate defect over a few hundred sessions is absent about
    # half the time - and a quality rule whose trigger was never generated
    # PASSES VACUOUSLY, which is the worst possible outcome for a suite whose
    # purpose is proving those rules fire.
    defects = dict(_require(document, "defects", "top level"))
    overrides = selected.get("defect_overrides", {}) or {}
    unknown_overrides = sorted(set(overrides) - set(defects))
    if unknown_overrides:
        raise ConfigurationError(
            f"Profile '{profile_name}' overrides undeclared defect rates: {unknown_overrides}",
            entity="generator.yml:defect_overrides",
        )
    defects.update({k: float(v) for k, v in overrides.items()})

    for name, value in defects.items():
        if not 0 <= float(value) <= 100:
            raise ConfigurationError(
                f"Defect rate '{name}' is {value}; rates are PERCENTAGES and must be 0-100",
                entity="generator.yml:defects",
            )

    # Change rates get exactly the same profile-override treatment as defect
    # rates, and for exactly the same reason. The headline SCD Type 2
    # demonstration in this project is a charge point upgraded from 30 kW to
    # 60 kW mid-window. At the global 6% rate, applied only to DC devices that
    # shipped at 30 kW, the tiny profile's ~60 devices produce ZERO upgrades
    # roughly a third of the time - and when that happens the before/after
    # analytics query returns an empty set, the SCD2 tests still pass, and the
    # single most important behaviour in the warehouse goes unexercised. A
    # demonstration that is present only on average is not a demonstration.
    change_rates = dict(_require(document, "change_rates", "top level"))
    rate_overrides = selected.get("change_rate_overrides", {}) or {}
    unknown_rate_overrides = sorted(set(rate_overrides) - set(change_rates))
    if unknown_rate_overrides:
        raise ConfigurationError(
            f"Profile '{profile_name}' overrides undeclared change rates: "
            f"{unknown_rate_overrides}",
            entity="generator.yml:change_rate_overrides",
        )
    change_rates.update({k: float(v) for k, v in rate_overrides.items()})

    for name, value in change_rates.items():
        # tariff_revisions_per_plan is a COUNT, not a percentage, and is the one
        # documented exception to the 0-100 range.
        if name != "tariff_revisions_per_plan" and not 0 <= float(value) <= 100:
            raise ConfigurationError(
                f"Change rate '{name}' is {value}; rates are PERCENTAGES and must be 0-100",
                entity="generator.yml:change_rates",
            )

    cp_range = document.get("charge_points_per_station", {"min": 2, "max": 12})
    roaming = document.get("roaming", {})
    evolution = document.get("schema_evolution", {})
    meter = document.get("meter", {})

    return GeneratorConfig(
        seed=(
            seed
            if seed is not None
            else int(os.environ.get("VOLTHIVE_GENERATOR_SEED", document.get("seed", 42)))
        ),
        profile=profile_name,
        start_date=start,
        end_date=end,
        cities=cities,
        station_count=int(_require(selected, "stations", profile_name)),
        customer_count=int(_require(selected, "customers", profile_name)),
        sessions_per_day=int(_require(selected, "sessions_per_day", profile_name)),
        site_types=list(document.get("site_types", [])),
        site_type_weights=list(document.get("site_type_weights", [])),
        charge_points_min=int(cp_range.get("min", 2)),
        charge_points_max=int(cp_range.get("max", 12)),
        customer_segments=list(document.get("customer_segments", [])),
        segment_weights=list(document.get("segment_weights", [])),
        subscription_plans=list(document.get("subscription_plans", [])),
        connector_types=list(document.get("connector_types", [])),
        connector_weights=list(document.get("connector_weights", [])),
        change_rates={k: float(v) for k, v in change_rates.items()},
        defects={k: float(v) for k, v in defects.items()},
        roaming_session_pct=float(roaming.get("session_pct", 6.0)),
        roaming_partners=list(roaming.get("partners", [])),
        roaming_revision_window_days=int(roaming.get("revision_window_days", 7)),
        schema_evolution_from=date.fromisoformat(
            str(evolution.get("new_field_from_date", "2099-01-01"))
        ),
        schema_evolution_field=str(evolution.get("new_field_name", "grid_carbon_intensity")),
        meter_sample_interval_seconds=int(meter.get("sample_interval_seconds", 300)),
        meter_max_samples=int(meter.get("max_samples_per_session", 40)),
        raw=document,
    )
