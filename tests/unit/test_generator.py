"""The generator's determinism and its ground-truth contract.

Determinism is not a nicety here - it is what makes every other test in the
suite able to assert an exact number instead of a tolerance. If an unseeded
random call ever crept in, the integration suite would become intermittently
red for reasons nobody could reproduce, and the usual response to that is to
weaken the assertions until it goes quiet.
"""

from __future__ import annotations

import hashlib
import json
from datetime import date
from pathlib import Path

import pytest

from volthive.generator.config import load_generator_config
from volthive.generator.defects import DEFECT_CATALOGUE, DefectLedger
from volthive.generator.entities import generate_master_data
from volthive.generator.run import generate_all
from volthive.generator.sessions import build_universe, generate_day

pytestmark = pytest.mark.unit

REPO_ROOT = Path(__file__).resolve().parents[2]
CONFIG_DIR = REPO_ROOT / "configs"


@pytest.fixture
def tiny_config():
    return load_generator_config("tiny", config_dir=CONFIG_DIR)


def _tree_hash(root: Path) -> str:
    digest = hashlib.sha256()
    for path in sorted(root.rglob("*")):
        if path.is_file():
            digest.update(str(path.relative_to(root)).encode())
            digest.update(path.read_bytes())
    return digest.hexdigest()


class TestConfiguration:
    def test_profiles_load(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("VOLTHIVE_CONFIG_DIR", str(CONFIG_DIR))
        for profile in ("default", "small", "tiny", "clean"):
            assert load_generator_config(profile, config_dir=CONFIG_DIR).profile == profile

    def test_unknown_profile_is_rejected(self) -> None:
        from volthive.exceptions import ConfigurationError

        with pytest.raises(ConfigurationError, match="Unknown generator profile"):
            load_generator_config("does_not_exist", config_dir=CONFIG_DIR)

    def test_unknown_defect_rate_is_rejected(self, tiny_config) -> None:
        """Returning 0.0 for an unknown name would silently disable a defect.

        The rule that catches it would then pass vacuously - a green check
        proving nothing, which is worse than a red one.
        """
        from volthive.exceptions import ConfigurationError

        with pytest.raises(ConfigurationError, match="Unknown defect rate"):
            tiny_config.defect_rate("not_a_real_defect_pct")

    def test_tiny_profile_amplifies_the_rarest_defects(self, tiny_config) -> None:
        """A 0.05%-rate defect over ~600 sessions is absent about half the time."""
        default = load_generator_config("default", config_dir=CONFIG_DIR)
        assert tiny_config.defect_rate("cdr_energy_out_of_range_pct") > default.defect_rate(
            "cdr_energy_out_of_range_pct"
        )

    def test_clean_profile_injects_nothing(self) -> None:
        clean = load_generator_config("clean", config_dir=CONFIG_DIR)
        assert all(rate == 0 for rate in clean.defects.values())


class TestDeterminism:
    def test_same_seed_produces_identical_files(self, tmp_path: Path) -> None:
        first = tmp_path / "a"
        second = tmp_path / "b"
        generate_all("tiny", data_dir=first, load_cms=False)
        generate_all("tiny", data_dir=second, load_cms=False)
        assert _tree_hash(first / "landing") == _tree_hash(second / "landing")

    def test_a_different_seed_produces_different_files(self, tmp_path: Path) -> None:
        """The other half of determinism: the seed must actually do something."""
        first = tmp_path / "a"
        second = tmp_path / "b"
        generate_all("tiny", seed=42, data_dir=first, load_cms=False)
        generate_all("tiny", seed=99, data_dir=second, load_cms=False)
        assert _tree_hash(first / "landing") != _tree_hash(second / "landing")

    def test_master_data_is_deterministic(self, tiny_config) -> None:
        first = generate_master_data(tiny_config)
        second = generate_master_data(tiny_config)
        assert [r["charge_point_id"] for r in first.charge_points] == [
            r["charge_point_id"] for r in second.charge_points
        ]

    def test_one_day_regenerates_identically_in_isolation(self, tiny_config) -> None:
        """The property a targeted restatement depends on.

        Regenerating a single day must produce exactly what generating the
        whole window produced for that day, or a restatement test would be
        comparing against data the pipeline never saw.
        """
        master = generate_master_data(tiny_config)
        day = date(2026, 6, 3)
        first = generate_day(day, tiny_config, build_universe(master, tiny_config), DefectLedger())
        second = generate_day(day, tiny_config, build_universe(master, tiny_config), DefectLedger())
        assert json.dumps(first.cdrs_by_city, default=str, sort_keys=True) == json.dumps(
            second.cdrs_by_city, default=str, sort_keys=True
        )


class TestChangeHistory:
    def test_updated_at_is_monotonic_per_key(self, tiny_config) -> None:
        """Version lists must be ordered, or the SCD2 merge chains them wrongly."""
        master = generate_master_data(tiny_config)
        for rows, key in (
            (master.charge_points, "charge_point_id"),
            (master.customers, "customer_id"),
            (master.tariff_plans, "tariff_plan_id"),
        ):
            seen: dict[str, object] = {}
            for row in rows:
                previous = seen.get(row[key])
                if previous is not None:
                    assert row["updated_at"] >= previous, key
                seen[row[key]] = row["updated_at"]

    def test_tariff_plans_are_revised_within_the_window(self, tiny_config) -> None:
        """No revisions means nothing for SCD Type 2 to demonstrate."""
        master = generate_master_data(tiny_config)
        versions_per_plan = {}
        for row in master.tariff_plans:
            versions_per_plan[row["tariff_plan_id"]] = (
                versions_per_plan.get(row["tariff_plan_id"], 0) + 1
            )
        assert min(versions_per_plan.values()) >= 2

    def test_the_power_upgrade_is_always_generated(self, tiny_config) -> None:
        """The 30 kW -> 60 kW upgrade must exist in EVERY tiny dataset.

        This is the headline SCD Type 2 demonstration: the before/after
        analytics query measures it, and the point-in-time join tests assert on
        it. It is eligible only for DC devices that shipped at 30 kW, so at the
        global 6% rate a ~60-device fleet produces none of them in a large
        minority of runs - and every downstream assertion about it would then
        pass VACUOUSLY over an empty set.

        The tiny profile raises the rate through change_rate_overrides. This
        test is what stops anyone quietly removing that override.
        """
        master = generate_master_data(tiny_config)
        powers: dict[str, set[float]] = {}
        for row in master.charge_points:
            powers.setdefault(row["charge_point_id"], set()).add(float(row["rated_power_kw"]))

        upgraded = {cp: seen for cp, seen in powers.items() if len(seen) > 1}
        assert upgraded, (
            "no charge point changed rated power - the SCD2 before/after "
            "demonstration would be an empty result set"
        )
        for charge_point_id, seen in upgraded.items():
            assert max(seen) > min(seen), charge_point_id
            assert 30.0 in seen and 60.0 in seen, (
                f"{charge_point_id} changed power but not via the documented "
                f"30 -> 60 kW upgrade: {sorted(seen)}"
            )

    def test_the_upgrade_lands_inside_the_simulation_window(self, tiny_config) -> None:
        """An upgrade dated before the first session has no 'before' period.

        The comparison needs sessions on both sides of the change. An upgrade
        stamped outside the window produces a one-sided cut, which the
        analytics query correctly drops - leaving nothing to show.
        """
        master = generate_master_data(tiny_config)
        by_key: dict[str, list[dict]] = {}
        for row in master.charge_points:
            by_key.setdefault(row["charge_point_id"], []).append(row)

        landed = 0
        for versions in by_key.values():
            for previous, current in zip(versions, versions[1:], strict=False):
                if float(current["rated_power_kw"]) > float(previous["rated_power_kw"]):
                    stamped = current["updated_at"].date()
                    if tiny_config.start_date <= stamped <= tiny_config.end_date:
                        landed += 1
        assert landed > 0, (
            "every power upgrade is stamped outside the simulation window, so "
            "no session precedes one"
        )

    def test_first_version_predates_the_window(self, tiny_config) -> None:
        """Otherwise a session early in the window has no version to resolve to."""
        master = generate_master_data(tiny_config)
        earliest: dict[str, object] = {}
        for row in master.tariff_plans:
            key = row["tariff_plan_id"]
            if key not in earliest:
                earliest[key] = row["updated_at"]
        for value in earliest.values():
            assert value.date() < tiny_config.start_date


class TestGroundTruth:
    def test_every_defect_type_is_produced_by_the_tiny_profile(self, tmp_path: Path) -> None:
        """A defect that never occurs leaves its rule untested and green."""
        result = generate_all("tiny", data_dir=tmp_path, load_cms=False)
        produced = set(result.ledger.counts)
        expected = set(DEFECT_CATALOGUE) - {"cms_dangling_tariff_fk"}
        missing = expected - produced
        assert not missing, f"the tiny profile produced no: {sorted(missing)}"

    def test_the_truth_manifest_is_written(self, tmp_path: Path) -> None:
        result = generate_all("tiny", data_dir=tmp_path, load_cms=False)
        manifest = json.loads(result.truth_manifest.read_text())
        assert manifest["defects"]
        assert manifest["totals"]["sessions_emitted"] > 0
        assert manifest["metadata"]["seed"] == 42

    def test_meter_energy_reconciles_to_session_energy(self, tiny_config) -> None:
        """Samples must span EXACTLY the session's energy - to the paisa.

        This is what lets an integration test assert that the interval fact
        RECONCILES to the session header rather than merely approximates it.
        A loose tolerance here would be worse than no test: it would quietly
        absorb a real drift between the two sources and leave the downstream
        reconciliation assertion resting on a coincidence.

        The tolerance is therefore ABSOLUTE and tiny - 0.5 Wh, enough to cover
        the two-decimal rounding applied to every register reading and nothing
        else. It is deliberately not a relative tolerance: 2% of a 25 kWh
        session is 500 Wh, which is large enough to hide an entire injected
        meter reset, and did.

        Sessions carrying that injected reset are excluded and counted, not
        silently tolerated. In those the register jumps BACKWARDS mid-session
        and never recovers, exactly as a physically reset meter would, so the
        sampled span legitimately under-reports the delivered energy by the
        size of the reset. That is the whole reason those intervals are
        quarantined downstream - so the exclusion is asserted to be non-empty,
        which makes this test cover both behaviours rather than one.
        """
        master = generate_master_data(tiny_config)
        output = generate_day(
            date(2026, 6, 2),
            tiny_config,
            build_universe(master, tiny_config),
            DefectLedger(),
        )

        by_transaction: dict[str, list[float]] = {}
        for sample in output.meter_samples:
            by_transaction.setdefault(sample["transaction_id"], []).append(
                float(sample["energy_register_wh"])
            )

        def has_meter_reset(registers: list[float]) -> bool:
            """A register that ever moves backwards had a reset injected."""
            return any(b < a for a, b in zip(registers, registers[1:], strict=False))

        checked = 0
        reset_sessions = 0
        for city_records in output.cdrs_by_city.values():
            for record in city_records:
                registers = by_transaction.get(record["transaction_id"])
                if not registers or record["energy_unit"] != "Wh" or record["record_version"] != 1:
                    continue
                delta = float(record["meter_stop_wh"]) - float(record["meter_start_wh"])
                if delta <= 0:
                    continue
                if has_meter_reset(registers):
                    reset_sessions += 1
                    continue
                sampled = max(registers) - min(registers)
                assert sampled == pytest.approx(delta, abs=0.5), (
                    f"{record['transaction_id']}: samples span {sampled} Wh but "
                    f"the record claims {delta} Wh"
                )
                checked += 1

        assert checked > 10, "too few clean sessions to call this a reconciliation"
        assert reset_sessions > 0, (
            "no meter reset was injected on this day, so the exclusion above is "
            "untested - MTR_NEGATIVE_INTERVAL_ENERGY may be passing vacuously"
        )

    def test_every_injected_meter_reset_produces_a_negative_interval(self, tiny_config) -> None:
        """A recorded defect that the data does not contain is a liar.

        The ledger is the ground-truth oracle the integration tests compare
        the warehouse against. If it counts a meter reset that produced no
        backwards register movement, then MTR_NEGATIVE_INTERVAL_ENERGY has
        nothing to quarantine, the counts disagree, and the discrepancy looks
        like a pipeline bug rather than a generator one.

        The original implementation subtracted a flat 500-5000 Wh, which a
        fast DC session's five-minute interval can more than replace. This
        asserts the ledger count and the observable backwards jumps agree
        EXACTLY - the only relationship worth asserting between an oracle and
        the data it describes.
        """
        ledger = DefectLedger()
        master = generate_master_data(tiny_config)
        output = generate_day(
            date(2026, 6, 2),
            tiny_config,
            build_universe(master, tiny_config),
            ledger,
        )

        by_transaction: dict[str, list[tuple[str, float]]] = {}
        for sample in output.meter_samples:
            by_transaction.setdefault(sample["transaction_id"], []).append(
                (sample["sample_timestamp"], float(sample["energy_register_wh"]))
            )

        backwards_steps = 0
        for readings in by_transaction.values():
            # Emission order is deliberately not timestamp order, and duplicate
            # frames are a separate injected defect; sort and de-duplicate so a
            # backwards step means a RESET rather than a delivery artefact.
            ordered = sorted(set(readings))
            backwards_steps += sum(
                1 for a, b in zip(ordered, ordered[1:], strict=False) if b[1] < a[1]
            )

        assert ledger.counts.get("meter_non_monotonic", 0) > 0, "nothing injected to check"
        assert backwards_steps == ledger.counts["meter_non_monotonic"], (
            f"the ledger claims {ledger.counts['meter_non_monotonic']} meter resets "
            f"but only {backwards_steps} intervals actually go backwards"
        )


class TestSyntheticDataMarkers:
    def test_emails_use_a_reserved_tld(self, tiny_config) -> None:
        """@example.invalid can never resolve. Synthetic by construction."""
        master = generate_master_data(tiny_config)
        assert all(row["email"].endswith("@example.invalid") for row in master.customers)

    def test_a_marker_file_is_written_into_the_landing_zone(self, tmp_path: Path) -> None:
        generate_all("tiny", data_dir=tmp_path, load_cms=False)
        marker = tmp_path / "GENERATED_SYNTHETIC_DATA.txt"
        assert marker.is_file()
        assert "SYNTHETIC" in marker.read_text()
