"""The seeded synthetic source-data simulator.

Produces four heterogeneous sources - an operational database, JSONL event
files, gzipped CSV telemetry and a roaming-partner dataset - from a fixed seed,
with a controlled and *labelled* rate of realistic defects.

Three properties make this worth having rather than downloading a dataset:

**Determinism.** Same seed, byte-identical output. Two people running this
project get identical warehouses, so a test can assert an exact number instead
of a tolerance.

**Labelled defects.** Every injected defect is counted as it is injected, and
the totals are written to ``data/_truth/expected_defects.json``. Tests assert
quarantine counts against that manifest - an oracle computed independently of
the pipeline - rather than against the pipeline's own output.

**Genuine change history.** Tariffs are revised, chargers are upgraded from
30 kW to 60 kW, customers relocate and change segment. Without changes there is
nothing for SCD Type 2 to track, and the whole point-in-time story would be
untestable.

A note on honesty: this is synthetic data and the README says so in its first
section. Real messy data would be more impressive if it existed - no public EV
charging dataset with genuine change history does, which is exactly why this
generator exists.
"""

from __future__ import annotations

from volthive.generator.config import GeneratorConfig, load_generator_config
from volthive.generator.defects import DefectLedger
from volthive.generator.entities import MasterData, generate_master_data
from volthive.generator.run import GenerationResult, generate_all
from volthive.generator.sessions import build_universe, generate_day

__all__ = [
    "DefectLedger",
    "GenerationResult",
    "GeneratorConfig",
    "MasterData",
    "build_universe",
    "generate_all",
    "generate_day",
    "generate_master_data",
    "load_generator_config",
]
