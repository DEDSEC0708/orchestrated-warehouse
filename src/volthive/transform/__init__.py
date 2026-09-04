"""Thin Python wrappers that execute the parameterised SQL in ``sql/``.

No business logic lives here. Every function in this package chooses a SQL
file, supplies parameters, and owns a transaction boundary - nothing more.

The reason is testability and reviewability. SQL in files is lintable by
sqlfluff, readable as a diff, runnable directly in ``psql`` when a load
misbehaves, and visible to anyone scrolling the repository. SQL hidden inside
Python string constants is none of those things, and the SQL is the part of
this project that a data engineer is actually being assessed on.
"""

from __future__ import annotations

from volthive.transform.core import (
    DIMENSION_STEPS,
    FACT_STEPS,
    MART_STEPS,
    CoreStep,
    run_core_step,
    run_dimensions,
    run_facts,
    run_mart,
)
from volthive.transform.staging import (
    STAGING_STEPS,
    StagingStep,
    run_staging,
    run_staging_step,
)

__all__ = [
    "DIMENSION_STEPS",
    "FACT_STEPS",
    "MART_STEPS",
    "STAGING_STEPS",
    "CoreStep",
    "StagingStep",
    "run_core_step",
    "run_dimensions",
    "run_facts",
    "run_mart",
    "run_staging",
    "run_staging_step",
]
