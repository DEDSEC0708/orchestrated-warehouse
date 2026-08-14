# ADR-013: One constraints file governs the entire toolchain

- **Status:** Accepted
- **Date:** 2026-08-13 (Phase 0)

## Context

The official Airflow constraints file pins ~760 packages, and that set includes
development tooling: `ruff==0.5.5`, `mypy==1.9.0`, `pytest==8.3.4`,
`pytest-cov==6.0.0`, `pytest-mock==3.14.0`, `httpx==0.27.0`,
`python-dotenv==1.0.1`.

Current releases of some of those tools are substantially newer (ruff 0.16.x,
mypy 2.x). Two workable strategies exist:

1. **Two environments.** Runtime installed with constraints; dev tooling
   installed without them, free to use the newest linters.
2. **One environment.** Everything installed with the same constraints file,
   accepting older linters.

Strategy 1 means `pip install -r requirements-dev.txt -c constraints.txt` fails
with a conflict (`ruff==0.16.3` vs the constrained `ruff==0.5.5`), so the
project would need two documented install commands and a rule about which one
applies where. That is a real source of "it works on my machine".

## Decision

**One constraints-governed environment.** Both `requirements.txt` and
`requirements-dev.txt` are installed with `-c constraints.txt`.

Packages already pinned by the constraints file are listed **unpinned** in the
requirements files, with a comment naming the effective version. Re-pinning them
would create a second source of truth that can silently disagree.

Packages Airflow does not depend on are pinned explicitly and **era-matched** to
the Airflow 2.10.5 release window (early 2025), so they were plausibly tested
against the same dependency generation:

| Package | Pin | Role |
|---|---|---|
| `psycopg[binary]` | 3.2.13 | Pipeline DB access: `COPY` bulk load, server-side cursors |
| `pydantic-settings` | 2.8.1 | Typed configuration from env + YAML |
| `structlog` | 25.4.0 | Structured JSON logging |
| `PyYAML` | 6.0.2 | DQ rules and source config (matches the constrained version) |
| `tenacity` | 9.0.0 | Retry policy for the partner API client |
| `fastapi` / `uvicorn` | 0.115.14 / 0.34.3 | Local mock partner API (source S4) |
| `sqlfluff` | 3.4.2 | SQL linting |
| `freezegun` / `respx` | 1.5.3 / 0.22.0 | Frozen clock and HTTP mocking in tests |

`pre-commit` is *also* constraint-governed (4.1.0) even though a plain
`grep "pre-commit=="` over the constraints file finds nothing: pip normalises
package names, and the file lists it as `pre_commit==4.1.0`. Pinning it
independently produced a hard `ResolutionImpossible` during Phase 0 validation.
The lesson is recorded here because it generalises - **check the constraints
file for both the hyphen and underscore spelling before pinning anything.**

## Consequences

**Positive**

- A single `pip install` command per requirements file, and it cannot conflict.
- The lint rules enforced locally, in pre-commit and in CI are identical by
  construction, because there is only one ruff version in play.
- Reproducibility is the project's headline claim; this makes it structurally
  true rather than aspirational.

**Negative**

- Linting runs on ruff 0.5.5, so newer rules and formatter refinements are
  unavailable. For a codebase this size that is cosmetic.
- Upgrading Airflow later also moves the dev toolchain. Acceptable, and arguably
  correct - they should move together.

**Note on two Postgres drivers.** `apache-airflow-providers-postgres` brings
`psycopg2-binary` for Airflow's own hooks, while pipeline code in
`src/volthive` uses `psycopg` 3 for its far better `COPY` API and server-side
cursor support. Both are installed deliberately. Forcing Airflow onto psycopg 3
is unsupported; forcing our code onto psycopg 2 would mean a worse bulk-load
path in the hottest part of the pipeline.

## Alternatives considered

| Alternative | Why rejected |
|---|---|
| **Split runtime/dev environments** | Two install commands, two truths, and a conflict for anyone who combines them. |
| **Poetry / uv / PDM lockfile** | A lockfile would be excellent, but Airflow's supported and documented install path is `pip` + constraints. Fighting that is how Airflow environments break. |
| **No pins at all** | Non-reproducible; guarantees eventual breakage. |
