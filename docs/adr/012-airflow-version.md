# ADR-012: Pin Apache Airflow to 2.10.5, not 3.x

- **Status:** Accepted
- **Date:** 2026-08-13 (Phase 0)
- **Supersedes:** none

## Context

At the time of building, PyPI offers Apache Airflow 3.3.1 as the latest release,
2.11.2 as the newest 2.x, and 2.10.5 as the final 2.10.x patch. The approved
specification calls for "Airflow 2.10.x".

Three forces pull in different directions:

1. **Recency signals currency.** A portfolio built on a two-major-versions-old
   release can read as dated.
2. **Reproducibility beats recency for a project a stranger must run.** Airflow
   publishes an official constraints file per (version, Python) pair. Pinning
   2.10.5 + `constraints-3.11.txt` gives a dependency graph that is guaranteed
   to resolve today and in six months.
3. **Learning-material alignment.** Airflow 3 changed the execution model (Task
   SDK, execution API, assets replacing datasets, DAG versioning). Most
   community material a junior engineer will reach for when debugging is still
   2.x-shaped.

## Decision

Pin **Apache Airflow 2.10.5 on Python 3.11**, installed with the official
constraints file committed as `constraints.txt`.

The pin is guarded by a test (`tests/test_sanity.py::test_airflow_pin_is_2_10_5`)
so it cannot drift silently through a dependency bump.

## Consequences

**Positive**

- One `pip install -r requirements.txt -c constraints.txt` resolves deterministically.
- The container image tag `apache/airflow:2.10.5-python3.11` matches the local
  environment exactly, so "works locally, fails in Docker" is largely designed out.
- Datasets (used for cross-DAG dependencies in this project) are stable in 2.10.

**Negative**

- 2.10.x is on the end of its patch line; no further fixes will land there.
- Airflow 3 features are unavailable: DAG versioning, the new Task SDK, backfill
  as a first-class scheduler concept.
- An interviewer may ask why not 3.x. The answer is this document.

**Migration notes (recorded now, while the deltas are fresh)**

Moving to Airflow 3 later would require: switching `Dataset` to `Asset`,
adapting to the Task SDK import paths (`airflow.sdk`), reviewing the removal of
implicit `execution_date`, and re-generating the constraints pin. The pipeline
logic in `src/volthive` is deliberately Airflow-agnostic - DAG files only
orchestrate - so the blast radius of that migration is limited to `dags/`.

## Alternatives considered

| Alternative | Why rejected |
|---|---|
| **Airflow 3.3.1** | Newest, but changes the execution model and thins out the available debugging material; the specification's approval explicitly excludes it absent a concrete blocker. |
| **Airflow 2.11.2** | Newer 2.x with 3.x-migration helpers. Rejected because the specification approved 2.10.x, and 2.11 buys nothing this project uses. Revisit only if a 2.10.5 blocker appears. |
| **Unpinned `apache-airflow>=2.10`** | Non-reproducible. A transitive release can break the environment overnight - the single most common Airflow failure mode. |
