# ADR-006: LocalExecutor and a four-container topology

- **Status:** Accepted
- **Date:** 2026-08-14 (Phase 2)
- **Related:** ADR-012 (Airflow 2.10.5)

## Context

Airflow can be deployed many ways. The reference `docker-compose.yaml` that
Airflow publishes runs **six** services: postgres, redis, webserver, scheduler,
worker and triggerer, using CeleryExecutor. Copying it would be the path of
least resistance and would look impressive in a repository listing.

This project's constraint is different: a stranger must be able to clone it and
run it on a laptop, and the author must be able to explain every component in an
interview. A container nobody can justify is a liability in both directions.

The workload is known: four DAGs, roughly a dozen tasks per run, a daily
schedule, the heaviest task being a bulk load of ~21,000 rows. Peak useful
parallelism is bounded by a single PostgreSQL container's I/O, not by CPU.

## Decision

**LocalExecutor, four containers: `postgres`, `airflow-init`,
`airflow-scheduler`, `airflow-webserver`.**

Components deliberately excluded:

| Excluded | Why |
|---|---|
| **Redis** | Only needed as CeleryExecutor's broker. With LocalExecutor there is no broker: the scheduler runs tasks as its own subprocesses. |
| **Celery worker** | Same. Horizontal worker scaling solves a problem this workload does not have. |
| **Triggerer** | Required only for deferrable operators. This project uses one short file sensor in `reschedule` mode, which frees its worker slot between pokes without needing async deferral. If deferrable operators are ever introduced, the triggerer must be added - noted here so the dependency is not forgotten. |
| **pgAdmin / Adminer** | `make psql` and the Airflow UI cover every need. A database GUI is a personal preference, not infrastructure. |
| **Metabase / Grafana / Prometheus** | Would double the container count and RAM to visualise a handful of metrics that a SQL view already answers. Observability here is structured logs plus the `audit` tables. |
| **A separate DAG processor** | Standalone DAG processing matters at hundreds of DAGs. At four it is a process to supervise for no benefit. |

**Why `airflow-init` is a service rather than a startup step.** Two long-running
services must not race each other running `airflow db migrate` against the same
metadata database. A dedicated one-shot container that must exit 0, with both
services gated on `condition: service_completed_successfully`, makes the
ordering a fact enforced by Docker. It also turns a migration failure into one
obviously-failed container instead of two crash-looping services.

## Consequences

**Positive**

- Four containers, roughly 2 GB of RAM, and a single build.
- Task execution and scheduling share a process tree, so debugging is one
  `docker compose logs airflow-scheduler` rather than a hunt across workers.
- Every service in `docker-compose.yml` can be justified in one sentence.

**Negative**

- No horizontal scaling: task throughput is bounded by the scheduler container.
  Correct at this scale, and the migration path is well-trodden - swap the
  executor, add Redis and a worker.
- A scheduler restart interrupts running tasks. Acceptable for a batch platform
  whose loads are idempotent and safely re-runnable by design.
- The scheduler container carries both scheduling and execution load, so a
  runaway task can starve scheduling. Mitigated by `execution_timeout` on every
  task (Phase 9/11) and by the `warehouse_pool` limit of 2.

**The threshold for revisiting this**: more than ~50 DAGs, tasks that need
different runtime images, or a requirement for execution isolation between
teams. None applies here.

## Alternatives considered

| Alternative | Why rejected |
|---|---|
| **CeleryExecutor + Redis + worker** | Six containers for a workload one container handles. The extra machinery would be decoration, and an interviewer asking "why do you need a message broker?" would get no good answer. |
| **SequentialExecutor** | The default with SQLite; runs exactly one task at a time and cannot demonstrate parallel task groups or a realistic DAG shape. |
| **KubernetesExecutor** | Explicitly out of scope, and would break the "runs on a laptop" requirement. |
| **`airflow standalone`** | One command, but it bundles SQLite and a dev server, hides the components, and teaches nothing about how the pieces fit. |

## Addendum: one image, one builder

The three Airflow services share one image, `volthive/airflow:2.10.5-local`,
built from `docker/airflow/Dockerfile`. They differ only in their command,
which is what the `x-airflow-common` anchor exists to keep true.

**Only `airflow-init` declares the `build:`.** The anchor carries the image
name; it deliberately does not carry the build.

Putting the build on the anchor is the obvious expression of "these are the
same image", and it is what the upstream Airflow compose file does. It is also
a race. Compose delegates builds to buildx bake, which turns every service
with a `build:` section into an independent target and runs them concurrently.
Three targets exporting the same image name collide in the image store:

```
target airflow-init: failed to solve:
image "docker.io/volthive/airflow:2.10.5-local": already exists
```

One service reports `CANCELED`, the others `ERROR`, and which one wins varies
between runs. The pre-bake builder deduplicated identical build definitions,
which is why this shape worked for years and then began failing.

`airflow-init` is the right owner rather than an arbitrary one: the dependency
graph already requires it to finish before either long-running service starts
(`condition: service_completed_successfully`), so the image cannot be missing
when the consumers are created. The build order is asserted once, in the place
that already enforced the run order.

Two consequences follow, both encoded as tests in
`tests/unit/test_compose_config.py`:

- The consumers set `pull_policy: never`. `volthive/airflow` is not a
  repository this project publishes, so a missing image must fail locally
  rather than send Docker to Docker Hub — where it would either fail
  confusingly or succeed against a stranger's image occupying that name.
- `make up` builds as a separate step rather than passing `--build`, so the
  image exists regardless of whether `--build` reaches a service that is
  present only as a dependency.
