-- ===========================================================================
-- 00_schemas.sql - the seven schemas of the VoltHive warehouse.
--
-- Each schema has exactly one job. The separation is what makes it possible to
-- say "this data is trustworthy" about `stg` while `raw` deliberately contains
-- values that break their own declared type.
--
--   raw    landed as received. Immutable, append-only, TEXT/JSONB.
--   stg    typed, conformed, deduplicated, validated. Rebuilt per window.
--   core   the Kimball star schema. Full history.
--   mart   consumer-facing aggregates and semantic views.
--   dq     rule registry, check results, quarantine. Append-only.
--   audit  what ran, when, how many rows, how long. Append-only.
--   ctl    operational state the pipeline READS TO DECIDE WHAT TO DO NEXT.
--
-- `ctl` and `audit` are separate on purpose. `ctl` is small, mutable, and
-- losing it changes behaviour; `audit` is a historical record and is safe to
-- prune. Conflating them means truncating your history table also resets your
-- pipeline's memory.
--
-- Every DDL file in this directory is re-runnable. `make db-init` applies them
-- in filename order and must be a no-op on the second run.
-- ===========================================================================

CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS stg;
CREATE SCHEMA IF NOT EXISTS core;
CREATE SCHEMA IF NOT EXISTS mart;
CREATE SCHEMA IF NOT EXISTS dq;
CREATE SCHEMA IF NOT EXISTS audit;
CREATE SCHEMA IF NOT EXISTS ctl;

COMMENT ON SCHEMA raw IS
    'Landed source data, source-shaped and untyped. Immutable, append-only. The replay tape: any downstream layer can be rebuilt from here without re-reading a source system.';
COMMENT ON SCHEMA stg IS
    'Typed, conformed, deduplicated and row-level validated data for the current restatement window. Rebuilt, never appended.';
COMMENT ON SCHEMA core IS
    'The dimensional model: SCD2 dimensions and fact tables at explicitly declared grains.';
COMMENT ON SCHEMA mart IS
    'The analytics contract: aggregate tables and semantic views that hide surrogate keys and SCD2 mechanics.';
COMMENT ON SCHEMA dq IS
    'Data quality: the rule registry, per-run check results, and quarantine tables holding rejected rows with their original payloads.';
COMMENT ON SCHEMA audit IS
    'The historical record of pipeline execution: runs, task attempts and per-load row statistics.';
COMMENT ON SCHEMA ctl IS
    'Mutable control state: watermarks, the processed-file registry and the source registry. Small, permanent, and load-bearing.';
