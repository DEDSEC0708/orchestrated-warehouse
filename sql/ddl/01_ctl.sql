-- ===========================================================================
-- 01_ctl.sql - the pipeline's memory.
--
-- Three tables answer three questions before any extract runs:
--   ctl.source_registry  what sources exist and how is each one loaded?
--   ctl.watermark        how far has each source been consumed?
--   ctl.ingested_file    which files have already been processed?
--
-- These are the smallest tables in the warehouse and the only ones whose loss
-- would change the pipeline's behaviour rather than merely its history.
-- ===========================================================================

-- ---------------------------------------------------------------------------
-- ctl.source_registry - declarative configuration of every source entity.
--
-- Exists so that the freshness data-quality rule and the maintenance DAG can
-- iterate over sources generically instead of hard-coding names. Adding a
-- source becomes a row, not a code change.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ctl.source_registry (
    source_system        VARCHAR(12)  NOT NULL,
    entity               VARCHAR(48)  NOT NULL,
    is_enabled           BOOLEAN      NOT NULL DEFAULT TRUE,
    load_strategy        VARCHAR(24)  NOT NULL,
    natural_key_columns  TEXT []      NOT NULL,
    watermark_column     TEXT         NULL,
    target_raw_table     TEXT         NOT NULL,
    sla_minutes          INT          NOT NULL DEFAULT 1440,
    owner                TEXT         NOT NULL DEFAULT 'data-eng',
    description          TEXT         NULL,
    updated_at_utc       TIMESTAMPTZ  NOT NULL DEFAULT now(),
    CONSTRAINT pk_source_registry PRIMARY KEY (source_system, entity),
    CONSTRAINT ck_source_registry_strategy CHECK (
        load_strategy IN ('incremental_timestamp', 'incremental_partition', 'incremental_cursor', 'full_snapshot')
    )
);

COMMENT ON TABLE ctl.source_registry IS
    'One row per (source_system, entity). Declares how the entity is loaded, its natural key and its freshness SLA.';
COMMENT ON COLUMN ctl.source_registry.load_strategy IS
    'incremental_timestamp (watermark on updated_at) | incremental_partition (dt= partitions + file registry) | incremental_cursor (API cursor) | full_snapshot (truncate and reload).';
COMMENT ON COLUMN ctl.source_registry.sla_minutes IS
    'Maximum acceptable age of this entity before the FRESHNESS data-quality rule reports it.';


-- ---------------------------------------------------------------------------
-- ctl.watermark - how far each source has been consumed.
--
-- Updated INSIDE the same transaction that commits the data it describes, at
-- the end, only on success. That single property is what makes a crash safe:
-- either both the rows and the watermark moved, or neither did.
--
-- `lookback_interval` is per source, not global, because it is a property of
-- how far back that specific source revises its own records. The partner API
-- revises for 7 days; a CMS row's updated_at is only ever a couple of hours
-- out due to clock skew.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ctl.watermark (
    source_system        VARCHAR(12)  NOT NULL,
    entity               VARCHAR(48)  NOT NULL,
    watermark_type       VARCHAR(16)  NOT NULL,
    watermark_ts_utc     TIMESTAMPTZ  NULL,
    watermark_str        TEXT         NULL,
    lookback_interval    INTERVAL     NOT NULL DEFAULT INTERVAL '0',
    last_success_run_id  UUID         NULL,
    last_success_at_utc  TIMESTAMPTZ  NULL,
    updated_at_utc       TIMESTAMPTZ  NOT NULL DEFAULT now(),
    CONSTRAINT pk_watermark PRIMARY KEY (source_system, entity),
    CONSTRAINT ck_watermark_type CHECK (
        watermark_type IN ('timestamp', 'date_partition', 'cursor', 'none')
    ),
    -- A timestamp/cursor watermark without a value is a bug that would silently
    -- extract everything; a date_partition watermark without a string is the
    -- same bug in the other shape. Catch it in the schema, not at 2 a.m.
    CONSTRAINT ck_watermark_value_present CHECK (
        (watermark_type = 'none')
        OR (watermark_type IN ('timestamp', 'cursor') AND watermark_ts_utc IS NOT NULL)
        OR (watermark_type = 'date_partition' AND watermark_str IS NOT NULL)
    )
);

COMMENT ON TABLE ctl.watermark IS
    'One row per (source_system, entity) recording how far that source has been consumed. Advanced with GREATEST so an out-of-order rerun can never move it backwards.';
COMMENT ON COLUMN ctl.watermark.lookback_interval IS
    'How far back each run re-reads to catch late-arriving or revised records. A property of the SOURCE, not a global constant.';
COMMENT ON COLUMN ctl.watermark.watermark_str IS
    'Opaque watermark for partition-oriented sources: the highest dt= partition consumed, as YYYY-MM-DD.';


-- ---------------------------------------------------------------------------
-- ctl.ingested_file - the entire file-level idempotency mechanism.
--
-- Keyed on (file_path, file_sha256), not on file_path alone. A file that is
-- REWRITTEN with corrections gets a new hash and is therefore reprocessed;
-- a file that is merely re-delivered unchanged is skipped. Both behaviours
-- fall out of the key choice rather than from branching logic.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ctl.ingested_file (
    file_path              TEXT         NOT NULL,
    file_sha256            CHAR(64)     NOT NULL,
    source_system          VARCHAR(12)  NOT NULL,
    dw_batch_key           DATE         NOT NULL,
    row_count              BIGINT       NOT NULL DEFAULT 0,
    bytes                  BIGINT       NOT NULL DEFAULT 0,
    first_ingested_run_id  UUID         NULL,
    first_ingested_at_utc  TIMESTAMPTZ  NOT NULL DEFAULT now(),
    status                 VARCHAR(20)  NOT NULL DEFAULT 'LOADED',
    CONSTRAINT pk_ingested_file PRIMARY KEY (file_path, file_sha256),
    CONSTRAINT ck_ingested_file_status CHECK (
        status IN ('LOADED', 'FAILED', 'SKIPPED_DUPLICATE')
    )
);

COMMENT ON TABLE ctl.ingested_file IS
    'Registry of processed landing files, keyed on (path, sha256). A re-delivered identical file is skipped; a rewritten file has a new hash and is reprocessed.';

CREATE INDEX IF NOT EXISTS ix_ingested_file_batch
    ON ctl.ingested_file (source_system, dw_batch_key);
