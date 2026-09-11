-- ===========================================================================
-- 02_audit.sql - the pipeline's own history.
--
-- Airflow's logs answer "did it run?". These tables answer the question you
-- are actually asked at 9 a.m. when a dashboard looks wrong: how many rows,
-- from where, how long, and is that normal compared with last week?
--
-- audit.pipeline_run.pipeline_run_id is the correlation ID. It is generated
-- once per DAG run, bound into the structured logging context, and stamped on
-- EVERY warehouse row as dw_run_id. From a single fact row you can reach the
-- run, the task, the source file and the git commit that produced it.
-- ===========================================================================

-- ---------------------------------------------------------------------------
-- audit.pipeline_run - one row per DAG run.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS audit.pipeline_run (
    pipeline_run_id           UUID         NOT NULL,
    dag_id                    TEXT         NOT NULL,
    airflow_run_id            TEXT         NOT NULL,
    data_interval_start_utc   TIMESTAMPTZ  NOT NULL,
    data_interval_end_utc     TIMESTAMPTZ  NOT NULL,
    triggered_by              VARCHAR(16)  NOT NULL DEFAULT 'schedule',
    started_at_utc            TIMESTAMPTZ  NOT NULL DEFAULT now(),
    ended_at_utc              TIMESTAMPTZ  NULL,
    status                    VARCHAR(12)  NOT NULL DEFAULT 'RUNNING',
    rows_ingested             BIGINT       NOT NULL DEFAULT 0,
    rows_quarantined          BIGINT       NOT NULL DEFAULT 0,
    rows_loaded_core          BIGINT       NOT NULL DEFAULT 0,
    error_summary             TEXT         NULL,
    git_sha                   VARCHAR(40)  NULL,
    CONSTRAINT pk_pipeline_run PRIMARY KEY (pipeline_run_id),
    CONSTRAINT ck_pipeline_run_status CHECK (
        status IN ('RUNNING', 'SUCCESS', 'FAILED', 'SKIPPED')
    ),
    CONSTRAINT ck_pipeline_run_triggered_by CHECK (
        triggered_by IN ('schedule', 'manual', 'backfill', 'dataset', 'test')
    ),
    -- >= not >, because a DATASET-scheduled DAG has no interval to speak of.
    -- Airflow gives such runs a point in time: data_interval_start equals
    -- data_interval_end. Requiring a strictly positive interval encoded an
    -- assumption that only holds for cron-scheduled DAGs, and it rejected
    -- every run of volthive_build_warehouse with
    --   CheckViolation: ... violates check constraint "ck_pipeline_run_interval"
    -- An inverted interval is still refused, which is the part that was ever
    -- protecting anything.
    CONSTRAINT ck_pipeline_run_interval CHECK (data_interval_end_utc >= data_interval_start_utc)
);

-- Existing warehouses: CREATE TABLE IF NOT EXISTS above is a no-op once the
-- table exists, so the relaxed constraint has to be applied explicitly. DROP
-- IF EXISTS followed by ADD is idempotent as a pair, and `make db-init` is
-- already the documented way to bring a database back in line with the repo.
ALTER TABLE audit.pipeline_run DROP CONSTRAINT IF EXISTS ck_pipeline_run_interval;
ALTER TABLE audit.pipeline_run ADD CONSTRAINT ck_pipeline_run_interval
    CHECK (data_interval_end_utc >= data_interval_start_utc);

COMMENT ON TABLE audit.pipeline_run IS
    'One row per DAG run. pipeline_run_id is the correlation ID stamped on every warehouse row as dw_run_id.';
COMMENT ON COLUMN audit.pipeline_run.git_sha IS
    'The commit of the code that produced this data. Answers "which version of the logic wrote these rows?" without guessing.';

CREATE INDEX IF NOT EXISTS ix_pipeline_run_dag_started
    ON audit.pipeline_run (dag_id, started_at_utc DESC);
-- Partial: the maintenance DAG sweeps runs left RUNNING by a hard crash, and
-- that sweep should never scan the full history to find a handful of rows.
CREATE INDEX IF NOT EXISTS ix_pipeline_run_open
    ON audit.pipeline_run (started_at_utc)
    WHERE status = 'RUNNING';


-- ---------------------------------------------------------------------------
-- audit.task_run - one row per task ATTEMPT, not per task.
--
-- try_number is part of the story: "it passed on attempt 3" is a reliability
-- signal that disappears if you only record the final outcome.
--
-- The foreign key is nullable and NOT enforced with ON DELETE, because a
-- failure callback can fire before open_pipeline_run has committed (for
-- example if that very task failed). An audit table that refuses to record a
-- failure is worse than useless.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS audit.task_run (
    task_run_id       BIGINT       GENERATED ALWAYS AS IDENTITY,
    pipeline_run_id   UUID         NULL,
    dag_id            TEXT         NOT NULL,
    task_id           TEXT         NOT NULL,
    airflow_run_id    TEXT         NULL,
    try_number        INT          NOT NULL DEFAULT 1,
    started_at_utc    TIMESTAMPTZ  NULL,
    ended_at_utc      TIMESTAMPTZ  NOT NULL DEFAULT now(),
    status            VARCHAR(12)  NOT NULL,
    duration_ms       INT          NULL,
    error_type        TEXT         NULL,
    error_message     TEXT         NULL,
    CONSTRAINT pk_task_run PRIMARY KEY (task_run_id),
    CONSTRAINT ck_task_run_status CHECK (
        status IN ('SUCCESS', 'FAILED', 'RETRY', 'SKIPPED', 'UPSTREAM_FAILED')
    )
);

COMMENT ON TABLE audit.task_run IS
    'One row per task attempt. Written by the Airflow on_failure_callback and on_retry_callback so retries are visible rather than hidden.';

CREATE INDEX IF NOT EXISTS ix_task_run_pipeline
    ON audit.task_run (pipeline_run_id);
CREATE INDEX IF NOT EXISTS ix_task_run_failures
    ON audit.task_run (dag_id, ended_at_utc DESC)
    WHERE status IN ('FAILED', 'RETRY');


-- ---------------------------------------------------------------------------
-- audit.load_stat - one row per (task, target table, batch).
--
-- THIS is the table that answers "is today's volume normal?". The row-count
-- anomaly rule reads its trailing history, partitioned by weekday, and
-- compares against a rolling median.
--
-- rows_read is deliberately distinct from rows_inserted: the difference is
-- exactly the rows that were deduplicated or quarantined, which is what makes
-- the reconciliation identity checkable.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS audit.load_stat (
    load_stat_id            BIGINT       GENERATED ALWAYS AS IDENTITY,
    pipeline_run_id         UUID         NULL,
    dag_id                  TEXT         NULL,
    task_id                 TEXT         NOT NULL,
    source_system           VARCHAR(12)  NULL,
    entity                  VARCHAR(48)  NULL,
    target_table            TEXT         NOT NULL,
    dw_batch_key            DATE         NOT NULL,
    rows_read               BIGINT       NOT NULL DEFAULT 0,
    rows_inserted           BIGINT       NOT NULL DEFAULT 0,
    rows_updated            BIGINT       NOT NULL DEFAULT 0,
    rows_deleted            BIGINT       NOT NULL DEFAULT 0,
    rows_quarantined        BIGINT       NOT NULL DEFAULT 0,
    rows_duplicate_skipped  BIGINT       NOT NULL DEFAULT 0,
    files_seen              INT          NOT NULL DEFAULT 0,
    files_loaded            INT          NOT NULL DEFAULT 0,
    files_skipped           INT          NOT NULL DEFAULT 0,
    bytes_read              BIGINT       NOT NULL DEFAULT 0,
    duration_ms             INT          NULL,
    created_at_utc          TIMESTAMPTZ  NOT NULL DEFAULT now(),
    CONSTRAINT pk_load_stat PRIMARY KEY (load_stat_id)
);

COMMENT ON TABLE audit.load_stat IS
    'Per-load row statistics. Feeds the row-count anomaly rule and the reconciliation identity raw = staged + quarantined + duplicates.';

CREATE INDEX IF NOT EXISTS ix_load_stat_target_batch
    ON audit.load_stat (target_table, dw_batch_key DESC);
CREATE INDEX IF NOT EXISTS ix_load_stat_pipeline
    ON audit.load_stat (pipeline_run_id);
