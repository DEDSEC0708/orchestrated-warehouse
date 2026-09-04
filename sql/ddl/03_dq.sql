-- ===========================================================================
-- 03_dq.sql - data quality: rules, results and quarantine.
--
-- Principle P3, enforced structurally: NEVER SILENTLY DROP A ROW. A record
-- that fails validation is written to a quarantine table with the rule that
-- rejected it, a human-readable detail, and its COMPLETE original payload -
-- so it can be triaged, fixed and replayed byte-for-byte.
--
-- A quarantine table nobody can act on is a landfill. These carry provenance
-- (source file + row sequence + run id) and a lifecycle status, which is what
-- turns "I have a quarantine table" into "I have a quarantine process".
-- ===========================================================================

-- ---------------------------------------------------------------------------
-- dq.rule - the rule registry.
--
-- Rules are AUTHORED in YAML under configs/dq/ (version-controlled, reviewable
-- in a diff) and SYNCED into this table at deploy time so they are also
-- queryable and joinable from check results and quarantine rows. YAML is the
-- source of truth; this table is the queryable projection of it.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dq.rule (
    rule_code       VARCHAR(48)  NOT NULL,
    entity          VARCHAR(64)  NOT NULL,
    layer           VARCHAR(12)  NOT NULL,
    rule_type       VARCHAR(24)  NOT NULL,
    scope           VARCHAR(12)  NOT NULL,
    severity        VARCHAR(8)   NOT NULL,
    rule_sql        TEXT         NULL,
    params          JSONB        NULL,
    description     TEXT         NOT NULL,
    is_enabled      BOOLEAN      NOT NULL DEFAULT TRUE,
    updated_at_utc  TIMESTAMPTZ  NOT NULL DEFAULT now(),
    CONSTRAINT pk_rule PRIMARY KEY (rule_code),
    CONSTRAINT ck_rule_layer CHECK (layer IN ('raw', 'stg', 'core', 'mart')),
    CONSTRAINT ck_rule_scope CHECK (scope IN ('row', 'dataset')),
    CONSTRAINT ck_rule_severity CHECK (severity IN ('error', 'warn')),
    CONSTRAINT ck_rule_type CHECK (
        rule_type IN (
            'not_null', 'unique', 'range', 'accepted_values', 'referential',
            'freshness', 'row_count_anomaly', 'reconciliation', 'schema',
            'ratio', 'cast', 'rule'
        )
    ),
    -- A dataset-scope rule with no SQL would silently never run and always
    -- "pass", which is the most dangerous possible failure for a quality gate.
    CONSTRAINT ck_rule_dataset_needs_sql CHECK (
        scope <> 'dataset' OR rule_sql IS NOT NULL
    )
);

COMMENT ON TABLE dq.rule IS
    'Registry of data-quality rules, synced from configs/dq/*.yml. severity=error blocks the mart publish gate; severity=warn is recorded and reported only.';
COMMENT ON COLUMN dq.rule.scope IS
    'row = evaluated per record during staging, failures go to quarantine. dataset = evaluated over a whole table/window, failures go to dq.check_result.';


-- ---------------------------------------------------------------------------
-- dq.check_result - one row per rule per run. Append-only: the history of
-- checks IS data, and overwriting it would destroy the quality trend.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dq.check_result (
    check_result_id  BIGINT        GENERATED ALWAYS AS IDENTITY,
    dw_run_id        UUID          NOT NULL,
    rule_code        VARCHAR(48)   NOT NULL,
    entity           VARCHAR(64)   NOT NULL,
    layer            VARCHAR(12)   NOT NULL,
    dw_batch_key     DATE          NOT NULL,
    rows_evaluated   BIGINT        NOT NULL DEFAULT 0,
    rows_failed      BIGINT        NOT NULL DEFAULT 0,
    fail_ratio       NUMERIC(9, 6)  NOT NULL DEFAULT 0,
    threshold_value  NUMERIC(18, 6) NULL,
    observed_value   NUMERIC(18, 6) NULL,
    status           VARCHAR(8)    NOT NULL,
    severity         VARCHAR(8)    NOT NULL,
    message          TEXT          NULL,
    checked_at_utc   TIMESTAMPTZ   NOT NULL DEFAULT now(),
    duration_ms      INT           NULL,
    CONSTRAINT pk_check_result PRIMARY KEY (check_result_id),
    CONSTRAINT fk_check_result_rule FOREIGN KEY (rule_code) REFERENCES dq.rule (rule_code),
    CONSTRAINT ck_check_result_status CHECK (status IN ('PASS', 'WARN', 'FAIL', 'ERROR')),
    CONSTRAINT ck_check_result_severity CHECK (severity IN ('error', 'warn'))
);

COMMENT ON TABLE dq.check_result IS
    'One row per dataset rule per run. Append-only. The publish gate reads this table for status=FAIL on error-severity rules.';
COMMENT ON COLUMN dq.check_result.status IS
    'PASS | WARN (rule failed but severity=warn) | FAIL (rule failed and severity=error, blocks the gate) | ERROR (the rule itself could not be executed).';

CREATE INDEX IF NOT EXISTS ix_check_result_run ON dq.check_result (dw_run_id);
CREATE INDEX IF NOT EXISTS ix_check_result_rule_time
    ON dq.check_result (rule_code, checked_at_utc DESC);
CREATE INDEX IF NOT EXISTS ix_check_result_not_pass
    ON dq.check_result (checked_at_utc DESC)
    WHERE status <> 'PASS';


-- ---------------------------------------------------------------------------
-- Quarantine tables.
--
-- One per source shape rather than a single polymorphic table: each has its
-- own retention, its own natural key semantics, and its own triage owner, and
-- a single table would need a discriminator column on every query. The column
-- set is deliberately identical so the writer and the requeue script can treat
-- them uniformly.
--
-- raw_payload is JSONB and holds the COMPLETE original record. That is what
-- makes requeue possible: the row can be re-injected exactly as it arrived,
-- with no reconstruction and no loss.
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS dq.quarantine_ocpp_cdr (
    quarantine_id       BIGINT       GENERATED ALWAYS AS IDENTITY,
    dw_run_id           UUID         NOT NULL,
    dw_batch_key        DATE         NOT NULL,
    source_system       VARCHAR(12)  NOT NULL,
    source_file         TEXT         NULL,
    source_row_seq      INT          NULL,
    natural_key         VARCHAR(64)  NULL,
    rule_code           VARCHAR(48)  NOT NULL,
    rule_detail         TEXT         NULL,
    raw_payload         JSONB        NOT NULL,
    quarantined_at_utc  TIMESTAMPTZ  NOT NULL DEFAULT now(),
    status              VARCHAR(16)  NOT NULL DEFAULT 'NEW',
    requeued_run_id     UUID         NULL,
    reviewed_note       TEXT         NULL,
    CONSTRAINT pk_quarantine_ocpp_cdr PRIMARY KEY (quarantine_id),
    CONSTRAINT fk_quarantine_ocpp_cdr_rule FOREIGN KEY (rule_code) REFERENCES dq.rule (rule_code),
    CONSTRAINT ck_quarantine_ocpp_cdr_status CHECK (
        status IN ('NEW', 'TRIAGED', 'REQUEUED', 'WONTFIX')
    )
);

CREATE TABLE IF NOT EXISTS dq.quarantine_meter_value (
    quarantine_id       BIGINT       GENERATED ALWAYS AS IDENTITY,
    dw_run_id           UUID         NOT NULL,
    dw_batch_key        DATE         NOT NULL,
    source_system       VARCHAR(12)  NOT NULL,
    source_file         TEXT         NULL,
    source_row_seq      INT          NULL,
    natural_key         VARCHAR(64)  NULL,
    rule_code           VARCHAR(48)  NOT NULL,
    rule_detail         TEXT         NULL,
    raw_payload         JSONB        NOT NULL,
    quarantined_at_utc  TIMESTAMPTZ  NOT NULL DEFAULT now(),
    status              VARCHAR(16)  NOT NULL DEFAULT 'NEW',
    requeued_run_id     UUID         NULL,
    reviewed_note       TEXT         NULL,
    CONSTRAINT pk_quarantine_meter_value PRIMARY KEY (quarantine_id),
    CONSTRAINT fk_quarantine_meter_value_rule FOREIGN KEY (rule_code) REFERENCES dq.rule (rule_code),
    CONSTRAINT ck_quarantine_meter_value_status CHECK (
        status IN ('NEW', 'TRIAGED', 'REQUEUED', 'WONTFIX')
    )
);

CREATE TABLE IF NOT EXISTS dq.quarantine_partner_cdr (
    quarantine_id       BIGINT       GENERATED ALWAYS AS IDENTITY,
    dw_run_id           UUID         NOT NULL,
    dw_batch_key        DATE         NOT NULL,
    source_system       VARCHAR(12)  NOT NULL,
    source_file         TEXT         NULL,
    source_row_seq      INT          NULL,
    natural_key         VARCHAR(64)  NULL,
    rule_code           VARCHAR(48)  NOT NULL,
    rule_detail         TEXT         NULL,
    raw_payload         JSONB        NOT NULL,
    quarantined_at_utc  TIMESTAMPTZ  NOT NULL DEFAULT now(),
    status              VARCHAR(16)  NOT NULL DEFAULT 'NEW',
    requeued_run_id     UUID         NULL,
    reviewed_note       TEXT         NULL,
    CONSTRAINT pk_quarantine_partner_cdr PRIMARY KEY (quarantine_id),
    CONSTRAINT fk_quarantine_partner_cdr_rule FOREIGN KEY (rule_code) REFERENCES dq.rule (rule_code),
    CONSTRAINT ck_quarantine_partner_cdr_status CHECK (
        status IN ('NEW', 'TRIAGED', 'REQUEUED', 'WONTFIX')
    )
);

CREATE TABLE IF NOT EXISTS dq.quarantine_cms_entity (
    quarantine_id       BIGINT       GENERATED ALWAYS AS IDENTITY,
    dw_run_id           UUID         NOT NULL,
    dw_batch_key        DATE         NOT NULL,
    source_system       VARCHAR(12)  NOT NULL,
    entity              VARCHAR(48)  NOT NULL,
    source_file         TEXT         NULL,
    source_row_seq      INT          NULL,
    natural_key         VARCHAR(64)  NULL,
    rule_code           VARCHAR(48)  NOT NULL,
    rule_detail         TEXT         NULL,
    raw_payload         JSONB        NOT NULL,
    quarantined_at_utc  TIMESTAMPTZ  NOT NULL DEFAULT now(),
    status              VARCHAR(16)  NOT NULL DEFAULT 'NEW',
    requeued_run_id     UUID         NULL,
    reviewed_note       TEXT         NULL,
    CONSTRAINT pk_quarantine_cms_entity PRIMARY KEY (quarantine_id),
    CONSTRAINT fk_quarantine_cms_entity_rule FOREIGN KEY (rule_code) REFERENCES dq.rule (rule_code),
    CONSTRAINT ck_quarantine_cms_entity_status CHECK (
        status IN ('NEW', 'TRIAGED', 'REQUEUED', 'WONTFIX')
    )
);

COMMENT ON TABLE dq.quarantine_ocpp_cdr IS
    'Charge detail records rejected by a row-level rule. raw_payload holds the complete original line so the record can be replayed byte-for-byte.';
COMMENT ON TABLE dq.quarantine_meter_value IS
    'Meter samples/intervals rejected by a row-level rule, including samples still orphaned after the lookback window expired.';
COMMENT ON TABLE dq.quarantine_partner_cdr IS
    'Roaming partner records rejected by a row-level rule.';
COMMENT ON TABLE dq.quarantine_cms_entity IS
    'Master-data rows rejected by a row-level rule. The entity column names which CMS entity, since all five share one table.';

CREATE INDEX IF NOT EXISTS ix_q_ocpp_new ON dq.quarantine_ocpp_cdr (quarantined_at_utc) WHERE status = 'NEW';
CREATE INDEX IF NOT EXISTS ix_q_ocpp_rule ON dq.quarantine_ocpp_cdr (rule_code, dw_batch_key);
CREATE INDEX IF NOT EXISTS ix_q_ocpp_key ON dq.quarantine_ocpp_cdr (natural_key);
CREATE INDEX IF NOT EXISTS ix_q_meter_new ON dq.quarantine_meter_value (quarantined_at_utc) WHERE status = 'NEW';
CREATE INDEX IF NOT EXISTS ix_q_meter_rule ON dq.quarantine_meter_value (rule_code, dw_batch_key);
CREATE INDEX IF NOT EXISTS ix_q_partner_new ON dq.quarantine_partner_cdr (quarantined_at_utc) WHERE status = 'NEW';
CREATE INDEX IF NOT EXISTS ix_q_partner_rule ON dq.quarantine_partner_cdr (rule_code, dw_batch_key);
CREATE INDEX IF NOT EXISTS ix_q_cms_new ON dq.quarantine_cms_entity (quarantined_at_utc) WHERE status = 'NEW';
CREATE INDEX IF NOT EXISTS ix_q_cms_rule ON dq.quarantine_cms_entity (rule_code, entity, dw_batch_key);
