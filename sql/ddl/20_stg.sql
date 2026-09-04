-- ===========================================================================
-- 20_stg.sql - staging: where data becomes trustworthy.
--
-- Responsibilities, in order: cast to real types (routing cast failures to
-- quarantine rather than exploding the statement), normalise units and casing,
-- deduplicate to the declared natural key, apply row-level quality rules, and
-- compute derived columns needed by more than one consumer.
--
-- REBUILT, NEVER APPENDED. Every staging load deletes the restatement window
-- and re-inserts it. That is the whole of staging's idempotency story, and it
-- is why the natural key can be a real PRIMARY KEY here but not in raw.
--
-- UNLOGGED, deliberately. Staging is derived from raw on every run, so WAL
-- durability buys nothing and costs write throughput. The trade-off is real
-- and stated: an unlogged table is TRUNCATED on crash recovery. That is
-- acceptable precisely because these tables are rebuildable in seconds - and
-- it would be entirely unacceptable for `core`, which is why `core` is logged.
-- ===========================================================================

-- === Master data ===========================================================

CREATE UNLOGGED TABLE IF NOT EXISTS stg.customer (
    customer_id            VARCHAR(20)  NOT NULL,
    full_name_masked       VARCHAR(120) NULL,
    email_domain           VARCHAR(80)  NULL,
    city                   VARCHAR(60)  NULL,
    state                  VARCHAR(60)  NULL,
    city_tier              VARCHAR(8)   NULL,
    customer_segment       VARCHAR(20)  NOT NULL,
    subscription_plan      VARCHAR(20)  NOT NULL,
    kyc_status             VARCHAR(20)  NOT NULL,
    signup_date            DATE         NULL,
    is_active              BOOLEAN      NOT NULL,
    is_deleted             BOOLEAN      NOT NULL DEFAULT FALSE,
    source_created_at_utc  TIMESTAMPTZ  NULL,
    source_updated_at_utc  TIMESTAMPTZ  NOT NULL,
    dw_run_id              UUID         NOT NULL,
    dw_batch_key           DATE         NOT NULL,
    dw_source_row_seq      INT          NULL,
    dw_inserted_at_utc     TIMESTAMPTZ  NOT NULL DEFAULT now(),
    CONSTRAINT pk_stg_customer PRIMARY KEY (customer_id, source_updated_at_utc)
);

COMMENT ON TABLE stg.customer IS
    'GRAIN: one row per customer_id per source change time. Staging for an SCD2 dimension keeps EVERY version in the window, not just the latest - collapsing to current state here would destroy exactly the history the dimension exists to record.';
COMMENT ON COLUMN stg.customer.full_name_masked IS
    'PII minimisation happens HERE, at the boundary between evidence and warehouse: raw retains what arrived (and is pruned by retention), the warehouse never stores a full name.';
COMMENT ON COLUMN stg.customer.email_domain IS
    'Only the domain is warehoused. Enough for segmentation analysis, useless for contacting anyone.';

-- Type 1: only the latest version per key is staged, because there is no
-- history to preserve. The asymmetry with the tables above is deliberate and
-- is the schema-level expression of the SCD policy.
CREATE UNLOGGED TABLE IF NOT EXISTS stg.vehicle (
    vehicle_id             VARCHAR(20)  NOT NULL,
    customer_id            VARCHAR(20)  NULL,
    make                   VARCHAR(40)  NULL,
    model                  VARCHAR(60)  NULL,
    model_year             INT          NULL,
    battery_capacity_kwh   NUMERIC(6, 2) NULL,
    connector_type         VARCHAR(16)  NULL,
    registration_state     VARCHAR(60)  NULL,
    vehicle_class          VARCHAR(16)  NULL,
    source_created_at_utc  TIMESTAMPTZ  NULL,
    source_updated_at_utc  TIMESTAMPTZ  NOT NULL,
    dw_run_id              UUID         NOT NULL,
    dw_batch_key           DATE         NOT NULL,
    dw_source_row_seq      INT          NULL,
    dw_inserted_at_utc     TIMESTAMPTZ  NOT NULL DEFAULT now(),
    CONSTRAINT pk_stg_vehicle PRIMARY KEY (vehicle_id)
);

CREATE UNLOGGED TABLE IF NOT EXISTS stg.station (
    station_id             VARCHAR(24)  NOT NULL,
    station_name           VARCHAR(120) NULL,
    address_line           VARCHAR(200) NULL,
    city                   VARCHAR(60)  NULL,
    state                  VARCHAR(60)  NULL,
    pincode                VARCHAR(10)  NULL,
    latitude               NUMERIC(9, 6) NULL,
    longitude              NUMERIC(9, 6) NULL,
    site_type              VARCHAR(20)  NULL,
    commissioned_date      DATE         NULL,
    num_bays               SMALLINT     NULL,
    operator_name          VARCHAR(80)  NULL,
    is_active              BOOLEAN      NOT NULL,
    is_deleted             BOOLEAN      NOT NULL DEFAULT FALSE,
    source_created_at_utc  TIMESTAMPTZ  NULL,
    source_updated_at_utc  TIMESTAMPTZ  NOT NULL,
    dw_run_id              UUID         NOT NULL,
    dw_batch_key           DATE         NOT NULL,
    dw_source_row_seq      INT          NULL,
    dw_inserted_at_utc     TIMESTAMPTZ  NOT NULL DEFAULT now(),
    CONSTRAINT pk_stg_station PRIMARY KEY (station_id, source_updated_at_utc)
);

CREATE UNLOGGED TABLE IF NOT EXISTS stg.charge_point (
    charge_point_id        VARCHAR(24)  NOT NULL,
    station_id             VARCHAR(24)  NULL,
    oem_vendor             VARCHAR(40)  NULL,
    model                  VARCHAR(60)  NULL,
    current_type           VARCHAR(2)   NULL,
    rated_power_kw         NUMERIC(6, 2) NULL,
    connector_type         VARCHAR(16)  NULL,
    tariff_plan_id         VARCHAR(20)  NULL,
    firmware_version       VARCHAR(16)  NULL,
    status                 VARCHAR(16)  NULL,
    commissioned_date      DATE         NULL,
    is_deleted             BOOLEAN      NOT NULL DEFAULT FALSE,
    source_created_at_utc  TIMESTAMPTZ  NULL,
    source_updated_at_utc  TIMESTAMPTZ  NOT NULL,
    dw_run_id              UUID         NOT NULL,
    dw_batch_key           DATE         NOT NULL,
    dw_source_row_seq      INT          NULL,
    dw_inserted_at_utc     TIMESTAMPTZ  NOT NULL DEFAULT now(),
    CONSTRAINT pk_stg_charge_point PRIMARY KEY (charge_point_id, source_updated_at_utc)
);

CREATE UNLOGGED TABLE IF NOT EXISTS stg.tariff_plan (
    tariff_plan_id           VARCHAR(20)   NOT NULL,
    plan_name                VARCHAR(80)   NULL,
    price_per_kwh_inr        NUMERIC(8, 2)  NOT NULL,
    price_per_minute_inr     NUMERIC(8, 2)  NOT NULL DEFAULT 0,
    idle_fee_per_minute_inr  NUMERIC(8, 2)  NOT NULL DEFAULT 0,
    min_billable_kwh         NUMERIC(8, 3)  NOT NULL DEFAULT 0,
    gst_rate_pct             NUMERIC(5, 2)  NOT NULL DEFAULT 18,
    valid_from_date          DATE          NULL,
    is_active                BOOLEAN       NOT NULL,
    is_deleted               BOOLEAN       NOT NULL DEFAULT FALSE,
    source_created_at_utc    TIMESTAMPTZ   NULL,
    source_updated_at_utc    TIMESTAMPTZ   NOT NULL,
    dw_run_id                UUID          NOT NULL,
    dw_batch_key             DATE          NOT NULL,
    dw_source_row_seq        INT           NULL,
    dw_inserted_at_utc       TIMESTAMPTZ   NOT NULL DEFAULT now(),
    CONSTRAINT pk_stg_tariff_plan PRIMARY KEY (tariff_plan_id, source_updated_at_utc)
);

CREATE UNLOGGED TABLE IF NOT EXISTS stg.grid_tariff_slab (
    state                        VARCHAR(60)   NOT NULL,
    effective_from_date          DATE          NOT NULL,
    effective_to_date            DATE          NOT NULL,
    slab_name                    VARCHAR(60)   NULL,
    commercial_rate_inr_per_kwh  NUMERIC(8, 3)  NOT NULL,
    source_note                  TEXT          NULL,
    dw_run_id                    UUID          NOT NULL,
    dw_batch_key                 DATE          NOT NULL,
    dw_inserted_at_utc           TIMESTAMPTZ   NOT NULL DEFAULT now(),
    CONSTRAINT pk_stg_grid_tariff_slab PRIMARY KEY (state, effective_from_date),
    CONSTRAINT ck_stg_grid_slab_range CHECK (effective_to_date > effective_from_date)
);

COMMENT ON TABLE stg.grid_tariff_slab IS
    'Source S5, loaded by FULL SNAPSHOT. Sixty rows of slowly-changing reference data are cheaper and safer to reload wholesale - "incremental everywhere" is dogma, not engineering.';


-- === Sessions ==============================================================

-- The typed roaming-partner record, staged separately from stg.session so the
-- cross-source conformance rule (OCPP is authoritative) is visible as one
-- explicit step rather than buried inside a union.
CREATE UNLOGGED TABLE IF NOT EXISTS stg.partner_session (
    cdr_id                 VARCHAR(48)   NOT NULL,
    transaction_id         VARCHAR(40)   NOT NULL,
    partner_code           VARCHAR(20)   NOT NULL,
    direction              VARCHAR(10)   NOT NULL,
    customer_id            VARCHAR(20)   NULL,
    partner_location_id    VARCHAR(40)   NULL,
    partner_evse_id        VARCHAR(40)   NULL,
    charge_point_id        VARCHAR(24)   NULL,
    session_start_utc      TIMESTAMPTZ   NOT NULL,
    session_end_utc        TIMESTAMPTZ   NOT NULL,
    energy_delivered_kwh   NUMERIC(10, 3) NOT NULL,
    duration_seconds       INT           NOT NULL,
    total_cost_inr         NUMERIC(12, 2) NOT NULL,
    currency               VARCHAR(3)    NOT NULL DEFAULT 'INR',
    last_updated_utc       TIMESTAMPTZ   NOT NULL,
    is_duplicate_source    BOOLEAN       NOT NULL DEFAULT FALSE,
    business_date_ist      DATE          NOT NULL,
    dw_run_id              UUID          NOT NULL,
    dw_batch_key           DATE          NOT NULL,
    dw_source_row_seq      INT           NULL,
    dw_inserted_at_utc     TIMESTAMPTZ   NOT NULL DEFAULT now(),
    CONSTRAINT pk_stg_partner_session PRIMARY KEY (cdr_id)
);

COMMENT ON COLUMN stg.partner_session.is_duplicate_source IS
    'TRUE when this partner record maps to a transaction_id that OCPP already reported. OCPP is authoritative; the record is retained here as evidence but never becomes a fact row.';

-- The conformed session: OCPP and partner records unified at one grain.
CREATE UNLOGGED TABLE IF NOT EXISTS stg.session (
    transaction_id         VARCHAR(40)   NOT NULL,
    record_version         INT           NOT NULL DEFAULT 1,
    -- NULLABLE, and the reason is the roaming model rather than laxity: an
    -- OUTBOUND roaming session happened on a PARTNER's hardware, so no
    -- VoltHive charge point exists for it. The fact load resolves that NULL to
    -- the NOT-APPLICABLE (-2) member rather than to UNKNOWN (-1), because
    -- "there is no such device" and "we could not identify the device" are
    -- different facts. An OCPP session with no charge_point_id is a different
    -- matter entirely and is quarantined as CDR_NULL_CHARGE_POINT.
    charge_point_id        VARCHAR(24)   NULL,
    connector_no           SMALLINT      NULL,
    customer_id            VARCHAR(20)   NULL,
    vehicle_id             VARCHAR(20)   NULL,
    id_tag                 VARCHAR(32)   NULL,
    session_start_utc      TIMESTAMPTZ   NOT NULL,
    session_end_utc        TIMESTAMPTZ   NOT NULL,
    energy_delivered_kwh   NUMERIC(10, 3) NOT NULL,
    duration_seconds       INT           NOT NULL,
    stop_reason            VARCHAR(24)   NULL,
    session_status         VARCHAR(16)   NOT NULL,
    auth_method            VARCHAR(16)   NULL,
    source_system          VARCHAR(12)   NOT NULL,
    is_roaming             BOOLEAN       NOT NULL DEFAULT FALSE,
    partner_code           VARCHAR(20)   NULL,
    partner_cost_inr       NUMERIC(12, 2) NULL,
    business_date_ist      DATE          NOT NULL,
    dw_run_id              UUID          NOT NULL,
    dw_batch_key           DATE          NOT NULL,
    dw_source_row_seq      INT           NULL,
    dw_inserted_at_utc     TIMESTAMPTZ   NOT NULL DEFAULT now(),
    CONSTRAINT pk_stg_session PRIMARY KEY (transaction_id),
    CONSTRAINT ck_stg_session_time CHECK (session_end_utc > session_start_utc),
    CONSTRAINT ck_stg_session_energy CHECK (energy_delivered_kwh >= 0)
);

COMMENT ON TABLE stg.session IS
    'GRAIN: one row per completed charging session, identified by transaction_id. OCPP and roaming-partner records conformed to a single grain and distinguished by source_system / is_roaming.';
COMMENT ON COLUMN stg.session.business_date_ist IS
    'session_start_utc AT TIME ZONE Asia/Kolkata. A session at 19:10 UTC belongs to the NEXT IST business day - keying facts on the UTC date would misplace roughly a quarter of evening sessions.';

CREATE INDEX IF NOT EXISTS ix_stg_session_business_date ON stg.session (business_date_ist);
-- Serves the point-in-time dimension join in the fact load.
CREATE INDEX IF NOT EXISTS ix_stg_session_cp_start ON stg.session (charge_point_id, session_start_utc);
CREATE INDEX IF NOT EXISTS ix_stg_session_batch ON stg.session (dw_batch_key);


-- === Meter telemetry =======================================================

-- Typed samples. An intermediate table rather than a CTE because the interval
-- derivation (a LAG over each session) and the orphan check both read it, and
-- because a sample-level table makes the "cumulative register vs additive
-- delta" distinction inspectable in the database rather than only in SQL.
CREATE UNLOGGED TABLE IF NOT EXISTS stg.meter_sample (
    sample_id            VARCHAR(48)   NOT NULL,
    transaction_id       VARCHAR(40)   NOT NULL,
    charge_point_id      VARCHAR(24)   NULL,
    sample_timestamp_utc TIMESTAMPTZ   NOT NULL,
    energy_register_wh   NUMERIC(14, 2) NOT NULL,
    power_kw             NUMERIC(8, 3)  NULL,
    soc_percent          NUMERIC(5, 2)  NULL,
    voltage_v            NUMERIC(8, 2)  NULL,
    current_a            NUMERIC(8, 2)  NULL,
    temperature_c        NUMERIC(5, 2)  NULL,
    business_date_ist    DATE          NOT NULL,
    dw_run_id            UUID          NOT NULL,
    dw_batch_key         DATE          NOT NULL,
    dw_source_row_seq    INT           NULL,
    dw_inserted_at_utc   TIMESTAMPTZ   NOT NULL DEFAULT now(),
    CONSTRAINT pk_stg_meter_sample PRIMARY KEY (sample_id)
);

CREATE INDEX IF NOT EXISTS ix_stg_meter_sample_txn
    ON stg.meter_sample (transaction_id, sample_timestamp_utc);

-- The additive interval. Note the deliberate choice of INTERVAL rather than
-- SAMPLE as the grain: an interval carries an energy DELTA, which is safely
-- summable. A sample carries a cumulative register reading, which is not.
-- Modelling the delta at this boundary is what makes the fact aggregatable.
CREATE UNLOGGED TABLE IF NOT EXISTS stg.meter_interval (
    transaction_id       VARCHAR(40)   NOT NULL,
    interval_seq         INT           NOT NULL,
    charge_point_id      VARCHAR(24)   NULL,
    interval_start_utc   TIMESTAMPTZ   NOT NULL,
    interval_end_utc     TIMESTAMPTZ   NOT NULL,
    interval_seconds     INT           NOT NULL,
    interval_energy_kwh  NUMERIC(10, 4) NOT NULL,
    avg_power_kw         NUMERIC(8, 3)  NULL,
    soc_start_pct        NUMERIC(5, 2)  NULL,
    soc_end_pct          NUMERIC(5, 2)  NULL,
    business_date_ist    DATE          NOT NULL,
    dw_run_id            UUID          NOT NULL,
    dw_batch_key         DATE          NOT NULL,
    dw_inserted_at_utc   TIMESTAMPTZ   NOT NULL DEFAULT now(),
    CONSTRAINT pk_stg_meter_interval PRIMARY KEY (transaction_id, interval_seq),
    CONSTRAINT ck_stg_meter_interval_seq CHECK (interval_seq >= 1),
    CONSTRAINT ck_stg_meter_interval_time CHECK (interval_end_utc > interval_start_utc)
);

COMMENT ON TABLE stg.meter_interval IS
    'GRAIN: one row per elapsed period between two consecutive meter samples within a session. interval_seq starts at 1 for the SECOND sample - the first sample has no predecessor and therefore no interval.';

CREATE INDEX IF NOT EXISTS ix_stg_meter_interval_batch ON stg.meter_interval (dw_batch_key);
