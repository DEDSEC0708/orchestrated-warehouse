-- ===========================================================================
-- 10_raw.sql - the landing layer.
--
-- DESIGN RULE: raw must be able to land ANYTHING, including a value that
-- breaks its own declared type. If ingestion can reject a row, the evidence of
-- what actually arrived is destroyed, and a transformation bug becomes
-- unfixable without re-reading a source that may no longer have the data.
--
-- Two shapes:
--   A. structured-but-untyped - every business column is TEXT (CMS, CSV)
--   B. payload-preserving     - the entire original document in JSONB (JSON)
--
-- Shape B is why schema evolution is a non-event here: when firmware 3.5 adds
-- grid_carbon_intensity, the raw layer has captured it from the first day it
-- appeared, so adopting the field later is a staging change with full history
-- available - not a "we started collecting it today" conversation.
--
-- INDEXING RESTRAINT: raw is write-heavy and read once per window. It carries
-- only the indexes that restatement deletes, retention pruning and dedupe
-- actually use. Every additional index here is pure cost.
-- ===========================================================================

-- ---------------------------------------------------------------------------
-- The standard audit block, present on every raw table:
--
--   dw_raw_id           surrogate row id
--   dw_run_id           correlation id of the run that landed the row
--   dw_source_system    CMS | OCPP | METER | PARTNER | SEED
--   dw_source_file      file path, or endpoint+offset for the API
--   dw_source_row_seq   line number in the file / index in the API page.
--                       Also the deterministic tie-breaker for dedupe.
--   dw_ingested_at_utc  landing time (processing time, NOT event time)
--   dw_batch_key        logical partition - THE DELETE KEY FOR RESTATEMENT
-- ---------------------------------------------------------------------------

-- === Shape A: CMS master data ==============================================

CREATE TABLE IF NOT EXISTS raw.cms_customers (
    dw_raw_id           BIGINT       GENERATED ALWAYS AS IDENTITY,
    dw_run_id           UUID         NOT NULL,
    dw_source_system    VARCHAR(12)  NOT NULL,
    dw_source_file      TEXT         NULL,
    dw_source_row_seq   INT          NULL,
    dw_ingested_at_utc  TIMESTAMPTZ  NOT NULL DEFAULT now(),
    dw_batch_key        DATE         NOT NULL,
    customer_id         TEXT         NULL,
    full_name           TEXT         NULL,
    email               TEXT         NULL,
    phone               TEXT         NULL,
    city                TEXT         NULL,
    state               TEXT         NULL,
    customer_segment    TEXT         NULL,
    subscription_plan   TEXT         NULL,
    kyc_status          TEXT         NULL,
    signup_date         TEXT         NULL,
    is_active           TEXT         NULL,
    created_at          TEXT         NULL,
    updated_at          TEXT         NULL,
    src_row_hash        CHAR(64)     NOT NULL,
    CONSTRAINT pk_raw_cms_customers PRIMARY KEY (dw_raw_id)
);

CREATE TABLE IF NOT EXISTS raw.cms_vehicles (
    dw_raw_id           BIGINT       GENERATED ALWAYS AS IDENTITY,
    dw_run_id           UUID         NOT NULL,
    dw_source_system    VARCHAR(12)  NOT NULL,
    dw_source_file      TEXT         NULL,
    dw_source_row_seq   INT          NULL,
    dw_ingested_at_utc  TIMESTAMPTZ  NOT NULL DEFAULT now(),
    dw_batch_key        DATE         NOT NULL,
    vehicle_id          TEXT         NULL,
    customer_id         TEXT         NULL,
    make                TEXT         NULL,
    model               TEXT         NULL,
    model_year          TEXT         NULL,
    battery_capacity_kwh TEXT        NULL,
    connector_type      TEXT         NULL,
    registration_state  TEXT         NULL,
    created_at          TEXT         NULL,
    updated_at          TEXT         NULL,
    src_row_hash        CHAR(64)     NOT NULL,
    CONSTRAINT pk_raw_cms_vehicles PRIMARY KEY (dw_raw_id)
);

CREATE TABLE IF NOT EXISTS raw.cms_stations (
    dw_raw_id           BIGINT       GENERATED ALWAYS AS IDENTITY,
    dw_run_id           UUID         NOT NULL,
    dw_source_system    VARCHAR(12)  NOT NULL,
    dw_source_file      TEXT         NULL,
    dw_source_row_seq   INT          NULL,
    dw_ingested_at_utc  TIMESTAMPTZ  NOT NULL DEFAULT now(),
    dw_batch_key        DATE         NOT NULL,
    station_id          TEXT         NULL,
    station_name        TEXT         NULL,
    address_line        TEXT         NULL,
    city                TEXT         NULL,
    state               TEXT         NULL,
    pincode             TEXT         NULL,
    latitude            TEXT         NULL,
    longitude           TEXT         NULL,
    site_type           TEXT         NULL,
    commissioned_date   TEXT         NULL,
    num_bays            TEXT         NULL,
    operator_name       TEXT         NULL,
    is_active           TEXT         NULL,
    created_at          TEXT         NULL,
    updated_at          TEXT         NULL,
    src_row_hash        CHAR(64)     NOT NULL,
    CONSTRAINT pk_raw_cms_stations PRIMARY KEY (dw_raw_id)
);

CREATE TABLE IF NOT EXISTS raw.cms_charge_points (
    dw_raw_id           BIGINT       GENERATED ALWAYS AS IDENTITY,
    dw_run_id           UUID         NOT NULL,
    dw_source_system    VARCHAR(12)  NOT NULL,
    dw_source_file      TEXT         NULL,
    dw_source_row_seq   INT          NULL,
    dw_ingested_at_utc  TIMESTAMPTZ  NOT NULL DEFAULT now(),
    dw_batch_key        DATE         NOT NULL,
    charge_point_id     TEXT         NULL,
    station_id          TEXT         NULL,
    oem_vendor          TEXT         NULL,
    model               TEXT         NULL,
    current_type        TEXT         NULL,
    rated_power_kw      TEXT         NULL,
    connector_type      TEXT         NULL,
    tariff_plan_id      TEXT         NULL,
    firmware_version    TEXT         NULL,
    status              TEXT         NULL,
    commissioned_date   TEXT         NULL,
    created_at          TEXT         NULL,
    updated_at          TEXT         NULL,
    src_row_hash        CHAR(64)     NOT NULL,
    CONSTRAINT pk_raw_cms_charge_points PRIMARY KEY (dw_raw_id)
);

CREATE TABLE IF NOT EXISTS raw.cms_tariff_plans (
    dw_raw_id           BIGINT       GENERATED ALWAYS AS IDENTITY,
    dw_run_id           UUID         NOT NULL,
    dw_source_system    VARCHAR(12)  NOT NULL,
    dw_source_file      TEXT         NULL,
    dw_source_row_seq   INT          NULL,
    dw_ingested_at_utc  TIMESTAMPTZ  NOT NULL DEFAULT now(),
    dw_batch_key        DATE         NOT NULL,
    tariff_plan_id      TEXT         NULL,
    plan_name           TEXT         NULL,
    price_per_kwh_inr   TEXT         NULL,
    price_per_minute_inr TEXT        NULL,
    idle_fee_per_minute_inr TEXT     NULL,
    min_billable_kwh    TEXT         NULL,
    gst_rate_pct        TEXT         NULL,
    valid_from_date     TEXT         NULL,
    is_active           TEXT         NULL,
    created_at          TEXT         NULL,
    updated_at          TEXT         NULL,
    src_row_hash        CHAR(64)     NOT NULL,
    CONSTRAINT pk_raw_cms_tariff_plans PRIMARY KEY (dw_raw_id)
);

-- === Shape A: file sources =================================================

CREATE TABLE IF NOT EXISTS raw.meter_value (
    dw_raw_id           BIGINT       GENERATED ALWAYS AS IDENTITY,
    dw_run_id           UUID         NOT NULL,
    dw_source_system    VARCHAR(12)  NOT NULL,
    dw_source_file      TEXT         NULL,
    dw_source_row_seq   INT          NULL,
    dw_ingested_at_utc  TIMESTAMPTZ  NOT NULL DEFAULT now(),
    dw_batch_key        DATE         NOT NULL,
    sample_id           TEXT         NULL,
    transaction_id      TEXT         NULL,
    charge_point_id     TEXT         NULL,
    sample_timestamp    TEXT         NULL,
    energy_register_wh  TEXT         NULL,
    power_kw            TEXT         NULL,
    soc_percent         TEXT         NULL,
    voltage_v           TEXT         NULL,
    current_a           TEXT         NULL,
    temperature_c       TEXT         NULL,
    src_row_hash        CHAR(64)     NOT NULL,
    CONSTRAINT pk_raw_meter_value PRIMARY KEY (dw_raw_id)
);

CREATE TABLE IF NOT EXISTS raw.grid_tariff_slab (
    dw_raw_id           BIGINT       GENERATED ALWAYS AS IDENTITY,
    dw_run_id           UUID         NOT NULL,
    dw_source_system    VARCHAR(12)  NOT NULL,
    dw_source_file      TEXT         NULL,
    dw_source_row_seq   INT          NULL,
    dw_ingested_at_utc  TIMESTAMPTZ  NOT NULL DEFAULT now(),
    dw_batch_key        DATE         NOT NULL,
    state               TEXT         NULL,
    effective_from_date TEXT         NULL,
    effective_to_date   TEXT         NULL,
    slab_name           TEXT         NULL,
    commercial_rate_inr_per_kwh TEXT NULL,
    source_note         TEXT         NULL,
    src_row_hash        CHAR(64)     NOT NULL,
    CONSTRAINT pk_raw_grid_tariff_slab PRIMARY KEY (dw_raw_id)
);

-- === Shape B: payload-preserving JSON sources ==============================

CREATE TABLE IF NOT EXISTS raw.ocpp_cdr (
    dw_raw_id           BIGINT       GENERATED ALWAYS AS IDENTITY,
    dw_run_id           UUID         NOT NULL,
    dw_source_system    VARCHAR(12)  NOT NULL,
    dw_source_file      TEXT         NULL,
    dw_source_row_seq   INT          NULL,
    dw_ingested_at_utc  TIMESTAMPTZ  NOT NULL DEFAULT now(),
    dw_batch_key        DATE         NOT NULL,
    -- Promoted from the payload purely so dedupe joins and restatement deletes
    -- do not have to unwrap JSONB on a million rows. The payload remains the
    -- source of truth; these are a materialised convenience.
    transaction_id      TEXT         NULL,
    charge_point_id     TEXT         NULL,
    start_timestamp_txt TEXT         NULL,
    record_version      TEXT         NULL,
    is_requeued         BOOLEAN      NOT NULL DEFAULT FALSE,
    payload             JSONB        NOT NULL,
    payload_hash        CHAR(64)     NOT NULL,
    CONSTRAINT pk_raw_ocpp_cdr PRIMARY KEY (dw_raw_id)
);

CREATE TABLE IF NOT EXISTS raw.partner_cdr (
    dw_raw_id           BIGINT       GENERATED ALWAYS AS IDENTITY,
    dw_run_id           UUID         NOT NULL,
    dw_source_system    VARCHAR(12)  NOT NULL,
    dw_source_file      TEXT         NULL,
    dw_source_row_seq   INT          NULL,
    dw_ingested_at_utc  TIMESTAMPTZ  NOT NULL DEFAULT now(),
    dw_batch_key        DATE         NOT NULL,
    cdr_id              TEXT         NULL,
    partner_code        TEXT         NULL,
    last_updated_txt    TEXT         NULL,
    is_requeued         BOOLEAN      NOT NULL DEFAULT FALSE,
    payload             JSONB        NOT NULL,
    payload_hash        CHAR(64)     NOT NULL,
    CONSTRAINT pk_raw_partner_cdr PRIMARY KEY (dw_raw_id)
);

COMMENT ON TABLE raw.ocpp_cdr IS
    'GRAIN: one row per delivered OCPP charge detail record line, including duplicates and corrections. Append-only evidence, deduplicated downstream in stg.';
COMMENT ON COLUMN raw.ocpp_cdr.payload IS
    'The complete original JSON line. Unknown keys from newer firmware are captured here losslessly, which is why a new upstream field is a warning rather than data loss.';
COMMENT ON COLUMN raw.ocpp_cdr.payload_hash IS
    'sha256 of the raw line. Byte-identical re-deliveries share a hash and are counted as duplicates rather than treated as errors - OCPP retry storms are normal protocol behaviour.';
COMMENT ON COLUMN raw.ocpp_cdr.is_requeued IS
    'TRUE when this row was re-injected from quarantine by scripts/requeue_quarantine.py rather than read from a landing file.';
