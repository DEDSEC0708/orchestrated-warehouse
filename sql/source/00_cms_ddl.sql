-- ===========================================================================
-- 00_cms_ddl.sql - schema of the SIMULATED SOURCE SYSTEM.
--
-- This file is NOT part of the warehouse. It defines the operational CMS that
-- the platform extracts FROM, and it lives in sql/source/ rather than sql/ddl/
-- so that nobody mistakes the source system for part of the platform. It is
-- applied by the data generator, as cms_owner, into the `cms` database.
--
-- In production this would be a different server, read from a replica. It
-- shares an instance here purely so the project runs on a laptop. What it does
-- NOT share is a database: PostgreSQL cannot join across databases, so the
-- pipeline is forced to perform a real extract instead of quietly writing a
-- cross-database join. That constraint is the point of the arrangement.
--
-- THE `_history` TABLES, AND THE HONEST CAVEAT
--
-- A real OLTP exposes only CURRENT state plus an `updated_at` column. An
-- incremental extract against it therefore cannot reconstruct history at
-- bootstrap - that is a genuine limitation of watermark-based change capture,
-- and log-based CDC (Debezium, logical replication) is the production answer.
--
-- To make the SCD Type 2 demonstration genuine rather than fabricated, this
-- simulator exposes BOTH shapes:
--
--   cms.<entity>          current state, as a real OLTP would present it.
--                         Read by the weekly key reconciliation that detects
--                         hard deletes.
--   cms.<entity>_history  every version, standing in for what a CDC feed
--                         would deliver. Read by the incremental extract.
--
-- The extract code is identical either way - a bounded `updated_at` window
-- over a table - so nothing about the pipeline is special-cased for the
-- simulation. The substitution is documented here, in the README and in
-- ADR-010 rather than glossed over.
-- ===========================================================================

CREATE TABLE IF NOT EXISTS customers (
    customer_id        VARCHAR(20)  PRIMARY KEY,
    full_name          VARCHAR(120) NOT NULL,
    email              VARCHAR(160) NULL,
    phone              VARCHAR(16)  NULL,
    city               VARCHAR(60)  NULL,
    state              VARCHAR(60)  NULL,
    customer_segment   VARCHAR(20)  NOT NULL,
    subscription_plan  VARCHAR(20)  NOT NULL,
    kyc_status         VARCHAR(20)  NOT NULL,
    signup_date        DATE         NULL,
    is_active          BOOLEAN      NOT NULL DEFAULT TRUE,
    created_at         TIMESTAMPTZ  NOT NULL,
    updated_at         TIMESTAMPTZ  NOT NULL
);

CREATE TABLE IF NOT EXISTS customers_history (
    customer_id        VARCHAR(20)  NOT NULL,
    full_name          VARCHAR(120) NOT NULL,
    email              VARCHAR(160) NULL,
    phone              VARCHAR(16)  NULL,
    city               VARCHAR(60)  NULL,
    state              VARCHAR(60)  NULL,
    customer_segment   VARCHAR(20)  NOT NULL,
    subscription_plan  VARCHAR(20)  NOT NULL,
    kyc_status         VARCHAR(20)  NOT NULL,
    signup_date        DATE         NULL,
    is_active          BOOLEAN      NOT NULL DEFAULT TRUE,
    created_at         TIMESTAMPTZ  NOT NULL,
    updated_at         TIMESTAMPTZ  NOT NULL,
    PRIMARY KEY (customer_id, updated_at)
);

CREATE TABLE IF NOT EXISTS vehicles (
    vehicle_id            VARCHAR(20)  PRIMARY KEY,
    customer_id           VARCHAR(20)  NULL,
    make                  VARCHAR(40)  NULL,
    model                 VARCHAR(60)  NULL,
    model_year            INT          NULL,
    battery_capacity_kwh  NUMERIC(6, 2) NULL,
    connector_type        VARCHAR(16)  NULL,
    registration_state    VARCHAR(60)  NULL,
    created_at            TIMESTAMPTZ  NOT NULL,
    updated_at            TIMESTAMPTZ  NOT NULL
);

CREATE TABLE IF NOT EXISTS vehicles_history (
    vehicle_id            VARCHAR(20)  NOT NULL,
    customer_id           VARCHAR(20)  NULL,
    make                  VARCHAR(40)  NULL,
    model                 VARCHAR(60)  NULL,
    model_year            INT          NULL,
    battery_capacity_kwh  NUMERIC(6, 2) NULL,
    connector_type        VARCHAR(16)  NULL,
    registration_state    VARCHAR(60)  NULL,
    created_at            TIMESTAMPTZ  NOT NULL,
    updated_at            TIMESTAMPTZ  NOT NULL,
    PRIMARY KEY (vehicle_id, updated_at)
);

CREATE TABLE IF NOT EXISTS stations (
    station_id         VARCHAR(24)  PRIMARY KEY,
    station_name       VARCHAR(120) NULL,
    address_line       VARCHAR(200) NULL,
    city               VARCHAR(60)  NULL,
    state              VARCHAR(60)  NULL,
    pincode            VARCHAR(10)  NULL,
    latitude           NUMERIC(9, 6) NULL,
    longitude          NUMERIC(9, 6) NULL,
    site_type          VARCHAR(20)  NULL,
    commissioned_date  DATE         NULL,
    num_bays           SMALLINT     NULL,
    operator_name      VARCHAR(80)  NULL,
    is_active          BOOLEAN      NOT NULL DEFAULT TRUE,
    created_at         TIMESTAMPTZ  NOT NULL,
    updated_at         TIMESTAMPTZ  NOT NULL
);

CREATE TABLE IF NOT EXISTS stations_history (
    station_id         VARCHAR(24)  NOT NULL,
    station_name       VARCHAR(120) NULL,
    address_line       VARCHAR(200) NULL,
    city               VARCHAR(60)  NULL,
    state              VARCHAR(60)  NULL,
    pincode            VARCHAR(10)  NULL,
    latitude           NUMERIC(9, 6) NULL,
    longitude          NUMERIC(9, 6) NULL,
    site_type          VARCHAR(20)  NULL,
    commissioned_date  DATE         NULL,
    num_bays           SMALLINT     NULL,
    operator_name      VARCHAR(80)  NULL,
    is_active          BOOLEAN      NOT NULL DEFAULT TRUE,
    created_at         TIMESTAMPTZ  NOT NULL,
    updated_at         TIMESTAMPTZ  NOT NULL,
    PRIMARY KEY (station_id, updated_at)
);

-- NOTE the deliberate ABSENCE of a foreign key from charge_points to
-- tariff_plans. Real operational systems accumulate dangling references
-- through bad admin edits, and the generator injects exactly that defect. A
-- warehouse that assumes its source is referentially perfect breaks the first
-- time it is not, so the source here is allowed to be as imperfect as sources
-- actually are.
CREATE TABLE IF NOT EXISTS charge_points (
    charge_point_id    VARCHAR(24)  PRIMARY KEY,
    station_id         VARCHAR(24)  NULL,
    oem_vendor         VARCHAR(40)  NULL,
    model              VARCHAR(60)  NULL,
    current_type       VARCHAR(2)   NULL,
    rated_power_kw     NUMERIC(6, 2) NULL,
    connector_type     VARCHAR(16)  NULL,
    tariff_plan_id     VARCHAR(20)  NULL,
    firmware_version   VARCHAR(16)  NULL,
    status             VARCHAR(16)  NULL,
    commissioned_date  DATE         NULL,
    created_at         TIMESTAMPTZ  NOT NULL,
    updated_at         TIMESTAMPTZ  NOT NULL
);

CREATE TABLE IF NOT EXISTS charge_points_history (
    charge_point_id    VARCHAR(24)  NOT NULL,
    station_id         VARCHAR(24)  NULL,
    oem_vendor         VARCHAR(40)  NULL,
    model              VARCHAR(60)  NULL,
    current_type       VARCHAR(2)   NULL,
    rated_power_kw     NUMERIC(6, 2) NULL,
    connector_type     VARCHAR(16)  NULL,
    tariff_plan_id     VARCHAR(20)  NULL,
    firmware_version   VARCHAR(16)  NULL,
    status             VARCHAR(16)  NULL,
    commissioned_date  DATE         NULL,
    created_at         TIMESTAMPTZ  NOT NULL,
    updated_at         TIMESTAMPTZ  NOT NULL,
    PRIMARY KEY (charge_point_id, updated_at)
);

CREATE TABLE IF NOT EXISTS tariff_plans (
    tariff_plan_id           VARCHAR(20)   PRIMARY KEY,
    plan_name                VARCHAR(80)   NULL,
    price_per_kwh_inr        NUMERIC(8, 2)  NOT NULL,
    price_per_minute_inr     NUMERIC(8, 2)  NOT NULL DEFAULT 0,
    idle_fee_per_minute_inr  NUMERIC(8, 2)  NOT NULL DEFAULT 0,
    min_billable_kwh         NUMERIC(8, 3)  NOT NULL DEFAULT 0,
    gst_rate_pct             NUMERIC(5, 2)  NOT NULL DEFAULT 18,
    valid_from_date          DATE          NULL,
    is_active                BOOLEAN       NOT NULL DEFAULT TRUE,
    created_at               TIMESTAMPTZ   NOT NULL,
    updated_at               TIMESTAMPTZ   NOT NULL
);

CREATE TABLE IF NOT EXISTS tariff_plans_history (
    tariff_plan_id           VARCHAR(20)   NOT NULL,
    plan_name                VARCHAR(80)   NULL,
    price_per_kwh_inr        NUMERIC(8, 2)  NOT NULL,
    price_per_minute_inr     NUMERIC(8, 2)  NOT NULL DEFAULT 0,
    idle_fee_per_minute_inr  NUMERIC(8, 2)  NOT NULL DEFAULT 0,
    min_billable_kwh         NUMERIC(8, 3)  NOT NULL DEFAULT 0,
    gst_rate_pct             NUMERIC(5, 2)  NOT NULL DEFAULT 18,
    valid_from_date          DATE          NULL,
    is_active                BOOLEAN       NOT NULL DEFAULT TRUE,
    created_at               TIMESTAMPTZ   NOT NULL,
    updated_at               TIMESTAMPTZ   NOT NULL,
    PRIMARY KEY (tariff_plan_id, updated_at)
);

-- The extract predicate is `updated_at > lower AND updated_at <= upper`, so
-- this index is the difference between a range scan and a full table scan on
-- every run - which is exactly the index a DBA would have added to the source
-- system when the warehouse team asked to extract from it incrementally.
CREATE INDEX IF NOT EXISTS ix_customers_history_updated ON customers_history (updated_at);
CREATE INDEX IF NOT EXISTS ix_vehicles_history_updated ON vehicles_history (updated_at);
CREATE INDEX IF NOT EXISTS ix_stations_history_updated ON stations_history (updated_at);
CREATE INDEX IF NOT EXISTS ix_charge_points_history_updated ON charge_points_history (updated_at);
CREATE INDEX IF NOT EXISTS ix_tariff_plans_history_updated ON tariff_plans_history (updated_at);
