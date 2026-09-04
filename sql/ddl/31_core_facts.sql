-- ===========================================================================
-- 31_core_facts.sql - the three fact tables.
--
-- Each begins with its GRAIN sentence, in business language, and each grain is
-- ENFORCED by a unique constraint rather than merely documented. A grain that
-- lives only in a comment is a grain that will be violated.
--
-- NO NULL FOREIGN KEYS, EVER. Every dimension is seeded with an UNKNOWN (-1)
-- and a NOT-APPLICABLE (-2) member, so a missing relationship is represented
-- by a real key rather than by NULL. The payoff is practical: JOINs never
-- silently drop rows, and "how many sessions have an unknown vehicle" is a
-- query instead of a mystery.
--
-- dw_batch_key is the restatement key. Every fact load deletes the window
-- `dw_batch_key BETWEEN :lo AND :hi` and re-inserts it, which is the entire
-- idempotency mechanism for facts.
-- ===========================================================================

-- ---------------------------------------------------------------------------
-- GRAIN: One row represents one completed charging session (one charge detail
--        record) at one charge point by one authenticated identity, uniquely
--        identified by transaction_id.
--
-- Included: OCPP sessions and roaming-partner sessions, conformed to the same
--           grain and distinguished by source_system / is_roaming. Faulted
--           sessions with zero energy ARE included, flagged by the outcome
--           dimension - deleting failures would bias every reliability metric.
-- Excluded: sessions still in progress (no stop record). Those are quarantined
--           as CDR_MISSING_STOP and requeued when the completing record lands.
--
-- Documented edge: a session that crosses a price change keeps the tariff in
-- effect AT SESSION START for its whole duration, matching VoltHive's stated
-- billing policy. The alternative (splitting the session across tariff
-- windows) would change the grain, and is rejected for that reason.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS core.fact_charging_session (
    charging_session_sk   BIGINT        GENERATED ALWAYS AS IDENTITY,
    transaction_id        VARCHAR(40)   NOT NULL,

    -- Conformed dimension keys, all resolved POINT-IN-TIME at session start.
    start_date_key        INT           NOT NULL,
    start_hour_key        SMALLINT      NOT NULL,
    end_date_key          INT           NOT NULL,
    customer_sk           BIGINT        NOT NULL,
    vehicle_sk            BIGINT        NOT NULL,
    station_sk            BIGINT        NOT NULL,
    charge_point_sk       BIGINT        NOT NULL,
    tariff_plan_sk        BIGINT        NOT NULL,
    session_outcome_sk    INT           NOT NULL,

    session_start_utc     TIMESTAMPTZ   NOT NULL,
    session_end_utc       TIMESTAMPTZ   NOT NULL,

    -- Measures. Additivity is stated per column because "can I SUM this?" is
    -- the question every consumer gets wrong at least once.
    energy_delivered_kwh  NUMERIC(10, 3) NOT NULL,
    duration_seconds      INT           NOT NULL,
    charging_seconds      INT           NOT NULL DEFAULT 0,
    idle_seconds          INT           NOT NULL DEFAULT 0,
    peak_power_kw         NUMERIC(8, 3)  NULL,
    avg_power_kw          NUMERIC(8, 3)  NULL,
    energy_charge_inr     NUMERIC(12, 2) NOT NULL DEFAULT 0,
    time_charge_inr       NUMERIC(12, 2) NOT NULL DEFAULT 0,
    idle_fee_inr          NUMERIC(12, 2) NOT NULL DEFAULT 0,
    discount_inr          NUMERIC(12, 2) NOT NULL DEFAULT 0,
    gst_inr               NUMERIC(12, 2) NOT NULL DEFAULT 0,
    gross_revenue_inr     NUMERIC(12, 2) NOT NULL DEFAULT 0,
    grid_energy_cost_inr  NUMERIC(12, 2) NOT NULL DEFAULT 0,
    gross_margin_inr      NUMERIC(12, 2) NOT NULL DEFAULT 0,
    session_count         SMALLINT      NOT NULL DEFAULT 1,

    is_roaming            BOOLEAN       NOT NULL DEFAULT FALSE,
    source_system         VARCHAR(12)   NOT NULL,
    has_meter_detail      BOOLEAN       NOT NULL DEFAULT FALSE,

    dw_run_id             UUID          NOT NULL,
    dw_batch_key          DATE          NOT NULL,
    dw_inserted_at_utc    TIMESTAMPTZ   NOT NULL DEFAULT now(),

    CONSTRAINT pk_fact_charging_session PRIMARY KEY (charging_session_sk),
    -- THIS is what enforces the grain. A dedupe bug fails the load loudly
    -- instead of quietly inflating every revenue number.
    CONSTRAINT uq_fact_charging_session_grain UNIQUE (transaction_id),
    CONSTRAINT ck_fact_session_time CHECK (session_end_utc > session_start_utc),
    CONSTRAINT ck_fact_session_energy CHECK (energy_delivered_kwh >= 0),
    CONSTRAINT ck_fact_session_revenue CHECK (gross_revenue_inr >= 0),
    CONSTRAINT fk_fact_session_start_date
        FOREIGN KEY (start_date_key) REFERENCES core.dim_date (date_key),
    CONSTRAINT fk_fact_session_end_date
        FOREIGN KEY (end_date_key) REFERENCES core.dim_date (date_key),
    CONSTRAINT fk_fact_session_hour
        FOREIGN KEY (start_hour_key) REFERENCES core.dim_hour (hour_key),
    CONSTRAINT fk_fact_session_customer
        FOREIGN KEY (customer_sk) REFERENCES core.dim_customer (customer_sk),
    CONSTRAINT fk_fact_session_vehicle
        FOREIGN KEY (vehicle_sk) REFERENCES core.dim_vehicle (vehicle_sk),
    CONSTRAINT fk_fact_session_station
        FOREIGN KEY (station_sk) REFERENCES core.dim_station (station_sk),
    CONSTRAINT fk_fact_session_charge_point
        FOREIGN KEY (charge_point_sk) REFERENCES core.dim_charge_point (charge_point_sk),
    CONSTRAINT fk_fact_session_tariff
        FOREIGN KEY (tariff_plan_sk) REFERENCES core.dim_tariff_plan (tariff_plan_sk),
    CONSTRAINT fk_fact_session_outcome
        FOREIGN KEY (session_outcome_sk) REFERENCES core.dim_session_outcome (session_outcome_sk)
);

COMMENT ON TABLE core.fact_charging_session IS
    'GRAIN: one completed charging session, identified by transaction_id. Transaction fact.';
COMMENT ON COLUMN core.fact_charging_session.transaction_id IS
    'DEGENERATE DIMENSION: a business key carried on the fact with no dimension table of its own, because it has no attributes worth storing.';
COMMENT ON COLUMN core.fact_charging_session.avg_power_kw IS
    'NON-ADDITIVE. Correct roll-up is SUM(energy_delivered_kwh) / SUM(charging_seconds), never AVG(avg_power_kw).';
COMMENT ON COLUMN core.fact_charging_session.peak_power_kw IS
    'NON-ADDITIVE (a maximum). Roll up with MAX, never SUM.';
COMMENT ON COLUMN core.fact_charging_session.session_count IS
    'Explicit counter set to 1. Makes SUM(session_count) unambiguous across roll-ups where COUNT(*) would be ambiguous after a join.';
COMMENT ON COLUMN core.fact_charging_session.tariff_plan_sk IS
    'The tariff version in effect at session start. Stored on the fact so revenue is reproducible from the fact row alone, without re-walking dimension history.';
COMMENT ON COLUMN core.fact_charging_session.dw_batch_key IS
    'Restatement key: the IST business date this row belongs to. Every fact load deletes this window and re-inserts it, which is what makes reruns idempotent.';


-- ---------------------------------------------------------------------------
-- GRAIN: One row represents one metering interval - the elapsed time between
--        two consecutive meter samples - within one charging session, uniquely
--        identified by (transaction_id, interval_seq).
--
-- INTERVAL, not SAMPLE. A sample carries a CUMULATIVE register reading, which
-- is non-additive and dangerous to sum. An interval carries the DELTA, which
-- is additive. Modelling the delta at ingest is what makes this fact safely
-- aggregatable, and it is the single best design point in the model to raise
-- unprompted.
--
-- PARTITIONED BY RANGE (date_key), monthly. Justified by volume (~11.5M rows
-- at the default profile), by the fact that every query filters on a date
-- range, and by restatement: the window delete becomes a partition-pruned
-- operation instead of a scan of the whole table. Nothing else in the schema
-- is partitioned, and saying why is worth more than partitioning everything.
--
-- NOTE the deliberate ABSENCE of a foreign key to fact_charging_session.
-- Declaring it would be correct in the abstract and wrong in practice: the
-- session restatement window could then not be deleted while its intervals
-- existed, so re-running the session fact task alone would fail, and ON DELETE
-- CASCADE would silently destroy interval rows nobody asked to remove.
-- The relationship is enforced instead by the error-severity referential rule
-- FACT_METER_ORPHAN_SESSION. Operability beat a declared constraint here, and
-- the trade-off is recorded rather than hidden.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS core.fact_meter_interval (
    meter_interval_sk     BIGINT        GENERATED ALWAYS AS IDENTITY,
    transaction_id        VARCHAR(40)   NOT NULL,
    interval_seq          INT           NOT NULL,
    charging_session_sk   BIGINT        NOT NULL,
    date_key              INT           NOT NULL,
    hour_key              SMALLINT      NOT NULL,
    charge_point_sk       BIGINT        NOT NULL,
    customer_sk           BIGINT        NOT NULL,
    interval_start_utc    TIMESTAMPTZ   NOT NULL,
    interval_end_utc      TIMESTAMPTZ   NOT NULL,
    interval_seconds      INT           NOT NULL,
    interval_energy_kwh   NUMERIC(10, 4) NOT NULL,
    avg_power_kw          NUMERIC(8, 3)  NULL,
    soc_start_pct         NUMERIC(5, 2)  NULL,
    soc_end_pct           NUMERIC(5, 2)  NULL,
    dw_run_id             UUID          NOT NULL,
    dw_batch_key          DATE          NOT NULL,
    dw_inserted_at_utc    TIMESTAMPTZ   NOT NULL DEFAULT now(),
    -- The partition key must appear in every unique constraint on a
    -- partitioned table, which is why the PK is composite rather than the bare
    -- surrogate key.
    CONSTRAINT pk_fact_meter_interval PRIMARY KEY (date_key, meter_interval_sk),
    CONSTRAINT uq_fact_meter_interval_grain UNIQUE (date_key, transaction_id, interval_seq),
    CONSTRAINT ck_fact_meter_interval_seq CHECK (interval_seq >= 1),
    CONSTRAINT ck_fact_meter_interval_time CHECK (interval_end_utc > interval_start_utc)
) PARTITION BY RANGE (date_key);

COMMENT ON TABLE core.fact_meter_interval IS
    'GRAIN: one interval between consecutive meter samples within a session, identified by (transaction_id, interval_seq). Partitioned monthly by date_key.';
COMMENT ON COLUMN core.fact_meter_interval.interval_seq IS
    'Starts at 1 for the SECOND sample of a session. The first sample has no predecessor and therefore produces no interval.';
COMMENT ON COLUMN core.fact_meter_interval.soc_start_pct IS
    'SEMI-ADDITIVE: meaningful as a first/last value within a session, meaningless when summed across intervals.';
COMMENT ON COLUMN core.fact_meter_interval.charging_session_sk IS
    'Fact-to-fact reference by surrogate key. A pragmatic, documented denormalisation that saves a join through transaction_id on every drill-down.';


-- ---------------------------------------------------------------------------
-- GRAIN: One row represents one station on one IST business date, WHETHER OR
--        NOT any sessions occurred.
--
-- "Whether or not" is load-bearing. Rows are generated from the cross join of
-- dim_date x active stations and then LEFT JOINed to session aggregates, so a
-- station with zero sessions produces a row of zeros. If only busy days
-- produced rows, every utilisation average would be computed over busy days
-- only and every number would be optimistically biased.
--
-- Why this fact exists at all: idle and available time are ABSENCES of
-- sessions. You cannot derive "this station was empty for fourteen hours" from
-- a table that only contains sessions. A periodic snapshot is the correct
-- answer to "measure a state over a period".
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS core.fact_station_daily_utilization (
    station_daily_sk       BIGINT        GENERATED ALWAYS AS IDENTITY,
    date_key               INT           NOT NULL,
    station_sk             BIGINT        NOT NULL,
    city                   VARCHAR(60)   NULL,
    charge_point_count     SMALLINT      NOT NULL DEFAULT 0,
    available_minutes      INT           NOT NULL DEFAULT 0,
    occupied_minutes       INT           NOT NULL DEFAULT 0,
    session_count          INT           NOT NULL DEFAULT 0,
    failed_session_count   INT           NOT NULL DEFAULT 0,
    unique_customer_count  INT           NOT NULL DEFAULT 0,
    energy_delivered_kwh   NUMERIC(12, 3) NOT NULL DEFAULT 0,
    gross_revenue_inr      NUMERIC(14, 2) NOT NULL DEFAULT 0,
    utilization_pct        NUMERIC(6, 2)  NOT NULL DEFAULT 0,
    dw_run_id              UUID          NOT NULL,
    dw_batch_key           DATE          NOT NULL,
    dw_inserted_at_utc     TIMESTAMPTZ   NOT NULL DEFAULT now(),
    CONSTRAINT pk_fact_station_daily PRIMARY KEY (station_daily_sk),
    CONSTRAINT uq_fact_station_daily_grain UNIQUE (date_key, station_sk),
    CONSTRAINT ck_fact_station_daily_occupied CHECK (occupied_minutes >= 0),
    CONSTRAINT fk_fact_station_daily_date
        FOREIGN KEY (date_key) REFERENCES core.dim_date (date_key),
    CONSTRAINT fk_fact_station_daily_station
        FOREIGN KEY (station_sk) REFERENCES core.dim_station (station_sk)
);

COMMENT ON TABLE core.fact_station_daily_utilization IS
    'GRAIN: one station on one IST business date, densely generated so zero-session days are present. Periodic snapshot fact.';
COMMENT ON COLUMN core.fact_station_daily_utilization.utilization_pct IS
    'NON-ADDITIVE RATIO, stored for convenience only. When aggregating, recompute as SUM(occupied_minutes) / SUM(available_minutes) - averaging this column across stations or days is wrong.';
COMMENT ON COLUMN core.fact_station_daily_utilization.unique_customer_count IS
    'NON-ADDITIVE: distinct counts cannot be summed across days or stations without double counting.';
COMMENT ON COLUMN core.fact_station_daily_utilization.charge_point_count IS
    'SEMI-ADDITIVE: summable across stations on one day, meaningless summed across days.';
