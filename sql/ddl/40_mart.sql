-- ===========================================================================
-- 40_mart.sql - the analytics contract.
--
-- Everything here is addressed by BUSINESS keys, not surrogate keys, and
-- exposes no SCD2 mechanics. That is the contract: a consumer of `mart` should
-- never need to know what a surrogate key is, nor write
-- `WHERE is_current AND effective_from <= ... AND ... < effective_to`.
--
-- Mart tables are REBUILT IN FULL inside one transaction on every run. They
-- are small (tens of thousands of rows), so a full rebuild is simpler than
-- incremental maintenance, trivially idempotent, and impossible to get subtly
-- wrong. That is the right trade at this size, and the honest statement is
-- that at 100x the volume it would become an incremental merge.
-- ===========================================================================

-- ---------------------------------------------------------------------------
-- GRAIN: one station x one calendar month.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS mart.mart_station_month_kpi (
    month_key              INT           NOT NULL,
    station_id             VARCHAR(24)   NOT NULL,
    station_name           VARCHAR(120)  NULL,
    city                   VARCHAR(60)   NULL,
    state                  VARCHAR(60)   NULL,
    site_type              VARCHAR(20)   NULL,
    session_count          INT           NOT NULL DEFAULT 0,
    failed_session_count   INT           NOT NULL DEFAULT 0,
    unique_customer_count  INT           NOT NULL DEFAULT 0,
    energy_delivered_kwh   NUMERIC(14, 3) NOT NULL DEFAULT 0,
    gross_revenue_inr      NUMERIC(16, 2) NOT NULL DEFAULT 0,
    grid_energy_cost_inr   NUMERIC(16, 2) NOT NULL DEFAULT 0,
    gross_margin_inr       NUMERIC(16, 2) NOT NULL DEFAULT 0,
    margin_pct             NUMERIC(6, 2)  NULL,
    avg_session_minutes    NUMERIC(8, 2)  NULL,
    occupied_minutes       BIGINT        NOT NULL DEFAULT 0,
    available_minutes      BIGINT        NOT NULL DEFAULT 0,
    utilization_pct        NUMERIC(6, 2)  NULL,
    dw_run_id              UUID          NOT NULL,
    dw_inserted_at_utc     TIMESTAMPTZ   NOT NULL DEFAULT now(),
    CONSTRAINT pk_mart_station_month PRIMARY KEY (month_key, station_id)
);

COMMENT ON TABLE mart.mart_station_month_kpi IS
    'GRAIN: one station per calendar month. utilization_pct and margin_pct are recomputed from the summed numerators and denominators, never averaged from daily ratios.';

-- ---------------------------------------------------------------------------
-- GRAIN: one customer x one calendar month, only for months with activity.
--
-- Deliberately SPARSE, unlike the station-day snapshot: 25,000 customers x 18
-- months of dense rows would be 450,000 rows, almost all zeros, to answer
-- questions nobody asks. "Which customers were inactive" is answerable from
-- absence here; "which stations were idle" was not answerable from absence in
-- the snapshot fact, which is why that one is dense and this one is not.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS mart.mart_customer_month_kpi (
    month_key              INT           NOT NULL,
    customer_id            VARCHAR(20)   NOT NULL,
    customer_segment       VARCHAR(20)   NULL,
    subscription_plan      VARCHAR(20)   NULL,
    city                   VARCHAR(60)   NULL,
    city_tier              VARCHAR(8)    NULL,
    session_count          INT           NOT NULL DEFAULT 0,
    distinct_station_count INT           NOT NULL DEFAULT 0,
    energy_delivered_kwh   NUMERIC(14, 3) NOT NULL DEFAULT 0,
    gross_revenue_inr      NUMERIC(16, 2) NOT NULL DEFAULT 0,
    avg_session_kwh        NUMERIC(10, 3) NULL,
    roaming_session_count  INT           NOT NULL DEFAULT 0,
    is_returning_customer  BOOLEAN       NOT NULL DEFAULT FALSE,
    dw_run_id              UUID          NOT NULL,
    dw_inserted_at_utc     TIMESTAMPTZ   NOT NULL DEFAULT now(),
    CONSTRAINT pk_mart_customer_month PRIMARY KEY (month_key, customer_id)
);

COMMENT ON COLUMN mart.mart_customer_month_kpi.is_returning_customer IS
    'TRUE when the customer had at least one session in an EARLIER month. Computed against full history, not just this month.';

-- ---------------------------------------------------------------------------
-- GRAIN: one charge point x one IST business date, only for days with activity.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS mart.mart_charge_point_daily (
    date_key              INT           NOT NULL,
    charge_point_id       VARCHAR(24)   NOT NULL,
    station_id            VARCHAR(24)   NULL,
    city                  VARCHAR(60)   NULL,
    rated_power_kw        NUMERIC(6, 2)  NULL,
    connector_type        VARCHAR(16)   NULL,
    firmware_version      VARCHAR(16)   NULL,
    session_count         INT           NOT NULL DEFAULT 0,
    failed_session_count  INT           NOT NULL DEFAULT 0,
    energy_delivered_kwh  NUMERIC(14, 3) NOT NULL DEFAULT 0,
    gross_revenue_inr     NUMERIC(16, 2) NOT NULL DEFAULT 0,
    avg_session_kwh       NUMERIC(10, 3) NULL,
    peak_power_kw         NUMERIC(8, 3)  NULL,
    failure_rate_pct      NUMERIC(6, 2)  NULL,
    dw_run_id             UUID          NOT NULL,
    dw_inserted_at_utc    TIMESTAMPTZ   NOT NULL DEFAULT now(),
    CONSTRAINT pk_mart_charge_point_daily PRIMARY KEY (date_key, charge_point_id)
);

COMMENT ON COLUMN mart.mart_charge_point_daily.rated_power_kw IS
    'The rated power IN EFFECT ON THAT DATE, carried from the point-in-time dimension version on the fact - not the device''s power today. This is what makes "did the 60 kW upgrade help?" answerable.';


-- ===========================================================================
-- Semantic views. These are the objects a human or a BI tool actually queries.
-- ===========================================================================

-- One row per session, with descriptive attributes resolved AS THEY WERE at
-- session time. No surrogate keys, no effective-date predicates.
CREATE OR REPLACE VIEW mart.v_sessions_enriched AS
SELECT
    f.transaction_id,
    f.session_start_utc,
    f.session_end_utc,
    d.full_date          AS business_date_ist,
    h.hour_label         AS start_hour_ist,
    h.day_part,
    d.is_weekend,
    cu.customer_id,
    cu.customer_segment,
    cu.subscription_plan,
    cu.city_tier         AS customer_city_tier,
    st.station_id,
    st.station_name,
    st.city,
    st.state,
    st.site_type,
    cp.charge_point_id,
    cp.rated_power_kw,
    cp.connector_type,
    cp.firmware_version,
    tp.tariff_plan_id,
    tp.price_per_kwh_inr,
    v.make               AS vehicle_make,
    v.model              AS vehicle_model,
    o.session_status,
    o.stop_reason,
    o.auth_method,
    o.outcome_group,
    o.is_successful,
    f.energy_delivered_kwh,
    f.duration_seconds,
    f.charging_seconds,
    f.idle_seconds,
    f.peak_power_kw,
    f.avg_power_kw,
    f.gross_revenue_inr,
    f.grid_energy_cost_inr,
    f.gross_margin_inr,
    f.is_roaming,
    f.source_system,
    f.has_meter_detail,
    f.dw_run_id
FROM core.fact_charging_session AS f
INNER JOIN core.dim_date AS d ON d.date_key = f.start_date_key
INNER JOIN core.dim_hour AS h ON h.hour_key = f.start_hour_key
INNER JOIN core.dim_customer AS cu ON cu.customer_sk = f.customer_sk
INNER JOIN core.dim_station AS st ON st.station_sk = f.station_sk
INNER JOIN core.dim_charge_point AS cp ON cp.charge_point_sk = f.charge_point_sk
INNER JOIN core.dim_tariff_plan AS tp ON tp.tariff_plan_sk = f.tariff_plan_sk
INNER JOIN core.dim_vehicle AS v ON v.vehicle_sk = f.vehicle_sk
INNER JOIN core.dim_session_outcome AS o ON o.session_outcome_sk = f.session_outcome_sk;

COMMENT ON VIEW mart.v_sessions_enriched IS
    'One row per session with point-in-time descriptive attributes. Every join is INNER because no fact foreign key is ever NULL - unknown members guarantee it.';

-- Per DAG per day: did it run, did it succeed, how long, how much moved.
CREATE OR REPLACE VIEW mart.v_pipeline_health AS
SELECT
    r.dag_id,
    r.started_at_utc::DATE                                                  AS run_date,
    count(*)                                                                AS run_count,
    count(*) FILTER (WHERE r.status = 'SUCCESS')                            AS success_count,
    count(*) FILTER (WHERE r.status = 'FAILED')                             AS failed_count,
    round(
        100.0 * count(*) FILTER (WHERE r.status = 'SUCCESS') / nullif(count(*), 0), 2
    )                                                                       AS success_rate_pct,
    percentile_disc(0.5) WITHIN GROUP (
        ORDER BY extract(EPOCH FROM (r.ended_at_utc - r.started_at_utc))
    )                                                                       AS p50_duration_seconds,
    percentile_disc(0.95) WITHIN GROUP (
        ORDER BY extract(EPOCH FROM (r.ended_at_utc - r.started_at_utc))
    )                                                                       AS p95_duration_seconds,
    sum(r.rows_ingested)                                                    AS rows_ingested,
    sum(r.rows_quarantined)                                                 AS rows_quarantined,
    sum(r.rows_loaded_core)                                                 AS rows_loaded_core
FROM audit.pipeline_run AS r
GROUP BY r.dag_id, r.started_at_utc::DATE;

-- Per rule per day: how often it ran, how often it failed, and what it said.
CREATE OR REPLACE VIEW mart.v_dq_scorecard AS
SELECT
    c.rule_code,
    c.entity,
    c.layer,
    c.severity,
    c.checked_at_utc::DATE                                       AS check_date,
    count(*)                                                     AS checks_run,
    count(*) FILTER (WHERE c.status = 'PASS')                    AS passed,
    count(*) FILTER (WHERE c.status = 'WARN')                    AS warned,
    count(*) FILTER (WHERE c.status IN ('FAIL', 'ERROR'))        AS failed,
    max(c.fail_ratio)                                            AS worst_fail_ratio,
    max(c.message) FILTER (WHERE c.status IN ('FAIL', 'ERROR'))  AS last_failure_message
FROM dq.check_result AS c
GROUP BY c.rule_code, c.entity, c.layer, c.severity, c.checked_at_utc::DATE;

-- Rule x day x count, plus how long the oldest untriaged row has been sitting.
-- The oldest-unresolved column is the one that turns a quarantine table into a
-- quarantine process: a growing number there is an unattended data problem.
CREATE OR REPLACE VIEW mart.v_quarantine_summary AS
WITH all_quarantine AS (
    SELECT 'ocpp_cdr' AS source_entity, rule_code, dw_batch_key, status, quarantined_at_utc
    FROM dq.quarantine_ocpp_cdr
    UNION ALL
    SELECT 'meter_value' AS source_entity, rule_code, dw_batch_key, status, quarantined_at_utc
    FROM dq.quarantine_meter_value
    UNION ALL
    SELECT 'partner_cdr' AS source_entity, rule_code, dw_batch_key, status, quarantined_at_utc
    FROM dq.quarantine_partner_cdr
    UNION ALL
    SELECT 'cms_entity' AS source_entity, rule_code, dw_batch_key, status, quarantined_at_utc
    FROM dq.quarantine_cms_entity
)

SELECT
    q.source_entity,
    q.rule_code,
    q.dw_batch_key,
    count(*)                                          AS quarantined_rows,
    count(*) FILTER (WHERE q.status = 'NEW')          AS untriaged_rows,
    count(*) FILTER (WHERE q.status = 'REQUEUED')     AS requeued_rows,
    min(q.quarantined_at_utc) FILTER (WHERE q.status = 'NEW') AS oldest_untriaged_at_utc
FROM all_quarantine AS q
GROUP BY q.source_entity, q.rule_code, q.dw_batch_key;

COMMENT ON TABLE mart.mart_customer_month_kpi IS
    'GRAIN: one customer per calendar month, SPARSE - only months with activity produce a row. Contrast the dense station-day snapshot: "which customers were inactive" is answerable from absence here, whereas "which stations were idle" was not.';
COMMENT ON TABLE mart.mart_charge_point_daily IS
    'GRAIN: one charge point per IST business date. Attributes are the point-in-time versions carried through the fact, which is what makes the 30 kW to 60 kW before/after comparison possible at all.';
COMMENT ON VIEW mart.v_pipeline_health IS
    'Per DAG per day: run count, success rate, p50/p95 duration and row totals. Answers "is the pipeline healthy?" without opening the Airflow UI.';
COMMENT ON VIEW mart.v_dq_scorecard IS
    'Per rule per day: how often it ran, how often it failed, and its worst failure ratio. The quality TREND, which matters more than any single run.';
COMMENT ON VIEW mart.v_quarantine_summary IS
    'Rejected rows by entity, rule and batch, with the age of the oldest untriaged one. Counts and rule codes only - never payloads, which is why analysts may read this view but not the quarantine tables themselves.';
