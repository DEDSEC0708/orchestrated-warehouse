-- ===========================================================================
-- 50_indexes.sql - indexes on raw, facts and mart.
--
-- Dimension indexes live with their tables in 30_core_dims.sql because they
-- ARE the SCD2 invariants (the partial unique index is a constraint wearing an
-- index's clothes). The indexes here are pure access-path decisions, so they
-- are gathered in one file where the whole strategy can be reviewed at once.
--
-- Every index below has a named consumer. Anti-patterns deliberately avoided:
--   * indexing raw beyond what restatement and dedupe need - raw is
--     write-heavy and read once per window, so every extra index is pure cost
--   * indexing low-cardinality booleans - a PARTIAL index on the rare value
--     is smaller and actually gets used
--   * indexing every foreign key "just in case"
--
-- Composite column ORDER matters and is chosen by query shape:
-- (charge_point_sk, start_date_key) serves "this charger over time", which is
-- the drill-down a station manager actually performs.
-- ===========================================================================

-- === raw: restatement deletes, retention pruning, dedupe joins =============

CREATE INDEX IF NOT EXISTS ix_raw_cms_customers_batch    ON raw.cms_customers (dw_batch_key);
CREATE INDEX IF NOT EXISTS ix_raw_cms_vehicles_batch     ON raw.cms_vehicles (dw_batch_key);
CREATE INDEX IF NOT EXISTS ix_raw_cms_stations_batch     ON raw.cms_stations (dw_batch_key);
CREATE INDEX IF NOT EXISTS ix_raw_cms_charge_points_batch ON raw.cms_charge_points (dw_batch_key);
CREATE INDEX IF NOT EXISTS ix_raw_cms_tariff_plans_batch ON raw.cms_tariff_plans (dw_batch_key);
CREATE INDEX IF NOT EXISTS ix_raw_grid_tariff_slab_batch ON raw.grid_tariff_slab (dw_batch_key);

-- The CMS staging load picks the latest row per natural key within the window.
CREATE INDEX IF NOT EXISTS ix_raw_cms_customers_key      ON raw.cms_customers (customer_id);
CREATE INDEX IF NOT EXISTS ix_raw_cms_vehicles_key       ON raw.cms_vehicles (vehicle_id);
CREATE INDEX IF NOT EXISTS ix_raw_cms_stations_key       ON raw.cms_stations (station_id);
CREATE INDEX IF NOT EXISTS ix_raw_cms_charge_points_key  ON raw.cms_charge_points (charge_point_id);
CREATE INDEX IF NOT EXISTS ix_raw_cms_tariff_plans_key   ON raw.cms_tariff_plans (tariff_plan_id);

CREATE INDEX IF NOT EXISTS ix_raw_ocpp_cdr_batch         ON raw.ocpp_cdr (dw_batch_key);
CREATE INDEX IF NOT EXISTS ix_raw_ocpp_cdr_txn           ON raw.ocpp_cdr (transaction_id);
CREATE INDEX IF NOT EXISTS ix_raw_ocpp_cdr_payload_hash  ON raw.ocpp_cdr (payload_hash);

CREATE INDEX IF NOT EXISTS ix_raw_meter_value_batch      ON raw.meter_value (dw_batch_key);
CREATE INDEX IF NOT EXISTS ix_raw_meter_value_txn        ON raw.meter_value (transaction_id);
CREATE INDEX IF NOT EXISTS ix_raw_meter_value_sample     ON raw.meter_value (sample_id);

CREATE INDEX IF NOT EXISTS ix_raw_partner_cdr_batch      ON raw.partner_cdr (dw_batch_key);
CREATE INDEX IF NOT EXISTS ix_raw_partner_cdr_id         ON raw.partner_cdr (cdr_id);


-- === core.fact_charging_session ===========================================

-- Restatement deletes. Without this, every rerun scans the whole fact table.
CREATE INDEX IF NOT EXISTS ix_fact_session_batch
    ON core.fact_charging_session (dw_batch_key);
-- "Revenue by day", "sessions per day" - the most common filter of all.
CREATE INDEX IF NOT EXISTS ix_fact_session_date
    ON core.fact_charging_session (start_date_key);
-- "This charger over time" - drill-down from the charge-point mart.
CREATE INDEX IF NOT EXISTS ix_fact_session_cp_date
    ON core.fact_charging_session (charge_point_sk, start_date_key);
-- "This customer's history" - drill-down from the customer mart.
CREATE INDEX IF NOT EXISTS ix_fact_session_customer_date
    ON core.fact_charging_session (customer_sk, start_date_key);
-- "This site over time" - the station-month aggregate build.
CREATE INDEX IF NOT EXISTS ix_fact_session_station_date
    ON core.fact_charging_session (station_sk, start_date_key);
-- PARTIAL rather than a plain index on a boolean: roaming is roughly 6% of
-- rows, so the partial index is a fraction of the size and the planner will
-- actually choose it.
CREATE INDEX IF NOT EXISTS ix_fact_session_roaming
    ON core.fact_charging_session (start_date_key)
    WHERE is_roaming;


-- === core.fact_meter_interval =============================================
--
-- Declared on the PARTITIONED PARENT, so Postgres creates and maintains a
-- matching index on every existing partition AND on every partition the
-- maintenance function adds later. Creating them per partition by hand is how
-- you end up with a September that is mysteriously slower than August.
--
-- Partition pruning already handles the date filter, so these serve the
-- join and drill-down paths that pruning cannot.

CREATE INDEX IF NOT EXISTS ix_fact_meter_interval_session
    ON core.fact_meter_interval (charging_session_sk);
CREATE INDEX IF NOT EXISTS ix_fact_meter_interval_txn
    ON core.fact_meter_interval (transaction_id);
CREATE INDEX IF NOT EXISTS ix_fact_meter_interval_batch
    ON core.fact_meter_interval (dw_batch_key);
CREATE INDEX IF NOT EXISTS ix_fact_meter_interval_cp
    ON core.fact_meter_interval (charge_point_sk, date_key);


-- === core.fact_station_daily_utilization ==================================

CREATE INDEX IF NOT EXISTS ix_fact_station_daily_batch
    ON core.fact_station_daily_utilization (dw_batch_key);
CREATE INDEX IF NOT EXISTS ix_fact_station_daily_date
    ON core.fact_station_daily_utilization (date_key);
CREATE INDEX IF NOT EXISTS ix_fact_station_daily_station_date
    ON core.fact_station_daily_utilization (station_sk, date_key);


-- === mart =================================================================
-- Small tables; the primary keys carry most access. These serve the two
-- filters the showcase queries actually use.

CREATE INDEX IF NOT EXISTS ix_mart_station_month_city
    ON mart.mart_station_month_kpi (city, month_key);
CREATE INDEX IF NOT EXISTS ix_mart_customer_month_segment
    ON mart.mart_customer_month_kpi (customer_segment, month_key);
CREATE INDEX IF NOT EXISTS ix_mart_charge_point_daily_cp
    ON mart.mart_charge_point_daily (charge_point_id, date_key);
