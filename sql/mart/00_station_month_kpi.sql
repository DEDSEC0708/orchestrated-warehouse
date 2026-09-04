-- ===========================================================================
-- mart/00_station_month_kpi.sql - revenue, margin and utilisation by site.
--
-- Parameters: :run_id
--
-- FULL REBUILD inside the caller's transaction. The table holds a few thousand
-- rows, so a rebuild is simpler than incremental maintenance, trivially
-- idempotent, and impossible to get subtly wrong. The honest statement is that
-- at a hundred times this volume it would become an incremental merge keyed on
-- the affected months.
--
-- RATIOS ARE RECOMPUTED FROM SUMMED NUMERATORS AND DENOMINATORS, never
-- averaged from the daily ratios underneath. AVG(daily utilisation) weights a
-- quiet Sunday the same as a busy Monday and gives an answer that is simply
-- wrong. This is the single most common aggregation error in BI work and the
-- reason utilization_pct carries a comment on the fact table warning about it.
--
-- Addressed by BUSINESS keys, not surrogate keys: a mart consumer should never
-- need to know what a surrogate key is.
-- ===========================================================================

DELETE FROM mart.mart_station_month_kpi;

INSERT INTO mart.mart_station_month_kpi (
    month_key, station_id, station_name, city, state, site_type,
    session_count, failed_session_count, unique_customer_count,
    energy_delivered_kwh, gross_revenue_inr, grid_energy_cost_inr,
    gross_margin_inr, margin_pct, avg_session_minutes,
    occupied_minutes, available_minutes, utilization_pct, dw_run_id
)
WITH session_side AS (
    SELECT
        d.year_num * 100 + d.month_num                       AS month_key,
        st.station_id,
        count(*)                                             AS session_count,
        count(*) FILTER (WHERE NOT o.is_successful)          AS failed_session_count,
        count(DISTINCT f.customer_sk)                        AS unique_customer_count,
        sum(f.energy_delivered_kwh)                          AS energy_delivered_kwh,
        sum(f.gross_revenue_inr)                             AS gross_revenue_inr,
        sum(f.grid_energy_cost_inr)                          AS grid_energy_cost_inr,
        sum(f.gross_margin_inr)                              AS gross_margin_inr,
        avg(f.duration_seconds) / 60.0                       AS avg_session_minutes
    FROM core.fact_charging_session AS f
    INNER JOIN core.dim_date AS d ON d.date_key = f.start_date_key
    INNER JOIN core.dim_station AS st ON st.station_sk = f.station_sk
    INNER JOIN core.dim_session_outcome AS o ON o.session_outcome_sk = f.session_outcome_sk
    WHERE f.station_sk > 0
    GROUP BY d.year_num * 100 + d.month_num, st.station_id
),

-- Capacity comes from the SNAPSHOT fact, not from the session fact. It has to:
-- available minutes on a day with no sessions exist only there.
capacity_side AS (
    SELECT
        d.year_num * 100 + d.month_num  AS month_key,
        st.station_id,
        sum(u.occupied_minutes)         AS occupied_minutes,
        sum(u.available_minutes)        AS available_minutes
    FROM core.fact_station_daily_utilization AS u
    INNER JOIN core.dim_date AS d ON d.date_key = u.date_key
    INNER JOIN core.dim_station AS st ON st.station_sk = u.station_sk
    GROUP BY d.year_num * 100 + d.month_num, st.station_id
),

current_attributes AS (
    SELECT
        st.station_id,
        st.station_name,
        st.city,
        st.state,
        st.site_type
    FROM core.dim_station AS st
    WHERE st.is_current AND st.station_sk > 0
)

SELECT
    COALESCE(s.month_key, c.month_key),
    COALESCE(s.station_id, c.station_id),
    a.station_name,
    a.city,
    a.state,
    a.site_type,
    COALESCE(s.session_count, 0),
    COALESCE(s.failed_session_count, 0),
    COALESCE(s.unique_customer_count, 0),
    COALESCE(s.energy_delivered_kwh, 0),
    COALESCE(s.gross_revenue_inr, 0),
    COALESCE(s.grid_energy_cost_inr, 0),
    COALESCE(s.gross_margin_inr, 0),
    round(100.0 * s.gross_margin_inr / nullif(s.gross_revenue_inr, 0), 2),
    round(s.avg_session_minutes, 2),
    COALESCE(c.occupied_minutes, 0),
    COALESCE(c.available_minutes, 0),
    round(100.0 * c.occupied_minutes / nullif(c.available_minutes, 0), 2),
    :run_id::UUID
FROM session_side AS s
-- FULL OUTER JOIN, deliberately: a station-month with capacity but no sessions
-- is a real and interesting row - it is an idle asset - and an inner join
-- would silently hide exactly the sites somebody needs to know about.
FULL OUTER JOIN capacity_side AS c
    ON c.month_key = s.month_key AND c.station_id = s.station_id
LEFT JOIN current_attributes AS a
    ON a.station_id = COALESCE(s.station_id, c.station_id);
