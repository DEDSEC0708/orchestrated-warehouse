-- ===========================================================================
-- 01 - Revenue, cost and gross margin by city by month.
--
-- The first question anybody asks of a charging network: where is the money,
-- and is any of it actually profitable? Margin is the interesting half -
-- revenue alone hides the fact that a DC fast charger in Maharashtra buys its
-- electricity at a different slab rate than one in Karnataka.
--
-- Reads the monthly station aggregate rather than the fact table: this is the
-- shape a BI tool would bind to, and it is the reason the mart exists.
--
-- share_of_month_pct is a window over the same result set, so the percentages
-- always sum to 100 within a month no matter how the outer filter is changed.
-- ===========================================================================

SELECT
    k.month_key,
    k.city,
    k.state,
    count(DISTINCT k.station_id)                                   AS active_stations,
    sum(k.session_count)                                           AS sessions,
    round(sum(k.energy_delivered_kwh), 1)                          AS energy_kwh,
    round(sum(k.gross_revenue_inr), 2)                             AS revenue_inr,
    round(sum(k.grid_energy_cost_inr), 2)                          AS grid_cost_inr,
    round(sum(k.gross_margin_inr), 2)                              AS margin_inr,
    round(
        100.0 * sum(k.gross_margin_inr)
        / nullif(sum(k.gross_revenue_inr), 0), 2
    )                                                              AS margin_pct,
    round(
        sum(k.gross_revenue_inr) / nullif(sum(k.energy_delivered_kwh), 0), 2
    )                                                              AS revenue_per_kwh_inr,
    round(
        100.0 * sum(k.gross_revenue_inr)
        / nullif(sum(sum(k.gross_revenue_inr)) OVER (PARTITION BY k.month_key), 0), 2
    )                                                              AS share_of_month_pct
FROM mart.mart_station_month_kpi AS k
WHERE k.city IS NOT NULL
GROUP BY k.month_key, k.city, k.state
ORDER BY k.month_key DESC, revenue_inr DESC;
