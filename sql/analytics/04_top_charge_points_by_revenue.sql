-- ===========================================================================
-- 04 - Top 20 charge points by revenue, with reliability alongside.
--
-- Revenue on its own produces a leaderboard. Revenue NEXT TO failure rate
-- produces a work queue: a device in the top twenty with a 15% failure rate is
-- the single most expensive maintenance ticket in the network, because every
-- failed session there is a lost sale at a site that demonstrably sells.
--
-- Reads the daily charge-point mart, which already carries the point-in-time
-- device attributes, so the firmware version shown is the one that was running
-- when the revenue was earned - relevant because firmware and failure rate are
-- correlated in this network.
--
-- revenue_rank is kept as a column rather than left implicit in ORDER BY so
-- the result stays interpretable after a copy-paste into a spreadsheet.
-- ===========================================================================

WITH by_charge_point AS (
    SELECT
        d.charge_point_id,
        max(d.station_id)                                     AS station_id,
        max(d.city)                                           AS city,
        max(d.rated_power_kw)                                 AS max_rated_power_kw,
        max(d.connector_type)                                 AS connector_type,
        max(d.firmware_version)                               AS latest_firmware_seen,
        count(DISTINCT d.date_key)                            AS active_days,
        sum(d.session_count)                                  AS sessions,
        sum(d.failed_session_count)                           AS failed_sessions,
        sum(d.energy_delivered_kwh)                           AS energy_kwh,
        sum(d.gross_revenue_inr)                              AS revenue_inr,
        max(d.peak_power_kw)                                  AS observed_peak_kw
    FROM mart.mart_charge_point_daily AS d
    GROUP BY d.charge_point_id
)

SELECT
    rank() OVER (ORDER BY b.revenue_inr DESC)                 AS revenue_rank,
    b.charge_point_id,
    b.station_id,
    b.city,
    b.max_rated_power_kw,
    b.connector_type,
    b.latest_firmware_seen,
    b.active_days,
    b.sessions,
    b.failed_sessions,
    round(100.0 * b.failed_sessions / nullif(b.sessions, 0), 1)   AS failure_rate_pct,
    round(b.energy_kwh, 1)                                        AS energy_kwh,
    round(b.revenue_inr, 2)                                       AS revenue_inr,
    round(b.revenue_inr / nullif(b.active_days, 0), 2)            AS revenue_per_active_day_inr,
    round(b.observed_peak_kw, 1)                                  AS observed_peak_kw,
    -- Utilisation of the installed capacity, not of the calendar: a device that
    -- never gets near its rating is either badly sited or badly configured.
    round(
        100.0 * b.observed_peak_kw / nullif(b.max_rated_power_kw, 0), 1
    )                                                             AS peak_vs_rated_pct
FROM by_charge_point AS b
ORDER BY b.revenue_inr DESC
LIMIT 20;
