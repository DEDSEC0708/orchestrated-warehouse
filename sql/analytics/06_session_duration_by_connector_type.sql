-- ===========================================================================
-- 06 - How long does a session take, by connector type and power class?
--
-- Duration drives everything downstream: how many bays a site needs, how long
-- the queue gets at 6 pm, and how much idle fee is collectable. A CCS2 session
-- on a 60 kW DC charger and a Type-2 session on a 7 kW AC charger are
-- different products that happen to share a table.
--
-- The mean alone is a bad summary here because the distribution has a long
-- right tail - a car left plugged in overnight is a real session, not an
-- outlier to be filtered away. Percentiles are reported alongside so the
-- shape is visible: p50 for the typical customer, p95 for the bay planner.
--
-- percentile_cont interpolates, which is what you want for a duration.
-- (percentile_disc would return an actual observed value, which is what you
-- want for something like a median transaction id - not this.)
--
-- Idle time is broken out separately because it is the operator's lever: idle
-- minutes occupy a bay without delivering energy, and the tariff's idle fee
-- exists specifically to reduce them.
-- ===========================================================================

SELECT
    cp.connector_type,
    cp.current_type,
    CASE
        WHEN cp.rated_power_kw IS NULL THEN 'UNKNOWN'
        WHEN cp.rated_power_kw < 11  THEN 'AC_SLOW_<11kW'
        WHEN cp.rated_power_kw < 30  THEN 'AC_FAST_11-29kW'
        WHEN cp.rated_power_kw < 60  THEN 'DC_30-59kW'
        ELSE 'DC_60kW+'
    END                                                              AS power_class,
    count(*)                                                         AS sessions,
    round(avg(f.duration_seconds) / 60.0, 1)                         AS avg_minutes,
    round(
        (percentile_cont(0.5) WITHIN GROUP (ORDER BY f.duration_seconds))::NUMERIC / 60.0, 1
    )                                                                AS p50_minutes,
    round(
        (percentile_cont(0.95) WITHIN GROUP (ORDER BY f.duration_seconds))::NUMERIC / 60.0, 1
    )                                                                AS p95_minutes,
    round(avg(f.charging_seconds) / 60.0, 1)                         AS avg_charging_minutes,
    round(avg(f.idle_seconds) / 60.0, 1)                             AS avg_idle_minutes,
    round(
        100.0 * sum(f.idle_seconds) / nullif(sum(f.duration_seconds), 0), 1
    )                                                                AS idle_share_pct,
    round(avg(f.energy_delivered_kwh), 2)                            AS avg_kwh,
    round(avg(f.avg_power_kw), 1)                                    AS avg_power_kw,
    round(max(f.peak_power_kw), 1)                                   AS max_peak_power_kw,
    -- kWh per occupied minute is the throughput number a site planner cares
    -- about; it collapses power, idle time and session length into one figure.
    round(
        sum(f.energy_delivered_kwh) / nullif(sum(f.duration_seconds) / 60.0, 0), 3
    )                                                                AS kwh_per_occupied_minute
FROM core.fact_charging_session AS f
INNER JOIN core.dim_charge_point AS cp ON cp.charge_point_sk = f.charge_point_sk
INNER JOIN core.dim_session_outcome AS o ON o.session_outcome_sk = f.session_outcome_sk
WHERE o.is_successful
  AND f.duration_seconds > 0
  AND cp.charge_point_sk > 0        -- a session on an unresolved device tells us
                                    -- nothing about connectors
GROUP BY
    cp.connector_type,
    cp.current_type,
    CASE
        WHEN cp.rated_power_kw IS NULL THEN 'UNKNOWN'
        WHEN cp.rated_power_kw < 11  THEN 'AC_SLOW_<11kW'
        WHEN cp.rated_power_kw < 30  THEN 'AC_FAST_11-29kW'
        WHEN cp.rated_power_kw < 60  THEN 'DC_30-59kW'
        ELSE 'DC_60kW+'
    END
ORDER BY sessions DESC;
