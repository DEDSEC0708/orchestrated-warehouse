-- ===========================================================================
-- 09 - Own-network versus roaming, by month and city.
--
-- Roaming is a separate business with separate economics wearing the same
-- table. Two directions matter and they are not symmetric:
--
--   OWN      a VoltHive customer on a VoltHive charger. Retail revenue, grid
--            cost, real margin.
--   INBOUND  another operator's customer on a VoltHive charger. VoltHive
--            still buys the electricity, and settles a wholesale rate.
--   OUTBOUND a VoltHive customer on somebody else's charger. VoltHive buys
--            no electricity at all, which is why grid_energy_cost_inr is zero
--            and why the naive margin ratio on these rows is 100%.
--
-- That last line is the trap. Anyone who computes network margin without
-- separating roaming will report a margin that improves every time roaming
-- grows, which is precisely backwards - the roaming settlement fee is thinner
-- than the retail margin it displaces. Splitting the mix is the whole point.
--
-- OUTBOUND rows are identified by the not-applicable charge point member
-- (-2): there is no VoltHive device involved, and the model says so
-- explicitly rather than leaving a NULL for a reader to interpret.
-- ===========================================================================

WITH classified AS (
    SELECT
        d.year_num * 100 + d.month_num        AS month_key,
        st.city,
        CASE
            WHEN NOT f.is_roaming             THEN 'OWN_NETWORK'
            WHEN f.charge_point_sk = -2       THEN 'ROAMING_OUTBOUND'
            ELSE                                   'ROAMING_INBOUND'
        END                                   AS network_mix,
        f.source_system,
        f.energy_delivered_kwh,
        f.gross_revenue_inr,
        f.grid_energy_cost_inr,
        f.gross_margin_inr,
        f.duration_seconds,
        f.customer_sk,
        o.is_successful
    FROM core.fact_charging_session AS f
    INNER JOIN core.dim_date AS d ON d.date_key = f.start_date_key
    INNER JOIN core.dim_station AS st ON st.station_sk = f.station_sk
    INNER JOIN core.dim_session_outcome AS o ON o.session_outcome_sk = f.session_outcome_sk
)

SELECT
    c.month_key,
    c.city,
    c.network_mix,
    count(*)                                                        AS sessions,
    round(
        100.0 * count(*) / nullif(sum(count(*)) OVER (
            PARTITION BY c.month_key, c.city
        ), 0), 1
    )                                                               AS share_of_city_month_pct,
    count(DISTINCT c.customer_sk)                                   AS distinct_customers,
    round(sum(c.energy_delivered_kwh), 1)                           AS energy_kwh,
    round(avg(c.energy_delivered_kwh), 2)                           AS avg_kwh_per_session,
    round(avg(c.duration_seconds) / 60.0, 1)                        AS avg_minutes,
    round(sum(c.gross_revenue_inr), 2)                              AS revenue_inr,
    round(sum(c.grid_energy_cost_inr), 2)                           AS grid_cost_inr,
    round(sum(c.gross_margin_inr), 2)                               AS margin_inr,
    round(
        100.0 * sum(c.gross_margin_inr) / nullif(sum(c.gross_revenue_inr), 0), 1
    )                                                               AS margin_pct,
    round(
        sum(c.gross_revenue_inr) / nullif(sum(c.energy_delivered_kwh), 0), 2
    )                                                               AS revenue_per_kwh_inr,
    round(
        100.0 * count(*) FILTER (WHERE c.is_successful) / nullif(count(*), 0), 1
    )                                                               AS success_rate_pct
FROM classified AS c
GROUP BY c.month_key, c.city, c.network_mix
ORDER BY c.month_key DESC, c.city ASC, c.network_mix ASC;
