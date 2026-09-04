-- ===========================================================================
-- 03 - Did the 30 kW -> 60 kW upgrade actually pay for itself?
--
-- THIS IS THE QUERY THE WHOLE SCD TYPE 2 APPARATUS EXISTS FOR.
--
-- A charge point that was upgraded mid-window has two dimension versions. The
-- fact rows point at whichever version was current when the session ran, so
-- "energy per session before the upgrade" and "after the upgrade" fall out of
-- an ordinary join - no as-of logic in the query, no snapshot tables, no
-- guessing.
--
-- With a Type 1 dimension this question would be UNANSWERABLE. Every session
-- would appear to have happened on a 60 kW charger, including the ones that
-- ran nine months earlier at half the power, and the measured "improvement"
-- would be exactly zero. That failure is silent: the query returns a number,
-- the number looks plausible, and it is wrong.
--
-- Caveats worth stating out loud, because a portfolio project that pretends a
-- before/after cut is a causal estimate is a portfolio project that will not
-- survive its first interview question:
--
--   * This is a naive pre/post comparison, not a difference-in-differences.
--     Seasonality and network growth are not controlled for.
--   * Upgrades are not randomly assigned. The busiest sites get upgraded
--     first, so the "before" period is already the high-demand period.
--
-- A real analysis would use the non-upgraded chargers as a control group.
-- The point here is that the DATA MODEL makes that analysis possible.
-- ===========================================================================

WITH versions AS (
    SELECT
        charge_point_id,
        rated_power_kw,
        effective_from_utc,
        first_value(rated_power_kw) OVER (
            PARTITION BY charge_point_id ORDER BY effective_from_utc
        ) AS original_power_kw
    FROM core.dim_charge_point
    WHERE charge_point_sk > 0          -- exclude the UNKNOWN / NOT_APPLICABLE members
      AND rated_power_kw IS NOT NULL
),

-- The first version at a HIGHER power than the device shipped with. Taking the
-- minimum effective_from means a device upgraded twice is measured from its
-- first upgrade, which is the conservative reading.
upgrades AS (
    SELECT
        charge_point_id,
        min(effective_from_utc)             AS upgraded_at_utc,
        min(original_power_kw)              AS power_before_kw,
        max(rated_power_kw)                 AS power_after_kw
    FROM versions
    WHERE rated_power_kw > original_power_kw
    GROUP BY charge_point_id
),

classified AS (
    SELECT
        u.charge_point_id,
        u.upgraded_at_utc,
        u.power_before_kw,
        u.power_after_kw,
        f.session_start_utc < u.upgraded_at_utc AS is_before,
        f.energy_delivered_kwh,
        f.duration_seconds,
        f.avg_power_kw,
        f.gross_revenue_inr,
        o.is_successful
    FROM core.fact_charging_session AS f
    INNER JOIN core.dim_charge_point AS cp ON cp.charge_point_sk = f.charge_point_sk
    INNER JOIN upgrades AS u ON u.charge_point_id = cp.charge_point_id
    INNER JOIN core.dim_session_outcome AS o ON o.session_outcome_sk = f.session_outcome_sk
)

SELECT
    c.charge_point_id,
    c.upgraded_at_utc::DATE                                              AS upgraded_on,
    c.power_before_kw,
    c.power_after_kw,
    count(*) FILTER (WHERE c.is_before)                                  AS sessions_before,
    count(*) FILTER (WHERE NOT c.is_before)                              AS sessions_after,
    round(avg(c.energy_delivered_kwh) FILTER (WHERE c.is_before), 2)     AS avg_kwh_before,
    round(avg(c.energy_delivered_kwh) FILTER (WHERE NOT c.is_before), 2) AS avg_kwh_after,
    round(avg(c.duration_seconds / 60.0) FILTER (WHERE c.is_before), 1)  AS avg_minutes_before,
    round(avg(c.duration_seconds / 60.0) FILTER (WHERE NOT c.is_before), 1)
                                                                         AS avg_minutes_after,
    round(avg(c.avg_power_kw) FILTER (WHERE c.is_before), 1)             AS avg_power_before_kw,
    round(avg(c.avg_power_kw) FILTER (WHERE NOT c.is_before), 1)         AS avg_power_after_kw,
    round(
        100.0 * (
            avg(c.energy_delivered_kwh) FILTER (WHERE NOT c.is_before)
            - avg(c.energy_delivered_kwh) FILTER (WHERE c.is_before)
        ) / nullif(avg(c.energy_delivered_kwh) FILTER (WHERE c.is_before), 0), 1
    )                                                                    AS energy_uplift_pct,
    round(
        100.0 * count(*) FILTER (WHERE c.is_successful AND NOT c.is_before)
        / nullif(count(*) FILTER (WHERE NOT c.is_before), 0), 1
    )                                                                    AS success_rate_after_pct
FROM classified AS c
GROUP BY
    c.charge_point_id, c.upgraded_at_utc, c.power_before_kw, c.power_after_kw
-- A one-sided comparison is not a comparison. Devices upgraded before the
-- first session or after the last one are dropped rather than reported with a
-- NULL that a reader might mistake for "no change".
HAVING count(*) FILTER (WHERE c.is_before) > 0
   AND count(*) FILTER (WHERE NOT c.is_before) > 0
ORDER BY energy_uplift_pct DESC NULLS LAST;
