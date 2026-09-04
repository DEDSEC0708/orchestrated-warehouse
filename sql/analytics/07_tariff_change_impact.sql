-- ===========================================================================
-- 07 - What happened to volume when a price changed?
--
-- The second question the Type 2 tariff dimension exists to answer. Each
-- tariff plan is revised a few times over the window; every revision is a
-- separate dimension version with its own effective date and its own price.
--
-- A fact row points at the version that was live when the session started, so
-- "sessions at the old price" and "sessions at the new price" are a join, and
-- the revenue on those rows was computed with the price that was actually
-- charged - not with today's price applied retroactively to last year's
-- energy. Recomputing history at current prices is the single most common way
-- a warehouse quietly reports a number that never happened.
--
-- The comparison window is +/- 14 days around the revision so that seasonality
-- and network growth have less room to contaminate the result. It is still a
-- pre/post cut, not an elasticity estimate: no control group, no confounders
-- adjusted for. Reported as a directional signal, labelled as such.
-- ===========================================================================

WITH revisions AS (
    SELECT
        tariff_plan_id,
        tariff_plan_sk,
        plan_name,
        price_per_kwh_inr,
        effective_from_utc,
        lag(price_per_kwh_inr) OVER (
            PARTITION BY tariff_plan_id ORDER BY effective_from_utc
        ) AS previous_price_per_kwh_inr
    FROM core.dim_tariff_plan
    WHERE tariff_plan_sk > 0
),

price_changes AS (
    SELECT
        r.tariff_plan_id,
        r.plan_name,
        r.effective_from_utc                            AS changed_at_utc,
        r.previous_price_per_kwh_inr                    AS price_before_inr,
        r.price_per_kwh_inr                             AS price_after_inr
    FROM revisions AS r
    WHERE r.previous_price_per_kwh_inr IS NOT NULL
      AND r.previous_price_per_kwh_inr <> r.price_per_kwh_inr
),

-- Every session on the plan within the symmetric window, labelled by which
-- side of the revision it fell on. The join goes through the fact's OWN
-- tariff version, so a session is attributed to the plan it was billed under.
windowed AS (
    SELECT
        pc.tariff_plan_id,
        pc.plan_name,
        pc.changed_at_utc,
        pc.price_before_inr,
        pc.price_after_inr,
        f.session_start_utc >= pc.changed_at_utc        AS is_after,
        f.energy_delivered_kwh,
        f.gross_revenue_inr,
        f.customer_sk
    FROM price_changes AS pc
    INNER JOIN core.dim_tariff_plan AS tp
        ON tp.tariff_plan_id = pc.tariff_plan_id
    INNER JOIN core.fact_charging_session AS f
        ON f.tariff_plan_sk = tp.tariff_plan_sk
    WHERE f.session_start_utc >= pc.changed_at_utc - INTERVAL '14 days'
      AND f.session_start_utc <  pc.changed_at_utc + INTERVAL '14 days'
)

SELECT
    w.tariff_plan_id,
    w.plan_name,
    w.changed_at_utc::DATE                                            AS changed_on,
    w.price_before_inr,
    w.price_after_inr,
    round(
        100.0 * (w.price_after_inr - w.price_before_inr)
        / nullif(w.price_before_inr, 0), 1
    )                                                                 AS price_change_pct,
    count(*) FILTER (WHERE NOT w.is_after)                            AS sessions_14d_before,
    count(*) FILTER (WHERE w.is_after)                                AS sessions_14d_after,
    round(
        100.0 * (
            count(*) FILTER (WHERE w.is_after)
            - count(*) FILTER (WHERE NOT w.is_after)
        )::NUMERIC / nullif(count(*) FILTER (WHERE NOT w.is_after), 0), 1
    )                                                                 AS session_change_pct,
    count(DISTINCT w.customer_sk) FILTER (WHERE NOT w.is_after)       AS customers_before,
    count(DISTINCT w.customer_sk) FILTER (WHERE w.is_after)           AS customers_after,
    round(sum(w.energy_delivered_kwh) FILTER (WHERE NOT w.is_after), 1) AS energy_kwh_before,
    round(sum(w.energy_delivered_kwh) FILTER (WHERE w.is_after), 1)     AS energy_kwh_after,
    round(sum(w.gross_revenue_inr) FILTER (WHERE NOT w.is_after), 2)    AS revenue_before_inr,
    round(sum(w.gross_revenue_inr) FILTER (WHERE w.is_after), 2)        AS revenue_after_inr,
    -- The commercial question in one column: did the price rise more than
    -- volume fell? A positive number means the revision made money.
    round(
        sum(w.gross_revenue_inr) FILTER (WHERE w.is_after)
        - sum(w.gross_revenue_inr) FILTER (WHERE NOT w.is_after), 2
    )                                                                 AS revenue_delta_inr
FROM windowed AS w
GROUP BY
    w.tariff_plan_id, w.plan_name, w.changed_at_utc,
    w.price_before_inr, w.price_after_inr
ORDER BY w.changed_at_utc ASC, w.tariff_plan_id ASC;
