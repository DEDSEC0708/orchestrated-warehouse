-- ===========================================================================
-- mart/01_customer_month_kpi.sql - customer activity by month.
--
-- Parameters: :run_id
--
-- DELIBERATELY SPARSE, unlike the station snapshot. 25,000 customers over 18
-- months of dense rows would be 450,000 rows of almost entirely zeros, to
-- answer a question nobody asks. "Which customers were inactive in May" is
-- answerable from absence here.
--
-- The station fact is dense for the opposite reason: "which stations were
-- idle" was NOT answerable from absence, because an absent row and a station
-- that did not exist yet look identical. Knowing which case you are in is the
-- whole skill.
--
-- Customer attributes come from the version in effect DURING that month, not
-- the current one, so a customer who moved from RETAIL to CORPORATE in April
-- appears as RETAIL in March.
-- ===========================================================================

DELETE FROM mart.mart_customer_month_kpi;

INSERT INTO mart.mart_customer_month_kpi (
    month_key, customer_id, customer_segment, subscription_plan, city, city_tier,
    session_count, distinct_station_count, energy_delivered_kwh,
    gross_revenue_inr, avg_session_kwh, roaming_session_count,
    is_returning_customer, dw_run_id
)
WITH monthly AS (
    SELECT
        d.year_num * 100 + d.month_num                 AS month_key,
        cu.customer_id,
        -- The attributes of the version that was current for the MAJORITY of
        -- that customer's sessions in the month. A customer who changed
        -- segment mid-month is reported under whichever segment did most of
        -- the charging, which is the honest answer at a monthly grain.
        mode() WITHIN GROUP (ORDER BY cu.customer_segment) AS customer_segment,
        mode() WITHIN GROUP (ORDER BY cu.subscription_plan) AS subscription_plan,
        mode() WITHIN GROUP (ORDER BY cu.city)             AS city,
        mode() WITHIN GROUP (ORDER BY cu.city_tier)        AS city_tier,
        count(*)                                       AS session_count,
        count(DISTINCT f.station_sk)                   AS distinct_station_count,
        sum(f.energy_delivered_kwh)                    AS energy_delivered_kwh,
        sum(f.gross_revenue_inr)                       AS gross_revenue_inr,
        avg(f.energy_delivered_kwh)                    AS avg_session_kwh,
        count(*) FILTER (WHERE f.is_roaming)           AS roaming_session_count,
        min(d.full_date)                               AS first_session_in_month
    FROM core.fact_charging_session AS f
    INNER JOIN core.dim_date AS d ON d.date_key = f.start_date_key
    INNER JOIN core.dim_customer AS cu ON cu.customer_sk = f.customer_sk
    WHERE f.customer_sk > 0
    GROUP BY d.year_num * 100 + d.month_num, cu.customer_id
),

first_ever AS (
    SELECT
        cu.customer_id,
        min(d.full_date) AS first_session_date
    FROM core.fact_charging_session AS f
    INNER JOIN core.dim_date AS d ON d.date_key = f.start_date_key
    INNER JOIN core.dim_customer AS cu ON cu.customer_sk = f.customer_sk
    WHERE f.customer_sk > 0
    GROUP BY cu.customer_id
)

SELECT
    m.month_key,
    m.customer_id,
    m.customer_segment,
    m.subscription_plan,
    m.city,
    m.city_tier,
    m.session_count,
    m.distinct_station_count,
    m.energy_delivered_kwh,
    m.gross_revenue_inr,
    round(m.avg_session_kwh, 3),
    m.roaming_session_count,
    -- Repeat status is computed against FULL HISTORY, not against this month.
    -- A customer whose first-ever session predates this month is a returning
    -- customer even if they charged only once in it.
    m.first_session_in_month > fe.first_session_date,
    :run_id::UUID
FROM monthly AS m
INNER JOIN first_ever AS fe ON fe.customer_id = m.customer_id;
