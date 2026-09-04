-- ===========================================================================
-- 05 - Repeat rate and month-over-month return rate by customer segment.
--
-- Acquisition is expensive and churn is invisible until it is measured, so the
-- two questions worth asking of a customer base are "did they come back within
-- the month" and "did they come back at all".
--
-- Both come out of the sparse customer-month mart. Sparseness is the point:
-- a customer with no sessions in July produces NO July row, so absence is a
-- fact rather than a zero that has to be distinguished from a real zero.
--
-- The segment used is the one recorded on the customer's row for THAT month,
-- so a customer promoted from RETAIL to CORPORATE in May counts toward RETAIL
-- retention in April and CORPORATE retention in June. That is the correct
-- reading and it is only possible because the segment is versioned.
--
-- Two different notions of "repeat" are reported side by side, because they
-- answer different questions and conflating them is a common reporting error:
--
--   repeat_rate_pct  more than one session WITHIN the month - engagement.
--   return_rate_pct  active in some EARLIER month too - retention.
--
-- Retention is deliberately "active in ANY earlier month" rather than "active
-- in the immediately preceding month": over a short simulation window the
-- strict definition is dominated by the window boundary and reports zero.
--
-- On a single-month dataset return_rate_pct is legitimately 0 for every
-- segment. That is the correct answer to "how many came back", not a bug -
-- there is nowhere for them to have come back from.
-- ===========================================================================

WITH monthly AS (
    SELECT
        k.month_key,
        k.customer_id,
        k.customer_segment,
        k.subscription_plan,
        k.session_count,
        k.gross_revenue_inr,
        k.energy_delivered_kwh,
        k.is_returning_customer,
        k.roaming_session_count,
        min(k.month_key) OVER (PARTITION BY k.customer_id) AS first_active_month
    FROM mart.mart_customer_month_kpi AS k
)

SELECT
    m.month_key,
    m.customer_segment,
    count(*)                                                        AS active_customers,
    count(*) FILTER (WHERE m.month_key > m.first_active_month)      AS returning_customers,
    count(*) FILTER (WHERE m.month_key = m.first_active_month)      AS new_customers,
    count(*) FILTER (WHERE m.session_count > 1)                     AS multi_session_customers,
    round(
        100.0 * count(*) FILTER (WHERE m.is_returning_customer)
        / nullif(count(*), 0), 1
    )                                                               AS return_rate_pct,
    round(
        100.0 * count(*) FILTER (WHERE m.session_count > 1) / nullif(count(*), 0), 1
    )                                                               AS repeat_rate_pct,
    sum(m.session_count)                                            AS sessions,
    round(avg(m.session_count), 2)                                  AS avg_sessions_per_customer,
    round(sum(m.gross_revenue_inr), 2)                              AS revenue_inr,
    round(sum(m.gross_revenue_inr) / nullif(count(*), 0), 2)        AS revenue_per_customer_inr,
    -- Roaming is a different economic animal: VoltHive earns a settlement fee
    -- rather than a retail margin, so a segment that roams heavily looks more
    -- profitable per session than it is.
    round(
        100.0 * sum(m.roaming_session_count) / nullif(sum(m.session_count), 0), 1
    )                                                               AS roaming_session_pct
FROM monthly AS m
GROUP BY m.month_key, m.customer_segment
ORDER BY m.month_key DESC, revenue_inr DESC;
