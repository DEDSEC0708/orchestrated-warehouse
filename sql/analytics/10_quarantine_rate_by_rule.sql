-- ===========================================================================
-- 10 - Quarantine rate by rule by month: is the source getting worse?
--
-- The other nine queries are about the business. This one is about the
-- pipeline, and it is the one an on-call engineer opens first.
--
-- A quarantine COUNT is close to meaningless on its own - it rises whenever
-- volume rises. A quarantine RATE, against the rows actually read for the same
-- batch, is the number that says whether the upstream system is degrading.
-- The denominator comes from audit.load_stat, which records rows_read
-- separately from rows_inserted precisely so this division is possible.
--
-- month_over_month_change_pp is in PERCENTAGE POINTS, not percent: going from
-- 0.2% to 0.4% is +0.2 pp, and reporting it as "+100%" would be technically
-- true and operationally useless.
--
-- untriaged_rows is the column that turns a quarantine table into a quarantine
-- PROCESS. Rows accumulating in NEW status mean nobody is looking at them, and
-- a quarantine nobody reads is a delete with extra storage costs.
-- ===========================================================================

WITH quarantined AS (
    SELECT
        to_char(q.dw_batch_key, 'YYYYMM')::INT   AS month_key,
        q.source_entity,
        q.rule_code,
        sum(q.quarantined_rows)                  AS quarantined_rows,
        sum(q.untriaged_rows)                    AS untriaged_rows,
        sum(q.requeued_rows)                     AS requeued_rows,
        min(q.oldest_untriaged_at_utc)           AS oldest_untriaged_at_utc
    FROM mart.v_quarantine_summary AS q
    GROUP BY to_char(q.dw_batch_key, 'YYYYMM')::INT, q.source_entity, q.rule_code
),

-- Rows read per month per source, from the load statistics. This is the
-- denominator; without it a count is not a rate.
volume AS (
    SELECT
        to_char(l.dw_batch_key, 'YYYYMM')::INT   AS month_key,
        l.source_system,
        sum(l.rows_read)                         AS rows_read,
        sum(l.rows_inserted)                     AS rows_inserted,
        sum(l.rows_quarantined)                  AS rows_quarantined,
        sum(l.rows_duplicate_skipped)            AS rows_duplicate_skipped
    FROM audit.load_stat AS l
    WHERE l.target_table LIKE 'raw.%'
    GROUP BY to_char(l.dw_batch_key, 'YYYYMM')::INT, l.source_system
),

month_source_total AS (
    SELECT month_key, sum(rows_read) AS rows_read_all_sources
    FROM volume
    GROUP BY month_key
),

rated AS (
    SELECT
        q.month_key,
        q.source_entity,
        q.rule_code,
        q.quarantined_rows,
        q.untriaged_rows,
        q.requeued_rows,
        q.oldest_untriaged_at_utc,
        t.rows_read_all_sources,
        round(
            100.0 * q.quarantined_rows / nullif(t.rows_read_all_sources, 0), 4
        ) AS quarantine_rate_pct
    FROM quarantined AS q
    LEFT JOIN month_source_total AS t ON t.month_key = q.month_key
),

-- The severity a rule was DECLARED with, so an operator can tell a rule that
-- blocks publication from one that merely annotates the run.
with_severity AS (
    SELECT
        r.*,
        COALESCE(declared.severity, 'UNKNOWN') AS severity,
        declared.description               AS rule_description
    FROM rated AS r
    LEFT JOIN dq.rule AS declared ON declared.rule_code = r.rule_code
)

SELECT
    w.month_key,
    w.source_entity,
    w.rule_code,
    w.severity,
    w.quarantined_rows,
    w.rows_read_all_sources,
    w.quarantine_rate_pct,
    round(
        w.quarantine_rate_pct - lag(w.quarantine_rate_pct) OVER (
            PARTITION BY w.source_entity, w.rule_code ORDER BY w.month_key
        ), 4
    )                                        AS month_over_month_change_pp,
    w.untriaged_rows,
    w.requeued_rows,
    w.oldest_untriaged_at_utc,
    left(w.rule_description, 90)             AS what_the_rule_checks
FROM with_severity AS w
ORDER BY w.month_key DESC, w.quarantined_rows DESC;
