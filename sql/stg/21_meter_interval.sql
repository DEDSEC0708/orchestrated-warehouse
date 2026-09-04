-- ===========================================================================
-- stg/21_meter_interval.sql - derive additive intervals from cumulative
-- register readings.
--
-- Parameters: :run_id, :batch_lo, :batch_hi
--
-- THIS FILE IS THE ANSWER TO "WHY MODEL INTERVALS RATHER THAN SAMPLES?".
--
-- A meter sample carries a CUMULATIVE register reading - a lifetime total.
-- Summing those is meaningless and actively dangerous: it produces a number
-- that looks like energy, has the right units, and is wrong by orders of
-- magnitude. An interval carries the DELTA between two consecutive readings,
-- which IS additive and can safely be summed across any grouping.
--
-- Turning one into the other is a LAG() over each session, ordered by sample
-- time. Three details that are easy to get wrong and all matter:
--
--   1. ORDER BY SAMPLE TIME, NOT ARRIVAL ORDER. Telemetry frames genuinely
--      arrive out of order - the generator injects exactly that - and a LAG
--      over arrival order would produce negative deltas for perfectly good
--      data.
--
--   2. THE FIRST SAMPLE PRODUCES NO INTERVAL. It has no predecessor, so
--      interval_seq starts at 1 for the SECOND sample. Documented in the DDL
--      and tested, because "off by one row per session" is invisible in
--      aggregate and obvious in a reconciliation.
--
--   3. A NEGATIVE DELTA IS A METER RESET, NOT AN INTERVAL. The register went
--      backwards mid-session. Quarantined as MTR_NEGATIVE_INTERVAL_ENERGY
--      rather than clamped to zero: clamping would silently under-report
--      energy for that session and nobody would ever know.
--
-- Only samples whose session is present in stg.session become intervals.
-- Orphans are HELD, not rejected - see 22_meter_orphan_expiry.sql.
-- ===========================================================================

DELETE FROM stg.meter_interval
WHERE dw_batch_key BETWEEN :batch_lo AND :batch_hi;

WITH ordered AS (
    SELECT
        m.transaction_id,
        m.charge_point_id,
        m.sample_timestamp_utc,
        m.energy_register_wh,
        m.power_kw,
        m.soc_percent,
        m.business_date_ist,
        m.dw_batch_key,
        lag(m.sample_timestamp_utc) OVER session_window AS prev_timestamp_utc,
        lag(m.energy_register_wh) OVER session_window   AS prev_register_wh,
        lag(m.soc_percent) OVER session_window          AS prev_soc_percent,
        row_number() OVER session_window                AS sample_seq
    FROM stg.meter_sample AS m
    -- INNER JOIN, deliberately: a sample with no session yet is not an error
    -- and not an interval. It is simply not ready, and the lookback window
    -- will bring it back next run once its session record has arrived.
    INNER JOIN stg.session AS s ON s.transaction_id = m.transaction_id
    WHERE m.dw_batch_key BETWEEN :batch_lo AND :batch_hi
    WINDOW session_window AS (
        PARTITION BY m.transaction_id ORDER BY m.sample_timestamp_utc, m.sample_id
    )
),

intervals AS (
    SELECT
        o.transaction_id,
        o.charge_point_id,
        o.prev_timestamp_utc                                  AS interval_start_utc,
        o.sample_timestamp_utc                                AS interval_end_utc,
        o.sample_seq - 1                                      AS interval_seq,
        extract(EPOCH FROM (o.sample_timestamp_utc - o.prev_timestamp_utc))::INT
        AS interval_seconds,
        round((o.energy_register_wh - o.prev_register_wh) / 1000.0, 4) AS interval_energy_kwh,
        o.prev_soc_percent                                    AS soc_start_pct,
        o.soc_percent                                         AS soc_end_pct,
        o.business_date_ist,
        o.dw_batch_key
    FROM ordered AS o
    WHERE o.prev_timestamp_utc IS NOT NULL
),

judged AS (
    SELECT
        i.*,
        CASE
            WHEN i.interval_energy_kwh < 0 THEN 'MTR_NEGATIVE_INTERVAL_ENERGY'
            WHEN i.interval_seconds <= 0 THEN 'MTR_ZERO_INTERVAL'
        END AS rejection_rule
    FROM intervals AS i
),

quarantined AS (
    INSERT INTO dq.quarantine_meter_value (
        dw_run_id, dw_batch_key, source_system, natural_key,
        rule_code, rule_detail, raw_payload
    )
    SELECT
        :run_id::UUID,
        j.dw_batch_key,
        'METER',
        j.transaction_id || ':' || j.interval_seq,
        j.rejection_rule,
        CASE j.rejection_rule
            WHEN 'MTR_NEGATIVE_INTERVAL_ENERGY'
                THEN 'register went backwards: delta = ' || j.interval_energy_kwh
                     || ' kWh over ' || j.interval_seconds || 's (meter reset mid-session)'
            ELSE 'interval_seconds = ' || j.interval_seconds
                 || ' - two samples share a timestamp'
        END,
        -- The derived interval rather than a source row, because the interval
        -- IS the thing that failed: neither of the two samples it came from is
        -- individually wrong. Recording the derivation is what makes this
        -- triageable.
        jsonb_build_object(
            'transaction_id', j.transaction_id,
            'interval_seq', j.interval_seq,
            'interval_start_utc', j.interval_start_utc,
            'interval_end_utc', j.interval_end_utc,
            'interval_energy_kwh', j.interval_energy_kwh,
            'interval_seconds', j.interval_seconds,
            'derived_from', 'stg.meter_sample LAG'
        )
    FROM judged AS j
    WHERE j.rejection_rule IS NOT NULL
    RETURNING 1
)

INSERT INTO stg.meter_interval (
    transaction_id, interval_seq, charge_point_id,
    interval_start_utc, interval_end_utc, interval_seconds, interval_energy_kwh,
    avg_power_kw, soc_start_pct, soc_end_pct, business_date_ist,
    dw_run_id, dw_batch_key
)
SELECT
    j.transaction_id,
    j.interval_seq,
    j.charge_point_id,
    j.interval_start_utc,
    j.interval_end_utc,
    j.interval_seconds,
    j.interval_energy_kwh,
    -- Average power RECOMPUTED from the interval's own energy and duration,
    -- not carried over from the sample's instantaneous reading. The sample's
    -- value is a spot measurement; this is the average that actually holds
    -- over the interval, and it is the one that aggregates correctly.
    round((j.interval_energy_kwh * 3600.0) / nullif(j.interval_seconds, 0), 3),
    j.soc_start_pct,
    j.soc_end_pct,
    j.business_date_ist,
    :run_id::UUID,
    j.dw_batch_key
FROM judged AS j
WHERE j.rejection_rule IS NULL;
