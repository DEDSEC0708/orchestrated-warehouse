-- ===========================================================================
-- stg/20_meter_sample.sql - type and deduplicate meter telemetry samples.
--
-- Parameters: :run_id, :batch_lo, :batch_hi
--
-- The highest-volume load in the platform: roughly eleven and a half million
-- rows at the default profile. Two properties keep it cheap:
--
--   * one pass, entirely set-based - no per-row function calls beyond the
--     inlinable safe-cast helpers;
--   * dedupe on sample_id via ROW_NUMBER rather than DISTINCT ON over a wide
--     row, so the sort is on the key alone.
--
-- A duplicated sample_id is DEDUPLICATED AND COUNTED, never quarantined. A
-- repeated telemetry frame is normal protocol behaviour, not a data error, and
-- treating it as one would fill the quarantine table with noise and hide the
-- rejections that actually matter.
--
-- Samples are NOT filtered against known sessions here. An orphan sample -
-- one whose session record has not arrived yet - is HELD, because the record
-- very often lands in tomorrow's file. Quarantining it immediately would
-- reject data that is about to become valid. The expiry check happens in
-- 22_meter_orphan_expiry.sql, after the lookback window has had its chance.
-- ===========================================================================

DELETE FROM stg.meter_sample
WHERE dw_batch_key BETWEEN :batch_lo AND :batch_hi;

DELETE FROM dq.quarantine_meter_value
WHERE dw_batch_key BETWEEN :batch_lo AND :batch_hi
  AND status = 'NEW';

WITH parsed AS (
    SELECT
        r.dw_raw_id,
        r.dw_batch_key,
        r.dw_source_file,
        r.dw_source_row_seq,
        r.dw_ingested_at_utc,
        nullif(btrim(r.sample_id), '')                   AS sample_id,
        nullif(btrim(r.transaction_id), '')              AS transaction_id,
        nullif(btrim(r.charge_point_id), '')             AS charge_point_id,
        stg.safe_timestamptz(r.sample_timestamp)         AS sample_timestamp_utc,
        stg.safe_numeric(r.energy_register_wh)           AS energy_register_wh,
        stg.safe_numeric(r.power_kw)                     AS power_kw,
        stg.safe_numeric(r.soc_percent)                  AS soc_percent,
        stg.safe_numeric(r.voltage_v)                    AS voltage_v,
        stg.safe_numeric(r.current_a)                    AS current_a,
        stg.safe_numeric(r.temperature_c)                AS temperature_c,
        to_jsonb(r) - 'dw_raw_id'                        AS raw_payload
    FROM raw.meter_value AS r
    WHERE r.dw_batch_key BETWEEN :batch_lo AND :batch_hi
),

deduped AS (
    SELECT
        p.*,
        row_number() OVER (
            PARTITION BY p.sample_id
            ORDER BY p.dw_ingested_at_utc DESC, p.dw_raw_id DESC
        ) AS winner_rank
    FROM parsed AS p
    WHERE p.sample_id IS NOT NULL
),

judged AS (
    SELECT
        d.*,
        CASE
            WHEN d.transaction_id IS NULL THEN 'MTR_NULL_TRANSACTION_ID'
            WHEN d.sample_timestamp_utc IS NULL THEN 'CDR_BAD_TIMESTAMP_FORMAT'
            WHEN d.energy_register_wh IS NULL THEN 'CDR_BAD_NUMERIC'
            -- State of charge is a percentage. 104% and -4% are meter faults,
            -- and a charging-curve analysis built on them is nonsense.
            WHEN d.soc_percent IS NOT NULL AND d.soc_percent NOT BETWEEN 0 AND 100
                THEN 'MTR_SOC_OUT_OF_RANGE'
        END AS rejection_rule
    FROM deduped AS d
    WHERE d.winner_rank = 1
),

quarantined AS (
    INSERT INTO dq.quarantine_meter_value (
        dw_run_id, dw_batch_key, source_system, source_file, source_row_seq,
        natural_key, rule_code, rule_detail, raw_payload
    )
    SELECT
        :run_id::UUID,
        j.dw_batch_key,
        'METER',
        j.dw_source_file,
        j.dw_source_row_seq,
        j.sample_id,
        j.rejection_rule,
        CASE j.rejection_rule
            WHEN 'MTR_NULL_TRANSACTION_ID'
                THEN 'transaction_id missing - the sample cannot be attached to any session'
            WHEN 'CDR_BAD_TIMESTAMP_FORMAT' THEN 'unparseable sample_timestamp'
            WHEN 'CDR_BAD_NUMERIC' THEN 'unparseable energy_register_wh'
            ELSE 'soc_percent ' || j.soc_percent || ' outside [0, 100]'
        END,
        j.raw_payload
    FROM judged AS j
    WHERE j.rejection_rule IS NOT NULL
    RETURNING 1
)

INSERT INTO stg.meter_sample (
    sample_id, transaction_id, charge_point_id, sample_timestamp_utc,
    energy_register_wh, power_kw, soc_percent, voltage_v, current_a,
    temperature_c, business_date_ist, dw_run_id, dw_batch_key, dw_source_row_seq
)
SELECT
    j.sample_id,
    j.transaction_id,
    j.charge_point_id,
    j.sample_timestamp_utc,
    j.energy_register_wh,
    j.power_kw,
    j.soc_percent,
    j.voltage_v,
    j.current_a,
    j.temperature_c,
    (j.sample_timestamp_utc AT TIME ZONE 'Asia/Kolkata')::DATE,
    :run_id::UUID,
    j.dw_batch_key,
    j.dw_source_row_seq
FROM judged AS j
WHERE j.rejection_rule IS NULL;
