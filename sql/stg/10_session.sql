-- ===========================================================================
-- stg/10_session.sql - conform OCPP charge detail records into stg.session.
--
-- Parameters: :run_id, :batch_lo, :batch_hi
--
-- The busiest transformation in the project. Five things happen here, in this
-- order, and the order matters:
--
--   1. PARSE   promote fields out of the JSONB payload and cast them safely
--   2. DEDUPE  an OCPP retry storm delivers the same record twice; a
--              correction delivers a higher record_version. These are
--              different and must be treated differently.
--   3. DERIVE  normalise the energy unit, compute the delta, the duration and
--              the IST business date
--   4. VALIDATE apply the row-level quality rules
--   5. SPLIT   valid rows to stg.session, invalid rows to quarantine WITH
--              THEIR COMPLETE ORIGINAL PAYLOAD
--
-- THE DEDUPE RULE IS "LATEST VERSION PER transaction_id", NOT "FIRST SEEN".
-- A corrected CDR is re-emitted with record_version = 2 and different meter
-- values; keeping the first would keep the wrong numbers. The superseded row
-- stays in raw as evidence.
--
-- UNIT NORMALISATION IS NOT REJECTION. Roughly 3% of records come from older
-- firmware that reports the meter register in kWh rather than Wh. That is a
-- dialect, not an error, and staging conforms it. Rejecting thousands of
-- perfectly good records over a unit difference would be a quality layer doing
-- active harm.
--
-- ONE RULE PER REJECTED ROW, evaluated in a fixed priority order. A record
-- with both a missing stop and an implausible energy value is reported under
-- the FIRST failure only, so quarantine counts stay attributable: a rule's
-- count is the number of rows it rejected, not the number it could have.
--
-- The valid/invalid split is ONE STATEMENT using a data-modifying CTE. Two
-- separate statements would evaluate the same expensive parse and dedupe
-- twice, and - worse - could disagree with each other if anything in between
-- changed.
--
-- ===========================================================================
-- WHY THE WINDOW IS DEFINED BY KEYS AND NOT BY BATCH DATES
-- ===========================================================================
-- stg.session is keyed on transaction_id, a BUSINESS key, while dw_batch_key
-- is a DELIVERY date. Those two disagree the moment a source re-delivers a
-- key: a corrected CDR with record_version = 2 arrives in a later file, the
-- dedupe below correctly prefers it, and the staged row then sits at the
-- CORRECTION's batch key rather than the original's.
--
-- Reprocessing the original window afterwards would delete nothing (the row
-- has moved) and then insert the superseded original, colliding on the
-- primary key. Partner conformance makes it worse still: stg/12 folds
-- OUTBOUND roaming rows into this same table under their own delivery dates.
--
-- Both halves are therefore scoped by KEY. keys_in_window collects every
-- transaction_id delivered anywhere in the window; the DELETE clears any
-- staged row for those keys wherever it currently sits, plus anything still
-- in the window so a key whose source vanished does not linger; and the
-- INSERT reads every raw row for those keys from every batch, so the dedupe
-- can still see a correction that arrived after the window being reprocessed.
--
-- Reading only the window's own raw rows would fix the crash and silently
-- revert corrected sessions to their superseded values, which is worse.
-- ===========================================================================

CREATE TEMP TABLE keys_in_window ON COMMIT DROP AS
SELECT DISTINCT nullif(btrim(r.payload ->> 'transaction_id'), '') AS transaction_id
FROM raw.ocpp_cdr AS r
WHERE r.dw_batch_key BETWEEN :batch_lo AND :batch_hi
  AND nullif(btrim(r.payload ->> 'transaction_id'), '') IS NOT NULL;

CREATE UNIQUE INDEX ON keys_in_window (transaction_id);

DELETE FROM stg.session
WHERE dw_batch_key BETWEEN :batch_lo AND :batch_hi
   OR transaction_id IN (SELECT k.transaction_id FROM keys_in_window AS k);

DELETE FROM dq.quarantine_ocpp_cdr
WHERE dw_batch_key BETWEEN :batch_lo AND :batch_hi
  -- Untriaged rows only: a row a human has already triaged or requeued is a
  -- record of work done, and a reprocess must not erase it.
  AND status = 'NEW';

WITH parsed AS (
    SELECT
        r.dw_raw_id,
        r.dw_batch_key,
        r.dw_source_file,
        r.dw_source_row_seq,
        r.dw_ingested_at_utc,
        r.payload,
        r.payload_hash,
        nullif(btrim(r.payload ->> 'transaction_id'), '')     AS transaction_id,
        nullif(btrim(r.payload ->> 'charge_point_id'), '')    AS charge_point_id,
        nullif(btrim(r.payload ->> 'customer_id'), '')        AS customer_id,
        nullif(btrim(r.payload ->> 'vehicle_id'), '')         AS vehicle_id,
        nullif(btrim(r.payload ->> 'id_tag'), '')             AS id_tag,
        stg.safe_int(r.payload ->> 'connector_no')            AS connector_no,
        stg.safe_timestamptz(r.payload ->> 'start_timestamp') AS session_start_utc,
        stg.safe_timestamptz(r.payload ->> 'stop_timestamp')  AS session_end_utc,
        stg.safe_numeric(r.payload ->> 'meter_start_wh')      AS meter_start_raw,
        stg.safe_numeric(r.payload ->> 'meter_stop_wh')       AS meter_stop_raw,
        upper(COALESCE(nullif(btrim(r.payload ->> 'energy_unit'), ''), 'WH')) AS energy_unit,
        COALESCE(nullif(btrim(r.payload ->> 'stop_reason'), ''), 'UNKNOWN')   AS stop_reason_raw,
        upper(COALESCE(nullif(btrim(r.payload ->> 'session_status'), ''), 'COMPLETED'))
        AS session_status,
        upper(COALESCE(nullif(btrim(r.payload ->> 'auth_method'), ''), 'UNKNOWN'))
        AS auth_method,
        COALESCE(stg.safe_int(r.payload ->> 'record_version'), 1) AS record_version,
        -- The raw stop_timestamp text is kept so the quarantine detail can
        -- distinguish "the source sent nothing" from "the source sent
        -- something we could not parse". Those are different upstream
        -- problems with different owners.
        r.payload ->> 'stop_timestamp'                        AS stop_timestamp_txt
    FROM raw.ocpp_cdr AS r
    -- Every raw row for a key seen in the window, from EVERY batch. A record
    -- with no transaction_id at all cannot be matched here, so it is unioned
    -- back in below - it still has to reach quarantine rather than vanish.
    INNER JOIN keys_in_window AS k
        ON k.transaction_id = nullif(btrim(r.payload ->> 'transaction_id'), '')

    UNION ALL

    SELECT
        r.dw_raw_id,
        r.dw_batch_key,
        r.dw_source_file,
        r.dw_source_row_seq,
        r.dw_ingested_at_utc,
        r.payload,
        r.payload_hash,
        NULL AS transaction_id,
        nullif(btrim(r.payload ->> 'charge_point_id'), '')    AS charge_point_id,
        nullif(btrim(r.payload ->> 'customer_id'), '')        AS customer_id,
        nullif(btrim(r.payload ->> 'vehicle_id'), '')         AS vehicle_id,
        nullif(btrim(r.payload ->> 'id_tag'), '')             AS id_tag,
        stg.safe_int(r.payload ->> 'connector_no')            AS connector_no,
        stg.safe_timestamptz(r.payload ->> 'start_timestamp') AS session_start_utc,
        stg.safe_timestamptz(r.payload ->> 'stop_timestamp')  AS session_end_utc,
        stg.safe_numeric(r.payload ->> 'meter_start_wh')      AS meter_start_raw,
        stg.safe_numeric(r.payload ->> 'meter_stop_wh')       AS meter_stop_raw,
        upper(COALESCE(nullif(btrim(r.payload ->> 'energy_unit'), ''), 'WH')) AS energy_unit,
        COALESCE(nullif(btrim(r.payload ->> 'stop_reason'), ''), 'UNKNOWN')   AS stop_reason_raw,
        upper(COALESCE(nullif(btrim(r.payload ->> 'session_status'), ''), 'COMPLETED'))
        AS session_status,
        upper(COALESCE(nullif(btrim(r.payload ->> 'auth_method'), ''), 'UNKNOWN'))
        AS auth_method,
        COALESCE(stg.safe_int(r.payload ->> 'record_version'), 1) AS record_version,
        r.payload ->> 'stop_timestamp'                        AS stop_timestamp_txt
    FROM raw.ocpp_cdr AS r
    WHERE r.dw_batch_key BETWEEN :batch_lo AND :batch_hi
      AND nullif(btrim(r.payload ->> 'transaction_id'), '') IS NULL
),

deduped AS (
    SELECT
        p.*,
        row_number() OVER (
            PARTITION BY p.transaction_id
            ORDER BY
                p.record_version DESC,
                p.dw_ingested_at_utc DESC,
                p.dw_source_row_seq DESC,
                p.dw_raw_id DESC
        ) AS winner_rank
    FROM parsed AS p
    WHERE p.transaction_id IS NOT NULL
),

-- Rows with no transaction_id at all cannot be deduplicated (there is nothing
-- to partition by) and cannot become a fact (the grain key is missing), so
-- they bypass the dedupe entirely and go straight to quarantine.
keyless AS (
    SELECT p.*
    FROM parsed AS p
    WHERE p.transaction_id IS NULL
),

candidates AS (
    SELECT
        d.*,
        -- Unit normalisation. Both register readings are converted to kWh
        -- here, once, so nothing downstream has to remember which firmware
        -- version produced a row.
        CASE
            WHEN d.energy_unit = 'KWH' THEN d.meter_start_raw
            ELSE d.meter_start_raw / 1000.0
        END AS meter_start_kwh,
        CASE
            WHEN d.energy_unit = 'KWH' THEN d.meter_stop_raw
            ELSE d.meter_stop_raw / 1000.0
        END AS meter_stop_kwh
    FROM deduped AS d
    WHERE d.winner_rank = 1

    UNION ALL

    SELECT
        k.*,
        1 AS winner_rank,
        NULL::NUMERIC AS meter_start_kwh,
        NULL::NUMERIC AS meter_stop_kwh
    FROM keyless AS k
),

derived AS (
    SELECT
        c.*,
        round(c.meter_stop_kwh - c.meter_start_kwh, 3) AS energy_delivered_kwh,
        CASE
            WHEN c.session_start_utc IS NOT NULL AND c.session_end_utc IS NOT NULL
                THEN extract(EPOCH FROM (c.session_end_utc - c.session_start_utc))::INT
        END AS duration_seconds
    FROM candidates AS c
),

judged AS (
    SELECT
        d.*,
        -- Priority order, top to bottom. The first matching condition wins, so
        -- every rejected row carries exactly one attributable rule code.
        CASE
            WHEN d.transaction_id IS NULL THEN 'CDR_NULL_TRANSACTION_ID'
            WHEN d.charge_point_id IS NULL THEN 'CDR_NULL_CHARGE_POINT'
            WHEN d.session_start_utc IS NULL THEN 'CDR_BAD_TIMESTAMP_FORMAT'
            WHEN d.stop_timestamp_txt IS NULL THEN 'CDR_MISSING_STOP'
            WHEN d.session_end_utc IS NULL THEN 'CDR_BAD_TIMESTAMP_FORMAT'
            WHEN d.meter_start_kwh IS NULL OR d.meter_stop_kwh IS NULL THEN 'CDR_BAD_NUMERIC'
            WHEN d.session_end_utc <= d.session_start_utc THEN 'CDR_TIME_INVERSION'
            WHEN d.meter_stop_kwh < d.meter_start_kwh THEN 'CDR_NEGATIVE_ENERGY'
            WHEN d.duration_seconds NOT BETWEEN 30 AND 86400 THEN 'CDR_IMPLAUSIBLE_DURATION'
            WHEN d.energy_delivered_kwh NOT BETWEEN 0 AND 350 THEN 'CDR_ENERGY_OUT_OF_RANGE'
        END AS rejection_rule
    FROM derived AS d
),

quarantined AS (
    INSERT INTO dq.quarantine_ocpp_cdr (
        dw_run_id, dw_batch_key, source_system, source_file, source_row_seq,
        natural_key, rule_code, rule_detail, raw_payload
    )
    SELECT
        :run_id::UUID,
        j.dw_batch_key,
        'OCPP',
        j.dw_source_file,
        j.dw_source_row_seq,
        j.transaction_id,
        j.rejection_rule,
        -- A human-readable detail with the ACTUAL VALUES in it. "meter_stop_wh
        -- (1280000) < meter_start_wh (1284500), delta = -4500" is triageable;
        -- "range check failed" is not.
        CASE j.rejection_rule
            WHEN 'CDR_NULL_TRANSACTION_ID' THEN 'transaction_id missing from payload'
            WHEN 'CDR_NULL_CHARGE_POINT' THEN 'charge_point_id missing from payload'
            WHEN 'CDR_MISSING_STOP'
                THEN 'stop_timestamp absent - session may still be open; requeueable'
            WHEN 'CDR_BAD_TIMESTAMP_FORMAT'
                THEN 'unparseable timestamp: start='
                     || COALESCE(j.payload ->> 'start_timestamp', '<null>')
                     || ' stop=' || COALESCE(j.stop_timestamp_txt, '<null>')
            WHEN 'CDR_BAD_NUMERIC'
                THEN 'unparseable meter register: start='
                     || COALESCE(j.payload ->> 'meter_start_wh', '<null>')
                     || ' stop=' || COALESCE(j.payload ->> 'meter_stop_wh', '<null>')
            WHEN 'CDR_TIME_INVERSION'
                THEN 'stop (' || j.session_end_utc || ') <= start (' || j.session_start_utc || ')'
            WHEN 'CDR_NEGATIVE_ENERGY'
                THEN 'meter_stop (' || j.meter_stop_kwh || ' kWh) < meter_start ('
                     || j.meter_start_kwh || ' kWh), delta = ' || j.energy_delivered_kwh
            WHEN 'CDR_IMPLAUSIBLE_DURATION'
                THEN 'duration ' || j.duration_seconds || 's outside [30, 86400]'
            WHEN 'CDR_ENERGY_OUT_OF_RANGE'
                THEN 'energy ' || j.energy_delivered_kwh || ' kWh outside [0, 350]'
        END,
        j.payload
    FROM judged AS j
    WHERE j.rejection_rule IS NOT NULL
    RETURNING 1
)

INSERT INTO stg.session (
    transaction_id, record_version, charge_point_id, connector_no,
    customer_id, vehicle_id, id_tag, session_start_utc, session_end_utc,
    energy_delivered_kwh, duration_seconds, stop_reason, session_status,
    auth_method, source_system, is_roaming, business_date_ist,
    dw_run_id, dw_batch_key, dw_source_row_seq
)
SELECT
    j.transaction_id,
    j.record_version,
    j.charge_point_id,
    j.connector_no,
    j.customer_id,
    j.vehicle_id,
    j.id_tag,
    j.session_start_utc,
    j.session_end_utc,
    j.energy_delivered_kwh,
    j.duration_seconds,
    -- An unrecognised stop reason is a WARNING, not a rejection: the record is
    -- kept and the reason is mapped to OTHER. Losing a session because a
    -- charger reported an unfamiliar reason code would be absurd.
    CASE
        WHEN j.stop_reason_raw IN (
            'Local', 'Remote', 'EVDisconnected', 'PowerLoss',
            'EmergencyStop', 'DeAuthorized', 'UNKNOWN'
        ) THEN j.stop_reason_raw
        ELSE 'OTHER'
    END,
    j.session_status,
    j.auth_method,
    'OCPP',
    j.auth_method = 'ROAMING',
    -- THE TIME ZONE DECISION, in one expression. A session starting at
    -- 19:10 UTC belongs to the NEXT IST business day. Keying facts on the UTC
    -- date instead would misplace roughly a quarter of evening sessions and
    -- quietly break every daily revenue number.
    (j.session_start_utc AT TIME ZONE 'Asia/Kolkata')::DATE,
    :run_id::UUID,
    j.dw_batch_key,
    j.dw_source_row_seq
FROM judged AS j
WHERE j.rejection_rule IS NULL;
