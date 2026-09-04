-- ===========================================================================
-- stg/11_partner_session.sql - conform roaming partner records.
--
-- Parameters: :run_id, :batch_lo, :batch_hi
--
-- Two things make this source interesting rather than "a third file format".
--
-- REVISIONS. A partner restates a record for up to a week while a billing
-- dispute settles, so the same cdr_id arrives more than once with different
-- money and a later last_updated. The dedupe therefore keeps the LATEST
-- last_updated per cdr_id - which is also why this source's lookback is seven
-- days while the file sources' is three. Lookback is a property of how a
-- source behaves, not a global constant.
--
-- DIRECTION. OUTBOUND means a VoltHive customer charged on a partner's
-- hardware: VoltHive's own OCPP system never saw it, so this API is the ONLY
-- source and missing it under-reports revenue. INBOUND means a partner's
-- customer charged on a VoltHive charger, so the session arrives TWICE - once
-- as an OCPP record and once from the partner. Loading both would double-count
-- it. The conformance rule is that OCPP is authoritative; the partner copy is
-- marked is_duplicate_source and never becomes a fact row, and the
-- SESSION_CROSS_SOURCE_DUP rule asserts that the fact table never gains one.
--
-- ===========================================================================
-- WHY THE WINDOW IS DEFINED BY KEYS AND NOT BY BATCH DATES
-- ===========================================================================
-- The obvious implementation - delete rows whose dw_batch_key is in the
-- window, then insert the raw rows whose dw_batch_key is in the window - is
-- WRONG here, and wrong in a way that only appears on a narrow window.
--
-- stg.partner_session is keyed on cdr_id, a BUSINESS key. dw_batch_key is a
-- DELIVERY date. A revision of a 1 June session delivered on 7 June carries
-- the same cdr_id and a later batch key, and the dedupe below correctly keeps
-- it - so the staged row for that session ends up sitting at batch key 7 June.
--
-- Now reprocess 29 May .. 3 June. The DELETE does not touch the staged row,
-- because it lives at 7 June. The INSERT then re-reads the ORIGINAL record
-- from 1 June and collides with it:
--
--     duplicate key value violates unique constraint "pk_stg_partner_session"
--
-- A row migrating out of the window that produced it makes a delete-insert
-- restatement neither idempotent nor safe. Both halves are therefore scoped
-- by KEY rather than by date:
--
--   * keys_in_window   - every cdr_id delivered anywhere in the window
--   * the DELETE       - removes any staged row for one of those keys,
--                        whichever batch it currently sits in, plus anything
--                        still sitting in the window itself so a key whose
--                        source disappeared does not linger
--   * the INSERT       - reads EVERY raw row for those keys, from every batch,
--                        so the dedupe can still see the 7 June revision while
--                        reprocessing 1 June
--
-- The result is order-independent and window-independent: reprocessing any
-- window, in any order, converges on the same staged rows. Reading only the
-- window's own raw rows would fix the crash and silently DOWNGRADE the
-- session back to its pre-revision money, which is worse than crashing.
--
-- Cost: one extra pass over raw.partner_cdr restricted to the keys in the
-- window. At this project's volume that is negligible; at a much larger one
-- the same shape holds with an index on (payload ->> 'cdr_id').
-- ===========================================================================

CREATE TEMP TABLE keys_in_window ON COMMIT DROP AS
SELECT DISTINCT nullif(btrim(r.payload ->> 'cdr_id'), '') AS cdr_id
FROM raw.partner_cdr AS r
WHERE r.dw_batch_key BETWEEN :batch_lo AND :batch_hi
  AND nullif(btrim(r.payload ->> 'cdr_id'), '') IS NOT NULL;

CREATE UNIQUE INDEX ON keys_in_window (cdr_id);

DELETE FROM stg.partner_session
WHERE dw_batch_key BETWEEN :batch_lo AND :batch_hi
   OR cdr_id IN (SELECT k.cdr_id FROM keys_in_window AS k);

DELETE FROM dq.quarantine_partner_cdr
WHERE dw_batch_key BETWEEN :batch_lo AND :batch_hi
  AND status = 'NEW';

WITH parsed AS (
    SELECT
        r.dw_raw_id,
        r.dw_batch_key,
        r.dw_source_file,
        r.dw_source_row_seq,
        r.dw_ingested_at_utc,
        r.payload,
        nullif(btrim(r.payload ->> 'cdr_id'), '')             AS cdr_id,
        nullif(btrim(r.payload ->> 'transaction_id'), '')     AS transaction_id,
        upper(COALESCE(nullif(btrim(r.payload ->> 'partner_code'), ''), 'UNKNOWN'))
        AS partner_code,
        upper(COALESCE(nullif(btrim(r.payload ->> 'direction'), ''), 'UNKNOWN'))
        AS direction,
        nullif(btrim(r.payload ->> 'customer_id'), '')        AS customer_id,
        nullif(btrim(r.payload ->> 'partner_location_id'), '') AS partner_location_id,
        nullif(btrim(r.payload ->> 'partner_evse_id'), '')    AS partner_evse_id,
        stg.safe_timestamptz(r.payload ->> 'start_date_time') AS session_start_utc,
        stg.safe_timestamptz(r.payload ->> 'end_date_time')   AS session_end_utc,
        stg.safe_numeric(r.payload ->> 'total_energy_kwh')    AS energy_delivered_kwh,
        stg.safe_numeric(r.payload ->> 'total_cost_inr')      AS total_cost_inr,
        upper(COALESCE(nullif(btrim(r.payload ->> 'currency'), ''), 'INR')) AS currency,
        stg.safe_timestamptz(r.payload ->> 'last_updated')    AS last_updated_utc
    FROM raw.partner_cdr AS r
    -- Every raw row for a key seen in the window, from EVERY batch - not just
    -- the rows delivered inside the window. See the header: this is what lets
    -- the dedupe below still find a revision that arrived after the window
    -- being reprocessed.
    INNER JOIN keys_in_window AS k
        ON k.cdr_id = nullif(btrim(r.payload ->> 'cdr_id'), '')
),

deduped AS (
    SELECT
        p.*,
        -- Latest revision wins. Ordering on last_updated FIRST is what makes
        -- a restatement supersede the original rather than racing it.
        row_number() OVER (
            PARTITION BY p.cdr_id
            ORDER BY
                p.last_updated_utc DESC NULLS LAST,
                p.dw_ingested_at_utc DESC,
                p.dw_raw_id DESC
        ) AS winner_rank
    FROM parsed AS p
),

judged AS (
    SELECT
        d.*,
        CASE
            WHEN d.cdr_id IS NULL THEN 'PTR_NULL_CDR_ID'
            WHEN d.transaction_id IS NULL THEN 'PTR_NULL_TRANSACTION_ID'
            WHEN d.session_start_utc IS NULL OR d.session_end_utc IS NULL
                THEN 'CDR_BAD_TIMESTAMP_FORMAT'
            WHEN d.last_updated_utc IS NULL THEN 'PTR_NULL_CURSOR'
            WHEN d.session_end_utc <= d.session_start_utc THEN 'CDR_TIME_INVERSION'
            WHEN d.energy_delivered_kwh IS NULL OR d.energy_delivered_kwh < 0
                THEN 'CDR_NEGATIVE_ENERGY'
            WHEN d.currency <> 'INR' THEN 'PTR_UNEXPECTED_CURRENCY'
        END AS rejection_rule
    FROM deduped AS d
    WHERE d.winner_rank = 1
),

quarantined AS (
    INSERT INTO dq.quarantine_partner_cdr (
        dw_run_id, dw_batch_key, source_system, source_file, source_row_seq,
        natural_key, rule_code, rule_detail, raw_payload
    )
    SELECT
        :run_id::UUID,
        j.dw_batch_key,
        'PARTNER',
        j.dw_source_file,
        j.dw_source_row_seq,
        j.cdr_id,
        j.rejection_rule,
        CASE j.rejection_rule
            WHEN 'PTR_NULL_CDR_ID' THEN 'cdr_id missing from payload'
            WHEN 'PTR_NULL_TRANSACTION_ID' THEN 'transaction_id missing from payload'
            WHEN 'PTR_NULL_CURSOR'
                THEN 'last_updated missing or unparseable - the cursor cannot advance past this record'
            WHEN 'PTR_UNEXPECTED_CURRENCY'
                THEN 'currency is ' || j.currency || ', expected INR; no conversion rate is configured'
            WHEN 'CDR_BAD_TIMESTAMP_FORMAT'
                THEN 'unparseable session timestamps'
            WHEN 'CDR_TIME_INVERSION'
                THEN 'end (' || j.session_end_utc || ') <= start (' || j.session_start_utc || ')'
            ELSE 'total_energy_kwh missing or negative: '
                 || COALESCE(j.energy_delivered_kwh::TEXT, '<null>')
        END,
        j.payload
    FROM judged AS j
    WHERE j.rejection_rule IS NOT NULL
    RETURNING 1
)

INSERT INTO stg.partner_session (
    cdr_id, transaction_id, partner_code, direction, customer_id,
    partner_location_id, partner_evse_id, charge_point_id,
    session_start_utc, session_end_utc, energy_delivered_kwh, duration_seconds,
    total_cost_inr, currency, last_updated_utc, is_duplicate_source,
    business_date_ist, dw_run_id, dw_batch_key, dw_source_row_seq
)
SELECT
    j.cdr_id,
    j.transaction_id,
    j.partner_code,
    j.direction,
    j.customer_id,
    j.partner_location_id,
    j.partner_evse_id,
    -- An INBOUND session happened on VoltHive hardware, so the OCPP record
    -- knows which charger. An OUTBOUND one happened on the partner's, so there
    -- is no VoltHive charge point and the fact resolves to the NOT-APPLICABLE
    -- member rather than to UNKNOWN - the device does not exist, as opposed to
    -- being unidentified.
    s.charge_point_id,
    j.session_start_utc,
    j.session_end_utc,
    j.energy_delivered_kwh,
    extract(EPOCH FROM (j.session_end_utc - j.session_start_utc))::INT,
    COALESCE(j.total_cost_inr, 0),
    j.currency,
    j.last_updated_utc,
    s.transaction_id IS NOT NULL,
    (j.session_start_utc AT TIME ZONE 'Asia/Kolkata')::DATE,
    :run_id::UUID,
    j.dw_batch_key,
    j.dw_source_row_seq
FROM judged AS j
LEFT JOIN stg.session AS s
    ON s.transaction_id = j.transaction_id AND s.source_system = 'OCPP'
WHERE j.rejection_rule IS NULL;
