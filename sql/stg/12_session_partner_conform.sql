-- ===========================================================================
-- stg/12_session_partner_conform.sql - fold OUTBOUND roaming into stg.session.
--
-- Parameters: :run_id, :batch_lo, :batch_hi
--
-- CONFORMED DIMENSIONS, applied to a fact. The whole point of a conformed
-- grain is that "a charging session" means one thing regardless of which
-- system reported it, so a question like "revenue by month" needs no union and
-- no caveat about which sources are included.
--
-- Only OUTBOUND records are folded in. INBOUND ones describe a session that
-- ALREADY EXISTS in stg.session from the OCPP file - loading both would
-- double-count it - so they were marked is_duplicate_source in the previous
-- step and are excluded here. OCPP is authoritative because it comes from
-- VoltHive's own hardware; the partner's copy is a courtesy.
--
-- This asymmetry is what the SESSION_CROSS_SOURCE_DUP rule verifies: zero
-- transaction_ids appear in the fact from both sources.
-- ===========================================================================

INSERT INTO stg.session (
    transaction_id, record_version, charge_point_id, connector_no,
    customer_id, vehicle_id, id_tag, session_start_utc, session_end_utc,
    energy_delivered_kwh, duration_seconds, stop_reason, session_status,
    auth_method, source_system, is_roaming, partner_code, partner_cost_inr,
    business_date_ist, dw_run_id, dw_batch_key, dw_source_row_seq
)
SELECT
    p.transaction_id,
    1,
    -- No VoltHive charge point exists for an OUTBOUND session: the energy came
    -- out of someone else's hardware. Left NULL here so the fact load resolves
    -- it to the NOT-APPLICABLE (-2) member rather than to UNKNOWN (-1). "There
    -- is no such device" and "we could not identify the device" are different
    -- facts and the model keeps them different.
    NULL,
    NULL,
    p.customer_id,
    NULL,
    NULL,
    p.session_start_utc,
    p.session_end_utc,
    p.energy_delivered_kwh,
    p.duration_seconds,
    'Remote',
    'COMPLETED',
    'ROAMING',
    'PARTNER',
    TRUE,
    p.partner_code,
    p.total_cost_inr,
    p.business_date_ist,
    :run_id::UUID,
    p.dw_batch_key,
    p.dw_source_row_seq
FROM stg.partner_session AS p
WHERE p.dw_batch_key BETWEEN :batch_lo AND :batch_hi
  AND p.direction = 'OUTBOUND'
  AND NOT p.is_duplicate_source
  -- Defensive against a partner that reports the same session under two
  -- different cdr_ids. The PK on stg.session would reject it loudly, which is
  -- correct but would fail the whole load; skipping it here keeps the load
  -- running and the duplicate is still visible in stg.partner_session.
  AND NOT EXISTS (
      SELECT 1 FROM stg.session AS s WHERE s.transaction_id = p.transaction_id
  );
