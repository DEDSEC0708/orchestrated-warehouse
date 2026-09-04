-- ===========================================================================
-- stg/22_meter_orphan_expiry.sql - quarantine samples whose session never came.
--
-- Parameters: :run_id, :batch_lo, :batch_hi, :lookback_days
--
-- THE LATE-ARRIVING RELATIONSHIP, and the file that makes the hold honest.
--
-- A meter sample whose session record has not arrived is NOT an error. The
-- session may still be open, or its charge detail record may simply be in
-- tomorrow's file. Rejecting it on sight would quarantine data that is about
-- to become perfectly valid, and a quarantine table full of records that
-- resolved themselves is a quarantine table nobody reads.
--
-- So orphans are HELD. The lookback window re-reads the partition on the next
-- run, and by then the record has usually arrived and the sample becomes an
-- interval normally.
--
-- But a hold with no limit is a leak. A sample whose session NEVER arrives
-- would sit unexamined for ever, and the energy it represents would silently
-- never appear anywhere. So once the sample is older than the lookback window
-- - the point past which no further file will bring its session - it is
-- quarantined as MTR_ORPHAN_EXPIRED.
--
-- "We hold late-arriving relationships, the hold has a limit, and here is what
-- happens at the limit" is the mature position. Holding for ever, or
-- rejecting immediately, are both easier and both wrong.
-- ===========================================================================

INSERT INTO dq.quarantine_meter_value (
    dw_run_id, dw_batch_key, source_system, source_file, source_row_seq,
    natural_key, rule_code, rule_detail, raw_payload
)
SELECT
    :run_id::UUID,
    m.dw_batch_key,
    'METER',
    NULL,
    m.dw_source_row_seq,
    m.sample_id,
    'MTR_ORPHAN_EXPIRED',
    'no session for transaction_id ' || m.transaction_id
    || ' after the ' || :lookback_days || '-day lookback expired'
    || ' (sample taken ' || m.sample_timestamp_utc || ')',
    jsonb_build_object(
        'sample_id', m.sample_id,
        'transaction_id', m.transaction_id,
        'charge_point_id', m.charge_point_id,
        'sample_timestamp_utc', m.sample_timestamp_utc,
        'energy_register_wh', m.energy_register_wh,
        'soc_percent', m.soc_percent
    )
FROM stg.meter_sample AS m
WHERE m.dw_batch_key BETWEEN :batch_lo AND :batch_hi
  -- Older than the lookback: no future file can still bring its session.
  AND m.business_date_ist < (:batch_hi::DATE - (:lookback_days || ' days')::INTERVAL)
  AND NOT EXISTS (
      SELECT 1 FROM stg.session AS s WHERE s.transaction_id = m.transaction_id
  )
  -- The fact table is checked as well as staging: a session loaded on an
  -- EARLIER run is no longer in the (window-scoped) staging table but is
  -- absolutely still a valid parent. Missing this check would quarantine
  -- every sample whose session happened to load yesterday.
  AND NOT EXISTS (
      SELECT 1 FROM core.fact_charging_session AS f
      WHERE f.transaction_id = m.transaction_id
  )
  -- Idempotency: a rerun must not add a second quarantine row for the same
  -- sample. Unlike the other staging loads this one cannot simply delete and
  -- rebuild its window, because a sample expires on the run that notices it,
  -- not on the run that staged it.
  AND NOT EXISTS (
      SELECT 1 FROM dq.quarantine_meter_value AS q
      WHERE q.natural_key = m.sample_id AND q.rule_code = 'MTR_ORPHAN_EXPIRED'
  );
