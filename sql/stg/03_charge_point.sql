-- ===========================================================================
-- stg/03_charge_point.sql - conform CMS charge points.
--
-- Parameters: :run_id, :batch_lo, :batch_hi
--
-- The flagship SCD Type 2 source. Every version here becomes a hardware and
-- pricing interval: the 30 kW to 60 kW upgrade, the firmware change that
-- correlates with defect rates, the move to a different tariff plan.
--
-- Note what is deliberately NOT validated: a tariff_plan_id that does not
-- exist. Real operational systems accumulate dangling references through bad
-- admin edits, and the generator injects exactly that. Quarantining the device
-- would lose every session on it; instead the reference is carried through and
-- the FACT load resolves it to the UNKNOWN tariff member, where the
-- FACT_UNKNOWN_MEMBER_RATIO rule makes it visible. Deliberately visible beats
-- quietly wrong.
-- ===========================================================================

DELETE FROM stg.charge_point;

DELETE FROM dq.quarantine_cms_entity
WHERE entity = 'charge_points'
  AND dw_batch_key BETWEEN :batch_lo AND :batch_hi
  AND status = 'NEW';

INSERT INTO dq.quarantine_cms_entity (
    dw_run_id, dw_batch_key, source_system, entity, source_file, source_row_seq,
    natural_key, rule_code, rule_detail, raw_payload
)
SELECT
    :run_id::UUID,
    r.dw_batch_key,
    'CMS',
    'charge_points',
    r.dw_source_file,
    r.dw_source_row_seq,
    r.charge_point_id,
    CASE
        WHEN r.charge_point_id IS NULL OR btrim(r.charge_point_id) = ''
            THEN 'CMS_NULL_NATURAL_KEY'
        ELSE 'CDR_BAD_TIMESTAMP_FORMAT'
    END,
    CASE
        WHEN r.charge_point_id IS NULL OR btrim(r.charge_point_id) = ''
            THEN 'charge_point_id is null or blank'
        ELSE 'updated_at could not be parsed: ' || COALESCE(r.updated_at, '<null>')
    END,
    to_jsonb(r) - 'dw_raw_id'
FROM raw.cms_charge_points AS r
WHERE r.dw_batch_key BETWEEN :batch_lo AND :batch_hi
  AND (
      r.charge_point_id IS NULL
      OR btrim(r.charge_point_id) = ''
      OR stg.safe_timestamptz(r.updated_at) IS NULL
  );

INSERT INTO stg.charge_point (
    charge_point_id, station_id, oem_vendor, model, current_type,
    rated_power_kw, connector_type, tariff_plan_id, firmware_version, status,
    commissioned_date, is_deleted, source_created_at_utc, source_updated_at_utc,
    dw_run_id, dw_batch_key, dw_source_row_seq
)
WITH windowed AS (
    SELECT
        r.*,
        stg.safe_timestamptz(r.updated_at) AS updated_at_utc,
        row_number() OVER (
            PARTITION BY r.charge_point_id, stg.safe_timestamptz(r.updated_at)
            ORDER BY r.dw_ingested_at_utc DESC, r.dw_raw_id DESC
        ) AS delivery_rank
    FROM raw.cms_charge_points AS r
    WHERE r.dw_batch_key BETWEEN :batch_lo AND :batch_hi
      AND r.charge_point_id IS NOT NULL
      AND btrim(r.charge_point_id) <> ''
      AND stg.safe_timestamptz(r.updated_at) IS NOT NULL
)

SELECT
    btrim(w.charge_point_id),
    nullif(btrim(w.station_id), ''),
    nullif(btrim(w.oem_vendor), ''),
    nullif(btrim(w.model), ''),
    upper(nullif(btrim(w.current_type), '')),
    stg.safe_numeric(w.rated_power_kw),
    upper(nullif(btrim(w.connector_type), '')),
    nullif(btrim(w.tariff_plan_id), ''),
    nullif(btrim(w.firmware_version), ''),
    upper(COALESCE(nullif(btrim(w.status), ''), 'ACTIVE')),
    stg.safe_date(w.commissioned_date),
    upper(COALESCE(nullif(btrim(w.status), ''), 'ACTIVE')) = 'DECOMMISSIONED',
    stg.safe_timestamptz(w.created_at),
    w.updated_at_utc,
    :run_id::UUID,
    w.dw_batch_key,
    w.dw_source_row_seq
FROM windowed AS w
WHERE w.delivery_rank = 1;
