-- ===========================================================================
-- stg/01_station.sql - conform CMS stations.
--
-- Parameters: :run_id, :batch_lo, :batch_hi
--
-- Same shape as the customer load: window-scoped rebuild, every version in the
-- window preserved (SCD Type 2 source), same-instant re-deliveries collapsed.
--
-- num_bays matters more than it looks: it is the denominator of every
-- utilisation figure in the mart. A station expanded from four bays to eight
-- must be compared against four for last month and eight for this one, which
-- is why the attribute is Type 2 and why staging must not collapse its
-- history.
-- ===========================================================================

DELETE FROM stg.station;

DELETE FROM dq.quarantine_cms_entity
WHERE entity = 'stations'
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
    'stations',
    r.dw_source_file,
    r.dw_source_row_seq,
    r.station_id,
    CASE
        WHEN r.station_id IS NULL OR btrim(r.station_id) = '' THEN 'CMS_NULL_NATURAL_KEY'
        ELSE 'CDR_BAD_TIMESTAMP_FORMAT'
    END,
    CASE
        WHEN r.station_id IS NULL OR btrim(r.station_id) = ''
            THEN 'station_id is null or blank'
        ELSE 'updated_at could not be parsed: ' || COALESCE(r.updated_at, '<null>')
    END,
    to_jsonb(r) - 'dw_raw_id'
FROM raw.cms_stations AS r
WHERE r.dw_batch_key BETWEEN :batch_lo AND :batch_hi
  AND (
      r.station_id IS NULL
      OR btrim(r.station_id) = ''
      OR stg.safe_timestamptz(r.updated_at) IS NULL
  );

INSERT INTO stg.station (
    station_id, station_name, address_line, city, state, pincode,
    latitude, longitude, site_type, commissioned_date, num_bays, operator_name,
    is_active, is_deleted, source_created_at_utc, source_updated_at_utc,
    dw_run_id, dw_batch_key, dw_source_row_seq
)
WITH windowed AS (
    SELECT
        r.*,
        stg.safe_timestamptz(r.updated_at) AS updated_at_utc,
        row_number() OVER (
            PARTITION BY r.station_id, stg.safe_timestamptz(r.updated_at)
            ORDER BY r.dw_ingested_at_utc DESC, r.dw_raw_id DESC
        ) AS delivery_rank
    FROM raw.cms_stations AS r
    WHERE r.dw_batch_key BETWEEN :batch_lo AND :batch_hi
      AND r.station_id IS NOT NULL
      AND btrim(r.station_id) <> ''
      AND stg.safe_timestamptz(r.updated_at) IS NOT NULL
)

SELECT
    btrim(w.station_id),
    nullif(btrim(w.station_name), ''),
    nullif(btrim(w.address_line), ''),
    nullif(btrim(w.city), ''),
    nullif(btrim(w.state), ''),
    nullif(btrim(w.pincode), ''),
    stg.safe_numeric(w.latitude),
    stg.safe_numeric(w.longitude),
    upper(nullif(btrim(w.site_type), '')),
    stg.safe_date(w.commissioned_date),
    stg.safe_int(w.num_bays),
    nullif(btrim(w.operator_name), ''),
    COALESCE(stg.safe_bool(w.is_active), TRUE),
    NOT COALESCE(stg.safe_bool(w.is_active), TRUE),
    stg.safe_timestamptz(w.created_at),
    w.updated_at_utc,
    :run_id::UUID,
    w.dw_batch_key,
    w.dw_source_row_seq
FROM windowed AS w
WHERE w.delivery_rank = 1;
