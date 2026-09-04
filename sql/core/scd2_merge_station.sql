-- ===========================================================================
-- scd2_merge_station.sql - SCD Type 2 merge for core.dim_station.
--
-- Parameters: :run_id, :batch_lo, :batch_hi
--
-- Tracked (Type 2): city, state, pincode, site_type, operator_name, num_bays,
--                   is_active, is_deleted
-- Type 1:           station_name, address_line, latitude, longitude
--
-- num_bays is the one that earns this dimension its Type 2 status. It is the
-- DENOMINATOR of every utilisation figure: a site expanded from four bays to
-- eight must be measured against four for last month and eight for this one.
-- Using today's count for both makes a successful expansion look like falling
-- utilisation, which is precisely backwards.
--
-- site_type matters for the same reason at a different grain - reclassifying a
-- mall site as a highway site changes which peer group it is compared against.
--
-- Type 1 for the geocode is deliberate: correcting a latitude that was typed
-- wrong should fix every historical row, because the station never moved. The
-- data about it was wrong. Contrast a device RELOCATION in
-- scd2_merge_charge_point.sql, which is a real change and is Type 2.
--
-- Algorithm and full commentary: scd2_merge_tariff_plan.sql.
-- ===========================================================================

CREATE TEMP TABLE scd2_src_station ON COMMIT DROP AS
WITH staged AS (
    SELECT
        s.station_id                                        AS bk,
        s.source_updated_at_utc                             AS eff_from,
        LEAST(
            COALESCE(s.source_created_at_utc, s.source_updated_at_utc),
            s.source_updated_at_utc
        )                                                   AS genesis_from,
        s.station_name,
        s.address_line,
        s.city,
        s.state,
        s.pincode,
        s.latitude,
        s.longitude,
        s.site_type,
        s.commissioned_date,
        s.num_bays,
        s.operator_name,
        s.is_active,
        s.is_deleted,
        core.row_hash(
            s.city,
            s.state,
            s.pincode,
            s.site_type,
            s.operator_name,
            s.num_bays::TEXT,
            s.is_active::TEXT,
            s.is_deleted::TEXT
        )                                                   AS row_hash
    FROM stg.station AS s
),

with_previous AS (
    SELECT
        st.*,
        lag(st.row_hash) OVER (PARTITION BY st.bk ORDER BY st.eff_from) AS prev_hash,
        row_number() OVER (PARTITION BY st.bk ORDER BY st.eff_from)     AS version_in_batch
    FROM staged AS st
),

changes AS (
    SELECT wp.*
    FROM with_previous AS wp
    WHERE wp.prev_hash IS NULL OR wp.prev_hash <> wp.row_hash
),

existing AS (
    SELECT
        d.station_id AS bk,
        d.station_sk AS cur_sk,
        d.effective_from_utc AS cur_from,
        d.row_hash AS cur_hash,
        d.version_no AS cur_version,
        d.is_inferred AS cur_is_inferred
    FROM core.dim_station AS d
    WHERE d.is_current AND d.station_sk > 0
),

-- Versions of each key that this dimension ALREADY HOLDS, identified by the
-- SOURCE CHANGE TIME they were loaded from. Without this, every historical
-- version in the staging window would be re-examined on the next run, found to
-- predate the current version, and misclassified as a retro-dated change -
-- filling the quarantine table with hundreds of rows that describe nothing but
-- the lookback window doing its job.
--
-- Matched on source_updated_at_utc rather than on effective_from_utc because
-- the two legitimately differ for the first version of a new key, whose
-- validity is backdated to the entity's genesis.
already_loaded AS (
    SELECT DISTINCT
        d.station_id AS bk,
        d.source_updated_at_utc AS loaded_from
    FROM core.dim_station AS d
    WHERE d.station_sk > 0 AND d.source_updated_at_utc IS NOT NULL
)

SELECT
    c.*,
    e.cur_sk,
    e.cur_from,
    e.cur_hash,
    e.cur_version,
    e.cur_is_inferred,
    CASE
        WHEN e.bk IS NULL THEN 'NEW_KEY'
        -- Already in the dimension: this run is simply re-reading history that
        -- the lookback window covers. Not a change, and emphatically not a
        -- retro-dated one.
        WHEN al.loaded_from IS NOT NULL THEN 'ALREADY_APPLIED'
        WHEN c.eff_from > e.cur_from AND c.row_hash <> e.cur_hash THEN 'CHANGED'
        WHEN c.eff_from > e.cur_from THEN 'UNCHANGED'
        WHEN c.row_hash = e.cur_hash THEN 'ALREADY_APPLIED'
        -- Genuinely retro-dated: a change we have never seen, dated before the
        -- current version began. Applying it would invert the validity
        -- interval, so it is refused and reported.
        ELSE 'RETRO_DATED'
    END AS disposition
FROM changes AS c
LEFT JOIN existing AS e ON e.bk = c.bk
LEFT JOIN already_loaded AS al ON al.bk = c.bk AND al.loaded_from = c.eff_from;

INSERT INTO dq.quarantine_cms_entity (
    dw_run_id, dw_batch_key, source_system, entity, natural_key,
    rule_code, rule_detail, raw_payload
)
SELECT
    :run_id::UUID,
    :batch_hi::DATE,
    'CMS',
    'stations',
    s.bk,
    'SCD_RETRO_DATED_CHANGE',
    'change dated ' || s.eff_from || ' precedes the current version''s '
    || 'effective_from ' || s.cur_from || '; applying it would invert the validity interval',
    to_jsonb(s)
FROM scd2_src_station AS s
WHERE s.disposition = 'RETRO_DATED'
  AND NOT EXISTS (
      SELECT 1 FROM dq.quarantine_cms_entity AS q
      WHERE q.natural_key = s.bk
        AND q.rule_code = 'SCD_RETRO_DATED_CHANGE'
        AND q.dw_batch_key = :batch_hi::DATE
  );

UPDATE core.dim_station AS d
SET station_name = p.station_name,
    address_line = p.address_line,
    city = p.city,
    state = p.state,
    pincode = p.pincode,
    latitude = p.latitude,
    longitude = p.longitude,
    site_type = p.site_type,
    commissioned_date = p.commissioned_date,
    num_bays = p.num_bays,
    operator_name = p.operator_name,
    is_active = p.is_active,
    is_deleted = p.is_deleted,
    row_hash = p.row_hash,
    is_inferred = FALSE,
    source_updated_at_utc = p.eff_from,
    dw_run_id = :run_id::UUID,
    dw_updated_at_utc = now()
FROM (
    SELECT DISTINCT ON (s.bk) s.*
    FROM scd2_src_station AS s
    WHERE s.cur_is_inferred
    ORDER BY s.bk, s.eff_from
) AS p
WHERE d.station_sk = p.cur_sk AND d.is_inferred;

UPDATE core.dim_station AS d
SET effective_to_utc = x.first_change_from,
    is_current = FALSE,
    dw_run_id = :run_id::UUID,
    dw_updated_at_utc = now()
FROM (
    SELECT s.bk, min(s.eff_from) AS first_change_from
    FROM scd2_src_station AS s
    WHERE s.disposition = 'CHANGED'
    GROUP BY s.bk
) AS x
WHERE d.station_id = x.bk
  AND d.is_current
  AND x.first_change_from > d.effective_from_utc;

INSERT INTO core.dim_station (
    station_id, station_name, address_line, city, state, pincode,
    latitude, longitude, site_type, operator_name, num_bays, commissioned_date,
    is_active, effective_from_utc, effective_to_utc, is_current, version_no,
    row_hash, is_inferred, is_deleted, source_updated_at_utc, dw_run_id
)
WITH to_insert AS (
    SELECT
        s.*,
        CASE
            WHEN s.disposition = 'NEW_KEY' AND s.version_in_batch = 1
                THEN LEAST(s.genesis_from, s.eff_from)
            ELSE s.eff_from
        END AS effective_from,
        row_number() OVER (PARTITION BY s.bk ORDER BY s.eff_from) AS insert_rank
    FROM scd2_src_station AS s
    WHERE s.disposition IN ('NEW_KEY', 'CHANGED')
),

chained AS (
    SELECT
        t.*,
        lead(t.effective_from) OVER (PARTITION BY t.bk ORDER BY t.effective_from)
        AS next_from
    FROM to_insert AS t
)

SELECT
    c.bk,
    c.station_name,
    c.address_line,
    c.city,
    c.state,
    c.pincode,
    c.latitude,
    c.longitude,
    c.site_type,
    c.operator_name,
    c.num_bays,
    c.commissioned_date,
    c.is_active,
    c.effective_from,
    COALESCE(c.next_from, TIMESTAMPTZ '9999-12-31 00:00:00+00'),
    c.next_from IS NULL,
    COALESCE(c.cur_version, 0) + c.insert_rank,
    c.row_hash,
    FALSE,
    c.is_deleted,
    c.eff_from,
    :run_id::UUID
FROM chained AS c;

UPDATE core.dim_station AS d
SET station_name = latest.station_name,
    address_line = latest.address_line,
    latitude = latest.latitude,
    longitude = latest.longitude,
    dw_updated_at_utc = now()
FROM (
    SELECT DISTINCT ON (s.bk)
        s.bk, s.station_name, s.address_line, s.latitude, s.longitude
    FROM scd2_src_station AS s
    ORDER BY s.bk ASC, s.eff_from DESC
) AS latest
WHERE d.station_id = latest.bk
  AND d.station_sk > 0
  AND (
      d.station_name IS DISTINCT FROM latest.station_name
      OR d.address_line IS DISTINCT FROM latest.address_line
      OR d.latitude IS DISTINCT FROM latest.latitude
      OR d.longitude IS DISTINCT FROM latest.longitude
  );
