-- ===========================================================================
-- scd2_merge_charge_point.sql - SCD Type 2 merge for core.dim_charge_point.
--
-- Parameters: :run_id, :batch_lo, :batch_hi
--
-- The flagship dimension. Charge point CP-BLR-0117 is upgraded from 30 kW to
-- 60 kW on 2026-04-15 and moved to the premium tariff. Two sessions 74 minutes
-- apart then resolve to DIFFERENT hardware and DIFFERENT prices - which is the
-- entire value proposition of SCD Type 2 in one screenshot.
--
-- Tracked (Type 2): station_id, current_type, rated_power_kw, connector_type,
--                   tariff_plan_id, firmware_version, status, is_deleted
-- Type 1:           oem_vendor, model
--
-- station_id is Type 2 because a device can be RELOCATED between sites, and
-- attributing its old sessions to its new station would move revenue between
-- locations retrospectively. tariff_plan_id is Type 2 because it decides what
-- the customer was actually charged. firmware_version is Type 2 because defect
-- rates correlate with it, which makes "which firmware produces bad CDRs?" an
-- answerable question rather than a hunch.
--
-- The algorithm is identical to scd2_merge_tariff_plan.sql, which carries the
-- full commentary. The one addition here is that this dimension receives
-- INFERRED MEMBERS from the fact load - devices that reported a session before
-- the CMS extract had ever heard of them - so step 3 does real work rather
-- than being a formality.
-- ===========================================================================

CREATE TEMP TABLE scd2_src_charge_point ON COMMIT DROP AS
WITH staged AS (
    SELECT
        s.charge_point_id                                   AS bk,
        s.source_updated_at_utc                             AS eff_from,
        LEAST(
            COALESCE(s.source_created_at_utc, s.source_updated_at_utc),
            s.source_updated_at_utc
        )                                                   AS genesis_from,
        s.station_id,
        s.oem_vendor,
        s.model,
        s.current_type,
        s.rated_power_kw,
        s.connector_type,
        s.tariff_plan_id,
        s.firmware_version,
        s.status,
        s.commissioned_date,
        s.is_deleted,
        core.row_hash(
            s.station_id,
            s.current_type,
            s.rated_power_kw::TEXT,
            s.connector_type,
            s.tariff_plan_id,
            s.firmware_version,
            s.status,
            s.is_deleted::TEXT
        )                                                   AS row_hash
    FROM stg.charge_point AS s
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
        d.charge_point_id AS bk,
        d.charge_point_sk AS cur_sk,
        d.effective_from_utc AS cur_from,
        d.row_hash AS cur_hash,
        d.version_no AS cur_version,
        d.is_inferred AS cur_is_inferred
    FROM core.dim_charge_point AS d
    WHERE d.is_current AND d.charge_point_sk > 0
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
        d.charge_point_id AS bk,
        d.source_updated_at_utc AS loaded_from
    FROM core.dim_charge_point AS d
    WHERE d.charge_point_sk > 0 AND d.source_updated_at_utc IS NOT NULL
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
    'charge_points',
    s.bk,
    'SCD_RETRO_DATED_CHANGE',
    'change dated ' || s.eff_from || ' precedes the current version''s '
    || 'effective_from ' || s.cur_from || '; applying it would invert the validity interval',
    to_jsonb(s)
FROM scd2_src_charge_point AS s
WHERE s.disposition = 'RETRO_DATED'
  AND NOT EXISTS (
      SELECT 1 FROM dq.quarantine_cms_entity AS q
      WHERE q.natural_key = s.bk
        AND q.rule_code = 'SCD_RETRO_DATED_CHANGE'
        AND q.dw_batch_key = :batch_hi::DATE
  );

-- Promote inferred members in place, KEEPING THE SURROGATE KEY. Every session
-- already pointing at the placeholder becomes correct the moment this runs -
-- no fact rewrite, no reload, no window to restate.
UPDATE core.dim_charge_point AS d
SET station_id = p.station_id,
    oem_vendor = p.oem_vendor,
    model = p.model,
    current_type = p.current_type,
    rated_power_kw = p.rated_power_kw,
    connector_type = p.connector_type,
    tariff_plan_id = p.tariff_plan_id,
    firmware_version = p.firmware_version,
    status = p.status,
    commissioned_date = p.commissioned_date,
    is_deleted = p.is_deleted,
    row_hash = p.row_hash,
    is_inferred = FALSE,
    source_updated_at_utc = p.eff_from,
    dw_run_id = :run_id::UUID,
    dw_updated_at_utc = now()
FROM (
    SELECT DISTINCT ON (s.bk) s.*
    FROM scd2_src_charge_point AS s
    WHERE s.cur_is_inferred
    ORDER BY s.bk, s.eff_from
) AS p
WHERE d.charge_point_sk = p.cur_sk AND d.is_inferred;

UPDATE core.dim_charge_point AS d
SET effective_to_utc = x.first_change_from,
    is_current = FALSE,
    dw_run_id = :run_id::UUID,
    dw_updated_at_utc = now()
FROM (
    SELECT s.bk, min(s.eff_from) AS first_change_from
    FROM scd2_src_charge_point AS s
    WHERE s.disposition = 'CHANGED'
    GROUP BY s.bk
) AS x
WHERE d.charge_point_id = x.bk
  AND d.is_current
  AND x.first_change_from > d.effective_from_utc;

INSERT INTO core.dim_charge_point (
    charge_point_id, station_id, oem_vendor, model, current_type,
    rated_power_kw, connector_type, tariff_plan_id, firmware_version, status,
    commissioned_date, effective_from_utc, effective_to_utc, is_current,
    version_no, row_hash, is_inferred, is_deleted, source_updated_at_utc, dw_run_id
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
    FROM scd2_src_charge_point AS s
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
    c.station_id,
    c.oem_vendor,
    c.model,
    c.current_type,
    c.rated_power_kw,
    c.connector_type,
    c.tariff_plan_id,
    c.firmware_version,
    c.status,
    c.commissioned_date,
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

-- Type-1 refresh. Correcting a catalogue entry for the OEM or model fixes
-- every historical version, because the device was always that model - the
-- record of it was simply wrong.
UPDATE core.dim_charge_point AS d
SET oem_vendor = latest.oem_vendor,
    model = latest.model,
    dw_updated_at_utc = now()
FROM (
    SELECT DISTINCT ON (s.bk) s.bk, s.oem_vendor, s.model
    FROM scd2_src_charge_point AS s
    ORDER BY s.bk ASC, s.eff_from DESC
) AS latest
WHERE d.charge_point_id = latest.bk
  AND d.charge_point_sk > 0
  AND (
      d.oem_vendor IS DISTINCT FROM latest.oem_vendor
      OR d.model IS DISTINCT FROM latest.model
  );

-- Denormalised convenience pointer to the station's CURRENT version. Facts
-- never use it - they resolve the station point-in-time themselves - but a
-- human browsing the dimension should not have to write a range join to see
-- where a device lives today.
UPDATE core.dim_charge_point AS d
SET station_sk_current = st.station_sk
FROM core.dim_station AS st
WHERE st.station_id = d.station_id
  AND st.is_current
  AND d.station_sk_current IS DISTINCT FROM st.station_sk;
