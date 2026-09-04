-- ===========================================================================
-- scd2_merge_customer.sql - SCD Type 2 merge for core.dim_customer.
--
-- Parameters: :run_id, :batch_lo, :batch_hi
--
-- Tracked (Type 2): city, state, city_tier, customer_segment,
--                   subscription_plan, kyc_status, is_active, is_deleted
-- Type 1:           full_name_masked, email_domain, signup_date
--
-- A customer relocating from Pune to Bengaluru changes revenue-by-city
-- attribution FROM THAT DATE FORWARD, not retrospectively - they really did
-- spend money in Pune before they moved. A plan upgrade must not retroactively
-- reprice last quarter's sessions. Segment migration (RETAIL becoming
-- CORPORATE as usage grows) is itself a KPI, and it is only measurable if the
-- old segment survives.
--
-- kyc_status is Type 2 for a different reason: it is compliance history.
-- "Was this customer verified at the time of that transaction?" has to remain
-- answerable after they were later verified.
--
-- A SOFT DELETE IS A CHANGE, NOT A DISAPPEARANCE. is_active going false
-- expires the current version and inserts a tombstone, so existing facts keep
-- pointing at the version that was current when they occurred and "how many
-- customers churned in May" stays answerable.
--
-- Note what is NOT here: a full name or an email address. PII minimisation
-- happened in staging - the warehouse holds a masked display name and a domain
-- and nothing else. Raw keeps what arrived and is pruned by retention.
--
-- Algorithm and full commentary: scd2_merge_tariff_plan.sql.
-- ===========================================================================

CREATE TEMP TABLE scd2_src_customer ON COMMIT DROP AS
WITH staged AS (
    SELECT
        s.customer_id                                       AS bk,
        s.source_updated_at_utc                             AS eff_from,
        LEAST(
            COALESCE(s.source_created_at_utc, s.source_updated_at_utc),
            s.source_updated_at_utc
        )                                                   AS genesis_from,
        s.full_name_masked,
        s.email_domain,
        s.city,
        s.state,
        s.city_tier,
        s.customer_segment,
        s.subscription_plan,
        s.kyc_status,
        s.signup_date,
        s.is_active,
        s.is_deleted,
        core.row_hash(
            s.city,
            s.state,
            s.city_tier,
            s.customer_segment,
            s.subscription_plan,
            s.kyc_status,
            s.is_active::TEXT,
            s.is_deleted::TEXT
        )                                                   AS row_hash
    FROM stg.customer AS s
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
        d.customer_id AS bk,
        d.customer_sk AS cur_sk,
        d.effective_from_utc AS cur_from,
        d.row_hash AS cur_hash,
        d.version_no AS cur_version,
        d.is_inferred AS cur_is_inferred
    FROM core.dim_customer AS d
    WHERE d.is_current AND d.customer_sk > 0
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
        d.customer_id AS bk,
        d.source_updated_at_utc AS loaded_from
    FROM core.dim_customer AS d
    WHERE d.customer_sk > 0 AND d.source_updated_at_utc IS NOT NULL
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
    'customers',
    s.bk,
    'SCD_RETRO_DATED_CHANGE',
    'change dated ' || s.eff_from || ' precedes the current version''s '
    || 'effective_from ' || s.cur_from || '; applying it would invert the validity interval',
    to_jsonb(s)
FROM scd2_src_customer AS s
WHERE s.disposition = 'RETRO_DATED'
  AND NOT EXISTS (
      SELECT 1 FROM dq.quarantine_cms_entity AS q
      WHERE q.natural_key = s.bk
        AND q.rule_code = 'SCD_RETRO_DATED_CHANGE'
        AND q.dw_batch_key = :batch_hi::DATE
  );

UPDATE core.dim_customer AS d
SET full_name_masked = p.full_name_masked,
    email_domain = p.email_domain,
    city = p.city,
    state = p.state,
    city_tier = p.city_tier,
    customer_segment = p.customer_segment,
    subscription_plan = p.subscription_plan,
    kyc_status = p.kyc_status,
    signup_date = p.signup_date,
    is_active = p.is_active,
    is_deleted = p.is_deleted,
    row_hash = p.row_hash,
    is_inferred = FALSE,
    source_updated_at_utc = p.eff_from,
    dw_run_id = :run_id::UUID,
    dw_updated_at_utc = now()
FROM (
    SELECT DISTINCT ON (s.bk) s.*
    FROM scd2_src_customer AS s
    WHERE s.cur_is_inferred
    ORDER BY s.bk, s.eff_from
) AS p
WHERE d.customer_sk = p.cur_sk AND d.is_inferred;

UPDATE core.dim_customer AS d
SET effective_to_utc = x.first_change_from,
    is_current = FALSE,
    dw_run_id = :run_id::UUID,
    dw_updated_at_utc = now()
FROM (
    SELECT s.bk, min(s.eff_from) AS first_change_from
    FROM scd2_src_customer AS s
    WHERE s.disposition = 'CHANGED'
    GROUP BY s.bk
) AS x
WHERE d.customer_id = x.bk
  AND d.is_current
  AND x.first_change_from > d.effective_from_utc;

INSERT INTO core.dim_customer (
    customer_id, full_name_masked, email_domain, city, state, city_tier,
    customer_segment, subscription_plan, kyc_status, signup_date, is_active,
    effective_from_utc, effective_to_utc, is_current, version_no, row_hash,
    is_inferred, is_deleted, source_updated_at_utc, dw_run_id
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
    FROM scd2_src_customer AS s
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
    c.full_name_masked,
    c.email_domain,
    c.city,
    c.state,
    c.city_tier,
    c.customer_segment,
    c.subscription_plan,
    c.kyc_status,
    c.signup_date,
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

UPDATE core.dim_customer AS d
SET full_name_masked = latest.full_name_masked,
    email_domain = latest.email_domain,
    signup_date = latest.signup_date,
    dw_updated_at_utc = now()
FROM (
    SELECT DISTINCT ON (s.bk)
        s.bk, s.full_name_masked, s.email_domain, s.signup_date
    FROM scd2_src_customer AS s
    ORDER BY s.bk ASC, s.eff_from DESC
) AS latest
WHERE d.customer_id = latest.bk
  AND d.customer_sk > 0
  AND (
      d.full_name_masked IS DISTINCT FROM latest.full_name_masked
      OR d.email_domain IS DISTINCT FROM latest.email_domain
      OR d.signup_date IS DISTINCT FROM latest.signup_date
  );
