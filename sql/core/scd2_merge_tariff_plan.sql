-- ===========================================================================
-- scd2_merge_tariff_plan.sql - SCD Type 2 merge for core.dim_tariff_plan.
--
-- Parameters: :run_id, :batch_lo, :batch_hi
--
-- ===========================================================================
-- WHY THIS DIMENSION EXISTS
-- ===========================================================================
-- VoltHive raised DC fast charging in Bengaluru from Rs 18.50/kWh to
-- Rs 21.00/kWh on 2026-03-01. The CFO asks what February's revenue was. If the
-- dimension is overwritten, the answer silently uses today's price and is
-- wrong - not missing, WRONG, which is far worse because nobody notices.
--
-- SCD Type 2 exists to make historical facts joinable to the attributes that
-- were true WHEN THE FACT HAPPENED. That is the whole justification, and it is
-- a business justification rather than an academic one.
--
-- ===========================================================================
-- THE ALGORITHM
-- ===========================================================================
-- This merge handles N VERSIONS PER KEY IN ONE PASS, set-based, no loop. That
-- matters for two cases that a simpler "compare current row to one new row"
-- merge gets wrong:
--
--   * BOOTSTRAP, where eighteen months of history arrive at once and every
--     intermediate version must become its own interval;
--   * a WIDE RESTATEMENT WINDOW, where several days of changes are reprocessed
--     together.
--
--   0. Materialise the source into a temp table with row hashes and the
--      key's existing current version alongside.
--   1. COLLAPSE consecutive identical hashes - a re-delivery is not a change.
--   2. CLASSIFY each surviving version against what the dimension already
--      holds: ALREADY_APPLIED (this run is re-reading history the lookback
--      window covers), CHANGED, UNCHANGED, NEW_KEY, or RETRO_DATED. Only a
--      version we have NEVER seen, dated before the current one began, is
--      retro-dated - and that one is quarantined rather than applied.
--   3. PROMOTE inferred members in place, keeping their surrogate key.
--   4. EXPIRE the current row at the first surviving new version's timestamp.
--   5. INSERT the surviving versions as a contiguous chain, the last one
--      current.
--   6. TYPE-1 REFRESH across ALL versions of the key, because a correction is
--      retroactive by definition.
--
-- ALL OF IT IS ONE TRANSACTION, owned by the caller. A crash between steps 4
-- and 5 would otherwise leave a business key with ZERO current rows - which
-- the partial unique index would NOT catch, because it prevents two currents,
-- not none. The transaction boundary is the protection; DIM_NO_CURRENT_ROW is
-- the verification.
--
-- effective_from comes from the SOURCE CHANGE TIME, never the load time, so
-- backfilled history is dated correctly rather than all landing on the day the
-- backfill ran.
-- ===========================================================================

CREATE TEMP TABLE scd2_src_tariff_plan ON COMMIT DROP AS
WITH staged AS (
    SELECT
        s.tariff_plan_id                                    AS bk,
        s.source_updated_at_utc                             AS eff_from,
        -- A brand-new key's FIRST version is dated from when the entity came
        -- into existence, not from when we happened to notice it. Without
        -- this, a session that occurred before the first extract that saw the
        -- plan would find no version to resolve against and would fall back to
        -- the UNKNOWN member - losing its price for no good reason.
        LEAST(
            COALESCE(s.source_created_at_utc, s.source_updated_at_utc),
            s.source_updated_at_utc
        )                                                   AS genesis_from,
        s.plan_name,
        s.price_per_kwh_inr,
        s.price_per_minute_inr,
        s.idle_fee_per_minute_inr,
        s.min_billable_kwh,
        s.gst_rate_pct,
        s.is_active,
        s.is_deleted,
        -- THE HASH COVERS TYPE-2 TRACKED COLUMNS ONLY. plan_name is Type 1 and
        -- is deliberately absent: correcting a plan's display name must not
        -- manufacture a price-change event that never happened.
        core.row_hash(
            s.price_per_kwh_inr::TEXT,
            s.price_per_minute_inr::TEXT,
            s.idle_fee_per_minute_inr::TEXT,
            s.min_billable_kwh::TEXT,
            s.gst_rate_pct::TEXT,
            s.is_active::TEXT,
            s.is_deleted::TEXT
        )                                                   AS row_hash
    FROM stg.tariff_plan AS s
),

with_previous AS (
    SELECT
        st.*,
        lag(st.row_hash) OVER (PARTITION BY st.bk ORDER BY st.eff_from) AS prev_hash,
        row_number() OVER (PARTITION BY st.bk ORDER BY st.eff_from)     AS version_in_batch
    FROM staged AS st
),

-- Step 1. A version whose hash equals its predecessor's is a re-delivery or a
-- Type-1-only edit, not a change. Dropping it here is what keeps the version
-- chain honest: version_no counts real changes, not extract events.
changes AS (
    SELECT wp.*
    FROM with_previous AS wp
    WHERE wp.prev_hash IS NULL OR wp.prev_hash <> wp.row_hash
),

existing AS (
    SELECT
        d.tariff_plan_id AS bk,
        d.tariff_plan_sk AS cur_sk,
        d.effective_from_utc AS cur_from,
        d.row_hash AS cur_hash,
        d.version_no AS cur_version,
        d.is_inferred AS cur_is_inferred
    FROM core.dim_tariff_plan AS d
    WHERE d.is_current AND d.tariff_plan_sk > 0
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
        d.tariff_plan_id AS bk,
        d.source_updated_at_utc AS loaded_from
    FROM core.dim_tariff_plan AS d
    WHERE d.tariff_plan_sk > 0 AND d.source_updated_at_utc IS NOT NULL
)

SELECT
    c.*,
    e.cur_sk,
    e.cur_from,
    e.cur_hash,
    e.cur_version,
    e.cur_is_inferred,
    -- Step 2's classification, computed once here so every subsequent
    -- statement agrees about what each row is.
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

-- --------------------------------------------------------------------------
-- Step 2b. Quarantine retro-dated changes.
--
-- A change dated EARLIER than the current version's effective_from cannot be
-- applied without inverting the validity interval. Expiring the current row at
-- a timestamp before its own start would produce effective_to < effective_from
-- - which the CHECK constraint would reject, aborting the whole load.
--
-- So the row is refused and recorded as a WARNING for manual repair. It is
-- repairable precisely BECAUSE raw is immutable: the key's full history can be
-- replayed from raw in updated_at order. See docs/runbook.md.
-- --------------------------------------------------------------------------
INSERT INTO dq.quarantine_cms_entity (
    dw_run_id, dw_batch_key, source_system, entity, natural_key,
    rule_code, rule_detail, raw_payload
)
SELECT
    :run_id::UUID,
    :batch_hi::DATE,
    'CMS',
    'tariff_plans',
    s.bk,
    'SCD_RETRO_DATED_CHANGE',
    'change dated ' || s.eff_from || ' precedes the current version''s '
    || 'effective_from ' || s.cur_from || '; applying it would invert the validity interval',
    to_jsonb(s)
FROM scd2_src_tariff_plan AS s
WHERE s.disposition = 'RETRO_DATED'
  AND NOT EXISTS (
      SELECT 1 FROM dq.quarantine_cms_entity AS q
      WHERE q.natural_key = s.bk
        AND q.rule_code = 'SCD_RETRO_DATED_CHANGE'
        AND q.dw_batch_key = :batch_hi::DATE
  );

-- --------------------------------------------------------------------------
-- Step 3. Promote inferred members IN PLACE.
--
-- An inferred member was created by a fact load for a key the CMS had not yet
-- reported. When the real record arrives, its attributes are filled in ON THE
-- SAME SURROGATE KEY - so every fact already pointing at it becomes correct
-- instantly, with no fact rewrite at all. That property is the entire reason
-- inferred members are worth having rather than just nulling the foreign key.
--
-- The hash is set to the promoting version's hash, so step 5 correctly treats
-- that version as already applied and inserts only what came after it.
-- --------------------------------------------------------------------------
UPDATE core.dim_tariff_plan AS d
SET plan_name = p.plan_name,
    price_per_kwh_inr = p.price_per_kwh_inr,
    price_per_minute_inr = p.price_per_minute_inr,
    idle_fee_per_minute_inr = p.idle_fee_per_minute_inr,
    min_billable_kwh = p.min_billable_kwh,
    gst_rate_pct = p.gst_rate_pct,
    is_active = p.is_active,
    is_deleted = p.is_deleted,
    row_hash = p.row_hash,
    is_inferred = FALSE,
    source_updated_at_utc = p.eff_from,
    dw_run_id = :run_id::UUID,
    dw_updated_at_utc = now()
FROM (
    SELECT DISTINCT ON (s.bk) s.*
    FROM scd2_src_tariff_plan AS s
    WHERE s.cur_is_inferred
    ORDER BY s.bk, s.eff_from
) AS p
WHERE d.tariff_plan_sk = p.cur_sk AND d.is_inferred;

-- --------------------------------------------------------------------------
-- Step 4. Expire the current row of every key that has a real change.
--
-- Expired at the FIRST surviving new version's timestamp, so the old version's
-- validity ends exactly where the new one begins. Half-open intervals mean no
-- gap and no overlap, and no microsecond arithmetic anywhere.
-- --------------------------------------------------------------------------
UPDATE core.dim_tariff_plan AS d
SET effective_to_utc = x.first_change_from,
    is_current = FALSE,
    dw_run_id = :run_id::UUID,
    dw_updated_at_utc = now()
FROM (
    SELECT s.bk, min(s.eff_from) AS first_change_from
    FROM scd2_src_tariff_plan AS s
    WHERE s.disposition = 'CHANGED'
    GROUP BY s.bk
) AS x
WHERE d.tariff_plan_id = x.bk
  AND d.is_current
  -- The guard that makes this safe: never expire a row at or before its own
  -- start. RETRO_DATED rows are already excluded above, but a defence in the
  -- statement that would actually corrupt the data costs one line.
  AND x.first_change_from > d.effective_from_utc;

-- --------------------------------------------------------------------------
-- Step 5. Insert the surviving versions as a contiguous chain.
--
-- LEAD() supplies each version's end from the next version's start, so a batch
-- carrying five changes for one key produces five correctly-abutting intervals
-- in one statement rather than five round trips. The last version of each key
-- gets the open sentinel and is_current = TRUE.
-- --------------------------------------------------------------------------
INSERT INTO core.dim_tariff_plan (
    tariff_plan_id, plan_name, price_per_kwh_inr, price_per_minute_inr,
    idle_fee_per_minute_inr, min_billable_kwh, gst_rate_pct, is_active,
    effective_from_utc, effective_to_utc, is_current, version_no, row_hash,
    is_inferred, is_deleted, source_updated_at_utc, dw_run_id
)
WITH to_insert AS (
    SELECT
        s.*,
        -- A brand-new key's first version starts at the entity's genesis, not
        -- at the moment the extract noticed it.
        CASE
            WHEN s.disposition = 'NEW_KEY' AND s.version_in_batch = 1
                THEN LEAST(s.genesis_from, s.eff_from)
            ELSE s.eff_from
        END AS effective_from,
        row_number() OVER (PARTITION BY s.bk ORDER BY s.eff_from) AS insert_rank
    FROM scd2_src_tariff_plan AS s
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
    c.plan_name,
    c.price_per_kwh_inr,
    c.price_per_minute_inr,
    c.idle_fee_per_minute_inr,
    c.min_billable_kwh,
    c.gst_rate_pct,
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

-- --------------------------------------------------------------------------
-- Step 6. Type-1 refresh, applied to ALL versions of the key.
--
-- This is the hybrid part - Type 1 and Type 2 attributes coexisting in one
-- table - and the behaviour is the whole point: correcting a misspelled plan
-- name fixes every historical version in place, because the name was always
-- wrong rather than ever different. Changing its price creates a new version,
-- because the price genuinely WAS different.
--
-- Values come from the key's LATEST staged version: the most recent correction
-- wins.
-- --------------------------------------------------------------------------
UPDATE core.dim_tariff_plan AS d
SET plan_name = latest.plan_name,
    dw_updated_at_utc = now()
FROM (
    SELECT DISTINCT ON (s.bk) s.bk, s.plan_name
    FROM scd2_src_tariff_plan AS s
    ORDER BY s.bk ASC, s.eff_from DESC
) AS latest
WHERE d.tariff_plan_id = latest.bk
  AND d.tariff_plan_sk > 0
  AND d.plan_name IS DISTINCT FROM latest.plan_name;
