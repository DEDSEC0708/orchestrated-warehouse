-- ===========================================================================
-- stg/00_customer.sql - conform CMS customers.
--
-- Parameters: :run_id, :batch_lo, :batch_hi
--
-- WINDOW-SCOPED REBUILD. The whole table is deleted and rebuilt from raw for
-- the restatement window, so re-running produces identical contents rather
-- than duplicates. That is staging's entire idempotency story, and it is why
-- the natural key can be a real PRIMARY KEY here but not in raw.
--
-- EVERY VERSION IN THE WINDOW IS KEPT, not just the latest per customer. This
-- is the single most important line in the file: staging for an SCD Type 2
-- dimension that collapsed to current state would destroy exactly the history
-- the dimension exists to record. Multiple changes at the SAME instant are
-- collapsed (that is a re-delivery, not two changes); changes at different
-- instants are all preserved and become separate versions.
--
-- CONFORMANCE, NOT REJECTION, where the value is recoverable. Casing and
-- whitespace on code columns are normalised - "Fleet", " FLEET " and "FLEET"
-- are the same segment, and rejecting 2% of perfectly good rows over a
-- capitalisation difference would be a quality layer doing harm.
--
-- PII MINIMISATION happens here, at the boundary between evidence and
-- warehouse: raw keeps what arrived and is pruned by retention; the warehouse
-- stores a masked display name and an email domain, and never the full values.
-- ===========================================================================

DELETE FROM stg.customer;

DELETE FROM dq.quarantine_cms_entity
WHERE entity = 'customers'
  AND dw_batch_key BETWEEN :batch_lo AND :batch_hi
  -- Only untriaged rows are cleared. A row a human has already triaged, or
  -- requeued, is a record of work done and must survive a reprocess.
  AND status = 'NEW';

-- --------------------------------------------------------------------------
-- 1. Quarantine the rows that cannot be conformed.
--
-- Three distinct failures, three distinct rule codes, because "we could not
-- read it" and "we read it and it was not allowed" are different problems with
-- different owners.
-- --------------------------------------------------------------------------
INSERT INTO dq.quarantine_cms_entity (
    dw_run_id, dw_batch_key, source_system, entity, source_file, source_row_seq,
    natural_key, rule_code, rule_detail, raw_payload
)
SELECT
    :run_id::UUID,
    r.dw_batch_key,
    'CMS',
    'customers',
    r.dw_source_file,
    r.dw_source_row_seq,
    r.customer_id,
    CASE
        WHEN r.customer_id IS NULL OR btrim(r.customer_id) = '' THEN 'CMS_NULL_NATURAL_KEY'
        WHEN stg.safe_timestamptz(r.updated_at) IS NULL THEN 'CDR_BAD_TIMESTAMP_FORMAT'
        ELSE 'CMS_INVALID_SEGMENT'
    END,
    CASE
        WHEN r.customer_id IS NULL OR btrim(r.customer_id) = ''
            THEN 'customer_id is null or blank'
        WHEN stg.safe_timestamptz(r.updated_at) IS NULL
            THEN 'updated_at could not be parsed as an ISO 8601 instant: ' || COALESCE(r.updated_at, '<null>')
        ELSE 'customer_segment not in (RETAIL, FLEET, CORPORATE): ' || COALESCE(r.customer_segment, '<null>')
    END,
    to_jsonb(r) - 'dw_raw_id'
FROM raw.cms_customers AS r
WHERE r.dw_batch_key BETWEEN :batch_lo AND :batch_hi
  AND (
      r.customer_id IS NULL
      OR btrim(r.customer_id) = ''
      OR stg.safe_timestamptz(r.updated_at) IS NULL
      OR upper(btrim(COALESCE(r.customer_segment, ''))) NOT IN ('RETAIL', 'FLEET', 'CORPORATE')
  );

-- --------------------------------------------------------------------------
-- 2. Insert everything that survived.
-- --------------------------------------------------------------------------
INSERT INTO stg.customer (
    customer_id, full_name_masked, email_domain, city, state, city_tier,
    customer_segment, subscription_plan, kyc_status, signup_date,
    is_active, is_deleted, source_created_at_utc, source_updated_at_utc,
    dw_run_id, dw_batch_key, dw_source_row_seq
)
WITH windowed AS (
    SELECT
        r.customer_id,
        r.full_name,
        r.email,
        r.city,
        r.state,
        r.customer_segment,
        r.subscription_plan,
        r.kyc_status,
        r.signup_date,
        r.is_active,
        r.created_at,
        r.dw_batch_key,
        r.dw_source_row_seq,
        stg.safe_timestamptz(r.updated_at) AS updated_at_utc,
        -- Collapse a re-delivery of the SAME instant, keeping the most
        -- recently landed copy. This is NOT collapsing distinct changes: two
        -- rows with different updated_at values are two versions and both
        -- survive.
        row_number() OVER (
            PARTITION BY r.customer_id, stg.safe_timestamptz(r.updated_at)
            ORDER BY r.dw_ingested_at_utc DESC, r.dw_raw_id DESC
        ) AS delivery_rank
    FROM raw.cms_customers AS r
    WHERE r.dw_batch_key BETWEEN :batch_lo AND :batch_hi
      AND r.customer_id IS NOT NULL
      AND btrim(r.customer_id) <> ''
      AND stg.safe_timestamptz(r.updated_at) IS NOT NULL
      AND upper(btrim(COALESCE(r.customer_segment, ''))) IN ('RETAIL', 'FLEET', 'CORPORATE')
)

SELECT
    btrim(w.customer_id),
    stg.mask_name(w.full_name),
    stg.email_domain(w.email),
    nullif(btrim(w.city), ''),
    nullif(btrim(w.state), ''),
    -- City tier is DERIVED here rather than carried from the source, because
    -- the source does not have it and because "which cities are Tier 1" is a
    -- business definition that should exist in exactly one place.
    CASE
        WHEN nullif(btrim(w.city), '') IS NULL THEN NULL
        WHEN btrim(w.city) IN (
            'Bengaluru', 'Delhi NCR', 'Mumbai', 'Hyderabad', 'Chennai', 'Kolkata'
        ) THEN 'TIER1'
        ELSE 'TIER2'
    END,
    upper(btrim(w.customer_segment)),
    upper(btrim(COALESCE(w.subscription_plan, 'PAYG'))),
    upper(btrim(COALESCE(w.kyc_status, 'PENDING'))),
    stg.safe_date(w.signup_date),
    COALESCE(stg.safe_bool(w.is_active), TRUE),
    -- A soft delete is a CHANGE, not a disappearance. It becomes a tombstone
    -- version in the dimension, so history stays intact and "how many
    -- customers churned in May" remains answerable.
    NOT COALESCE(stg.safe_bool(w.is_active), TRUE),
    stg.safe_timestamptz(w.created_at),
    w.updated_at_utc,
    :run_id::UUID,
    w.dw_batch_key,
    w.dw_source_row_seq
FROM windowed AS w
WHERE w.delivery_rank = 1;
