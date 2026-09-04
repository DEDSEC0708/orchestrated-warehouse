-- ===========================================================================
-- stg/02_tariff_plan.sql - conform CMS tariff plans.
--
-- Parameters: :run_id, :batch_lo, :batch_hi
--
-- The most consequential of the master-data loads. Every version preserved
-- here becomes a priced interval in core.dim_tariff_plan, and a session in
-- February must resolve to February's price. Losing one version silently
-- reprices a month of revenue.
--
-- price_per_kwh_inr is the one column that cannot be defaulted. A plan whose
-- price will not parse is QUARANTINED rather than coerced to zero: a session
-- priced at zero looks like a free charge, which is a plausible-looking number
-- and therefore far more dangerous than a missing one.
-- ===========================================================================

DELETE FROM stg.tariff_plan;

DELETE FROM dq.quarantine_cms_entity
WHERE entity = 'tariff_plans'
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
    'tariff_plans',
    r.dw_source_file,
    r.dw_source_row_seq,
    r.tariff_plan_id,
    CASE
        WHEN r.tariff_plan_id IS NULL OR btrim(r.tariff_plan_id) = ''
            THEN 'CMS_NULL_NATURAL_KEY'
        WHEN stg.safe_timestamptz(r.updated_at) IS NULL
            THEN 'CDR_BAD_TIMESTAMP_FORMAT'
        ELSE 'CMS_INVALID_PRICE'
    END,
    CASE
        WHEN r.tariff_plan_id IS NULL OR btrim(r.tariff_plan_id) = ''
            THEN 'tariff_plan_id is null or blank'
        WHEN stg.safe_timestamptz(r.updated_at) IS NULL
            THEN 'updated_at could not be parsed: ' || COALESCE(r.updated_at, '<null>')
        ELSE 'price_per_kwh_inr is missing or not a number: '
             || COALESCE(r.price_per_kwh_inr, '<null>')
    END,
    to_jsonb(r) - 'dw_raw_id'
FROM raw.cms_tariff_plans AS r
WHERE r.dw_batch_key BETWEEN :batch_lo AND :batch_hi
  AND (
      r.tariff_plan_id IS NULL
      OR btrim(r.tariff_plan_id) = ''
      OR stg.safe_timestamptz(r.updated_at) IS NULL
      OR stg.safe_numeric(r.price_per_kwh_inr) IS NULL
      OR stg.safe_numeric(r.price_per_kwh_inr) < 0
  );

INSERT INTO stg.tariff_plan (
    tariff_plan_id, plan_name, price_per_kwh_inr, price_per_minute_inr,
    idle_fee_per_minute_inr, min_billable_kwh, gst_rate_pct, valid_from_date,
    is_active, is_deleted, source_created_at_utc, source_updated_at_utc,
    dw_run_id, dw_batch_key, dw_source_row_seq
)
WITH windowed AS (
    SELECT
        r.*,
        stg.safe_timestamptz(r.updated_at) AS updated_at_utc,
        row_number() OVER (
            PARTITION BY r.tariff_plan_id, stg.safe_timestamptz(r.updated_at)
            ORDER BY r.dw_ingested_at_utc DESC, r.dw_raw_id DESC
        ) AS delivery_rank
    FROM raw.cms_tariff_plans AS r
    WHERE r.dw_batch_key BETWEEN :batch_lo AND :batch_hi
      AND r.tariff_plan_id IS NOT NULL
      AND btrim(r.tariff_plan_id) <> ''
      AND stg.safe_timestamptz(r.updated_at) IS NOT NULL
      AND stg.safe_numeric(r.price_per_kwh_inr) IS NOT NULL
      AND stg.safe_numeric(r.price_per_kwh_inr) >= 0
)

SELECT
    btrim(w.tariff_plan_id),
    nullif(btrim(w.plan_name), ''),
    stg.safe_numeric(w.price_per_kwh_inr),
    -- Optional money columns DO default to zero, unlike the per-kWh price:
    -- "no per-minute component" is the normal case for most plans, so an
    -- absent value genuinely means zero here rather than "unknown".
    COALESCE(stg.safe_numeric(w.price_per_minute_inr), 0),
    COALESCE(stg.safe_numeric(w.idle_fee_per_minute_inr), 0),
    COALESCE(stg.safe_numeric(w.min_billable_kwh), 0),
    COALESCE(stg.safe_numeric(w.gst_rate_pct), 18),
    stg.safe_date(w.valid_from_date),
    COALESCE(stg.safe_bool(w.is_active), TRUE),
    NOT COALESCE(stg.safe_bool(w.is_active), TRUE),
    stg.safe_timestamptz(w.created_at),
    w.updated_at_utc,
    :run_id::UUID,
    w.dw_batch_key,
    w.dw_source_row_seq
FROM windowed AS w
WHERE w.delivery_rank = 1;
