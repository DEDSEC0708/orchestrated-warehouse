-- ===========================================================================
-- stg/05_grid_tariff_slab.sql - conform source S5, the grid tariff reference.
--
-- Parameters: :run_id, :batch_lo, :batch_hi
--
-- Sixty rows of hand-authored reference data, loaded as a FULL SNAPSHOT. This
-- is the one source that is deliberately not incremental, and having it is
-- what makes "incremental everywhere" visibly a choice rather than a reflex.
--
-- Its purpose is to turn gross_margin_inr into a real derived measure: what
-- the customer paid, minus what VoltHive paid the grid for the same energy in
-- the same state on the same date. Without it, "margin" would just be revenue
-- under another name.
-- ===========================================================================

DELETE FROM stg.grid_tariff_slab;

INSERT INTO stg.grid_tariff_slab (
    state, effective_from_date, effective_to_date, slab_name,
    commercial_rate_inr_per_kwh, source_note, dw_run_id, dw_batch_key
)
SELECT DISTINCT ON (btrim(r.state), stg.safe_date(r.effective_from_date))
    btrim(r.state),
    stg.safe_date(r.effective_from_date),
    stg.safe_date(r.effective_to_date),
    nullif(btrim(r.slab_name), ''),
    stg.safe_numeric(r.commercial_rate_inr_per_kwh),
    r.source_note,
    :run_id::UUID,
    r.dw_batch_key
FROM raw.grid_tariff_slab AS r
WHERE stg.safe_date(r.effective_from_date) IS NOT NULL
  AND stg.safe_date(r.effective_to_date) IS NOT NULL
  AND stg.safe_numeric(r.commercial_rate_inr_per_kwh) IS NOT NULL
  AND stg.safe_date(r.effective_to_date) > stg.safe_date(r.effective_from_date)
ORDER BY
    btrim(r.state),
    stg.safe_date(r.effective_from_date),
    r.dw_raw_id DESC;
