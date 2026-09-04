-- ===========================================================================
-- stg/04_vehicle.sql - conform CMS vehicles.
--
-- Parameters: :run_id, :batch_lo, :batch_hi
--
-- The ONLY master-data staging load that collapses to the latest version per
-- key, and the asymmetry is the point. dim_vehicle is SCD Type 1 by deliberate
-- decision: a vehicle's physical attributes do not change, only data
-- corrections do, and correcting a battery-capacity typo should fix every
-- historical row rather than invent a change event that never happened.
--
-- vehicle_class is DERIVED from battery capacity rather than carried from the
-- source, because the source does not have it and because the banding is a
-- business definition that belongs in one place.
-- ===========================================================================

DELETE FROM stg.vehicle;

INSERT INTO stg.vehicle (
    vehicle_id, customer_id, make, model, model_year, battery_capacity_kwh,
    connector_type, registration_state, vehicle_class,
    source_created_at_utc, source_updated_at_utc,
    dw_run_id, dw_batch_key, dw_source_row_seq
)
WITH windowed AS (
    SELECT
        r.*,
        stg.safe_timestamptz(r.updated_at) AS updated_at_utc,
        row_number() OVER (
            PARTITION BY r.vehicle_id
            ORDER BY
                stg.safe_timestamptz(r.updated_at) DESC,
                r.dw_ingested_at_utc DESC,
                r.dw_raw_id DESC
        ) AS version_rank
    FROM raw.cms_vehicles AS r
    WHERE r.dw_batch_key BETWEEN :batch_lo AND :batch_hi
      AND r.vehicle_id IS NOT NULL
      AND btrim(r.vehicle_id) <> ''
      AND stg.safe_timestamptz(r.updated_at) IS NOT NULL
)

SELECT
    btrim(w.vehicle_id),
    nullif(btrim(w.customer_id), ''),
    nullif(btrim(w.make), ''),
    nullif(btrim(w.model), ''),
    stg.safe_int(w.model_year),
    stg.safe_numeric(w.battery_capacity_kwh),
    upper(nullif(btrim(w.connector_type), '')),
    nullif(btrim(w.registration_state), ''),
    CASE
        WHEN stg.safe_numeric(w.battery_capacity_kwh) IS NULL THEN NULL
        WHEN stg.safe_numeric(w.battery_capacity_kwh) < 35 THEN 'HATCH'
        WHEN stg.safe_numeric(w.battery_capacity_kwh) < 50 THEN 'SEDAN'
        WHEN stg.safe_numeric(w.battery_capacity_kwh) < 100 THEN 'SUV'
        ELSE 'COMMERCIAL'
    END,
    stg.safe_timestamptz(w.created_at),
    w.updated_at_utc,
    :run_id::UUID,
    w.dw_batch_key,
    w.dw_source_row_seq
FROM windowed AS w
WHERE w.version_rank = 1;
