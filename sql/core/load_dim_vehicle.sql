-- ===========================================================================
-- load_dim_vehicle.sql - SCD Type 1 load for core.dim_vehicle.
--
-- Parameters: :run_id, :batch_lo, :batch_hi
--
-- ONE STATEMENT, and that is the point: choosing Type 1 deliberately, and
-- being able to say why, is a stronger signal than making every dimension
-- Type 2 because Type 2 sounds more advanced.
--
-- WHY TYPE 1 HERE. A vehicle's physical attributes - make, model, battery
-- capacity, connector - do not change. Only data corrections do, and
-- correcting a battery-capacity typo should retroactively fix every session
-- that referenced it rather than manufacture a "the battery got bigger" event
-- that never happened.
--
-- Ownership CAN change on resale, which is the obvious objection. The answer
-- is that VoltHive attributes a session to the CUSTOMER ON THE SESSION, not to
-- the vehicle's registered owner - so ownership history has no analytical
-- consumer, and modelling it would be effort spent on a question nobody asks.
-- The column is named customer_id_current to say so out loud.
--
-- Inferred members are promoted in place by the same ON CONFLICT: an inferred
-- row is simply one whose attributes are all NULL, and the real record fills
-- them in on the surrogate key the facts already point at.
-- ===========================================================================

INSERT INTO core.dim_vehicle (
    vehicle_id, customer_id_current, make, model, model_year,
    battery_capacity_kwh, connector_type, registration_state, vehicle_class,
    is_inferred, dw_run_id
)
SELECT
    s.vehicle_id,
    s.customer_id,
    s.make,
    s.model,
    s.model_year,
    s.battery_capacity_kwh,
    s.connector_type,
    s.registration_state,
    s.vehicle_class,
    FALSE,
    :run_id::UUID
FROM stg.vehicle AS s
ON CONFLICT (vehicle_id) DO UPDATE SET
customer_id_current = excluded.customer_id_current,
make = excluded.make,
model = excluded.model,
model_year = excluded.model_year,
battery_capacity_kwh = excluded.battery_capacity_kwh,
connector_type = excluded.connector_type,
registration_state = excluded.registration_state,
vehicle_class = excluded.vehicle_class,
is_inferred = FALSE,
dw_run_id = excluded.dw_run_id,
dw_updated_at_utc = now()
-- The WHERE clause is what makes a rerun a genuine NO-OP rather than a
-- no-visible-change. Without it, every run would rewrite every row, bump
-- dw_updated_at_utc, generate dead tuples for VACUUM to clean up, and make the
-- idempotency checksum test fail on a column that has nothing to do with the
-- data.
WHERE
    core.dim_vehicle.customer_id_current IS DISTINCT FROM excluded.customer_id_current
    OR core.dim_vehicle.make IS DISTINCT FROM excluded.make
    OR core.dim_vehicle.model IS DISTINCT FROM excluded.model
    OR core.dim_vehicle.model_year IS DISTINCT FROM excluded.model_year
    OR core.dim_vehicle.battery_capacity_kwh IS DISTINCT FROM excluded.battery_capacity_kwh
    OR core.dim_vehicle.connector_type IS DISTINCT FROM excluded.connector_type
    OR core.dim_vehicle.registration_state IS DISTINCT FROM excluded.registration_state
    OR core.dim_vehicle.vehicle_class IS DISTINCT FROM excluded.vehicle_class
    OR core.dim_vehicle.is_inferred;
