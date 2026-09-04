-- ===========================================================================
-- create_inferred_members.sql - late-arriving dimension members.
--
-- Parameters: :run_id, :batch_lo, :batch_hi
--
-- ===========================================================================
-- THE PROBLEM
-- ===========================================================================
-- A charge detail record references CP-BLR-0904, a device commissioned this
-- morning and not yet in the CMS extract. Three options:
--
--   DROP THE FACT      loses revenue, and loses it silently
--   NULL THE FK        breaks every join, and COUNT(*) starts lying
--   INFER THE MEMBER   create a placeholder now, fill it in later
--
-- The third is the Kimball answer and it is right for a reason worth stating:
-- the fact is TRUE. Energy really was delivered and money really was charged.
-- It is the master data that is late, and losing a true fact because a
-- different system was slow is the wrong trade.
--
-- ===========================================================================
-- WHY THIS WORKS SO WELL
-- ===========================================================================
-- The placeholder is created with effective_from at the beginning of time, so
-- a fact of ANY date resolves to it in a point-in-time join. When the real record
-- arrives, the SCD2 merge fills in its attributes IN PLACE, keeping the SAME
-- SURROGATE KEY - so every fact already pointing at it becomes correct
-- instantly, with no fact rewrite and no restatement.
--
-- Sessions on an inferred charge point resolve their tariff to the UNKNOWN
-- (-1) member and therefore carry zero revenue, which is DELIBERATELY VISIBLE:
-- the FACT_UNKNOWN_MEMBER_RATIO rule reports the share, so the gap shows up on
-- the scorecard instead of hiding inside a revenue total.
--
-- Runs BEFORE the fact load, so the fact's foreign keys always resolve.
--
-- The BEGINNING-OF-TIME sentinel is '0001-01-01 00:00:00+00', not the
-- PostgreSQL '-infinity' that would be the obvious choice. -infinity is
-- correct in the database and UNREADABLE from Python: psycopg raises
-- "timestamp too small (before year 1)" the moment any code SELECTs the
-- column, which turns a perfectly good sentinel into a landmine for every
-- script, test and future reader. Year 1 is far enough in the past to serve
-- exactly the same purpose - any fact of any plausible date resolves against
-- it - and it round-trips through every client.
-- ===========================================================================

INSERT INTO core.dim_charge_point (
    charge_point_id, status, effective_from_utc, effective_to_utc,
    is_current, version_no, row_hash, is_inferred, dw_run_id
)
SELECT DISTINCT
    s.charge_point_id,
    'UNKNOWN',
    TIMESTAMPTZ '0001-01-01 00:00:00+00',
    TIMESTAMPTZ '9999-12-31 00:00:00+00',
    TRUE,
    1,
    -- A hash over nothing but the key. It cannot collide with a real version's
    -- hash (which covers eight attributes), so the merge always classifies the
    -- promoting record as a genuine change and fills the placeholder in.
    core.row_hash('INFERRED', s.charge_point_id),
    TRUE,
    :run_id::UUID
FROM stg.session AS s
WHERE s.dw_batch_key BETWEEN :batch_lo AND :batch_hi
  AND s.charge_point_id IS NOT NULL
  AND NOT EXISTS (
      SELECT 1 FROM core.dim_charge_point AS d
      WHERE d.charge_point_id = s.charge_point_id
  );

-- Customers referenced by a session but absent from the CMS extract. Same
-- reasoning: a session by a customer registered five minutes ago is a real
-- session.
INSERT INTO core.dim_customer (
    customer_id, customer_segment, subscription_plan, kyc_status, is_active,
    effective_from_utc, effective_to_utc, is_current, version_no, row_hash,
    is_inferred, dw_run_id
)
SELECT DISTINCT
    s.customer_id,
    'UNKNOWN',
    'UNKNOWN',
    'UNKNOWN',
    TRUE,
    TIMESTAMPTZ '0001-01-01 00:00:00+00',
    TIMESTAMPTZ '9999-12-31 00:00:00+00',
    TRUE,
    1,
    core.row_hash('INFERRED', s.customer_id),
    TRUE,
    :run_id::UUID
FROM stg.session AS s
WHERE s.dw_batch_key BETWEEN :batch_lo AND :batch_hi
  AND s.customer_id IS NOT NULL
  AND NOT EXISTS (
      SELECT 1 FROM core.dim_customer AS d WHERE d.customer_id = s.customer_id
  );

-- Vehicles are Type 1, so an inferred member is just a row with NULL
-- attributes that the next load fills in.
INSERT INTO core.dim_vehicle (vehicle_id, is_inferred, dw_run_id)
SELECT DISTINCT
    s.vehicle_id,
    TRUE,
    :run_id::UUID
FROM stg.session AS s
WHERE s.dw_batch_key BETWEEN :batch_lo AND :batch_hi
  AND s.vehicle_id IS NOT NULL
  AND NOT EXISTS (
      SELECT 1 FROM core.dim_vehicle AS d WHERE d.vehicle_id = s.vehicle_id
  )
ON CONFLICT (vehicle_id) DO NOTHING;

-- Stations referenced by a charge point that the station extract has not
-- reported. Rare, but a device cannot resolve its site without one, and a
-- session with an unknown station is invisible in every site-level report.
INSERT INTO core.dim_station (
    station_id, is_active, effective_from_utc, effective_to_utc,
    is_current, version_no, row_hash, is_inferred, dw_run_id
)
SELECT DISTINCT
    cp.station_id,
    TRUE,
    TIMESTAMPTZ '0001-01-01 00:00:00+00',
    TIMESTAMPTZ '9999-12-31 00:00:00+00',
    TRUE,
    1,
    core.row_hash('INFERRED', cp.station_id),
    TRUE,
    :run_id::UUID
FROM core.dim_charge_point AS cp
WHERE cp.station_id IS NOT NULL
  AND NOT EXISTS (
      SELECT 1 FROM core.dim_station AS d WHERE d.station_id = cp.station_id
  );

-- Tariff plans referenced by a charge point that do not exist - the dangling
-- foreign key a bad admin edit leaves behind. Inferred with ZERO prices, so a
-- session on such a device produces zero revenue and appears on the unknown-
-- member scorecard rather than being priced at some plausible-looking default
-- that would quietly poison a revenue total.
INSERT INTO core.dim_tariff_plan (
    tariff_plan_id, plan_name, price_per_kwh_inr, price_per_minute_inr,
    idle_fee_per_minute_inr, min_billable_kwh, gst_rate_pct, is_active,
    effective_from_utc, effective_to_utc, is_current, version_no, row_hash,
    is_inferred, dw_run_id
)
SELECT DISTINCT
    cp.tariff_plan_id,
    'Inferred (referenced but not in the CMS)',
    0, 0, 0, 0, 0,
    TRUE,
    TIMESTAMPTZ '0001-01-01 00:00:00+00',
    TIMESTAMPTZ '9999-12-31 00:00:00+00',
    TRUE,
    1,
    core.row_hash('INFERRED', cp.tariff_plan_id),
    TRUE,
    :run_id::UUID
FROM core.dim_charge_point AS cp
WHERE cp.tariff_plan_id IS NOT NULL
  AND NOT EXISTS (
      SELECT 1 FROM core.dim_tariff_plan AS d WHERE d.tariff_plan_id = cp.tariff_plan_id
  );
