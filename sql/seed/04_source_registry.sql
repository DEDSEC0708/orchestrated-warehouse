-- ===========================================================================
-- 04_source_registry.sql - declare every source entity.
--
-- The freshness rule, the maintenance DAG's watermark audit and the bootstrap
-- script all iterate over THIS TABLE rather than over a hard-coded list. That
-- is the difference between a framework and a script: adding a source becomes
-- a row and a YAML block, not an edit to five modules.
--
-- Note the four different load strategies. "Incremental everywhere" is dogma,
-- not engineering: sixty rows of grid tariff reference data are cheaper and
-- safer to reload wholesale, and the registry says so out loud.
--
-- Uses ON CONFLICT DO UPDATE rather than DO NOTHING because this table is
-- CONFIGURATION: when the SQL file changes, the database should follow. The
-- watermark table below is the opposite - it is STATE, and re-seeding must
-- never reset it.
-- ===========================================================================

INSERT INTO ctl.source_registry (
    source_system, entity, is_enabled, load_strategy, natural_key_columns,
    watermark_column, target_raw_table, sla_minutes, owner, description
)
VALUES
-- S1: the operational CMS. Five entities, all watermarked on updated_at.
('CMS', 'customers', TRUE, 'incremental_timestamp', ARRAY['customer_id'],
 'updated_at', 'raw.cms_customers', 2880, 'data-eng',
 'Customer master. SCD2 source for dim_customer.'),
('CMS', 'vehicles', TRUE, 'incremental_timestamp', ARRAY['vehicle_id'],
 'updated_at', 'raw.cms_vehicles', 2880, 'data-eng',
 'Vehicle master. SCD1 source for dim_vehicle.'),
('CMS', 'stations', TRUE, 'incremental_timestamp', ARRAY['station_id'],
 'updated_at', 'raw.cms_stations', 2880, 'data-eng',
 'Station master. SCD2 source for dim_station.'),
('CMS', 'charge_points', TRUE, 'incremental_timestamp', ARRAY['charge_point_id'],
 'updated_at', 'raw.cms_charge_points', 2880, 'data-eng',
 'Charge point master. SCD2 source for dim_charge_point.'),
('CMS', 'tariff_plans', TRUE, 'incremental_timestamp', ARRAY['tariff_plan_id'],
 'updated_at', 'raw.cms_tariff_plans', 2880, 'data-eng',
 'Tariff plan master. SCD2 source for dim_tariff_plan - the price history.'),

-- S2/S3: partitioned landing files, guarded by the file-hash registry.
('OCPP', 'ocpp_cdr', TRUE, 'incremental_partition', ARRAY['transaction_id'],
 NULL, 'raw.ocpp_cdr', 2160, 'data-eng',
 'OCPP charge detail records, one JSONL file per city per day.'),
('METER', 'meter_value', TRUE, 'incremental_partition', ARRAY['sample_id'],
 NULL, 'raw.meter_value', 2160, 'data-eng',
 'Meter value telemetry samples, one gzipped CSV per day.'),

-- S4: the roaming partner API, cursored on last_updated with a 7-day lookback
-- because that source revises records for up to a week.
('PARTNER', 'partner_cdr', TRUE, 'incremental_cursor', ARRAY['cdr_id'],
 'last_updated', 'raw.partner_cdr', 2880, 'data-eng',
 'Roaming partner charge detail records over an OCPI-style paginated API.'),

-- S5: static reference data, full snapshot, deliberately.
('SEED', 'grid_tariff_slab', TRUE, 'full_snapshot', ARRAY['state', 'effective_from_date'],
 NULL, 'raw.grid_tariff_slab', 20160, 'data-eng',
 'State grid commercial tariff slabs. Sixty rows, reloaded wholesale.')
ON CONFLICT (source_system, entity) DO UPDATE SET
is_enabled = excluded.is_enabled,
load_strategy = excluded.load_strategy,
natural_key_columns = excluded.natural_key_columns,
watermark_column = excluded.watermark_column,
target_raw_table = excluded.target_raw_table,
sla_minutes = excluded.sla_minutes,
owner = excluded.owner,
description = excluded.description,
updated_at_utc = now();
