-- ===========================================================================
-- 05_watermarks.sql - bootstrap the watermark for every source entity.
--
-- Seeded to an epoch value so the FIRST run's predicate matches everything and
-- the bootstrap is a normal incremental load rather than a special case. From
-- then on the pipeline advances them itself, inside the same transaction that
-- commits the data.
--
-- ON CONFLICT DO NOTHING, emphatically. This table is STATE, not
-- configuration: re-running the seed after six months of operation must not
-- rewind the pipeline's memory and cause every source to be re-extracted from
-- 1970. (Contrast ctl.source_registry, which IS configuration and therefore
-- uses DO UPDATE.) Getting these two the same way round is the kind of
-- mistake that only shows up in production.
--
-- LOOKBACK IS PER SOURCE, and each value is a claim about how that specific
-- source behaves:
--   CMS      2 hours - tolerates clock skew between the OLTP box and here
--   OCPP     3 days  - a dt= partition is occasionally rewritten with
--                      corrections a day or two later
--   METER    3 days  - same file-rewrite behaviour
--   PARTNER  7 days  - the partner revises records for up to a week while
--                      billing disputes settle
--   SEED     none    - full snapshot, so there is nothing to look back at
-- ===========================================================================

INSERT INTO ctl.watermark (
    source_system, entity, watermark_type, watermark_ts_utc, watermark_str, lookback_interval
)
VALUES
('CMS', 'customers', 'timestamp', '1970-01-01 00:00:00+00', NULL, INTERVAL '2 hours'),
('CMS', 'vehicles', 'timestamp', '1970-01-01 00:00:00+00', NULL, INTERVAL '2 hours'),
('CMS', 'stations', 'timestamp', '1970-01-01 00:00:00+00', NULL, INTERVAL '2 hours'),
('CMS', 'charge_points', 'timestamp', '1970-01-01 00:00:00+00', NULL, INTERVAL '2 hours'),
('CMS', 'tariff_plans', 'timestamp', '1970-01-01 00:00:00+00', NULL, INTERVAL '2 hours'),
('OCPP', 'ocpp_cdr', 'date_partition', NULL, '1970-01-01', INTERVAL '3 days'),
('METER', 'meter_value', 'date_partition', NULL, '1970-01-01', INTERVAL '3 days'),
('PARTNER', 'partner_cdr', 'cursor', '1970-01-01 00:00:00+00', NULL, INTERVAL '7 days'),
('SEED', 'grid_tariff_slab', 'none', NULL, NULL, INTERVAL '0')
ON CONFLICT (source_system, entity) DO NOTHING;
