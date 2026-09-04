-- ===========================================================================
-- 60_grants.sql - who may read what.
--
-- The database bootstrap (docker/postgres/init/) created the roles and granted
-- database-level CONNECT. It could not grant on schemas that did not exist
-- yet. This file closes that gap, and it runs as wh_etl - the owner of every
-- object in the warehouse - so no superuser is needed to apply the schema.
--
-- wh_analyst is the CONSUMER role: SELECT on `mart` only. Not on `core`, not
-- on `raw`. That is a deliberate statement about what the warehouse's contract
-- with its consumers is: the mart is the interface, and the star schema is an
-- implementation detail that may change.
--
-- The two ALTER DEFAULT PRIVILEGES statements are the load-bearing ones.
-- Without them, every mart table created LATER - by a future phase, or by the
-- mart rebuild recreating a table - would be invisible to wh_analyst, and the
-- failure would appear as "permission denied for a table that exists".
-- ===========================================================================

GRANT USAGE ON SCHEMA mart TO wh_analyst;
GRANT SELECT ON ALL TABLES IN SCHEMA mart TO wh_analyst;

ALTER DEFAULT PRIVILEGES FOR ROLE wh_etl IN SCHEMA mart
GRANT SELECT ON TABLES TO wh_analyst;

-- The data-quality posture is deliberately readable by consumers: someone
-- looking at a number should be able to see whether that day's checks passed
-- without asking the data team. Transparency about quality is a feature.
GRANT USAGE ON SCHEMA dq TO wh_analyst;
GRANT SELECT ON dq.rule, dq.check_result TO wh_analyst;

ALTER DEFAULT PRIVILEGES FOR ROLE wh_etl IN SCHEMA dq
GRANT SELECT ON TABLES TO wh_analyst;

-- Quarantine tables are NOT granted: they hold complete original payloads,
-- including source records with personal data that the warehouse layers
-- deliberately minimise away. Analysts see the quarantine SUMMARY view in
-- mart, which carries counts and rule codes but no payloads.
REVOKE ALL ON dq.quarantine_ocpp_cdr FROM wh_analyst;
REVOKE ALL ON dq.quarantine_meter_value FROM wh_analyst;
REVOKE ALL ON dq.quarantine_partner_cdr FROM wh_analyst;
REVOKE ALL ON dq.quarantine_cms_entity FROM wh_analyst;

-- Pipeline observability: an analyst asking "is today's data complete yet?"
-- should be able to answer it themselves from the audit tables.
GRANT USAGE ON SCHEMA audit TO wh_analyst;
GRANT SELECT ON ALL TABLES IN SCHEMA audit TO wh_analyst;

ALTER DEFAULT PRIVILEGES FOR ROLE wh_etl IN SCHEMA audit
GRANT SELECT ON TABLES TO wh_analyst;
