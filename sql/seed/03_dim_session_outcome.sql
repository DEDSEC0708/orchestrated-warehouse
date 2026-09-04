-- ===========================================================================
-- 03_dim_session_outcome.sql - seed the junk dimension's cross product.
--
-- A junk dimension consolidates several unrelated low-cardinality flags into
-- one small table so they do not each occupy a column on a million-row fact.
-- With four attributes of 2, 8, 4 and 2 values the whole cross product is
-- under 130 rows, so it is cheaper to seed exhaustively than to discover
-- combinations at load time and risk a fact load failing at 02:00 because a
-- charger reported a stop reason nobody had seen before.
--
-- The dimension load (sql/core/load_dim_session_outcome.sql) still inserts any
-- genuinely new combination it encounters, so an unseen value degrades to a
-- new row rather than to a failure. Belt and braces, cheaply.
--
-- is_successful / is_interrupted / outcome_group are DERIVED here, once, so
-- every consumer agrees on what "a successful session" means.
-- ===========================================================================

INSERT INTO core.dim_session_outcome (
    session_status, stop_reason, auth_method, is_roaming,
    is_successful, is_interrupted, outcome_group
)
SELECT
    s.session_status,
    r.stop_reason,
    a.auth_method,
    g.is_roaming,
    s.session_status = 'COMPLETED' AND r.stop_reason IN ('Local', 'Remote')  AS is_successful,
    r.stop_reason IN ('EVDisconnected', 'PowerLoss', 'EmergencyStop')        AS is_interrupted,
    CASE
        WHEN s.session_status = 'FAULTED' THEN 'FAULTED'
        WHEN r.stop_reason IN ('PowerLoss', 'EmergencyStop') THEN 'ABORTED'
        WHEN r.stop_reason = 'EVDisconnected' THEN 'DISCONNECTED'
        WHEN r.stop_reason = 'OTHER' THEN 'OTHER'
        ELSE 'NORMAL'
    END                                                                      AS outcome_group
FROM (VALUES ('COMPLETED'), ('FAULTED')) AS s (session_status)
CROSS JOIN (
    VALUES ('Local'), ('Remote'), ('EVDisconnected'), ('PowerLoss'),
    ('EmergencyStop'), ('DeAuthorized'), ('OTHER'), ('UNKNOWN')
) AS r (stop_reason)
CROSS JOIN (VALUES ('APP'), ('RFID'), ('ROAMING'), ('UNKNOWN')) AS a (auth_method)
CROSS JOIN (VALUES (TRUE), (FALSE)) AS g (is_roaming)
ON CONFLICT (session_status, stop_reason, auth_method, is_roaming) DO NOTHING;
