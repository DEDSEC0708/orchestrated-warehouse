-- ===========================================================================
-- load_dim_session_outcome.sql - add any unseen outcome combination.
--
-- Parameters: :run_id, :batch_lo, :batch_hi
--
-- The junk dimension's cross product is seeded exhaustively at deploy time
-- (sql/seed/03_dim_session_outcome.sql), so this normally inserts nothing.
-- It exists as a safety net: if a charger ever reports a stop reason nobody
-- anticipated, the fact load must degrade to a NEW DIMENSION ROW rather than
-- to a foreign-key violation at 02:00.
--
-- Belt and braces, for about twenty lines. The alternative - discovering the
-- combination at fact-load time and failing - is the kind of outage that
-- happens once and is remembered for years.
-- ===========================================================================

INSERT INTO core.dim_session_outcome (
    session_status, stop_reason, auth_method, is_roaming,
    is_successful, is_interrupted, outcome_group, dw_run_id
)
SELECT DISTINCT
    s.session_status,
    COALESCE(s.stop_reason, 'UNKNOWN'),
    COALESCE(s.auth_method, 'UNKNOWN'),
    s.is_roaming,
    s.session_status = 'COMPLETED'
    AND COALESCE(s.stop_reason, 'UNKNOWN') IN ('Local', 'Remote'),
    COALESCE(s.stop_reason, 'UNKNOWN') IN ('EVDisconnected', 'PowerLoss', 'EmergencyStop'),
    CASE
        WHEN s.session_status = 'FAULTED' THEN 'FAULTED'
        WHEN COALESCE(s.stop_reason, 'UNKNOWN') IN ('PowerLoss', 'EmergencyStop') THEN 'ABORTED'
        WHEN COALESCE(s.stop_reason, 'UNKNOWN') = 'EVDisconnected' THEN 'DISCONNECTED'
        WHEN COALESCE(s.stop_reason, 'UNKNOWN') = 'OTHER' THEN 'OTHER'
        ELSE 'NORMAL'
    END,
    :run_id::UUID
FROM stg.session AS s
WHERE s.dw_batch_key BETWEEN :batch_lo AND :batch_hi
ON CONFLICT (session_status, stop_reason, auth_method, is_roaming) DO NOTHING;
