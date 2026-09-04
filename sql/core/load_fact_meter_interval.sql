-- ===========================================================================
-- load_fact_meter_interval.sql - the high-volume interval fact.
--
-- Parameters: :run_id, :batch_lo, :batch_hi
--
-- Runs AFTER the session fact, because it needs charging_session_sk. That
-- ordering is why the two are separate tasks in the DAG rather than one.
--
-- PARTITION-PRUNED RESTATEMENT. The delete is expressed on date_key as well as
-- dw_batch_key, so PostgreSQL can prune to the affected monthly partitions
-- instead of scanning eleven and a half million rows to find a few thousand.
-- That single predicate is most of the justification for partitioning this
-- table and nothing else.
--
-- The relationship to fact_charging_session is NOT a declared foreign key -
-- see the note in sql/ddl/31_core_facts.sql for why operability beat a
-- constraint here - so the INNER JOIN below is what enforces it, and the
-- error-severity FACT_METER_ORPHAN_SESSION rule is what verifies it held.
--
-- ===========================================================================
-- WHY THE WINDOW IS ALSO SCOPED BY TRANSACTION
-- ===========================================================================
-- A session that starts at 23:40 IST has intervals on the FOLLOWING business
-- date. The session fact is windowed on the session's business date and the
-- interval fact on the interval's, so those two windows do not always contain
-- the same session - and when they disagree, this happens:
--
--   1. a restatement window ends on 4 June
--   2. the session fact deletes and re-inserts TXN-...-20260604, which is
--      assigned a NEW charging_session_sk by the identity sequence
--   3. its intervals are dated 5 June, outside this window, so they are left
--      untouched - still pointing at the surrogate key that no longer exists
--
-- The result is an orphaned interval: a row referencing a session that is not
-- there, on a table that deliberately has no foreign key to catch it. In a
-- warehouse that is silent data corruption - the intervals simply drop out of
-- any join and the energy they carry disappears from every total.
--
-- The fix is to restate by the SET OF SESSIONS this run rewrote, in addition
-- to the interval date window. Every interval belonging to a session in the
-- window is deleted and rebuilt with the session's new key, whichever date
-- the interval itself falls on.
--
-- The date_key predicate is KEPT alongside it, because it is what lets
-- PostgreSQL prune to the affected monthly partitions; the transaction scope
-- is an addition to it, not a replacement. The temp table is indexed so the
-- extra predicate is a hash join rather than a per-row scan.
-- ===========================================================================

CREATE TEMP TABLE restated_sessions ON COMMIT DROP AS
SELECT DISTINCT s.transaction_id
FROM stg.session AS s
WHERE s.business_date_ist BETWEEN :batch_lo::DATE AND (:batch_hi::DATE + 1);

CREATE UNIQUE INDEX ON restated_sessions (transaction_id);

DELETE FROM core.fact_meter_interval
WHERE (
        dw_batch_key BETWEEN :batch_lo::DATE AND (:batch_hi::DATE + 1)
        AND date_key BETWEEN to_char(:batch_lo::DATE, 'YYYYMMDD')::INT
        AND to_char(:batch_hi::DATE + 1, 'YYYYMMDD')::INT
      )
   OR transaction_id IN (SELECT k.transaction_id FROM restated_sessions AS k);

INSERT INTO core.fact_meter_interval (
    transaction_id, interval_seq, charging_session_sk, date_key, hour_key,
    charge_point_sk, customer_sk, interval_start_utc, interval_end_utc,
    interval_seconds, interval_energy_kwh, avg_power_kw,
    soc_start_pct, soc_end_pct, dw_run_id, dw_batch_key
)
SELECT
    mi.transaction_id,
    mi.interval_seq,
    f.charging_session_sk,
    to_char(mi.business_date_ist, 'YYYYMMDD')::INT,
    extract(HOUR FROM (mi.interval_start_utc AT TIME ZONE 'Asia/Kolkata'))::SMALLINT,
    -- The interval INHERITS the session's dimension keys rather than resolving
    -- its own. That is deliberate: an interval belongs to a session, and if it
    -- re-resolved point-in-time on its own timestamp, a session that spanned a
    -- firmware update would have its intervals split across two versions of
    -- the same device while its header pointed at one. The session's
    -- resolution is the single source of truth for the whole session.
    f.charge_point_sk,
    f.customer_sk,
    mi.interval_start_utc,
    mi.interval_end_utc,
    mi.interval_seconds,
    mi.interval_energy_kwh,
    mi.avg_power_kw,
    mi.soc_start_pct,
    mi.soc_end_pct,
    :run_id::UUID,
    mi.business_date_ist
FROM stg.meter_interval AS mi
INNER JOIN core.fact_charging_session AS f ON f.transaction_id = mi.transaction_id
-- Mirrors the DELETE above exactly. Any asymmetry between the two would
-- either leave a hole (deleted and not re-inserted) or raise a unique
-- violation (re-inserted without being deleted), so they are written as the
-- same pair of conditions in the same order.
WHERE mi.business_date_ist BETWEEN :batch_lo::DATE AND (:batch_hi::DATE + 1)
   OR mi.transaction_id IN (SELECT k.transaction_id FROM restated_sessions AS k);
