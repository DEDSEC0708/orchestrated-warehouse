-- ===========================================================================
-- load_fact_station_daily_utilization.sql - the periodic snapshot fact.
--
-- Parameters: :run_id, :batch_lo, :batch_hi
--
-- ===========================================================================
-- WHY THIS TABLE EXISTS AT ALL
-- ===========================================================================
-- Idle time and available time are ABSENCES OF SESSIONS. You cannot derive
-- "this station stood empty for fourteen hours" from a table that only
-- contains sessions - there is nothing there to count. A periodic snapshot is
-- the correct Kimball answer to "measure a state over a period", and having
-- one alongside a transaction fact is what demonstrates knowing the difference
-- rather than merely knowing both names.
--
-- ===========================================================================
-- DENSE GENERATION IS LOAD-BEARING
-- ===========================================================================
-- Rows come from the CROSS JOIN of dim_date x active stations, then LEFT JOIN
-- to session aggregates. A station with zero sessions produces a row of zeros.
--
-- This is not tidiness. If only busy days produced rows, every utilisation
-- average would be computed over busy days only and every number would be
-- optimistically biased - and the bias would be invisible, because the missing
-- rows are missing. The cost is roughly 65,000 rows instead of 50,000, which
-- is nothing.
--
-- The station is resolved POINT-IN-TIME for each date, so a site that gained
-- four bays in April is measured against four bays in March and eight in May.
-- Using today's bay count for both would make a successful expansion look like
-- falling utilisation.
--
-- ===========================================================================
-- WHY THE WINDOW IS batch_hi + 1 EVERYWHERE BELOW
-- ===========================================================================
-- The batch key is a UTC date; date_key is the IST BUSINESS date. India is
-- UTC+5:30, so a session starting at 20:00 UTC on the last day of the window
-- belongs to the FOLLOWING business date. Generating the calendar only to
-- batch_hi would leave those sessions with no snapshot row to land in.
--
-- The extra day is not a fudge factor, and it is applied consistently to all
-- three predicates - the DELETE, the calendar and the session aggregate - so
-- the load stays a clean delete-insert over one window and re-running it with
-- the same parameters remains a no-op.
--
-- CONSEQUENCE FOR ANYTHING THAT READS THE WINDOW BACK: rows exist with
-- dw_batch_key = batch_hi + 1. A caller that derives its next window from
-- max(dw_batch_key) in this table would therefore extend it by a day on every
-- pass. scripts/rebuild_dimension_history.py takes its upper bound from RAW
-- for exactly this reason.
-- ===========================================================================

DELETE FROM core.fact_station_daily_utilization
WHERE dw_batch_key BETWEEN :batch_lo::DATE AND (:batch_hi::DATE + 1);

INSERT INTO core.fact_station_daily_utilization (
    date_key, station_sk, city, charge_point_count, available_minutes,
    occupied_minutes, session_count, failed_session_count,
    unique_customer_count, energy_delivered_kwh, gross_revenue_inr,
    utilization_pct, dw_run_id, dw_batch_key
)
WITH calendar AS (
    SELECT d.date_key, d.full_date
    FROM core.dim_date AS d
    WHERE d.full_date BETWEEN :batch_lo::DATE AND (:batch_hi::DATE + 1)
),

-- The station version in effect on each date. A range join rather than a
-- current-state join, which is what makes the denominator historically
-- correct.
station_days AS (
    SELECT
        c.date_key,
        c.full_date,
        st.station_sk,
        st.station_id,
        st.city,
        COALESCE(st.num_bays, 0) AS num_bays
    FROM calendar AS c
    INNER JOIN core.dim_station AS st
        ON st.station_sk > 0
        AND (c.full_date + TIME '12:00') AT TIME ZONE 'Asia/Kolkata'
        >= st.effective_from_utc
        AND (c.full_date + TIME '12:00') AT TIME ZONE 'Asia/Kolkata'
        < st.effective_to_utc
    WHERE st.is_active
),

-- Devices in service at each station on each date, again point-in-time.
device_counts AS (
    SELECT
        sd.date_key,
        sd.station_sk,
        count(DISTINCT cp.charge_point_id) AS charge_point_count
    FROM station_days AS sd
    INNER JOIN core.dim_charge_point AS cp
        ON cp.station_id = sd.station_id
        AND cp.charge_point_sk > 0
        AND (sd.full_date + TIME '12:00') AT TIME ZONE 'Asia/Kolkata'
        >= cp.effective_from_utc
        AND (sd.full_date + TIME '12:00') AT TIME ZONE 'Asia/Kolkata'
        < cp.effective_to_utc
    WHERE cp.status = 'ACTIVE'
    GROUP BY sd.date_key, sd.station_sk
),

session_rollup AS (
    SELECT
        f.start_date_key                                     AS date_key,
        f.station_sk,
        count(*)                                             AS session_count,
        count(*) FILTER (WHERE NOT o.is_successful)          AS failed_session_count,
        count(DISTINCT f.customer_sk)                        AS unique_customer_count,
        sum(f.energy_delivered_kwh)                          AS energy_delivered_kwh,
        sum(f.gross_revenue_inr)                             AS gross_revenue_inr,
        -- Occupied minutes counts PLUGGED-IN time, not charging time: a bay
        -- occupied by a fully-charged car is still unavailable to the next
        -- driver, which is what utilisation is about.
        sum(f.duration_seconds) / 60                         AS occupied_minutes
    FROM core.fact_charging_session AS f
    INNER JOIN core.dim_session_outcome AS o
        ON o.session_outcome_sk = f.session_outcome_sk
    WHERE f.dw_batch_key BETWEEN :batch_lo::DATE AND (:batch_hi::DATE + 1)
      AND f.station_sk > 0
    GROUP BY f.start_date_key, f.station_sk
)

SELECT
    sd.date_key,
    sd.station_sk,
    sd.city,
    COALESCE(dc.charge_point_count, 0),
    -- The capacity denominator: devices in service times minutes in a day.
    COALESCE(dc.charge_point_count, 0) * 1440,
    LEAST(
        COALESCE(sr.occupied_minutes, 0),
        -- Capped at capacity. Overlapping sessions on different bays can sum
        -- past the total available minutes when a device count is stale, and a
        -- utilisation above 100% is a number nobody can act on.
        COALESCE(dc.charge_point_count, 0) * 1440
    ),
    COALESCE(sr.session_count, 0),
    COALESCE(sr.failed_session_count, 0),
    COALESCE(sr.unique_customer_count, 0),
    COALESCE(sr.energy_delivered_kwh, 0),
    COALESCE(sr.gross_revenue_inr, 0),
    CASE
        WHEN COALESCE(dc.charge_point_count, 0) = 0 THEN 0
        ELSE round(
            100.0 * LEAST(
                COALESCE(sr.occupied_minutes, 0), dc.charge_point_count * 1440
            ) / (dc.charge_point_count * 1440),
            2
        )
    END,
    :run_id::UUID,
    sd.full_date
FROM station_days AS sd
LEFT JOIN device_counts AS dc
    ON dc.date_key = sd.date_key AND dc.station_sk = sd.station_sk
-- LEFT JOIN, and this is the line that makes the fact dense: a station with no
-- sessions still produces a row, with zeros.
LEFT JOIN session_rollup AS sr
    ON sr.date_key = sd.date_key AND sr.station_sk = sd.station_sk;
