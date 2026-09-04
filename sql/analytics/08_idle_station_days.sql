-- ===========================================================================
-- 08 - Stations that earned nothing, and for how many days in a row.
--
-- THIS IS THE QUERY THE PERIODIC SNAPSHOT FACT EXISTS FOR.
--
-- A transaction fact can only tell you about things that happened. "Which
-- stations had no sessions on Tuesday" is a question about ABSENCE, and no
-- amount of clever SQL over core.fact_charging_session will answer it,
-- because a station with no sessions contributes no rows to reason about.
--
-- core.fact_station_daily_utilization is generated DENSELY: every station gets
-- a row for every date it was commissioned for, session or not. Zero-session
-- days are therefore rows with session_count = 0, and the question becomes an
-- ordinary WHERE clause.
--
-- The consecutive-run grouping is the classic gaps-and-islands trick: for a
-- contiguous run of dead days, (row_number over the whole station) minus
-- (row_number over the dead days only) is constant, so it can be grouped on.
-- A five-day run is an outage worth a truck roll; five scattered single days
-- is just a quiet suburb.
-- ===========================================================================

WITH daily AS (
    SELECT
        u.station_sk,
        st.station_id,
        st.station_name,
        st.city,
        st.site_type,
        d.full_date,
        d.is_weekend,
        u.session_count,
        u.charge_point_count,
        row_number() OVER (PARTITION BY u.station_sk ORDER BY d.full_date)
            AS all_day_seq,
        row_number() OVER (
            PARTITION BY u.station_sk, (u.session_count = 0) ORDER BY d.full_date
        )   AS same_state_seq
    FROM core.fact_station_daily_utilization AS u
    INNER JOIN core.dim_date AS d ON d.date_key = u.date_key
    INNER JOIN core.dim_station AS st ON st.station_sk = u.station_sk
),

dead_days AS (
    SELECT
        dy.*,
        dy.all_day_seq - dy.same_state_seq AS run_group
    FROM daily AS dy
    WHERE dy.session_count = 0
)

SELECT
    dd.station_id,
    dd.station_name,
    dd.city,
    dd.site_type,
    max(dd.charge_point_count)                            AS charge_points,
    min(dd.full_date)                                     AS run_started_on,
    max(dd.full_date)                                     AS run_ended_on,
    count(*)                                              AS consecutive_dead_days,
    count(*) FILTER (WHERE dd.is_weekend)                 AS of_which_weekend,
    -- Capacity that existed and produced nothing. This is the number that
    -- turns "a station was quiet" into "we paid for N bay-days of nothing".
    sum(dd.charge_point_count)                            AS idle_bay_days,
    CASE
        WHEN count(*) >= 5 THEN 'INVESTIGATE_OUTAGE'
        WHEN count(*) >= 2 THEN 'WATCH'
        ELSE 'NORMAL_QUIET_DAY'
    END                                                   AS assessment
FROM dead_days AS dd
GROUP BY dd.station_sk, dd.station_id, dd.station_name, dd.city, dd.site_type, dd.run_group
ORDER BY consecutive_dead_days DESC, run_started_on ASC;
