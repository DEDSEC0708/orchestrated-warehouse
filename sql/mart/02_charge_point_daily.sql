-- ===========================================================================
-- mart/02_charge_point_daily.sql - per-device daily performance.
--
-- Parameters: :run_id
--
-- THE SCD2 PAYOFF LIVES IN THIS TABLE. rated_power_kw comes from the device
-- version that was in effect ON THAT DATE, carried through the fact's
-- point-in-time charge_point_sk. So the question
--
--     "did the 60 kW upgrade increase energy delivered per session?"
--
-- becomes a GROUP BY on this table. With an overwritten dimension it would be
-- unanswerable, because every historical row would claim the device was always
-- 60 kW and the before/after comparison would compare 60 against 60.
-- ===========================================================================

DELETE FROM mart.mart_charge_point_daily;

INSERT INTO mart.mart_charge_point_daily (
    date_key, charge_point_id, station_id, city, rated_power_kw,
    connector_type, firmware_version, session_count, failed_session_count,
    energy_delivered_kwh, gross_revenue_inr, avg_session_kwh, peak_power_kw,
    failure_rate_pct, dw_run_id
)
SELECT
    f.start_date_key,
    cp.charge_point_id,
    -- The grain is (date, device). A device UPGRADED MID-DAY has two versions
    -- in effect on that date, so its point-in-time attributes are not
    -- single-valued at this grain - and grouping by them would produce two
    -- rows for one device-day and violate the declared key.
    --
    -- Attributes are therefore reduced with mode(): the value that applied to
    -- MOST of that day's sessions wins. That is the honest answer at a daily
    -- grain, and it is why the upgrade analysis compares MONTHS rather than
    -- the day of the change itself. The session fact keeps the exact
    -- point-in-time version for anyone who needs it.
    mode() WITHIN GROUP (ORDER BY st.station_id),
    mode() WITHIN GROUP (ORDER BY st.city),
    mode() WITHIN GROUP (ORDER BY cp.rated_power_kw),
    mode() WITHIN GROUP (ORDER BY cp.connector_type),
    mode() WITHIN GROUP (ORDER BY cp.firmware_version),
    count(*),
    count(*) FILTER (WHERE NOT o.is_successful),
    sum(f.energy_delivered_kwh),
    sum(f.gross_revenue_inr),
    round(avg(f.energy_delivered_kwh), 3),
    max(f.peak_power_kw),
    round(100.0 * count(*) FILTER (WHERE NOT o.is_successful) / count(*), 2),
    :run_id::UUID
FROM core.fact_charging_session AS f
INNER JOIN core.dim_charge_point AS cp ON cp.charge_point_sk = f.charge_point_sk
LEFT JOIN core.dim_station AS st ON st.station_sk = f.station_sk
INNER JOIN core.dim_session_outcome AS o ON o.session_outcome_sk = f.session_outcome_sk
WHERE f.charge_point_sk > 0
GROUP BY f.start_date_key, cp.charge_point_id;
