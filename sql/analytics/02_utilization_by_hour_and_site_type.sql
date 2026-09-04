-- ===========================================================================
-- 02 - Demand shape by hour of day and site type.
--
-- Where should the next charger go, and when does it earn? A HIGHWAY site and
-- a MALL site have almost inverted demand curves, and an OFFICE site peaks on
-- weekdays only. Siting and staffing decisions come out of this table.
--
-- Two things make this query honest rather than decorative:
--
--   * The hour is the IST business hour from dim_hour, not an extract() over a
--     UTC timestamp. Every stakeholder thinks in local time; doing the shift
--     once in the dimension means no analyst ever gets it wrong.
--   * site_type comes from the point-in-time station version carried on the
--     fact, so a site reclassified from MALL to HIGHWAY in June contributes
--     its pre-June sessions to MALL. Joining to the CURRENT station row would
--     silently rewrite history - the classic Type-2 mistake.
--
-- energy_share_pct is normalised within a site type, which is what makes the
-- curves comparable when one site type has ten times the volume of another.
-- ===========================================================================

SELECT
    st.site_type,
    h.hour_key,
    h.hour_label,
    h.day_part,
    h.is_peak_tariff_hour,
    count(*)                                                     AS sessions,
    count(*) FILTER (WHERE d.is_weekend)                         AS weekend_sessions,
    round(sum(f.energy_delivered_kwh), 1)                        AS energy_kwh,
    round(avg(f.energy_delivered_kwh), 2)                        AS avg_kwh_per_session,
    round(avg(f.duration_seconds) / 60.0, 1)                     AS avg_minutes,
    round(avg(f.avg_power_kw), 1)                                AS avg_power_kw,
    round(sum(f.gross_revenue_inr), 2)                           AS revenue_inr,
    round(
        100.0 * sum(f.energy_delivered_kwh)
        / nullif(sum(sum(f.energy_delivered_kwh)) OVER (PARTITION BY st.site_type), 0), 2
    )                                                            AS energy_share_of_site_type_pct
FROM core.fact_charging_session AS f
INNER JOIN core.dim_hour AS h ON h.hour_key = f.start_hour_key
INNER JOIN core.dim_date AS d ON d.date_key = f.start_date_key
INNER JOIN core.dim_station AS st ON st.station_sk = f.station_sk
INNER JOIN core.dim_session_outcome AS o ON o.session_outcome_sk = f.session_outcome_sk
WHERE o.is_successful
  AND st.site_type IS NOT NULL
GROUP BY st.site_type, h.hour_key, h.hour_label, h.day_part, h.is_peak_tariff_hour
ORDER BY st.site_type ASC, h.hour_key ASC;
