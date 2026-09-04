-- ===========================================================================
-- 01_dim_hour.sql - the 24-row hour-of-day dimension (IST).
--
-- Twenty-four rows earn their place because hour-of-day utilisation is the
-- most-asked question in charging analytics, and because day_part and
-- is_peak_tariff_hour are BUSINESS definitions that belong in one place. Left
-- to EXTRACT(hour ...) in every query, "evening" ends up meaning three
-- different things in three different dashboards.
--
-- Peak hours reflect the commercial pattern this domain actually shows:
-- a morning office-arrival peak and a long evening retail/highway peak.
-- ===========================================================================

INSERT INTO core.dim_hour (hour_key, hour_label, day_part, is_peak_tariff_hour)
SELECT
    h                                              AS hour_key,
    lpad(h::TEXT, 2, '0') || ':00'                 AS hour_label,
    CASE
        WHEN h BETWEEN 0 AND 5 THEN 'NIGHT'
        WHEN h BETWEEN 6 AND 11 THEN 'MORNING'
        WHEN h BETWEEN 12 AND 16 THEN 'AFTERNOON'
        WHEN h BETWEEN 17 AND 21 THEN 'EVENING'
        ELSE 'NIGHT'
    END                                            AS day_part,
    (h BETWEEN 9 AND 11) OR (h BETWEEN 18 AND 22)  AS is_peak_tariff_hour
FROM generate_series(0, 23) AS g (h)
ON CONFLICT (hour_key) DO NOTHING;
