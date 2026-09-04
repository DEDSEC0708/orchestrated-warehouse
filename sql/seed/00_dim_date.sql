-- ===========================================================================
-- 00_dim_date.sql - generate the date dimension, 2023-01-01 to 2030-12-31.
--
-- This is an IST BUSINESS CALENDAR. Every fact's date_key is derived as
-- (session_start_utc AT TIME ZONE 'Asia/Kolkata')::date, because VoltHive's
-- business day, its peak tariff windows and its management reporting are all
-- IST. A session at 2026-06-14T19:10Z belongs to business date 2026-06-15.
-- Keying facts on the UTC date instead would misplace roughly a quarter of
-- evening sessions and quietly break every daily revenue number.
--
-- Idempotent: ON CONFLICT DO NOTHING, so re-running extends the calendar
-- without disturbing rows that facts already point at.
-- ===========================================================================

INSERT INTO core.dim_date (
    date_key, full_date, day_of_month, day_of_week, day_name, week_of_year,
    month_num, month_name, quarter_num, year_num, fiscal_year, fiscal_quarter,
    is_weekend, is_month_end, is_quarter_end, is_indian_public_holiday, holiday_name
)
WITH calendar AS (
    SELECT generate_series(DATE '2023-01-01', DATE '2030-12-31', INTERVAL '1 day')::DATE AS d
),

-- Fixed-date national holidays only. Movable festivals (Diwali, Holi, Eid)
-- follow lunar calendars and are deliberately NOT guessed here - an
-- almost-right holiday calendar is worse than an honestly partial one. The
-- column is documented as illustrative in the DDL and in the README.
holidays AS (
    SELECT * FROM (
        VALUES
        (1, 26, 'Republic Day'),
        (8, 15, 'Independence Day'),
        (10, 2, 'Gandhi Jayanti'),
        (12, 25, 'Christmas Day')
    ) AS h (month_num, day_num, holiday_name)
)

SELECT
    to_char(c.d, 'YYYYMMDD')::INT                                     AS date_key,
    c.d                                                               AS full_date,
    extract(DAY FROM c.d)::SMALLINT                                   AS day_of_month,
    extract(ISODOW FROM c.d)::SMALLINT                                AS day_of_week,
    trim(to_char(c.d, 'Day'))                                         AS day_name,
    extract(WEEK FROM c.d)::SMALLINT                                  AS week_of_year,
    extract(MONTH FROM c.d)::SMALLINT                                 AS month_num,
    trim(to_char(c.d, 'Month'))                                       AS month_name,
    extract(QUARTER FROM c.d)::SMALLINT                               AS quarter_num,
    extract(YEAR FROM c.d)::SMALLINT                                  AS year_num,
    -- Indian FY runs April-March and is labelled by its ENDING year:
    -- 2026-04-01 falls in FY2027, 2026-03-31 in FY2026.
    (extract(YEAR FROM c.d) + CASE WHEN extract(MONTH FROM c.d) >= 4 THEN 1 ELSE 0 END)::SMALLINT
    AS fiscal_year,
    (((extract(MONTH FROM c.d)::INT - 4 + 12) % 12) / 3 + 1)::SMALLINT AS fiscal_quarter,
    extract(ISODOW FROM c.d) >= 6                                     AS is_weekend,
    c.d = (date_trunc('month', c.d) + INTERVAL '1 month - 1 day')::DATE AS is_month_end,
    c.d = (date_trunc('quarter', c.d) + INTERVAL '3 months - 1 day')::DATE AS is_quarter_end,
    h.holiday_name IS NOT NULL                                        AS is_indian_public_holiday,
    h.holiday_name
FROM calendar AS c
LEFT JOIN holidays AS h
    ON extract(MONTH FROM c.d) = h.month_num AND extract(DAY FROM c.d) = h.day_num
ON CONFLICT (date_key) DO NOTHING;
