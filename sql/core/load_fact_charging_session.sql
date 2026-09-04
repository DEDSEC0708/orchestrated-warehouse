-- ===========================================================================
-- load_fact_charging_session.sql - the transaction fact.
--
-- Parameters: :run_id, :batch_lo, :batch_hi
--
-- ===========================================================================
-- THE POINT-IN-TIME JOIN
-- ===========================================================================
-- Every dimension key is resolved AT SESSION START, not as of now:
--
--     JOIN core.dim_charge_point AS cp
--       ON  cp.charge_point_id   = s.charge_point_id
--       AND s.session_start_utc >= cp.effective_from_utc
--       AND s.session_start_utc <  cp.effective_to_utc
--
-- Three things to notice, all deliberate:
--
--   * it is SESSION_START_UTC - the EVENT time. Not now(), not the load date.
--     The version is chosen by when the thing happened.
--   * the interval is HALF-OPEN (>= from, < to), so consecutive versions abut
--     exactly: no gaps, no overlaps, no microsecond arithmetic.
--   * the join is on BUSINESS KEY plus time range, which is why the index
--     (charge_point_id, effective_from_utc, effective_to_utc) exists.
--
-- The tariff is resolved through the charge point's tariff_plan_id AS IT WAS
-- AT SESSION START - so a device moved to a different price list in April
-- still prices its March sessions from the March plan.
--
-- ===========================================================================
-- IDEMPOTENCY: DELETE-INSERT OVER A RESTATEMENT WINDOW
-- ===========================================================================
-- The window is deleted and rewritten in full. Upsert alone would be wrong,
-- and the reason is specific: it cannot remove a row that DISAPPEARED from the
-- window - a session that was valid yesterday and is quarantined today because
-- a corrected CDR revealed it as invalid. Delete-insert makes the window's
-- contents exactly equal to what current logic produces from current raw data,
-- which is a stronger and simpler guarantee.
--
-- The cost is rewriting rows that did not change. At roughly 1,900 sessions a
-- day that is irrelevant. The honest statement is that at a hundred times this
-- volume it would become a merge with a change-detection hash, or a partition
-- swap.
--
-- The window runs to :batch_hi + 1 day because the IST business date of a
-- session can be one day AHEAD of the UTC partition its file landed in - a
-- session at 19:10 UTC on the 14th is business date the 15th.
--
-- ===========================================================================
-- NO NULL FOREIGN KEYS
-- ===========================================================================
-- Every LEFT JOIN falls back to an explicit member:
--   -1 UNKNOWN         we could not resolve the key
--   -2 NOT APPLICABLE  the relationship does not exist (an OUTBOUND roaming
--                      session has no VoltHive charge point at all)
-- Keeping those distinct is what makes "how many sessions have an
-- unidentified charger" a query rather than a mystery.
-- ===========================================================================

DELETE FROM core.fact_charging_session
WHERE dw_batch_key BETWEEN :batch_lo::DATE AND (:batch_hi::DATE + 1);

INSERT INTO core.fact_charging_session (
    transaction_id, start_date_key, start_hour_key, end_date_key,
    customer_sk, vehicle_sk, station_sk, charge_point_sk, tariff_plan_sk,
    session_outcome_sk, session_start_utc, session_end_utc,
    energy_delivered_kwh, duration_seconds, charging_seconds, idle_seconds,
    peak_power_kw, avg_power_kw,
    energy_charge_inr, time_charge_inr, idle_fee_inr, discount_inr, gst_inr,
    gross_revenue_inr, grid_energy_cost_inr, gross_margin_inr, session_count,
    is_roaming, source_system, has_meter_detail, dw_run_id, dw_batch_key
)
WITH meter_rollup AS (
    -- Charging time is NOT session duration. A car plugged in for four hours
    -- may draw power for forty minutes; the difference is idle time, and it is
    -- what idle fees exist to price. Deriving it from the interval fact is the
    -- only way to know it - the session header cannot tell you.
    SELECT
        mi.transaction_id,
        sum(mi.interval_seconds)                            AS charging_seconds,
        max(mi.avg_power_kw)                                AS peak_power_kw,
        sum(mi.interval_energy_kwh)                         AS metered_energy_kwh
    FROM stg.meter_interval AS mi
    GROUP BY mi.transaction_id
),

resolved AS (
    SELECT
        s.transaction_id,
        s.session_start_utc,
        s.session_end_utc,
        s.energy_delivered_kwh,
        s.duration_seconds,
        s.business_date_ist,
        s.source_system,
        s.is_roaming,
        s.partner_cost_inr,
        s.session_status,
        s.stop_reason,
        s.auth_method,

        -- Point-in-time dimension resolution.
        COALESCE(cp.charge_point_sk, CASE WHEN s.charge_point_id IS NULL THEN -2 ELSE -1 END)
        AS charge_point_sk,
        COALESCE(st.station_sk, CASE WHEN s.charge_point_id IS NULL THEN -2 ELSE -1 END)
        AS station_sk,
        COALESCE(tp.tariff_plan_sk, CASE WHEN s.charge_point_id IS NULL THEN -2 ELSE -1 END)
        AS tariff_plan_sk,
        COALESCE(cu.customer_sk, CASE WHEN s.customer_id IS NULL THEN -2 ELSE -1 END)
        AS customer_sk,
        COALESCE(v.vehicle_sk, CASE WHEN s.vehicle_id IS NULL THEN -2 ELSE -1 END)
        AS vehicle_sk,
        COALESCE(o.session_outcome_sk, -1)                  AS session_outcome_sk,

        -- Pricing attributes, captured from the version in effect at start.
        COALESCE(tp.price_per_kwh_inr, 0)                   AS price_per_kwh_inr,
        COALESCE(tp.price_per_minute_inr, 0)                AS price_per_minute_inr,
        COALESCE(tp.idle_fee_per_minute_inr, 0)             AS idle_fee_per_minute_inr,
        COALESCE(tp.min_billable_kwh, 0)                    AS min_billable_kwh,
        COALESCE(tp.gst_rate_pct, 0)                        AS gst_rate_pct,
        cu.subscription_plan,
        st.state                                            AS station_state,

        COALESCE(mr.charging_seconds, 0)                    AS charging_seconds,
        mr.peak_power_kw,
        mr.transaction_id IS NOT NULL                       AS has_meter_detail
    FROM stg.session AS s
    LEFT JOIN core.dim_charge_point AS cp
        ON cp.charge_point_id = s.charge_point_id
        AND s.session_start_utc >= cp.effective_from_utc
        AND s.session_start_utc < cp.effective_to_utc
    LEFT JOIN core.dim_station AS st
        ON st.station_id = cp.station_id
        AND s.session_start_utc >= st.effective_from_utc
        AND s.session_start_utc < st.effective_to_utc
    LEFT JOIN core.dim_tariff_plan AS tp
        ON tp.tariff_plan_id = cp.tariff_plan_id
        AND s.session_start_utc >= tp.effective_from_utc
        AND s.session_start_utc < tp.effective_to_utc
    LEFT JOIN core.dim_customer AS cu
        ON cu.customer_id = s.customer_id
        AND s.session_start_utc >= cu.effective_from_utc
        AND s.session_start_utc < cu.effective_to_utc
    -- Vehicle is Type 1: there is only ever one row per key, so no time
    -- predicate applies. The asymmetry is visible here on purpose.
    LEFT JOIN core.dim_vehicle AS v ON v.vehicle_id = s.vehicle_id
    LEFT JOIN core.dim_session_outcome AS o
        ON o.session_status = s.session_status
        AND o.stop_reason = COALESCE(s.stop_reason, 'UNKNOWN')
        AND o.auth_method = COALESCE(s.auth_method, 'UNKNOWN')
        AND o.is_roaming = s.is_roaming
    LEFT JOIN meter_rollup AS mr ON mr.transaction_id = s.transaction_id
    WHERE s.business_date_ist BETWEEN :batch_lo::DATE AND (:batch_hi::DATE + 1)
),

priced AS (
    SELECT
        r.*,
        -- A tariff can set a minimum billable quantity, so a two-minute top-up
        -- is still charged for the minimum. Real, and a good reason the fact
        -- stores both the energy DELIVERED and the money CHARGED rather than
        -- letting a consumer multiply one by a rate and get a different answer.
        GREATEST(r.energy_delivered_kwh, r.min_billable_kwh)  AS billable_kwh,
        GREATEST(r.duration_seconds - r.charging_seconds, 0)  AS idle_seconds_raw
    FROM resolved AS r
),

charged AS (
    SELECT
        p.*,
        round(p.billable_kwh * p.price_per_kwh_inr, 2)                AS energy_charge_inr,
        round(p.duration_seconds * p.price_per_minute_inr / 60.0, 2) AS time_charge_inr,
        -- Idle fees start after a ten-minute grace period. Charging for the
        -- first second a car sits idle would be commercially absurd, and the
        -- grace period is exactly the kind of business rule that belongs in
        -- one place with a name rather than scattered through queries.
        round(
            GREATEST(p.idle_seconds_raw - 600, 0) * p.idle_fee_per_minute_inr / 60.0, 2
        )                                                             AS idle_fee_inr
    FROM priced AS p
),

totalled AS (
    SELECT
        c.*,
        -- Subscription discount on the energy and time components only. Idle
        -- fees are a penalty and are deliberately not discounted.
        round(
            (c.energy_charge_inr + c.time_charge_inr)
            * CASE c.subscription_plan
                WHEN 'PLUS' THEN 0.05
                WHEN 'FLEET_PRO' THEN 0.12
                ELSE 0.00
            END,
            2
        ) AS discount_inr
    FROM charged AS c
),

final_amounts AS (
    SELECT
        t.*,
        round(
            (t.energy_charge_inr + t.time_charge_inr + t.idle_fee_inr - t.discount_inr)
            * t.gst_rate_pct / 100.0, 2
        ) AS gst_inr,
        -- What VoltHive PAID the grid for the same energy, in the same state,
        -- under the slab in force on the session's business date. This is what
        -- turns gross_margin_inr into a real derived measure rather than a
        -- second name for revenue.
        round(t.energy_delivered_kwh * COALESCE(g.commercial_rate_inr_per_kwh, 0), 2)
        AS grid_energy_cost_inr
    FROM totalled AS t
    LEFT JOIN stg.grid_tariff_slab AS g
        ON g.state = t.station_state
        AND t.business_date_ist >= g.effective_from_date
        AND t.business_date_ist < g.effective_to_date
)

SELECT
    f.transaction_id,
    to_char(f.business_date_ist, 'YYYYMMDD')::INT,
    -- The hour dimension is the IST hour of day, for the same reason the date
    -- is the IST business date: "the evening peak" is an IST concept.
    extract(HOUR FROM (f.session_start_utc AT TIME ZONE 'Asia/Kolkata'))::SMALLINT,
    to_char((f.session_end_utc AT TIME ZONE 'Asia/Kolkata')::DATE, 'YYYYMMDD')::INT,
    f.customer_sk,
    f.vehicle_sk,
    f.station_sk,
    f.charge_point_sk,
    f.tariff_plan_sk,
    f.session_outcome_sk,
    f.session_start_utc,
    f.session_end_utc,
    f.energy_delivered_kwh,
    f.duration_seconds,
    f.charging_seconds,
    f.idle_seconds_raw,
    f.peak_power_kw,
    -- avg_power_kw over the time actually spent CHARGING, not over the whole
    -- session. Averaging over plugged-in time would report a 60 kW charger as
    -- a 9 kW one for anyone who left their car overnight.
    CASE
        WHEN f.charging_seconds > 0
            THEN round((f.energy_delivered_kwh * 3600.0) / f.charging_seconds, 3)
    END,
    -- A roaming session was priced by the PARTNER, on the partner's own price
    -- list, and VoltHive's tariff never applied to it. Reporting it under the
    -- component columns would imply a breakdown that does not exist, so the
    -- components are zero and the partner's total is carried as revenue.
    CASE WHEN f.source_system = 'PARTNER' THEN 0 ELSE f.energy_charge_inr END,
    CASE WHEN f.source_system = 'PARTNER' THEN 0 ELSE f.time_charge_inr END,
    CASE WHEN f.source_system = 'PARTNER' THEN 0 ELSE f.idle_fee_inr END,
    CASE WHEN f.source_system = 'PARTNER' THEN 0 ELSE f.discount_inr END,
    CASE WHEN f.source_system = 'PARTNER' THEN 0 ELSE f.gst_inr END,
    CASE
        WHEN f.source_system = 'PARTNER' THEN COALESCE(f.partner_cost_inr, 0)
        ELSE round(
            f.energy_charge_inr + f.time_charge_inr + f.idle_fee_inr
            - f.discount_inr + f.gst_inr, 2
        )
    END,
    f.grid_energy_cost_inr,
    CASE
        WHEN f.source_system = 'PARTNER'
            THEN round(COALESCE(f.partner_cost_inr, 0) - f.grid_energy_cost_inr, 2)
        ELSE round(
            f.energy_charge_inr + f.time_charge_inr + f.idle_fee_inr
            - f.discount_inr + f.gst_inr - f.grid_energy_cost_inr, 2
        )
    END,
    1,
    f.is_roaming,
    f.source_system,
    f.has_meter_detail,
    :run_id::UUID,
    f.business_date_ist
FROM final_amounts AS f;
