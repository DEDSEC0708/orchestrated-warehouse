-- ===========================================================================
-- 32_core_partitions.sql - monthly partitions for core.fact_meter_interval.
--
-- A missing partition is a classic production incident: everything works until
-- midnight on the 1st, when the first insert of the new month fails because
-- nobody created next month's partition. It is pre-empted here in two ways:
--
--   1. A function that creates partitions for a date range, idempotently.
--   2. The maintenance DAG calls it every week for the next three months.
--
-- There is deliberately NO DEFAULT PARTITION. A default partition turns a
-- missing-partition failure - loud, immediate, fixable in one command - into a
-- silent mis-filing that has to be discovered later and is then painful to
-- correct, because Postgres will not let you create an overlapping partition
-- while rows sit in the default. A loud failure is the better trade here.
-- ===========================================================================

CREATE OR REPLACE FUNCTION core.ensure_meter_interval_partitions(
    p_from DATE,
    p_to   DATE
)
RETURNS INT
LANGUAGE plpgsql
AS $$
DECLARE
    month_start DATE := date_trunc('month', p_from)::DATE;
    month_end   DATE;
    part_name   TEXT;
    created     INT := 0;
BEGIN
    IF p_to < p_from THEN
        RAISE EXCEPTION 'ensure_meter_interval_partitions: p_to (%) precedes p_from (%)', p_to, p_from;
    END IF;

    WHILE month_start <= p_to LOOP
        month_end := (month_start + INTERVAL '1 month')::DATE;
        part_name := format('fact_meter_interval_%s', to_char(month_start, 'YYYYMM'));

        IF NOT EXISTS (
            SELECT 1
            FROM pg_class AS c
            INNER JOIN pg_namespace AS n ON n.oid = c.relnamespace
            WHERE n.nspname = 'core' AND c.relname = part_name
        ) THEN
            -- Bounds are date_key integers (YYYYMMDD), inclusive lower and
            -- exclusive upper - the same half-open convention used for SCD2
            -- validity ranges, for the same reason: no gaps, no overlaps.
            EXECUTE format(
                'CREATE TABLE core.%I PARTITION OF core.fact_meter_interval '
                'FOR VALUES FROM (%s) TO (%s)',
                part_name,
                to_char(month_start, 'YYYYMMDD'),
                to_char(month_end, 'YYYYMMDD')
            );
            created := created + 1;
        END IF;

        month_start := month_end;
    END LOOP;

    RETURN created;
END;
$$;

COMMENT ON FUNCTION core.ensure_meter_interval_partitions(DATE, DATE) IS
    'Idempotently creates monthly partitions of core.fact_meter_interval covering [p_from, p_to]. Returns how many were created. Called by the DDL for the initial range and weekly by the maintenance DAG for the next three months.';

-- Initial coverage. Generous enough for the default 18-month generator window
-- plus room either side, small enough that partition-planning overhead stays
-- irrelevant. The maintenance DAG extends it forward from here.
SELECT core.ensure_meter_interval_partitions(DATE '2024-01-01', DATE '2027-12-01');
