-- ===========================================================================
-- 05_functions.sql - safe casting and hashing.
--
-- Two problems solved here, both of which would otherwise be solved badly and
-- repeatedly across a dozen transformation files.
--
-- ---------------------------------------------------------------------------
-- PROBLEM 1: a cast that fails must reject ONE ROW, not the whole statement.
--
-- The raw layer is TEXT so that anything can land, including values that break
-- their own declared type. Staging then has to cast them - and PostgreSQL has
-- no TRY_CAST. A bare `value::NUMERIC` on a million rows aborts the entire
-- INSERT the moment one row contains "12.5 kWh", losing 999,999 good records
-- to one bad one.
--
-- The obvious fix - a plpgsql function with an EXCEPTION block - works, but
-- every EXCEPTION block opens a subtransaction, and a subtransaction per row
-- over eleven million meter samples is genuinely slow.
--
-- So these validate with a regular expression FIRST and cast only what will
-- succeed, returning NULL otherwise. Pure SQL, IMMUTABLE, no subtransactions,
-- inlinable by the planner. The staging layer then treats NULL-where-a-value-
-- was-expected as the quarantine signal, which is why every one of these is
-- paired with an `IS NULL AND source IS NOT NULL` check at its call site: that
-- distinguishes "the source sent nothing" from "the source sent something
-- unparseable", and those are different data-quality stories.
--
-- ---------------------------------------------------------------------------
-- PROBLEM 2: change detection needs a NULL-SAFE, ORDER-SAFE hash.
--
-- core.row_hash exists because the obvious implementation is subtly wrong.
-- `concat_ws('||', a, b, c)` SKIPS nulls, so ('A', NULL, 'B') and
-- ('A', 'B', NULL) produce the same string and therefore the same hash - and
-- two genuinely different dimension versions look unchanged, so the SCD2 merge
-- silently drops a version. There is a unit test named
-- test_row_hash_distinguishes_null_placement for exactly this.
--
-- Note the sentinel avoids the NUL byte: PostgreSQL text cannot contain one,
-- so the usual \0 delimiter is unavailable. Unit and record separators (U+001F
-- and U+001E) cannot occur in JSON, CSV or SQL source data either.
-- ===========================================================================

-- ---------------------------------------------------------------------------
-- core.row_hash(VARIADIC TEXT[]) - sha256 over an ordered value list.
--
-- The ORDER of the arguments is part of the hash's meaning. Reordering a
-- dimension's tracked-column list changes every hash and makes the next merge
-- treat every row as changed, which is why those lists live in exactly one
-- place per dimension.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION core.row_hash(VARIADIC vals TEXT [])
RETURNS CHAR(64)
LANGUAGE SQL
IMMUTABLE
PARALLEL SAFE
AS $$
    SELECT encode(
        sha256(
            convert_to(
                COALESCE(
                    (
                        SELECT string_agg(
                            COALESCE(v.val, E'\x1eNULL\x1e'), E'\x1f' ORDER BY v.ord
                        )
                        FROM unnest(vals) WITH ORDINALITY AS v (val, ord)
                    ),
                    ''
                ),
                'UTF8'
            )
        ),
        'hex'
    )::CHAR(64);
$$;

COMMENT ON FUNCTION core.row_hash(TEXT []) IS
    'NULL-safe, order-safe sha256 over a value list. Used for SCD2 change detection over TYPE-2 TRACKED COLUMNS ONLY - a Type-1 column must never enter the hash, or correcting a typo would create a spurious version.';


-- ---------------------------------------------------------------------------
-- Safe casts. Each returns NULL when the input cannot be represented, and each
-- is IMMUTABLE so the planner can inline and index around it.
-- ---------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION stg.safe_numeric(value TEXT)
RETURNS NUMERIC
LANGUAGE SQL
IMMUTABLE
PARALLEL SAFE
AS $$
    SELECT CASE
        WHEN value IS NULL THEN NULL
        WHEN btrim(value) ~ '^[+-]?[0-9]+(\.[0-9]+)?([eE][+-]?[0-9]+)?$'
            THEN btrim(value)::NUMERIC
    END;
$$;

CREATE OR REPLACE FUNCTION stg.safe_int(value TEXT)
RETURNS INT
LANGUAGE SQL
IMMUTABLE
PARALLEL SAFE
AS $$
    -- Bounded as well as validated: '99999999999' is a perfectly good integer
    -- literal and still overflows INT, which would abort the statement exactly
    -- as an unparseable value would.
    SELECT CASE
        WHEN value IS NULL THEN NULL
        WHEN btrim(value) ~ '^[+-]?[0-9]{1,9}$' THEN btrim(value)::INT
    END;
$$;

CREATE OR REPLACE FUNCTION stg.safe_timestamptz(value TEXT)
RETURNS TIMESTAMPTZ
LANGUAGE SQL
IMMUTABLE
PARALLEL SAFE
AS $$
    -- ISO 8601 with an explicit UTC marker or offset. A timestamp WITHOUT a
    -- zone is rejected rather than assumed: guessing the zone of an ambiguous
    -- instant is how a warehouse ends up 5.5 hours wrong for one source and
    -- nobody can say which one.
    SELECT CASE
        WHEN value IS NULL THEN NULL
        WHEN btrim(value) ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}[T ][0-9]{2}:[0-9]{2}:[0-9]{2}(\.[0-9]+)?(Z|[+-][0-9]{2}:?[0-9]{2})$'
            THEN btrim(value)::TIMESTAMPTZ
    END;
$$;

CREATE OR REPLACE FUNCTION stg.safe_date(value TEXT)
RETURNS DATE
LANGUAGE SQL
IMMUTABLE
PARALLEL SAFE
AS $$
    SELECT CASE
        WHEN value IS NULL THEN NULL
        WHEN btrim(value) ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' THEN btrim(value)::DATE
        WHEN btrim(value) ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}[T ]' THEN left(btrim(value), 10)::DATE
    END;
$$;

CREATE OR REPLACE FUNCTION stg.safe_bool(value TEXT)
RETURNS BOOLEAN
LANGUAGE SQL
IMMUTABLE
PARALLEL SAFE
AS $$
    -- Every spelling PostgreSQL, Python and CSV exporters actually emit.
    -- Anything else is NULL rather than silently FALSE: "we could not tell"
    -- and "the source said no" are different facts.
    SELECT CASE lower(btrim(value))
        WHEN 't' THEN TRUE
        WHEN 'true' THEN TRUE
        WHEN 'y' THEN TRUE
        WHEN 'yes' THEN TRUE
        WHEN '1' THEN TRUE
        WHEN 'f' THEN FALSE
        WHEN 'false' THEN FALSE
        WHEN 'n' THEN FALSE
        WHEN 'no' THEN FALSE
        WHEN '0' THEN FALSE
    END;
$$;

COMMENT ON FUNCTION stg.safe_timestamptz(TEXT) IS
    'Parse an ISO 8601 instant that carries an explicit zone, else NULL. A zone-less timestamp is deliberately rejected rather than assumed.';


-- ---------------------------------------------------------------------------
-- stg.mask_name / stg.email_domain - PII minimisation at the staging boundary.
--
-- The warehouse stores "Priya S." and "example.invalid", never the full name
-- or address. Raw retains what actually arrived (it is evidence, and it is
-- pruned by the 180-day retention policy); everything downstream of staging
-- holds the minimum that answers the analytical question.
--
-- Doing this in a function rather than inline means there is ONE definition of
-- "masked", it is testable, and a new staging load cannot accidentally carry
-- the raw column through.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION stg.mask_name(full_name TEXT)
RETURNS TEXT
LANGUAGE SQL
IMMUTABLE
PARALLEL SAFE
AS $$
    SELECT CASE
        WHEN full_name IS NULL OR btrim(full_name) = '' THEN NULL
        WHEN position(' ' IN btrim(full_name)) = 0 THEN btrim(full_name)
        ELSE split_part(btrim(full_name), ' ', 1)
             || ' '
             || upper(left(split_part(btrim(full_name), ' ', 2), 1))
             || '.'
    END;
$$;

CREATE OR REPLACE FUNCTION stg.email_domain(email TEXT)
RETURNS TEXT
LANGUAGE SQL
IMMUTABLE
PARALLEL SAFE
AS $$
    SELECT CASE
        WHEN email IS NULL OR position('@' IN email) = 0 THEN NULL
        ELSE lower(split_part(btrim(email), '@', 2))
    END;
$$;

COMMENT ON FUNCTION stg.mask_name(TEXT) IS
    'Reduce a full name to a display form ("Priya Sharma" -> "Priya S."). The warehouse never stores the full name; the raw layer keeps what arrived and is pruned by retention.';
