## What changed and why

<!-- The reason, not the diff. "Fixed the SCD2 merge" describes the file that
     changed; "a historical version inside the lookback window was quarantined
     as retro-dated, because the merge compared against the CURRENT row rather
     than against what had already been loaded" describes the bug. -->

## How the change was verified

<!-- Name the evidence, not the intention. Which test now fails without this
     change? What did the numbers look like before and after? -->

- [ ] `make lint` (ruff, format, mypy, sqlfluff) passes
- [ ] `make test` (unit + dags) passes
- [ ] `make test-int` (integration, real PostgreSQL) passes
- [ ] `make test-e2e` (idempotency, restatement, rebuild) passes
- [ ] A test was added or changed that FAILS without this diff

## Data-model and pipeline checklist

<!-- Delete the lines that do not apply. Anything left ticked is a claim. -->

- [ ] Grain is unchanged, or the new grain is stated in a `COMMENT ON TABLE`
- [ ] Every new fact foreign key is `NOT NULL` and resolves to a real member,
      an inferred member, UNKNOWN (-1) or NOT APPLICABLE (-2)
- [ ] The load is idempotent: re-running it over the same window changes no
      data (`tests/e2e` proves this by checksum)
- [ ] Restatement windows are scoped so a row cannot migrate out of the window
      that wrote it
- [ ] New or changed SQL carries a comment saying WHY, not what
- [ ] Any new data-quality rule declares its severity and is in `configs/dq_rules.yml`
- [ ] No credential, DSN or token is added to a tracked file

## Anything a reviewer should push back on

<!-- Shortcuts taken, alternatives rejected, things left undone. A pull
     request that lists none of these is usually one that has not been read by
     its author. -->
