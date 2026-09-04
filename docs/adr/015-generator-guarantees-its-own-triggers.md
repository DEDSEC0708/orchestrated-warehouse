# ADR-015: The generator guarantees the phenomena the tests assert on

- **Status:** Accepted
- **Date:** 2026-09-04 (Phase 10)
- **Related:** ADR-005 (synthetic data generator), ADR-011 (ground-truth oracle)

## Context

The test suite's strongest assertions are about behaviours that only exist if
the synthetic data contains the phenomenon being tested:

- the SCD Type 2 before/after analytics query needs a charge point whose rated
  power actually changed mid-window
- `MTR_NEGATIVE_INTERVAL_ENERGY` needs a meter register that actually goes
  backwards
- every one of the 21 catalogued defect types needs at least one occurrence

Rates are declared as percentages of a population. On the `default` profile
(120 stations, 25,000 customers, ~1M sessions) a 0.4% rate produces thousands
of instances. On the `tiny` profile used by CI — 7 days, 6 stations, 200
customers, ~540 sessions — the same rate produces an expected count near
**one**, and a Poisson-distributed count near one is zero a large fraction of
the time.

A test whose trigger was never generated does not fail. It **passes over an
empty set**, which is the worst possible outcome for a suite whose entire
purpose is proving those rules fire. Three concrete instances were found:

1. `charge_point_power_upgrade_pct` is 6%, and only DC devices that shipped at
   30 kW are eligible — about a quarter of the fleet. The effective rate is
   ~1.7%, so `tiny`'s ~60 devices produced **no upgrade at all** in a large
   minority of runs. The headline demonstration of the whole Type 2 apparatus
   was present only on average.
2. `cms_updated_before_created_pct` is 0.4% over 200 customers: 0.8 expected.
   It vanished the moment an unrelated change shifted the RNG stream, and the
   "every defect type is produced" test went red for a reason that had nothing
   to do with the change.
3. The injected meter reset subtracted a flat 500–5,000 Wh from the register.
   On a fast DC session a five-minute interval adds more than that, so the
   register still moved **forwards** and no negative interval was produced —
   while the ground-truth ledger counted a defect the data did not contain.

## Decision

**The generator must guarantee, not merely expect, every phenomenon a test
asserts on.** Three mechanisms:

1. **`change_rate_overrides` per profile**, mirroring the existing
   `defect_overrides`. Dimension change rates get the same amplification
   treatment as defect rates, with the same validation.
2. **Every rate in the `tiny` profile is set so its expected count over that
   profile's population is at least ~8**, which puts the probability of zero
   occurrences below 0.1%. The numbers differ per defect because the
   populations differ — 0.2% is generous against 11,000 meter samples and
   useless against 200 customers.
3. **An injected defect must be unconditional, not probabilistic.** The meter
   reset now subtracts the interval's own energy gain *plus* a margin, so the
   register goes backwards whatever the session's power.

Each guarantee has a test that holds it:
`test_the_power_upgrade_is_always_generated`,
`test_every_injected_meter_reset_produces_a_negative_interval` (which asserts
the ledger count and the observable backwards jumps agree **exactly**), and
`test_every_defect_type_is_produced_by_the_tiny_profile`.

## Alternatives rejected

**Seed the fixtures directly instead of generating them.** Hand-written
fixtures would guarantee the phenomena trivially, and would also stop being
representative the moment the pipeline changed. The generator's value is that
the data has the *shape* of real telemetry, defects included.

**Widen the `tiny` window until the rates work out.** That trades CI time for
statistical comfort and does not remove the failure mode — it only makes it
rarer, which is worse, because a test that fails once a month is a test people
learn to re-run rather than read.

**Loosen the assertions.** Tolerances are how a suite stops detecting
anything. The meter reconciliation test previously used `rel=0.02`, and 2% of
a 25 kWh session is 500 Wh — large enough to hide an entire injected meter
reset, and it did. It now uses an absolute 0.5 Wh tolerance covering only
two-decimal rounding, and excludes reset sessions explicitly while asserting
that the exclusion is non-empty.

## Consequences

- The `tiny` profile is not statistically representative, and is not meant to
  be. It is a *fixture* whose defect rates are chosen for coverage. `small`
  and `default` carry the realistic rates.
- One visible side effect: `DIM_INFERRED_MEMBER_RATIO` warns on `tiny`,
  because that profile amplifies the "device not yet in the CMS" defect to
  ~1.5%. The warning is correct and the README says so; it does not fire on
  the realistic profiles.
- Changing a rate in `tiny` is now a change that can break CI on purpose,
  which is the point: the override is load-bearing, and a test says so.
