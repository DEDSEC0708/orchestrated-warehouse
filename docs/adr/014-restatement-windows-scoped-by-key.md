# ADR-014: Restatement windows are scoped by business key, not by delivery date

- **Status:** Accepted
- **Date:** 2026-09-04 (Phase 11)
- **Related:** ADR-004 (EL-then-T), ADR-009 (idempotency mechanisms)

## Context

Every layer below `raw` is restated by delete-insert over a window:

```sql
DELETE FROM stg.session WHERE dw_batch_key BETWEEN :batch_lo AND :batch_hi;
INSERT INTO stg.session SELECT ... FROM raw.ocpp_cdr
WHERE dw_batch_key BETWEEN :batch_lo AND :batch_hi;
```

That shape is correct only if a row cannot move between windows. It can.

`dw_batch_key` is a **delivery** date — the partition or extract a row arrived
in. The staging tables are keyed on **business** keys: `stg.session` on
`transaction_id`, `stg.partner_session` on `cdr_id`. The two disagree the
moment a source re-delivers a key, which all three file and API sources do by
design:

- a corrected OCPP record arrives later with `record_version = 2`
- a roaming partner restates a record for up to seven days
- the dedupe correctly prefers the later version

so the staged row ends up sitting at the **correction's** batch key, not the
original's.

Reprocess the original window afterwards and the `DELETE` matches nothing —
the row has moved — while the `INSERT` re-reads the superseded original:

```
psycopg.errors.UniqueViolation: duplicate key value violates unique
constraint "pk_stg_partner_session"
```

This was not theoretical. It was found by running `scripts/backfill.sh` in
three-day chunks over a window that a single full run had already processed
successfully. Chunk one failed immediately. Every test in the suite passed,
because every test ran the full window.

The same class of error exists between the two transaction facts.
`fact_charging_session` is windowed on the session's IST business date and
`fact_meter_interval` on the interval's. A session starting at 23:40 IST has
intervals on the *following* business date, so a window whose upper edge falls
between them reissues the session's surrogate key while leaving its intervals
pointing at a key that no longer exists. There is no foreign key to catch it
(ADR-008), so the intervals stop joining and their energy silently disappears
from every total.

## Decision

**A restatement window is defined by the set of business keys it touches, in
addition to the date range.**

Concretely, in `sql/stg/10_session.sql` and `sql/stg/11_partner_session.sql`:

1. `keys_in_window` — a temp table of every business key delivered anywhere in
   the date window.
2. The `DELETE` removes staged rows for those keys **whichever batch they
   currently sit in**, plus anything still in the date window, so a key whose
   source disappeared does not linger.
3. The `INSERT` reads **every** raw row for those keys, from every batch, so
   the dedupe can still see a revision that arrived after the window being
   reprocessed.

And in `sql/core/load_fact_meter_interval.sql`, the delete-insert additionally
covers every interval belonging to a session this run rewrote, whichever date
the interval falls on. The `date_key` predicate is kept alongside it because
it is what lets PostgreSQL prune to the affected monthly partitions; the
transaction scope is an addition, not a replacement.

## Alternatives rejected

**Read only the window's own raw rows, and delete by key.** Fixes the crash in
two lines. It also silently reverts a corrected session to its superseded
values whenever a narrow window is reprocessed — turning a loud failure into a
quiet wrong number, which is strictly worse.

**Make `dw_batch_key` the business date everywhere.** Removes the divergence,
but breaks the thing the delivery date is for: a revision arriving on 20 June
for a 1 June session would be staged at 1 June and then fall outside the fact
restatement window of the run that read it, so the correction would never
reach the facts at all.

**Upsert instead of delete-insert.** `ON CONFLICT DO UPDATE` with a recency
guard would avoid the collision, but a row whose source row vanished would
never be removed, and staging would stop being a faithful rebuild of the
window.

## Consequences

- Reprocessing any window, in any order, converges on the same staged rows.
  `scripts/backfill.sh` in three-day chunks now produces byte-identical facts
  to a single full run — asserted in `tests/e2e`.
- One extra pass over the raw table per staging step, restricted to the keys
  in the window. Negligible at this project's volume; at a much larger one the
  same shape holds with an index on the promoted key column.
- The reasoning is written at the top of each affected SQL file, because the
  naive form is what a reader would otherwise expect to find and "correct"
  back.
- `tests/integration/test_facts.py::test_a_window_that_ends_mid_session_leaves_no_orphan`
  reproduces the interval case specifically: it finds a session that spans
  midnight, restates a window ending on the session's own date, and asserts
  zero orphans. Reverting the fix makes it fail with 44 orphans.
