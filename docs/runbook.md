# Runbook

What to do when something is wrong. Written for the person on call at 07:00
who did not build this, so every entry names the symptom first, then the
command, then the reasoning.

The general rule: **this warehouse is safe to re-run.** Every stage is
idempotent and proved so by checksum in `tests/e2e`. Re-running a failed run
over the same window is almost always the correct first move, and it is never
the destructive one.

---

## Where to look first

```sql
-- Did it run, did it succeed, how long, how much moved.
SELECT * FROM mart.v_pipeline_health ORDER BY run_date DESC LIMIT 10;

-- Which rules fired, and what they said.
SELECT * FROM mart.v_dq_scorecard WHERE check_date >= current_date - 3
ORDER BY check_date DESC, failed DESC;

-- What is piling up in quarantine, and how long it has been sitting.
SELECT * FROM mart.v_quarantine_summary
ORDER BY oldest_untriaged_at_utc NULLS LAST;

-- Everything one run touched. The run id is in every log line.
SELECT * FROM audit.task_run WHERE pipeline_run_id = '<uuid>' ORDER BY started_at_utc;
SELECT * FROM audit.load_stat WHERE pipeline_run_id = '<uuid>' ORDER BY created_at_utc;
```

Every log line carries `pipeline_run_id`, and every warehouse row carries it
as `dw_run_id`. "Which run produced this row?" is a `WHERE` clause:

```sql
SELECT dw_run_id, count(*) FROM core.fact_charging_session
WHERE dw_batch_key = DATE '2026-06-03' GROUP BY 1;
```

---

## Symptom: the mart did not update, but nothing failed

**Almost certainly the data-quality gate held it back.** That is the gate
doing its job: an error-severity rule failed, so consumers keep yesterday's
correct data rather than receiving today's wrong data.

```bash
python scripts/dq_report.py
```

```sql
SELECT rule_code, severity, status, rows_failed, observed_value, message
FROM dq.check_result
WHERE dw_run_id = '<uuid>' AND status <> 'PASS'
ORDER BY severity, rule_code;
```

**Decide, do not reflexively override.** Two outcomes:

- *The rule is right and the data is bad.* Fix the upstream problem, then
  re-run. The window is restated cleanly.
- *The rule is right and the data is unusual but acceptable* — a genuine
  demand spike tripping the row-count anomaly rule, say. Publish once with the
  override, then adjust the rule's threshold in `configs/dq_rules.yml` so the
  next run does not need a human:

  ```bash
  python scripts/run_pipeline.py --from 2026-06-03 --to 2026-06-03 \
      --stages mart --skip-dq-gate
  ```

  `--skip-dq-gate` is logged loudly and recorded on the run. It is an override,
  not a workaround, and a run that used it should be visible in review.

Never edit a rule to make it pass for one bad day. A threshold changed to
silence a real signal is a rule everybody learns to ignore.

---

## Symptom: a task failed with a database error

1. Read the **whole** error, including the `DETAIL:` line. PostgreSQL names
   the constraint and the offending key; that is usually the entire diagnosis.
2. Check whether the run left partial state:

   ```sql
   SELECT status, error_summary FROM audit.pipeline_run WHERE pipeline_run_id = '<uuid>';
   ```

   It will not have. Each stage commits in its own transaction, and a failure
   rolls back its stage's data **and** its watermark together — there is no
   state in which rows landed but the pipeline forgot.
3. Re-run the same window. Clear the Airflow task and let it retry, or:

   ```bash
   python scripts/run_pipeline.py --from <date> --to <date>
   ```

If it fails identically, it is a code or data problem, not a transient one —
go to the specific symptoms below.

---

## Symptom: `duplicate key value violates unique constraint "pk_stg_..."`

This means a staged row moved out of the window that wrote it. It should no
longer be possible — the staging restatement is scoped by business key
precisely to prevent it (ADR-014) — so treat it as a real defect rather than
as something to work around.

Diagnose by finding where the row actually sits:

```sql
SELECT source_system, dw_batch_key FROM stg.session WHERE transaction_id = '<id>';
SELECT dw_batch_key, dw_source_file FROM raw.ocpp_cdr WHERE transaction_id = '<id>';
```

If the staged `dw_batch_key` is outside the window being reprocessed, the
key-scoped delete has been narrowed or removed. Do **not** widen the window to
get past it; that hides the bug and makes the next narrow run fail again.

---

## Symptom: orphaned meter intervals

```sql
SELECT count(*) FROM core.fact_meter_interval AS i
LEFT JOIN core.fact_charging_session AS f
    ON f.charging_session_sk = i.charging_session_sk
WHERE f.charging_session_sk IS NULL;
```

Non-zero means intervals reference a session surrogate key that no longer
exists — their energy has silently dropped out of every join. The
`FACT_METER_ORPHAN_SESSION` rule is error-severity and should have blocked the
publish.

Cause is almost always a restatement window whose upper edge fell between a
session's business date and its intervals' (a session crossing midnight IST).
Fix by restating a window that covers both:

```bash
python scripts/restate.py --from <session_date> --to <session_date + 1>
```

---

## Symptom: quarantine is growing

Growth is not automatically a problem — the generator injects defects on
purpose, and a real source has a baseline defect rate. What matters is the
*rate* and the *untriaged* count.

```bash
psql -d warehouse -f sql/analytics/10_quarantine_rate_by_rule.sql
```

`month_over_month_change_pp` is in percentage points. A jump concentrated in
one rule means one upstream thing changed; a jump spread across many rules
usually means the source changed shape — check `SCHEMA_DRIFT_*`.

Rows accumulating in `NEW` status mean nobody is looking at them, and a
quarantine nobody reads is a delete with extra storage costs. Triage:

```sql
SELECT rule_code, rule_detail, raw_payload
FROM dq.quarantine_ocpp_cdr WHERE status = 'NEW' ORDER BY quarantined_at_utc LIMIT 20;
```

Once the upstream fix has landed and corrected records have been re-ingested:

```bash
python scripts/requeue_quarantine.py --rule CDR_MISSING_STOP --dry-run
python scripts/requeue_quarantine.py --rule CDR_MISSING_STOP --since 2026-06-01
```

Rows a human has already triaged are never erased by a reprocess — the
staging delete only clears `status = 'NEW'`, because a triaged row is a record
of work done.

---

## Symptom: a transformation has been wrong for weeks

This is what the immutable `raw` layer is for. No source system is contacted,
and it does not matter whether the OCPP central system still holds June's
records — the landing zone does, byte for byte, and so does `raw`.

```bash
python scripts/restate.py --from 2026-06-01 --to 2026-06-30 --dry-run
python scripts/restate.py --from 2026-06-01 --to 2026-06-30
```

The dry run reports what would be rewritten before rewriting it.

`restate.py` deliberately does **not** re-run the dimension merges. A
restatement fixes *facts* by re-resolving them against the dimension history
that already exists, which is correct. See the next entry for the other case.

---

## Symptom: the SCD2 merge logic itself was wrong

Distinct from the above, and far more destructive. Symptoms: chains with the
wrong effective dates, a tracked attribute that should have been versioned and
was overwritten, or overlapping validity intervals (which the exclusion
constraint should have made impossible — if you see one, the constraint is
missing, check `btree_gist`).

```sql
-- Chain health, per dimension.
SELECT charge_point_id, count(*) AS versions,
       count(*) FILTER (WHERE is_current) AS current_rows
FROM core.dim_charge_point WHERE charge_point_sk > 0
GROUP BY charge_point_id HAVING count(*) FILTER (WHERE is_current) <> 1;
```

Rebuilding history replays every raw version in `updated_at` order through the
corrected merge:

```bash
python scripts/rebuild_dimension_history.py --dim charge_point --dry-run
python scripts/rebuild_dimension_history.py --dim charge_point --yes
```

**Understand the cost before running it.** Rebuilding a dimension reissues
every surrogate key, so every fact referencing them is deleted and rebuilt in
the same transaction. There is no surgical version of this operation, and the
script says so before it starts. It is atomic — the warehouse ends up fully
rebuilt or completely untouched — and it converges, so re-running it after a
failure is safe.

Afterwards, always:

```bash
python scripts/dq_report.py
```

---

## Symptom: a backfill or bootstrap is needed

Initial load and backfill are the same operation; the watermarks start at the
beginning-of-time sentinel so the first chunk simply matches everything.

```bash
bash scripts/backfill.sh --from 2025-01-01 --to 2026-06-30 --dry-run
bash scripts/backfill.sh --from 2025-01-01 --to 2026-06-30 --chunk-days 30
```

Chunking is the point: each chunk commits independently and advances its own
watermark, so a crash at chunk 14 resumes from chunk 14 rather than from
nothing. If a chunk fails, the script prints the exact command to resume.

Dimension merges are skipped during a backfill, deliberately — see above and
ADR-014. To run it through Airflow instead, so each interval gets its own
retries and UI entry:

```bash
bash scripts/backfill.sh --from 2025-01-01 --to 2026-06-30 --airflow
```

---

## Symptom: two runs are fighting

`max_active_runs=1` stops concurrent DAG runs, and a PostgreSQL advisory lock
stops a manual `run_pipeline.py` from colliding with a scheduled one. If a run
is blocked waiting for the lock:

```sql
SELECT pid, application_name, state, query_start, left(query, 80)
FROM pg_stat_activity WHERE datname = 'warehouse' ORDER BY query_start;

SELECT * FROM pg_locks WHERE locktype = 'advisory';
```

`application_name` is set to the task id on every connection, so the holder is
identifiable without correlating timestamps in the Airflow UI. The lock is
released by a task with `trigger_rule='all_done'`, so a failed run still
releases it; a lock still held with no matching backend means a process was
killed, and the lock will clear when its session ends.

---

## Symptom: no partitions for next month

`fact_meter_interval` is range-partitioned monthly and has **no default
partition** — deliberately, because a default partition turns "we forgot to
create next month" into rows silently landing in the wrong place.

The weekly maintenance DAG creates partitions well ahead. If it has not run:

```sql
SELECT core.ensure_meter_interval_partitions(DATE '2027-01-01', DATE '2027-12-31');
```

The function is idempotent.

---

## Symptom: the stack will not start

```bash
make ps            # what is unhealthy
make logs          # follow everything
make verify        # assert the privilege model is what it claims
```

`make up` uses `--wait`, so it returns only when every service is healthy. If
it times out, the logs name the service. The most common causes are a missing
`.env` (run `bash scripts/generate_env.sh`, which writes it) and a port
5432 already in use on the host — change `POSTGRES_HOST_PORT` in `.env`.

To start completely fresh, destroying the data volume:

```bash
make clean && make up && make run-clean
```

---

## Things that are safe

- Re-running any stage over any window.
- `restate.py` over any range.
- `backfill.sh`, including re-running a range that already succeeded.
- `rebuild_dimension_history.py` without `--yes` (it previews and exits).
- `requeue_quarantine.py --dry-run`.

## Things that are not

- `make clean` — destroys the data volume.
- `rebuild_dimension_history.py --yes` — deletes and rebuilds every fact.
- `--skip-dq-gate` — publishes data a rule said was wrong. Sometimes correct,
  never routine, always visible in the audit trail.
