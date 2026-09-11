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

## Symptom: `image "volthive/airflow:2.10.5-local": already exists` during build

Seen as one Airflow service reporting `CANCELED` during export and the others
`ERROR`, with the winner varying between runs.

This should no longer be reachable. It happened when more than one service
declared a `build:` section producing the same image tag. Compose hands builds
to buildx bake, which makes each such service its own target and runs them
**concurrently** — so several targets exported the same image name at once and
the image store rejected all but one. The older, pre-bake builder
deduplicated identical build definitions, which is why the shape worked for
years before failing.

Exactly one service now owns that build (`airflow-init`), and the two
long-running services consume the image it produces. If you see this again,
something has re-added a `build:` to `x-airflow-common` or to a second
service. Confirm with the build plan rather than by reading the file:

```bash
docker compose build --print
```

The `target` object must contain exactly one entry per image tag. Two entries
sharing a `tags` value is the bug.
`tests/unit/test_compose_config.py::test_exactly_one_service_builds_each_image`
asserts this, so it should fail before you ever get here.

Note that this is a build-time collision, **not** a stale-image problem —
`docker system prune` does not prevent it and is not the fix.

---

## Symptom: `service "airflow-init" didn't complete successfully: exit 1`

Postgres is healthy, the image built, and the scheduler and webserver sit at
`Created` because they gate on the bootstrap finishing.

Read the bootstrap's own output first — it is the only place the real error
appears, and `docker compose up` does not show it:

```bash
docker compose logs airflow-init
```

**If it says the user has no username:**

```
airflow.exceptions.AirflowConfigException: The user that Airflow is running as
has no username; you must run Airflow as a full user, with a username and home
directory, in order for it to function properly.
```

then something is running an Airflow container as a UID with no `/etc/passwd`
entry, and without the image's entrypoint, which is what creates that entry
(ADR-017). `AIRFLOW_UID` in `.env` is whoever ran `scripts/generate_env.sh`, so
it is normally *not* a UID the image knows. Check what actually starts:

```bash
docker compose config | grep -E 'entrypoint|command|user:'
```

No Airflow service may set `entrypoint`. The bootstrap runs as
`command: ["bash", "/opt/airflow/init.sh"]`, which the image dispatches after
writing the passwd entry. `tests/unit/test_compose_config.py` asserts this, so
it should fail long before you reach this page.

Do **not** work around it by setting `AIRFLOW_UID=50000`, by running the
service as root, or by deleting containers and volumes. The first two only hide
it for one machine, and none of them is the bug.

**If it says the username already exists:**

```
airflow command error: the user admin already exists
```

then the existence guard in `init.sh` has been rewritten as a pipeline into
`grep -q`. Under `set -o pipefail` that returns 141 rather than 0, because
`grep -q` exits on the first match and the upstream commands take `SIGPIPE`, so
the guard reports "not found" for a user that exists. Capture the list into a
variable and match it with a here-string. Note the shape of this failure: the
first `make up` on an empty database succeeds and every later one fails.

**Anything else:** the bootstrap does three things — `db migrate`, create the
admin user, ensure `warehouse_pool` — and each logs a `[airflow-init]` line
before it starts, so the last line printed names the step that failed. The
container is safe to re-run once the cause is fixed:

```bash
docker compose up airflow-init
```

---

## Symptom: the Airflow UI says "Invalid login"

The username is `admin`. The password is in your own `.env`, which is
git-ignored and generated per machine:

```bash
grep AIRFLOW_ADMIN_PASSWORD .env
```

It is deliberately **not** in `.env.example` and is never printed by the
bootstrap or by `generate_env.sh`, so there is nowhere else to look and nothing
to guess. If that value is what you typed and it still fails, work down:

**Does the account exist, and is it an Admin?**

```bash
docker compose run --rm airflow-init bash -c 'airflow users list'
```

**Is the stack reading the `.env` you just edited?** Compose reads it at
`up` time, not at login time — an edit needs a restart:

```bash
docker compose config | grep AIRFLOW_ADMIN_USER   # what the container will get
make up                                            # reconciles the password
```

The bootstrap resets the admin password from `.env` on **every** run, so a
disagreement between the file and the metadata database cannot persist past one
`make up`. If it does, the bootstrap did not run — check that it completed:

```bash
docker compose logs airflow-init | tail -20
```

Its last lines name the UI URL and the username. They never name the password.

**Do not** "fix" this by disabling authentication, by setting
`AIRFLOW__WEBSERVER__AUTHENTICATE=False`, or by deleting the pgdata volume.
The first two remove the control rather than the fault; the third destroys the
warehouse to reset a password that one `make up` already resets.

---

## Symptom: the local credentials need rotating

Every password in `.env` starts life as the same visible placeholder. Replacing
them is one command:

```bash
bash scripts/rotate_credentials.sh --dry-run   # names the keys, shows no values
bash scripts/rotate_credentials.sh --yes
```

**Do not just edit `.env`.** The PostgreSQL roles are created by
`docker/postgres/init/01_create_roles.sh`, which the postgres image runs exactly
once — on the first boot of an empty `pgdata` volume. It never runs again while
that volume survives, so after the first boot `.env` is not the source of truth
for role passwords; it is only what the clients send. Rewrite it alone and every
component starts presenting a credential the server no longer accepts:

```
FATAL:  password authentication failed for user "wh_etl"
```

The script exists because the fix is to `ALTER` the roles in the running server
so they match the new file, in the same operation, before anything restarts.
That keeps the warehouse and changes the credentials. `make clean` also "works"
and destroys every row you have to change a password.

Ordering is the safety property, and it is deliberate: the new file is built
first but staged as `.env.new`; the roles are altered next, authenticating with
the credential still live in the running container; and `.env` is only replaced
once that transaction commits. A failure at any point leaves the stack exactly
as it was. The previous file is kept as `.env.backup.<timestamp>` — git-ignored,
and still a live credential until you delete it.

Afterwards:

```bash
bash scripts/verify_stack.sh          # every role can do exactly what it should
bash scripts/check_airflow_login.sh   # the UI accepts the new admin password
make run                              # the pipeline still connects
```

**The Fernet key.** Rotating it is safe here only because this project stores
nothing encrypted in the Airflow metadata database — connections are injected as
`AIRFLOW_CONN_*` environment variables precisely so each password exists once.
The script does not take that on trust: it counts rows in `connection` and
`variable` first and refuses to rotate the key if either is non-empty, since a
new key would make those rows permanently undecryptable. Use `--keep-fernet` to
rotate everything else in that case.

---

## Symptom: `relation "audit.pipeline_run" does not exist`

The warehouse schema has not been applied. Starting the stack creates the
roles, the three databases and the extensions — it does **not** create the
schema, deliberately: `docker/postgres/init/*.sh` is infrastructure bootstrap,
`sql/ddl/**` is the schema, and they are separate concerns with separate
failure modes (`docker/airflow/init.sh` says so at the point where the
temptation to merge them is strongest).

So `make up` leaves an empty `warehouse` database, and the first thing the
pipeline does is insert into `audit.pipeline_run`.

```bash
make db-init      # apply the schema, seeds and data-quality rules
```

`make run` assumes both the schema **and** generated data already exist. From
an empty database the one command that does everything is:

```bash
make run-clean    # db-init, generate, run
```

The pipeline now refuses to start in this state and says which schemas are
missing and which command applies them, rather than failing fifteen frames deep
inside the first INSERT. If you see the raw `UndefinedTable` error again,
`require_initialised_warehouse` has been removed from `pipeline.open_run`.

**A related symptom with the same cause:** `make run` succeeds as far as the
data-quality gate and then reports

```
dq_gate_blocked  rules=["ROWCOUNT_ZERO_SESSION"]
```

That is the gate doing its job — the schema exists but nothing was ever
generated, so there are no sessions to publish. Run `make generate` (or
`make run-clean`), not `--skip-dq-gate`.

`make db-init` is idempotent: every DDL statement is `CREATE ... IF NOT
EXISTS`, and the data-quality rules are re-synced from `configs/dq_rules.yml`,
so re-running it is a cheap way to bring an existing database back in line with
the repository.

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
