# Data quality rules

Rules are authored here as YAML, synced into `dq.rule` at deploy time, and
executed by the engine in `src/volthive/dq/`. YAML is the source of truth; the
table is the queryable projection of it, so a check result can join to the rule
that produced it.

## Two scopes, two behaviours

| Scope | Runs where | Granularity | On failure |
|---|---|---|---|
| `row` | inside the staging loads | one record | the record goes to `dq.quarantine_*` with its rule code and complete payload; valid records continue |
| `dataset` | after staging, after core, before mart | whole table or window | a row in `dq.check_result`; `warn` is recorded, `error` **blocks the publish gate** |

**Neither ever deletes a row.**

Row-scope rules are declared here but ENFORCED IN THE STAGING SQL, where the
data is already parsed and the rejection can carry the original payload. This
file is their registry: it gives each code a description, a severity and a
foreign-key target for quarantine rows, and a test asserts that every rule code
used in `sql/stg/` exists here. Declaring them in YAML and enforcing them in
SQL is a deliberate split — pushing row filtering into a generic engine would
mean a second pass over eleven million rows to re-find what the first pass
already knew.

## The dataset rule contract

Every `dataset` rule's SQL must return **exactly one row** with:

| Column | Required | Meaning |
|---|---|---|
| `rows_evaluated` | yes | how many rows the rule looked at |
| `rows_failed` | yes | how many violated it |
| `observed_value` | no | a measured number, for threshold rules |

Available parameters: `:batch_lo`, `:batch_hi`, `:run_id`.

A rule fails when `rows_failed` exceeds `max_failed_rows` (default 0), or when
`fail_ratio` exceeds `max_fail_ratio`, or when `observed_value` falls outside
`[min_value, max_value]`. Everything else is a PASS.

`rows_evaluated = 0` is deliberately a PASS, not a failure: a window with no
data is a job for the freshness and zero-row rules, which say so explicitly,
rather than something every other rule should report separately.
