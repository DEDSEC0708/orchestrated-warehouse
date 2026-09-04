# ===========================================================================
# ORCHESTRATED WAREHOUSE - developer interface
#
# PORTABILITY RULE: every recipe line is a single executable invocation with
# no shell syntax (no if/for/pipes/redirects/&&). GNU Make on Windows runs
# recipes through cmd.exe unless a POSIX sh is on PATH, so shell-specific
# syntax would work in Git Bash and silently break in PowerShell. Keeping
# recipes shell-agnostic means `make` behaves identically everywhere.
# For the same reason, no @echo line may contain ( ) | & < > or ; - /bin/sh
# treats them as syntax and `make help` fails with a parse error.
#
# ===========================================================================

.DEFAULT_GOAL := help
.PHONY: help venv install install-dev hooks lint format typecheck sqlfluff test \
        test-unit test-int test-e2e version-check \
        build up down restart clean ps logs psql compose-config verify \
        db-init generate run run-clean dq-report restate backfill rebuild-dims \
        analytics check

help:
	@echo ORCHESTRATED WAREHOUSE - available targets
	@echo   make install       Install runtime dependencies, using constraints.txt
	@echo   make install-dev   Install runtime and dev dependencies, plus editable package
	@echo   make hooks         Install the pre-commit git hooks
	@echo   make lint          Run ruff lint and format checks
	@echo   make format        Auto-fix lint issues and format the code
	@echo   make typecheck     Run mypy over src
	@echo   make sqlfluff      Lint the SQL under sql/
	@echo   make check         Everything a reviewer runs - lint, types, SQL, all tests
	@echo   make test          Fast tests only - unit and DAG integrity, no database
	@echo   make test-unit     Unit tests only
	@echo   make test-int      Integration tests - REQUIRES a running PostgreSQL
	@echo   make test-e2e      End-to-end idempotency and restatement proofs
	@echo   make version-check Print the versions of the required tools
	@echo   make build         Build the Airflow image
	@echo   make up            Start the stack and wait until every service is healthy
	@echo   make ps            Show service status and health
	@echo   make verify        Run the Phase 2 infrastructure verification script
	@echo   make logs          Follow logs from all services
	@echo   make psql          Open a psql shell on the warehouse database
	@echo   make compose-config Validate and render the compose file
	@echo   make down          Stop the stack, KEEPING the data volume
	@echo   make clean         Stop the stack and DELETE the data volume
	@echo   make db-init       Apply the warehouse schema, seeds and DQ rules
	@echo   make generate      Generate the synthetic dataset - PROFILE=tiny default
	@echo   make run           Run the whole pipeline over the generated window
	@echo   make run-clean     db-init, generate and run, from an empty database
	@echo   make analytics     Execute the ten showcase queries against the warehouse
	@echo   make dq-report     Print the data-quality scorecard for the latest run
	@echo   make restate       Replay a window from raw - FROM= and TO= required
	@echo   make backfill      Chunked replay of a range - FROM= and TO= required
	@echo   make rebuild-dims  Rebuild SCD2 history - previews unless YES=1

# --- Setup -----------------------------------------------------------------
# ALWAYS pass -c constraints.txt. Installing Airflow without its official
# constraints file is the most common cause of a broken Airflow environment.

install:
	python -m pip install --upgrade pip
	python -m pip install -r requirements.txt -c constraints.txt

install-dev:
	python -m pip install --upgrade pip
	python -m pip install -r requirements-dev.txt -c constraints.txt
	python -m pip install -e . --no-deps

hooks:
	pre-commit install

# --- Quality gates ---------------------------------------------------------

lint:
	ruff check .
	ruff format --check .

format:
	ruff check --fix .
	ruff format .

typecheck:
	mypy src

sqlfluff:
	sqlfluff lint sql/

# The default `test` target is deliberately the FAST one. A developer who has
# to wait for a database to come up before finding a typo stops running tests.
test:
	pytest -m "unit or dags"

test-unit:
	pytest -m unit

# Requires PostgreSQL. VOLTHIVE_TEST_DB tells the suite where it is; without
# it every database test SKIPS rather than fails, which is why `check` below
# runs them explicitly rather than relying on a bare `pytest`.
test-int:
	pytest -m integration

test-e2e:
	pytest -m e2e

# One command that runs every gate CI runs, in the same order: cheap first.
check:
	ruff check .
	ruff format --check .
	mypy src
	sqlfluff lint sql/
	pytest -q

version-check:
	python --version
	git --version
	docker --version
	docker compose version

# --- Docker stack ----------------------------------------------------------
# `up` uses --wait, so the command does not return until every service reports
# healthy or its healthcheck gives up. That is the whole point of having real
# healthchecks: no sleep, no guessing, no "it was probably ready".

build:
	docker compose build

# airflow-init is deliberately NOT named in the --wait list: it is a one-shot
# container that exits 0, and `--wait` waits for services to become RUNNING or
# healthy, which a completed container never does. It still runs, because both
# Airflow services declare service_completed_successfully on it.
up:
	docker compose up -d --build --wait --wait-timeout 300 postgres airflow-scheduler airflow-webserver

down:
	docker compose down

restart:
	docker compose restart

# Destroys the pgdata volume. The next `up` re-runs the database bootstrap.
clean:
	docker compose down --volumes --remove-orphans

ps:
	docker compose ps

logs:
	docker compose logs --follow --tail=100

psql:
	docker compose exec postgres sh -c "psql -U $$POSTGRES_USER -d $$WH_DB"

compose-config:
	docker compose config --quiet

verify:
	bash scripts/verify_stack.sh

# --- Warehouse lifecycle ---------------------------------------------------
# These talk to whatever PostgreSQL the environment points at - the compose
# stack by default, a CI service container in the pipeline. They are the same
# commands CI runs, so a green pipeline and a working laptop mean the same
# thing.

PROFILE ?= tiny

db-init:
	python scripts/apply_schema.py

generate:
	python scripts/generate_data.py --profile $(PROFILE)

# FROM and TO default to the tiny profile's window so `make run` works
# immediately after `make generate`. Override for any other range.
FROM ?= 2026-06-01
TO   ?= 2026-06-07

run:
	python scripts/run_pipeline.py --from $(FROM) --to $(TO)

run-clean: db-init generate run

analytics:
	bash scripts/run_analytics.sh

dq-report:
	python scripts/dq_report.py

# Replays a window from raw without contacting any source. See the header of
# scripts/restate.py for why that is the payoff of keeping raw immutable.
restate:
	python scripts/restate.py --from $(FROM) --to $(TO)

# Chunked replay. CHUNK_DAYS controls how often it commits, and therefore how
# far a crash rewinds.
CHUNK_DAYS ?= 30

backfill:
	bash scripts/backfill.sh --from $(FROM) --to $(TO) --chunk-days $(CHUNK_DAYS)

# Destructive. Previews unless YES=1, because rebuilding dimension history
# reissues every surrogate key and rebuilds every fact.
DIM ?= all
YES ?= 0

rebuild-dims:
	python scripts/rebuild_dimension_history.py --dim $(DIM) $(if $(filter 1,$(YES)),--yes,--dry-run)
