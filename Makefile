# ===========================================================================
# ORCHESTRATED WAREHOUSE - developer interface
#
# PORTABILITY RULE: every recipe line is a single executable invocation with
# no shell syntax (no if/for/pipes/redirects/&&). GNU Make on Windows runs
# recipes through cmd.exe unless a POSIX sh is on PATH, so shell-specific
# syntax would work in Git Bash and silently break in PowerShell. Keeping
# recipes shell-agnostic means `make` behaves identically everywhere.
#
# Targets are added phase by phase. Phase 0 provides setup, lint and test only.
# ===========================================================================

.DEFAULT_GOAL := help
.PHONY: help venv install install-dev hooks lint format typecheck test version-check

help:
	@echo ORCHESTRATED WAREHOUSE - available targets
	@echo   make install       Install runtime dependencies (with constraints)
	@echo   make install-dev   Install runtime + dev dependencies (with constraints)
	@echo   make hooks         Install the pre-commit git hooks
	@echo   make lint          Run ruff lint and format checks
	@echo   make format        Auto-fix lint issues and format the code
	@echo   make typecheck     Run mypy (active from Phase 1)
	@echo   make test          Run the test suite
	@echo   make version-check Print the versions of the required tools
	@echo Phase 0 provides setup, lint and test targets only.
	@echo Docker, database, generator and pipeline targets arrive in later phases.

# --- Setup -----------------------------------------------------------------
# ALWAYS pass -c constraints.txt. Installing Airflow without its official
# constraints file is the most common cause of a broken Airflow environment.

install:
	python -m pip install --upgrade pip
	python -m pip install -r requirements.txt -c constraints.txt

install-dev:
	python -m pip install --upgrade pip
	python -m pip install -r requirements-dev.txt -c constraints.txt

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

test:
	pytest

version-check:
	python --version
	git --version
	docker --version
	docker compose version
