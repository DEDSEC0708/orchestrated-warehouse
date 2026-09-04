"""VoltHive Energy - Orchestrated Warehouse.

A local, production-style batch data platform for EV charging analytics.
VoltHive Energy is a fictional company and all data is synthetic.

Deliberately minimal: this module exposes the package version and nothing
else. It performs no configuration loading, no logging setup and no database
connection at import time.

Why that matters: the Airflow scheduler re-imports every DAG file on a short
interval, and each DAG file imports this package. Any work done here - reading
the environment, opening a connection, parsing YAML - would run hundreds of
times an hour and would turn a configuration mistake into a scheduler-wide
failure instead of a single failed task. Import cheaply; do work inside
functions.
"""

from __future__ import annotations

__version__ = "0.1.0"

__all__ = ["__version__"]
