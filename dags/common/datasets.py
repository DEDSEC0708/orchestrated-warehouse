"""Dataset definitions shared across DAGs.

Airflow Datasets express the REAL dependency - "the warehouse build needs raw
master data AND raw session data to be fresh" - rather than a coincidence of
schedules. When both are published, the consuming DAG triggers. Nothing polls,
no worker slot is burned waiting, and the two producers can be rescheduled
independently without anyone remembering to update a sensor.

Why not ExternalTaskSensor? It couples the consumer to the PRODUCER'S SCHEDULE:
it looks for a task instance at a particular logical date, so changing when the
upstream DAG runs silently breaks the downstream one. It also occupies a worker
slot while it polls. Datasets have neither problem, and ADR-005 records the
decision.

The URIs are deliberately named after the RAW TABLES they describe rather than
after the tasks that write them. A dataset is a statement about data being
ready, not about a task having finished.

The scheme is a custom ``volthive://`` rather than ``postgres://`` on purpose.
Airflow validates the URI format of schemes it recognises, and a ``postgres://``
dataset URI must name a database, schema and table - which would force these to
be spelled ``postgres://warehouse/raw/cms_master`` in a way that looks like a
connection string and invites someone to try connecting to it. These are
LOGICAL signals about data readiness, not addresses, and a custom scheme says
so while keeping the project warning-free on 2.10 and forward-compatible with
Airflow 3's stricter asset URI rules.
"""

from __future__ import annotations

from airflow.datasets import Dataset

#: Published by volthive_ingest_master once all five CMS entities have landed.
RAW_CMS_MASTER = Dataset("volthive://warehouse/raw/cms_master")

#: Published by volthive_ingest_sessions once the session-shaped sources have
#: landed: OCPP charge detail records, meter telemetry and the roaming partner.
RAW_SESSIONS = Dataset("volthive://warehouse/raw/sessions")

__all__ = ["RAW_CMS_MASTER", "RAW_SESSIONS"]
