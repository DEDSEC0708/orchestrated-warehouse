"""Unit tests for structured logging, run-ID context and PII redaction."""

from __future__ import annotations

import json
import uuid
from typing import Any

import pytest

from volthive.logging_setup import (
    REDACTED,
    bind_run_context,
    clear_run_context,
    configure_logging,
    get_logger,
    new_pipeline_run_id,
    run_context,
)

pytestmark = pytest.mark.unit


def _emit(capsys: pytest.CaptureFixture[str], event: str, **fields: Any) -> dict[str, Any]:
    """Log one JSON line and return it parsed."""
    configure_logging(level="DEBUG", json_logs=True, force=True)
    get_logger("test").info(event, **fields)
    captured = capsys.readouterr().out.strip().splitlines()
    assert captured, "expected exactly one log line on stdout"
    return json.loads(captured[-1])


# --------------------------------------------------------------------------
# Structure and run-ID binding
# --------------------------------------------------------------------------


def test_log_line_is_json_with_level_and_timestamp(capsys: pytest.CaptureFixture[str]) -> None:
    entry = _emit(capsys, "ingest_completed", rows_read=1912)

    assert entry["event"] == "ingest_completed"
    assert entry["level"] == "info"
    assert entry["rows_read"] == 1912
    # ISO-8601 UTC, so log lines sort chronologically as plain strings.
    assert entry["timestamp"].endswith("Z")
    assert "T" in entry["timestamp"]


def test_run_id_is_bound_to_every_subsequent_line(capsys: pytest.CaptureFixture[str]) -> None:
    """The correlation ID must appear without being passed to each call."""
    run_id = new_pipeline_run_id()
    configure_logging(level="DEBUG", json_logs=True, force=True)
    bind_run_context(
        pipeline_run_id=run_id,
        dag_id="volthive_ingest_sessions",
        task_id="ingest_cdr_files",
        dw_batch_key="2026-06-14",
    )

    log = get_logger("test")
    log.info("first_event")
    log.info("second_event", rows_read=10)

    lines = [json.loads(line) for line in capsys.readouterr().out.strip().splitlines()]
    assert len(lines) == 2
    for entry in lines:
        assert entry["pipeline_run_id"] == run_id
        assert entry["dag_id"] == "volthive_ingest_sessions"
        assert entry["task_id"] == "ingest_cdr_files"
        assert entry["dw_batch_key"] == "2026-06-14"


def test_run_context_clears_even_when_the_body_raises(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """A failed task must not leak its run ID into the next one."""
    configure_logging(level="DEBUG", json_logs=True, force=True)

    with pytest.raises(RuntimeError), run_context(pipeline_run_id="run-abc"):
        get_logger("test").info("inside")
        raise RuntimeError("task failed")

    get_logger("test").info("after")
    lines = [json.loads(line) for line in capsys.readouterr().out.strip().splitlines()]

    assert lines[0]["pipeline_run_id"] == "run-abc"
    assert "pipeline_run_id" not in lines[-1]


def test_new_pipeline_run_id_is_a_unique_uuid4() -> None:
    first, second = new_pipeline_run_id(), new_pipeline_run_id()
    assert first != second
    assert uuid.UUID(first).version == 4


def test_console_mode_renders_without_error(capsys: pytest.CaptureFixture[str]) -> None:
    """Human-readable local development mode must still work."""
    configure_logging(level="INFO", json_logs=False, force=True)
    get_logger("test").info("readable_event", rows_read=5)
    out = capsys.readouterr().out
    assert "readable_event" in out
    assert "rows_read" in out


# --------------------------------------------------------------------------
# PII redaction
# --------------------------------------------------------------------------


def test_sensitive_fields_are_redacted(capsys: pytest.CaptureFixture[str]) -> None:
    entry = _emit(
        capsys,
        "row_rejected",
        email="priya@example.invalid",
        phone="+919900000001",
        full_name="Priya Sharma",
        id_tag="IDT-8842013",
        # Deliberately shaped like placeholders: the repository-wide secret
        # scanner in test_no_secrets_committed.py has no carve-out for test
        # files, because a credential in a fixture is still a credential.
        password="placeholder_not_a_real_password",
        token="placeholder_not_a_real_token",
        customer_id="CUS-011482",
    )

    for key in ("email", "phone", "full_name", "id_tag", "password", "token"):
        assert entry[key] == REDACTED, f"{key} leaked into the log"
    # Non-sensitive identifiers must survive - they are how you debug.
    assert entry["customer_id"] == "CUS-011482"


def test_email_domain_is_not_redacted(capsys: pytest.CaptureFixture[str]) -> None:
    """Exact-key matching, not substring matching.

    ``email_domain`` is deliberately warehoused in ``core.dim_customer``. A
    substring rule would redact it and quietly destroy a legitimate attribute.
    """
    entry = _emit(capsys, "customer_loaded", email_domain="example.invalid")
    assert entry["email_domain"] == "example.invalid"


def test_redaction_reaches_nested_payloads(capsys: pytest.CaptureFixture[str]) -> None:
    """A raw source payload logged wholesale must not leak."""
    entry = _emit(
        capsys,
        "quarantined",
        rule_code="CDR_NEGATIVE_ENERGY",
        payload={
            "transaction_id": "TXN-20260614-BLR-004182",
            "id_tag": "IDT-8842013",
            "customer": {"email": "priya@example.invalid", "city": "Bengaluru"},
        },
        contacts=[{"phone": "+919900000001"}],
    )

    assert entry["payload"]["transaction_id"] == "TXN-20260614-BLR-004182"
    assert entry["payload"]["id_tag"] == REDACTED
    assert entry["payload"]["customer"]["email"] == REDACTED
    assert entry["payload"]["customer"]["city"] == "Bengaluru"
    assert entry["contacts"][0]["phone"] == REDACTED
    assert entry["rule_code"] == "CDR_NEGATIVE_ENERGY"


def test_redaction_covers_context_bound_values(capsys: pytest.CaptureFixture[str]) -> None:
    """Redaction runs after context merging, not only on call arguments."""
    configure_logging(level="DEBUG", json_logs=True, force=True)
    bind_run_context(pipeline_run_id="run-xyz", id_tag="IDT-0001")
    get_logger("test").info("event")
    clear_run_context()

    entry = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    assert entry["pipeline_run_id"] == "run-xyz"
    assert entry["id_tag"] == REDACTED


def test_redaction_is_case_insensitive_on_keys(capsys: pytest.CaptureFixture[str]) -> None:
    entry = _emit(capsys, "event", Email="a@example.invalid", API_KEY="k")
    assert entry["Email"] == REDACTED
    assert entry["API_KEY"] == REDACTED
