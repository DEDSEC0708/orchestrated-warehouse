"""Unit tests for the exception taxonomy.

These tests assert *behaviour classification*, not error text. The contract
that matters is: which failures may be retried, which must be quarantined, and
which must stop the pipeline. Later phases branch on exactly these flags.
"""

from __future__ import annotations

import pytest

from volthive.exceptions import (
    ConfigurationError,
    ContractError,
    DataError,
    TransientError,
    VoltHiveError,
)

pytestmark = pytest.mark.unit

ALL_ERRORS = (TransientError, DataError, ContractError, ConfigurationError)


def test_every_error_derives_from_the_base_class() -> None:
    for error_class in ALL_ERRORS:
        assert issubclass(error_class, VoltHiveError)
        assert issubclass(error_class, Exception)


def test_only_transient_errors_are_retryable() -> None:
    """The single most important distinction in the taxonomy."""
    assert TransientError("connection reset").is_retryable is True
    assert DataError("negative energy delta").is_retryable is False
    assert ContractError("column disappeared").is_retryable is False
    assert ConfigurationError("missing password").is_retryable is False


def test_categories_are_stable_labels() -> None:
    assert TransientError("x").category == "transient"
    assert DataError("x").category == "data"
    assert ContractError("x").category == "contract"
    assert ConfigurationError("x").category == "configuration"


def test_configuration_error_is_a_contract_error() -> None:
    """A missing setting behaves exactly like any other broken assumption."""
    error = ConfigurationError("WH_ETL_PASSWORD is not set")
    assert isinstance(error, ContractError)
    assert error.is_retryable is False


def test_transient_error_carries_retry_context() -> None:
    error = TransientError("HTTP 503 from partner API", source_system="PARTNER", attempt=2)
    rendered = error.as_dict()

    assert rendered["error_type"] == "TransientError"
    assert rendered["error_category"] == "transient"
    assert rendered["is_retryable"] is True
    assert rendered["source_system"] == "PARTNER"
    assert rendered["attempt"] == 2


def test_data_error_carries_quarantine_context() -> None:
    """A quarantined row is useless without its rule code and provenance."""
    error = DataError(
        "meter_stop_wh(1280000) < meter_start_wh(1284500)",
        rule_code="CDR_NEGATIVE_ENERGY",
        natural_key="TXN-20260614-BLR-004182",
        source_file="data/landing/cdr/dt=2026-06-14/city=BLR/cdr_BLR_20260614.jsonl",
    )
    rendered = error.as_dict()

    assert rendered["rule_code"] == "CDR_NEGATIVE_ENERGY"
    assert rendered["natural_key"] == "TXN-20260614-BLR-004182"
    assert rendered["source_file"].endswith(".jsonl")
    assert rendered["is_retryable"] is False


def test_contract_error_carries_expected_and_actual() -> None:
    error = ContractError(
        "source column missing",
        entity="cms.customers",
        expected="updated_at",
        actual="not present",
    )
    rendered = error.as_dict()

    assert rendered["entity"] == "cms.customers"
    assert rendered["expected"] == "updated_at"
    assert rendered["actual"] == "not present"


def test_arbitrary_context_is_preserved() -> None:
    error = TransientError("timeout", source_system="CMS", dw_batch_key="2026-06-14")
    assert error.context["dw_batch_key"] == "2026-06-14"
    assert error.as_dict()["dw_batch_key"] == "2026-06-14"


def test_message_is_accessible_and_str_works() -> None:
    error = DataError("bad row")
    assert error.message == "bad row"
    assert str(error) == "bad row"
    assert "DataError" in repr(error)


def test_catching_the_base_class_catches_everything() -> None:
    for error_class in ALL_ERRORS:
        with pytest.raises(VoltHiveError):
            raise error_class("boom")
