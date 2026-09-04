"""Unit tests for typed configuration loading."""

from __future__ import annotations

from pathlib import Path

import pytest

from volthive.config.settings import (
    Settings,
    get_settings,
    load_yaml_config,
    repo_root,
    reset_settings_cache,
)
from volthive.exceptions import ConfigurationError, ContractError

pytestmark = pytest.mark.unit


# --------------------------------------------------------------------------
# Loading and validation
# --------------------------------------------------------------------------


def test_settings_load_from_environment(minimal_env: dict[str, str]) -> None:
    settings = get_settings()

    assert settings.wh_etl_password.get_secret_value() == minimal_env["WH_ETL_PASSWORD"]
    # Non-secret values fall back to documented defaults.
    assert settings.env == "local"
    assert settings.log_level == "INFO"
    assert settings.postgres_host == "postgres"
    assert settings.postgres_port == 5432
    assert settings.wh_db == "warehouse"
    assert settings.cms_db == "cms"
    assert settings.partner_mode == "file"
    assert settings.generator_seed == 42


def test_environment_overrides_defaults(
    minimal_env: dict[str, str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("VOLTHIVE_ENV", "ci")
    monkeypatch.setenv("VOLTHIVE_LOG_LEVEL", "DEBUG")
    monkeypatch.setenv("POSTGRES_HOST", "localhost")
    monkeypatch.setenv("VOLTHIVE_GENERATOR_PROFILE", "tiny")

    settings = get_settings()

    assert settings.env == "ci"
    assert settings.log_level == "DEBUG"
    assert settings.postgres_host == "localhost"
    assert settings.generator_profile == "tiny"


def test_missing_required_secret_raises_configuration_error() -> None:
    """A missing password must fail loudly and name the variable."""
    with pytest.raises(ConfigurationError) as exc_info:
        get_settings()

    message = str(exc_info.value)
    assert "WH_ETL_PASSWORD" in message
    assert "CMS_READER_PASSWORD" in message
    # It is a contract problem, so it must not be retryable.
    assert isinstance(exc_info.value, ContractError)
    assert exc_info.value.is_retryable is False


def test_invalid_log_level_is_rejected(
    minimal_env: dict[str, str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("VOLTHIVE_LOG_LEVEL", "CHATTY")
    with pytest.raises(ConfigurationError):
        get_settings()


def test_negative_generator_seed_is_rejected(
    minimal_env: dict[str, str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("VOLTHIVE_GENERATOR_SEED", "-1")
    with pytest.raises(ConfigurationError) as exc_info:
        get_settings()
    assert "reproducible" in str(exc_info.value)


def test_http_partner_mode_requires_a_url(
    minimal_env: dict[str, str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("VOLTHIVE_PARTNER_MODE", "http")
    monkeypatch.delenv("VOLTHIVE_PARTNER_API_URL", raising=False)

    with pytest.raises(ConfigurationError) as exc_info:
        get_settings()
    assert "VOLTHIVE_PARTNER_API_URL" in str(exc_info.value)


def test_empty_optional_url_is_treated_as_unset(
    minimal_env: dict[str, str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """`ALERT_WEBHOOK_URL=` in .env means "disabled", not "empty URL"."""
    monkeypatch.setenv("ALERT_WEBHOOK_URL", "   ")
    settings = get_settings()
    assert settings.alert_webhook_url is None
    assert settings.safe_summary()["alerting_enabled"] is False


# --------------------------------------------------------------------------
# Secret handling
# --------------------------------------------------------------------------


def test_secrets_are_masked_in_repr(minimal_env: dict[str, str]) -> None:
    settings = get_settings()
    rendered = repr(settings) + str(settings)
    assert minimal_env["WH_ETL_PASSWORD"] not in rendered


def test_safe_summary_excludes_secrets(minimal_env: dict[str, str]) -> None:
    summary = get_settings().safe_summary()
    flattened = str(summary)

    assert minimal_env["WH_ETL_PASSWORD"] not in flattened
    assert "wh_etl_password" not in summary
    assert "cms_reader_password" not in summary
    # Non-secret operational context is present.
    assert summary["wh_db"] == "warehouse"
    assert summary["wh_etl_user"] == "wh_etl"


def test_dsn_uses_least_privilege_roles_and_quotes_the_password() -> None:
    """A password containing URI-special characters must not break the DSN."""
    settings = Settings(
        _env_file=None,
        wh_etl_password="p@ss/word:1#x",
        cms_reader_password="reader_pw",
        postgres_host="localhost",
    )
    assert settings.warehouse_dsn.startswith("postgresql://wh_etl:")
    assert settings.warehouse_dsn.endswith("@localhost:5432/warehouse")
    # Raw special characters must be percent-encoded.
    assert "p%40ss%2Fword%3A1%23x" in settings.warehouse_dsn
    assert settings.cms_dsn.startswith("postgresql://cms_reader:")
    assert settings.cms_dsn.endswith("/cms")


# --------------------------------------------------------------------------
# Caching
# --------------------------------------------------------------------------


def test_get_settings_is_cached_and_can_be_reset(
    minimal_env: dict[str, str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    first = get_settings()
    assert get_settings() is first, "settings should be cached"

    monkeypatch.setenv("VOLTHIVE_ENV", "ci")
    assert get_settings().env == "local", "cache should not observe the change"

    reset_settings_cache()
    assert get_settings().env == "ci", "reset should pick up the new value"


def test_repo_root_points_at_the_repository() -> None:
    root = repo_root()
    assert (root / "pyproject.toml").is_file()
    assert (root / "src" / "volthive").is_dir()


# --------------------------------------------------------------------------
# YAML configuration loading
# --------------------------------------------------------------------------


def test_load_yaml_config_returns_mapping(tmp_path: Path) -> None:
    (tmp_path / "sources.yml").write_text(
        "sources:\n  - name: ocpp_cdr\n    strategy: incremental\n",
        encoding="utf-8",
    )
    parsed = load_yaml_config("sources.yml", config_dir=tmp_path)
    assert parsed["sources"][0]["name"] == "ocpp_cdr"


def test_load_yaml_config_supports_nested_paths(tmp_path: Path) -> None:
    nested = tmp_path / "dq"
    nested.mkdir()
    (nested / "session.yml").write_text("rules: []\n", encoding="utf-8")
    assert load_yaml_config("dq/session.yml", config_dir=tmp_path) == {"rules": []}


def test_load_yaml_config_empty_file_is_empty_mapping(tmp_path: Path) -> None:
    (tmp_path / "empty.yml").write_text("", encoding="utf-8")
    assert load_yaml_config("empty.yml", config_dir=tmp_path) == {}


def test_load_yaml_config_missing_file_raises(tmp_path: Path) -> None:
    with pytest.raises(ConfigurationError) as exc_info:
        load_yaml_config("absent.yml", config_dir=tmp_path)
    assert "not found" in str(exc_info.value)


def test_load_yaml_config_rejects_invalid_yaml(tmp_path: Path) -> None:
    (tmp_path / "broken.yml").write_text("key: [unclosed\n", encoding="utf-8")
    with pytest.raises(ConfigurationError) as exc_info:
        load_yaml_config("broken.yml", config_dir=tmp_path)
    assert "not valid YAML" in str(exc_info.value)


def test_load_yaml_config_rejects_non_mapping(tmp_path: Path) -> None:
    """A list at the top level would silently break every consumer."""
    (tmp_path / "list.yml").write_text("- one\n- two\n", encoding="utf-8")
    with pytest.raises(ConfigurationError) as exc_info:
        load_yaml_config("list.yml", config_dir=tmp_path)
    assert "mapping" in str(exc_info.value)
