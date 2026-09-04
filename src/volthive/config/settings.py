"""Typed configuration for the VoltHive warehouse platform.

Design rules, in priority order:

1. **No secret has a default value.** A password with a fallback is a password
   that ships to production. Missing secrets must fail at startup with a
   message naming exactly what is absent.
2. **Everything non-secret has a safe default**, so a developer can import the
   package without a fully populated environment.
3. **Configuration is validated once, at load time**, not discovered field by
   field at 3 a.m. when a task fails on line 400.
4. **Two sources, one precedence order**: process environment > ``.env`` file.
   Structured rule sets (data-quality rules, source definitions) live in YAML
   under ``configs/`` and are loaded separately - see :func:`load_yaml_config`.
   The split is deliberate: env vars carry *deployment* facts (hosts,
   credentials, modes), YAML carries *domain* facts that belong in review-able,
   diff-able version control.
5. **Nothing is read at import time.** Airflow re-parses DAG files constantly;
   a module that reads the environment on import turns a config problem into
   a scheduler-wide outage. Call :func:`get_settings` inside a function.

Values here mirror ``.env.example``, which is the human-readable contract.
"""

from __future__ import annotations

import os
from functools import lru_cache
from pathlib import Path
from typing import Any, Literal
from urllib.parse import quote_plus

import yaml
from pydantic import Field, SecretStr, ValidationError, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from volthive.exceptions import ConfigurationError

__all__ = [
    "Settings",
    "default_config_dir",
    "default_data_dir",
    "get_settings",
    "load_yaml_config",
    "repo_root",
    "reset_settings_cache",
]

LogLevel = Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
PartnerMode = Literal["file", "http"]
RunEnvironment = Literal["local", "ci", "test"]
GeneratorProfile = Literal["default", "small", "tiny", "clean"]


def repo_root() -> Path:
    """Absolute path to the repository root.

    Derived from this file's location (``src/volthive/config/settings.py``)
    rather than from the current working directory, because Airflow tasks,
    pytest and an interactive shell all run with different working
    directories and only the package location is stable.
    """
    return Path(__file__).resolve().parents[3]


def default_config_dir() -> Path:
    """Where ``configs/`` lives, WITHOUT requiring valid database credentials.

    Reading a YAML rule set or a generator profile should not depend on a
    password being set. Routing those through the full :class:`Settings` model
    would mean a developer with no ``.env`` could not run the unit tests, and a
    CI job that only lints configuration would need database secrets it has no
    use for.
    """
    override = os.environ.get("VOLTHIVE_CONFIG_DIR")
    return Path(override) if override else repo_root() / "configs"


def default_data_dir() -> Path:
    """Where ``data/`` lives, without requiring valid database credentials."""
    override = os.environ.get("VOLTHIVE_DATA_DIR")
    return Path(override) if override else repo_root() / "data"


class Settings(BaseSettings):
    """All deployment configuration, validated.

    Instantiate through :func:`get_settings` in application code so the result
    is cached. Instantiate directly in tests with ``_env_file=None`` so the
    developer's real ``.env`` cannot influence a test outcome.
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        # Allow construction by field name as well as by environment alias,
        # so tests can write Settings(wh_etl_password="x") instead of
        # Settings(WH_ETL_PASSWORD="x").
        populate_by_name=True,
        # The environment also holds AIRFLOW__* and POSTGRES_* superuser vars
        # that this class deliberately does not model. Ignoring them is
        # correct; erroring on them would make the app fragile to unrelated
        # environment changes.
        extra="ignore",
    )

    # --- Application ------------------------------------------------------
    env: RunEnvironment = Field(default="local", alias="VOLTHIVE_ENV")
    log_level: LogLevel = Field(default="INFO", alias="VOLTHIVE_LOG_LEVEL")
    log_json: bool = Field(default=True, alias="VOLTHIVE_LOG_JSON")

    # --- PostgreSQL -------------------------------------------------------
    # Host defaults to the compose service name, which is what every container
    # in the stack resolves. Running from the host instead means overriding it
    # with localhost - hence it is configurable rather than hard-coded.
    postgres_host: str = Field(default="postgres", alias="POSTGRES_HOST")
    postgres_port: int = Field(default=5432, alias="POSTGRES_PORT")
    wh_db: str = Field(default="warehouse", alias="WH_DB")
    cms_db: str = Field(default="cms", alias="CMS_DB")

    # Least-privilege roles. The pipeline never connects as the superuser.
    wh_etl_user: str = Field(default="wh_etl", alias="WH_ETL_USER")
    cms_reader_user: str = Field(default="cms_reader", alias="CMS_READER_USER")

    # REQUIRED - no default, by design. See rule 1 in the module docstring.
    wh_etl_password: SecretStr = Field(alias="WH_ETL_PASSWORD")
    cms_reader_password: SecretStr = Field(alias="CMS_READER_PASSWORD")

    # --- Roaming partner source (S4) --------------------------------------
    partner_mode: PartnerMode = Field(default="file", alias="VOLTHIVE_PARTNER_MODE")
    partner_api_url: str | None = Field(default=None, alias="VOLTHIVE_PARTNER_API_URL")
    partner_api_token: SecretStr | None = Field(default=None, alias="VOLTHIVE_PARTNER_API_TOKEN")

    # --- Paths ------------------------------------------------------------
    data_dir: Path = Field(default_factory=lambda: repo_root() / "data", alias="VOLTHIVE_DATA_DIR")
    config_dir: Path = Field(
        default_factory=lambda: repo_root() / "configs", alias="VOLTHIVE_CONFIG_DIR"
    )

    # --- Synthetic data generator ----------------------------------------
    generator_seed: int = Field(default=42, alias="VOLTHIVE_GENERATOR_SEED")
    generator_profile: GeneratorProfile = Field(
        default="default", alias="VOLTHIVE_GENERATOR_PROFILE"
    )

    # --- Optional alerting -------------------------------------------------
    # Empty by default so the platform has no external dependency to run.
    alert_webhook_url: str | None = Field(default=None, alias="ALERT_WEBHOOK_URL")

    # ------------------------------------------------------------------ #
    # Validators
    # ------------------------------------------------------------------ #

    @field_validator("generator_seed")
    @classmethod
    def _seed_must_be_non_negative(cls, value: int) -> int:
        if value < 0:
            msg = "VOLTHIVE_GENERATOR_SEED must be >= 0 for reproducible data generation"
            raise ValueError(msg)
        return value

    @field_validator("postgres_port")
    @classmethod
    def _port_must_be_valid(cls, value: int) -> int:
        if not (1 <= value <= 65535):
            msg = f"POSTGRES_PORT must be between 1 and 65535, got {value}"
            raise ValueError(msg)
        return value

    @field_validator("alert_webhook_url", "partner_api_url", mode="before")
    @classmethod
    def _empty_string_is_none(cls, value: Any) -> Any:
        """Treat ``FOO=`` in a .env file as unset rather than as an empty URL.

        Without this, the documented "leave it empty to disable alerting"
        instruction would produce an empty string that later code would try to
        POST to.
        """
        if isinstance(value, str) and not value.strip():
            return None
        return value

    @model_validator(mode="after")
    def _http_partner_mode_needs_a_url(self) -> Settings:
        if self.partner_mode == "http" and not self.partner_api_url:
            msg = (
                "VOLTHIVE_PARTNER_MODE=http requires VOLTHIVE_PARTNER_API_URL to be set. "
                "Use VOLTHIVE_PARTNER_MODE=file for the deterministic local fixture client."
            )
            raise ValueError(msg)
        return self

    # ------------------------------------------------------------------ #
    # Derived values
    # ------------------------------------------------------------------ #

    def _dsn(self, user: str, password: SecretStr, database: str) -> str:
        # quote_plus so that a password containing @ / : / # cannot break the
        # URI - a real and annoying class of connection bug.
        return (
            f"postgresql://{quote_plus(user)}:{quote_plus(password.get_secret_value())}"
            f"@{self.postgres_host}:{self.postgres_port}/{database}"
        )

    @property
    def warehouse_dsn(self) -> str:
        """Connection URI for the warehouse, as the least-privilege ETL role."""
        return self._dsn(self.wh_etl_user, self.wh_etl_password, self.wh_db)

    @property
    def cms_dsn(self) -> str:
        """Connection URI for the simulated source OLTP, read-only role."""
        return self._dsn(self.cms_reader_user, self.cms_reader_password, self.cms_db)

    def safe_summary(self) -> dict[str, Any]:
        """Configuration rendered for logging - secrets excluded, not masked.

        Excluded rather than redacted: a value that is never placed in the
        dictionary cannot leak through a formatter that ignores redaction.
        """
        return {
            "env": self.env,
            "log_level": self.log_level,
            "postgres_host": self.postgres_host,
            "postgres_port": self.postgres_port,
            "wh_db": self.wh_db,
            "cms_db": self.cms_db,
            "wh_etl_user": self.wh_etl_user,
            "cms_reader_user": self.cms_reader_user,
            "partner_mode": self.partner_mode,
            "data_dir": str(self.data_dir),
            "config_dir": str(self.config_dir),
            "generator_seed": self.generator_seed,
            "generator_profile": self.generator_profile,
            "alerting_enabled": self.alert_webhook_url is not None,
        }


def _format_validation_error(error: ValidationError) -> str:
    """Turn a pydantic ValidationError into an operator-readable message.

    pydantic's default rendering is accurate but noisy. What someone staring
    at a failed container needs is the *environment variable name* they must
    set, which is exactly what this extracts.
    """
    lines: list[str] = []
    for problem in error.errors():
        location = ".".join(str(part) for part in problem["loc"]) or "<root>"
        lines.append(f"  - {location}: {problem['msg']}")
    return "\n".join(lines)


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Load, validate and cache settings.

    Cached because Airflow re-imports modules frequently and re-reading and
    re-validating the environment on every call is wasted work. Tests must
    call :func:`reset_settings_cache` to avoid leaking state between cases.

    Raises:
        ConfigurationError: if any required setting is missing or invalid.
            Deliberately a ``ContractError`` subclass - a missing password is
            a broken deployment assumption, not something a retry can fix.
    """
    try:
        return Settings()  # type: ignore[call-arg]  # values come from env/.env
    except ValidationError as exc:
        message = (
            "Invalid or missing configuration. Copy .env.example to .env and "
            "fill in the required values (scripts/generate_env.sh does this for you).\n"
            f"{_format_validation_error(exc)}"
        )
        raise ConfigurationError(message, entity="Settings") from exc


def reset_settings_cache() -> None:
    """Clear the cached settings. Intended for tests and for reload scenarios."""
    get_settings.cache_clear()


def load_yaml_config(name: str, *, config_dir: Path | None = None) -> dict[str, Any]:
    """Load a YAML configuration file from ``configs/``.

    Used from Phase 5 onwards for ``sources.yml``, the data-quality rule sets
    and the source schema contracts. Kept here so there is exactly one place
    that knows how configuration is found and how it fails.

    Args:
        name: File name relative to the config directory, e.g. ``"sources.yml"``
            or ``"dq/session.yml"``.
        config_dir: Override the directory. Defaults to the configured one,
            which makes this trivially testable with ``tmp_path``.

    Returns:
        The parsed mapping. An empty file yields an empty dict.

    Raises:
        ConfigurationError: the file is missing, unreadable, not valid YAML,
            or does not contain a mapping at the top level. All four are
            deployment problems, so all four fail loudly rather than
            defaulting to empty - a silently empty rule set would mean
            "no data-quality checks ran" while reporting success.
    """
    base = config_dir if config_dir is not None else default_config_dir()
    path = base / name

    if not path.is_file():
        raise ConfigurationError(
            f"Configuration file not found: {path}",
            entity=name,
            expected=str(path),
        )

    try:
        with path.open("r", encoding="utf-8") as handle:
            parsed = yaml.safe_load(handle)
    except yaml.YAMLError as exc:
        raise ConfigurationError(
            f"Configuration file is not valid YAML: {path} ({exc})",
            entity=name,
        ) from exc
    except OSError as exc:
        raise ConfigurationError(
            f"Configuration file could not be read: {path} ({exc})",
            entity=name,
        ) from exc

    if parsed is None:
        return {}

    if not isinstance(parsed, dict):
        raise ConfigurationError(
            f"Configuration file must contain a mapping at the top level: {path}",
            entity=name,
            expected="mapping",
            actual=type(parsed).__name__,
        )

    return parsed
