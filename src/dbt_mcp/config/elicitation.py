"""MCP elicitation primitives."""

from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import TYPE_CHECKING, Any

import yaml
from filelock import FileLock
from mcp.server.elicitation import (
    AcceptedElicitation,
    ElicitSchemaModelT,
    elicit_with_validation,
)
from mcp.server.session import ServerSession
from mcp.types import RequestId
from pydantic import BaseModel, Field, field_validator

from dbt_mcp.errors.common import MissingHostError

if TYPE_CHECKING:
    from dbt_mcp.config.credentials import AuthenticationMethod, CredentialsProvider
    from dbt_mcp.config.headers import TokenProvider
    from dbt_mcp.config.settings import DbtMcpSettings

logger = logging.getLogger(__name__)


def _default_config_dir() -> Path:
    profiles_dir = os.environ.get("DBT_PROFILES_DIR")
    if profiles_dir:
        return Path(profiles_dir).expanduser()
    return Path.home() / ".dbt"


def get_mcp_session() -> tuple[ServerSession, RequestId] | None:
    """Return the active MCP session and request ID, or None if elicitation is unsupported."""
    from mcp.server.lowlevel.server import request_ctx

    ctx = request_ctx.get(None)
    if ctx is None:
        return None

    session: ServerSession = ctx.session
    client_params = session.client_params
    if client_params is None:
        return None

    caps = client_params.capabilities
    if caps is None or caps.elicitation is None:
        return None

    return session, ctx.request_id


async def elicit_or_raise(
    error: Exception,
    schema: type[ElicitSchemaModelT],
    message: str,
) -> ElicitSchemaModelT:
    """Elicit information from the user, or re-raise the original error."""
    session_info = get_mcp_session()
    if session_info is None:
        raise error

    session, request_id = session_info
    result = await elicit_with_validation(
        session=session,
        message=message,
        schema=schema,
        related_request_id=request_id,
    )

    if isinstance(result, AcceptedElicitation):
        return result.data

    raise error


class ConfigPersistence:
    """Read/write elicited config values to ~/.dbt/mcp-config.yml."""

    def __init__(self, config_path: Path | None = None) -> None:
        if config_path is None:
            config_path = _default_config_dir() / "mcp-config.yml"
        self._path = config_path
        self._lock_path = config_path.with_suffix(".lock")

    def _load_yaml(self) -> dict[str, Any]:
        """Parse the config file. Caller must hold the lock."""
        if not self._path.exists():
            return {}
        try:
            data = yaml.safe_load(self._path.read_text())
        except yaml.YAMLError:
            logger.warning(
                "Failed to parse elicited config at %s — ignoring file",
                self._path,
            )
            return {}
        if not isinstance(data, dict):
            return {}
        return data

    def read(self) -> dict[str, Any]:
        """Read all persisted config values. Returns {} on missing or invalid files."""
        if not self._path.exists():
            return {}
        with FileLock(self._lock_path):
            return self._load_yaml()

    def write(self, key: str, value: Any) -> None:
        """Write a single config value. Preserves existing keys."""
        with FileLock(self._lock_path):
            existing = self._load_yaml()
            existing[key] = value
            self._path.parent.mkdir(parents=True, exist_ok=True)
            self._path.write_text(yaml.dump(existing, default_flow_style=False))

    def read_value(self, key: str) -> Any | None:
        """Read a single config value. Returns None if not found."""
        return self.read().get(key)


class DbtHostSchema(BaseModel):
    """Elicitation form for dbt Cloud host."""

    dbt_host: str = Field(description="Your dbt Cloud host (e.g., cloud.getdbt.com)")

    @field_validator("dbt_host")
    @classmethod
    def strip_whitespace(cls, v: str) -> str:
        return v.strip()


class ElicitingCredentialsProvider:
    """Wrap CredentialsProvider to elicit DBT_HOST when missing."""

    def __init__(
        self,
        inner: CredentialsProvider,
        persistence: ConfigPersistence,
    ) -> None:
        self._inner = inner
        self._persistence = persistence

    async def get_credentials(self) -> tuple[DbtMcpSettings, TokenProvider]:
        """Delegate to inner provider; elicit DBT_HOST on MissingHostError."""
        try:
            return await self._inner.get_credentials()
        except MissingHostError as e:
            data = await elicit_or_raise(
                e, DbtHostSchema, "Let's set up dbt-mcp. What's your dbt Cloud host?"
            )
            self._inner.settings.dbt_host = data.dbt_host
            self._persistence.write("dbt_host", data.dbt_host)
            return await self._inner.get_credentials()

    @property
    def settings(self) -> DbtMcpSettings:
        return self._inner.settings

    @property
    def token_provider(self) -> TokenProvider | None:
        return self._inner.token_provider

    @property
    def authentication_method(self) -> AuthenticationMethod | None:
        return self._inner.authentication_method

    @property
    def account_identifier(self) -> str | None:
        return self._inner.account_identifier
