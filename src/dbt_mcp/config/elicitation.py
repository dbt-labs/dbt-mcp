"""General-purpose MCP elicitation primitives.

Not coupled to any specific config value or consumer.
"""

import logging
import os
from pathlib import Path
from typing import Any

import yaml
from filelock import FileLock
from mcp.server.elicitation import (
    AcceptedElicitation,
    ElicitSchemaModelT,
    elicit_with_validation,
)
from mcp.server.session import ServerSession
from mcp.types import RequestId

logger = logging.getLogger(__name__)


def _default_config_dir() -> Path:
    # Same logic as credentials.get_dbt_profiles_path(), duplicated here
    # to avoid a circular import through the credentials → config_providers chain.
    profiles_dir = os.environ.get("DBT_PROFILES_DIR")
    if profiles_dir:
        return Path(profiles_dir).expanduser()
    return Path.home() / ".dbt"


def get_mcp_session() -> tuple[ServerSession, RequestId] | None:
    """Return the active MCP session and request ID, or None if elicitation is unsupported."""
    # Lazy import — avoids hard dependency on lowlevel internals at module
    # load time.
    from mcp.server.lowlevel.server import request_ctx

    ctx = request_ctx.get(None)
    if ctx is None:
        return None

    session: ServerSession = ctx.session
    client_params = session.client_params
    if client_params is None:
        return None

    caps = client_params.capabilities
    if caps is None or caps.elicitation is None or caps.elicitation.form is None:
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
        """Parse the config file into a dict. Caller must hold the lock."""
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
