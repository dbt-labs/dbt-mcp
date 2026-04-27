"""MCP elicitation primitives."""

from __future__ import annotations

import asyncio
import logging
from pathlib import Path
from typing import TYPE_CHECKING

from mcp.server.elicitation import (
    AcceptedElicitation,
    ElicitSchemaModelT,
    elicit_with_validation,
)
from mcp.server.lowlevel.server import request_ctx
from mcp.server.session import ServerSession
from mcp.types import RequestId
from pydantic import BaseModel, Field, field_validator

from dbt_mcp.errors.common import MissingHostError
from dbt_mcp.oauth.context_manager import DbtPlatformContextManager

if TYPE_CHECKING:
    from dbt_mcp.config.credentials import AuthenticationMethod, CredentialsProvider
    from dbt_mcp.config.headers import TokenProvider
    from dbt_mcp.config.settings import DbtMcpSettings

logger = logging.getLogger(__name__)

MAX_ELICITATION_RETRIES = 3
HOST_VALIDATION_TIMEOUT = 10
OAUTH_TIMEOUT = 120
DEFAULT_HOST_DESCRIPTION = (
    "Your dbt Platform host "
    "(e.g., ab123.us1.dbt.com — find it in your browser URL bar when logged into dbt Platform)"
)


def get_elicitation_session() -> tuple[ServerSession, RequestId] | None:
    """Return the active MCP session and request ID if the client supports elicitation."""
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
    session_info = get_elicitation_session()
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


class DbtHostSchema(BaseModel):
    """Elicitation form for dbt Platform host."""

    dbt_host: str = Field(
        min_length=1,
        description=DEFAULT_HOST_DESCRIPTION,
    )

    @field_validator("dbt_host", mode="before")
    @classmethod
    def normalize_host(cls, v: str) -> str:
        v = v.strip().removeprefix("https://").removeprefix("http://")
        return v.rstrip("/")


def _host_schema_with_hint(hint: str | None = None) -> type[DbtHostSchema]:
    """Return a DbtHostSchema variant with *hint* surfaced in the field title.

    The hint replaces the default field title so the error feedback appears
    as the input label in the elicitation UI.
    """
    if not hint:
        return DbtHostSchema

    # Need to create new DbtHostSchemaWithHint on each new call
    # for dynamic hints if re-elicitation fails
    class DbtHostSchemaWithHint(DbtHostSchema):
        dbt_host: str = Field(
            min_length=1,
            title=hint,
            description=DEFAULT_HOST_DESCRIPTION,
        )

    return DbtHostSchemaWithHint


async def validate_host_reachable(
    host: str, *, timeout: float = HOST_VALIDATION_TIMEOUT
) -> bool:
    """Check if a host is reachable by attempting a TCP connection on port 443."""
    try:
        _, writer = await asyncio.wait_for(
            asyncio.open_connection(host, 443),
            timeout=timeout,
        )
        writer.close()
        await writer.wait_closed()
        return True
    except (OSError, TimeoutError):
        return False


def _clear_persisted_host(dbt_profiles_dir: str | None) -> None:
    """Remove dbt_host from mcp.yml so subsequent calls re-trigger elicitation."""
    dbt_user_dir = (
        Path(dbt_profiles_dir).expanduser()
        if dbt_profiles_dir
        else Path.home() / ".dbt"
    )
    config_location = dbt_user_dir / "mcp.yml"
    if not config_location.exists():
        return
    context_manager = DbtPlatformContextManager(config_location)
    existing = context_manager.read_context()
    if existing and existing.dbt_host:
        context_manager.write_context_to_file(
            existing.model_copy(update={"dbt_host": None})
        )


class ElicitingCredentialsProvider:
    """Wrap CredentialsProvider to elicit DBT_HOST when missing."""

    def __init__(self, inner: CredentialsProvider) -> None:
        self._inner = inner
        self._lock = asyncio.Lock()

    async def get_credentials(self) -> tuple[DbtMcpSettings, TokenProvider]:
        """Delegate to inner provider; elicit DBT_HOST on MissingHostError.

        On MissingHostError the user is prompted for their dbt Platform host.
        The entered host is validated via a TCP probe before the expensive
        OAuth flow is attempted.  If validation or OAuth fails the host is
        cleared from both in-memory settings and mcp.yml, and the user is
        re-prompted (up to MAX_ELICITATION_RETRIES times).
        """
        async with self._lock:
            try:
                return await self._inner.get_credentials()
            except MissingHostError as last_error:
                elicit_message = (
                    "Let's set up dbt-mcp. What's your dbt Platform host? "
                    "(You can also set the DBT_HOST environment variable "
                    "to skip this prompt.)"
                )
                host_schema = DbtHostSchema

                for _attempt in range(MAX_ELICITATION_RETRIES):
                    data = await elicit_or_raise(
                        last_error,
                        host_schema,
                        elicit_message,
                    )

                    if not await validate_host_reachable(data.dbt_host):
                        logger.warning(
                            "User input host '%s' is not reachable on port 443 (attempt %d/%d)",
                            data.dbt_host,
                            _attempt + 1,
                            MAX_ELICITATION_RETRIES,
                        )
                        error_hint = (
                            f"Could not reach '{data.dbt_host}'. "
                            "Please double-check the hostname."
                        )
                        last_error = MissingHostError(error_hint)
                        host_schema = _host_schema_with_hint(error_hint)
                        continue

                    self._inner.settings.dbt_host = data.dbt_host
                    try:
                        return await asyncio.wait_for(
                            self._inner.get_credentials(),
                            timeout=OAUTH_TIMEOUT,
                        )
                    except TimeoutError:
                        logger.warning(
                            "Authentication timed out for host '%s' (attempt %d/%d)",
                            data.dbt_host,
                            _attempt + 1,
                            MAX_ELICITATION_RETRIES,
                        )
                        self._inner.settings.dbt_host = None
                        _clear_persisted_host(self._inner.settings.dbt_profiles_dir)
                        error_hint = (
                            f"Authentication timed out for '{data.dbt_host}'. "
                            "Please verify the hostname is correct."
                        )
                        last_error = MissingHostError(error_hint)
                        host_schema = _host_schema_with_hint(error_hint)
                        continue
                    except Exception:
                        self._inner.settings.dbt_host = None
                        _clear_persisted_host(self._inner.settings.dbt_profiles_dir)
                        raise

                raise last_error

    @property
    def inner_provider(self) -> CredentialsProvider:
        return self._inner

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
