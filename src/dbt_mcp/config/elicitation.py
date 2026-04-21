"""MCP elicitation primitives."""

from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING

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


class DbtHostSchema(BaseModel):
    """Elicitation form for dbt Cloud host."""

    dbt_host: str = Field(
        min_length=1,
        description="Your dbt Cloud host (e.g., ab123.us1.dbt.com — find it in your browser URL bar when logged into dbt Cloud)",
    )

    @field_validator("dbt_host", mode="before")
    @classmethod
    def strip_whitespace(cls, v: str) -> str:
        return v.strip()


class ElicitingCredentialsProvider:
    """Wrap CredentialsProvider to elicit DBT_HOST when missing."""

    def __init__(self, inner: CredentialsProvider) -> None:
        self._inner = inner
        self._lock = asyncio.Lock()

    async def get_credentials(self) -> tuple[DbtMcpSettings, TokenProvider]:
        """Delegate to inner provider; elicit DBT_HOST on MissingHostError."""
        async with self._lock:
            try:
                return await self._inner.get_credentials()
            except MissingHostError as e:
                data = await elicit_or_raise(
                    e,
                    DbtHostSchema,
                    "Let's set up dbt-mcp. What's your dbt Cloud host?",
                )
                self._inner.settings.dbt_host = data.dbt_host
                try:
                    return await self._inner.get_credentials()
                except Exception:
                    self._inner.settings.dbt_host = None
                    raise

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
