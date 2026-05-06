from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

from mcp.client.session import ClientSession
from mcp.shared.memory import (
    create_connected_server_and_client_session as client_session,
)

from dbt_mcp.config.config import load_config
from dbt_mcp.mcp.server import create_dbt_mcp


@asynccontextmanager
async def client_session_context() -> AsyncGenerator[ClientSession, None]:
    config = load_config()
    server = await create_dbt_mcp(config)
    async with client_session(server) as client:
        yield client
