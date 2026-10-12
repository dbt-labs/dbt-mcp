import asyncio
import socket
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import timedelta
from unittest.mock import AsyncMock, MagicMock

import uvicorn
from mcp import ClientSession
from mcp.client.streamable_http import streamable_http_client
from mcp.server.fastmcp import Context, FastMCP
from mcp.types import TextContent

from dbt_mcp.config.config import Config
from dbt_mcp.config.config_providers import ProxiedToolConfig, StaticConfigProvider
from dbt_mcp.config.headers import ProxiedToolHeadersProvider
from dbt_mcp.config.settings import DbtMcpSettings
from dbt_mcp.mcp.server import DbtMCP, app_lifespan
from dbt_mcp.oauth.token_provider import StaticTokenProvider
from dbt_mcp.tools.tool_names import ToolName
from dbt_mcp.tracking.tracking import UsageTracker
from tests.mocks.config import MockCredentialsProvider


@asynccontextmanager
async def running_server(server: FastMCP) -> AsyncIterator[str]:
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    http_server = uvicorn.Server(
        uvicorn.Config(server.streamable_http_app(), log_level="error")
    )
    task = asyncio.create_task(http_server.serve(sockets=[sock]))
    try:
        async with asyncio.timeout(5):
            while not http_server.started:
                await asyncio.sleep(0.01)
        yield f"http://127.0.0.1:{port}/mcp"
    finally:
        http_server.should_exit = True
        await task
        sock.close()


@asynccontextmanager
async def connected_client(url: str) -> AsyncIterator[ClientSession]:
    # Each client owns its transport contexts in a separate task, just as real
    # clients do. The test can disconnect one without closing the other.
    ready: asyncio.Future[ClientSession] = asyncio.get_running_loop().create_future()
    stop = asyncio.Event()

    async def run() -> None:
        async with streamable_http_client(url) as (reader, writer, _):
            async with ClientSession(
                reader, writer, read_timeout_seconds=timedelta(seconds=3)
            ) as session:
                await session.initialize()
                ready.set_result(session)
                await stop.wait()

    task = asyncio.create_task(run())
    try:
        yield await asyncio.wait_for(asyncio.shield(ready), 5)
    finally:
        stop.set()
        await task


async def proxy_result(client: ClientSession) -> str:
    result = await client.call_tool("execute_sql", {"sql": "select 1"})
    assert not result.isError, result
    block = result.content[0]
    assert isinstance(block, TextContent)
    return block.text


async def test_http_proxy_sessions_survive_disconnect_and_reconnect() -> None:
    remote = FastMCP("remote")

    @remote.tool(name="execute_sql")
    async def identify_connection(sql: str, context: Context) -> str:
        return f"{id(context.request_context.session)}:{sql}"

    async with running_server(remote) as remote_url:
        config = MagicMock(spec=Config)
        config.credentials_provider = MockCredentialsProvider(
            DbtMcpSettings.model_construct(dbt_project_ids=None)
        )
        config.proxied_tool_config_provider = StaticConfigProvider(
            ProxiedToolConfig(
                url=remote_url,
                headers_provider=ProxiedToolHeadersProvider(
                    token_provider=StaticTokenProvider(token="test-token")
                ),
                prod_environment_id=1,
                dev_environment_id=2,
                user_id=3,
            )
        )
        config.disable_tools = []
        config.enable_tools = [ToolName.EXECUTE_SQL]
        config.enabled_toolsets = set()
        config.disabled_toolsets = set()
        config.lsp_config = None
        local = DbtMCP(
            name="local",
            config=config,
            multi_project_mcp=FastMCP(),
            single_project_mcp=FastMCP(),
            usage_tracker=AsyncMock(spec=UsageTracker),
            lifespan=app_lifespan,
        )

        async with running_server(local) as local_url:
            async with connected_client(local_url) as first:
                assert [t.name for t in (await first.list_tools()).tools] == [
                    "execute_sql"
                ]
                first_connection = await proxy_result(first)
                async with connected_client(local_url) as second:
                    second_connection = await proxy_result(second)
                    assert second_connection != first_connection
                    assert await proxy_result(first) == first_connection
                assert await proxy_result(first) == first_connection
            async with connected_client(local_url) as reconnected:
                assert [t.name for t in (await reconnected.list_tools()).tools] == [
                    "execute_sql"
                ]
                assert await proxy_result(reconnected)
