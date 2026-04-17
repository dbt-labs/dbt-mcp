import contextlib
import os
from collections.abc import AsyncGenerator

from mcp import ClientSession
from mcp.client.streamable_http import streamable_http_client
from mcp.shared._httpx_utils import create_mcp_http_client


@contextlib.asynccontextmanager
async def session_context() -> AsyncGenerator[ClientSession, None]:
    host = os.environ.get("DBT_HOST")
    if not host:
        raise ValueError("DBT_HOST environment variable is required")
    prefix = os.environ.get("DBT_HOST_PREFIX") or os.environ.get(
        "MULTICELL_ACCOUNT_PREFIX"
    )
    url = (
        f"https://{prefix}.{host}/api/ai/v1/mcp/"
        if prefix
        else f"https://{host}/api/ai/v1/mcp/"
    )
    token = os.environ.get("DBT_TOKEN")
    prod_environment_id = os.environ.get("DBT_PROD_ENV_ID", "")
    async with (
        create_mcp_http_client(
            headers={
                "Authorization": f"token {token}",
                "x-dbt-prod-environment-id": prod_environment_id,
            }
        ) as http_client,
        streamable_http_client(
            url=url,
            http_client=http_client,
        ) as (
            read_stream,
            write_stream,
            _,
        ),
        ClientSession(read_stream, write_stream) as session,
    ):
        await session.initialize()
        yield session
