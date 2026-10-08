import httpx

from dbt_mcp.config.settings import REMOTE_MCP_READ_TIMEOUT, REMOTE_MCP_TIMEOUT
from dbt_mcp.proxy.http_client import create_mcp_http_client


async def test_create_mcp_http_client_applies_headers_and_mcp_timeouts():
    headers = {"Authorization": "token abc", "x-dbt-prod-environment-id": "3"}

    async with create_mcp_http_client(headers=headers) as client:
        assert isinstance(client, httpx.AsyncClient)
        assert client.headers["Authorization"] == "token abc"
        assert client.headers["x-dbt-prod-environment-id"] == "3"
        assert client.timeout == httpx.Timeout(
            REMOTE_MCP_TIMEOUT, read=REMOTE_MCP_READ_TIMEOUT
        )
