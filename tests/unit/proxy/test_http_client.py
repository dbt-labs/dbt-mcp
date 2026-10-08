import httpx

from dbt_mcp.proxy.http_client import (
    MCP_HTTP_READ_TIMEOUT_SECONDS,
    MCP_HTTP_TIMEOUT_SECONDS,
    create_mcp_http_client,
)


async def test_create_mcp_http_client_applies_headers_and_mcp_timeouts():
    headers = {"Authorization": "token abc", "x-dbt-prod-environment-id": "3"}

    async with create_mcp_http_client(headers=headers) as client:
        assert isinstance(client, httpx.AsyncClient)
        assert client.headers["Authorization"] == "token abc"
        assert client.headers["x-dbt-prod-environment-id"] == "3"
        assert client.timeout == httpx.Timeout(
            MCP_HTTP_TIMEOUT_SECONDS, read=MCP_HTTP_READ_TIMEOUT_SECONDS
        )
