import httpx

# These match the defaults the MCP SDK recommends for streamable HTTP
# transports: 30s for connect/write/pool, and a 300s read timeout so a server
# holding a response stream open isn't cut off.
MCP_HTTP_TIMEOUT_SECONDS = 30.0
MCP_HTTP_READ_TIMEOUT_SECONDS = 300.0


def create_mcp_http_client(headers: dict[str, str]) -> httpx.AsyncClient:
    """Create an httpx client configured for a remote MCP streamable HTTP connection.

    The returned client must be used as an async context manager so its
    connection pool is closed.
    """
    return httpx.AsyncClient(
        headers=headers,
        timeout=httpx.Timeout(
            MCP_HTTP_TIMEOUT_SECONDS, read=MCP_HTTP_READ_TIMEOUT_SECONDS
        ),
    )
