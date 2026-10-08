import httpx

from dbt_mcp.config.settings import REMOTE_MCP_READ_TIMEOUT, REMOTE_MCP_TIMEOUT


def create_mcp_http_client(headers: dict[str, str]) -> httpx.AsyncClient:
    """Create an httpx client for a remote MCP streamable HTTP connection.

    Redirects are left at the httpx default; the MCP transport follows
    same-origin redirects itself. The returned client must be used as an async
    context manager so its connection pool is closed.
    """
    return httpx.AsyncClient(
        headers=headers,
        timeout=httpx.Timeout(REMOTE_MCP_TIMEOUT, read=REMOTE_MCP_READ_TIMEOUT),
    )
