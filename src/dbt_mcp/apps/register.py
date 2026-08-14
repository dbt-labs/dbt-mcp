import httpx
from mcp.server.fastmcp import FastMCP

from dbt_mcp.config.config import AppsConfig

# Bound the CDN fetch so a slow/unreachable CDN can't hang the resources/read.
_HTTP_TIMEOUT_SECONDS = 10.0


def register_app_resource(
    dbt_mcp: FastMCP,
    config: AppsConfig,
    *,
    app_name: str,
) -> None:
    """Register a ``ui://`` resource that serves an MCP App's single-file HTML.

    The app is built as a self-contained ``index.html`` (all JS/CSS inlined) and
    published to the CDN at ``<cdn_base>/<app_name>/index.html``. On read, the
    server fetches that HTML and returns it directly, so the host renders a fully
    self-contained app with no further external requests.
    """
    uri = f"ui://dbt-mcp/{app_name}"
    # Normalize cdn_base so a configured trailing slash doesn't produce a
    # double-slash URL (e.g. ``https://cdn//get-lineage/index.html``).
    app_url = f"{config.cdn_base.rstrip('/')}/{app_name}/index.html"

    @dbt_mcp.resource(
        uri=uri,
        name=app_name,
        mime_type="text/html;profile=mcp-app",
    )
    async def get_app_ui() -> str:
        async with httpx.AsyncClient(
            timeout=_HTTP_TIMEOUT_SECONDS, follow_redirects=True
        ) as client:
            response = await client.get(app_url)
            response.raise_for_status()
            return response.text
