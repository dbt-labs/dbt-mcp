import httpx
from mcp.server.fastmcp import FastMCP

from dbt_mcp.config.config import AppsConfig


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
    app_url = f"{config.cdn_base}/{app_name}/index.html"

    @dbt_mcp.resource(
        uri=uri,
        name=app_name,
        mime_type="text/html;profile=mcp-app",
    )
    def get_app_ui() -> str:
        with httpx.Client() as client:
            return client.get(app_url).raise_for_status().text
