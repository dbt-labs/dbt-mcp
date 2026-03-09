from mcp.server.fastmcp import FastMCP

from dbt_mcp.apps.wrapper import app_wrapper_html
from dbt_mcp.config.config import AppsConfig


def register_app_resource(
    dbt_mcp: FastMCP,
    config: AppsConfig,
    *,
    app_name: str,
    version: str = "latest",
) -> None:
    """Register a ui:// resource that serves the HTML shell for an MCP App."""
    uri = f"ui://dbt-mcp/{app_name}"

    @dbt_mcp.resource(
        uri=uri,
        name=app_name,
        mime_type="text/html;profile=mcp-app",
    )
    def get_app_ui() -> str:
        return app_wrapper_html(
            app_name=app_name,
            cdn_base=config.cdn_base,
            version=version,
        )
