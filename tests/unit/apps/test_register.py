from mcp.server.fastmcp import FastMCP

from dbt_mcp.apps.register import register_app_resource
from dbt_mcp.config.config import AppsConfig

CDN_BASE = "https://cloud-ui.cdn.getdbt.com/dbt-ui/mcp-apps"


class TestRegisterAppResource:
    def test_registers_resource_with_correct_uri(self):
        server = FastMCP("test")
        config = AppsConfig(cdn_base=CDN_BASE)
        register_app_resource(server, config, app_name="query-data-preview")

        resource_manager = server._resource_manager
        template = resource_manager._templates.get("ui://dbt-mcp/{app_name}")
        resource = resource_manager._resources.get("ui://dbt-mcp/query-data-preview")

        # Should be registered as a static resource (no URI params)
        assert resource is not None or template is not None

    def test_registers_resource_with_correct_mime_type(self):
        server = FastMCP("test")
        config = AppsConfig(cdn_base=CDN_BASE)
        register_app_resource(server, config, app_name="query-data-preview")

        resource = server._resource_manager._resources.get(
            "ui://dbt-mcp/query-data-preview"
        )
        assert resource is not None
        assert resource.mime_type == "text/html;profile=mcp-app"

    async def test_resource_returns_html_with_cdn_urls(self):
        server = FastMCP("test")
        config = AppsConfig(cdn_base=CDN_BASE)
        register_app_resource(server, config, app_name="query-data-preview")

        resource = server._resource_manager._resources.get(
            "ui://dbt-mcp/query-data-preview"
        )
        assert resource is not None
        html = await resource.read()
        assert isinstance(html, str)
        assert f"{CDN_BASE}/harness/v1/index.js" in html
        assert f"{CDN_BASE}/query-data-preview/latest/bundle.js" in html
