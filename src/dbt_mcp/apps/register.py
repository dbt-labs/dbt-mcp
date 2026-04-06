import json

import httpx
from mcp.server.fastmcp import FastMCP

from dbt_mcp.config.config import AppsConfig


def _inline_app_html(bundle_js: str) -> str:
    """Generate self-contained HTML with bundle inlined as a blob URL.

    Fetches bundle from CDN at serve time so the iframe makes no external requests.
    """
    bundle_json = json.dumps(bundle_js)
    return f"""<!DOCTYPE html>
<html><head>
  <meta charset="utf-8">
  <meta http-equiv="Content-Security-Policy" content="script-src 'unsafe-inline' blob:;">
</head>
<body>
  <div id="root"></div>
  <script type="module">
    const bundleBlob = new Blob([{bundle_json}], {{type: "text/javascript"}});
    const appUrl = URL.createObjectURL(bundleBlob);
    await import(appUrl);
  </script>
</body></html>"""


def register_app_resource(
    dbt_mcp: FastMCP,
    config: AppsConfig,
    *,
    app_name: str,
) -> None:
    """Register a ui:// resource that fetches the bundle from CDN and inlines it."""
    uri = f"ui://dbt-mcp/{app_name}"
    bundle_url = f"{config.cdn_base}/{app_name}/bundle.js"

    @dbt_mcp.resource(
        uri=uri,
        name=app_name,
        mime_type="text/html;profile=mcp-app",
    )
    def get_app_ui() -> str:
        with httpx.Client() as client:
            bundle_js = client.get(bundle_url).raise_for_status().text
        return _inline_app_html(bundle_js=bundle_js)
