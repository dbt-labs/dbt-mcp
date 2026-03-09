from urllib.parse import urlparse


def app_wrapper_html(
    *,
    app_name: str,
    cdn_base: str,
    version: str = "latest",
) -> str:
    """Generate a minimal HTML shell that loads an MCP App from CDN."""
    parsed = urlparse(cdn_base)
    origin = f"{parsed.scheme}://{parsed.netloc}"
    harness_url = f"{cdn_base}/harness/v1/index.js"
    app_url = f"{cdn_base}/{app_name}/{version}/bundle.js"

    return f"""<!DOCTYPE html>
<html><head>
  <meta charset="utf-8">
  <meta http-equiv="Content-Security-Policy"
        content="script-src 'unsafe-inline' {origin};">
</head>
<body>
  <div id="root"></div>
  <script type="module">
    const {{ init }} = await import("{harness_url}");
    init({{
      el: "#root",
      appUrl: "{app_url}",
    }});
  </script>
</body></html>"""
