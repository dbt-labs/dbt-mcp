from dbt_mcp.apps.wrapper import app_wrapper_html

PROD_CDN_BASE = "https://cloud-ui.cdn.getdbt.com/dbt-ui/mcp-apps"
STAGING_CDN_BASE = "https://cloud-ui.staging.cdn.getdbt.com/dbt-ui/mcp-apps"
LOCAL_CDN_BASE = "http://localhost:3000/dbt-ui/mcp-apps"


class TestAppWrapperHtml:
    def test_contains_correct_harness_url(self):
        html = app_wrapper_html(app_name="query-data-preview", cdn_base=PROD_CDN_BASE)
        assert f"{PROD_CDN_BASE}/harness/v1/index.js" in html

    def test_contains_correct_app_bundle_url(self):
        html = app_wrapper_html(app_name="query-data-preview", cdn_base=PROD_CDN_BASE)
        assert f"{PROD_CDN_BASE}/query-data-preview/latest/bundle.js" in html

    def test_version_interpolated_in_bundle_url(self):
        html = app_wrapper_html(
            app_name="query-data-preview", cdn_base=PROD_CDN_BASE, version="v2"
        )
        assert f"{PROD_CDN_BASE}/query-data-preview/v2/bundle.js" in html

    def test_csp_matches_production_origin(self):
        html = app_wrapper_html(app_name="query-data-preview", cdn_base=PROD_CDN_BASE)
        assert "script-src 'unsafe-inline' https://cloud-ui.cdn.getdbt.com;" in html

    def test_csp_matches_staging_origin(self):
        html = app_wrapper_html(
            app_name="query-data-preview", cdn_base=STAGING_CDN_BASE
        )
        assert (
            "script-src 'unsafe-inline' https://cloud-ui.staging.cdn.getdbt.com;" in html
        )

    def test_csp_matches_localhost_origin(self):
        html = app_wrapper_html(app_name="query-data-preview", cdn_base=LOCAL_CDN_BASE)
        assert "script-src 'unsafe-inline' http://localhost:3000;" in html

    def test_html_is_valid_document(self):
        html = app_wrapper_html(app_name="query-data-preview", cdn_base=PROD_CDN_BASE)
        assert html.startswith("<!DOCTYPE html>")
        assert "<div id=\"root\"></div>" in html
        assert "</html>" in html
