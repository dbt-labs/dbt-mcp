"""
Tests for validate_dbt_platform_settings with has_token parameter.
When has_token=True, the dbt_token check should be skipped.
"""

from dbt_mcp.config.settings import (
    DbtMcpSettings,
    validate_dbt_platform_settings,
)


class TestValidateDbtPlatformSettingsHasToken:
    def test_no_token_without_has_token_produces_error(self):
        """Without has_token, missing dbt_token should produce an error."""
        settings = DbtMcpSettings.model_construct(
            dbt_host="cloud.getdbt.com",
            dbt_prod_env_id=123,
            dbt_token=None,
            disable_semantic_layer=False,
            disable_discovery=True,
            disable_admin_api=True,
            disable_sql=True,
        )
        errors = validate_dbt_platform_settings(settings)
        assert any("DBT_TOKEN" in e for e in errors)

    def test_no_token_with_has_token_skips_error(self):
        """With has_token=True, missing dbt_token should NOT produce an error."""
        settings = DbtMcpSettings.model_construct(
            dbt_host="cloud.getdbt.com",
            dbt_prod_env_id=123,
            dbt_token=None,
            disable_semantic_layer=False,
            disable_discovery=True,
            disable_admin_api=True,
            disable_sql=True,
        )
        errors = validate_dbt_platform_settings(settings, has_token=True)
        assert not any("DBT_TOKEN" in e for e in errors)

    def test_with_token_set_no_error_regardless(self):
        """When dbt_token is set, no error even without has_token."""
        settings = DbtMcpSettings.model_construct(
            dbt_host="cloud.getdbt.com",
            dbt_prod_env_id=123,
            dbt_token="some_token",
            disable_semantic_layer=False,
            disable_discovery=True,
            disable_admin_api=True,
            disable_sql=True,
        )
        errors = validate_dbt_platform_settings(settings)
        assert not any("DBT_TOKEN" in e for e in errors)
