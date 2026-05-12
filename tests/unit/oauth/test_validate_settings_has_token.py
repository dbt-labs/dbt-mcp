"""Tests for validate_dbt_platform_settings token validation."""

from dbt_mcp.config.settings import (
    DbtMcpSettings,
    validate_dbt_platform_settings,
)


class TestValidateDbtPlatformSettingsToken:
    def test_missing_token_produces_error(self):
        """Missing dbt_token should produce an error."""
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

    def test_with_token_set_no_error(self):
        """When dbt_token is set, no error."""
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
