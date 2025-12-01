import os
from unittest.mock import patch

import pytest

from dbt_mcp.config.config import (
    DbtMcpSettings,
    load_config,
)
from dbt_mcp.config.settings import DEFAULT_DBT_CLI_TIMEOUT
from dbt_mcp.dbt_cli.binary_type import BinaryType
from dbt_mcp.tools.tool_names import ToolName


class TestDbtMcpSettings:
    def setup_method(self):
        # Clear environment variables that could interfere with default value tests
        env_vars_to_clear = [
            "DBT_HOST",
            "DBT_MCP_HOST",
            "DBT_PROD_ENV_ID",
            "DBT_ENV_ID",
            "DBT_DEV_ENV_ID",
            "DBT_USER_ID",
            "DBT_TOKEN",
            "DBT_PROJECT_DIR",
            "DBT_PATH",
            "DBT_CLI_TIMEOUT",
            "DISABLE_DBT_CLI",
            "DISABLE_DBT_CODEGEN",
            "DISABLE_SEMANTIC_LAYER",
            "DISABLE_DISCOVERY",
            "DISABLE_REMOTE",
            "DISABLE_ADMIN_API",
            "MULTICELL_ACCOUNT_PREFIX",
            "DBT_WARN_ERROR_OPTIONS",
            "DISABLE_TOOLS",
            "DBT_ACCOUNT_ID",
        ]
        for var in env_vars_to_clear:
            os.environ.pop(var, None)

    def test_default_values(self, env_setup):
        # Test with clean environment and no .env file
        clean_env = {
            "HOME": os.environ.get("HOME", ""),
        }  # Keep HOME for potential path resolution
        with env_setup(env_vars=clean_env):
            settings = DbtMcpSettings(_env_file=None)
            assert settings.dbt_path == "dbt"
            assert settings.dbt_cli_timeout == DEFAULT_DBT_CLI_TIMEOUT
            assert settings.disable_remote is None, "disable_remote"
            assert settings.disable_dbt_cli is False, "disable_dbt_cli"
            assert settings.disable_dbt_codegen is True, "disable_dbt_codegen"
            assert settings.disable_admin_api is False, "disable_admin_api"
            assert settings.disable_semantic_layer is False, "disable_semantic_layer"
            assert settings.disable_discovery is False, "disable_discovery"
            assert settings.disable_sql is None, "disable_sql"
            assert settings.disable_tools == [], "disable_tools"

    def test_usage_tracking_disabled_by_env_vars(self):
        env_vars = {
            "DO_NOT_TRACK": "true",
            "DBT_SEND_ANONYMOUS_USAGE_STATS": "1",
        }

        with patch.dict(os.environ, env_vars, clear=True):
            settings = DbtMcpSettings(_env_file=None)
            assert settings.usage_tracking_enabled is False

    def test_usage_tracking_respects_dbt_project_yaml(self, env_setup):
        with env_setup() as (project_dir, helpers):
            (project_dir / "dbt_project.yml").write_text(
                "flags:\n  send_anonymous_usage_stats: false\n"
            )

            settings = DbtMcpSettings(_env_file=None)
            assert settings.usage_tracking_enabled is False

    def test_usage_tracking_env_var_precedence_over_yaml(self, env_setup):
        env_vars = {
            "DBT_SEND_ANONYMOUS_USAGE_STATS": "false",
        }
        with env_setup(env_vars=env_vars) as (project_dir, helpers):
            (project_dir / "dbt_project.yml").write_text(
                "flags:\n  send_anonymous_usage_stats: true\n"
            )

            settings = DbtMcpSettings(_env_file=None)
            assert settings.usage_tracking_enabled is False

    @pytest.mark.parametrize(
        "do_not_track, send_anonymous_usage_stats",
        [
            ("true", "1"),
            ("1", "true"),
            ("true", None),
            ("1", None),
            (None, "false"),
            (None, "0"),
        ],
    )
    def test_usage_tracking_conflicting_env_vars_bias_off(
        self, do_not_track, send_anonymous_usage_stats
    ):
        env_vars = {}
        if do_not_track is not None:
            env_vars["DO_NOT_TRACK"] = do_not_track
        if send_anonymous_usage_stats is not None:
            env_vars["DBT_SEND_ANONYMOUS_USAGE_STATS"] = send_anonymous_usage_stats

        with patch.dict(os.environ, env_vars, clear=True):
            settings = DbtMcpSettings(_env_file=None)
            assert settings.usage_tracking_enabled is False

    def test_env_var_parsing(self, env_setup):
        env_vars = {
            "DBT_HOST": "test.dbt.com",
            "DBT_PROD_ENV_ID": "123",
            "DBT_TOKEN": "test_token",
            "DISABLE_DBT_CLI": "true",
            "DISABLE_TOOLS": "build,compile,docs",
        }

        with env_setup(env_vars=env_vars) as (project_dir, _):
            settings = DbtMcpSettings(_env_file=None)
            assert settings.dbt_host == "test.dbt.com"
            assert settings.dbt_prod_env_id == 123
            assert settings.dbt_token == "test_token"
            assert settings.dbt_project_dir == str(project_dir)
            assert settings.disable_dbt_cli is True
            assert settings.disable_tools == [
                ToolName.BUILD,
                ToolName.COMPILE,
                ToolName.DOCS,
            ]

    def test_disable_tools_parsing_edge_cases(self):
        test_cases = [
            ("build,compile,docs", [ToolName.BUILD, ToolName.COMPILE, ToolName.DOCS]),
            (
                "build, compile , docs",
                [ToolName.BUILD, ToolName.COMPILE, ToolName.DOCS],
            ),
            ("build,,docs", [ToolName.BUILD, ToolName.DOCS]),
            ("", []),
            ("run", [ToolName.RUN]),
        ]

        for input_val, expected in test_cases:
            with patch.dict(os.environ, {"DISABLE_TOOLS": input_val}):
                settings = DbtMcpSettings(_env_file=None)
                assert settings.disable_tools == expected

    def test_actual_host_property(self):
        with patch.dict(os.environ, {"DBT_HOST": "host1.com"}):
            settings = DbtMcpSettings(_env_file=None)
            assert settings.actual_host == "host1.com"

        with patch.dict(os.environ, {"DBT_MCP_HOST": "host2.com"}):
            settings = DbtMcpSettings(_env_file=None)
            assert settings.actual_host == "host2.com"

        with patch.dict(
            os.environ, {"DBT_HOST": "host1.com", "DBT_MCP_HOST": "host2.com"}
        ):
            settings = DbtMcpSettings(_env_file=None)
            assert settings.actual_host == "host1.com"  # DBT_HOST takes precedence

    def test_actual_prod_environment_id_property(self):
        with patch.dict(os.environ, {"DBT_PROD_ENV_ID": "123"}):
            settings = DbtMcpSettings(_env_file=None)
            assert settings.actual_prod_environment_id == 123

        with patch.dict(os.environ, {"DBT_ENV_ID": "456"}):
            settings = DbtMcpSettings(_env_file=None)
            assert settings.actual_prod_environment_id == 456

        with patch.dict(os.environ, {"DBT_PROD_ENV_ID": "123", "DBT_ENV_ID": "456"}):
            settings = DbtMcpSettings(_env_file=None)
            assert (
                settings.actual_prod_environment_id == 123
            )  # DBT_PROD_ENV_ID takes precedence

    def test_auto_disable_platform_features_logging(self):
        with patch.dict(os.environ, {}, clear=True):
            settings = DbtMcpSettings(_env_file=None)
            # When DBT_HOST is missing, platform features should be disabled
            assert settings.disable_admin_api is True
            assert settings.disable_sql is True
            assert settings.disable_semantic_layer is True
            assert settings.disable_discovery is True
            assert settings.disable_dbt_cli is True
            assert settings.disable_dbt_codegen is True


class TestLoadConfig:
    def setup_method(self):
        # Clear any existing environment variables that might interfere
        env_vars_to_clear = [
            "DBT_HOST",
            "DBT_MCP_HOST",
            "DBT_PROD_ENV_ID",
            "DBT_ENV_ID",
            "DBT_DEV_ENV_ID",
            "DBT_USER_ID",
            "DBT_TOKEN",
            "DBT_PROJECT_DIR",
            "DBT_PATH",
            "DBT_CLI_TIMEOUT",
            "DISABLE_DBT_CLI",
            "DISABLE_SEMANTIC_LAYER",
            "DISABLE_DISCOVERY",
            "DISABLE_REMOTE",
            "DISABLE_ADMIN_API",
            "MULTICELL_ACCOUNT_PREFIX",
            "DBT_WARN_ERROR_OPTIONS",
            "DISABLE_TOOLS",
            "DBT_ACCOUNT_ID",
        ]
        for var in env_vars_to_clear:
            os.environ.pop(var, None)

    def _load_config_with_env(self, env_vars):
        """Helper method to load config with test environment variables, avoiding .env file interference"""
        with (
            patch.dict(os.environ, env_vars),
            patch("dbt_mcp.config.config.DbtMcpSettings") as mock_settings_class,
            patch(
                "dbt_mcp.config.config.detect_binary_type",
                return_value=BinaryType.DBT_CORE,
            ),
        ):
            # Create a real instance with test values, but without .env file loading
            with patch.dict(os.environ, env_vars, clear=True):
                settings_instance = DbtMcpSettings(_env_file=None)
            mock_settings_class.return_value = settings_instance
            return load_config()

    def test_valid_config_all_services_enabled(self, env_setup):
        env_vars = {
            "DBT_HOST": "test.dbt.com",
            "DBT_PROD_ENV_ID": "123",
            "DBT_DEV_ENV_ID": "456",
            "DBT_USER_ID": "789",
            "DBT_ACCOUNT_ID": "123",
            "DBT_TOKEN": "test_token",
            "DISABLE_SEMANTIC_LAYER": "false",
            "DISABLE_DISCOVERY": "false",
            "DISABLE_REMOTE": "false",
            "DISABLE_ADMIN_API": "false",
            "DISABLE_DBT_CODEGEN": "false",
        }
        with env_setup(env_vars=env_vars) as (_project_dir, _helpers):
            config = load_config()

            assert config.proxied_tool_config_provider is not None, (
                "proxied_tool_config_provider should be set"
            )
            assert config.dbt_cli_config is not None, "dbt_cli_config should be set"
            assert config.discovery_config_provider is not None, (
                "discovery_config_provider should be set"
            )
            assert config.semantic_layer_config_provider is not None, (
                "semantic_layer_config_provider should be set"
            )
            assert config.admin_api_config_provider is not None, (
                "admin_api_config_provider should be set"
            )
            assert config.credentials_provider is not None, (
                "credentials_provider should be set"
            )
            assert config.dbt_codegen_config is not None, (
                "dbt_codegen_config should be set"
            )

    def test_valid_config_all_services_disabled(self):
        env_vars = {
            "DBT_TOKEN": "test_token",
            "DISABLE_DBT_CLI": "true",
            "DISABLE_SEMANTIC_LAYER": "true",
            "DISABLE_DISCOVERY": "true",
            "DISABLE_REMOTE": "true",
            "DISABLE_ADMIN_API": "true",
        }

        config = self._load_config_with_env(env_vars)

        assert config.proxied_tool_config_provider is None
        assert config.dbt_cli_config is None
        assert config.discovery_config_provider is None
        assert config.semantic_layer_config_provider is None

    def test_invalid_environment_variable_types(self):
        # Test invalid integer types
        env_vars = {
            "DBT_HOST": "test.dbt.com",
            "DBT_PROD_ENV_ID": "not_an_integer",
            "DBT_TOKEN": "test_token",
            "DISABLE_DISCOVERY": "false",
        }

        with pytest.raises(ValueError):
            self._load_config_with_env(env_vars)

    def test_multicell_account_prefix_configurations(self):
        env_vars = {
            "DBT_HOST": "test.dbt.com",
            "DBT_PROD_ENV_ID": "123",
            "DBT_TOKEN": "test_token",
            "MULTICELL_ACCOUNT_PREFIX": "prefix",
            "DISABLE_DISCOVERY": "false",
            "DISABLE_SEMANTIC_LAYER": "false",
            "DISABLE_DBT_CLI": "true",
            "DISABLE_REMOTE": "true",
        }

        config = self._load_config_with_env(env_vars)

        assert config.discovery_config_provider is not None
        assert config.semantic_layer_config_provider is not None

    def test_localhost_semantic_layer_config(self):
        env_vars = {
            "DBT_HOST": "localhost:8080",
            "DBT_PROD_ENV_ID": "123",
            "DBT_TOKEN": "test_token",
            "DISABLE_SEMANTIC_LAYER": "false",
            "DISABLE_DBT_CLI": "true",
            "DISABLE_DISCOVERY": "true",
            "DISABLE_REMOTE": "true",
        }

        config = self._load_config_with_env(env_vars)

        assert config.semantic_layer_config_provider is not None

    def test_warn_error_options_default_setting(self):
        env_vars = {
            "DBT_TOKEN": "test_token",
            "DISABLE_DBT_CLI": "true",
            "DISABLE_SEMANTIC_LAYER": "true",
            "DISABLE_DISCOVERY": "true",
            "DISABLE_REMOTE": "true",
            "DISABLE_ADMIN_API": "true",
        }

        # For this test, we need to call load_config directly to see environment side effects
        with (
            patch.dict(os.environ, env_vars, clear=True),
            patch("dbt_mcp.config.config.DbtMcpSettings") as mock_settings_class,
        ):
            settings_instance = DbtMcpSettings(_env_file=None)
            mock_settings_class.return_value = settings_instance
            load_config()

            assert (
                os.environ["DBT_WARN_ERROR_OPTIONS"]
                == '{"error": ["NoNodesForSelectionCriteria"]}'
            )

    def test_warn_error_options_not_overridden_if_set(self):
        env_vars = {
            "DBT_TOKEN": "test_token",
            "DBT_WARN_ERROR_OPTIONS": "custom_options",
            "DISABLE_DBT_CLI": "true",
            "DISABLE_SEMANTIC_LAYER": "true",
            "DISABLE_DISCOVERY": "true",
            "DISABLE_REMOTE": "true",
            "DISABLE_ADMIN_API": "true",
        }

        # For this test, we need to call load_config directly to see environment side effects
        with (
            patch.dict(os.environ, env_vars, clear=True),
            patch("dbt_mcp.config.config.DbtMcpSettings") as mock_settings_class,
        ):
            settings_instance = DbtMcpSettings(_env_file=None)
            mock_settings_class.return_value = settings_instance
            load_config()

            assert os.environ["DBT_WARN_ERROR_OPTIONS"] == "custom_options"

    def test_local_user_id_loading_from_dbt_profile(self):
        user_data = {"id": "local_user_123"}

        env_vars = {
            "DBT_TOKEN": "test_token",
            "HOME": "/fake/home",
            "DISABLE_DBT_CLI": "true",
            "DISABLE_SEMANTIC_LAYER": "true",
            "DISABLE_DISCOVERY": "true",
            "DISABLE_REMOTE": "true",
            "DISABLE_ADMIN_API": "true",
        }

        with (
            patch.dict(os.environ, env_vars),
            patch("dbt_mcp.tracking.tracking.try_read_yaml", return_value=user_data),
        ):
            config = self._load_config_with_env(env_vars)
            # local_user_id is now loaded by UsageTracker, not Config
            assert config.credentials_provider is not None

    def test_local_user_id_loading_failure_handling(self):
        env_vars = {
            "DBT_TOKEN": "test_token",
            "HOME": "/fake/home",
            "DISABLE_DBT_CLI": "true",
            "DISABLE_SEMANTIC_LAYER": "true",
            "DISABLE_DISCOVERY": "true",
            "DISABLE_REMOTE": "true",
            "DISABLE_ADMIN_API": "true",
        }

        with (
            patch.dict(os.environ, env_vars),
            patch("dbt_mcp.tracking.tracking.try_read_yaml", return_value=None),
        ):
            config = self._load_config_with_env(env_vars)
            # local_user_id is now loaded by UsageTracker, not Config
            assert config.credentials_provider is not None

    def test_legacy_env_id_support(self):
        # Test that DBT_ENV_ID still works for backward compatibility
        env_vars = {
            "DBT_HOST": "test.dbt.com",
            "DBT_ENV_ID": "123",  # Using legacy variable
            "DBT_TOKEN": "test_token",
            "DISABLE_DISCOVERY": "false",
            "DISABLE_DBT_CLI": "true",
            "DISABLE_SEMANTIC_LAYER": "true",
            "DISABLE_REMOTE": "true",
        }

        config = self._load_config_with_env(env_vars)
        assert config.discovery_config_provider is not None
        assert config.credentials_provider is not None

    def test_case_insensitive_environment_variables(self):
        # pydantic_settings should handle case insensitivity based on config
        env_vars = {
            "dbt_host": "test.dbt.com",  # lowercase
            "DBT_PROD_ENV_ID": "123",  # uppercase
            "dbt_token": "test_token",  # lowercase
            "DISABLE_DISCOVERY": "false",
            "DISABLE_DBT_CLI": "true",
            "DISABLE_SEMANTIC_LAYER": "true",
            "DISABLE_REMOTE": "true",
        }

        config = self._load_config_with_env(env_vars)
        assert config.discovery_config_provider is not None
        assert config.credentials_provider is not None
