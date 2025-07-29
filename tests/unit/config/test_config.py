import os
from unittest.mock import mock_open, patch

import pytest
import yaml

from dbt_mcp.config.config import (
    DbtMcpSettings,
    load_config,
)
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
            "DISABLE_SEMANTIC_LAYER",
            "DISABLE_DISCOVERY",
            "DISABLE_REMOTE",
            "MULTICELL_ACCOUNT_PREFIX",
            "DBT_WARN_ERROR_OPTIONS",
            "DISABLE_TOOLS",
            "DBT_ACCOUNT_ID",
        ]
        for var in env_vars_to_clear:
            os.environ.pop(var, None)

    def test_default_values(self):
        # Test with clean environment and no .env file
        clean_env = {
            "HOME": os.environ.get("HOME", "")
        }  # Keep HOME for potential path resolution
        with patch.dict(os.environ, clean_env, clear=True):
            settings = DbtMcpSettings(_env_file=None)
            assert settings.dbt_path == "dbt"
            assert settings.dbt_cli_timeout == 10
            assert settings.disable_dbt_cli is False
            assert settings.disable_semantic_layer is False
            assert settings.disable_discovery is False
            assert settings.disable_remote is None
            assert settings.disable_sql is None
            assert settings.disable_tools == []

    def test_env_var_parsing(self):
        env_vars = {
            "DBT_HOST": "test.dbt.com",
            "DBT_PROD_ENV_ID": "123",
            "DBT_TOKEN": "test_token",
            "DBT_PROJECT_DIR": "/test/project",
            "DISABLE_DBT_CLI": "true",
            "DISABLE_TOOLS": "build,compile,docs",
        }

        with patch.dict(os.environ, env_vars):
            settings = DbtMcpSettings(_env_file=None)
            assert settings.dbt_host == "test.dbt.com"
            assert settings.dbt_prod_env_id == 123
            assert settings.dbt_token == "test_token"
            assert settings.dbt_project_dir == "/test/project"
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
        ):
            # Create a real instance with test values, but without .env file loading
            with patch.dict(os.environ, env_vars, clear=True):
                settings_instance = DbtMcpSettings(_env_file=None)
            mock_settings_class.return_value = settings_instance
            return load_config()

    def test_valid_config_all_services_enabled(self):
        env_vars = {
            "DBT_HOST": "test.dbt.com",
            "DBT_PROD_ENV_ID": "123",
            "DBT_DEV_ENV_ID": "456",
            "DBT_USER_ID": "789",
            "DBT_TOKEN": "test_token",
            "DBT_PROJECT_DIR": "/test/project",
            "DISABLE_SEMANTIC_LAYER": "false",
            "DISABLE_DISCOVERY": "false",
            "DISABLE_REMOTE": "false",
        }

        config = self._load_config_with_env(env_vars)

        assert config.tracking_config.host == "test.dbt.com"
        assert config.tracking_config.prod_environment_id == 123
        assert config.sql_config is not None
        assert config.sql_config.host == "test.dbt.com"
        assert config.dbt_cli_config is not None
        assert config.discovery_config is not None
        assert config.semantic_layer_config is not None

    def test_valid_config_all_services_disabled(self):
        env_vars = {
            "DISABLE_DBT_CLI": "true",
            "DISABLE_SEMANTIC_LAYER": "true",
            "DISABLE_DISCOVERY": "true",
            "DISABLE_REMOTE": "true",
        }

        config = self._load_config_with_env(env_vars)

        assert config.sql_config is None
        assert config.dbt_cli_config is None
        assert config.discovery_config is None
        assert config.semantic_layer_config is None

    def test_missing_required_host_error(self):
        env_vars = {
            "DBT_PROD_ENV_ID": "123",
            "DBT_TOKEN": "test_token",
            "DISABLE_SEMANTIC_LAYER": "false",
        }

        with pytest.raises(
            ValueError, match="DBT_HOST environment variable is required"
        ):
            self._load_config_with_env(env_vars)

    def test_missing_required_prod_env_id_error(self):
        env_vars = {
            "DBT_HOST": "test.dbt.com",
            "DBT_TOKEN": "test_token",
            "DISABLE_DISCOVERY": "false",
        }

        with pytest.raises(
            ValueError, match="DBT_PROD_ENV_ID environment variable is required"
        ):
            self._load_config_with_env(env_vars)

    def test_missing_required_token_error(self):
        env_vars = {
            "DBT_HOST": "test.dbt.com",
            "DBT_PROD_ENV_ID": "123",
        }

        with pytest.raises(
            ValueError, match="DBT_TOKEN environment variable is required"
        ):
            self._load_config_with_env(env_vars)

    def test_missing_required_dev_env_id_for_remote(self):
        env_vars = {
            "DBT_HOST": "test.dbt.com",
            "DBT_PROD_ENV_ID": "123",
            "DBT_TOKEN": "test_token",
            "DBT_USER_ID": "789",
            "DISABLE_REMOTE": "false",
        }

        with pytest.raises(
            ValueError, match="DBT_DEV_ENV_ID environment variable is required"
        ):
            self._load_config_with_env(env_vars)

    def test_missing_required_user_id_for_remote(self):
        env_vars = {
            "DBT_HOST": "test.dbt.com",
            "DBT_PROD_ENV_ID": "123",
            "DBT_DEV_ENV_ID": "456",
            "DBT_TOKEN": "test_token",
            "DISABLE_REMOTE": "false",
        }

        with pytest.raises(
            ValueError, match="DBT_USER_ID environment variable is required"
        ):
            self._load_config_with_env(env_vars)

    def test_missing_required_project_dir_for_cli(self):
        env_vars = {
            "DISABLE_DBT_CLI": "false",
        }

        with pytest.raises(
            ValueError, match="DBT_PROJECT_DIR environment variable is required"
        ):
            self._load_config_with_env(env_vars)

    def test_invalid_host_starting_with_metadata(self):
        env_vars = {
            "DBT_HOST": "metadata.test.dbt.com",
            "DBT_PROD_ENV_ID": "123",
            "DBT_TOKEN": "test_token",
            "DISABLE_DISCOVERY": "false",
            "DISABLE_DBT_CLI": "true",
            "DISABLE_SEMANTIC_LAYER": "true",
            "DISABLE_REMOTE": "true",
        }

        with pytest.raises(
            ValueError,
            match="DBT_HOST must not start with 'metadata' or 'semantic-layer'",
        ):
            self._load_config_with_env(env_vars)

    def test_invalid_host_starting_with_semantic_layer(self):
        env_vars = {
            "DBT_HOST": "semantic-layer.test.dbt.com",
            "DBT_PROD_ENV_ID": "123",
            "DBT_TOKEN": "test_token",
            "DISABLE_SEMANTIC_LAYER": "false",
            "DISABLE_DBT_CLI": "true",
            "DISABLE_DISCOVERY": "true",
            "DISABLE_REMOTE": "true",
        }

        with pytest.raises(
            ValueError,
            match="DBT_HOST must not start with 'metadata' or 'semantic-layer'",
        ):
            self._load_config_with_env(env_vars)

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

    def test_empty_environment_variables(self):
        env_vars = {
            "DBT_HOST": "",
            "DBT_PROD_ENV_ID": "123",
            "DBT_TOKEN": "test_token",
            "DISABLE_DISCOVERY": "false",
        }

        with pytest.raises(
            ValueError, match="DBT_HOST environment variable is required"
        ):
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

        assert "prefix.metadata.test.dbt.com" in config.discovery_config.url
        assert config.semantic_layer_config.host == "prefix.semantic-layer.test.dbt.com"

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

        assert config.semantic_layer_config.url.startswith("http://")
        assert "localhost:8080" in config.semantic_layer_config.url

    def test_warn_error_options_default_setting(self):
        env_vars = {
            "DISABLE_DBT_CLI": "true",
            "DISABLE_SEMANTIC_LAYER": "true",
            "DISABLE_DISCOVERY": "true",
            "DISABLE_REMOTE": "true",
        }

        # For this test, we need to call load_config directly to see environment side effects
        with patch.dict(os.environ, env_vars, clear=True):
            with patch("dbt_mcp.config.config.DbtMcpSettings") as mock_settings_class:
                settings_instance = DbtMcpSettings(_env_file=None)
                mock_settings_class.return_value = settings_instance
                load_config()

                assert (
                    os.environ["DBT_WARN_ERROR_OPTIONS"]
                    == '{"error": ["NoNodesForSelectionCriteria"]}'
                )

    def test_warn_error_options_not_overridden_if_set(self):
        env_vars = {
            "DBT_WARN_ERROR_OPTIONS": "custom_options",
            "DISABLE_DBT_CLI": "true",
            "DISABLE_SEMANTIC_LAYER": "true",
            "DISABLE_DISCOVERY": "true",
            "DISABLE_REMOTE": "true",
        }

        # For this test, we need to call load_config directly to see environment side effects
        with patch.dict(os.environ, env_vars, clear=True):
            with patch("dbt_mcp.config.config.DbtMcpSettings") as mock_settings_class:
                settings_instance = DbtMcpSettings(_env_file=None)
                mock_settings_class.return_value = settings_instance
                load_config()

                assert os.environ["DBT_WARN_ERROR_OPTIONS"] == "custom_options"

    def test_local_user_id_loading_from_dbt_profile(self):
        user_data = {"id": "local_user_123"}
        mock_file_content = yaml.dump(user_data)

        env_vars = {
            "HOME": "/fake/home",
            "DISABLE_DBT_CLI": "true",
            "DISABLE_SEMANTIC_LAYER": "true",
            "DISABLE_DISCOVERY": "true",
            "DISABLE_REMOTE": "true",
        }

        with patch.dict(os.environ, env_vars):
            with patch("pathlib.Path.exists", return_value=True):
                with patch("builtins.open", mock_open(read_data=mock_file_content)):
                    config = self._load_config_with_env(env_vars)
                    assert config.tracking_config.local_user_id == "local_user_123"

    def test_local_user_id_loading_failure_handling(self):
        env_vars = {
            "HOME": "/fake/home",
            "DISABLE_DBT_CLI": "true",
            "DISABLE_SEMANTIC_LAYER": "true",
            "DISABLE_DISCOVERY": "true",
            "DISABLE_REMOTE": "true",
        }

        with patch.dict(os.environ, env_vars):
            with patch("pathlib.Path.exists", return_value=False):
                config = self._load_config_with_env(env_vars)
                assert config.tracking_config.local_user_id is None

    def test_remote_requirements(self):
        # Test that remote_config is only created when remote tools are enabled
        # and all required fields are present
        env_vars = {
            "DBT_HOST": "test.dbt.com",
            "DBT_PROD_ENV_ID": "123",
            "DBT_TOKEN": "test_token",
            "DISABLE_REMOTE": "true",
            "DISABLE_DBT_CLI": "true",
            "DISABLE_SEMANTIC_LAYER": "true",
            "DISABLE_DISCOVERY": "true",
        }

        config = self._load_config_with_env(env_vars)
        # Remote config should not be created when remote tools are disabled
        assert config.sql_config is None

        # Test remote requirements (needs user_id and dev_env_id too)
        env_vars.update(
            {
                "DBT_USER_ID": "789",
                "DBT_DEV_ENV_ID": "456",
                "DISABLE_REMOTE": "false",
            }
        )

        config = self._load_config_with_env(env_vars)
        assert config.sql_config is not None
        assert config.sql_config.user_id == 789
        assert config.sql_config.dev_environment_id == 456

    def test_disable_flags_combinations(self):
        base_env = {
            "DBT_HOST": "test.dbt.com",
            "DBT_PROD_ENV_ID": "123",
            "DBT_TOKEN": "test_token",
            "DBT_PROJECT_DIR": "/test",
        }

        test_cases = [
            # Only CLI enabled
            {
                "DISABLE_DBT_CLI": "false",
                "DISABLE_SEMANTIC_LAYER": "true",
                "DISABLE_DISCOVERY": "true",
                "DISABLE_REMOTE": "true",
            },
            # Only semantic layer enabled
            {
                "DISABLE_DBT_CLI": "true",
                "DISABLE_SEMANTIC_LAYER": "false",
                "DISABLE_DISCOVERY": "true",
                "DISABLE_REMOTE": "true",
            },
            # Multiple services enabled
            {
                "DISABLE_DBT_CLI": "false",
                "DISABLE_SEMANTIC_LAYER": "false",
                "DISABLE_DISCOVERY": "false",
                "DISABLE_REMOTE": "true",
            },
        ]

        for disable_flags in test_cases:
            env_vars = {**base_env, **disable_flags}
            config = self._load_config_with_env(env_vars)

            # Verify configs are created only when services are enabled
            assert (config.dbt_cli_config is not None) == (
                disable_flags["DISABLE_DBT_CLI"] == "false"
            )
            assert (config.semantic_layer_config is not None) == (
                disable_flags["DISABLE_SEMANTIC_LAYER"] == "false"
            )
            assert (config.discovery_config is not None) == (
                disable_flags["DISABLE_DISCOVERY"] == "false"
            )

    def test_multiple_validation_errors(self):
        # Test that multiple validation errors are collected and reported
        env_vars = {
            "DISABLE_DISCOVERY": "false",
            "DISABLE_REMOTE": "false",
            "DISABLE_DBT_CLI": "false",
        }

        with pytest.raises(ValueError) as exc_info:
            self._load_config_with_env(env_vars)

        error_message = str(exc_info.value)
        assert "DBT_HOST environment variable is required" in error_message
        assert "DBT_PROD_ENV_ID environment variable is required" in error_message
        assert "DBT_TOKEN environment variable is required" in error_message
        assert "DBT_DEV_ENV_ID environment variable is required" in error_message
        assert "DBT_USER_ID environment variable is required" in error_message
        assert "DBT_PROJECT_DIR environment variable is required" in error_message

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
        assert config.tracking_config.prod_environment_id == 123
        assert config.discovery_config.environment_id == 123

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
        assert config.tracking_config.host == "test.dbt.com"
