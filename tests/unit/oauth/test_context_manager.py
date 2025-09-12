"""
Unit tests for DbtPlatformContextManager.
"""

import tempfile
from pathlib import Path
from unittest.mock import patch

import pytest
import yaml

from dbt_mcp.oauth.context_manager import DbtPlatformContextManager
from dbt_mcp.oauth.dbt_platform import (
    DbtPlatformContext,
    DbtPlatformEnvironment,
)
from dbt_mcp.oauth.token import AccessTokenResponse, DecodedAccessToken


class TestDbtPlatformContextManager:
    """Test the DbtPlatformContextManager class."""

    @pytest.fixture
    def temp_config_file(self):
        """Create a temporary config file for testing."""
        with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".yaml") as f:
            temp_path = Path(f.name)
        yield temp_path
        # Cleanup
        if temp_path.exists():
            temp_path.unlink()
        if temp_path.parent.exists() and temp_path.parent.name.startswith("tmp"):
            temp_path.parent.rmdir()

    @pytest.fixture
    def temp_config_dir(self):
        """Create a temporary directory for config files."""
        with tempfile.TemporaryDirectory() as temp_dir:
            yield Path(temp_dir)

    @pytest.fixture
    def sample_context(self):
        """Create a sample DbtPlatformContext for testing."""
        access_token_response = AccessTokenResponse(
            access_token="test_access_token",
            refresh_token="test_refresh_token",
            expires_in=3600,
            scope="read write",
            token_type="Bearer",
            expires_at=1609459200,
        )

        decoded_claims = {
            "sub": "123",
            "iss": "https://auth.example.com",
            "exp": 1609459200,
        }

        decoded_token = DecodedAccessToken(
            access_token_response=access_token_response, decoded_claims=decoded_claims
        )

        return DbtPlatformContext(
            decoded_access_token=decoded_token,
            host_prefix="test-host",
            dev_environment=DbtPlatformEnvironment(
                id=1, name="Development", deployment_type="development"
            ),
            prod_environment=DbtPlatformEnvironment(
                id=2, name="Production", deployment_type="production"
            ),
        )

    def test_init(self, temp_config_file):
        """Test DbtPlatformContextManager initialization."""
        manager = DbtPlatformContextManager(temp_config_file)
        assert manager.config_location == temp_config_file

    def test_read_context_file_not_exists(self, temp_config_dir):
        """Test read_context when config file doesn't exist."""
        non_existent_file = temp_config_dir / "non_existent.yaml"
        manager = DbtPlatformContextManager(non_existent_file)

        result = manager.read_context()

        assert result is None

    def test_read_context_empty_file(self, temp_config_file):
        """Test read_context when config file is empty."""
        temp_config_file.write_text("")
        manager = DbtPlatformContextManager(temp_config_file)

        result = manager.read_context()

        assert result is None

    def test_read_context_whitespace_only_file(self, temp_config_file):
        """Test read_context when config file contains only whitespace."""
        temp_config_file.write_text("   \n  \t  \n  ")
        manager = DbtPlatformContextManager(temp_config_file)

        result = manager.read_context()

        assert result is None

    def test_read_context_valid_file(self, temp_config_file, sample_context):
        """Test read_context when config file contains valid YAML."""
        # Write sample context to file
        yaml_content = yaml.dump(sample_context.model_dump(), default_flow_style=False)
        temp_config_file.write_text(yaml_content)

        manager = DbtPlatformContextManager(temp_config_file)
        result = manager.read_context()

        assert result is not None
        assert isinstance(result, DbtPlatformContext)
        assert result.host_prefix == sample_context.host_prefix
        assert result.dev_environment.id == sample_context.dev_environment.id
        assert result.prod_environment.name == sample_context.prod_environment.name

    def test_read_context_invalid_yaml(self, temp_config_file):
        """Test read_context when config file contains invalid YAML."""
        temp_config_file.write_text("invalid: yaml: content: [")
        manager = DbtPlatformContextManager(temp_config_file)

        with pytest.raises(yaml.YAMLError):
            manager.read_context()

    def test_ensure_config_location_exists_creates_parent_dirs(self, temp_config_dir):
        """Test that _ensure_config_location_exists creates parent directories."""
        nested_path = temp_config_dir / "nested" / "deep" / "config.yaml"
        manager = DbtPlatformContextManager(nested_path)

        assert not nested_path.parent.exists()

        manager._ensure_config_location_exists()

        assert nested_path.parent.exists()
        assert nested_path.exists()

    def test_ensure_config_location_exists_touches_file(self, temp_config_dir):
        """Test that _ensure_config_location_exists creates the config file."""
        config_file = temp_config_dir / "config.yaml"
        manager = DbtPlatformContextManager(config_file)

        assert not config_file.exists()

        manager._ensure_config_location_exists()

        assert config_file.exists()
        assert config_file.read_text() == ""

    def test_ensure_config_location_exists_idempotent(self, temp_config_file):
        """Test that _ensure_config_location_exists is idempotent."""
        temp_config_file.write_text("existing content")
        manager = DbtPlatformContextManager(temp_config_file)

        original_content = temp_config_file.read_text()

        manager._ensure_config_location_exists()

        # Should not modify existing file
        assert temp_config_file.read_text() == original_content

    def test_write_context_to_file(self, temp_config_file, sample_context):
        """Test write_context_to_file writes valid YAML."""
        manager = DbtPlatformContextManager(temp_config_file)

        manager.write_context_to_file(sample_context)

        assert temp_config_file.exists()
        written_content = temp_config_file.read_text()
        assert written_content.strip() != ""

        # Verify it's valid YAML and can be parsed back
        parsed_data = yaml.safe_load(written_content)
        recreated_context = DbtPlatformContext(**parsed_data)
        assert recreated_context.host_prefix == sample_context.host_prefix

    def test_write_context_to_file_creates_directories(
        self, temp_config_dir, sample_context
    ):
        """Test write_context_to_file creates parent directories."""
        nested_path = temp_config_dir / "nested" / "config.yaml"
        manager = DbtPlatformContextManager(nested_path)

        assert not nested_path.parent.exists()

        manager.write_context_to_file(sample_context)

        assert nested_path.exists()
        assert nested_path.parent.exists()

    def test_update_context_with_no_existing_context(
        self, temp_config_dir, sample_context
    ):
        """Test update_context when no existing context exists."""
        config_file = temp_config_dir / "config.yaml"
        manager = DbtPlatformContextManager(config_file)

        result = manager.update_context(sample_context)

        assert result == sample_context
        assert config_file.exists()

        # Verify file was written correctly
        written_context = manager.read_context()
        assert written_context.host_prefix == sample_context.host_prefix

    def test_update_context_merges_with_existing_context(
        self, temp_config_file, sample_context
    ):
        """Test update_context merges new context with existing context."""
        # Write initial context
        initial_context = DbtPlatformContext(host_prefix="initial-host")
        manager = DbtPlatformContextManager(temp_config_file)
        manager.write_context_to_file(initial_context)

        # Create new context with different fields
        new_context = DbtPlatformContext(
            dev_environment=sample_context.dev_environment,
            decoded_access_token=sample_context.decoded_access_token,
        )

        result = manager.update_context(new_context)

        # Should merge: keep initial host_prefix, add new fields
        assert result.host_prefix == "initial-host"  # From existing
        assert (
            result.dev_environment.id == sample_context.dev_environment.id
        )  # From new
        assert (
            result.decoded_access_token == sample_context.decoded_access_token
        )  # From new

    def test_update_context_new_values_override_existing(
        self, temp_config_file, sample_context
    ):
        """Test update_context where new values override existing ones."""
        # Write initial context
        initial_context = DbtPlatformContext(
            host_prefix="initial-host",
            dev_environment=DbtPlatformEnvironment(
                id=999, name="Old Dev", deployment_type="development"
            ),
        )
        manager = DbtPlatformContextManager(temp_config_file)
        manager.write_context_to_file(initial_context)

        # Create new context that overrides dev_environment
        new_context = DbtPlatformContext(dev_environment=sample_context.dev_environment)

        result = manager.update_context(new_context)

        # New dev_environment should override old one
        assert result.host_prefix == "initial-host"  # Preserved
        assert (
            result.dev_environment.id == sample_context.dev_environment.id
        )  # Overridden
        assert (
            result.dev_environment.name == sample_context.dev_environment.name
        )  # Overridden

    @patch("dbt_mcp.oauth.context_manager.logger")
    def test_update_context_handles_file_read_errors_gracefully(
        self, mock_logger, temp_config_dir, sample_context
    ):
        """Test update_context handles file reading errors gracefully."""
        config_file = temp_config_dir / "config.yaml"
        # Create file with invalid YAML
        config_file.write_text("invalid: yaml: content: [")

        manager = DbtPlatformContextManager(config_file)

        # Should raise exception due to invalid YAML
        with pytest.raises(yaml.YAMLError):
            manager.update_context(sample_context)

    def test_update_context_empty_existing_file(self, temp_config_file, sample_context):
        """Test update_context when existing file is empty."""
        temp_config_file.write_text("")
        manager = DbtPlatformContextManager(temp_config_file)

        result = manager.update_context(sample_context)

        assert result == sample_context

        # Verify file was updated
        written_context = manager.read_context()
        assert written_context.host_prefix == sample_context.host_prefix

    def test_roundtrip_write_and_read(self, temp_config_file, sample_context):
        """Test that writing and then reading a context preserves all data."""
        manager = DbtPlatformContextManager(temp_config_file)

        # Write context
        manager.write_context_to_file(sample_context)

        # Read it back
        read_context = manager.read_context()

        # Should be equivalent
        assert read_context.host_prefix == sample_context.host_prefix
        assert read_context.dev_environment.id == sample_context.dev_environment.id
        assert read_context.dev_environment.name == sample_context.dev_environment.name
        assert read_context.prod_environment.id == sample_context.prod_environment.id
        assert (
            read_context.prod_environment.name == sample_context.prod_environment.name
        )

        # Token comparison
        assert (
            read_context.decoded_access_token.access_token_response.access_token
            == sample_context.decoded_access_token.access_token_response.access_token
        )

    def test_multiple_updates_preserve_data(self, temp_config_file):
        """Test that multiple updates preserve and accumulate data correctly."""
        manager = DbtPlatformContextManager(temp_config_file)

        # First update: add host_prefix
        context1 = DbtPlatformContext(host_prefix="host1")
        result1 = manager.update_context(context1)
        assert result1.host_prefix == "host1"

        # Second update: add dev_environment
        context2 = DbtPlatformContext(
            dev_environment=DbtPlatformEnvironment(
                id=1, name="Dev", deployment_type="development"
            )
        )
        result2 = manager.update_context(context2)
        assert result2.host_prefix == "host1"  # Preserved
        assert result2.dev_environment.name == "Dev"  # Added

        # Third update: override host_prefix, add prod_environment
        context3 = DbtPlatformContext(
            host_prefix="host2",
            prod_environment=DbtPlatformEnvironment(
                id=2, name="Prod", deployment_type="production"
            ),
        )
        result3 = manager.update_context(context3)
        assert result3.host_prefix == "host2"  # Overridden
        assert result3.dev_environment.name == "Dev"  # Preserved
        assert result3.prod_environment.name == "Prod"  # Added
