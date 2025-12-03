from unittest.mock import Mock

import pytest

from dbt_mcp.config.config_providers import AdminApiConfig


class MockHeadersProvider:
    """Mock headers provider for testing."""

    def get_headers(self) -> dict[str, str]:
        return {"Authorization": "Bearer test_token"}


@pytest.fixture
def admin_config():
    """Admin API config for testing."""
    return AdminApiConfig(
        account_id=12345,
        headers_provider=MockHeadersProvider(),
        url="https://cloud.getdbt.com",
    )


@pytest.fixture
def mock_client():
    """Base mock client - behavior configured per test."""
    return Mock()
