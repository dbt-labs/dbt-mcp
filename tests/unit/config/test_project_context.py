import time
from unittest.mock import AsyncMock, Mock

import pytest

from dbt_mcp.config.project_context import (
    ProjectContext,
    ProjectContextResolver,
    _parse_environments,
)


@pytest.fixture
def mock_admin_client():
    client = Mock()
    client.list_environments = AsyncMock(
        return_value=[
            {"id": 100, "name": "Production", "type": "deployment", "deployment_type": "production"},
            {"id": 200, "name": "Development", "type": "development", "deployment_type": None},
            {"id": 300, "name": "Staging", "type": "deployment", "deployment_type": "staging"},
        ]
    )
    return client


def test_parse_environments_finds_prod_and_dev():
    environments = [
        {"id": 100, "name": "Production", "type": "deployment", "deployment_type": "production"},
        {"id": 200, "name": "Development", "type": "development", "deployment_type": None},
    ]
    result = _parse_environments(42, environments)
    assert result == ProjectContext(project_id=42, prod_environment_id=100, dev_environment_id=200)


def test_parse_environments_no_dev():
    environments = [
        {"id": 100, "name": "Production", "type": "deployment", "deployment_type": "production"},
    ]
    result = _parse_environments(42, environments)
    assert result == ProjectContext(project_id=42, prod_environment_id=100, dev_environment_id=None)


def test_parse_environments_no_prod():
    environments = [
        {"id": 200, "name": "Development", "type": "development", "deployment_type": None},
    ]
    result = _parse_environments(42, environments)
    assert result == ProjectContext(project_id=42, prod_environment_id=None, dev_environment_id=200)


def test_parse_environments_empty():
    result = _parse_environments(42, [])
    assert result == ProjectContext(project_id=42, prod_environment_id=None, dev_environment_id=None)


def test_parse_environments_picks_first_prod():
    environments = [
        {"id": 100, "name": "Prod 1", "type": "deployment", "deployment_type": "production"},
        {"id": 101, "name": "Prod 2", "type": "deployment", "deployment_type": "production"},
    ]
    result = _parse_environments(42, environments)
    assert result.prod_environment_id == 100


async def test_resolver_calls_api(mock_admin_client):
    resolver = ProjectContextResolver(admin_client=mock_admin_client)
    result = await resolver.resolve(account_id=1, project_id=42)

    assert result.project_id == 42
    assert result.prod_environment_id == 100
    assert result.dev_environment_id == 200
    mock_admin_client.list_environments.assert_called_once_with(1, 42)


async def test_resolver_caches_result(mock_admin_client):
    resolver = ProjectContextResolver(admin_client=mock_admin_client, ttl_seconds=60)

    result1 = await resolver.resolve(account_id=1, project_id=42)
    result2 = await resolver.resolve(account_id=1, project_id=42)

    assert result1 == result2
    # Should only call API once due to caching
    mock_admin_client.list_environments.assert_called_once()


async def test_resolver_cache_expires(mock_admin_client, monkeypatch):
    resolver = ProjectContextResolver(admin_client=mock_admin_client, ttl_seconds=0.01)

    await resolver.resolve(account_id=1, project_id=42)

    # Wait for cache to expire
    import asyncio
    await asyncio.sleep(0.02)

    await resolver.resolve(account_id=1, project_id=42)

    # Should call API twice since cache expired
    assert mock_admin_client.list_environments.call_count == 2


async def test_resolver_different_keys_not_cached(mock_admin_client):
    resolver = ProjectContextResolver(admin_client=mock_admin_client, ttl_seconds=60)

    await resolver.resolve(account_id=1, project_id=42)
    await resolver.resolve(account_id=1, project_id=43)

    assert mock_admin_client.list_environments.call_count == 2
