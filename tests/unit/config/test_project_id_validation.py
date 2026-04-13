"""Unit tests for project ID validation in multi-project config providers."""

from unittest.mock import AsyncMock, MagicMock

import pytest

from dbt_mcp.config.config_providers.discovery import (
    MultiProjectDiscoveryConfigProvider,
)
from dbt_mcp.config.config_providers.semantic_layer import (
    MultiProjectSemanticLayerConfigProvider,
)
from dbt_mcp.config.credentials import CredentialsProvider
from dbt_mcp.config.settings import DbtMcpSettings
from dbt_mcp.dbt_admin.client import DbtAdminAPIClient
from dbt_mcp.oauth.token_provider import StaticTokenProvider


def _make_credentials_provider(project_ids: list[int] | None) -> CredentialsProvider:
    settings = DbtMcpSettings.model_construct(
        dbt_host="cloud.getdbt.com",
        dbt_project_ids=project_ids,
        dbt_prod_env_id=99,
    )
    provider = MagicMock(spec=CredentialsProvider)
    provider.get_credentials = AsyncMock(
        return_value=(settings, StaticTokenProvider(token="tok"))
    )
    return provider


def _make_admin_client(env_id: int | None = 42) -> DbtAdminAPIClient:
    env = MagicMock()
    env.id = env_id
    client = MagicMock(spec=DbtAdminAPIClient)
    client.get_environments_for_project = AsyncMock(return_value=(env, None))
    return client


class TestMultiProjectDiscoveryConfigProvider:
    async def test_raises_when_project_id_not_in_selected(self):
        provider = MultiProjectDiscoveryConfigProvider(
            credentials_provider=_make_credentials_provider([1, 2, 3]),
            admin_client=_make_admin_client(),
        )
        with pytest.raises(
            ValueError, match="Project 99 is not in the selected projects"
        ):
            await provider.get_config(project_id=99)

    async def test_succeeds_when_project_id_in_selected(self):
        provider = MultiProjectDiscoveryConfigProvider(
            credentials_provider=_make_credentials_provider([1, 2, 3]),
            admin_client=_make_admin_client(env_id=42),
        )
        config = await provider.get_config(project_id=1)
        assert config.environment_id == 42

    async def test_skips_validation_when_project_ids_none(self):
        """When dbt_project_ids is None (no filter), any project_id is allowed."""
        provider = MultiProjectDiscoveryConfigProvider(
            credentials_provider=_make_credentials_provider(None),
            admin_client=_make_admin_client(env_id=42),
        )
        config = await provider.get_config(project_id=99)
        assert config.environment_id == 42


class TestMultiProjectSemanticLayerConfigProvider:
    async def test_raises_when_project_id_not_in_selected(self):
        provider = MultiProjectSemanticLayerConfigProvider(
            credentials_provider=_make_credentials_provider([10, 20]),
            admin_client=_make_admin_client(),
        )
        with pytest.raises(
            ValueError, match="Project 99 is not in the selected projects"
        ):
            await provider.get_config(project_id=99)

    async def test_succeeds_when_project_id_in_selected(self):
        provider = MultiProjectSemanticLayerConfigProvider(
            credentials_provider=_make_credentials_provider([10, 20]),
            admin_client=_make_admin_client(env_id=55),
        )
        config = await provider.get_config(project_id=10)
        assert config.prod_environment_id == 55

    async def test_skips_validation_when_project_ids_none(self):
        """When dbt_project_ids is None (no filter), any project_id is allowed."""
        provider = MultiProjectSemanticLayerConfigProvider(
            credentials_provider=_make_credentials_provider(None),
            admin_client=_make_admin_client(env_id=55),
        )
        config = await provider.get_config(project_id=99)
        assert config.prod_environment_id == 55
