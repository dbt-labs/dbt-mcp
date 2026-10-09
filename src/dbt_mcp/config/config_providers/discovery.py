from __future__ import annotations

from typing import TYPE_CHECKING

from dbt_mcp.config.headers import DiscoveryHeadersProvider

if TYPE_CHECKING:
    from dbt_mcp.config.credentials import CredentialsProvider
    from dbt_mcp.dbt_admin.client import DbtAdminAPIClient

from .environments import resolve_production_environment_id
from .base import DiscoveryConfig, ProjectConfigProvider


class DefaultDiscoveryConfigProvider(ProjectConfigProvider[DiscoveryConfig]):
    def __init__(
        self,
        credentials_provider: CredentialsProvider,
        *,
        admin_client: DbtAdminAPIClient | None = None,
    ):
        self.credentials_provider = credentials_provider
        self.admin_client = admin_client

    async def get_config(self, project_id: int | None = None) -> DiscoveryConfig:
        settings, token_provider = await self.credentials_provider.get_credentials()
        assert settings.actual_host
        environment_id = await resolve_production_environment_id(
            settings, project_id=project_id, admin_client=self.admin_client
        )
        if settings.actual_host_prefix:
            url = f"https://{settings.actual_host_prefix}.metadata.{settings.base_host}/graphql"
        else:
            url = f"https://metadata.{settings.actual_host}/graphql"

        return DiscoveryConfig(
            url=url,
            headers_provider=DiscoveryHeadersProvider(token_provider=token_provider),
            environment_id=environment_id,
        )
