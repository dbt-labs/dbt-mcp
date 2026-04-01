from dbt_mcp.config.credentials import CredentialsProvider
from dbt_mcp.config.headers import DiscoveryHeadersProvider
from dbt_mcp.dbt_admin.client import DbtAdminAPIClient
from dbt_mcp.errors import NotFoundError

from .base import ConfigProvider, DiscoveryConfig, MultiProjectConfigProvider


class DefaultDiscoveryConfigProvider(ConfigProvider[DiscoveryConfig]):
    def __init__(self, credentials_provider: CredentialsProvider):
        self.credentials_provider = credentials_provider

    async def get_config(self) -> DiscoveryConfig:
        settings, token_provider = await self.credentials_provider.get_credentials()
        assert settings.actual_host and settings.actual_prod_environment_id
        if settings.actual_host_prefix:
            url = f"https://{settings.actual_host_prefix}.metadata.{settings.base_host}/graphql"
        else:
            url = f"https://metadata.{settings.actual_host}/graphql"

        return DiscoveryConfig(
            url=url,
            headers_provider=DiscoveryHeadersProvider(token_provider=token_provider),
            environment_id=settings.actual_prod_environment_id,
        )


class MultiProjectDiscoveryConfigProvider(MultiProjectConfigProvider[DiscoveryConfig]):
    def __init__(
        self,
        *,
        account_id: int,
        credentials_provider: CredentialsProvider,
        admin_client: DbtAdminAPIClient,
    ):
        self.account_id = account_id
        self.credentials_provider = credentials_provider
        self.admin_client = admin_client

    async def get_config(self, project_id: int) -> DiscoveryConfig:
        settings, token_provider = await self.credentials_provider.get_credentials()
        assert settings.actual_host and settings.actual_prod_environment_id
        if settings.actual_host_prefix:
            url = f"https://{settings.actual_host_prefix}.metadata.{settings.base_host}/graphql"
        else:
            url = f"https://metadata.{settings.actual_host}/graphql"
        prod_env, _ = await self.admin_client.get_environments_for_project(project_id)
        if not prod_env or not prod_env.id:
            raise NotFoundError(
                f"No production environment found for project {project_id}"
            )
        return DiscoveryConfig(
            url=url,
            headers_provider=DiscoveryHeadersProvider(token_provider=token_provider),
            environment_id=prod_env.id,
        )
