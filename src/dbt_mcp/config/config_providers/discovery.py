from dbt_mcp.config.headers import DiscoveryHeadersProvider
from dbt_mcp.config.settings import CredentialsProvider

from .base import ConfigProvider, DiscoveryConfig


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
