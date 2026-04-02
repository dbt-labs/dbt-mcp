from dbt_mcp.config.headers import AdminApiHeadersProvider
from dbt_mcp.config.credentials import CredentialsProvider

from .base import AdminApiConfig, ConfigProvider


class DefaultAdminApiConfigProvider(ConfigProvider[AdminApiConfig]):
    def __init__(self, credentials_provider: CredentialsProvider):
        self.credentials_provider = credentials_provider

    async def get_config(self) -> AdminApiConfig:
        settings, token_provider = await self.credentials_provider.get_credentials()
        assert settings.actual_host and settings.dbt_account_id
        if settings.actual_host_prefix:
            url = f"https://{settings.actual_host_prefix}.{settings.base_host}"
        else:
            url = f"https://{settings.actual_host}"

        return AdminApiConfig(
            url=url,
            headers_provider=AdminApiHeadersProvider(token_provider=token_provider),
            account_id=settings.dbt_account_id,
            prod_environment_id=settings.actual_prod_environment_id,
        )
