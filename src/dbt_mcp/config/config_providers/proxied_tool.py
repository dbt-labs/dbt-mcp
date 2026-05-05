from dbt_mcp.config.headers import ProxiedToolHeadersProvider
from dbt_mcp.config.credentials import CredentialsProvider
from dbt_mcp.errors.common import MissingHostError

from .base import ConfigProvider, ProxiedToolConfig


class DefaultProxiedToolConfigProvider(ConfigProvider[ProxiedToolConfig]):
    def __init__(self, credentials_provider: CredentialsProvider):
        self.credentials_provider = credentials_provider

    async def get_config(self) -> ProxiedToolConfig:
        settings, token_provider = await self.credentials_provider.get_credentials()
        if not settings.actual_host:
            raise MissingHostError("DBT_HOST is required for proxied tools")
        is_local = settings.actual_host and settings.actual_host.startswith("localhost")
        path = "/v1/mcp/" if is_local else "/api/ai/v1/mcp/"
        scheme = "http://" if is_local else "https://"
        host_prefix = (
            f"{settings.actual_host_prefix}." if settings.actual_host_prefix else ""
        )
        url = f"{scheme}{host_prefix}{settings.base_host}{path}"

        return ProxiedToolConfig(
            user_id=settings.dbt_user_id,
            dev_environment_id=settings.dbt_dev_env_id,
            prod_environment_id=settings.actual_prod_environment_id,
            url=url,
            headers_provider=ProxiedToolHeadersProvider(token_provider=token_provider),
        )
