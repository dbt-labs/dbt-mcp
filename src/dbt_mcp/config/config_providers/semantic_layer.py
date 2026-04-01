from dbt_mcp.config.headers import (
    AdminApiHeadersProvider,
    SemanticLayerHeadersProvider,
    TokenProvider,
)
from dbt_mcp.config.credentials import CredentialsProvider
from dbt_mcp.config.settings import DbtMcpSettings
from dbt_mcp.dbt_admin.client import DbtAdminAPIClient
from dbt_mcp.oauth.dbt_platform import DbtPlatformEnvironment

from .admin_api import AdminApiConfig
from .base import ConfigProvider, SemanticLayerConfig, StaticConfigProvider


async def _resolve_project_environments(
    credentials_provider: CredentialsProvider,
    project_id: int,
) -> tuple[
    DbtMcpSettings,
    TokenProvider,
    DbtPlatformEnvironment,
    DbtPlatformEnvironment | None,
]:
    settings, token_provider = await credentials_provider.get_credentials()
    assert settings.actual_host and settings.dbt_account_id
    dbt_platform_url = (
        f"https://{settings.actual_host_prefix}.{settings.actual_host}"
        if settings.actual_host_prefix
        else f"https://{settings.actual_host}"
    )
    prod_env, dev_env = await (
        DbtAdminAPIClient(
            StaticConfigProvider(
                config=AdminApiConfig(
                    url=dbt_platform_url,
                    account_id=settings.dbt_account_id,
                    headers_provider=AdminApiHeadersProvider(
                        token_provider=token_provider
                    ),
                    prod_environment_id=settings.actual_prod_environment_id,
                )
            )
        )
    ).get_environments_for_project(project_id)
    if not prod_env:
        raise ValueError(f"No production environment found for project {project_id}")
    return settings, token_provider, prod_env, dev_env


class DefaultSemanticLayerConfigProvider(ConfigProvider[SemanticLayerConfig]):
    def __init__(self, credentials_provider: CredentialsProvider):
        self.credentials_provider = credentials_provider

    async def get_config_for_project(self, project_id: int) -> SemanticLayerConfig:
        settings, token_provider, prod_env, _ = await _resolve_project_environments(
            self.credentials_provider, project_id
        )
        assert settings.actual_host
        host = settings.actual_host
        host_prefix = settings.actual_host_prefix
        is_local = host.startswith("localhost")
        if is_local:
            sl_host = host
        elif host_prefix:
            sl_host = f"{host_prefix}.semantic-layer.{host}"
        else:
            sl_host = f"semantic-layer.{host}"
        return SemanticLayerConfig(
            url=f"http://{sl_host}" if is_local else f"https://{sl_host}/api/graphql",
            host=sl_host,
            prod_environment_id=prod_env.id,
            token_provider=token_provider,
            headers_provider=SemanticLayerHeadersProvider(
                token_provider=token_provider
            ),
        )

    async def get_config(self) -> SemanticLayerConfig:
        settings, token_provider = await self.credentials_provider.get_credentials()
        assert settings.actual_host and settings.actual_prod_environment_id
        is_local = settings.actual_host and settings.actual_host.startswith("localhost")
        if is_local:
            host = settings.actual_host
        elif settings.actual_host_prefix:
            host = f"{settings.actual_host_prefix}.semantic-layer.{settings.base_host}"
        else:
            host = f"semantic-layer.{settings.actual_host}"
        assert host is not None

        return SemanticLayerConfig(
            url=f"http://{host}" if is_local else f"https://{host}" + "/api/graphql",
            host=host,
            prod_environment_id=settings.actual_prod_environment_id,
            token_provider=token_provider,
            headers_provider=SemanticLayerHeadersProvider(
                token_provider=token_provider
            ),
        )
