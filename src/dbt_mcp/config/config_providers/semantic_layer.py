from dbt_mcp.config.credentials import CredentialsProvider
from dbt_mcp.config.headers import (
    SemanticLayerHeadersProvider,
)
from dbt_mcp.dbt_admin.client import DbtAdminAPIClient
from dbt_mcp.errors import NotFoundError

from .base import ConfigProvider, MultiProjectConfigProvider, SemanticLayerConfig


class DefaultSemanticLayerConfigProvider(ConfigProvider[SemanticLayerConfig]):
    def __init__(
        self,
        credentials_provider: CredentialsProvider,
        *,
        metrics_related_max: int = 10,
    ):
        self.credentials_provider = credentials_provider
        self.metrics_related_max = metrics_related_max

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
            metrics_related_max=self.metrics_related_max,
        )


class MultiProjectSemanticLayerConfigProvider(
    MultiProjectConfigProvider[SemanticLayerConfig]
):
    def __init__(
        self,
        credentials_provider: CredentialsProvider,
        admin_client: DbtAdminAPIClient,
        *,
        metrics_related_max: int = 10,
    ):
        self.credentials_provider = credentials_provider
        self.admin_client = admin_client
        self.metrics_related_max = metrics_related_max

    async def get_config(self, project_id: int) -> SemanticLayerConfig:
        settings, token_provider = await self.credentials_provider.get_credentials()
        assert settings.actual_host
        if settings.dbt_project_ids and project_id not in settings.dbt_project_ids:
            raise ValueError(
                f"Project {project_id} is not in the selected projects. "
                f"Available project IDs: {settings.dbt_project_ids}"
            )
        is_local = settings.actual_host and settings.actual_host.startswith("localhost")
        if is_local:
            host = settings.actual_host
        elif settings.actual_host_prefix:
            host = f"{settings.actual_host_prefix}.semantic-layer.{settings.base_host}"
        else:
            host = f"semantic-layer.{settings.actual_host}"
        assert host is not None
        prod_env, _ = await self.admin_client.get_environments_for_project(project_id)
        if not prod_env or not prod_env.id:
            raise NotFoundError(
                f"No production environment found for project {project_id}"
            )
        return SemanticLayerConfig(
            url=f"http://{host}" if is_local else f"https://{host}" + "/api/graphql",
            host=host,
            prod_environment_id=prod_env.id,
            token_provider=token_provider,
            headers_provider=SemanticLayerHeadersProvider(
                token_provider=token_provider
            ),
            metrics_related_max=self.metrics_related_max,
        )
