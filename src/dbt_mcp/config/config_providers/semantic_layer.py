from dbt_mcp.config.credentials import CredentialsProvider
from dbt_mcp.config.headers import (
    SemanticLayerHeadersProvider,
)
from dbt_mcp.dbt_admin.client import DbtAdminAPIClient
from dbt_mcp.errors.warehouse_auth import WarehouseAuthHintProvider

from .admin_api import DefaultAdminApiConfigProvider
from .environments import resolve_production_environment_id
from .base import ProjectConfigProvider, SemanticLayerConfig


class DefaultSemanticLayerConfigProvider(ProjectConfigProvider[SemanticLayerConfig]):
    def __init__(
        self,
        credentials_provider: CredentialsProvider,
        *,
        admin_client: DbtAdminAPIClient | None = None,
        metrics_related_max: int = 10,
        max_response_chars: int = 16000,
    ):
        self.credentials_provider = credentials_provider
        self.metrics_related_max = metrics_related_max
        self.max_response_chars = max_response_chars
        self.admin_client = admin_client or DbtAdminAPIClient(
            DefaultAdminApiConfigProvider(credentials_provider)
        )
        self.warehouse_auth_hint_provider = WarehouseAuthHintProvider(self.admin_client)

    async def get_config(self, project_id: int | None = None) -> SemanticLayerConfig:
        settings, token_provider = await self.credentials_provider.get_credentials()
        assert settings.actual_host
        environment_id = await resolve_production_environment_id(
            settings, project_id=project_id, admin_client=self.admin_client
        )
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
            prod_environment_id=environment_id,
            token_provider=token_provider,
            headers_provider=SemanticLayerHeadersProvider(
                token_provider=token_provider
            ),
            metrics_related_max=self.metrics_related_max,
            max_response_chars=self.max_response_chars,
            warehouse_auth_hint_provider=self.warehouse_auth_hint_provider,
        )
