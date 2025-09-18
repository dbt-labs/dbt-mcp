from dataclasses import dataclass

from dbt_mcp.config.headers import (
    HeadersProvider,
    SemanticLayerHeadersProvider,
)
from dbt_mcp.config.settings import CredentialsProvider


@dataclass
class SemanticLayerConfig:
    url: str
    host: str
    prod_environment_id: int
    service_token: str
    headers_provider: HeadersProvider


class SemanticLayerConfigProvider:
    def __init__(self, credentials_provider: CredentialsProvider):
        self.credentials_provider = credentials_provider

    def get_config(self) -> SemanticLayerConfig:
        settings, token_provider = self.credentials_provider.get_credentials()
        assert (
            settings.actual_host
            and settings.actual_prod_environment_id
            and settings.dbt_token
        )
        is_local = settings.actual_host and settings.actual_host.startswith("localhost")
        if is_local:
            host = settings.actual_host
        elif settings.actual_host_prefix:
            host = (
                f"{settings.actual_host_prefix}.semantic-layer.{settings.actual_host}"
            )
        else:
            host = f"semantic-layer.{settings.actual_host}"
        assert host is not None

        return SemanticLayerConfig(
            url=f"http://{host}" if is_local else f"https://{host}" + "/api/graphql",
            host=host,
            prod_environment_id=settings.actual_prod_environment_id,
            service_token=settings.dbt_token,
            headers_provider=SemanticLayerHeadersProvider(
                token_provider=token_provider
            ),
        )
