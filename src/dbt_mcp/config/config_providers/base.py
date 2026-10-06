from abc import ABC, abstractmethod
from dataclasses import dataclass

from dbt_mcp.config.headers import (
    HeadersProvider,
    ProxiedToolHeadersProvider,
    TokenProvider,
)
from dbt_mcp.errors.warehouse_auth import WarehouseAuthHintProvider
from dbt_mcp.resource_limits import ArtifactConfig, HttpConfig


class ConfigProvider[ConfigType](ABC):
    @abstractmethod
    async def get_config(self) -> ConfigType: ...


class ProjectConfigProvider[ConfigType](ConfigProvider[ConfigType]):
    @abstractmethod
    async def get_config(self, project_id: int | None = None) -> ConfigType: ...


class StaticConfigProvider[T](ConfigProvider[T]):
    def __init__(self, config: T):
        self.config = config

    async def get_config(self) -> T:
        return self.config


@dataclass
class AdminApiConfig:
    url: str
    headers_provider: HeadersProvider
    account_id: int
    prod_environment_id: int | None = None
    http_config: HttpConfig = HttpConfig()
    artifact_config: ArtifactConfig = ArtifactConfig()


@dataclass
class DiscoveryConfig:
    url: str
    headers_provider: HeadersProvider
    environment_id: int
    http_config: HttpConfig = HttpConfig()


@dataclass
class ProxiedToolConfig:
    user_id: int | None
    dev_environment_id: int | None
    prod_environment_id: int | None
    url: str
    headers_provider: ProxiedToolHeadersProvider
    warehouse_auth_hint_provider: WarehouseAuthHintProvider | None = None


@dataclass
class SemanticLayerConfig:
    url: str
    host: str
    prod_environment_id: int
    token_provider: TokenProvider
    headers_provider: HeadersProvider
    metrics_related_max: int = 10
    max_response_chars: int = 16000
    warehouse_auth_hint_provider: WarehouseAuthHintProvider | None = None
    http_config: HttpConfig = HttpConfig()
