from abc import ABC, abstractmethod
from dataclasses import dataclass

from dbt_mcp.config.headers import (
    HeadersProvider,
    ProxiedToolHeadersProvider,
    TokenProvider,
)


class ConfigProvider[ConfigType](ABC):
    @abstractmethod
    async def get_config(self) -> ConfigType: ...


class MultiProjectConfigProvider[ConfigType](ABC):
    @abstractmethod
    async def get_config(self, project_id: int) -> ConfigType: ...


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


@dataclass
class DiscoveryConfig:
    url: str
    headers_provider: HeadersProvider
    environment_id: int


@dataclass
class ProxiedToolConfig:
    user_id: int | None
    dev_environment_id: int | None
    prod_environment_id: int | None
    url: str
    headers_provider: ProxiedToolHeadersProvider


@dataclass
class SemanticLayerConfig:
    url: str
    host: str
    prod_environment_id: int
    token_provider: TokenProvider
    headers_provider: HeadersProvider
    metrics_related_max: int = 10
    max_response_chars: int = 16000
