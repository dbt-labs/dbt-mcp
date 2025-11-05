from abc import ABC, abstractmethod
from dataclasses import dataclass

from dbt_mcp.config.headers import (
    AdminApiHeadersProvider,
    DiscoveryHeadersProvider,
    HeadersProvider,
    SemanticLayerHeadersProvider,
    SqlHeadersProvider,
)
from dbt_mcp.config.settings import CredentialsProvider


@dataclass
class SemanticLayerConfig:
    url: str
    host: str
    prod_environment_id: int
    token: str
    headers_provider: HeadersProvider


@dataclass
class DiscoveryConfig:
    url: str
    headers_provider: HeadersProvider
    environment_id: int


@dataclass
class AdminApiConfig:
    url: str
    headers_provider: HeadersProvider
    account_id: int
    prod_environment_id: int | None = None


@dataclass
class SqlConfig:
    user_id: int
    dev_environment_id: int
    prod_environment_id: int
    url: str
    headers_provider: HeadersProvider


class ConfigProvider[ConfigType](ABC):
    @abstractmethod
    async def get_config(self) -> ConfigType: ...


class DefaultSemanticLayerConfigProvider(ConfigProvider[SemanticLayerConfig]):
    def __init__(self, credentials_provider: CredentialsProvider):
        self.credentials_provider = credentials_provider

    async def get_config(self) -> SemanticLayerConfig:
        settings, token_provider = await self.credentials_provider.get_credentials()
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
            token=settings.dbt_token,
            headers_provider=SemanticLayerHeadersProvider(
                token_provider=token_provider
            ),
        )


class DefaultDiscoveryConfigProvider(ConfigProvider[DiscoveryConfig]):
    def __init__(self, credentials_provider: CredentialsProvider):
        self.credentials_provider = credentials_provider

    async def get_config(self) -> DiscoveryConfig:
        settings, token_provider = await self.credentials_provider.get_credentials()
        assert (
            settings.actual_host
            and settings.actual_prod_environment_id
            and settings.dbt_token
        )
        if settings.actual_host_prefix:
            url = f"https://{settings.actual_host_prefix}.metadata.{settings.actual_host}/graphql"
        else:
            url = f"https://metadata.{settings.actual_host}/graphql"

        return DiscoveryConfig(
            url=url,
            headers_provider=DiscoveryHeadersProvider(token_provider=token_provider),
            environment_id=settings.actual_prod_environment_id,
        )


class DefaultAdminApiConfigProvider(ConfigProvider[AdminApiConfig]):
    def __init__(self, credentials_provider: CredentialsProvider):
        self.credentials_provider = credentials_provider

    async def get_config(self) -> AdminApiConfig:
        settings, token_provider = await self.credentials_provider.get_credentials()
        assert settings.dbt_token and settings.actual_host and settings.dbt_account_id
        if settings.actual_host_prefix:
            url = f"https://{settings.actual_host_prefix}.{settings.actual_host}"
        else:
            url = f"https://{settings.actual_host}"

        return AdminApiConfig(
            url=url,
            headers_provider=AdminApiHeadersProvider(token_provider=token_provider),
            account_id=settings.dbt_account_id,
            prod_environment_id=settings.actual_prod_environment_id,
        )


class DefaultSqlConfigProvider(ConfigProvider[SqlConfig]):
    def __init__(self, credentials_provider: CredentialsProvider):
        self.credentials_provider = credentials_provider

    async def get_config(self) -> SqlConfig:
        settings, token_provider = await self.credentials_provider.get_credentials()
        assert (
            settings.dbt_user_id
            and settings.dbt_token
            and settings.dbt_dev_env_id
            and settings.actual_prod_environment_id
            and settings.actual_host
        )
        is_local = settings.actual_host and settings.actual_host.startswith("localhost")
        path = "/v1/mcp/" if is_local else "/api/ai/v1/mcp/"
        scheme = "http://" if is_local else "https://"
        host_prefix = (
            f"{settings.actual_host_prefix}." if settings.actual_host_prefix else ""
        )
        url = f"{scheme}{host_prefix}{settings.actual_host}{path}"

        return SqlConfig(
            user_id=settings.dbt_user_id,
            dev_environment_id=settings.dbt_dev_env_id,
            prod_environment_id=settings.actual_prod_environment_id,
            url=url,
            headers_provider=SqlHeadersProvider(token_provider=token_provider),
        )
