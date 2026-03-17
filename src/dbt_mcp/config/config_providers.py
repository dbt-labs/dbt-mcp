import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass

from dbt_mcp.config.headers import (
    AdminApiHeadersProvider,
    DiscoveryHeadersProvider,
    HeadersProvider,
    ProxiedToolHeadersProvider,
    SemanticLayerHeadersProvider,
    TokenProvider,
)
from dbt_mcp.config.settings import CredentialsProvider
from dbt_mcp.project.environment_resolver import get_environments_for_project

logger = logging.getLogger(__name__)


@dataclass
class SemanticLayerConfig:
    url: str
    host: str
    prod_environment_id: int
    token_provider: TokenProvider
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
class ProxiedToolConfig:
    user_id: int | None
    dev_environment_id: int | None
    prod_environment_id: int | None
    url: str
    headers_provider: ProxiedToolHeadersProvider


class ConfigProvider[ConfigType](ABC):
    @abstractmethod
    async def get_config(self) -> ConfigType: ...


class DefaultSemanticLayerConfigProvider(ConfigProvider[SemanticLayerConfig]):
    def __init__(self, credentials_provider: CredentialsProvider):
        self.credentials_provider = credentials_provider

    async def get_config(self) -> SemanticLayerConfig:
        settings, token_provider = await self.credentials_provider.get_credentials()
        assert settings.actual_host and settings.actual_prod_environment_id
        return self._build_config(
            host=settings.actual_host,
            host_prefix=settings.actual_host_prefix,
            prod_environment_id=settings.actual_prod_environment_id,
            token_provider=token_provider,
        )

    async def get_config_for_project(self, project_id: int) -> SemanticLayerConfig:
        settings, token_provider = await self.credentials_provider.get_credentials()
        assert settings.actual_host and settings.dbt_account_id
        dbt_platform_url = f"https://{settings.actual_host}"
        if settings.actual_host_prefix:
            dbt_platform_url = (
                f"https://{settings.actual_host_prefix}.{settings.actual_host}"
            )
        auth_headers = {
            "Accept": "application/json",
            "Authorization": f"Bearer {token_provider.get_token()}",
        }
        prod_env, _ = get_environments_for_project(
            dbt_platform_url=dbt_platform_url,
            account_id=settings.dbt_account_id,
            project_id=project_id,
            headers=auth_headers,
        )
        if not prod_env:
            raise ValueError(
                f"No production environment found for project {project_id}"
            )
        return self._build_config(
            host=settings.actual_host,
            host_prefix=settings.actual_host_prefix,
            prod_environment_id=prod_env.id,
            token_provider=token_provider,
        )

    @staticmethod
    def _build_config(
        *,
        host: str,
        host_prefix: str | None,
        prod_environment_id: int,
        token_provider: TokenProvider,
    ) -> SemanticLayerConfig:
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
            prod_environment_id=prod_environment_id,
            token_provider=token_provider,
            headers_provider=SemanticLayerHeadersProvider(
                token_provider=token_provider
            ),
        )


class DefaultDiscoveryConfigProvider(ConfigProvider[DiscoveryConfig]):
    def __init__(self, credentials_provider: CredentialsProvider):
        self.credentials_provider = credentials_provider

    async def get_config(self) -> DiscoveryConfig:
        settings, token_provider = await self.credentials_provider.get_credentials()
        assert settings.actual_host and settings.actual_prod_environment_id
        return self._build_config(
            host=settings.actual_host,
            host_prefix=settings.actual_host_prefix,
            environment_id=settings.actual_prod_environment_id,
            token_provider=token_provider,
        )

    async def get_config_for_project(self, project_id: int) -> DiscoveryConfig:
        settings, token_provider = await self.credentials_provider.get_credentials()
        assert settings.actual_host and settings.dbt_account_id
        dbt_platform_url = f"https://{settings.actual_host}"
        if settings.actual_host_prefix:
            dbt_platform_url = (
                f"https://{settings.actual_host_prefix}.{settings.actual_host}"
            )
        auth_headers = {
            "Accept": "application/json",
            "Authorization": f"Bearer {token_provider.get_token()}",
        }
        prod_env, _ = get_environments_for_project(
            dbt_platform_url=dbt_platform_url,
            account_id=settings.dbt_account_id,
            project_id=project_id,
            headers=auth_headers,
        )
        if not prod_env:
            raise ValueError(
                f"No production environment found for project {project_id}"
            )
        return self._build_config(
            host=settings.actual_host,
            host_prefix=settings.actual_host_prefix,
            environment_id=prod_env.id,
            token_provider=token_provider,
        )

    @staticmethod
    def _build_config(
        *,
        host: str,
        host_prefix: str | None,
        environment_id: int,
        token_provider: TokenProvider,
    ) -> DiscoveryConfig:
        if host_prefix:
            url = f"https://{host_prefix}.metadata.{host}/graphql"
        else:
            url = f"https://metadata.{host}/graphql"

        return DiscoveryConfig(
            url=url,
            headers_provider=DiscoveryHeadersProvider(token_provider=token_provider),
            environment_id=environment_id,
        )


class DefaultAdminApiConfigProvider(ConfigProvider[AdminApiConfig]):
    def __init__(self, credentials_provider: CredentialsProvider):
        self.credentials_provider = credentials_provider

    async def get_config(self) -> AdminApiConfig:
        settings, token_provider = await self.credentials_provider.get_credentials()
        assert settings.actual_host and settings.dbt_account_id
        return self._build_config(
            host=settings.actual_host,
            host_prefix=settings.actual_host_prefix,
            account_id=settings.dbt_account_id,
            prod_environment_id=settings.actual_prod_environment_id,
            token_provider=token_provider,
        )

    async def get_config_for_project(self, project_id: int) -> AdminApiConfig:
        settings, token_provider = await self.credentials_provider.get_credentials()
        assert settings.actual_host and settings.dbt_account_id
        dbt_platform_url = f"https://{settings.actual_host}"
        if settings.actual_host_prefix:
            dbt_platform_url = (
                f"https://{settings.actual_host_prefix}.{settings.actual_host}"
            )
        auth_headers = {
            "Accept": "application/json",
            "Authorization": f"Bearer {token_provider.get_token()}",
        }
        prod_env, _ = get_environments_for_project(
            dbt_platform_url=dbt_platform_url,
            account_id=settings.dbt_account_id,
            project_id=project_id,
            headers=auth_headers,
        )
        return self._build_config(
            host=settings.actual_host,
            host_prefix=settings.actual_host_prefix,
            account_id=settings.dbt_account_id,
            prod_environment_id=prod_env.id if prod_env else None,
            token_provider=token_provider,
        )

    @staticmethod
    def _build_config(
        *,
        host: str,
        host_prefix: str | None,
        account_id: int,
        prod_environment_id: int | None,
        token_provider: TokenProvider,
    ) -> AdminApiConfig:
        if host_prefix:
            url = f"https://{host_prefix}.{host}"
        else:
            url = f"https://{host}"

        return AdminApiConfig(
            url=url,
            headers_provider=AdminApiHeadersProvider(token_provider=token_provider),
            account_id=account_id,
            prod_environment_id=prod_environment_id,
        )


class DefaultProxiedToolConfigProvider(ConfigProvider[ProxiedToolConfig]):
    def __init__(self, credentials_provider: CredentialsProvider):
        self.credentials_provider = credentials_provider

    async def get_config(self) -> ProxiedToolConfig:
        settings, token_provider = await self.credentials_provider.get_credentials()
        assert settings.actual_host
        return self._build_config(
            host=settings.actual_host,
            host_prefix=settings.actual_host_prefix,
            user_id=settings.dbt_user_id,
            dev_environment_id=settings.dbt_dev_env_id,
            prod_environment_id=settings.actual_prod_environment_id,
            token_provider=token_provider,
        )

    async def get_config_for_project(self, project_id: int) -> ProxiedToolConfig:
        settings, token_provider = await self.credentials_provider.get_credentials()
        assert settings.actual_host and settings.dbt_account_id
        dbt_platform_url = f"https://{settings.actual_host}"
        if settings.actual_host_prefix:
            dbt_platform_url = (
                f"https://{settings.actual_host_prefix}.{settings.actual_host}"
            )
        auth_headers = {
            "Accept": "application/json",
            "Authorization": f"Bearer {token_provider.get_token()}",
        }
        prod_env, dev_env = get_environments_for_project(
            dbt_platform_url=dbt_platform_url,
            account_id=settings.dbt_account_id,
            project_id=project_id,
            headers=auth_headers,
        )
        return self._build_config(
            host=settings.actual_host,
            host_prefix=settings.actual_host_prefix,
            user_id=settings.dbt_user_id,
            dev_environment_id=dev_env.id if dev_env else None,
            prod_environment_id=prod_env.id if prod_env else None,
            token_provider=token_provider,
        )

    @staticmethod
    def _build_config(
        *,
        host: str,
        host_prefix: str | None,
        user_id: int | None,
        dev_environment_id: int | None,
        prod_environment_id: int | None,
        token_provider: TokenProvider,
    ) -> ProxiedToolConfig:
        is_local = host.startswith("localhost")
        path = "/v1/mcp/" if is_local else "/api/ai/v1/mcp/"
        scheme = "http://" if is_local else "https://"
        prefix = f"{host_prefix}." if host_prefix else ""
        url = f"{scheme}{prefix}{host}{path}"

        return ProxiedToolConfig(
            user_id=user_id,
            dev_environment_id=dev_environment_id,
            prod_environment_id=prod_environment_id,
            url=url,
            headers_provider=ProxiedToolHeadersProvider(token_provider=token_provider),
        )
