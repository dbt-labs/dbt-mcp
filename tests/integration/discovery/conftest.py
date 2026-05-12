import os

import pytest

from dbt_mcp.config.config_providers import (
    ConfigProvider,
    DiscoveryConfig,
)
from dbt_mcp.config.config_providers.discovery import DefaultDiscoveryConfigProvider
from dbt_mcp.config.credentials import CredentialsProvider
from dbt_mcp.config.settings import DbtMcpSettings
from dbt_mcp.discovery.client import (
    DEFAULT_MAX_NODE_QUERY_LIMIT,
    DEFAULT_PAGE_SIZE,
    ExposuresFetcher,
    MacrosFetcher,
    ModelsFetcher,
    PaginatedResourceFetcher,
    SourcesFetcher,
)


@pytest.fixture
async def discovery_config(
    config_provider: ConfigProvider[DiscoveryConfig],
) -> DiscoveryConfig:
    return await config_provider.get_config()


@pytest.fixture
def config_provider() -> ConfigProvider[DiscoveryConfig]:
    # Set up environment variables needed by DbtMcpSettings
    host = os.getenv("DBT_HOST")
    token = os.getenv("DBT_TOKEN")
    prod_env_id = os.getenv("DBT_PROD_ENV_ID")

    if not host or not token or not prod_env_id:
        raise ValueError(
            "DBT_HOST, DBT_TOKEN, and DBT_PROD_ENV_ID environment variables are "
            "required"
        )

    # DbtMcpSettings will automatically pick up from environment variables
    settings = DbtMcpSettings()  # type: ignore
    credentials_provider = CredentialsProvider(settings)
    return DefaultDiscoveryConfigProvider(credentials_provider)


@pytest.fixture
def credentials_provider() -> CredentialsProvider:
    settings = DbtMcpSettings()  # type: ignore
    return CredentialsProvider(settings)


@pytest.fixture
def models_fetcher() -> ModelsFetcher:
    paginator = PaginatedResourceFetcher(
        edges_path=("data", "environment", "applied", "models", "edges"),
        page_info_path=("data", "environment", "applied", "models", "pageInfo"),
        page_size=DEFAULT_PAGE_SIZE,
        max_node_query_limit=DEFAULT_MAX_NODE_QUERY_LIMIT,
    )
    return ModelsFetcher(paginator=paginator)


@pytest.fixture
def exposures_fetcher() -> ExposuresFetcher:
    paginator = PaginatedResourceFetcher(
        edges_path=("data", "environment", "definition", "exposures", "edges"),
        page_info_path=(
            "data",
            "environment",
            "definition",
            "exposures",
            "pageInfo",
        ),
        page_size=DEFAULT_PAGE_SIZE,
        max_node_query_limit=DEFAULT_MAX_NODE_QUERY_LIMIT,
    )
    return ExposuresFetcher(paginator=paginator)


@pytest.fixture
def sources_fetcher() -> SourcesFetcher:
    paginator = PaginatedResourceFetcher(
        edges_path=("data", "environment", "applied", "sources", "edges"),
        page_info_path=("data", "environment", "applied", "sources", "pageInfo"),
        page_size=DEFAULT_PAGE_SIZE,
        max_node_query_limit=DEFAULT_MAX_NODE_QUERY_LIMIT,
    )
    return SourcesFetcher(paginator=paginator)


@pytest.fixture
def macros_fetcher() -> MacrosFetcher:
    paginator = PaginatedResourceFetcher(
        edges_path=("data", "environment", "applied", "resources", "edges"),
        page_info_path=("data", "environment", "applied", "resources", "pageInfo"),
        page_size=DEFAULT_PAGE_SIZE,
        max_node_query_limit=DEFAULT_MAX_NODE_QUERY_LIMIT,
    )
    return MacrosFetcher(paginator=paginator)
