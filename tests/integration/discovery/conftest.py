import os

import pytest

from dbt_mcp.config.config_providers import (
    ConfigProvider,
    DefaultDiscoveryConfigProvider,
    DiscoveryConfig,
)
from dbt_mcp.config.settings import CredentialsProvider, DbtMcpSettings
from dbt_mcp.discovery.client import (
    DEFAULT_MAX_NODE_QUERY_LIMIT,
    DEFAULT_PAGE_SIZE,
    ExposuresFetcher,
    MetadataAPIClient,
    ModelsFetcher,
    PaginatedResourceFetcher,
    SourcesFetcher,
)


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
def api_client(config_provider: ConfigProvider[DiscoveryConfig]) -> MetadataAPIClient:
    return MetadataAPIClient(config_provider)


@pytest.fixture
def models_fetcher(api_client: MetadataAPIClient) -> ModelsFetcher:
    paginator = PaginatedResourceFetcher(
        api_client,
        edges_path=("data", "environment", "applied", "models", "edges"),
        page_info_path=("data", "environment", "applied", "models", "pageInfo"),
        page_size=DEFAULT_PAGE_SIZE,
        max_node_query_limit=DEFAULT_MAX_NODE_QUERY_LIMIT,
    )
    return ModelsFetcher(api_client, paginator=paginator)


@pytest.fixture
def exposures_fetcher(api_client: MetadataAPIClient) -> ExposuresFetcher:
    paginator = PaginatedResourceFetcher(
        api_client,
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
    return ExposuresFetcher(api_client, paginator=paginator)


@pytest.fixture
def sources_fetcher(api_client: MetadataAPIClient) -> SourcesFetcher:
    paginator = PaginatedResourceFetcher(
        api_client,
        edges_path=("data", "environment", "applied", "sources", "edges"),
        page_info_path=("data", "environment", "applied", "sources", "pageInfo"),
        page_size=DEFAULT_PAGE_SIZE,
        max_node_query_limit=DEFAULT_MAX_NODE_QUERY_LIMIT,
    )
    return SourcesFetcher(api_client, paginator=paginator)
