import os

import pytest

from dbt_mcp.config.config_providers import DefaultDiscoveryConfigProvider
from dbt_mcp.config.settings import CredentialsProvider, DbtMcpSettings
from dbt_mcp.discovery.client import (
    ExposuresFetcher,
    MetadataAPIClient,
    ModelsFetcher,
    SourcesFetcher,
)


@pytest.fixture
def config_provider() -> DefaultDiscoveryConfigProvider:
    """Provides a configured DefaultDiscoveryConfigProvider for tests."""
    # Set up environment variables needed by DbtMcpSettings
    host = os.getenv("DBT_HOST")
    token = os.getenv("DBT_TOKEN")
    prod_env_id = os.getenv("DBT_PROD_ENV_ID")

    if not host or not token or not prod_env_id:
        raise ValueError(
            "DBT_HOST, DBT_TOKEN, and DBT_PROD_ENV_ID environment variables are required"
        )

    # Create settings and credentials provider
    # DbtMcpSettings will automatically pick up from environment variables
    settings = DbtMcpSettings()  # type: ignore
    credentials_provider = CredentialsProvider(settings)
    return DefaultDiscoveryConfigProvider(credentials_provider)


@pytest.fixture
def api_client(config_provider: DefaultDiscoveryConfigProvider) -> MetadataAPIClient:
    """Provides a configured MetadataAPIClient for tests."""
    return MetadataAPIClient(config_provider)


@pytest.fixture
def models_fetcher(api_client: MetadataAPIClient) -> ModelsFetcher:
    return ModelsFetcher(api_client)


@pytest.fixture
def exposures_fetcher(api_client: MetadataAPIClient) -> ExposuresFetcher:
    return ExposuresFetcher(api_client)


@pytest.fixture
def sources_fetcher(api_client: MetadataAPIClient) -> SourcesFetcher:
    return SourcesFetcher(api_client)


@pytest.fixture
async def sample_models(models_fetcher):
    """Provides sample models for testing lineage functionality."""
    models = await models_fetcher.fetch_models()
    if not models:
        pytest.skip("No models available for testing")
    return models


@pytest.fixture
def lineage_methods(models_fetcher):
    """Provides easy access to lineage fetching methods."""
    return {
        "ancestors": models_fetcher.fetch_model_ancestors,
        "descendants": models_fetcher.fetch_model_descendants,
    }


def assert_basic_lineage_structure(result: dict, lineage_key: str):
    """Reusable assertion for basic lineage structure.

    Args:
        result: The result dictionary from fetch_model_ancestors/descendants
        lineage_key: Either "ancestors" or "descendants"
    """
    assert isinstance(result, dict), "Result should be a dictionary"
    assert "name" in result, "Result should contain 'name'"
    assert "uniqueId" in result, "Result should contain 'uniqueId'"
    assert lineage_key in result, f"Result should contain '{lineage_key}'"
    assert isinstance(result[lineage_key], list), f"'{lineage_key}' should be a list"
