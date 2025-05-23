import os

import pytest

from dbt_mcp.discovery.client import MetadataAPIClient, ModelFilter, ModelsFetcher


@pytest.fixture
def api_client() -> MetadataAPIClient:
    host = os.getenv("DBT_HOST")
    token = os.getenv("DBT_TOKEN")

    if not host or not token:
        raise ValueError("DBT_HOST and DBT_TOKEN environment variables are required")
    return MetadataAPIClient(host=host, token=token)


@pytest.fixture
def models_fetcher(api_client: MetadataAPIClient) -> ModelsFetcher:
    environment_id = os.getenv("DBT_PROD_ENV_ID")
    if not environment_id:
        raise ValueError("DBT_PROD_ENV_ID environment variable is required")

    return ModelsFetcher(api_client=api_client, environment_id=int(environment_id))


def test_fetch_models(models_fetcher: ModelsFetcher):
    results = models_fetcher.fetch_models()

    # Basic validation of the response
    assert isinstance(results, list)
    assert len(results) > 0

    # Validate structure of returned models
    for model in results:
        assert "name" in model
        assert "compiledCode" in model
        assert isinstance(model["name"], str)

        # If catalog exists, validate its structure
        if model.get("catalog"):
            assert isinstance(model["catalog"], dict)
            if "columns" in model["catalog"]:
                for column in model["catalog"]["columns"]:
                    assert "name" in column
                    assert "type" in column


def test_fetch_models_with_filter(models_fetcher: ModelsFetcher):
    # model_filter: ModelFilter = {"access": "protected"}
    model_filter: ModelFilter = {"modelingLayer": "marts"}

    # Fetch filtered results
    filtered_results = models_fetcher.fetch_models(model_filter=model_filter)

    # Validate filtered results
    assert len(filtered_results) > 0


def test_fetch_model_details(models_fetcher: ModelsFetcher):
    models = models_fetcher.fetch_models()
    model_name = models[0]["name"]

    # Fetch filtered results
    filtered_results = models_fetcher.fetch_model_details(model_name)

    # Validate filtered results
    assert len(filtered_results) > 0


def test_fetch_model_details_with_uniqueId(models_fetcher: ModelsFetcher):
    models = models_fetcher.fetch_models()
    model = models[0]
    model_name = model["name"]
    unique_id = model["uniqueId"]

    # Fetch by name
    results_by_name = models_fetcher.fetch_model_details(model_name)

    # Fetch by uniqueId
    results_by_uniqueId = models_fetcher.fetch_model_details(model_name, unique_id)

    # Validate that both methods return the same result
    assert results_by_name["uniqueId"] == results_by_uniqueId["uniqueId"]
    assert results_by_name["name"] == results_by_uniqueId["name"]


def test_fetch_model_parents(models_fetcher: ModelsFetcher):
    models = models_fetcher.fetch_models()
    model_name = models[0]["name"]

    # Fetch filtered results
    filtered_results = models_fetcher.fetch_model_parents(model_name)

    # Validate filtered results
    assert len(filtered_results) > 0


def test_fetch_model_parents_with_uniqueId(models_fetcher: ModelsFetcher):
    models = models_fetcher.fetch_models()
    model = models[0]
    model_name = model["name"]
    unique_id = model["uniqueId"]

    # Fetch by name
    results_by_name = models_fetcher.fetch_model_parents(model_name)

    # Fetch by uniqueId
    results_by_uniqueId = models_fetcher.fetch_model_parents(model_name, unique_id)

    # Validate that both methods return the same result
    assert len(results_by_name) == len(results_by_uniqueId)
    if len(results_by_name) > 0:
        # Compare the first parent's name if there are any parents
        assert results_by_name[0]["name"] == results_by_uniqueId[0]["name"]


def test_fetch_model_children(models_fetcher: ModelsFetcher):
    models = models_fetcher.fetch_models()
    model_name = models[0]["name"]

    # Fetch filtered results
    filtered_results = models_fetcher.fetch_model_children(model_name)

    # Validate filtered results
    assert isinstance(filtered_results, list)


def test_fetch_model_children_with_uniqueId(models_fetcher: ModelsFetcher):
    models = models_fetcher.fetch_models()
    model = models[0]
    model_name = model["name"]
    unique_id = model["uniqueId"]

    # Fetch by name
    results_by_name = models_fetcher.fetch_model_children(model_name)

    # Fetch by uniqueId
    results_by_uniqueId = models_fetcher.fetch_model_children(model_name, unique_id)

    # Validate that both methods return the same result
    assert len(results_by_name) == len(results_by_uniqueId)
    if len(results_by_name) > 0:
        # Compare the first child's name if there are any children
        assert results_by_name[0]["name"] == results_by_uniqueId[0]["name"]
