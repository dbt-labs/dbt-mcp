import os

import pytest

from dbt_mcp.discovery.client import (
    MetadataAPIClient,
    ModelFilter,
    ModelsFetcher,
    ExposuresFetcher,
)


@pytest.fixture
def api_client() -> MetadataAPIClient:
    host = os.getenv("DBT_HOST")
    token = os.getenv("DBT_TOKEN")

    if not host or not token:
        raise ValueError("DBT_HOST and DBT_TOKEN environment variables are required")
    return MetadataAPIClient(
        url=f"https://metadata.{host}/graphql",
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        },
    )


@pytest.fixture
def models_fetcher(api_client: MetadataAPIClient) -> ModelsFetcher:
    environment_id = os.getenv("DBT_PROD_ENV_ID")
    if not environment_id:
        raise ValueError("DBT_PROD_ENV_ID environment variable is required")

    return ModelsFetcher(api_client=api_client, environment_id=int(environment_id))


@pytest.fixture
def exposures_fetcher(api_client: MetadataAPIClient) -> ExposuresFetcher:
    environment_id = os.getenv("DBT_PROD_ENV_ID")
    if not environment_id:
        raise ValueError("DBT_PROD_ENV_ID environment variable is required")

    return ExposuresFetcher(api_client=api_client, environment_id=int(environment_id))


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


def test_fetch_exposures(exposures_fetcher: ExposuresFetcher):
    results = exposures_fetcher.fetch_exposures()

    # Basic validation of the response
    assert isinstance(results, list)

    # If there are exposures, validate their structure
    if len(results) > 0:
        for exposure in results:
            assert "name" in exposure
            assert "uniqueId" in exposure
            assert isinstance(exposure["name"], str)
            assert isinstance(exposure["uniqueId"], str)


def test_fetch_exposures_pagination(exposures_fetcher: ExposuresFetcher):
    # Test that pagination works correctly by fetching all exposures
    # This test ensures the pagination logic handles multiple pages properly
    results = exposures_fetcher.fetch_exposures()

    # Validate that we get results (assuming the test environment has some exposures)
    assert isinstance(results, list)

    # If we have more than the page size, ensure no duplicates
    if len(results) > 100:  # PAGE_SIZE is 100
        unique_ids = set()
        for exposure in results:
            unique_id = exposure["uniqueId"]
            assert unique_id not in unique_ids, f"Duplicate exposure found: {unique_id}"
            unique_ids.add(unique_id)


def test_fetch_exposure_details_by_unique_ids(exposures_fetcher: ExposuresFetcher):
    # First get all exposures to find one to test with
    exposures = exposures_fetcher.fetch_exposures()

    # Skip test if no exposures are available
    if not exposures:
        pytest.skip("No exposures available in the test environment")

    # Pick the first exposure to test with
    test_exposure = exposures[0]
    unique_id = test_exposure["uniqueId"]

    # Fetch the same exposure by unique_ids
    result = exposures_fetcher.fetch_exposure_details(unique_ids=[unique_id])

    # Validate that we got the correct exposure back
    assert isinstance(result, list)
    assert len(result) == 1
    exposure = result[0]
    assert exposure["uniqueId"] == unique_id
    assert exposure["name"] == test_exposure["name"]
    assert "exposureType" in exposure
    assert "maturity" in exposure

    # Validate structure
    if "parents" in exposure and exposure["parents"]:
        assert isinstance(exposure["parents"], list)
        for parent in exposure["parents"]:
            assert "uniqueId" in parent


def test_fetch_exposure_details_nonexistent(exposures_fetcher: ExposuresFetcher):
    # Test with a non-existent exposure
    result = exposures_fetcher.fetch_exposure_details(
        unique_ids=["exposure.nonexistent.exposure"]
    )

    # Should return empty list when not found
    assert result == []
