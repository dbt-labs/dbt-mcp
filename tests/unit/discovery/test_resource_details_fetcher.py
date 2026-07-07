from unittest.mock import AsyncMock, Mock, call, patch

import pytest

from dbt_mcp.config.config_providers import DiscoveryConfig
from dbt_mcp.discovery.client import (
    AppliedResourceType,
    ResourceDetailsFetcher,
)
from dbt_mcp.errors import InvalidParameterError


@pytest.fixture
def models_fetcher():
    """Mock ModelsFetcher for model-by-name resolution."""
    return AsyncMock()


@pytest.fixture
def resource_details_fetcher(models_fetcher):
    return ResourceDetailsFetcher(models_fetcher=models_fetcher)


async def test_fetch_details_requires_identifier(
    resource_details_fetcher: ResourceDetailsFetcher,
    unit_discovery_config: DiscoveryConfig,
):
    with pytest.raises(
        InvalidParameterError, match="Either name or unique_id must be provided"
    ):
        await resource_details_fetcher.fetch_details(
            AppliedResourceType.MODEL, unit_discovery_config
        )


async def test_fetch_details_validates_name_unique_id_match(
    resource_details_fetcher: ResourceDetailsFetcher,
    unit_discovery_config: DiscoveryConfig,
):
    with pytest.raises(InvalidParameterError, match="Name and unique_id do not match"):
        await resource_details_fetcher.fetch_details(
            AppliedResourceType.MODEL,
            unit_discovery_config,
            name="orders",
            unique_id="model.pkg.customers",
        )


@patch("dbt_mcp.discovery.client.raise_gql_error")
async def test_fetch_details_with_unique_id(
    mock_raise_gql_error: Mock,
    resource_details_fetcher: ResourceDetailsFetcher,
    mock_api_client: Mock,
    unit_discovery_config: DiscoveryConfig,
):
    details_response = {
        "data": {
            "environment": {
                "applied": {
                    "resources": {
                        "edges": [
                            {
                                "node": {
                                    "name": "orders",
                                    "uniqueId": "model.jaffle.orders",
                                    "description": "Orders model",
                                }
                            }
                        ]
                    }
                }
            }
        }
    }
    mock_api_client.return_value = details_response

    result = await resource_details_fetcher.fetch_details(
        AppliedResourceType.MODEL,
        unit_discovery_config,
        unique_id=" model.jaffle.orders ",
    )

    assert result == [
        {
            "name": "orders",
            "uniqueId": "model.jaffle.orders",
            "description": "Orders model",
        }
    ]

    mock_api_client.assert_called_once()
    query, variables = mock_api_client.call_args[0]
    assert query == ResourceDetailsFetcher.GQL_QUERIES[AppliedResourceType.MODEL]
    assert variables["filter"]["uniqueIds"] == ["model.jaffle.orders"]
    assert variables["filter"]["types"] == ["Model"]
    assert variables["first"] == 1
    mock_raise_gql_error.assert_called_once_with(details_response)


@patch("dbt_mcp.discovery.client.raise_gql_error")
async def test_fetch_details_with_name_builds_unique_ids(
    mock_raise_gql_error: Mock,
    resource_details_fetcher: ResourceDetailsFetcher,
    mock_api_client: Mock,
    unit_discovery_config: DiscoveryConfig,
):
    macro_packages_response = {
        "data": {"environment": {"applied": {"packages": ["core_macros"]}}}
    }
    model_packages_response = {
        "data": {"environment": {"applied": {"packages": ["analytics_models"]}}}
    }
    details_node = {
        "name": "my_macro",
        "uniqueId": "macro.core_macros.my_macro",
        "packageName": "core_macros",
    }
    details_response = {
        "data": {
            "environment": {
                "applied": {"resources": {"edges": [{"node": details_node}]}}
            }
        }
    }

    async def execute_side_effect(query, variables, *, config):
        if query == ResourceDetailsFetcher.GET_PACKAGES_QUERY:
            if variables["resource"] == "macro":
                return macro_packages_response
            if variables["resource"] == "model":
                return model_packages_response
        elif query == ResourceDetailsFetcher.GQL_QUERIES[AppliedResourceType.MACRO]:
            # Both original case and lowercase are tried to handle resources with uppercase names
            expected_unique_ids = [
                "macro.core_macros.My_Macro",
                "macro.core_macros.my_macro",
                "macro.analytics_models.My_Macro",
                "macro.analytics_models.my_macro",
            ]
            assert variables["filter"]["uniqueIds"] == expected_unique_ids
            assert variables["filter"]["types"] == ["Macro"]
            assert variables["first"] == len(expected_unique_ids)
            return details_response
        raise AssertionError(f"Unexpected query: {query}")

    mock_api_client.side_effect = execute_side_effect

    result = await resource_details_fetcher.fetch_details(
        AppliedResourceType.MACRO,
        unit_discovery_config,
        name=" My_Macro ",
    )

    assert result == [details_node]
    assert mock_api_client.call_count == 3
    macro_call = mock_api_client.call_args_list[0]
    model_call = mock_api_client.call_args_list[1]
    assert macro_call.kwargs["variables"]["resource"] == "macro"
    assert model_call.kwargs["variables"]["resource"] == "model"
    mock_raise_gql_error.assert_has_calls(
        [
            call(macro_packages_response),
            call(model_packages_response),
            call(details_response),
        ]
    )


@patch("dbt_mcp.discovery.client.raise_gql_error")
async def test_fetch_details_preserves_unique_id_casing(
    mock_raise_gql_error: Mock,
    resource_details_fetcher: ResourceDetailsFetcher,
    mock_api_client: Mock,
    unit_discovery_config: DiscoveryConfig,
):
    """unique_id casing must be preserved — the Discovery API uniqueIds filter is case-sensitive."""
    details_response = {
        "data": {
            "environment": {
                "applied": {
                    "resources": {
                        "edges": [
                            {
                                "node": {
                                    "name": "MY_MODEL",
                                    "uniqueId": "model.jaffle.MY_MODEL",
                                }
                            }
                        ]
                    }
                }
            }
        }
    }
    mock_api_client.return_value = details_response

    await resource_details_fetcher.fetch_details(
        AppliedResourceType.MODEL,
        unit_discovery_config,
        unique_id="model.jaffle.MY_MODEL",
    )

    _, variables = mock_api_client.call_args[0]
    assert variables["filter"]["uniqueIds"] == ["model.jaffle.MY_MODEL"]


@patch("dbt_mcp.discovery.client.raise_gql_error")
async def test_fetch_details_with_lowercase_name_no_duplicate_candidates(
    mock_raise_gql_error: Mock,
    resource_details_fetcher: ResourceDetailsFetcher,
    mock_api_client: Mock,
    unit_discovery_config: DiscoveryConfig,
):
    """All-lowercase names should produce a single candidate per package, not
    duplicates. Only exercised for non-model types now, since MODEL resolves
    via ModelsFetcher.resolve_unique_ids_by_name instead of the packages
    Cartesian product."""
    packages_response = {"data": {"environment": {"applied": {"packages": ["jaffle"]}}}}
    details_response: dict[str, dict[str, dict[str, dict[str, dict[str, list]]]]] = {
        "data": {"environment": {"applied": {"resources": {"edges": []}}}}
    }

    async def execute_side_effect(query, variables, *, config):
        if query == ResourceDetailsFetcher.GET_PACKAGES_QUERY:
            if variables["resource"] == "macro":
                return packages_response
            return {"data": {"environment": {"applied": {"packages": []}}}}
        elif query == ResourceDetailsFetcher.GQL_QUERIES[AppliedResourceType.MACRO]:
            assert variables["filter"]["uniqueIds"] == ["macro.jaffle.orders"]
            assert variables["first"] == 1
            return details_response
        raise AssertionError(f"Unexpected query: {query}")

    mock_api_client.side_effect = execute_side_effect

    await resource_details_fetcher.fetch_details(
        AppliedResourceType.MACRO,
        unit_discovery_config,
        name="orders",
    )


@patch("dbt_mcp.discovery.client.raise_gql_error")
async def test_fetch_details_with_name_resolves_model_via_models_fetcher(
    mock_raise_gql_error: Mock,
    resource_details_fetcher: ResourceDetailsFetcher,
    models_fetcher: AsyncMock,
    mock_api_client: Mock,
    unit_discovery_config: DiscoveryConfig,
):
    """Models resolve name->unique_id via ModelsFetcher.resolve_unique_ids_by_name,
    not the packages-enumeration + Cartesian product used for other resource
    types. No GetPackages query should be issued."""
    models_fetcher.resolve_unique_ids_by_name.return_value = ["model.jaffle.orders"]
    details_node = {"name": "orders", "uniqueId": "model.jaffle.orders"}
    details_response = {
        "data": {
            "environment": {
                "applied": {"resources": {"edges": [{"node": details_node}]}}
            }
        }
    }
    mock_api_client.return_value = details_response

    result = await resource_details_fetcher.fetch_details(
        AppliedResourceType.MODEL,
        unit_discovery_config,
        name="orders",
    )

    assert result == [details_node]
    models_fetcher.resolve_unique_ids_by_name.assert_called_once_with(
        "orders", config=unit_discovery_config
    )
    mock_api_client.assert_called_once()
    _, variables = mock_api_client.call_args[0]
    assert variables["filter"]["uniqueIds"] == ["model.jaffle.orders"]
    assert variables["filter"]["types"] == ["Model"]
    assert variables["first"] == 1
    mock_raise_gql_error.assert_called_once_with(details_response)


@patch("dbt_mcp.discovery.client.raise_gql_error")
async def test_fetch_details_with_name_resolves_multiple_models(
    mock_raise_gql_error: Mock,
    resource_details_fetcher: ResourceDetailsFetcher,
    models_fetcher: AsyncMock,
    mock_api_client: Mock,
    unit_discovery_config: DiscoveryConfig,
):
    """All unique_ids resolved by ModelsFetcher flow through into the details
    filter -- fetch_details doesn't itself disambiguate multi-match, that's the
    caller's responsibility (see ModelPerformanceFetcher)."""
    models_fetcher.resolve_unique_ids_by_name.return_value = [
        "model.jaffle.orders",
        "model.other_pkg.orders",
    ]
    details_response = {
        "data": {
            "environment": {
                "applied": {
                    "resources": {
                        "edges": [
                            {
                                "node": {
                                    "name": "orders",
                                    "uniqueId": "model.jaffle.orders",
                                }
                            },
                            {
                                "node": {
                                    "name": "orders",
                                    "uniqueId": "model.other_pkg.orders",
                                }
                            },
                        ]
                    }
                }
            }
        }
    }
    mock_api_client.return_value = details_response

    result = await resource_details_fetcher.fetch_details(
        AppliedResourceType.MODEL,
        unit_discovery_config,
        name="orders",
    )

    assert len(result) == 2
    _, variables = mock_api_client.call_args[0]
    assert variables["filter"]["uniqueIds"] == [
        "model.jaffle.orders",
        "model.other_pkg.orders",
    ]
    assert variables["first"] == 2


async def test_fetch_details_with_name_returns_empty_when_model_not_resolved(
    resource_details_fetcher: ResourceDetailsFetcher,
    models_fetcher: AsyncMock,
    mock_api_client: Mock,
    unit_discovery_config: DiscoveryConfig,
):
    """When ModelsFetcher resolves no unique_ids, fetch_details returns []
    without issuing a details query."""
    models_fetcher.resolve_unique_ids_by_name.return_value = []

    result = await resource_details_fetcher.fetch_details(
        AppliedResourceType.MODEL,
        unit_discovery_config,
        name="nonexistent",
    )

    assert result == []
    mock_api_client.assert_not_called()


@patch("dbt_mcp.discovery.client.raise_gql_error")
async def test_fetch_details_returns_empty_when_no_edges(
    mock_raise_gql_error: Mock,
    resource_details_fetcher: ResourceDetailsFetcher,
    mock_api_client: Mock,
    unit_discovery_config: DiscoveryConfig,
):
    empty_response: dict[str, dict[str, dict[str, dict[str, dict[str, list]]]]] = {
        "data": {
            "environment": {"applied": {"resources": {"edges": []}}},
        }
    }
    mock_api_client.return_value = empty_response

    result = await resource_details_fetcher.fetch_details(
        AppliedResourceType.SOURCE,
        unit_discovery_config,
        unique_id="source.jaffle.raw_customers",
    )

    assert result == []
    mock_api_client.assert_called_once()
    mock_raise_gql_error.assert_called_once_with(empty_response)


@patch("dbt_mcp.discovery.client.raise_gql_error")
async def test_fetch_details_name_raises_when_no_packages(
    mock_raise_gql_error: Mock,
    resource_details_fetcher: ResourceDetailsFetcher,
    mock_api_client: Mock,
    unit_discovery_config: DiscoveryConfig,
):
    no_packages_response: dict[str, dict[str, dict[str, dict[str, list]]]] = {
        "data": {"environment": {"applied": {"packages": []}}},
    }

    async def execute_side_effect(query, variables, *, config):
        if query == ResourceDetailsFetcher.GET_PACKAGES_QUERY:
            return no_packages_response
        raise AssertionError("Details query should not be executed when no packages")

    mock_api_client.side_effect = execute_side_effect

    with pytest.raises(InvalidParameterError, match="No packages found for project"):
        await resource_details_fetcher.fetch_details(
            AppliedResourceType.MACRO,
            unit_discovery_config,
            name="orders",
        )

    assert mock_api_client.call_count == 2
    mock_raise_gql_error.assert_has_calls(
        [call(no_packages_response), call(no_packages_response)]
    )
