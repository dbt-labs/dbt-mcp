from unittest.mock import patch

import pytest

from dbt_mcp.discovery.client import (
    DEFAULT_MAX_NODE_QUERY_LIMIT,
    DEFAULT_PAGE_SIZE,
    ExposuresFetcher,
    PaginatedResourceFetcher,
)


@pytest.fixture
def exposures_fetcher(mock_api_client):
    paginator = PaginatedResourceFetcher(
        mock_api_client,
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
    return ExposuresFetcher(api_client=mock_api_client, paginator=paginator)


async def test_fetch_exposures_single_page(exposures_fetcher, mock_api_client):
    mock_response = {
        "data": {
            "environment": {
                "definition": {
                    "exposures": {
                        "pageInfo": {"hasNextPage": False, "endCursor": None},
                        "edges": [
                            {
                                "node": {
                                    "name": "test_exposure",
                                    "uniqueId": "exposure.test.test_exposure",
                                    "exposureType": "application",
                                    "maturity": "high",
                                    "ownerEmail": "test@example.com",
                                    "ownerName": "Test Owner",
                                    "url": "https://example.com",
                                    "meta": {},
                                    "freshnessStatus": "Unknown",
                                    "description": "Test exposure",
                                    "label": None,
                                    "parents": [
                                        {"uniqueId": "model.test.parent_model"}
                                    ],
                                }
                            }
                        ],
                    }
                }
            }
        }
    }

    mock_api_client.execute_query.return_value = mock_response

    with patch("dbt_mcp.discovery.client.raise_gql_error"):
        result = await exposures_fetcher.fetch_exposures()

    assert len(result) == 1
    assert result[0]["name"] == "test_exposure"
    assert result[0]["uniqueId"] == "exposure.test.test_exposure"
    assert result[0]["exposureType"] == "application"
    assert result[0]["maturity"] == "high"
    assert result[0]["ownerEmail"] == "test@example.com"
    assert result[0]["ownerName"] == "Test Owner"
    assert result[0]["url"] == "https://example.com"
    assert result[0]["meta"] == {}
    assert result[0]["freshnessStatus"] == "Unknown"
    assert result[0]["description"] == "Test exposure"
    assert result[0]["parents"] == [{"uniqueId": "model.test.parent_model"}]

    mock_api_client.execute_query.assert_called_once()
    args, kwargs = mock_api_client.execute_query.call_args
    assert args[1]["environmentId"] == 123
    assert args[1]["first"] == 100


async def test_fetch_exposures_multiple_pages(exposures_fetcher, mock_api_client):
    page1_response = {
        "data": {
            "environment": {
                "definition": {
                    "exposures": {
                        "pageInfo": {"hasNextPage": True, "endCursor": "cursor123"},
                        "edges": [
                            {
                                "node": {
                                    "name": "exposure1",
                                    "uniqueId": "exposure.test.exposure1",
                                    "exposureType": "application",
                                    "maturity": "high",
                                    "ownerEmail": "test1@example.com",
                                    "ownerName": "Test Owner 1",
                                    "url": "https://example1.com",
                                    "meta": {},
                                    "freshnessStatus": "Unknown",
                                    "description": "Test exposure 1",
                                    "label": None,
                                    "parents": [],
                                }
                            }
                        ],
                    }
                }
            }
        }
    }

    page2_response = {
        "data": {
            "environment": {
                "definition": {
                    "exposures": {
                        "pageInfo": {"hasNextPage": False, "endCursor": "cursor456"},
                        "edges": [
                            {
                                "node": {
                                    "name": "exposure2",
                                    "uniqueId": "exposure.test.exposure2",
                                    "exposureType": "dashboard",
                                    "maturity": "medium",
                                    "ownerEmail": "test2@example.com",
                                    "ownerName": "Test Owner 2",
                                    "url": "https://example2.com",
                                    "meta": {"key": "value"},
                                    "freshnessStatus": "Fresh",
                                    "description": "Test exposure 2",
                                    "label": "Label 2",
                                    "parents": [
                                        {"uniqueId": "model.test.parent_model2"}
                                    ],
                                }
                            }
                        ],
                    }
                }
            }
        }
    }

    mock_api_client.execute_query.side_effect = [page1_response, page2_response]

    with patch("dbt_mcp.discovery.client.raise_gql_error"):
        result = await exposures_fetcher.fetch_exposures()

    assert len(result) == 2
    assert result[0]["name"] == "exposure1"
    assert result[1]["name"] == "exposure2"
    assert result[1]["meta"] == {"key": "value"}
    assert result[1]["label"] == "Label 2"

    assert mock_api_client.execute_query.call_count == 2

    # Check first call (no cursor)
    first_call = mock_api_client.execute_query.call_args_list[0]
    assert first_call[0][1]["environmentId"] == 123
    assert first_call[0][1]["first"] == 100
    assert "after" not in first_call[0][1]

    # Check second call (with cursor)
    second_call = mock_api_client.execute_query.call_args_list[1]
    assert second_call[0][1]["environmentId"] == 123
    assert second_call[0][1]["first"] == 100
    assert second_call[0][1]["after"] == "cursor123"


async def test_fetch_exposures_empty_response(exposures_fetcher, mock_api_client):
    mock_response = {
        "data": {
            "environment": {
                "definition": {
                    "exposures": {
                        "pageInfo": {"hasNextPage": False, "endCursor": None},
                        "edges": [],
                    }
                }
            }
        }
    }

    mock_api_client.execute_query.return_value = mock_response

    with patch("dbt_mcp.discovery.client.raise_gql_error"):
        result = await exposures_fetcher.fetch_exposures()

    assert len(result) == 0
    assert isinstance(result, list)


async def test_fetch_exposures_handles_malformed_edges(
    exposures_fetcher, mock_api_client
):
    mock_response = {
        "data": {
            "environment": {
                "definition": {
                    "exposures": {
                        "pageInfo": {"hasNextPage": False, "endCursor": None},
                        "edges": [
                            {
                                "node": {
                                    "name": "valid_exposure",
                                    "uniqueId": "exposure.test.valid_exposure",
                                    "exposureType": "application",
                                    "maturity": "high",
                                    "ownerEmail": "test@example.com",
                                    "ownerName": "Test Owner",
                                    "url": "https://example.com",
                                    "meta": {},
                                    "freshnessStatus": "Unknown",
                                    "description": "Valid exposure",
                                    "label": None,
                                    "parents": [],
                                }
                            },
                            {"invalid": "edge"},  # Missing "node" key
                            {"node": "not_a_dict"},  # Node is not a dict
                            {
                                "node": {
                                    "name": "another_valid_exposure",
                                    "uniqueId": "exposure.test.another_valid_exposure",
                                    "exposureType": "dashboard",
                                    "maturity": "low",
                                    "ownerEmail": "test2@example.com",
                                    "ownerName": "Test Owner 2",
                                    "url": "https://example2.com",
                                    "meta": {},
                                    "freshnessStatus": "Stale",
                                    "description": "Another valid exposure",
                                    "label": None,
                                    "parents": [],
                                }
                            },
                        ],
                    }
                }
            }
        }
    }

    mock_api_client.execute_query.return_value = mock_response

    with patch("dbt_mcp.discovery.client.raise_gql_error"):
        result = await exposures_fetcher.fetch_exposures()

    # Should only get the valid exposures (malformed edges should be filtered out)
    assert len(result) == 2
    assert result[0]["name"] == "valid_exposure"
    assert result[1]["name"] == "another_valid_exposure"


async def test_fetch_exposure_details_by_unique_ids_single(
    exposures_fetcher, mock_api_client
):
    mock_response = {
        "data": {
            "environment": {
                "definition": {
                    "exposures": {
                        "edges": [
                            {
                                "node": {
                                    "name": "customer_dashboard",
                                    "uniqueId": "exposure.analytics.customer_dashboard",
                                    "exposureType": "dashboard",
                                    "maturity": "high",
                                    "ownerEmail": "analytics@example.com",
                                    "ownerName": "Analytics Team",
                                    "url": "https://dashboard.example.com/customers",
                                    "meta": {"team": "analytics", "priority": "high"},
                                    "freshnessStatus": "Fresh",
                                    "description": "Customer analytics dashboard",
                                    "label": "Customer Dashboard",
                                    "parents": [
                                        {"uniqueId": "model.analytics.customers"},
                                        {
                                            "uniqueId": "model.analytics.customer_metrics"
                                        },
                                    ],
                                }
                            }
                        ]
                    }
                }
            }
        }
    }

    mock_api_client.execute_query.return_value = mock_response

    with patch("dbt_mcp.discovery.client.raise_gql_error"):
        result = await exposures_fetcher.fetch_exposure_details(
            unique_ids=["exposure.analytics.customer_dashboard"]
        )

    assert isinstance(result, list)
    assert len(result) == 1
    exposure = result[0]
    assert exposure["name"] == "customer_dashboard"
    assert exposure["uniqueId"] == "exposure.analytics.customer_dashboard"
    assert exposure["exposureType"] == "dashboard"
    assert exposure["maturity"] == "high"
    assert exposure["ownerEmail"] == "analytics@example.com"
    assert exposure["ownerName"] == "Analytics Team"
    assert exposure["url"] == "https://dashboard.example.com/customers"
    assert exposure["meta"] == {"team": "analytics", "priority": "high"}
    assert exposure["freshnessStatus"] == "Fresh"
    assert exposure["description"] == "Customer analytics dashboard"
    assert exposure["label"] == "Customer Dashboard"
    assert len(exposure["parents"]) == 2
    assert exposure["parents"][0]["uniqueId"] == "model.analytics.customers"
    assert exposure["parents"][1]["uniqueId"] == "model.analytics.customer_metrics"

    mock_api_client.execute_query.assert_called_once()
    args, kwargs = mock_api_client.execute_query.call_args
    assert args[1]["environmentId"] == 123
    assert args[1]["first"] == 1
    assert args[1]["filter"] == {"uniqueIds": ["exposure.analytics.customer_dashboard"]}


async def test_fetch_exposure_details_by_unique_ids_multiple(
    exposures_fetcher, mock_api_client
):
    mock_response = {
        "data": {
            "environment": {
                "definition": {
                    "exposures": {
                        "edges": [
                            {
                                "node": {
                                    "name": "customer_dashboard",
                                    "uniqueId": "exposure.analytics.customer_dashboard",
                                    "exposureType": "dashboard",
                                    "maturity": "high",
                                    "ownerEmail": "analytics@example.com",
                                    "ownerName": "Analytics Team",
                                    "url": "https://dashboard.example.com/customers",
                                    "meta": {"team": "analytics", "priority": "high"},
                                    "freshnessStatus": "Fresh",
                                    "description": "Customer analytics dashboard",
                                    "label": "Customer Dashboard",
                                    "parents": [],
                                }
                            },
                            {
                                "node": {
                                    "name": "sales_report",
                                    "uniqueId": "exposure.sales.sales_report",
                                    "exposureType": "analysis",
                                    "maturity": "medium",
                                    "ownerEmail": "sales@example.com",
                                    "ownerName": "Sales Team",
                                    "url": None,
                                    "meta": {},
                                    "freshnessStatus": "Stale",
                                    "description": "Monthly sales analysis report",
                                    "label": None,
                                    "parents": [{"uniqueId": "model.sales.sales_data"}],
                                }
                            },
                        ]
                    }
                }
            }
        }
    }

    mock_api_client.execute_query.return_value = mock_response

    with patch("dbt_mcp.discovery.client.raise_gql_error"):
        result = await exposures_fetcher.fetch_exposure_details(
            unique_ids=[
                "exposure.analytics.customer_dashboard",
                "exposure.sales.sales_report",
            ]
        )

    assert isinstance(result, list)
    assert len(result) == 2

    # Check first exposure
    exposure1 = result[0]
    assert exposure1["name"] == "customer_dashboard"
    assert exposure1["uniqueId"] == "exposure.analytics.customer_dashboard"
    assert exposure1["exposureType"] == "dashboard"

    # Check second exposure
    exposure2 = result[1]
    assert exposure2["name"] == "sales_report"
    assert exposure2["uniqueId"] == "exposure.sales.sales_report"
    assert exposure2["exposureType"] == "analysis"

    mock_api_client.execute_query.assert_called_once()
    args, kwargs = mock_api_client.execute_query.call_args
    assert args[1]["environmentId"] == 123
    assert args[1]["first"] == 2
    assert args[1]["filter"] == {
        "uniqueIds": [
            "exposure.analytics.customer_dashboard",
            "exposure.sales.sales_report",
        ]
    }


async def test_fetch_exposure_details_by_name(exposures_fetcher, mock_api_client):
    # Mock the response for fetch_exposures (which gets called when filtering by name)
    mock_exposures_response = {
        "data": {
            "environment": {
                "definition": {
                    "exposures": {
                        "pageInfo": {"hasNextPage": False, "endCursor": None},
                        "edges": [
                            {
                                "node": {
                                    "name": "sales_report",
                                    "uniqueId": "exposure.sales.sales_report",
                                    "exposureType": "analysis",
                                    "maturity": "medium",
                                    "ownerEmail": "sales@example.com",
                                    "ownerName": "Sales Team",
                                    "url": None,
                                    "meta": {},
                                    "freshnessStatus": "Stale",
                                    "description": "Monthly sales analysis report",
                                    "label": None,
                                    "parents": [{"uniqueId": "model.sales.sales_data"}],
                                }
                            },
                            {
                                "node": {
                                    "name": "other_exposure",
                                    "uniqueId": "exposure.other.other_exposure",
                                    "exposureType": "dashboard",
                                    "maturity": "high",
                                    "ownerEmail": "other@example.com",
                                    "ownerName": "Other Team",
                                    "url": None,
                                    "meta": {},
                                    "freshnessStatus": "Fresh",
                                    "description": "Other exposure",
                                    "label": None,
                                    "parents": [],
                                }
                            },
                        ],
                    }
                }
            }
        }
    }

    mock_api_client.execute_query.return_value = mock_exposures_response

    with patch("dbt_mcp.discovery.client.raise_gql_error"):
        result = await exposures_fetcher.fetch_exposure_details(
            exposure_name="sales_report"
        )

    assert isinstance(result, list)
    assert len(result) == 1
    exposure = result[0]
    assert exposure["name"] == "sales_report"
    assert exposure["uniqueId"] == "exposure.sales.sales_report"
    assert exposure["exposureType"] == "analysis"
    assert exposure["maturity"] == "medium"
    assert exposure["url"] is None
    assert exposure["meta"] == {}
    assert exposure["freshnessStatus"] == "Stale"
    assert exposure["label"] is None

    # Should have called the GET_EXPOSURES query (not GET_EXPOSURE_DETAILS)
    mock_api_client.execute_query.assert_called_once()
    args, kwargs = mock_api_client.execute_query.call_args
    assert args[1]["environmentId"] == 123
    assert args[1]["first"] == 100  # PAGE_SIZE for fetch_exposures


async def test_fetch_exposure_details_not_found(exposures_fetcher, mock_api_client):
    mock_response = {
        "data": {"environment": {"definition": {"exposures": {"edges": []}}}}
    }

    mock_api_client.execute_query.return_value = mock_response

    with patch("dbt_mcp.discovery.client.raise_gql_error"):
        result = await exposures_fetcher.fetch_exposure_details(
            unique_ids=["exposure.nonexistent.exposure"]
        )

    assert result == []


async def test_get_exposure_filters_unique_ids(exposures_fetcher):
    filters = exposures_fetcher._get_exposure_filters(
        unique_ids=["exposure.test.test_exposure"]
    )
    assert filters == {"uniqueIds": ["exposure.test.test_exposure"]}


async def test_get_exposure_filters_multiple_unique_ids(exposures_fetcher):
    filters = exposures_fetcher._get_exposure_filters(
        unique_ids=["exposure.test.test1", "exposure.test.test2"]
    )
    assert filters == {"uniqueIds": ["exposure.test.test1", "exposure.test.test2"]}


async def test_get_exposure_filters_name_raises_error(exposures_fetcher):
    from dbt_mcp.errors import InvalidParameterError

    with pytest.raises(
        InvalidParameterError, match="ExposureFilter only supports uniqueIds"
    ):
        exposures_fetcher._get_exposure_filters(exposure_name="test_exposure")


async def test_get_exposure_filters_no_params(exposures_fetcher):
    from dbt_mcp.errors import InvalidParameterError

    with pytest.raises(
        InvalidParameterError,
        match="unique_ids must be provided for exposure filtering",
    ):
        exposures_fetcher._get_exposure_filters()


async def test_fetch_exposure_details_by_name_not_found(
    exposures_fetcher, mock_api_client
):
    # Mock empty response for fetch_exposures
    mock_response = {
        "data": {
            "environment": {
                "definition": {
                    "exposures": {
                        "pageInfo": {"hasNextPage": False, "endCursor": None},
                        "edges": [],
                    }
                }
            }
        }
    }

    mock_api_client.execute_query.return_value = mock_response

    with patch("dbt_mcp.discovery.client.raise_gql_error"):
        result = await exposures_fetcher.fetch_exposure_details(
            exposure_name="nonexistent_exposure"
        )

    assert result == []
