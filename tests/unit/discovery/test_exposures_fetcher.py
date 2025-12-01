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
