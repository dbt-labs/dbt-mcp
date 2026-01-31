import pytest

from dbt_mcp.discovery.client import (
    DEFAULT_MAX_NODE_QUERY_LIMIT,
    DEFAULT_PAGE_SIZE,
    MacrosFetcher,
    PaginatedResourceFetcher,
)


@pytest.fixture
def macros_fetcher(mock_api_client):
    paginator = PaginatedResourceFetcher(
        mock_api_client,
        edges_path=("data", "environment", "applied", "resources", "edges"),
        page_info_path=("data", "environment", "applied", "resources", "pageInfo"),
        page_size=DEFAULT_PAGE_SIZE,
        max_node_query_limit=DEFAULT_MAX_NODE_QUERY_LIMIT,
    )
    return MacrosFetcher(api_client=mock_api_client, paginator=paginator)


async def test_fetch_macros_single_page(macros_fetcher, mock_api_client):
    mock_response = {
        "data": {
            "environment": {
                "applied": {
                    "resources": {
                        "pageInfo": {"hasNextPage": False, "endCursor": "cursor_end"},
                        "edges": [
                            {
                                "node": {
                                    "name": "my_macro",
                                    "uniqueId": "macro.my_project.my_macro",
                                    "description": "A custom macro",
                                    "packageName": "my_project",
                                }
                            },
                            {
                                "node": {
                                    "name": "another_macro",
                                    "uniqueId": "macro.my_project.another_macro",
                                    "description": "Another custom macro",
                                    "packageName": "my_project",
                                }
                            },
                        ],
                    }
                }
            }
        }
    }

    mock_api_client.execute_query.return_value = mock_response

    result = await macros_fetcher.fetch_macros()

    mock_api_client.execute_query.assert_called_once()
    call_args = mock_api_client.execute_query.call_args

    query = call_args[0][0]
    assert "GetMacros" in query
    assert "environment" in query
    assert "applied" in query
    assert "resources" in query

    variables = call_args[0][1]
    assert variables["environmentId"] == 123
    assert variables["first"] == 100
    assert variables["filter"] == {"types": ["Macro"]}

    assert len(result) == 2
    assert result[0]["name"] == "my_macro"
    assert result[0]["packageName"] == "my_project"
    assert result[1]["name"] == "another_macro"


async def test_fetch_macros_excludes_dbt_builtin_by_default(
    macros_fetcher, mock_api_client
):
    """Test that dbt-labs first-party macros are excluded by default."""
    mock_response = {
        "data": {
            "environment": {
                "applied": {
                    "resources": {
                        "pageInfo": {"hasNextPage": False, "endCursor": "cursor_end"},
                        "edges": [
                            {
                                "node": {
                                    "name": "my_macro",
                                    "uniqueId": "macro.my_project.my_macro",
                                    "description": "A custom macro",
                                    "packageName": "my_project",
                                }
                            },
                            {
                                "node": {
                                    "name": "run_query",
                                    "uniqueId": "macro.dbt.run_query",
                                    "description": "dbt core macro",
                                    "packageName": "dbt",
                                }
                            },
                            {
                                "node": {
                                    "name": "snapshot_merge_sql",
                                    "uniqueId": "macro.dbt_postgres.snapshot_merge_sql",
                                    "description": "dbt_postgres adapter macro",
                                    "packageName": "dbt_postgres",
                                }
                            },
                            {
                                "node": {
                                    "name": "generate_schema_name",
                                    "uniqueId": "macro.dbt_utils.generate_schema_name",
                                    "description": "dbt_utils macro (community package)",
                                    "packageName": "dbt_utils",
                                }
                            },
                        ],
                    }
                }
            }
        }
    }

    mock_api_client.execute_query.return_value = mock_response

    result = await macros_fetcher.fetch_macros()

    # Custom macro and dbt_utils should be returned (dbt_utils is NOT filtered)
    # Only dbt core and dbt_postgres (first-party) should be filtered
    assert len(result) == 2
    assert result[0]["name"] == "my_macro"
    assert result[0]["packageName"] == "my_project"
    assert result[1]["name"] == "generate_schema_name"
    assert result[1]["packageName"] == "dbt_utils"


async def test_fetch_macros_return_package_names_only(macros_fetcher, mock_api_client):
    """Test that return_package_names_only returns only unique package names."""
    mock_response = {
        "data": {
            "environment": {
                "applied": {
                    "resources": {
                        "pageInfo": {"hasNextPage": False, "endCursor": "cursor_end"},
                        "edges": [
                            {
                                "node": {
                                    "name": "my_macro",
                                    "uniqueId": "macro.my_project.my_macro",
                                    "description": "A custom macro",
                                    "packageName": "my_project",
                                }
                            },
                            {
                                "node": {
                                    "name": "another_macro",
                                    "uniqueId": "macro.my_project.another_macro",
                                    "description": "Another custom macro",
                                    "packageName": "my_project",
                                }
                            },
                            {
                                "node": {
                                    "name": "utils_macro",
                                    "uniqueId": "macro.dbt_utils.utils_macro",
                                    "description": "A dbt_utils macro",
                                    "packageName": "dbt_utils",
                                }
                            },
                            {
                                "node": {
                                    "name": "dbt_macro",
                                    "uniqueId": "macro.dbt.dbt_macro",
                                    "description": "A dbt core macro (should be filtered)",
                                    "packageName": "dbt",
                                }
                            },
                        ],
                    }
                }
            }
        }
    }

    mock_api_client.execute_query.return_value = mock_response

    result = await macros_fetcher.fetch_macros(return_package_names_only=True)

    # Should return sorted unique package names (excluding dbt core)
    assert result == ["dbt_utils", "my_project"]


async def test_fetch_macros_filters_by_package_names(macros_fetcher, mock_api_client):
    """Test that macros can be filtered by package names."""
    mock_response = {
        "data": {
            "environment": {
                "applied": {
                    "resources": {
                        "pageInfo": {"hasNextPage": False, "endCursor": "cursor_end"},
                        "edges": [
                            {
                                "node": {
                                    "name": "my_macro",
                                    "uniqueId": "macro.my_project.my_macro",
                                    "description": "A custom macro",
                                    "packageName": "my_project",
                                }
                            },
                            {
                                "node": {
                                    "name": "other_macro",
                                    "uniqueId": "macro.other_package.other_macro",
                                    "description": "Macro from other package",
                                    "packageName": "other_package",
                                }
                            },
                        ],
                    }
                }
            }
        }
    }

    mock_api_client.execute_query.return_value = mock_response

    result = await macros_fetcher.fetch_macros(package_names=["my_project"])

    # Only the my_project macro should be returned
    assert len(result) == 1
    assert result[0]["name"] == "my_macro"
    assert result[0]["packageName"] == "my_project"


async def test_fetch_macros_package_filter_case_insensitive(
    macros_fetcher, mock_api_client
):
    """Test that package name filtering is case insensitive."""
    mock_response = {
        "data": {
            "environment": {
                "applied": {
                    "resources": {
                        "pageInfo": {"hasNextPage": False, "endCursor": "cursor_end"},
                        "edges": [
                            {
                                "node": {
                                    "name": "my_macro",
                                    "uniqueId": "macro.My_Project.my_macro",
                                    "description": "A custom macro",
                                    "packageName": "My_Project",
                                }
                            },
                        ],
                    }
                }
            }
        }
    }

    mock_api_client.execute_query.return_value = mock_response

    result = await macros_fetcher.fetch_macros(package_names=["my_project"])

    assert len(result) == 1
    assert result[0]["packageName"] == "My_Project"


async def test_fetch_macros_empty_response(macros_fetcher, mock_api_client):
    mock_response = {
        "data": {
            "environment": {
                "applied": {
                    "resources": {
                        "pageInfo": {"hasNextPage": False, "endCursor": None},
                        "edges": [],
                    }
                }
            }
        }
    }

    mock_api_client.execute_query.return_value = mock_response

    result = await macros_fetcher.fetch_macros()

    assert result == []


def test_is_dbt_builtin_package():
    """Test the _is_dbt_builtin_package helper method."""
    from dbt_mcp.discovery.client import MacrosFetcher

    # Create a minimal fetcher just to test the helper
    fetcher = MacrosFetcher.__new__(MacrosFetcher)

    # dbt-labs first-party packages (should be filtered)
    assert fetcher._is_dbt_builtin_package("dbt") is True
    assert fetcher._is_dbt_builtin_package("dbt_postgres") is True
    assert fetcher._is_dbt_builtin_package("dbt_redshift") is True
    assert fetcher._is_dbt_builtin_package("dbt_snowflake") is True
    assert fetcher._is_dbt_builtin_package("dbt_bigquery") is True
    assert fetcher._is_dbt_builtin_package("dbt_spark") is True
    assert fetcher._is_dbt_builtin_package("dbt_athena") is True
    # Case insensitive
    assert fetcher._is_dbt_builtin_package("DBT") is True
    assert fetcher._is_dbt_builtin_package("DBT_POSTGRES") is True

    # Community packages (should NOT be filtered)
    assert fetcher._is_dbt_builtin_package("dbt_utils") is False
    assert fetcher._is_dbt_builtin_package("dbt_expectations") is False
    assert fetcher._is_dbt_builtin_package("dbt_date") is False

    # Partner adapters (should NOT be filtered)
    assert fetcher._is_dbt_builtin_package("dbt_databricks") is False
    assert fetcher._is_dbt_builtin_package("dbt_trino") is False
    assert fetcher._is_dbt_builtin_package("dbt_synapse") is False

    # User project packages (should NOT be filtered)
    assert fetcher._is_dbt_builtin_package("my_project") is False
    assert fetcher._is_dbt_builtin_package("jaffle_shop") is False
    assert fetcher._is_dbt_builtin_package("") is False
