from unittest.mock import AsyncMock, Mock, patch

import pytest

from dbt_mcp.errors import InvalidParameterError


@pytest.fixture
def mock_lineage_fetcher():
    """Create a mock LineageFetcher for testing the tool function."""
    mock = Mock()
    mock.search_all_resources = AsyncMock()
    mock.fetch_lineage = AsyncMock()
    return mock


@pytest.fixture
def mock_config_provider():
    """Create a mock config provider."""
    mock_provider = Mock()
    mock_config = Mock()
    mock_config.environment_id = 123

    async def mock_get_config():
        return mock_config

    mock_provider.get_config = mock_get_config
    return mock_provider


class TestGetLineageValidation:
    """Test parameter validation in the get_lineage tool."""

    async def test_neither_name_nor_unique_id(self, mock_config_provider):
        """Should raise error when neither name nor unique_id is provided."""
        from dbt_mcp.discovery.tools import create_discovery_tool_definitions

        with patch('dbt_mcp.discovery.tools.MetadataAPIClient'):
            tool_definitions = create_discovery_tool_definitions(mock_config_provider)

            # Find the get_lineage tool
            get_lineage_tool = next(
                t for t in tool_definitions if t.get_name() == "get_lineage"
            )

            with pytest.raises(InvalidParameterError) as exc_info:
                await get_lineage_tool.fn()

            assert "Either 'name' or 'unique_id' must be provided" in str(exc_info.value)

    async def test_both_name_and_unique_id(self, mock_config_provider):
        """Should raise error when both name and unique_id are provided."""
        from dbt_mcp.discovery.tools import create_discovery_tool_definitions

        with patch('dbt_mcp.discovery.tools.MetadataAPIClient'):
            tool_definitions = create_discovery_tool_definitions(mock_config_provider)

            get_lineage_tool = next(
                t for t in tool_definitions if t.get_name() == "get_lineage"
            )

            with pytest.raises(InvalidParameterError) as exc_info:
                await get_lineage_tool.fn(name="customers", unique_id="model.test.customers")

            assert "Only one of 'name' or 'unique_id' should be provided" in str(exc_info.value)

    async def test_invalid_direction(self, mock_config_provider):
        """Should raise error for invalid direction value."""
        from dbt_mcp.discovery.tools import create_discovery_tool_definitions

        with patch('dbt_mcp.discovery.tools.MetadataAPIClient'):
            tool_definitions = create_discovery_tool_definitions(mock_config_provider)

            get_lineage_tool = next(
                t for t in tool_definitions if t.get_name() == "get_lineage"
            )

            with pytest.raises(InvalidParameterError) as exc_info:
                await get_lineage_tool.fn(name="customers", direction="invalid")

            assert "direction must be one of:" in str(exc_info.value)

    async def test_invalid_types(self, mock_config_provider):
        """Should raise error for invalid resource types."""
        from dbt_mcp.discovery.tools import create_discovery_tool_definitions

        with patch('dbt_mcp.discovery.tools.MetadataAPIClient'):
            tool_definitions = create_discovery_tool_definitions(mock_config_provider)

            get_lineage_tool = next(
                t for t in tool_definitions if t.get_name() == "get_lineage"
            )

            with pytest.raises(InvalidParameterError) as exc_info:
                await get_lineage_tool.fn(name="customers", types=["InvalidType"])

            assert "Invalid resource type(s)" in str(exc_info.value)

    async def test_valid_types(self, mock_config_provider):
        """Should accept valid resource types."""
        from dbt_mcp.discovery.tools import create_discovery_tool_definitions

        with patch('dbt_mcp.discovery.tools.MetadataAPIClient') as mock_client_class:
            # Set up mock chain
            mock_client = Mock()
            mock_client_class.return_value = mock_client
            mock_client.config_provider = mock_config_provider
            mock_client.execute_query = AsyncMock(return_value={
                "data": {
                    "environment": {
                        "applied": {
                            "models": {"pageInfo": {"endCursor": None}, "edges": []},
                            "sources": {"pageInfo": {"hasNextPage": False, "endCursor": None}, "edges": []},
                        }
                    }
                }
            })

            tool_definitions = create_discovery_tool_definitions(mock_config_provider)

            get_lineage_tool = next(
                t for t in tool_definitions if t.get_name() == "get_lineage"
            )

            # Should raise "not found" error (valid types but resource doesn't exist)
            with pytest.raises(InvalidParameterError) as exc_info:
                await get_lineage_tool.fn(name="customers", types=["Model", "Source"])

            # The error should be about not finding the resource, not invalid types
            assert "No resource found" in str(exc_info.value)


class TestGetLineageResolution:
    """Test name resolution in the get_lineage tool."""

    async def test_single_match_resolves(self, mock_config_provider):
        """Should resolve name to unique_id when single match found."""
        from dbt_mcp.discovery.tools import create_discovery_tool_definitions

        with patch('dbt_mcp.discovery.tools.MetadataAPIClient') as mock_client_class:
            mock_client = Mock()
            mock_client_class.return_value = mock_client
            mock_client.config_provider = mock_config_provider

            # First call for model search returns a match
            # Second call for source search returns no matches
            # Third call for lineage query returns lineage data
            mock_client.execute_query = AsyncMock(side_effect=[
                {
                    "data": {
                        "environment": {
                            "applied": {
                                "models": {
                                    "pageInfo": {"endCursor": None},
                                    "edges": [
                                        {
                                            "node": {
                                                "name": "customers",
                                                "uniqueId": "model.test.customers",
                                                "description": "Test model",
                                            }
                                        }
                                    ],
                                }
                            }
                        }
                    }
                },
                {
                    "data": {
                        "environment": {
                            "applied": {
                                "sources": {
                                    "pageInfo": {"hasNextPage": False, "endCursor": None},
                                    "edges": [],
                                }
                            }
                        }
                    }
                },
                {
                    "data": {
                        "environment": {
                            "applied": {
                                "lineage": [
                                    {
                                        "uniqueId": "model.test.customers",
                                        "name": "customers",
                                        "resourceType": "Model",
                                        "matchesMethod": True,
                                    }
                                ]
                            }
                        }
                    }
                },
            ])

            tool_definitions = create_discovery_tool_definitions(mock_config_provider)
            get_lineage_tool = next(
                t for t in tool_definitions if t.get_name() == "get_lineage"
            )

            result = await get_lineage_tool.fn(name="customers")

            assert result["target"]["uniqueId"] == "model.test.customers"

    async def test_multiple_matches_raises_error(self, mock_config_provider):
        """Should raise error with options when multiple matches found."""
        from dbt_mcp.discovery.tools import create_discovery_tool_definitions

        with patch('dbt_mcp.discovery.tools.MetadataAPIClient') as mock_client_class:
            mock_client = Mock()
            mock_client_class.return_value = mock_client
            mock_client.config_provider = mock_config_provider

            # Model search returns one match
            # Source search also returns a match (same name, different type)
            mock_client.execute_query = AsyncMock(side_effect=[
                {
                    "data": {
                        "environment": {
                            "applied": {
                                "models": {
                                    "pageInfo": {"endCursor": None},
                                    "edges": [
                                        {
                                            "node": {
                                                "name": "customers",
                                                "uniqueId": "model.test.customers",
                                                "description": "Test model",
                                            }
                                        }
                                    ],
                                }
                            }
                        }
                    }
                },
                {
                    "data": {
                        "environment": {
                            "applied": {
                                "sources": {
                                    "pageInfo": {"hasNextPage": False, "endCursor": None},
                                    "edges": [
                                        {
                                            "node": {
                                                "name": "customers",
                                                "uniqueId": "source.test.raw.customers",
                                                "description": "Test source",
                                            }
                                        }
                                    ],
                                }
                            }
                        }
                    }
                },
            ])

            tool_definitions = create_discovery_tool_definitions(mock_config_provider)
            get_lineage_tool = next(
                t for t in tool_definitions if t.get_name() == "get_lineage"
            )

            with pytest.raises(InvalidParameterError) as exc_info:
                await get_lineage_tool.fn(name="customers")

            error_msg = str(exc_info.value)
            assert "Multiple resources found" in error_msg
            assert "model.test.customers" in error_msg
            assert "source.test.raw.customers" in error_msg

    async def test_unique_id_skips_resolution(self, mock_config_provider):
        """Should skip resolution when unique_id is provided."""
        from dbt_mcp.discovery.tools import create_discovery_tool_definitions

        with patch('dbt_mcp.discovery.tools.MetadataAPIClient') as mock_client_class:
            mock_client = Mock()
            mock_client_class.return_value = mock_client
            mock_client.config_provider = mock_config_provider

            # Only the lineage query should be called (no search)
            mock_client.execute_query = AsyncMock(return_value={
                "data": {
                    "environment": {
                        "applied": {
                            "lineage": [
                                {
                                    "uniqueId": "model.test.customers",
                                    "name": "customers",
                                    "resourceType": "Model",
                                    "matchesMethod": True,
                                }
                            ]
                        }
                    }
                }
            })

            tool_definitions = create_discovery_tool_definitions(mock_config_provider)
            get_lineage_tool = next(
                t for t in tool_definitions if t.get_name() == "get_lineage"
            )

            result = await get_lineage_tool.fn(unique_id="model.test.customers")

            # Should only be called once (for lineage, not for search)
            assert mock_client.execute_query.call_count == 1
            assert result["target"]["uniqueId"] == "model.test.customers"
