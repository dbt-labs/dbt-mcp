from unittest.mock import AsyncMock, Mock, patch

import pytest

from dbt_mcp.errors import InvalidParameterError
from mcp.server.elicitation import (
    AcceptedElicitation,
    CancelledElicitation,
    DeclinedElicitation,
)
from dbt_mcp.discovery.tools import ResourceSelection


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

            # Lineage response for both ancestors and descendants queries
            lineage_response = {
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
            }

            # API calls: model search, source search, ancestors lineage, descendants lineage
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
                lineage_response,  # ancestors query
                lineage_response,  # descendants query
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

            # Lineage response for both ancestors and descendants queries
            lineage_response = {
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
            }

            # Only lineage queries (no search) - 2 calls for "both" direction
            mock_client.execute_query = AsyncMock(side_effect=[
                lineage_response,  # ancestors query
                lineage_response,  # descendants query
            ])

            tool_definitions = create_discovery_tool_definitions(mock_config_provider)
            get_lineage_tool = next(
                t for t in tool_definitions if t.get_name() == "get_lineage"
            )

            result = await get_lineage_tool.fn(unique_id="model.test.customers")

            # Should be called twice for lineage (ancestors + descendants), not for search
            assert mock_client.execute_query.call_count == 2
            assert result["target"]["uniqueId"] == "model.test.customers"


class TestGetLineageElicitation:
    """Test MCP elicitation for multiple matches in the get_lineage tool."""

    @pytest.fixture
    def multiple_matches_responses(self):
        """Mock responses for multiple matches scenario."""
        return [
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
        ]

    @pytest.fixture
    def lineage_response(self):
        """Mock lineage response."""
        return {
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
        }

    async def test_elicitation_user_accepts_valid_selection(
        self, mock_config_provider, multiple_matches_responses, lineage_response
    ):
        """Should resolve to selected unique_id when user accepts elicitation."""
        from dbt_mcp.discovery.tools import create_discovery_tool_definitions

        with patch('dbt_mcp.discovery.tools.MetadataAPIClient') as mock_client_class:
            mock_client = Mock()
            mock_client_class.return_value = mock_client
            mock_client.config_provider = mock_config_provider

            # Add lineage responses after multiple matches responses
            # (2 lineage calls for "both" direction: ancestors + descendants)
            mock_client.execute_query = AsyncMock(
                side_effect=multiple_matches_responses + [lineage_response, lineage_response]
            )

            tool_definitions = create_discovery_tool_definitions(mock_config_provider)
            get_lineage_tool = next(
                t for t in tool_definitions if t.get_name() == "get_lineage"
            )

            # Create mock context with elicitation
            mock_ctx = Mock()
            mock_ctx.elicit = AsyncMock(
                return_value=AcceptedElicitation(
                    data=ResourceSelection(unique_id="model.test.customers")
                )
            )

            result = await get_lineage_tool.fn(name="customers", ctx=mock_ctx)

            # Verify elicitation was called
            mock_ctx.elicit.assert_called_once()
            call_kwargs = mock_ctx.elicit.call_args.kwargs
            assert "Multiple resources found" in call_kwargs["message"]
            assert call_kwargs["schema"] == ResourceSelection

            # Verify the correct resource was resolved
            assert result["target"]["uniqueId"] == "model.test.customers"

    async def test_elicitation_user_accepts_invalid_selection(
        self, mock_config_provider, multiple_matches_responses
    ):
        """Should raise error when user selects invalid unique_id."""
        from dbt_mcp.discovery.tools import create_discovery_tool_definitions

        with patch('dbt_mcp.discovery.tools.MetadataAPIClient') as mock_client_class:
            mock_client = Mock()
            mock_client_class.return_value = mock_client
            mock_client.config_provider = mock_config_provider

            mock_client.execute_query = AsyncMock(side_effect=multiple_matches_responses)

            tool_definitions = create_discovery_tool_definitions(mock_config_provider)
            get_lineage_tool = next(
                t for t in tool_definitions if t.get_name() == "get_lineage"
            )

            # Create mock context with invalid selection
            mock_ctx = Mock()
            mock_ctx.elicit = AsyncMock(
                return_value=AcceptedElicitation(
                    data=ResourceSelection(unique_id="model.test.invalid")
                )
            )

            with pytest.raises(InvalidParameterError) as exc_info:
                await get_lineage_tool.fn(name="customers", ctx=mock_ctx)

            assert "Invalid selection" in str(exc_info.value)
            assert "model.test.invalid" in str(exc_info.value)

    async def test_elicitation_user_declines(
        self, mock_config_provider, multiple_matches_responses
    ):
        """Should raise error when user declines elicitation."""
        from dbt_mcp.discovery.tools import create_discovery_tool_definitions

        with patch('dbt_mcp.discovery.tools.MetadataAPIClient') as mock_client_class:
            mock_client = Mock()
            mock_client_class.return_value = mock_client
            mock_client.config_provider = mock_config_provider

            mock_client.execute_query = AsyncMock(side_effect=multiple_matches_responses)

            tool_definitions = create_discovery_tool_definitions(mock_config_provider)
            get_lineage_tool = next(
                t for t in tool_definitions if t.get_name() == "get_lineage"
            )

            # Create mock context with declined elicitation
            mock_ctx = Mock()
            mock_ctx.elicit = AsyncMock(return_value=DeclinedElicitation())

            with pytest.raises(InvalidParameterError) as exc_info:
                await get_lineage_tool.fn(name="customers", ctx=mock_ctx)

            assert "User declined to select a resource" in str(exc_info.value)

    async def test_elicitation_user_cancels(
        self, mock_config_provider, multiple_matches_responses
    ):
        """Should raise error when user cancels elicitation."""
        from dbt_mcp.discovery.tools import create_discovery_tool_definitions

        with patch('dbt_mcp.discovery.tools.MetadataAPIClient') as mock_client_class:
            mock_client = Mock()
            mock_client_class.return_value = mock_client
            mock_client.config_provider = mock_config_provider

            mock_client.execute_query = AsyncMock(side_effect=multiple_matches_responses)

            tool_definitions = create_discovery_tool_definitions(mock_config_provider)
            get_lineage_tool = next(
                t for t in tool_definitions if t.get_name() == "get_lineage"
            )

            # Create mock context with cancelled elicitation
            mock_ctx = Mock()
            mock_ctx.elicit = AsyncMock(return_value=CancelledElicitation())

            with pytest.raises(InvalidParameterError) as exc_info:
                await get_lineage_tool.fn(name="customers", ctx=mock_ctx)

            assert "Operation cancelled by user" in str(exc_info.value)

    async def test_fallback_when_no_context(
        self, mock_config_provider, multiple_matches_responses
    ):
        """Should fall back to error message when context is None."""
        from dbt_mcp.discovery.tools import create_discovery_tool_definitions

        with patch('dbt_mcp.discovery.tools.MetadataAPIClient') as mock_client_class:
            mock_client = Mock()
            mock_client_class.return_value = mock_client
            mock_client.config_provider = mock_config_provider

            mock_client.execute_query = AsyncMock(side_effect=multiple_matches_responses)

            tool_definitions = create_discovery_tool_definitions(mock_config_provider)
            get_lineage_tool = next(
                t for t in tool_definitions if t.get_name() == "get_lineage"
            )

            # Call without context (ctx=None is default)
            with pytest.raises(InvalidParameterError) as exc_info:
                await get_lineage_tool.fn(name="customers")

            error_msg = str(exc_info.value)
            assert "Multiple resources found" in error_msg
            assert "Please specify the full unique_id instead" in error_msg

    async def test_elicitation_timeout_fallback(
        self, mock_config_provider, multiple_matches_responses
    ):
        """Should fall back to error message when elicitation times out or fails."""
        from dbt_mcp.discovery.tools import create_discovery_tool_definitions

        with patch('dbt_mcp.discovery.tools.MetadataAPIClient') as mock_client_class:
            mock_client = Mock()
            mock_client_class.return_value = mock_client
            mock_client.config_provider = mock_config_provider

            mock_client.execute_query = AsyncMock(side_effect=multiple_matches_responses)

            tool_definitions = create_discovery_tool_definitions(mock_config_provider)
            get_lineage_tool = next(
                t for t in tool_definitions if t.get_name() == "get_lineage"
            )

            # Create mock context where elicitation raises an exception (timeout)
            mock_ctx = Mock()
            mock_ctx.elicit = AsyncMock(
                side_effect=Exception("MCP error -32001: Request timed out")
            )

            with pytest.raises(InvalidParameterError) as exc_info:
                await get_lineage_tool.fn(name="customers", ctx=mock_ctx)

            # Should get helpful error with match list, not the timeout exception
            error_msg = str(exc_info.value)
            assert "Multiple resources found" in error_msg
            assert "model.test.customers" in error_msg
            assert "source.test.raw.customers" in error_msg
            assert "Please specify the full unique_id instead" in error_msg
