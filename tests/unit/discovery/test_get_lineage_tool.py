from unittest.mock import AsyncMock, Mock, patch

import pytest

from dbt_mcp.errors import InvalidParameterError
from mcp.server.elicitation import (
    AcceptedElicitation,
    CancelledElicitation,
    DeclinedElicitation,
)
from dbt_mcp.discovery.tools import ResourceSelection


class TestGetLineageValidation:
    """CRITICAL: Test parameter validation in the get_lineage tool - first line of defense against bad data."""

    async def test_neither_name_nor_unique_id(self, mock_config_provider):
        """Should raise error when neither name nor unique_id is provided."""
        from dbt_mcp.discovery.tools import create_discovery_tool_definitions

        with patch('dbt_mcp.discovery.tools.MetadataAPIClient'):
            tool_definitions = create_discovery_tool_definitions(mock_config_provider)

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


class TestGetLineageResolution:
    """CRITICAL: Test name resolution and disambiguation - core UX behavior."""

    async def test_multiple_matches_returns_disambiguation(self, mock_config_provider, response_builders):
        """Should return disambiguation response when multiple matches found (core UX behavior)."""
        from dbt_mcp.discovery.tools import create_discovery_tool_definitions

        with patch('dbt_mcp.discovery.tools.MetadataAPIClient') as mock_client_class:
            mock_client = Mock()
            mock_client_class.return_value = mock_client
            mock_client.config_provider = mock_config_provider

            # Model search returns one match, source search also returns a match
            mock_client.execute_query = AsyncMock(side_effect=[
                response_builders.model_search_response(
                    models=[{"name": "customers", "uniqueId": "model.test.customers", "description": "Test model"}]
                ),
                response_builders.source_search_response(
                    sources=[{"name": "customers", "uniqueId": "source.test.raw.customers", "description": "Test source"}]
                ),
            ])

            tool_definitions = create_discovery_tool_definitions(mock_config_provider)
            get_lineage_tool = next(
                t for t in tool_definitions if t.get_name() == "get_lineage"
            )

            result = await get_lineage_tool.fn(name="customers")

            # Should return disambiguation response, not an error
            assert result["status"] == "disambiguation_required"
            assert "Multiple resources found" in result["message"]
            assert len(result["matches"]) == 2
            # Verify both matches are in the response
            unique_ids = [m["uniqueId"] for m in result["matches"]]
            assert "model.test.customers" in unique_ids
            assert "source.test.raw.customers" in unique_ids


class TestGetLineageElicitation:
    """CRITICAL: Test MCP elicitation flows - all user interaction paths must work."""

    @pytest.fixture
    def multiple_matches_responses(self, response_builders):
        """Mock responses for multiple matches scenario."""
        return [
            response_builders.model_search_response(
                models=[{"name": "customers", "uniqueId": "model.test.customers", "description": "Test model"}]
            ),
            response_builders.source_search_response(
                sources=[{"name": "customers", "uniqueId": "source.test.raw.customers", "description": "Test source"}]
            ),
        ]

    @pytest.fixture
    def lineage_response(self, response_builders):
        """Mock lineage response."""
        return response_builders.lineage_response(
            nodes=[
                {
                    "uniqueId": "model.test.customers",
                    "name": "customers",
                    "resourceType": "Model",
                    "matchesMethod": True,
                }
            ]
        )

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
            mock_client.execute_query = AsyncMock(
                side_effect=multiple_matches_responses + [lineage_response, lineage_response]
            )

            tool_definitions = create_discovery_tool_definitions(mock_config_provider)
            get_lineage_tool = next(
                t for t in tool_definitions if t.get_name() == "get_lineage"
            )

            # Create mock context with elicitation capability
            mock_ctx = Mock()
            mock_ctx.request_context.session.check_client_capability = Mock(
                return_value=True
            )
            mock_ctx.elicit = AsyncMock(
                return_value=AcceptedElicitation(
                    data=ResourceSelection(unique_id="model.test.customers")
                )
            )

            result = await get_lineage_tool.fn(name="customers", ctx=mock_ctx)

            # Verify elicitation was called with correct schema
            mock_ctx.elicit.assert_called_once()
            call_kwargs = mock_ctx.elicit.call_args.kwargs
            assert "Multiple resources found" in call_kwargs["message"]
            assert call_kwargs["schema"] == ResourceSelection

            # Verify the correct resource was resolved
            assert result["target"]["uniqueId"] == "model.test.customers"

    async def test_elicitation_user_accepts_invalid_selection(
        self, mock_config_provider, multiple_matches_responses
    ):
        """SECURITY: Should raise error when user selects invalid unique_id (prevents injection)."""
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
            mock_ctx.request_context.session.check_client_capability = Mock(
                return_value=True
            )
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

            mock_ctx = Mock()
            mock_ctx.request_context.session.check_client_capability = Mock(
                return_value=True
            )
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

            mock_ctx = Mock()
            mock_ctx.request_context.session.check_client_capability = Mock(
                return_value=True
            )
            mock_ctx.elicit = AsyncMock(return_value=CancelledElicitation())

            with pytest.raises(InvalidParameterError) as exc_info:
                await get_lineage_tool.fn(name="customers", ctx=mock_ctx)

            assert "Operation cancelled by user" in str(exc_info.value)

    async def test_fallback_when_no_context(
        self, mock_config_provider, multiple_matches_responses
    ):
        """Graceful degradation: Should return disambiguation response when context is None."""
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

            # Call without context
            result = await get_lineage_tool.fn(name="customers")

            # Should return disambiguation response, not an error
            assert result["status"] == "disambiguation_required"
            assert "Multiple resources found" in result["message"]
            assert len(result["matches"]) == 2
            assert "unique_id parameter" in result["instruction"]

    async def test_elicitation_timeout_fallback(
        self, mock_config_provider, multiple_matches_responses
    ):
        """Graceful degradation: Should return disambiguation response when elicitation times out."""
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

            # Create mock context with timeout
            mock_ctx = Mock()
            mock_ctx.request_context.session.check_client_capability = Mock(
                return_value=True
            )
            mock_ctx.elicit = AsyncMock(
                side_effect=Exception("MCP error -32001: Request timed out")
            )

            result = await get_lineage_tool.fn(name="customers", ctx=mock_ctx)

            # Should return disambiguation response, not an error
            assert result["status"] == "disambiguation_required"
            assert "Multiple resources found" in result["message"]
            assert len(result["matches"]) == 2
            assert "unique_id parameter" in result["instruction"]

    async def test_fallback_when_no_elicitation_capability(
        self, mock_config_provider, multiple_matches_responses
    ):
        """Graceful degradation: Should return disambiguation response when client doesn't support elicitation."""
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

            # Create mock context WITHOUT elicitation capability
            mock_ctx = Mock()
            mock_ctx.request_context.session.check_client_capability = Mock(
                return_value=False
            )

            result = await get_lineage_tool.fn(name="customers", ctx=mock_ctx)

            # Should return disambiguation response, not an error
            assert result["status"] == "disambiguation_required"
            assert "Multiple resources found" in result["message"]
            assert len(result["matches"]) == 2
            assert "unique_id parameter" in result["instruction"]
