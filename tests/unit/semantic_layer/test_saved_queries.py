import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from dbt_mcp.semantic_layer.client import SemanticLayerFetcher
from dbt_mcp.semantic_layer.types import SavedQueryToolResponse


class TestSavedQueries:
    @pytest.fixture
    def mock_config_provider(self):
        """Create a mock config provider."""
        config_provider = AsyncMock()
        config_provider.get_config.return_value = MagicMock(
            prod_environment_id=123,
            token="test-token",
            host="test-host",
            url="https://test-host/api/graphql",
        )
        return config_provider

    @pytest.fixture
    def mock_client_provider(self):
        """Create a mock client provider."""
        client_provider = AsyncMock()
        return client_provider

    @pytest.fixture
    def fetcher(self, mock_config_provider, mock_client_provider):
        """Create a SemanticLayerFetcher instance with mocked dependencies."""
        return SemanticLayerFetcher(
            config_provider=mock_config_provider,
            client_provider=mock_client_provider,
        )

    @pytest.mark.asyncio
    @patch("dbt_mcp.semantic_layer.client.submit_request")
    async def test_list_saved_queries_no_filter(
        self, mock_submit_request, fetcher, mock_config_provider
    ):
        """Test listing saved queries without a search filter."""
        # Mock GraphQL response
        mock_submit_request.return_value = {
            "data": {
                "savedQueriesPaginated": {
                    "items": [
                        {
                            "name": "daily_revenue",
                            "label": "Daily Revenue Report",
                            "description": "Daily revenue metrics by product",
                            "queryParams": {
                                "metrics": [{"name": "revenue"}, {"name": "profit"}],
                                "groupBy": [{"name": "product_name"}, {"name": "date"}],
                                "where": {"whereSqlTemplate": "date >= '2024-01-01'"},
                            },
                        },
                        {
                            "name": "monthly_users",
                            "label": "Monthly Active Users",
                            "description": "Monthly active user counts",
                            "queryParams": {
                                "metrics": [{"name": "user_count"}],
                                "groupBy": [{"name": "month"}],
                                "where": None,
                            },
                        },
                    ]
                }
            }
        }

        # Call the method
        result = await fetcher.list_saved_queries()

        # Assertions
        assert len(result) == 2
        assert isinstance(result[0], SavedQueryToolResponse)
        assert result[0].name == "daily_revenue"
        assert result[0].label == "Daily Revenue Report"
        assert result[0].metrics == ["revenue", "profit"]
        assert result[0].group_by == ["product_name", "date"]
        assert result[0].where == "date >= '2024-01-01'"
        assert result[1].name == "monthly_users"
        assert result[1].metrics == ["user_count"]
        assert result[1].where is None

    @pytest.mark.asyncio
    @patch("dbt_mcp.semantic_layer.client.submit_request")
    async def test_list_saved_queries_with_search(
        self, mock_submit_request, fetcher, mock_config_provider
    ):
        """Test listing saved queries with a search filter."""
        # Mock GraphQL response - only revenue query matches search
        mock_submit_request.return_value = {
            "data": {
                "savedQueriesPaginated": {
                    "items": [
                        {
                            "name": "daily_revenue",
                            "label": "Daily Revenue Report",
                            "description": "Daily revenue metrics",
                            "queryParams": {},
                        }
                    ]
                }
            }
        }

        # Call the method with search filter
        result = await fetcher.list_saved_queries(search="revenue")

        # Should return the matched query
        assert len(result) == 1
        assert result[0].name == "daily_revenue"

        # Verify search parameter was passed
        mock_submit_request.assert_called_once()
        call_args = mock_submit_request.call_args[0]
        assert call_args[1]["variables"]["search"] == "revenue"

    @pytest.mark.asyncio
    @patch("dbt_mcp.semantic_layer.client.submit_request")
    async def test_list_saved_queries_empty_result(
        self, mock_submit_request, fetcher, mock_config_provider
    ):
        """Test listing saved queries when no queries exist."""
        # Mock empty GraphQL response
        mock_submit_request.return_value = {
            "data": {"savedQueriesPaginated": {"items": []}}
        }

        # Call the method
        result = await fetcher.list_saved_queries()

        # Should return empty list
        assert result == []

    @pytest.mark.asyncio
    @patch("dbt_mcp.semantic_layer.client.submit_request")
    async def test_list_saved_queries_missing_attributes(
        self, mock_submit_request, fetcher, mock_config_provider
    ):
        """Test listing saved queries when some attributes are missing."""
        # Mock GraphQL response with missing attributes
        mock_submit_request.return_value = {
            "data": {
                "savedQueriesPaginated": {
                    "items": [
                        {
                            "name": "test_query",
                            # Missing label, description, and queryParams
                        }
                    ]
                }
            }
        }

        # Call the method
        result = await fetcher.list_saved_queries()

        # Should handle missing attributes gracefully
        assert len(result) == 1
        assert result[0].name == "test_query"
        assert result[0].label is None
        assert result[0].description is None
        assert result[0].metrics is None
        assert result[0].group_by is None
        assert result[0].where is None
