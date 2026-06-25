import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from dbt_mcp.config.config_providers import SemanticLayerConfig
from dbt_mcp.semantic_layer.client import SemanticLayerFetcher
from dbt_mcp.semantic_layer.types import SavedQueryToolResponse


class TestSavedQueries:
    @pytest.fixture
    def mock_config(self):
        token_p = MagicMock()
        token_p.get_token.return_value = "test-token"
        headers_p = MagicMock()
        headers_p.get_headers.return_value = {}
        return SemanticLayerConfig(
            url="https://test-host/api/graphql",
            host="test-host",
            prod_environment_id=123,
            token_provider=token_p,
            headers_provider=headers_p,
        )

    @pytest.fixture
    def mock_client_provider(self):
        """Create a mock client provider."""
        client_provider = AsyncMock()
        return client_provider

    @pytest.fixture
    def fetcher(self, mock_client_provider):
        """Create a SemanticLayerFetcher instance with mocked dependencies."""
        return SemanticLayerFetcher(
            client_provider=mock_client_provider,
        )

    @pytest.mark.asyncio
    @patch("dbt_mcp.semantic_layer.client.submit_request")
    async def test_list_saved_queries_no_filter(
        self, mock_submit_request, fetcher, mock_config
    ):
        """Test listing saved queries without a search filter."""
        mock_submit_request.return_value = {
            "data": {
                "savedQueries": [
                    {
                        "name": "daily_revenue",
                        "label": "Daily Revenue Report",
                        "description": "Daily revenue metrics by product",
                    },
                    {
                        "name": "monthly_users",
                        "label": "Monthly Active Users",
                        "description": "Monthly active user counts",
                    },
                ]
            }
        }

        result = await fetcher.list_saved_queries(config=mock_config)

        assert len(result) == 2
        assert isinstance(result[0], SavedQueryToolResponse)
        assert result[0].name == "daily_revenue"
        assert result[0].label == "Daily Revenue Report"
        assert result[0].metrics is None
        assert result[0].group_by is None
        assert result[0].where is None
        assert result[1].name == "monthly_users"

    @pytest.mark.asyncio
    @patch("dbt_mcp.semantic_layer.client.submit_request")
    async def test_list_saved_queries_with_search(
        self, mock_submit_request, fetcher, mock_config
    ):
        """Test that client-side search filters by name."""
        mock_submit_request.return_value = {
            "data": {
                "savedQueries": [
                    {
                        "name": "daily_revenue",
                        "label": "Daily Revenue Report",
                        "description": "Daily revenue metrics",
                    },
                    {
                        "name": "monthly_users",
                        "label": "Monthly Active Users",
                        "description": "Monthly active user counts",
                    },
                ]
            }
        }

        result = await fetcher.list_saved_queries(config=mock_config, search="revenue")

        assert len(result) == 1
        assert result[0].name == "daily_revenue"

        # Search is client-side; both calls pass empty variables
        assert mock_submit_request.call_count == 2
        for call in mock_submit_request.call_args_list:
            assert call[0][1]["variables"] == {}

    @pytest.mark.asyncio
    @patch("dbt_mcp.semantic_layer.client.submit_request")
    async def test_list_saved_queries_empty_result(
        self, mock_submit_request, fetcher, mock_config
    ):
        """Test listing saved queries when no queries exist."""
        mock_submit_request.return_value = {"data": {"savedQueries": []}}

        result = await fetcher.list_saved_queries(config=mock_config)

        assert result == []

    @pytest.mark.asyncio
    @patch("dbt_mcp.semantic_layer.client.submit_request")
    async def test_list_saved_queries_missing_attributes(
        self, mock_submit_request, fetcher, mock_config
    ):
        """Test listing saved queries when optional attributes are missing."""
        mock_submit_request.return_value = {
            "data": {
                "savedQueries": [
                    {
                        "name": "test_query",
                        # Missing label and description
                    }
                ]
            }
        }

        result = await fetcher.list_saved_queries(config=mock_config)

        assert len(result) == 1
        assert result[0].name == "test_query"
        assert result[0].label is None
        assert result[0].description is None
        assert result[0].metrics is None
        assert result[0].group_by is None
        assert result[0].where is None

    @pytest.mark.asyncio
    @patch("dbt_mcp.semantic_layer.client.submit_request")
    async def test_list_saved_queries_search_filters_by_name(
        self, mock_submit_request, fetcher, mock_config
    ):
        """Test that client-side search matches on name substring."""
        mock_submit_request.return_value = {
            "data": {
                "savedQueries": [
                    {
                        "name": "arr_growth",
                        "label": "ARR Growth",
                        "description": "Annual recurring revenue growth",
                    },
                    {
                        "name": "churn_rate",
                        "label": "Churn Rate",
                        "description": "Customer churn rate",
                    },
                ]
            }
        }

        result = await fetcher.list_saved_queries(config=mock_config, search="arr")

        assert len(result) == 1
        assert result[0].name == "arr_growth"

    @pytest.mark.asyncio
    @patch("dbt_mcp.semantic_layer.client.submit_request")
    async def test_list_saved_queries_search_filters_by_label(
        self, mock_submit_request, fetcher, mock_config
    ):
        """Test that client-side search matches on label substring."""
        mock_submit_request.return_value = {
            "data": {
                "savedQueries": [
                    {
                        "name": "q1",
                        "label": "Weekly Active Users",
                        "description": "WAU metric",
                    },
                    {
                        "name": "q2",
                        "label": "Monthly Revenue",
                        "description": "MRR metric",
                    },
                ]
            }
        }

        result = await fetcher.list_saved_queries(config=mock_config, search="monthly")

        assert len(result) == 1
        assert result[0].name == "q2"

    @pytest.mark.asyncio
    @patch("dbt_mcp.semantic_layer.client.submit_request")
    async def test_list_saved_queries_search_filters_by_description(
        self, mock_submit_request, fetcher, mock_config
    ):
        """Test that client-side search matches on description substring."""
        mock_submit_request.return_value = {
            "data": {
                "savedQueries": [
                    {
                        "name": "q1",
                        "label": "Label A",
                        "description": "Tracks pipeline conversion",
                    },
                    {
                        "name": "q2",
                        "label": "Label B",
                        "description": "Tracks customer retention",
                    },
                ]
            }
        }

        result = await fetcher.list_saved_queries(config=mock_config, search="pipeline")

        assert len(result) == 1
        assert result[0].name == "q1"

    @pytest.mark.asyncio
    @patch("dbt_mcp.semantic_layer.client.submit_request")
    async def test_list_saved_queries_search_is_case_insensitive(
        self, mock_submit_request, fetcher, mock_config
    ):
        """Test that client-side search is case-insensitive."""
        mock_submit_request.return_value = {
            "data": {
                "savedQueries": [
                    {
                        "name": "arr_current",
                        "label": "ARR Current",
                        "description": "Current ARR",
                    },
                ]
            }
        }

        result = await fetcher.list_saved_queries(config=mock_config, search="ARR")

        assert len(result) == 1
        assert result[0].name == "arr_current"

    @pytest.mark.asyncio
    @patch("dbt_mcp.semantic_layer.client.submit_request")
    async def test_list_saved_queries_search_none_returns_all(
        self, mock_submit_request, fetcher, mock_config
    ):
        """Test that search=None returns all items unfiltered."""
        mock_submit_request.return_value = {
            "data": {
                "savedQueries": [
                    {"name": "q1", "label": None, "description": None},
                    {"name": "q2", "label": None, "description": None},
                    {"name": "q3", "label": None, "description": None},
                ]
            }
        }

        result = await fetcher.list_saved_queries(config=mock_config, search=None)

        assert len(result) == 3

    @pytest.mark.asyncio
    @patch("dbt_mcp.semantic_layer.client.submit_request")
    async def test_list_saved_queries_null_label_and_description(
        self, mock_submit_request, fetcher, mock_config
    ):
        """Test that explicit null label and description from the API are handled correctly."""
        mock_submit_request.return_value = {
            "data": {
                "savedQueries": [
                    {
                        "name": "minimal_query",
                        "label": None,
                        "description": None,
                    }
                ]
            }
        }

        result = await fetcher.list_saved_queries(config=mock_config)

        assert len(result) == 1
        assert result[0].name == "minimal_query"
        assert result[0].label is None
        assert result[0].description is None
        assert result[0].metrics is None
        assert result[0].group_by is None
        assert result[0].where is None

    @pytest.mark.asyncio
    @patch("dbt_mcp.semantic_layer.client.submit_request")
    async def test_list_saved_queries_with_params_success(
        self, mock_submit_request, fetcher, mock_config
    ):
        """Test that queryParams fields are populated when the with_params query succeeds."""
        full_response = {
            "data": {
                "savedQueries": [
                    {
                        "name": "revenue_query",
                        "label": "Revenue",
                        "description": "Revenue metrics",
                        "queryParams": {
                            "metrics": [{"name": "revenue"}, {"name": "profit"}],
                            "groupBy": [{"name": "date"}],
                            "where": {"whereSqlTemplate": "date >= '2024-01-01'"},
                        },
                    }
                ]
            }
        }
        # First call (simple) and second call (with_params) both return the full response
        mock_submit_request.side_effect = [full_response, full_response]

        result = await fetcher.list_saved_queries(config=mock_config)

        assert len(result) == 1
        assert result[0].metrics == ["revenue", "profit"]
        assert result[0].group_by == ["date"]
        assert result[0].where == "date >= '2024-01-01'"

    @pytest.mark.asyncio
    @patch("dbt_mcp.semantic_layer.client.submit_request")
    async def test_list_saved_queries_with_params_fallback(
        self, mock_submit_request, fetcher, mock_config
    ):
        """Test that a timeout on the with_params query falls back to simple results."""
        simple_response = {
            "data": {
                "savedQueries": [
                    {
                        "name": "revenue_query",
                        "label": "Revenue",
                        "description": "Revenue metrics",
                    },
                ]
            }
        }
        # First call (simple) succeeds; second call (with_params) times out
        mock_submit_request.side_effect = [simple_response, Exception("ReadTimeout")]

        result = await fetcher.list_saved_queries(config=mock_config)

        assert len(result) == 1
        assert result[0].name == "revenue_query"
        assert result[0].metrics is None
        assert result[0].group_by is None
        assert result[0].where is None
