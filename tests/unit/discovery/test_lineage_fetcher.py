from unittest.mock import patch

import pytest

from dbt_mcp.discovery.client import LineageFetcher, LineageDirection
from dbt_mcp.errors import GraphQLError


@pytest.fixture
def lineage_fetcher(mock_api_client):
    return LineageFetcher(api_client=mock_api_client)


class TestSearchModelsByName:
    async def test_multiple_matches(self, lineage_fetcher, mock_api_client, response_builders):
        """CRITICAL: Ensures disambiguation works when multiple models have same name."""
        mock_response = response_builders.model_search_response(
            models=[
                {
                    "name": "customers",
                    "uniqueId": "model.project_a.customers",
                    "description": "Project A customers",
                },
                {
                    "name": "customers",
                    "uniqueId": "model.project_b.customers",
                    "description": "Project B customers",
                },
            ]
        )

        mock_api_client.execute_query.return_value = mock_response

        result = await lineage_fetcher.search_models_by_name("customers")

        assert len(result) == 2
        assert result[0]["uniqueId"] == "model.project_a.customers"
        assert result[1]["uniqueId"] == "model.project_b.customers"


class TestSearchAllResources:
    async def test_both_model_and_source_found(self, lineage_fetcher, mock_api_client, response_builders):
        """CRITICAL: Ensures both models AND sources are returned when searching by name."""
        model_response = response_builders.model_search_response(
            models=[
                {
                    "name": "customers",
                    "uniqueId": "model.jaffle_shop.customers",
                    "description": "Model",
                }
            ]
        )
        source_response = response_builders.source_search_response(
            sources=[
                {
                    "name": "customers",
                    "uniqueId": "source.jaffle_shop.raw.customers",
                    "description": "Source",
                }
            ]
        )

        mock_api_client.execute_query.side_effect = [model_response, source_response]

        result = await lineage_fetcher.search_all_resources("customers")

        assert len(result) == 2
        assert result[0]["resourceType"] == "Model"
        assert result[1]["resourceType"] == "Source"


class TestFetchLineage:
    async def test_fetch_both_directions(self, lineage_fetcher, mock_api_client, response_builders):
        """CRITICAL: Tests BUG-001 fix - ancestors and descendants properly separated
        by making two separate API calls instead of incorrectly putting all nodes in ancestors.
        """
        # Mock response for ancestors query (+uniqueId)
        ancestors_response = response_builders.lineage_response(
            nodes=[
                {
                    "uniqueId": "model.jaffle_shop.stg_orders",
                    "name": "stg_orders",
                    "resourceType": "Model",
                    "matchesMethod": True,
                    "filePath": "models/staging/stg_orders.sql",
                },
                {
                    "uniqueId": "seed.jaffle_shop.raw_orders",
                    "name": "raw_orders",
                    "resourceType": "Seed",
                    "matchesMethod": False,
                    "filePath": "seeds/raw_orders.csv",
                },
            ]
        )

        # Mock response for descendants query (uniqueId+)
        descendants_response = response_builders.lineage_response(
            nodes=[
                {
                    "uniqueId": "model.jaffle_shop.stg_orders",
                    "name": "stg_orders",
                    "resourceType": "Model",
                    "matchesMethod": True,
                    "filePath": "models/staging/stg_orders.sql",
                },
                {
                    "uniqueId": "model.jaffle_shop.orders",
                    "name": "orders",
                    "resourceType": "Model",
                    "matchesMethod": False,
                    "filePath": "models/marts/orders.sql",
                },
            ]
        )

        # Return different responses for the two API calls
        mock_api_client.execute_query.side_effect = [
            ancestors_response,
            descendants_response,
        ]

        result = await lineage_fetcher.fetch_lineage(
            unique_id="model.jaffle_shop.stg_orders",
            direction=LineageDirection.BOTH,
        )

        # Verify two API calls were made
        assert mock_api_client.execute_query.call_count == 2

        # Verify target is present
        assert result["target"] is not None
        assert result["target"]["uniqueId"] == "model.jaffle_shop.stg_orders"

        # Verify ancestors contains the upstream seed (not the downstream model)
        assert "ancestors" in result
        assert len(result["ancestors"]) == 1
        assert result["ancestors"][0]["uniqueId"] == "seed.jaffle_shop.raw_orders"

        # Verify descendants contains the downstream model (not the upstream seed)
        assert "descendants" in result
        assert len(result["descendants"]) == 1
        assert result["descendants"][0]["uniqueId"] == "model.jaffle_shop.orders"

    async def test_empty_lineage(self, lineage_fetcher, mock_api_client, response_builders):
        """CRITICAL: Ensures orphan nodes (no ancestors/descendants) return empty arrays, not errors."""
        mock_response = response_builders.lineage_response(nodes=[])

        mock_api_client.execute_query.return_value = mock_response

        result = await lineage_fetcher.fetch_lineage(
            unique_id="model.jaffle_shop.orphan",
            direction=LineageDirection.BOTH,
        )

        assert result["target"] is None
        assert result["ancestors"] == []
        assert result["descendants"] == []

    @patch("dbt_mcp.discovery.client.raise_gql_error")
    async def test_graphql_error_handling(
        self, mock_raise_gql_error, lineage_fetcher, mock_api_client, response_builders
    ):
        """CRITICAL: Ensures GraphQL errors are properly raised and not swallowed."""
        mock_response = response_builders.lineage_response(nodes=[])

        mock_raise_gql_error.side_effect = GraphQLError("Test GraphQL error")
        mock_api_client.execute_query.return_value = mock_response

        with pytest.raises(GraphQLError, match="Test GraphQL error"):
            await lineage_fetcher.fetch_lineage(
                unique_id="model.jaffle_shop.customers",
                direction=LineageDirection.ANCESTORS,
            )


class TestPaginationAndTruncation:
    """CRITICAL: Tests for pagination metadata and truncation behavior - prevents silent data loss."""

    async def test_no_truncation_under_limit(self, lineage_fetcher, mock_api_client, response_builders):
        """Results under 50 nodes should not be truncated."""
        # Create 10 ancestor nodes (under limit of 50)
        lineage_nodes = [
            {
                "uniqueId": "model.jaffle_shop.target",
                "name": "target",
                "resourceType": "Model",
                "matchesMethod": True,
            }
        ] + [
            {
                "uniqueId": f"model.jaffle_shop.ancestor_{i}",
                "name": f"ancestor_{i}",
                "resourceType": "Model",
                "matchesMethod": False,
            }
            for i in range(10)
        ]

        mock_response = response_builders.lineage_response(nodes=lineage_nodes)
        mock_api_client.execute_query.return_value = mock_response

        result = await lineage_fetcher.fetch_lineage(
            unique_id="model.jaffle_shop.target",
            direction=LineageDirection.ANCESTORS,
        )

        assert len(result["ancestors"]) == 10
        assert result["pagination"]["limit"] == 50
        assert result["pagination"]["ancestors_total"] == 10
        assert result["pagination"]["ancestors_truncated"] is False

    async def test_truncation_over_limit(self, lineage_fetcher, mock_api_client, response_builders):
        """Results over 50 nodes should be truncated to 50."""
        # Create 60 ancestor nodes (over limit of 50)
        lineage_nodes = [
            {
                "uniqueId": "model.jaffle_shop.target",
                "name": "target",
                "resourceType": "Model",
                "matchesMethod": True,
            }
        ] + [
            {
                "uniqueId": f"model.jaffle_shop.ancestor_{i}",
                "name": f"ancestor_{i}",
                "resourceType": "Model",
                "matchesMethod": False,
            }
            for i in range(60)
        ]

        mock_response = response_builders.lineage_response(nodes=lineage_nodes)
        mock_api_client.execute_query.return_value = mock_response

        result = await lineage_fetcher.fetch_lineage(
            unique_id="model.jaffle_shop.target",
            direction=LineageDirection.ANCESTORS,
        )

        assert len(result["ancestors"]) == 50  # Truncated to limit
        assert result["pagination"]["limit"] == 50
        assert result["pagination"]["ancestors_total"] == 60  # Original count
        assert result["pagination"]["ancestors_truncated"] is True

    async def test_both_directions_merges_pagination(
        self, lineage_fetcher, mock_api_client, response_builders
    ):
        """Both direction should merge pagination from ancestors and descendants."""
        # Ancestors response (70 nodes - will be truncated)
        ancestors_nodes = [
            {
                "uniqueId": "model.jaffle_shop.target",
                "name": "target",
                "resourceType": "Model",
                "matchesMethod": True,
            }
        ] + [
            {
                "uniqueId": f"model.jaffle_shop.ancestor_{i}",
                "name": f"ancestor_{i}",
                "resourceType": "Model",
                "matchesMethod": False,
            }
            for i in range(70)
        ]

        # Descendants response (30 nodes - not truncated)
        descendants_nodes = [
            {
                "uniqueId": "model.jaffle_shop.target",
                "name": "target",
                "resourceType": "Model",
                "matchesMethod": True,
            }
        ] + [
            {
                "uniqueId": f"model.jaffle_shop.descendant_{i}",
                "name": f"descendant_{i}",
                "resourceType": "Model",
                "matchesMethod": False,
            }
            for i in range(30)
        ]

        mock_api_client.execute_query.side_effect = [
            response_builders.lineage_response(nodes=ancestors_nodes),
            response_builders.lineage_response(nodes=descendants_nodes),
        ]

        result = await lineage_fetcher.fetch_lineage(
            unique_id="model.jaffle_shop.target",
            direction=LineageDirection.BOTH,
        )

        # Check merged pagination
        assert result["pagination"]["limit"] == 50
        assert result["pagination"]["ancestors_total"] == 70
        assert result["pagination"]["ancestors_truncated"] is True
        assert result["pagination"]["descendants_total"] == 30
        assert result["pagination"]["descendants_truncated"] is False

        # Check actual data is truncated
        assert len(result["ancestors"]) == 50
        assert len(result["descendants"]) == 30

    async def test_exactly_at_limit(self, lineage_fetcher, mock_api_client, response_builders):
        """Exactly 50 nodes should not be marked as truncated."""
        lineage_nodes = [
            {
                "uniqueId": "model.jaffle_shop.target",
                "name": "target",
                "resourceType": "Model",
                "matchesMethod": True,
            }
        ] + [
            {
                "uniqueId": f"model.jaffle_shop.ancestor_{i}",
                "name": f"ancestor_{i}",
                "resourceType": "Model",
                "matchesMethod": False,
            }
            for i in range(50)
        ]

        mock_response = response_builders.lineage_response(nodes=lineage_nodes)
        mock_api_client.execute_query.return_value = mock_response

        result = await lineage_fetcher.fetch_lineage(
            unique_id="model.jaffle_shop.target",
            direction=LineageDirection.ANCESTORS,
        )

        assert len(result["ancestors"]) == 50
        assert result["pagination"]["ancestors_total"] == 50
        assert result["pagination"]["ancestors_truncated"] is False
