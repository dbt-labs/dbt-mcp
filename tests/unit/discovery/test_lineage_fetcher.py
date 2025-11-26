from unittest.mock import patch

import pytest

from dbt_mcp.discovery.client import LineageFetcher, LineageDirection
from dbt_mcp.errors import GraphQLError


@pytest.fixture
def lineage_fetcher(mock_api_client):
    return LineageFetcher(api_client=mock_api_client)


async def test_get_environment_id(lineage_fetcher):
    environment_id = await lineage_fetcher.get_environment_id()
    assert environment_id == 123


class TestSearchModelsByName:
    async def test_single_match(self, lineage_fetcher, mock_api_client):
        mock_response = {
            "data": {
                "environment": {
                    "applied": {
                        "models": {
                            "pageInfo": {"endCursor": None},
                            "edges": [
                                {
                                    "node": {
                                        "name": "customers",
                                        "uniqueId": "model.jaffle_shop.customers",
                                        "description": "Customer model",
                                    }
                                }
                            ],
                        }
                    }
                }
            }
        }

        mock_api_client.execute_query.return_value = mock_response

        result = await lineage_fetcher.search_models_by_name("customers")

        assert len(result) == 1
        assert result[0]["uniqueId"] == "model.jaffle_shop.customers"
        assert result[0]["name"] == "customers"
        assert result[0]["resourceType"] == "Model"

    async def test_no_match(self, lineage_fetcher, mock_api_client):
        mock_response = {
            "data": {
                "environment": {
                    "applied": {
                        "models": {
                            "pageInfo": {"endCursor": None},
                            "edges": [],
                        }
                    }
                }
            }
        }

        mock_api_client.execute_query.return_value = mock_response

        result = await lineage_fetcher.search_models_by_name("nonexistent")

        assert result == []

    async def test_multiple_matches(self, lineage_fetcher, mock_api_client):
        mock_response = {
            "data": {
                "environment": {
                    "applied": {
                        "models": {
                            "pageInfo": {"endCursor": None},
                            "edges": [
                                {
                                    "node": {
                                        "name": "customers",
                                        "uniqueId": "model.project_a.customers",
                                        "description": "Project A customers",
                                    }
                                },
                                {
                                    "node": {
                                        "name": "customers",
                                        "uniqueId": "model.project_b.customers",
                                        "description": "Project B customers",
                                    }
                                },
                            ],
                        }
                    }
                }
            }
        }

        mock_api_client.execute_query.return_value = mock_response

        result = await lineage_fetcher.search_models_by_name("customers")

        assert len(result) == 2
        assert result[0]["uniqueId"] == "model.project_a.customers"
        assert result[1]["uniqueId"] == "model.project_b.customers"


class TestSearchSourcesByName:
    async def test_single_match(self, lineage_fetcher, mock_api_client):
        mock_response = {
            "data": {
                "environment": {
                    "applied": {
                        "sources": {
                            "pageInfo": {"hasNextPage": False, "endCursor": None},
                            "edges": [
                                {
                                    "node": {
                                        "name": "customers",
                                        "uniqueId": "source.jaffle_shop.stripe.customers",
                                        "description": "Raw customer data",
                                    }
                                }
                            ],
                        }
                    }
                }
            }
        }

        mock_api_client.execute_query.return_value = mock_response

        result = await lineage_fetcher.search_sources_by_name("customers")

        assert len(result) == 1
        assert result[0]["uniqueId"] == "source.jaffle_shop.stripe.customers"
        assert result[0]["name"] == "customers"
        assert result[0]["resourceType"] == "Source"

    async def test_no_match(self, lineage_fetcher, mock_api_client):
        mock_response = {
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
        }

        mock_api_client.execute_query.return_value = mock_response

        result = await lineage_fetcher.search_sources_by_name("nonexistent")

        assert result == []


class TestSearchAllResources:
    async def test_model_found_first(self, lineage_fetcher, mock_api_client):
        """When model is found, sources should still be searched."""
        model_response = {
            "data": {
                "environment": {
                    "applied": {
                        "models": {
                            "pageInfo": {"endCursor": None},
                            "edges": [
                                {
                                    "node": {
                                        "name": "customers",
                                        "uniqueId": "model.jaffle_shop.customers",
                                        "description": "Model",
                                    }
                                }
                            ],
                        }
                    }
                }
            }
        }
        source_response = {
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
        }

        mock_api_client.execute_query.side_effect = [model_response, source_response]

        result = await lineage_fetcher.search_all_resources("customers")

        assert len(result) == 1
        assert result[0]["uniqueId"] == "model.jaffle_shop.customers"
        assert result[0]["resourceType"] == "Model"

    async def test_both_model_and_source_found(self, lineage_fetcher, mock_api_client):
        """When both model and source exist with same name."""
        model_response = {
            "data": {
                "environment": {
                    "applied": {
                        "models": {
                            "pageInfo": {"endCursor": None},
                            "edges": [
                                {
                                    "node": {
                                        "name": "customers",
                                        "uniqueId": "model.jaffle_shop.customers",
                                        "description": "Model",
                                    }
                                }
                            ],
                        }
                    }
                }
            }
        }
        source_response = {
            "data": {
                "environment": {
                    "applied": {
                        "sources": {
                            "pageInfo": {"hasNextPage": False, "endCursor": None},
                            "edges": [
                                {
                                    "node": {
                                        "name": "customers",
                                        "uniqueId": "source.jaffle_shop.raw.customers",
                                        "description": "Source",
                                    }
                                }
                            ],
                        }
                    }
                }
            }
        }

        mock_api_client.execute_query.side_effect = [model_response, source_response]

        result = await lineage_fetcher.search_all_resources("customers")

        assert len(result) == 2
        assert result[0]["resourceType"] == "Model"
        assert result[1]["resourceType"] == "Source"

    async def test_no_matches(self, lineage_fetcher, mock_api_client):
        """When neither model nor source is found."""
        empty_model_response = {
            "data": {
                "environment": {
                    "applied": {
                        "models": {
                            "pageInfo": {"endCursor": None},
                            "edges": [],
                        }
                    }
                }
            }
        }
        empty_source_response = {
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
        }

        mock_api_client.execute_query.side_effect = [
            empty_model_response,
            empty_source_response,
        ]

        result = await lineage_fetcher.search_all_resources("nonexistent")

        assert result == []


class TestBuildSelector:
    def test_ancestors(self, lineage_fetcher):
        result = lineage_fetcher._build_selector(
            "model.jaffle_shop.customers", LineageDirection.ANCESTORS
        )
        assert result == "+model.jaffle_shop.customers"

    def test_descendants(self, lineage_fetcher):
        result = lineage_fetcher._build_selector(
            "model.jaffle_shop.customers", LineageDirection.DESCENDANTS
        )
        assert result == "model.jaffle_shop.customers+"

    def test_both(self, lineage_fetcher):
        result = lineage_fetcher._build_selector(
            "model.jaffle_shop.customers", LineageDirection.BOTH
        )
        assert result == "+model.jaffle_shop.customers+"


class TestFetchLineage:
    async def test_fetch_ancestors(self, lineage_fetcher, mock_api_client):
        mock_response = {
            "data": {
                "environment": {
                    "applied": {
                        "lineage": [
                            {
                                "uniqueId": "model.jaffle_shop.customers",
                                "name": "customers",
                                "resourceType": "Model",
                                "matchesMethod": True,
                                "filePath": "models/customers.sql",
                            },
                            {
                                "uniqueId": "source.jaffle_shop.raw.customers",
                                "name": "customers",
                                "resourceType": "Source",
                                "matchesMethod": False,
                                "filePath": "models/sources.yml",
                            },
                        ]
                    }
                }
            }
        }

        mock_api_client.execute_query.return_value = mock_response

        result = await lineage_fetcher.fetch_lineage(
            unique_id="model.jaffle_shop.customers",
            direction=LineageDirection.ANCESTORS,
        )

        assert result["target"]["uniqueId"] == "model.jaffle_shop.customers"
        assert result["target"]["matchesMethod"] is True
        assert "ancestors" in result
        assert len(result["ancestors"]) == 1
        assert result["ancestors"][0]["uniqueId"] == "source.jaffle_shop.raw.customers"
        assert "descendants" not in result

    async def test_fetch_descendants(self, lineage_fetcher, mock_api_client):
        mock_response = {
            "data": {
                "environment": {
                    "applied": {
                        "lineage": [
                            {
                                "uniqueId": "model.jaffle_shop.customers",
                                "name": "customers",
                                "resourceType": "Model",
                                "matchesMethod": True,
                                "filePath": "models/customers.sql",
                            },
                            {
                                "uniqueId": "model.jaffle_shop.customer_orders",
                                "name": "customer_orders",
                                "resourceType": "Model",
                                "matchesMethod": False,
                                "filePath": "models/customer_orders.sql",
                            },
                        ]
                    }
                }
            }
        }

        mock_api_client.execute_query.return_value = mock_response

        result = await lineage_fetcher.fetch_lineage(
            unique_id="model.jaffle_shop.customers",
            direction=LineageDirection.DESCENDANTS,
        )

        assert result["target"]["uniqueId"] == "model.jaffle_shop.customers"
        assert "descendants" in result
        assert len(result["descendants"]) == 1
        assert (
            result["descendants"][0]["uniqueId"] == "model.jaffle_shop.customer_orders"
        )
        assert "ancestors" not in result

    async def test_fetch_both_directions(self, lineage_fetcher, mock_api_client):
        """Test that 'both' direction makes two API calls and correctly categorizes results.

        This test verifies BUG-001 fix: ancestors and descendants are properly separated
        by making two separate API calls instead of incorrectly putting all nodes in ancestors.
        """
        # Mock response for ancestors query (+uniqueId)
        ancestors_response = {
            "data": {
                "environment": {
                    "applied": {
                        "lineage": [
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
                    }
                }
            }
        }

        # Mock response for descendants query (uniqueId+)
        descendants_response = {
            "data": {
                "environment": {
                    "applied": {
                        "lineage": [
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
                    }
                }
            }
        }

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

    async def test_fetch_with_types_filter(self, lineage_fetcher, mock_api_client):
        mock_response = {
            "data": {
                "environment": {
                    "applied": {
                        "lineage": [
                            {
                                "uniqueId": "model.jaffle_shop.customers",
                                "name": "customers",
                                "resourceType": "Model",
                                "matchesMethod": True,
                            }
                        ]
                    }
                }
            }
        }

        mock_api_client.execute_query.return_value = mock_response

        await lineage_fetcher.fetch_lineage(
            unique_id="model.jaffle_shop.customers",
            direction=LineageDirection.ANCESTORS,
            types=["Model", "Source"],
        )

        # Verify the filter was passed correctly
        call_args = mock_api_client.execute_query.call_args
        variables = call_args[0][1]
        assert variables["filter"]["types"] == ["Model", "Source"]

    async def test_empty_lineage(self, lineage_fetcher, mock_api_client):
        mock_response = {
            "data": {"environment": {"applied": {"lineage": []}}}
        }

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
        self, mock_raise_gql_error, lineage_fetcher, mock_api_client
    ):
        mock_response = {"data": {"environment": {"applied": {"lineage": []}}}}

        mock_raise_gql_error.side_effect = GraphQLError("Test GraphQL error")
        mock_api_client.execute_query.return_value = mock_response

        with pytest.raises(GraphQLError, match="Test GraphQL error"):
            await lineage_fetcher.fetch_lineage(
                unique_id="model.jaffle_shop.customers",
                direction=LineageDirection.ANCESTORS,
            )


class TestPaginationAndTruncation:
    """Tests for pagination metadata and truncation behavior."""

    async def test_no_truncation_under_limit(self, lineage_fetcher, mock_api_client):
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

        mock_response = {
            "data": {"environment": {"applied": {"lineage": lineage_nodes}}}
        }
        mock_api_client.execute_query.return_value = mock_response

        result = await lineage_fetcher.fetch_lineage(
            unique_id="model.jaffle_shop.target",
            direction=LineageDirection.ANCESTORS,
        )

        assert len(result["ancestors"]) == 10
        assert result["pagination"]["limit"] == 50
        assert result["pagination"]["ancestors_total"] == 10
        assert result["pagination"]["ancestors_truncated"] is False

    async def test_truncation_over_limit(self, lineage_fetcher, mock_api_client):
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

        mock_response = {
            "data": {"environment": {"applied": {"lineage": lineage_nodes}}}
        }
        mock_api_client.execute_query.return_value = mock_response

        result = await lineage_fetcher.fetch_lineage(
            unique_id="model.jaffle_shop.target",
            direction=LineageDirection.ANCESTORS,
        )

        assert len(result["ancestors"]) == 50  # Truncated to limit
        assert result["pagination"]["limit"] == 50
        assert result["pagination"]["ancestors_total"] == 60  # Original count
        assert result["pagination"]["ancestors_truncated"] is True

    async def test_pagination_metadata_descendants(self, lineage_fetcher, mock_api_client):
        """Descendants direction should have descendants_* pagination fields."""
        lineage_nodes = [
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
            for i in range(5)
        ]

        mock_response = {
            "data": {"environment": {"applied": {"lineage": lineage_nodes}}}
        }
        mock_api_client.execute_query.return_value = mock_response

        result = await lineage_fetcher.fetch_lineage(
            unique_id="model.jaffle_shop.target",
            direction=LineageDirection.DESCENDANTS,
        )

        assert "pagination" in result
        assert result["pagination"]["descendants_total"] == 5
        assert result["pagination"]["descendants_truncated"] is False

    async def test_both_directions_merges_pagination(
        self, lineage_fetcher, mock_api_client
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

        ancestors_response = {
            "data": {"environment": {"applied": {"lineage": ancestors_nodes}}}
        }

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

        descendants_response = {
            "data": {"environment": {"applied": {"lineage": descendants_nodes}}}
        }

        mock_api_client.execute_query.side_effect = [
            ancestors_response,
            descendants_response,
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

    async def test_exactly_at_limit(self, lineage_fetcher, mock_api_client):
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

        mock_response = {
            "data": {"environment": {"applied": {"lineage": lineage_nodes}}}
        }
        mock_api_client.execute_query.return_value = mock_response

        result = await lineage_fetcher.fetch_lineage(
            unique_id="model.jaffle_shop.target",
            direction=LineageDirection.ANCESTORS,
        )

        assert len(result["ancestors"]) == 50
        assert result["pagination"]["ancestors_total"] == 50
        assert result["pagination"]["ancestors_truncated"] is False
