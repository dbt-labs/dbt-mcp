import pytest

from dbt_mcp.discovery.client import LineageDirection


async def test_get_environment_id(lineage_fetcher):
    environment_id = await lineage_fetcher.get_environment_id()
    assert environment_id == 123


class TestSearchResources:
    """Test resource search functionality."""

    async def test_search_resource_by_name_models(
        self, lineage_fetcher, mock_api_client
    ):
        """Test searching for models using the generic search method."""
        # Mock packages query response
        packages_response = {
            "data": {"environment": {"applied": {"packages": ["jaffle_shop"]}}}
        }

        # Mock resource details query response
        models_response = {
            "data": {
                "environment": {
                    "applied": {
                        "resources": {
                            "pageInfo": {"hasNextPage": False, "endCursor": None},
                            "edges": [
                                {
                                    "node": {
                                        "name": "customers",
                                        "uniqueId": "model.jaffle_shop.customers",
                                        "resourceType": "Model",
                                    }
                                }
                            ],
                        }
                    }
                }
            }
        }

        # First call is packages, second is resource query
        mock_api_client.execute_query.side_effect = [packages_response, models_response]

        result = await lineage_fetcher.search_resource_by_name("customers", "Model")

        assert len(result) == 1
        assert result[0]["uniqueId"] == "model.jaffle_shop.customers"
        assert result[0]["resourceType"] == "Model"

    async def test_search_resource_by_name_seeds(
        self, lineage_fetcher, mock_api_client
    ):
        """Test searching for seeds using the generic search method."""
        # Mock packages query response
        packages_response = {
            "data": {"environment": {"applied": {"packages": ["jaffle_shop"]}}}
        }

        # Mock resource details query response
        seeds_response = {
            "data": {
                "environment": {
                    "applied": {
                        "resources": {
                            "pageInfo": {"hasNextPage": False, "endCursor": None},
                            "edges": [
                                {
                                    "node": {
                                        "name": "raw_customers",
                                        "uniqueId": "seed.jaffle_shop.raw_customers",
                                        "resourceType": "Seed",
                                    }
                                }
                            ],
                        }
                    }
                }
            }
        }

        # First call is packages, second is resource query
        mock_api_client.execute_query.side_effect = [packages_response, seeds_response]

        result = await lineage_fetcher.search_resource_by_name("raw_customers", "Seed")

        assert len(result) == 1
        assert result[0]["uniqueId"] == "seed.jaffle_shop.raw_customers"
        assert result[0]["resourceType"] == "Seed"

    async def test_search_resource_by_name_snapshots(
        self, lineage_fetcher, mock_api_client
    ):
        """Test searching for snapshots using the generic search method."""
        # Mock packages query response
        packages_response = {
            "data": {"environment": {"applied": {"packages": ["jaffle_shop"]}}}
        }

        # Mock resource details query response
        snapshots_response = {
            "data": {
                "environment": {
                    "applied": {
                        "resources": {
                            "pageInfo": {"hasNextPage": False, "endCursor": None},
                            "edges": [
                                {
                                    "node": {
                                        "name": "orders_snapshot",
                                        "uniqueId": "snapshot.jaffle_shop.orders_snapshot",
                                        "resourceType": "Snapshot",
                                    }
                                }
                            ],
                        }
                    }
                }
            }
        }

        # First call is packages, second is resource query
        mock_api_client.execute_query.side_effect = [
            packages_response,
            snapshots_response,
        ]

        result = await lineage_fetcher.search_resource_by_name(
            "orders_snapshot", "Snapshot"
        )

        assert len(result) == 1
        assert result[0]["uniqueId"] == "snapshot.jaffle_shop.orders_snapshot"
        assert result[0]["resourceType"] == "Snapshot"

    async def test_search_all_resources_all_types_found(
        self, lineage_fetcher, mock_api_client
    ):
        """When model, source, seed, and snapshot all exist with same name."""
        # Mock packages query response (same for all 4 resource types)
        packages_response = {
            "data": {"environment": {"applied": {"packages": ["jaffle_shop"]}}}
        }

        model_response = {
            "data": {
                "environment": {
                    "applied": {
                        "resources": {
                            "pageInfo": {"hasNextPage": False, "endCursor": None},
                            "edges": [
                                {
                                    "node": {
                                        "name": "customers",
                                        "uniqueId": "model.jaffle_shop.customers",
                                        "resourceType": "Model",
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
                        "resources": {
                            "pageInfo": {"hasNextPage": False, "endCursor": None},
                            "edges": [
                                {
                                    "node": {
                                        "name": "customers",
                                        "uniqueId": "source.jaffle_shop.raw.customers",
                                        "resourceType": "Source",
                                    }
                                }
                            ],
                        }
                    }
                }
            }
        }
        seed_response = {
            "data": {
                "environment": {
                    "applied": {
                        "resources": {
                            "pageInfo": {"hasNextPage": False, "endCursor": None},
                            "edges": [
                                {
                                    "node": {
                                        "name": "customers",
                                        "uniqueId": "seed.jaffle_shop.customers",
                                        "resourceType": "Seed",
                                    }
                                }
                            ],
                        }
                    }
                }
            }
        }
        snapshot_response = {
            "data": {
                "environment": {
                    "applied": {
                        "resources": {
                            "pageInfo": {"hasNextPage": False, "endCursor": None},
                            "edges": [
                                {
                                    "node": {
                                        "name": "customers",
                                        "uniqueId": "snapshot.jaffle_shop.customers",
                                        "resourceType": "Snapshot",
                                    }
                                }
                            ],
                        }
                    }
                }
            }
        }

        # search_all_resources calls 4 resource types in parallel
        # Each call needs: packages query + resource query = 8 total queries
        mock_api_client.execute_query.side_effect = [
            packages_response,  # Model packages
            model_response,  # Model resources
            packages_response,  # Source packages
            source_response,  # Source resources
            packages_response,  # Seed packages
            seed_response,  # Seed resources
            packages_response,  # Snapshot packages
            snapshot_response,  # Snapshot resources
        ]

        result = await lineage_fetcher.search_all_resources("customers")

        assert len(result) == 4
        assert result[0]["resourceType"] == "Model"
        assert result[1]["resourceType"] == "Source"
        assert result[2]["resourceType"] == "Seed"
        assert result[3]["resourceType"] == "Snapshot"


class TestBuildSelector:
    """Test dbt selector syntax building."""

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

    def test_invalid_direction_raises_error(self, lineage_fetcher):
        """Should raise ValueError for invalid direction."""
        with pytest.raises(ValueError, match="Invalid direction"):
            lineage_fetcher._build_selector(
                "model.jaffle_shop.customers", "invalid_direction"
            )

    def test_invalid_direction_none(self, lineage_fetcher):
        """Should raise error when direction is None."""
        with pytest.raises((ValueError, TypeError)):
            lineage_fetcher._build_selector("model.jaffle_shop.customers", None)

    def test_invalid_direction_integer(self, lineage_fetcher):
        """Should raise error for integer direction."""
        with pytest.raises((ValueError, TypeError)):
            lineage_fetcher._build_selector("model.jaffle_shop.customers", 123)

    def test_invalid_direction_list(self, lineage_fetcher):
        """Should raise error for list/collection types."""
        with pytest.raises((ValueError, TypeError)):
            lineage_fetcher._build_selector(
                "model.jaffle_shop.customers", ["ancestors"]
            )

    def test_invalid_direction_empty_string(self, lineage_fetcher):
        """Should raise ValueError for empty string."""
        with pytest.raises(ValueError, match="Invalid direction"):
            lineage_fetcher._build_selector("model.jaffle_shop.customers", "")

    def test_invalid_direction_dict(self, lineage_fetcher):
        """Should raise error for dictionary type."""
        with pytest.raises((ValueError, TypeError)):
            lineage_fetcher._build_selector(
                "model.jaffle_shop.customers", {"direction": "ancestors"}
            )


class TestFetchLineage:
    """Test lineage fetching functionality."""

    async def test_fetch_both_directions_separates_correctly(
        self, lineage_fetcher, mock_api_client
    ):
        """Test that 'both' direction makes two API calls and correctly categorizes results."""
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
        assert result["target"]["uniqueId"] == "model.jaffle_shop.stg_orders"

        # Verify ancestors contains upstream seed
        assert len(result["ancestors"]) == 1
        assert result["ancestors"][0]["uniqueId"] == "seed.jaffle_shop.raw_orders"

        # Verify descendants contains downstream model
        assert len(result["descendants"]) == 1
        assert result["descendants"][0]["uniqueId"] == "model.jaffle_shop.orders"

    async def test_invalid_direction_raises_error(self, lineage_fetcher):
        """Should raise ValueError for invalid direction in fetch_lineage."""
        with pytest.raises(ValueError, match="Invalid direction"):
            await lineage_fetcher.fetch_lineage(
                unique_id="model.jaffle_shop.customers", direction="invalid_direction"
            )

    async def test_invalid_direction_none(self, lineage_fetcher):
        """Should raise error when direction is None in fetch_lineage."""
        with pytest.raises((ValueError, TypeError)):
            await lineage_fetcher.fetch_lineage(
                unique_id="model.jaffle_shop.customers", direction=None
            )

    async def test_invalid_direction_integer(self, lineage_fetcher):
        """Should raise error for integer direction in fetch_lineage."""
        with pytest.raises((ValueError, TypeError)):
            await lineage_fetcher.fetch_lineage(
                unique_id="model.jaffle_shop.customers", direction=123
            )

    async def test_invalid_direction_list(self, lineage_fetcher):
        """Should raise error for list direction in fetch_lineage."""
        with pytest.raises((ValueError, TypeError)):
            await lineage_fetcher.fetch_lineage(
                unique_id="model.jaffle_shop.customers", direction=["ancestors"]
            )

    async def test_invalid_direction_empty_string(self, lineage_fetcher):
        """Should raise ValueError for empty string direction in fetch_lineage."""
        with pytest.raises(ValueError, match="Invalid direction"):
            await lineage_fetcher.fetch_lineage(
                unique_id="model.jaffle_shop.customers", direction=""
            )

    async def test_invalid_types_raises_error(self, lineage_fetcher):
        """Should raise ValueError for invalid resource types."""
        with pytest.raises(ValueError, match="Invalid resource type"):
            await lineage_fetcher.fetch_lineage(
                unique_id="model.jaffle_shop.customers",
                types=["Model", "InvalidType", "AnotherBadType"],
            )


class TestPagination:
    """Test pagination and truncation behavior."""

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
        assert result["pagination"]["ancestors_total"] == 60  # Original count
        assert result["pagination"]["ancestors_truncated"] is True

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
        assert result["pagination"]["ancestors_total"] == 70
        assert result["pagination"]["ancestors_truncated"] is True
        assert result["pagination"]["descendants_total"] == 30
        assert result["pagination"]["descendants_truncated"] is False

        # Check actual data is truncated
        assert len(result["ancestors"]) == 50
        assert len(result["descendants"]) == 30
