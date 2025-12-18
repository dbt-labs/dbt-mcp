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


class TestFetchLineage:
    """Test lineage fetching functionality."""

    async def test_fetch_both_directions_separates_correctly(
        self, lineage_fetcher, mock_api_client
    ):
        """Test that 'both' direction fetches ancestors and descendants recursively."""
        # Mock target node fetch (stg_orders with its parents and children)
        target_response = {
            "data": {
                "environment": {
                    "applied": {
                        "models": {
                            "edges": [
                                {
                                    "node": {
                                        "uniqueId": "model.jaffle_shop.stg_orders",
                                        "name": "stg_orders",
                                        "resourceType": "Model",
                                        "filePath": "models/staging/stg_orders.sql",
                                        "parents": [
                                            {
                                                "uniqueId": "seed.jaffle_shop.raw_orders",
                                                "name": "raw_orders",
                                                "resourceType": "Seed",
                                            }
                                        ],
                                        "children": [
                                            {
                                                "uniqueId": "model.jaffle_shop.orders",
                                                "name": "orders",
                                                "resourceType": "Model",
                                            }
                                        ],
                                    }
                                }
                            ]
                        }
                    }
                }
            }
        }

        # Mock child node fetch (orders with no more children)
        orders_response = {
            "data": {
                "environment": {
                    "applied": {
                        "models": {
                            "edges": [
                                {
                                    "node": {
                                        "uniqueId": "model.jaffle_shop.orders",
                                        "name": "orders",
                                        "resourceType": "Model",
                                        "parents": [
                                            {
                                                "uniqueId": "model.jaffle_shop.stg_orders",
                                                "name": "stg_orders",
                                                "resourceType": "Model",
                                            }
                                        ],
                                        "children": [],
                                    }
                                }
                            ]
                        }
                    }
                }
            }
        }

        # Mock calls:
        # 1. Initial target fetch
        # 2. Ancestor traversal (fetches target again)
        # 3. Descendant traversal (fetches target again)
        # 4. Descendant child traversal (fetches orders)
        mock_api_client.execute_query.side_effect = [
            target_response,  # Initial fetch
            target_response,  # For ancestor/descendant traversal
            target_response,  # For descendant/ancestor traversal
            orders_response,  # For descendant child (orders)
        ]

        result = await lineage_fetcher.fetch_lineage(
            unique_id="model.jaffle_shop.stg_orders",
            types=[],
            direction=LineageDirection.BOTH,
        )

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
                unique_id="model.jaffle_shop.customers",
                types=[],
                direction="invalid_direction",
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
                unique_id="model.jaffle_shop.customers", types=[], direction=""
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
        # Create target with 60 seed parents (seeds don't recurse, giving exactly 60 ancestors)
        target_parents = [
            {
                "uniqueId": f"seed.jaffle_shop.ancestor_{i}",
                "name": f"ancestor_{i}",
                "resourceType": "Seed",
            }
            for i in range(60)
        ]

        target_response = {
            "data": {
                "environment": {
                    "applied": {
                        "models": {
                            "edges": [
                                {
                                    "node": {
                                        "uniqueId": "model.jaffle_shop.target",
                                        "name": "target",
                                        "resourceType": "Model",
                                        "parents": target_parents,
                                        "children": [],
                                    }
                                }
                            ]
                        }
                    }
                }
            }
        }

        # Mock calls: initial fetch + ancestor traversal
        mock_api_client.execute_query.side_effect = [
            target_response,  # Initial fetch
            target_response,  # Ancestor traversal
        ]

        result = await lineage_fetcher.fetch_lineage(
            unique_id="model.jaffle_shop.target",
            types=[],
            direction=LineageDirection.ANCESTORS,
        )

        assert len(result["ancestors"]) == 50  # Truncated to limit
        assert result["pagination"]["ancestors_total"] == 60  # Original count
        assert result["pagination"]["ancestors_truncated"] is True

    async def test_both_directions_merges_pagination(
        self, lineage_fetcher, mock_api_client
    ):
        """Both direction should merge pagination from ancestors and descendants."""
        # Target with 70 seed parents (ancestors) and 30 model children (descendants)
        target_parents = [
            {
                "uniqueId": f"seed.jaffle_shop.ancestor_{i}",
                "name": f"ancestor_{i}",
                "resourceType": "Seed",
            }
            for i in range(70)
        ]

        target_children = [
            {
                "uniqueId": f"model.jaffle_shop.descendant_{i}",
                "name": f"descendant_{i}",
                "resourceType": "Model",
            }
            for i in range(30)
        ]

        target_response = {
            "data": {
                "environment": {
                    "applied": {
                        "models": {
                            "edges": [
                                {
                                    "node": {
                                        "uniqueId": "model.jaffle_shop.target",
                                        "name": "target",
                                        "resourceType": "Model",
                                        "parents": target_parents,
                                        "children": target_children,
                                    }
                                }
                            ]
                        }
                    }
                }
            }
        }

        # Response for each descendant child (no more children)
        child_response = {
            "data": {
                "environment": {
                    "applied": {
                        "models": {
                            "edges": [
                                {
                                    "node": {
                                        "uniqueId": "model.jaffle_shop.descendant_0",
                                        "name": "descendant_0",
                                        "resourceType": "Model",
                                        "parents": [],
                                        "children": [],
                                    }
                                }
                            ]
                        }
                    }
                }
            }
        }

        # Mock calls: initial + ancestor traversal + descendant traversal + 30 child fetches
        mock_responses = [
            target_response,  # Initial fetch
            target_response,  # Ancestor traversal
            target_response,  # Descendant traversal
        ] + [child_response] * 30  # Each child has no more children

        mock_api_client.execute_query.side_effect = mock_responses

        result = await lineage_fetcher.fetch_lineage(
            unique_id="model.jaffle_shop.target",
            types=[],
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
