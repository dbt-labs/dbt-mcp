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

        # Batch response for ancestor/descendant level 0 (target node)
        batch_target_response = {
            "data": {
                "environment": {
                    "applied": {
                        "batch_model": {
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

        # Batch response for descendant child (orders with no more children)
        batch_orders_response = {
            "data": {
                "environment": {
                    "applied": {
                        "batch_model": {
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
        # 2. Ancestor traversal level 0 (batch fetch target)
        # 3. Descendant traversal level 0 (batch fetch target)
        # 4. Descendant traversal level 1 (batch fetch orders)
        mock_api_client.execute_query.side_effect = [
            target_response,  # Initial fetch
            batch_target_response,  # Ancestor traversal level 0
            batch_target_response,  # Descendant traversal level 0
            batch_orders_response,  # Descendant traversal level 1
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

        # Batch response for ancestor traversal level 0 (target with 60 parents)
        batch_target_response = {
            "data": {
                "environment": {
                    "applied": {
                        "batch_model": {
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

        # Mock calls: initial fetch + ancestor traversal level 0
        mock_api_client.execute_query.side_effect = [
            target_response,  # Initial fetch
            batch_target_response,  # Ancestor traversal level 0
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

        # Batch response for ancestor/descendant traversal level 0 (target)
        batch_target_response = {
            "data": {
                "environment": {
                    "applied": {
                        "batch_model": {
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

        # Batch response for all 30 descendant children (no more children)
        batch_children_edges = [
            {
                "node": {
                    "uniqueId": f"model.jaffle_shop.descendant_{i}",
                    "name": f"descendant_{i}",
                    "resourceType": "Model",
                    "parents": [],
                    "children": [],
                }
            }
            for i in range(30)
        ]

        batch_children_response = {
            "data": {
                "environment": {
                    "applied": {"batch_model": {"edges": batch_children_edges}}
                }
            }
        }

        # Mock calls: initial + ancestor traversal level 0 + descendant traversal level 0 + descendant level 1 (batch 30 children)
        mock_responses = [
            target_response,  # Initial fetch
            batch_target_response,  # Ancestor traversal level 0
            batch_target_response,  # Descendant traversal level 0
            batch_children_response,  # Descendant traversal level 1 (all 30 children in one batch)
        ]

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


class TestBatchedLineage:
    """Test batched lineage fetching optimization."""

    def test_build_batch_node_query_single_type(self, lineage_fetcher):
        """Test query generation for single resource type."""
        batch_specs = {"Model": ["model.x.a", "model.x.b"]}
        query = lineage_fetcher._build_batch_node_query(batch_specs)

        assert "batch_model" in query
        assert "model.x.a" in query
        assert "model.x.b" in query
        assert "BatchNodeRelations" in query
        assert "$environmentId: BigInt!" in query

    def test_build_batch_node_query_mixed_types(self, lineage_fetcher):
        """Test query with multiple resource types."""
        batch_specs = {
            "Model": ["model.x.a"],
            "Source": ["source.x.b"],
            "Seed": ["seed.x.c"],
        }
        query = lineage_fetcher._build_batch_node_query(batch_specs)

        assert "batch_model" in query
        assert "batch_source" in query
        assert "batch_seed" in query
        assert "model.x.a" in query
        assert "source.x.b" in query
        assert "seed.x.c" in query

    def test_parse_batch_response(self, lineage_fetcher):
        """Test parsing batched response."""
        response = {
            "data": {
                "environment": {
                    "applied": {
                        "batch_model": {
                            "edges": [
                                {
                                    "node": {
                                        "uniqueId": "model.x.a",
                                        "name": "a",
                                        "children": [],
                                    }
                                },
                                {
                                    "node": {
                                        "uniqueId": "model.x.b",
                                        "name": "b",
                                        "children": [],
                                    }
                                },
                            ]
                        },
                        "batch_source": {
                            "edges": [
                                {
                                    "node": {
                                        "uniqueId": "source.x.c",
                                        "name": "c",
                                        "children": [],
                                    }
                                },
                            ]
                        },
                    }
                }
            }
        }

        nodes = lineage_fetcher._parse_batch_response(response)

        assert len(nodes) == 3
        assert "model.x.a" in nodes
        assert "model.x.b" in nodes
        assert "source.x.c" in nodes
        assert nodes["model.x.a"]["name"] == "a"
        assert nodes["source.x.c"]["name"] == "c"

    def test_parse_batch_response_filters_empty_children(self, lineage_fetcher):
        """Test that empty children are filtered out."""
        response = {
            "data": {
                "environment": {
                    "applied": {
                        "batch_model": {
                            "edges": [
                                {
                                    "node": {
                                        "uniqueId": "model.x.a",
                                        "name": "a",
                                        "children": [
                                            {"uniqueId": "model.x.b", "name": "b"},
                                            {"uniqueId": "", "name": ""},  # Empty
                                        ],
                                    }
                                }
                            ]
                        }
                    }
                }
            }
        }

        nodes = lineage_fetcher._parse_batch_response(response)

        assert len(nodes) == 1
        assert "model.x.a" in nodes
        # Empty children should be filtered out
        assert len(nodes["model.x.a"]["children"]) == 1
        assert nodes["model.x.a"]["children"][0]["uniqueId"] == "model.x.b"

    async def test_batching_reduces_api_calls(self, lineage_fetcher, mock_api_client):
        """Verify batching reduces API calls."""
        # Mock: target node with 3 seed parents
        target_response = {
            "data": {
                "environment": {
                    "applied": {
                        "models": {
                            "edges": [
                                {
                                    "node": {
                                        "uniqueId": "model.x.target",
                                        "name": "target",
                                        "resourceType": "Model",
                                        "parents": [
                                            {
                                                "uniqueId": "seed.x.p1",
                                                "name": "p1",
                                                "resourceType": "Seed",
                                            },
                                            {
                                                "uniqueId": "seed.x.p2",
                                                "name": "p2",
                                                "resourceType": "Seed",
                                            },
                                            {
                                                "uniqueId": "seed.x.p3",
                                                "name": "p3",
                                                "resourceType": "Seed",
                                            },
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

        mock_api_client.execute_query.side_effect = [
            target_response,  # Initial target fetch (target reused at depth 0)
        ]

        result = await lineage_fetcher.fetch_lineage(
            unique_id="model.x.target",
            types=[],
            direction=LineageDirection.ANCESTORS,
        )

        # Optimization: Only 1 call! (initial fetch, target reused at depth 0)
        # Old: initial + batch target at depth 0 = 2 calls
        # Optimized: initial only = 1 call (target reused)
        # Seeds don't recurse, so no additional calls needed
        assert mock_api_client.execute_query.call_count == 1
        assert len(result["ancestors"]) == 3

    async def test_batch_descendants_reduces_calls(
        self, lineage_fetcher, mock_api_client
    ):
        """Verify batching reduces API calls for descendants."""
        # Mock: target node with 4 model children
        target_children = [
            {
                "uniqueId": f"model.x.child_{i}",
                "name": f"child_{i}",
                "resourceType": "Model",
            }
            for i in range(4)
        ]

        target_response = {
            "data": {
                "environment": {
                    "applied": {
                        "models": {
                            "edges": [
                                {
                                    "node": {
                                        "uniqueId": "model.x.target",
                                        "name": "target",
                                        "resourceType": "Model",
                                        "parents": [],
                                        "children": target_children,
                                    }
                                }
                            ]
                        }
                    }
                }
            }
        }

        # Batch response for all 4 children (no more children)
        batch_children_response = {
            "data": {
                "environment": {
                    "applied": {
                        "batch_model": {
                            "edges": [
                                {
                                    "node": {
                                        "uniqueId": f"model.x.child_{i}",
                                        "name": f"child_{i}",
                                        "resourceType": "Model",
                                        "parents": [],
                                        "children": [],
                                    }
                                }
                                for i in range(4)
                            ]
                        }
                    }
                }
            }
        }

        mock_api_client.execute_query.side_effect = [
            target_response,  # Initial fetch (target reused at depth 0)
            batch_children_response,  # Descendant level 1 (batch all 4 children)
        ]

        result = await lineage_fetcher.fetch_lineage(
            unique_id="model.x.target",
            types=[],
            direction=LineageDirection.DESCENDANTS,
        )

        # Optimized with target reuse:
        # Call 1: Initial fetch (target with children)
        # Call 2: Batch fetch level 1 (all 4 children)
        # Total: 2 calls (vs 6 without batching: initial + target + 4 individual)
        # vs 3 calls without optimization: initial + batch target + batch children
        assert mock_api_client.execute_query.call_count == 2
        assert len(result["descendants"]) == 4
