from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from dbt_mcp.discovery.client import (
    MetadataAPIClient,
    PaginatedResourceFetcher,
    ResourceDetailsFetcher,
)
from dbt_mcp.discovery.tools import get_resource_details
from dbt_mcp.errors import InvalidParameterError


def _make_applied_resources_response(node: dict) -> dict:
    return {
        "data": {
            "environment": {
                "applied": {
                    "resources": {
                        "pageInfo": {"hasNextPage": False, "endCursor": None},
                        "edges": [{"node": node}],
                    }
                }
            }
        }
    }


@pytest.fixture
def mock_api_client():
    mock_client = Mock(spec=MetadataAPIClient)
    mock_config = Mock()
    mock_config.environment_id = 999

    async def mock_get_config():
        return mock_config

    mock_config_provider = Mock()
    mock_config_provider.get_config = mock_get_config

    mock_client.config_provider = mock_config_provider
    mock_client.execute_query = AsyncMock()
    return mock_client


@pytest.fixture
def resource_details_fetcher(
    mock_api_client: MetadataAPIClient,
) -> ResourceDetailsFetcher:
    paginator = PaginatedResourceFetcher(
        mock_api_client,
        edges_path=("data", "environment", "applied", "resources", "edges"),
        page_info_path=("data", "environment", "applied", "resources", "pageInfo"),
    )
    return ResourceDetailsFetcher(paginator=paginator)


RESOURCE_CASES = [
    (
        "model",
        "AppliedModelResources",
        {
            "__typename": "ModelAppliedStateNode",
            "resourceType": "model",
            "uniqueId": "model.analytics.stg_orders",
            "name": "stg_orders",
            "description": "Orders staging model",
            "compiledCode": "select * from src_orders",
            "database": "analytics",
            "schema": "staging",
            "alias": "stg_orders",
            "catalog": {
                "columns": [
                    {
                        "name": "order_id",
                        "type": "integer",
                        "description": "Primary key",
                    },
                    {"name": "order_date", "type": "timestamp", "description": None},
                ]
            },
        },
        {"compiledCode", "catalog"},
        "model",
    ),
    (
        "source",
        "AppliedSourceResources",
        {
            "__typename": "SourceAppliedStateNode",
            "resourceType": "source",
            "uniqueId": "source.analytics.raw_orders.orders",
            "name": "orders",
            "description": "Raw orders source",
            "sourceName": "raw_orders",
            "identifier": "orders",
            "database": "raw",
            "schema": "orders",
            "freshness": {
                "maxLoadedAt": "2024-01-01T00:00:00Z",
                "maxLoadedAtTimeAgoInS": 1800,
                "freshnessStatus": "pass",
            },
            "catalog": {
                "columns": [
                    {"name": "order_id", "type": "integer", "description": None},
                ]
            },
        },
        {"freshness", "catalog"},
        "source",
    ),
    (
        "exposure",
        "AppliedExposureResources",
        {
            "__typename": "ExposureDefinitionNode",
            "resourceType": "exposure",
            "uniqueId": "exposure.analytics.dashboard.orders_overview",
            "name": "orders_overview",
            "description": "Dashboard exposure",
            "exposureType": "dashboard",
            "maturity": "high",
            "label": "Orders Overview",
            "ownerEmail": "owner@example.com",
            "ownerName": "Data Owner",
            "url": "https://example.com/dashboard",
            "meta": {"team": "analytics"},
        },
        {"exposureType", "ownerEmail"},
        "exposure",
    ),
    (
        "test",
        "AppliedTestResources",
        {
            "__typename": "TestAppliedStateNode",
            "resourceType": "test",
            "uniqueId": "test.analytics.orders.unique_order_id",
            "name": "unique_order_id",
            "description": "Ensures order_id is unique",
            "columnName": "order_id",
            "testType": "unique",
            "dependsOnMacros": [
                {"uniqueId": "macro.analytics.test_unique", "name": "test_unique"}
            ],
            "meta": {"severity": "error"},
        },
        {"columnName", "dependsOnMacros"},
        "test",
    ),
    (
        "seed",
        "AppliedSeedResources",
        {
            "__typename": "SeedAppliedStateNode",
            "resourceType": "seed",
            "uniqueId": "seed.analytics.country_codes",
            "name": "country_codes",
            "description": "Country codes seed",
            "database": "analytics",
            "schema": "seeds",
            "alias": "country_codes",
            "path": "seeds/country_codes.csv",
            "compiledCode": "select * from country_codes",
            "catalog": {
                "columns": [
                    {"name": "country_code", "type": "string", "description": None},
                ]
            },
        },
        {"path", "catalog"},
        "seed",
    ),
    (
        "snapshot",
        "AppliedSnapshotResources",
        {
            "__typename": "SnapshotAppliedStateNode",
            "resourceType": "snapshot",
            "uniqueId": "snapshot.analytics.orders_snapshot",
            "name": "orders_snapshot",
            "description": "Snapshot of orders",
            "database": "analytics",
            "schema": "snapshots",
            "targetDatabase": "analytics_prod",
            "targetSchema": "snapshots_prod",
            "strategy": "timestamp",
            "primaryKey": ["order_id"],
            "snapshotExecutionInfo": {
                "lastRunStatus": "success",
                "executeCompletedAt": "2024-01-10T00:00:00Z",
                "executeStartedAt": "2024-01-10T00:03:00Z",
            },
        },
        {"strategy", "snapshotExecutionInfo"},
        "snapshot",
    ),
    (
        "macro",
        "AppliedMacroResources",
        {
            "__typename": "MacroDefinitionNode",
            "uniqueId": "macro.analytics.generate_calendar",
            "name": "generate_calendar",
            "description": "Generates a calendar table",
            "path": "macros/generate_calendar.sql",
            "macroSql": "{% macro generate_calendar() %} ... {% endmacro %}",
        },
        {"macroSql"},
        "macro",
    ),
    (
        "semantic_model",
        "AppliedSemanticModelResources",
        {
            "__typename": "SemanticModelDefinitionNode",
            "uniqueId": "semantic_model.analytics.orders",
            "name": "orders",
            "description": "Semantic model for orders",
            "defaults": {"aggTimeDimension": "order_date"},
            "entities": [
                {"name": "order_id", "type": "primary", "description": None},
            ],
            "dimensions": [
                {
                    "name": "order_date",
                    "type": "time",
                    "description": "Order date",
                    "typeParams": {"timeGranularity": "day"},
                },
            ],
            "measures": [
                {
                    "name": "order_count",
                    "expr": "count(*)",
                    "description": None,
                    "agg": "sum",
                },
            ],
        },
        {"dimensions", "measures"},
        "semantic_model",
    ),
]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "resource_type,query_name,node_payload,query_fields,expected_type",
    RESOURCE_CASES,
)
async def test_fetch_resource_details_per_type(
    resource_details_fetcher: ResourceDetailsFetcher,
    mock_api_client: MetadataAPIClient,
    resource_type: str,
    query_name: str,
    node_payload: dict,
    query_fields: set[str],
    expected_type: str,
) -> None:
    mock_api_client.execute_query.reset_mock()
    mock_api_client.execute_query.return_value = _make_applied_resources_response(
        node_payload
    )

    results = await resource_details_fetcher.fetch_details(
        resource_type, unique_ids=[node_payload["uniqueId"]]
    )

    assert mock_api_client.execute_query.await_count == 1
    executed_call = mock_api_client.execute_query.await_args_list[0]
    executed_query = executed_call.args[0]
    variables = executed_call.args[1]
    assert query_name in executed_query
    for field in query_fields:
        assert field in executed_query
    assert variables["filter"] == {"uniqueIds": [node_payload["uniqueId"]]}
    assert variables["environmentId"] == 999

    assert len(results) == 1
    normalized = results[0]
    assert normalized["resourceType"] == expected_type
    assert "__typename" not in normalized
    for field in query_fields:
        assert field in normalized
    assert normalized["uniqueId"] == node_payload["uniqueId"]


@pytest.mark.asyncio
async def test_fetch_resource_details_with_unique_id_parameter(
    resource_details_fetcher: ResourceDetailsFetcher,
    mock_api_client: MetadataAPIClient,
) -> None:
    node_payload = {
        "__typename": "ModelAppliedStateNode",
        "resourceType": "model",
        "uniqueId": "model.analytics.stg_payments",
        "name": "stg_payments",
    }
    mock_api_client.execute_query.return_value = _make_applied_resources_response(
        node_payload
    )

    results = await resource_details_fetcher.fetch_details(
        "model",
        unique_id="model.analytics.stg_payments",
    )

    assert results[0]["uniqueId"] == "model.analytics.stg_payments"
    executed_call = mock_api_client.execute_query.await_args_list[0]
    call_vars = executed_call.args[1]
    assert call_vars["filter"] == {"uniqueIds": ["model.analytics.stg_payments"]}


@pytest.mark.asyncio
async def test_fetch_resource_details_requires_unique_ids(
    resource_details_fetcher: ResourceDetailsFetcher,
) -> None:
    with pytest.raises(InvalidParameterError):
        await resource_details_fetcher.fetch_details("model")


@pytest.mark.asyncio
async def test_fetch_resource_details_rejects_unknown_type(
    resource_details_fetcher: ResourceDetailsFetcher,
) -> None:
    with pytest.raises(InvalidParameterError):
        await resource_details_fetcher.fetch_details(
            "invalid_type", unique_ids=["model.analytics.stg_orders"]
        )


@pytest.mark.asyncio
async def test_fetch_resource_details_filters_mismatched_types(
    resource_details_fetcher: ResourceDetailsFetcher,
    mock_api_client: MetadataAPIClient,
) -> None:
    mismatched_node = {
        "__typename": "SourceAppliedStateNode",
        "resourceType": "source",
        "uniqueId": "source.analytics.raw.orders",
        "name": "orders",
    }
    mock_api_client.execute_query.return_value = _make_applied_resources_response(
        mismatched_node
    )

    results = await resource_details_fetcher.fetch_details(
        "model", unique_ids=[mismatched_node["uniqueId"]]
    )
    assert results == []


@pytest.mark.asyncio
async def test_get_resource_details_tool_delegates_to_fetcher() -> None:
    fetcher = AsyncMock()
    fetcher.fetch_details.return_value = [{"resourceType": "model"}]
    context = SimpleNamespace(resource_details_fetcher=fetcher)

    result = await get_resource_details(
        context,
        resource_type="model",
        unique_id="model.analytics.stg_orders",
    )

    fetcher.fetch_details.assert_awaited_once_with(
        resource_type="model",
        unique_ids=None,
        unique_id="model.analytics.stg_orders",
    )
    assert result == [{"resourceType": "model"}]
