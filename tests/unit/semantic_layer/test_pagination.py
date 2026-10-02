from unittest.mock import AsyncMock, Mock, patch

import pytest

from dbt_mcp.config.config_providers import SemanticLayerConfig
from dbt_mcp.errors import InvalidParameterError
from dbt_mcp.semantic_layer.client import SemanticLayerFetcher


COLLECTIONS: list[tuple[str, str, dict[str, list[str]]]] = [
    ("list_metrics", "metricsPaginated", {}),
    ("list_saved_queries", "savedQueriesPaginated", {}),
    ("get_dimensions", "dimensionsPaginated", {"metrics": []}),
    ("get_entities", "entitiesPaginated", {"metrics": []}),
]


@pytest.mark.parametrize("method_name,field,arguments", COLLECTIONS)
@pytest.mark.parametrize("page_num", [0, -1, True, False, 1.5, "2", None])
async def test_collection_rejects_invalid_page_numbers(
    method_name, field, arguments, page_num
):
    fetcher = SemanticLayerFetcher(client_provider=Mock())
    with patch(
        "dbt_mcp.semantic_layer.client.submit_request", new_callable=AsyncMock
    ) as request:
        with pytest.raises(InvalidParameterError, match="page_num"):
            await getattr(fetcher, method_name)(
                config=Mock(spec=SemanticLayerConfig), page_num=page_num, **arguments
            )

    request.assert_not_awaited()


@pytest.mark.parametrize("method_name,field,arguments", COLLECTIONS)
async def test_collection_preserves_one_based_page_number(
    method_name, field, arguments
):
    fetcher = SemanticLayerFetcher(client_provider=Mock())
    response = {
        "data": {field: {"items": [], "pageNum": 2, "totalPages": 3, "totalItems": 30}}
    }
    with patch(
        "dbt_mcp.semantic_layer.client.submit_request", new_callable=AsyncMock
    ) as request:
        request.return_value = response
        result = await getattr(fetcher, method_name)(
            config=Mock(spec=SemanticLayerConfig), page_num=2, page_size=10, **arguments
        )

    assert request.await_args.args[1]["variables"]["pageNum"] == 2
    assert request.await_args.args[1]["variables"]["pageSize"] == 10
    assert result.pagination.has_more
    assert result.pagination.next_page == 3
