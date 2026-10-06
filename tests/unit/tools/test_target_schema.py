from unittest.mock import AsyncMock, MagicMock

import pytest

from dbt_mcp.config.config_providers.base import DiscoveryConfig, StaticConfigProvider
from dbt_mcp.discovery.tools import (
    DISCOVERY_TOOLS,
    DiscoveryToolContext,
    get_all_models,
)
from dbt_mcp.semantic_layer.tools import (
    SEMANTIC_LAYER_TOOLS,
    SemanticLayerToolContext,
    list_metrics,
)
from dbt_mcp.semantic_layer.types import ListMetricsResponse
from dbt_mcp.tools.targets import ProjectTarget, target_parameters


def test_canonical_service_schemas_explicitly_declare_target_parameters() -> None:
    for tool in DISCOVERY_TOOLS + SEMANTIC_LAYER_TOOLS:
        declarations = target_parameters(tool.fn)
        assert set(declarations) == {"project_id"}
        assert isinstance(declarations["project_id"], ProjectTarget)
        schema = (
            tool.bind_arguments(context=MagicMock())
            .to_fastmcp_internal_tool()
            .parameters
        )
        assert "project_id" in schema["required"]
        assert schema["properties"]["project_id"]["type"] == "integer"


async def test_semantic_layer_uses_the_bound_configuration(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = MagicMock(metrics_related_max=10, max_response_chars=16000)
    context = SemanticLayerToolContext(StaticConfigProvider(config), MagicMock())
    fetch = AsyncMock(return_value=ListMetricsResponse(metrics=[]))
    monkeypatch.setattr(context.semantic_layer_fetcher, "list_metrics", fetch)
    await list_metrics.fn(context=context, project_id=20)
    assert fetch.call_args.kwargs["config"] is config


async def test_discovery_uses_the_bound_configuration(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = DiscoveryConfig(
        url="https://example.com", headers_provider=MagicMock(), environment_id=12
    )
    context = DiscoveryToolContext(StaticConfigProvider(config))
    fetch = AsyncMock(return_value=[{"name": "orders"}])
    monkeypatch.setattr(context.models_fetcher, "fetch_models", fetch)
    result = await get_all_models.fn(context=context, project_id=20)
    assert result == [{"name": "orders"}]
    assert fetch.call_args.kwargs["config"] is config
