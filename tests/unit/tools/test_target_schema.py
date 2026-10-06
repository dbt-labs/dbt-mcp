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
from dbt_mcp.tools.targets import EnvironmentRole, ProjectTarget


def test_canonical_service_schemas_explicitly_declare_target_parameters() -> None:
    def context(project_id: int) -> DiscoveryToolContext:
        return MagicMock()

    def sl_context(project_id: int) -> SemanticLayerToolContext:
        return MagicMock()

    for tool in DISCOVERY_TOOLS + SEMANTIC_LAYER_TOOLS:
        declarations = tool.targets
        assert set(declarations) == {"project_id"}
        assert isinstance(declarations["project_id"], ProjectTarget)
        assert declarations["project_id"].environment == EnvironmentRole.PRODUCTION
        schema = (
            tool.adapt_with_mappers(
                context=context if tool in DISCOVERY_TOOLS else sl_context
            )
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
    await list_metrics.fn(context=context)
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
    result = await get_all_models.fn(context=context)
    assert result == [{"name": "orders"}]
    assert fetch.call_args.kwargs["config"] is config
