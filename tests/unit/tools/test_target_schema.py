import inspect
from unittest.mock import AsyncMock, MagicMock

import pytest

from dbt_mcp.config.config_providers.base import DiscoveryConfig, StaticConfigProvider
from dbt_mcp.discovery.tools import (
    DISCOVERY_TOOLS,
    DiscoveryToolContext,
    get_all_models,
)
from dbt_mcp.semantic_layer.tools import SEMANTIC_LAYER_TOOLS
from dbt_mcp.tools.targets import EnvironmentTarget, ProjectTarget, target_parameters


def test_canonical_service_schemas_explicitly_declare_target_parameters() -> None:
    for tool in [*DISCOVERY_TOOLS, *SEMANTIC_LAYER_TOOLS]:
        declarations = target_parameters(tool.fn)
        assert isinstance(declarations["project_id"], ProjectTarget)
        assert isinstance(declarations["environment_id"], EnvironmentTarget)
        assert (
            inspect.signature(tool.fn).parameters["environment_id"].default
            is inspect.Parameter.empty
        )


async def test_discovery_uses_the_resolved_environment_argument(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = DiscoveryConfig(
        url="https://example.com", headers_provider=MagicMock(), environment_id=12
    )
    context = DiscoveryToolContext(StaticConfigProvider(config))
    fetch = AsyncMock(return_value=[{"name": "orders"}])
    monkeypatch.setattr(context.models_fetcher, "fetch_models", fetch)
    result = await get_all_models.fn(context=context, environment_id=22, project_id=20)
    assert result == [{"name": "orders"}]
    assert fetch.call_args.kwargs["config"].environment_id == 22
    assert config.environment_id == 12
