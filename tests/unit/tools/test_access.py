from enum import Enum

import pytest

from dbt_mcp.discovery.tools import DISCOVERY_TOOLS
from dbt_mcp.discovery.tools_multiproject import MULTIPROJECT_DISCOVERY_TOOLS
from dbt_mcp.semantic_layer.tools import SEMANTIC_LAYER_TOOLS
from dbt_mcp.tools.access import ToolAccess, ToolPermission, ToolTarget
from dbt_mcp.tools.definitions import (
    GenericToolDefinition,
    ToolDefinition,
    generic_dbt_mcp_tool,
)


class ExampleName(Enum):
    EXAMPLE = "example"


def test_decorator_requires_access() -> None:
    with pytest.raises(TypeError, match="access"):
        generic_dbt_mcp_tool(  # type: ignore[call-arg]
            name_enum=ExampleName, title="Example", description="Example"
        )


@pytest.mark.parametrize("definition_type", [GenericToolDefinition, ToolDefinition])
def test_direct_definition_requires_access(definition_type: type) -> None:
    with pytest.raises(TypeError, match="access"):
        definition_type(
            fn=lambda: "example",
            title="Example",
            description="Example",
            name_enum=ExampleName,
        )


def test_context_adaptation_preserves_access_and_public_schema() -> None:
    access = ToolAccess(
        target=ToolTarget.PRODUCTION_ENVIRONMENT,
        permissions=(ToolPermission.METADATA_READ,),
    )

    @generic_dbt_mcp_tool(
        name_enum=ExampleName, title="Example", description="Example", access=access
    )
    async def example(context: int, query: str) -> str:
        return query

    def mapper() -> int:
        return 1

    adapted = example.adapt_context(mapper)
    assert adapted.access == access
    tool = adapted.to_fastmcp_internal_tool()
    assert tool.parameters["properties"] == {
        "query": {"title": "Query", "type": "string"}
    }
    assert tool.parameters["required"] == ["query"]


def test_shared_service_tools_declare_their_target_and_permission() -> None:
    for tool in [*DISCOVERY_TOOLS, *MULTIPROJECT_DISCOVERY_TOOLS]:
        assert tool.access == ToolAccess(
            target=ToolTarget.PRODUCTION_ENVIRONMENT,
            permissions=(ToolPermission.METADATA_READ,),
        )
    for tool in SEMANTIC_LAYER_TOOLS:
        assert tool.access == ToolAccess(
            target=ToolTarget.PRODUCTION_ENVIRONMENT,
            permissions=(ToolPermission.SEMANTIC_LAYER_CONFIGURATION_READ,),
        )
