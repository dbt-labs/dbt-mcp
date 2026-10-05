from enum import Enum

import pytest

from dbt_mcp.discovery.tools import DISCOVERY_TOOLS
from dbt_mcp.semantic_layer.tools import SEMANTIC_LAYER_TOOLS
from dbt_mcp.tools.access import AccessPolicy
from dbt_mcp.tools.definitions import (
    GenericToolDefinition,
    ToolDefinition,
    generic_dbt_mcp_tool,
)


class ExampleName(Enum):
    EXAMPLE = "example"


def test_shared_tools_declare_named_access_policies() -> None:
    for tool in [
        *DISCOVERY_TOOLS,
        *SEMANTIC_LAYER_TOOLS,
    ]:
        assert isinstance(tool.access, Enum)


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
    access = AccessPolicy.PRODUCTION_METADATA_READ

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


def test_shared_service_tools_declare_their_access_policy() -> None:
    for tool in DISCOVERY_TOOLS:
        assert tool.access == AccessPolicy.PRODUCTION_METADATA_READ
    for tool in SEMANTIC_LAYER_TOOLS:
        assert tool.access == AccessPolicy.PRODUCTION_SEMANTIC_LAYER_CONFIGURATION_READ


def test_host_defined_policy_is_preserved_through_context_adaptation() -> None:
    class HostPolicy(Enum):
        EXAMPLE = "example"

    @generic_dbt_mcp_tool(
        name_enum=ExampleName,
        title="Example",
        description="Example",
        access=HostPolicy.EXAMPLE,
    )
    async def example(context: int, query: str) -> str:
        return query

    def mapper() -> int:
        return 1

    adapted = example.adapt_context(mapper)
    assert adapted.access is HostPolicy.EXAMPLE
    assert adapted.to_fastmcp_internal_tool().parameters["required"] == ["query"]
