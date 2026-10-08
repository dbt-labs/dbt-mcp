from typing import Annotated, Any

import pytest
from mcp.server.fastmcp.exceptions import ToolError
from pydantic import Field

from dbt_mcp.tools.binding import InputBinding
from dbt_mcp.tools.definitions import ToolDefinition, dbt_mcp_tool
from dbt_mcp.tools.injection import AdaptError
from dbt_mcp.tools.targets import Permission, ProjectTarget


async def test_call_hook_receives_validated_bound_inputs_before_context_mapping():
    events = []

    @dbt_mcp_tool(title="Models", description="Models")
    async def get_all_models(
        context: str,
        query: str,
        *,
        project_id: Annotated[
            int, ProjectTarget(requires=Permission.METADATA_READ)
        ] = Field(gt=0, description="Project ID."),
        environment_id: int = Field(description="Environment ID."),
        limit: int = Field(default=10, ge=1, le=100),
    ) -> str:
        events.append(("body", context))
        return f"{context}:{query}:{limit}"

    def build_context(prepared: int) -> str:
        events.append(("context", prepared))
        return f"project {prepared}"

    async def prepare_call(inputs: dict[str, Any]) -> int:
        events.append(("hook", inputs))
        return inputs["project_id"]

    definition = get_all_models.adapt_with_mappers(context=build_context)
    hooked = definition.with_call_hook(prepare_call, inject="prepared")
    assert set(hooked.fastmcp_tool.parameters["properties"]) == {
        "project_id",
        "environment_id",
        "query",
        "limit",
    }
    assert hooked.fastmcp_tool.parameters["required"] == [
        "query",
        "project_id",
        "environment_id",
    ]
    assert hooked.targets == get_all_models.targets
    assert "prepared" in definition.fastmcp_tool.parameters["properties"]

    tool = hooked.bind_inputs(
        InputBinding(values={"project_id": 42}, hidden=frozenset({"environment_id"}))
    ).fastmcp_tool
    assert set(tool.parameters["properties"]) == {"query", "limit"}
    for extra in ({"prepared": 42}, {"project_id": 42}, {"limit": 0}):
        with pytest.raises(ToolError):
            await tool.run({"query": "orders", **extra})
    assert events == []
    assert await tool.run({"query": "orders", "limit": "5"}) == "project 42:orders:5"
    assert events == [
        ("hook", {"query": "orders", "limit": 5, "project_id": 42}),
        ("context", 42),
        ("body", "project 42"),
    ]


async def test_call_hook_failure_stops_context_mapping_and_body():
    events = []

    async def get_all_models(context: str, query: str) -> str:
        events.append("body")
        return query

    def build_context(prepared: int) -> str:
        events.append("context")
        return str(prepared)

    async def prepare_call(inputs: dict[str, Any]) -> int:
        events.append("hook")
        raise PermissionError("Access denied")

    tool = (
        ToolDefinition(
            fn=get_all_models, title="Models", description="Models", requirements=()
        )
        .adapt_with_mappers(context=build_context)
        .with_call_hook(prepare_call, inject="prepared")
    )
    with pytest.raises(ToolError, match="Access denied"):
        await tool.fastmcp_tool.run({"query": "orders"})
    assert events == ["hook"]


async def test_call_hook_supports_sync_callables_and_keeps_argument_order():
    def get_all_models(context: str, /, query: str) -> str:
        return f"{context}:{query}"

    def prepare_call(inputs: dict[str, Any]) -> str:
        return "configured"

    tool = ToolDefinition(
        fn=get_all_models, title="Models", description="Models", requirements=()
    )
    hooked = tool.with_call_hook(prepare_call, inject="context")
    assert await hooked.fn("orders") == "configured:orders"
    assert await hooked.fastmcp_tool.run({"query": "orders"}) == "configured:orders"


def test_call_hook_rejects_incompatible_or_unknown_injection():
    def get_all_models(context: int, query: str) -> str:
        return query

    def prepare_call(inputs: dict[str, Any]) -> int | None:
        return None

    tool = ToolDefinition(
        fn=get_all_models, title="Models", description="Models", requirements=()
    )
    with pytest.raises(AdaptError, match="context.*incompatible"):
        tool.with_call_hook(prepare_call, inject="context")
    with pytest.raises(AdaptError, match="Unknown.*missing"):
        tool.with_call_hook(prepare_call, inject="missing")
