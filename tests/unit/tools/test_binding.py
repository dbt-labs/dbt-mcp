import pytest

from dbt_mcp.tools.definitions import ToolDefinition
from dbt_mcp.tools.injection import AdaptError


async def selected_project(project_id: int, query: str) -> str:
    return f"{project_id}:{query}"


def selected_project_id() -> int:
    return 42


def definition() -> ToolDefinition:
    return ToolDefinition(
        fn=selected_project,
        name="get_all_models",
        title="Models",
        description="Models",
        requirements=(),
    )


async def test_binding_hides_and_injects_an_argument_without_mutating_definition() -> (
    None
):
    tool = definition()
    bound = tool.adapt_with_mappers(project_id=selected_project_id)
    assert await bound.fn(query="orders") == "42:orders"
    assert bound.to_fastmcp_internal_tool().parameters["required"] == ["query"]
    assert "project_id" not in bound.to_fastmcp_internal_tool().parameters["properties"]
    assert "project_id" in tool.to_fastmcp_internal_tool().parameters["properties"]

    def other_project_id() -> int:
        return 10

    assert (
        await tool.adapt_with_mappers(project_id=other_project_id).fn(query="orders")
        == "10:orders"
    )


async def test_binding_rejects_conflicting_direct_arguments() -> None:
    with pytest.raises(TypeError, match="project_id"):
        await (
            definition()
            .adapt_with_mappers(project_id=selected_project_id)
            .fn(project_id=10, query="orders")
        )


def test_binding_rejects_unknown_arguments_at_registration() -> None:
    with pytest.raises(AdaptError, match="environment_id"):
        definition().adapt_with_mappers(environment_id=selected_project_id)
