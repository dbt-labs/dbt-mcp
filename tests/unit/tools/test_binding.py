import pytest

from dbt_mcp.tools.definitions import ToolDefinition


async def selected_project(project_id: int, query: str) -> str:
    return f"{project_id}:{query}"


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
    bound = tool.bind_arguments(project_id=42)
    assert await bound.fn(query="orders") == "42:orders"
    assert bound.to_fastmcp_internal_tool().parameters["required"] == ["query"]
    assert "project_id" not in bound.to_fastmcp_internal_tool().parameters["properties"]
    assert "project_id" in tool.to_fastmcp_internal_tool().parameters["properties"]
    assert await tool.bind_arguments(project_id=10).fn(query="orders") == "10:orders"


async def test_binding_rejects_conflicting_direct_arguments() -> None:
    with pytest.raises(ValueError, match="project_id.*bound"):
        await (
            definition().bind_arguments(project_id=42).fn(project_id=10, query="orders")
        )


def test_binding_rejects_unknown_arguments_at_registration() -> None:
    with pytest.raises(ValueError, match="environment_id"):
        definition().bind_arguments(environment_id=81)
