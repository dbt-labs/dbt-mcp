import pytest
from mcp.server.fastmcp import Context

from dbt_mcp.tools.definitions import ToolDefinition
from dbt_mcp.tools.injection import AdaptError
from dbt_mcp.tools.binding import InputBinding


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


async def test_request_binding_uses_one_contract_for_schema_and_invocation() -> None:
    tool = definition()
    bound = tool.bind_inputs(InputBinding(values={"project_id": 42}))
    assert set(bound.to_fastmcp_internal_tool().parameters["properties"]) == {"query"}
    assert (
        await bound.to_fastmcp_internal_tool().run({"query": "orders"}) == "42:orders"
    )
    assert bound.validate_and_bind({"query": "orders"}) == {
        "project_id": 42,
        "query": "orders",
    }
    with pytest.raises(ValueError, match="bound"):
        bound.validate_and_bind({"project_id": 42, "query": "orders"})
    assert "project_id" in tool.to_fastmcp_internal_tool().parameters["properties"]


@pytest.mark.parametrize("project_id", [None, True, "42", 99])
def test_request_binding_enforces_the_advertised_choices(project_id: object) -> None:
    bound = definition().bind_inputs(InputBinding(choices={"project_id": (10, 42)}))
    schema = bound.to_fastmcp_internal_tool().parameters
    assert schema["properties"]["project_id"]["enum"] == [10, 42]
    with pytest.raises(ValueError, match="project_id.*choose"):
        bound.validate_and_bind({"project_id": project_id, "query": "orders"})
    assert bound.validate_and_bind({"project_id": 42, "query": "orders"}) == {
        "project_id": 42,
        "query": "orders",
    }


def test_request_binding_validates_ordinary_inputs() -> None:
    bound = definition().bind_inputs(InputBinding(values={"project_id": 42}))
    with pytest.raises(ValueError, match="query"):
        bound.validate_and_bind({})
    with pytest.raises(ValueError, match="Unknown.*typo"):
        bound.validate_and_bind({"query": "orders", "typo": 1})


def test_framework_context_is_not_a_model_input() -> None:
    def resolved_project(c: Context, project_id: int | None = None) -> int:
        return project_id or 42

    adapted = definition().adapt_with_mappers(project_id=resolved_project)
    assert set(adapted.input_schema["properties"]) == {"project_id", "query"}
    assert set(adapted.input_schema["required"]) == {"project_id", "query"}
    bound = adapted.bind_inputs(InputBinding(values={"project_id": 42}))
    assert bound.validate_and_bind({"query": "orders"}) == {
        "project_id": 42,
        "query": "orders",
    }
