import pytest
from contextlib import asynccontextmanager
from mcp.server.fastmcp import Context
from mcp.server.fastmcp.exceptions import ToolError
from pydantic import Field

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
    with pytest.raises(ToolError, match="project_id.*|Extra inputs"):
        await bound.to_fastmcp_internal_tool().run(
            {"project_id": 42, "query": "orders"}
        )
    assert "project_id" in tool.to_fastmcp_internal_tool().parameters["properties"]


@pytest.mark.parametrize("project_id", [None, True, "42", 99])
async def test_request_binding_enforces_the_advertised_choices(
    project_id: object,
) -> None:
    bound = definition().bind_inputs(InputBinding(choices={"project_id": (10, 42)}))
    schema = bound.to_fastmcp_internal_tool().parameters
    assert schema["properties"]["project_id"]["enum"] == [10, 42]
    with pytest.raises(ToolError, match="choose"):
        await bound.to_fastmcp_internal_tool().run(
            {"project_id": project_id, "query": "orders"}
        )
    assert (
        await bound.to_fastmcp_internal_tool().run(
            {"project_id": 42, "query": "orders"}
        )
        == "42:orders"
    )


async def test_selectable_input_preserves_declared_field_constraints():
    async def positive_project(
        project_id: int = Field(gt=0, description="Project"),
    ) -> int:
        return project_id

    tool = (
        ToolDefinition(
            fn=positive_project, title="test", description="test", requirements=()
        )
        .bind_inputs(InputBinding(choices={"project_id": (-1, 42)}))
        .to_fastmcp_internal_tool()
    )
    with pytest.raises(ToolError, match="greater than 0"):
        await tool.run({"project_id": -1})
    assert await tool.run({"project_id": 42}) == 42
    assert tool.parameters["properties"]["project_id"]["description"] == "Project"


async def test_request_binding_validates_ordinary_inputs() -> None:
    bound = definition().bind_inputs(InputBinding(values={"project_id": 42}))
    with pytest.raises(ToolError, match="query"):
        await bound.to_fastmcp_internal_tool().run({})
    with pytest.raises(ToolError, match="typo"):
        await bound.to_fastmcp_internal_tool().run({"query": "orders", "typo": 1})


async def test_framework_context_is_not_a_model_input() -> None:
    def resolved_project(c: Context, project_id: int | None = None) -> int:
        return project_id or 42

    adapted = definition().adapt_with_mappers(project_id=resolved_project)
    assert set(adapted.to_fastmcp_internal_tool().parameters["properties"]) == {
        "project_id",
        "query",
    }
    assert set(adapted.to_fastmcp_internal_tool().parameters["required"]) == {
        "project_id",
        "query",
    }
    bound = adapted.bind_inputs(InputBinding(values={"project_id": 42}))
    assert (
        await bound.to_fastmcp_internal_tool().run({"query": "orders"}) == "42:orders"
    )


async def test_call_scope_runs_after_validation_and_before_context_mapping():
    events = []

    @asynccontextmanager
    async def authorize(inputs):
        events.append(("authorize", inputs["project_id"]))
        try:
            yield inputs | {"project_id": 43}
        finally:
            events.append("exit")

    def project(project_id: int | None = None) -> int:
        events.append(("map", project_id))
        return project_id or 42

    tool = (
        definition()
        .adapt_with_mappers(project_id=project)
        .bind_inputs(InputBinding(values={"project_id": 42}), call_scope=authorize)
        .to_fastmcp_internal_tool()
    )
    with pytest.raises(ToolError, match="Extra inputs"):
        await tool.run({"project_id": 42, "query": "orders"})
    assert events == []
    assert await tool.run({"query": "orders"}) == "43:orders"
    assert events == [("authorize", 42), ("map", 43), "exit"]


@pytest.mark.parametrize("extra", [{"typo": 1}, {"project_id": 42}])
async def test_fastmcp_rejects_extra_inputs_before_context_mapping(extra):
    mapped = []

    def project() -> int:
        mapped.append(42)
        return 42

    tool = definition().adapt_with_mappers(project_id=project)
    with pytest.raises(ToolError, match="Extra inputs are not permitted"):
        await tool.to_fastmcp_internal_tool().run({"query": "orders", **extra})
    assert mapped == []
    assert tool.to_fastmcp_internal_tool().parameters["additionalProperties"] is False


async def test_fastmcp_validates_the_declared_selector_before_mapping():
    mapped = []

    def project(project_id: int | None = None) -> int:
        mapped.append(project_id)
        return project_id or 42

    tool = definition().adapt_with_mappers(project_id=project)
    with pytest.raises(ToolError, match="project_id"):
        await tool.to_fastmcp_internal_tool().run({"query": "orders"})
    with pytest.raises(ToolError, match="project_id"):
        await tool.to_fastmcp_internal_tool().run(
            {"project_id": None, "query": "orders"}
        )
    assert mapped == []
