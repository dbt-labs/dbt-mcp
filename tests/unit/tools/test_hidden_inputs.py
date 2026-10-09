from typing import Annotated

import pytest
from mcp.server.fastmcp.exceptions import ToolError
from pydantic import Field

from dbt_mcp.tools.definitions import dbt_mcp_tool
from dbt_mcp.tools.injection import HIDE, AdaptError, adapt_with_mappers
from dbt_mcp.tools.targets import Permission, ProjectTarget


async def test_context_mapper_authorizes_shared_arguments_after_validation():
    events = []

    @dbt_mcp_tool(title="Models", description="Models")
    async def get_all_models(
        context: str,
        sql: str,
        *,
        project_id: Annotated[
            int, ProjectTarget(requires=Permission.METADATA_READ)
        ] = Field(gt=0),
        environment_id: int = Field(gt=0),
    ) -> str:
        events.append(("body", sql, context))
        return f"{context}:{sql}"

    async def build_context(
        sql: str, *, project_id: int | None = None, environment_id: int | None = None
    ) -> str:
        events.append(("authorize", sql, project_id, environment_id))
        if sql == "denied":
            raise PermissionError("Access denied")
        return f"environment {environment_id}"

    def selected_environment() -> int:
        return 12

    adapted = get_all_models.adapt_with_mappers(
        context=build_context, project_id=HIDE
    ).adapt_with_mappers(environment_id=selected_environment)
    tool = adapted.fastmcp_tool
    assert set(tool.parameters["properties"]) == {"sql"}
    assert tool.parameters["required"] == ["sql"]
    assert adapted.targets == get_all_models.targets
    assert "project_id" in get_all_models.fastmcp_tool.parameters["properties"]
    for inputs in ({}, {"sql": "select 1", "project_id": 42}):
        with pytest.raises(ToolError):
            await tool.run(inputs)
    assert events == []
    assert await tool.run({"sql": "select 1"}) == "environment 12:select 1"
    assert events == [
        ("authorize", "select 1", None, 12),
        ("body", "select 1", "environment 12"),
    ]
    events.clear()
    with pytest.raises(ToolError, match="Access denied"):
        await tool.run({"sql": "denied"})
    assert events == [("authorize", "denied", None, 12)]


def test_hide_preserves_visible_types_and_supports_sync_calls():
    def body(context: str, /, *, project_id: int = Field(gt=0)) -> str:
        return context

    def build_context(project_id: int | None = None) -> str:
        return str(project_id or 42)

    hidden = adapt_with_mappers(body, context=build_context, project_id=HIDE)
    assert hidden() == "42"
    with pytest.raises(TypeError, match="project_id"):
        hidden(project_id=42)


def test_hide_rejects_required_mapper_inputs_and_required_body_arguments():
    def body(context: str, *, project_id: int = Field(gt=0)) -> str:
        return context

    def required_context(project_id: int) -> str:
        return str(project_id)

    with pytest.raises(AdaptError, match="project_id.*required"):
        adapt_with_mappers(body, context=required_context, project_id=HIDE)

    def uses_project(project_id: int) -> int:
        return project_id

    with pytest.raises(AdaptError, match="project_id.*required"):
        adapt_with_mappers(uses_project, project_id=HIDE)
