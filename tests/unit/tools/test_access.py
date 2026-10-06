from enum import Enum
from typing import Annotated

import pytest

from dbt_mcp.dbt_admin.tools import list_jobs_runs, list_projects
from dbt_mcp.discovery.tools import DISCOVERY_TOOLS
from dbt_mcp.semantic_layer.tools import SEMANTIC_LAYER_TOOLS
from dbt_mcp.tools.definitions import (
    GenericToolDefinition,
    ToolDefinition,
    generic_dbt_mcp_tool,
)
from dbt_mcp.tools.targets import (
    AccountTarget,
    Permission,
    ProjectTarget,
    target_parameters,
)


class ExampleName(Enum):
    EXAMPLE = "example"


@pytest.mark.parametrize("definition_type", [GenericToolDefinition, ToolDefinition])
def test_direct_definition_requires_an_explicit_permission_declaration(
    definition_type: type,
) -> None:
    with pytest.raises(ValueError, match="access requirements"):
        definition_type(
            fn=lambda: "example",
            title="Example",
            description="Example",
            name_enum=ExampleName,
        )


def test_tool_decorator_requires_an_explicit_permission_declaration() -> None:
    with pytest.raises(ValueError, match="access requirements"):

        @generic_dbt_mcp_tool(
            name_enum=ExampleName, title="Example", description="Example"
        )
        async def example(query: str) -> str:
            return query


def test_service_tools_declare_permissions_on_target_arguments() -> None:
    for tools, parameter, target_type, permission in (
        (DISCOVERY_TOOLS, "project_id", ProjectTarget, Permission.METADATA_READ),
        (
            SEMANTIC_LAYER_TOOLS,
            "project_id",
            ProjectTarget,
            Permission.SEMANTIC_LAYER_CONFIGURATION_READ,
        ),
    ):
        for tool in tools:
            declarations = tool.targets
            assert isinstance(declarations[parameter], target_type)
            assert declarations[parameter].requires == permission
            assert declarations["project_id"].requires == permission


@pytest.mark.parametrize(
    "tool,permission",
    [(list_projects, Permission.PROJECTS_READ), (list_jobs_runs, Permission.RUNS_READ)],
)
def test_account_requirement_is_explicit(
    tool: ToolDefinition, permission: Permission
) -> None:
    assert tool.requirements == (AccountTarget(requires=permission),)


def test_context_adaptation_preserves_requirements_and_argument_metadata() -> None:
    class HostPolicy(Enum):
        EXAMPLE = "example"

    @generic_dbt_mcp_tool(
        name_enum=ExampleName,
        title="Example",
        description="Example",
        requirements=(HostPolicy.EXAMPLE,),
    )
    async def example(
        context: int,
        project_id: Annotated[int, ProjectTarget(requires=Permission.METADATA_READ)],
        query: str,
    ) -> str:
        return query

    def mapper(project_id: int) -> int:
        return project_id

    adapted = example.adapt_context(mapper)
    assert adapted.requirements == (HostPolicy.EXAMPLE,)
    assert target_parameters(adapted.fn) == target_parameters(example.fn)
    assert adapted.to_fastmcp_internal_tool().parameters["required"] == [
        "project_id",
        "query",
    ]
