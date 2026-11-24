from dataclasses import replace
from typing import Any
from unittest.mock import AsyncMock

import pytest
from mcp.server.fastmcp import FastMCP
from mcp.server.fastmcp.exceptions import ToolError

from dbt_mcp.config.config_providers import AdminApiConfig, StaticConfigProvider
from dbt_mcp.dbt_admin.client import DbtAdminAPIClient
from dbt_mcp.dbt_admin.tools import register_admin_api_tools
from dbt_mcp.pagination import Pagination, ResultPage
from dbt_mcp.tools.tool_names import ToolName
from tests.mocks.config import mock_admin_api_config
from tests.unit.mcp.test_dispatcher import make_server


def register_jobs(
    registry: FastMCP, config: AdminApiConfig, monkeypatch: pytest.MonkeyPatch
) -> AsyncMock:
    fetch = AsyncMock(
        return_value=ResultPage(
            result=[{"id": 1}], pagination=Pagination(has_more=False)
        )
    )
    monkeypatch.setattr(DbtAdminAPIClient, "list_jobs", fetch)
    register_admin_api_tools(
        registry,
        StaticConfigProvider(config),
        disabled_tools=set(ToolName) - {ToolName.LIST_JOBS},
        enabled_tools={ToolName.LIST_JOBS},
        enabled_toolsets=set(),
        disabled_toolsets=set(),
    )
    return fetch


@pytest.mark.parametrize(
    "environment,configured_project,arguments,filters",
    [
        (100, 10, {"project_id": 20}, {"project_id": 20}),
        (100, 10, {}, {"environment_id": 100}),
        (100, None, {"project_id": None}, {"environment_id": 100}),
        (None, 10, {}, {"project_id": 10}),
        (None, None, {}, {}),
        (None, None, {"project_id": 20}, {"project_id": 20}),
    ],
)
async def test_jobs_filter_precedence_and_pagination(
    environment: int | None,
    configured_project: int | None,
    arguments: dict[str, Any],
    filters: dict[str, int],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    registry = FastMCP()
    fetch = register_jobs(
        registry,
        replace(
            mock_admin_api_config,
            environment_id=environment,
            project_id=configured_project,
        ),
        monkeypatch,
    )
    await registry.call_tool("list_jobs", {**arguments, "limit": 10, "offset": 20})
    fetch.assert_awaited_once_with(12345, **filters, limit=10, offset=20)


async def test_unbound_jobs_schema_has_an_optional_nullable_project(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    registry = FastMCP()
    register_jobs(registry, mock_admin_api_config, monkeypatch)
    schema = (await registry.list_tools())[0].inputSchema
    selector = schema["properties"]["project_id"]
    assert "project_id" not in schema.get("required", [])
    assert selector["default"] is None
    assert {variant["type"] for variant in selector["anyOf"]} == {"integer", "null"}


@pytest.mark.parametrize(
    "projects,environment,arguments,filters",
    [
        (None, 100, {}, {"environment_id": 100}),
        (None, None, {}, {}),
        ([10], 100, {}, {"environment_id": 100}),
        ([10], None, {}, {"project_id": 10}),
        ([10, 20], None, {}, {}),
        ([10, 20], 100, {"project_id": 20}, {"project_id": 20}),
    ],
)
async def test_local_jobs_binding_preserves_optional_selection(
    projects: list[int] | None,
    environment: int | None,
    arguments: dict[str, Any],
    filters: dict[str, int],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    server = make_server(projects)
    fetch = register_jobs(
        server.tool_server,
        replace(
            mock_admin_api_config,
            environment_id=environment,
            project_id=projects[0] if projects and len(projects) == 1 else None,
        ),
        monkeypatch,
    )
    schema = next(
        tool for tool in await server.list_tools() if tool.name == "list_jobs"
    ).inputSchema
    assert ("project_id" in schema["properties"]) == bool(
        projects and len(projects) > 1
    )
    assert "project_id" not in schema.get("required", [])
    await server.call_tool("list_jobs", arguments)
    fetch.assert_awaited_once_with(12345, **filters, limit=50, offset=0)


async def test_local_jobs_rejects_an_unconfigured_explicit_project(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    server = make_server([10, 20])
    fetch = register_jobs(server.tool_server, mock_admin_api_config, monkeypatch)
    with pytest.raises(ToolError, match="project_id"):
        await server.call_tool("list_jobs", {"project_id": 30})
    fetch.assert_not_called()
