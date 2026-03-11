from mcp.types import Tool as MCPTool

from dbt_mcp.tools.schema_utils import strip_project_id_param


def _make_tool(properties: dict, required: list[str] | None = None) -> MCPTool:
    schema: dict = {"type": "object", "properties": properties}
    if required is not None:
        schema["required"] = required
    return MCPTool(name="test_tool", inputSchema=schema)


def test_strip_project_id_from_properties_and_required():
    tool = _make_tool(
        properties={
            "project_id": {"type": "integer"},
            "name": {"type": "string"},
        },
        required=["project_id", "name"],
    )
    result = strip_project_id_param(tool)
    assert "project_id" not in result.inputSchema["properties"]
    assert "project_id" not in result.inputSchema.get("required", [])
    assert "name" in result.inputSchema["properties"]
    assert "name" in result.inputSchema["required"]


def test_strip_project_id_not_in_required():
    tool = _make_tool(
        properties={
            "project_id": {"type": "integer"},
            "name": {"type": "string"},
        },
        required=["name"],
    )
    result = strip_project_id_param(tool)
    assert "project_id" not in result.inputSchema["properties"]
    assert result.inputSchema["required"] == ["name"]


def test_strip_no_project_id_is_noop():
    tool = _make_tool(
        properties={"name": {"type": "string"}},
        required=["name"],
    )
    result = strip_project_id_param(tool)
    assert result.inputSchema == tool.inputSchema


def test_strip_removes_empty_required_list():
    tool = _make_tool(
        properties={"project_id": {"type": "integer"}},
        required=["project_id"],
    )
    result = strip_project_id_param(tool)
    assert result.inputSchema["properties"] == {}
    assert "required" not in result.inputSchema


def test_strip_preserves_other_fields():
    tool = _make_tool(
        properties={
            "project_id": {"type": "integer"},
            "limit": {"type": "integer"},
            "offset": {"type": "integer"},
        },
        required=["project_id"],
    )
    result = strip_project_id_param(tool)
    assert set(result.inputSchema["properties"].keys()) == {"limit", "offset"}


def test_strip_does_not_mutate_original():
    tool = _make_tool(
        properties={
            "project_id": {"type": "integer"},
            "name": {"type": "string"},
        },
        required=["project_id", "name"],
    )
    strip_project_id_param(tool)
    assert "project_id" in tool.inputSchema["properties"]
    assert "project_id" in tool.inputSchema["required"]
