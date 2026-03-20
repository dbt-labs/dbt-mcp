"""Unit tests for code mode spec builder."""

from types import SimpleNamespace
from unittest.mock import patch

import pytest

from dbt_mcp.code_mode.spec import ToolSpec, _extract_params_from_schema, _load_prompt


class TestExtractParamsFromSchema:
    def test_extracts_names_types_and_required(self) -> None:
        schema = {
            "properties": {
                "unique_id": {"type": "string", "description": "The resource ID"},
                "depth": {"type": "integer", "default": 5},
            },
            "required": ["unique_id"],
        }
        params = _extract_params_from_schema(schema)
        assert len(params) == 2
        uid = next(p for p in params if p["name"] == "unique_id")
        assert uid["type"] == "string"
        assert uid["required"] is True
        assert uid["description"] == "The resource ID"
        depth = next(p for p in params if p["name"] == "depth")
        assert depth["required"] is False
        assert depth["default"] == 5

    def test_empty_schema(self) -> None:
        assert _extract_params_from_schema({}) == []

    def test_enum_values_included(self) -> None:
        schema = {
            "properties": {
                "status": {"type": "string", "enum": ["active", "inactive"]},
            },
        }
        params = _extract_params_from_schema(schema)
        assert params[0]["enum"] == ["active", "inactive"]


class TestLoadPrompt:
    def test_returns_none_for_unknown_tool(self) -> None:
        assert _load_prompt("nonexistent_tool_xyz") is None

    def test_loads_known_tool_prompt(self) -> None:
        prompt = _load_prompt("get_lineage")
        assert prompt is not None
        assert "lineage" in prompt.lower()

    def test_returns_none_for_tool_without_prompt_dir(self) -> None:
        assert _load_prompt("get_mcp_server_version") is None


class TestToolSpec:
    @pytest.fixture()
    def sample_tools(self) -> dict[str, SimpleNamespace]:
        return {
            "get_all_models": SimpleNamespace(
                name="get_all_models",
                description="Retrieves name and description of all models.",
                parameters={
                    "properties": {
                        "environment_id": {"type": "integer", "description": "Env ID"},
                    },
                    "required": ["environment_id"],
                },
            ),
            "get_lineage": SimpleNamespace(
                name="get_lineage",
                description="Gets full lineage graph.",
                parameters={
                    "properties": {
                        "unique_id": {"type": "string"},
                        "types": {"type": "array"},
                        "depth": {"type": "integer", "default": 5},
                    },
                    "required": ["unique_id"],
                },
            ),
            "build": SimpleNamespace(
                name="build",
                description="Executes models, tests, snapshots, and seeds.",
                parameters={"properties": {"select": {"type": "string"}}},
            ),
        }

    def test_build_from_internal_tools(self, sample_tools: dict) -> None:
        spec = ToolSpec()
        spec.build_from_internal_tools(sample_tools)
        assert spec.tool_count == 3

    def test_list_categories(self, sample_tools: dict) -> None:
        spec = ToolSpec()
        spec.build_from_internal_tools(sample_tools)
        cats = spec.list_categories()
        cat_names = {c["category"] for c in cats}
        assert "discovery" in cat_names
        assert "dbt_cli" in cat_names

    def test_list_tools_all(self, sample_tools: dict) -> None:
        spec = ToolSpec()
        spec.build_from_internal_tools(sample_tools)
        tools = spec.list_tools()
        assert len(tools) == 3
        names = {t["name"] for t in tools}
        assert names == {"get_all_models", "get_lineage", "build"}

    def test_list_tools_by_category(self, sample_tools: dict) -> None:
        spec = ToolSpec()
        spec.build_from_internal_tools(sample_tools)
        tools = spec.list_tools(category="discovery")
        names = {t["name"] for t in tools}
        assert "get_all_models" in names
        assert "get_lineage" in names
        assert "build" not in names

    def test_get_tool_detail(self, sample_tools: dict) -> None:
        spec = ToolSpec()
        spec.build_from_internal_tools(sample_tools)
        detail = spec.get_tool_detail("get_lineage")
        assert detail is not None
        assert detail["name"] == "get_lineage"
        assert detail["category"] == "discovery"
        param_names = [p["name"] for p in detail["params"]]
        assert "unique_id" in param_names
        assert "depth" in param_names

    def test_get_tool_detail_unknown(self, sample_tools: dict) -> None:
        spec = ToolSpec()
        spec.build_from_internal_tools(sample_tools)
        assert spec.get_tool_detail("nonexistent") is None

    def test_get_tool_guide(self, sample_tools: dict) -> None:
        spec = ToolSpec()
        spec.build_from_internal_tools(sample_tools)
        guide = spec.get_tool_guide("get_lineage")
        assert guide is not None
        assert "lineage" in guide.lower()

    def test_get_tool_guide_no_prompt(self, sample_tools: dict) -> None:
        spec = ToolSpec()
        spec.build_from_internal_tools(sample_tools)
        assert spec.get_tool_guide("build") is not None

    def test_skips_non_toolname_tools(self) -> None:
        tools = {
            "custom_unknown_tool": SimpleNamespace(
                name="custom_unknown_tool",
                description="Not a real tool",
                parameters={"properties": {}},
            ),
        }
        spec = ToolSpec()
        spec.build_from_internal_tools(tools)
        assert spec.tool_count == 0

    def test_uses_human_descriptions(self, sample_tools: dict) -> None:
        spec = ToolSpec()
        spec.build_from_internal_tools(sample_tools)
        tools = spec.list_tools()
        models_tool = next(t for t in tools if t["name"] == "get_all_models")
        assert "Retrieves name and description of all models" in models_tool["summary"]
