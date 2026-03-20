"""Unit tests for code mode catalog."""

from types import SimpleNamespace

import pytest

from dbt_mcp.code_mode.catalog import build_catalog_from_tools


def test_build_catalog_from_tools_with_input_schema() -> None:
    tools = [
        SimpleNamespace(
            name="list_models",
            description="List dbt models",
            inputSchema={"properties": {"environment_id": {"type": "string"}}},
        ),
    ]
    catalog = build_catalog_from_tools(tools)
    assert len(catalog) == 1
    assert catalog[0]["name"] == "list_models"
    assert catalog[0]["description"] == "List dbt models"
    assert catalog[0]["param_names"] == ["environment_id"]


def test_build_catalog_from_tools_with_parameters() -> None:
    tools = [
        SimpleNamespace(
            name="query_metrics",
            description="Query metrics",
            parameters={"properties": {"query": {"type": "object"}}},
        ),
    ]
    catalog = build_catalog_from_tools(tools)
    assert catalog[0]["param_names"] == ["query"]


def test_build_catalog_truncates_long_descriptions() -> None:
    long_desc = "x" * 600
    tools = [SimpleNamespace(name="t", description=long_desc, inputSchema={})]
    catalog = build_catalog_from_tools(tools)
    assert len(catalog[0]["description"]) == 500
