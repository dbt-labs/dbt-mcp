"""Unit tests for code mode executor."""

import pytest

from dbt_mcp.code_mode.executor import execute_code, run_search_code


def test_run_search_code_filters_catalog() -> None:
    catalog = [
        {"name": "list_models", "description": "List models", "param_names": []},
        {"name": "query_metrics", "description": "Query SL", "param_names": ["query"]},
    ]
    code = "return [t for t in catalog if 'model' in t['name']]"
    result = run_search_code(code, catalog)
    assert result == [{"name": "list_models", "description": "List models", "param_names": []}]


def test_run_search_code_syntax_error_raises() -> None:
    with pytest.raises(ValueError, match="Invalid Python"):
        run_search_code("return catalog[", [])


def test_run_search_code_disallows_imports() -> None:
    with pytest.raises(ValueError, match="not allowed"):
        run_search_code("import os\nreturn catalog", [])


@pytest.mark.asyncio
async def test_execute_code_disallows_imports() -> None:
    async def _call_tool(name: str, arguments: dict[str, object]) -> object:
        return {"name": name, "arguments": arguments}

    with pytest.raises(ValueError, match="not allowed"):
        await execute_code("import os\nreturn 1", _call_tool)
