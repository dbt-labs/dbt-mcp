"""Restricted executor for code mode: runs user Python with a dbt tool proxy."""

import ast
import asyncio
import json
import logging
import subprocess
import sys
from collections.abc import Awaitable, Callable
from typing import Any

logger = logging.getLogger(__name__)

DEFAULT_TIMEOUT_SECONDS = 60
MAX_CODE_CHARS = 8000

_DISALLOWED_CALL_NAMES = frozenset(
    {
        "compile",
        "delattr",
        "eval",
        "exec",
        "getattr",
        "globals",
        "input",
        "locals",
        "open",
        "setattr",
        "vars",
    }
)

_DISALLOWED_NODES = (
    ast.AsyncFor,
    ast.ClassDef,
    ast.For,
    ast.Global,
    ast.Import,
    ast.ImportFrom,
    ast.Nonlocal,
    ast.While,
    ast.With,
)

# Type for async (name, arguments) -> tool result
CallToolFn = Callable[[str, dict[str, Any]], Awaitable[Any]]


class DbtToolProxy:
    """Proxy that turns attribute access into MCP tool calls.

    Used inside execute(): when the agent's code calls await dbt.list_models(),
    this dispatches to the server's call_tool("list_models", {}).
    """

    def __init__(self, call_tool_fn: CallToolFn) -> None:
        self._call_tool = call_tool_fn

    def __getattr__(self, name: str) -> Callable[..., Any]:
        """Return an async function that calls the MCP tool with the given name."""

        async def tool_fn(**kwargs: Any) -> Any:
            return await self._call_tool(name, kwargs)

        return tool_fn


def _indent(code: str, spaces: int = 4) -> str:
    prefix = " " * spaces
    return "\n".join(prefix + line for line in code.splitlines())


def _restricted_builtins() -> dict[str, Any]:
    return {
        "None": None,
        "True": True,
        "False": False,
        "abs": abs,
        "all": all,
        "any": any,
        "dict": dict,
        "enumerate": enumerate,
        "filter": filter,
        "len": len,
        "list": list,
        "map": map,
        "max": max,
        "min": min,
        "range": range,
        "repr": repr,
        "reversed": reversed,
        "set": set,
        "sorted": sorted,
        "str": str,
        "sum": sum,
        "tuple": tuple,
        "zip": zip,
    }


def _build_restricted_globals(dbt: DbtToolProxy) -> dict[str, Any]:
    """Minimal globals for code mode execution: only dbt and safe builtins."""
    return {"dbt": dbt, **_restricted_builtins()}


def _validate_code(code: str, *, allow_await: bool) -> None:
    if len(code) > MAX_CODE_CHARS:
        raise ValueError(
            f"Code is too large for code mode ({len(code)} chars > {MAX_CODE_CHARS})"
        )
    try:
        parsed = ast.parse(code, mode="exec")
    except SyntaxError as e:
        raise ValueError(f"Invalid Python in code mode: {e}") from e

    for node in ast.walk(parsed):
        if isinstance(node, _DISALLOWED_NODES):
            raise ValueError(f"{type(node).__name__} is not allowed in code mode")
        if isinstance(node, ast.Await) and not allow_await:
            raise ValueError("await is not allowed in codemode_search")
        if isinstance(node, ast.Attribute) and node.attr.startswith("__"):
            raise ValueError("Dunder attribute access is not allowed in code mode")
        if isinstance(node, ast.Name) and node.id.startswith("__"):
            raise ValueError("Dunder names are not allowed in code mode")
        if isinstance(node, ast.Call):
            if isinstance(node.func, ast.Name) and node.func.id in _DISALLOWED_CALL_NAMES:
                raise ValueError(f"{node.func.id}() is not allowed in code mode")
            if isinstance(node.func, ast.Attribute) and node.func.attr in _DISALLOWED_CALL_NAMES:
                raise ValueError(f"{node.func.attr}() is not allowed in code mode")


async def execute_code(
    code: str,
    call_tool_fn: CallToolFn,
    *,
    timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS,
) -> Any:
    """Execute async Python code with dbt proxy in a restricted environment.

    The code is wrapped as the body of an async function and must return a value.
    Only the dbt proxy and a minimal set of builtins are available.

    Args:
        code: Python source (body of an async function).
        call_tool_fn: Async (name, arguments) -> tool result.
        timeout_seconds: Max execution time.

    Returns:
        The return value of the executed code.

    Raises:
        TimeoutError: If execution exceeds timeout_seconds.
        Exception: Any exception raised by the user code.
    """
    _validate_code(code, allow_await=True)

    dbt = DbtToolProxy(call_tool_fn)
    globals_dict = _build_restricted_globals(dbt)
    globals_dict["__builtins__"] = _restricted_builtins()

    indented = _indent(code)
    wrapped = f"async def __codemode__():\n{indented}"

    exec(wrapped, globals_dict)

    coro = globals_dict["__codemode__"]()
    if not asyncio.iscoroutine(coro):
        raise ValueError("Code mode execute must define async code that returns a value")

    return await asyncio.wait_for(coro, timeout=timeout_seconds)


def _search_restricted_globals() -> dict[str, Any]:
    """Minimal globals for search code: no I/O, no dbt proxy."""
    return _restricted_builtins()


def run_search_code(code: str, catalog: list[dict[str, Any]]) -> Any:
    """Run synchronous Python code with catalog in scope; return the result.

    Used by codemode_search: the agent's code filters or queries the catalog
    without loading full tool schemas into context.
    """
    _validate_code(code, allow_await=False)
    payload = json.dumps({"code": code, "catalog": catalog})
    executor_script = """
import json
import sys

SAFE_BUILTINS = {
    "None": None,
    "True": True,
    "False": False,
    "abs": abs,
    "all": all,
    "any": any,
    "dict": dict,
    "enumerate": enumerate,
    "filter": filter,
    "len": len,
    "list": list,
    "map": map,
    "max": max,
    "min": min,
    "range": range,
    "repr": repr,
    "reversed": reversed,
    "set": set,
    "sorted": sorted,
    "str": str,
    "sum": sum,
    "tuple": tuple,
    "zip": zip,
}

def _indent(value: str, spaces: int = 4) -> str:
    prefix = " " * spaces
    return "\\n".join(prefix + line for line in value.splitlines())

try:
    request = json.loads(sys.stdin.read())
    code = request["code"]
    catalog = request["catalog"]
    globals_dict = {"catalog": catalog, **SAFE_BUILTINS, "__builtins__": SAFE_BUILTINS}
    wrapped = "def __codemode_search__():\\n" + _indent(code)
    exec(wrapped, globals_dict)
    result = globals_dict["__codemode_search__"]()
    print(json.dumps({"ok": True, "result": result}, default=str))
except Exception as exc:
    print(json.dumps({"ok": False, "error": str(exc)}))
"""
    try:
        completed = subprocess.run(
            [sys.executable, "-I", "-c", executor_script],
            input=payload,
            text=True,
            capture_output=True,
            timeout=DEFAULT_TIMEOUT_SECONDS,
            check=False,
        )
    except subprocess.TimeoutExpired as exc:
        raise TimeoutError("codemode_search timed out") from exc

    raw_output = completed.stdout.strip().splitlines()
    if not raw_output:
        stderr = completed.stderr.strip()
        raise ValueError(f"codemode_search failed: {stderr or 'empty output'}")
    try:
        response = json.loads(raw_output[-1])
    except json.JSONDecodeError as exc:
        raise ValueError("codemode_search returned invalid output") from exc
    if not response.get("ok", False):
        raise ValueError(str(response.get("error", "codemode_search failed")))
    return response.get("result")
