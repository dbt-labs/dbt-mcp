"""Run LLM-written Python in a Monty sandbox whose only capability is the dbt MCP tools.

Usage:
    uv run main.py                      # run the built-in demo script
    uv run main.py script.py            # run a script from a file
    echo 'print(await list_metrics())' | uv run main.py -
    uv run main.py --instructions       # print the guide to give the LLM

Inside the sandbox every tool exposed by the MCP server is an async function that
takes keyword arguments and returns parsed JSON (or raw text when the tool doesn't
return JSON). No filesystem, network, subprocess or env access. Credentials stay in
the host process and never enter the sandbox.
"""

import argparse
import asyncio
import json
import sys
from collections.abc import Awaitable, Callable
from dataclasses import dataclass

import pydantic_monty as monty
from mcp import ClientSession
from mcp.types import TextContent, Tool

from remote_mcp.session import session_context

CALL_TIMEOUT_SECS = 120
ERROR_SNIPPET_CHARS = 2000

DEMO_SCRIPT = """\
metrics = await list_metrics()
metrics = metrics if isinstance(metrics, list) else [metrics]
names = [m["name"] for m in metrics]
print(f"{len(names)} metrics: {', '.join(names)}")
result = await query_metrics(metrics=names[:1])
print(result)
"""

GUIDE = """\
To query dbt through the dbt MCP server, write a Python script and run it with:

    uv run main.py <<'EOF'
    ...your code...
    EOF

The script runs in a sandbox (Monty) with top-level `await`. Each MCP tool is an
async function that takes keyword arguments and returns parsed JSON, or raw text
when the tool doesn't return JSON. A tool that returns several items gives you a
list; one that returns a single item gives you that item on its own, not a list of one.

Rules:
- Always `await`. A missing await gives you a coroutine object, not data.
- Chain calls inside one script and filter in Python; `print()` only what answers
  the question. Intermediate results stay in the sandbox and cost no tokens.
- Use `asyncio.gather(...)` for independent calls.
- Inspect a response before indexing into it; shapes vary per tool.
- Failed calls raise RuntimeError with the server's error text; catch it or fix the
  arguments and rerun.
- No files, network, subprocess or env access. Each run starts with fresh state.

Available functions:
"""


def _signature(tool: Tool) -> str:
    properties: dict[str, dict[str, object]] = tool.inputSchema.get("properties", {})
    required = set(tool.inputSchema.get("required", []))
    params = ", ".join(
        f"{name}" if name in required else f"{name}=None" for name in properties
    )
    description = (tool.description or "").strip().splitlines()
    summary = description[0] if description else ""
    return f"- `await {tool.name}({params})`: {summary}"


def build_guide(tools: list[Tool]) -> str:
    return GUIDE + "\n".join(_signature(t) for t in tools) + "\n"


@dataclass
class Stats:
    calls: int = 0
    result_bytes: int = 0
    failures: int = 0


class McpBridge:
    """Exposes the tools of an MCP session as async functions for the sandbox."""

    def __init__(self, session: ClientSession, tools: list[Tool]) -> None:
        self.session = session
        self.tools = tools
        self.stats = Stats()

    def externals(self) -> dict[str, Callable[..., Awaitable[object]]]:
        return {tool.name: self._make_caller(tool.name) for tool in self.tools}

    def _make_caller(self, name: str) -> Callable[..., Awaitable[object]]:
        async def call(**arguments: object) -> object:
            return await self._call(name, arguments)

        return call

    async def _call(self, name: str, arguments: dict[str, object]) -> object:
        self.stats.calls += 1
        try:
            result = await asyncio.wait_for(
                self.session.call_tool(name=name, arguments=arguments),
                CALL_TIMEOUT_SECS,
            )
        except TimeoutError:
            self.stats.failures += 1
            raise RuntimeError(f"{name} timed out after {CALL_TIMEOUT_SECS}s")
        texts = [c.text for c in result.content if isinstance(c, TextContent)]
        self.stats.result_bytes += sum(len(t) for t in texts)
        if result.isError:
            self.stats.failures += 1
            raise RuntimeError(
                f"{name} failed: {' '.join(texts)[:ERROR_SNIPPET_CHARS]}"
            )
        # MCP returns a list result as one text block per item
        parsed = [_parse(t) for t in texts]
        return parsed[0] if len(parsed) == 1 else parsed


def _parse(text: str) -> object:
    try:
        return json.loads(text)
    except ValueError:
        return text


async def run(code: str, bridge: McpBridge, *, max_chars: int) -> int:
    printed = monty.CollectString()
    exit_code = 0
    async with monty.AsyncMonty() as pool:
        async with pool.checkout(
            limits=monty.ResourceLimits(max_feed_duration_secs=60)
        ) as sandbox:
            try:
                value = await sandbox.feed_run(
                    code, external_lookup=bridge.externals(), print_callback=printed
                )
            except monty.MontyError as e:
                value, exit_code = None, 1
                print(f"{type(e).__name__}: {e}", file=sys.stderr)

    output = printed.output
    if value is not None:
        output += repr(value) + "\n"
    if len(output) > max_chars:
        output = (
            output[:max_chars]
            + f"\n... [truncated, {len(output) - max_chars} more chars]\n"
        )
    sys.stdout.write(output)

    s = bridge.stats
    print(
        f"[monty_code_mode] {s.calls} tool calls, {s.result_bytes / 1024:.1f} KB kept in "
        f"sandbox, {len(output)} chars returned, {s.failures} failed",
        file=sys.stderr,
    )
    return exit_code


async def main() -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "script", nargs="?", help="Python file to run, or - for stdin (default: demo)"
    )
    parser.add_argument("--max-chars", type=int, default=20_000)
    parser.add_argument(
        "--instructions", action="store_true", help="print the LLM guide and exit"
    )
    args = parser.parse_args()

    async with session_context() as session:
        tools = (await session.list_tools()).tools
        if args.instructions:
            print(build_guide(tools))
            return 0
        if args.script is None:
            code = DEMO_SCRIPT
        elif args.script == "-":
            code = sys.stdin.read()
        else:
            with open(args.script) as f:
                code = f.read()
        return await run(code, McpBridge(session, tools), max_chars=args.max_chars)


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
