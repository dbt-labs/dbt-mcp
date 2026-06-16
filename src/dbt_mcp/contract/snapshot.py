"""Generate and validate the published-app contract snapshot.

Hosts that publish an MCP server as an app (notably OpenAI/ChatGPT) cache the
server's metadata as a versioned API contract at submission time. The cached
snapshot includes tool names/titles/descriptions, input/output schemas,
annotations, ``_meta`` fields, linked UI resource metadata (including CSP), and
the server ``instructions`` returned during initialization. Deploying a server
change does not update that published snapshot, and breaking changes (removing
or renaming a tool, making an input schema incompatible, or changing the
content served at a published UI resource URI) can break the published version
as soon as they deploy.

This module renders that contract surface to a deterministic JSON document so
CI can diff it against a committed snapshot. When the snapshot changes, the PR
author must regenerate and commit it, which is the signal that a new app
version needs to be submitted for review.

Scope: the snapshot covers only tools whose definitions live in this repo.
Proxied tools (see ``dbt_mcp.proxy.tools``) are hosted remotely -- their
contract is owned by the remote endpoint, not by this repo -- so they are
excluded and asserted out. The included toolsets are declared by
``INCLUDED_TOOLSETS`` below; change that set to change what is guarded.

Notes on completeness:
  - Tool "security schemes" (in OpenAI's cached-contract list) are not a
    distinct field on the MCP ``Tool`` object; if present they live in ``_meta``
    which is captured via the ``meta`` field.
  - The ``Tool.icons`` and ``Tool.execution`` fields are not part of OpenAI's
    documented cached contract and are unpopulated for every current tool, so
    they are intentionally omitted. Add them here if that changes.
  - The published app currently exposes no MCP App / UI resource (resource
    registration is server-level, not toolset-gated, so force-enabling only the
    included toolsets does not hide one). The resource content-hash guard is
    therefore dormant until the first UI resource is added.
"""

import argparse
import asyncio
import hashlib
import json
import logging
import os
import sys
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, patch

from pydantic import BaseModel

from dbt_mcp.tools.tool_names import ToolName
from dbt_mcp.tools.toolsets import TOOL_TO_TOOLSET, Toolset, proxied_tools, toolsets

logger = logging.getLogger(__name__)

SNAPSHOT_PATH = (
    Path(__file__).parents[3] / "tests" / "unit" / "contract" / "contract_snapshot.json"
)

# Declarative scope: which toolsets the published app exposes and that this repo
# owns the contract for. Edit this set (and regenerate the snapshot) to change
# what is guarded; the generator force-enables exactly these toolsets so the
# result does not depend on the server's default enable/disable settings.
#
# Excluded by design:
#   - dbt_cli, dbt_codegen: not exposed by the published ChatGPT app.
#   - mcp_server_metadata: dev-oriented, disabled by default, not published.
#   - dbt_lsp: depends on a local dbt Fusion/LSP server; not exposed by the
#     remote endpoint (its fusion_* tools are proxied anyway).
#   - sql: fully proxied (see proxied_tools); its contract is owned by the
#     remote endpoint, not this repo, so it contributes nothing here anyway.
INCLUDED_TOOLSETS: set[Toolset] = {
    Toolset.DISCOVERY,
    Toolset.SEMANTIC_LAYER,
    Toolset.ADMIN_API,
    Toolset.PRODUCT_DOCS,
}

# Fake-but-stable environment so load_config() never blocks on network or a
# real dbt binary. Only metadata is read, so the values never reach a server.
_SNAPSHOT_ENV = {
    "DBT_HOST": "http://localhost:8000",
    "DBT_TOKEN": "fake-token",
    "DBT_ACCOUNT_ID": "1",
    "DBT_PROD_ENV_ID": "1",
    "DBT_DEV_ENV_ID": "1",
    "DBT_USER_ID": "1",
}


class ToolContract(BaseModel):
    name: str
    toolset: str
    title: str | None
    description: str | None
    input_schema: dict[str, Any] | None
    output_schema: dict[str, Any] | None
    annotations: dict[str, Any] | None
    meta: dict[str, Any] | None


class ResourceContract(BaseModel):
    uri: str
    name: str | None
    mime_type: str | None
    meta: dict[str, Any] | None
    content_sha256: str | None


class ContractSnapshot(BaseModel):
    included_toolsets: list[str]
    excluded_toolsets: list[str]
    server_instructions: str | None
    tools: list[ToolContract]
    resources: list[ResourceContract]


def expected_tool_names() -> set[str]:
    """Tool names the snapshot must contain: every tool in an included toolset,
    minus proxied tools (which are owned by the remote endpoint)."""
    expected: set[ToolName] = set()
    for toolset in INCLUDED_TOOLSETS:
        expected |= toolsets[toolset]
    expected -= proxied_tools
    return {tool.value for tool in expected}


@asynccontextmanager
async def _snapshot_server() -> AsyncIterator[Any]:
    """Build the in-process server with a deterministic, network-free config.

    Proxied tools are disabled at config load and their registration is patched
    out so the enumeration never reaches a remote endpoint. The server is forced
    into single-project mode so ``list_tools`` returns the locally registered
    tools.
    """
    from dbt_mcp.config.config import load_config
    from dbt_mcp.dbt_cli.binary_type import BinaryType
    from dbt_mcp.mcp.server import create_dbt_mcp

    previous_env = {k: os.environ.get(k) for k in _SNAPSHOT_ENV}
    os.environ.update(_SNAPSHOT_ENV)
    try:
        with (
            patch(
                "dbt_mcp.config.config.detect_binary_type",
                return_value=BinaryType.DBT_CORE,
            ),
            patch("dbt_mcp.mcp.server.register_proxied_tools", return_value=None),
            patch(
                "dbt_mcp.mcp.server.DbtMCP._is_multi_project",
                new_callable=AsyncMock,
                return_value=False,
            ),
        ):
            config = load_config(enable_proxied_tools=False)
            # Force registration to exactly the included toolsets so the
            # snapshot does not depend on the server's default enable/disable
            # settings (which, e.g., disable mcp_server_metadata and sql).
            config.enabled_toolsets = set(INCLUDED_TOOLSETS)
            config.disabled_toolsets = set(Toolset) - INCLUDED_TOOLSETS
            dbt_mcp = await create_dbt_mcp(config)
            yield dbt_mcp
    finally:
        for key, value in previous_env.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value


def _normalize(value: Any) -> dict[str, Any] | None:
    """Coerce schema/annotation/meta values to a plain JSON-able dict or None."""
    if value is None:
        return None
    if isinstance(value, BaseModel):
        value = value.model_dump(mode="json", exclude_none=True)
    if isinstance(value, dict):
        return value if value else None
    return value


async def _read_resource_hash(dbt_mcp: Any, uri: str) -> str | None:
    """Hash the content served at a UI resource URI.

    The URI alone does not catch a content change at a published resource, which
    OpenAI treats as breaking, so the snapshot pins a hash of the served bytes.
    """
    try:
        contents = await dbt_mcp.read_resource(uri)
    except Exception as exc:  # pragma: no cover - defensive
        logger.warning(f"Could not read resource {uri}: {exc}")
        return None
    hasher = hashlib.sha256()
    for content in contents:
        payload = getattr(content, "content", None)
        if payload is None:
            payload = getattr(content, "blob", None)
        if isinstance(payload, str):
            hasher.update(payload.encode("utf-8"))
        elif isinstance(payload, bytes):
            hasher.update(payload)
    return hasher.hexdigest()


async def generate_snapshot() -> ContractSnapshot:
    """Enumerate the live in-process server and render the contract surface."""
    async with _snapshot_server() as dbt_mcp:
        all_tools = await dbt_mcp.list_tools()

        included_names: set[str] = set()
        tool_contracts: list[ToolContract] = []
        for tool in all_tools:
            try:
                tool_name = ToolName(tool.name)
            except ValueError:
                # An enumerated tool with no ToolName entry is unexpected once
                # proxied tools are patched out -- surface it loudly.
                raise ValueError(
                    f"Enumerated tool '{tool.name}' is not a known ToolName. "
                    "If it is a new local tool, add it to ToolName and a toolset."
                )
            toolset = TOOL_TO_TOOLSET[tool_name]
            if toolset not in INCLUDED_TOOLSETS or tool_name in proxied_tools:
                continue
            included_names.add(tool.name)
            tool_contracts.append(
                ToolContract(
                    name=tool.name,
                    toolset=toolset.value,
                    title=getattr(tool, "title", None),
                    description=tool.description,
                    input_schema=_normalize(getattr(tool, "inputSchema", None)),
                    output_schema=_normalize(getattr(tool, "outputSchema", None)),
                    annotations=_normalize(getattr(tool, "annotations", None)),
                    meta=_normalize(
                        getattr(tool, "meta", None) or getattr(tool, "_meta", None)
                    ),
                )
            )

        expected = expected_tool_names()
        if included_names != expected:
            missing = sorted(expected - included_names)
            extra = sorted(included_names - expected)
            raise ValueError(
                "Enumerated contract tools do not match the expected set for "
                f"INCLUDED_TOOLSETS.\n  missing (failed to register?): {missing}\n"
                f"  unexpected: {extra}"
            )

        resource_contracts: list[ResourceContract] = []
        try:
            resources = await dbt_mcp.list_resources()
        except Exception as exc:  # pragma: no cover - defensive
            logger.warning(f"Could not list resources: {exc}")
            resources = []
        for resource in resources:
            uri = str(resource.uri)
            resource_contracts.append(
                ResourceContract(
                    uri=uri,
                    name=getattr(resource, "name", None),
                    mime_type=getattr(resource, "mimeType", None),
                    meta=_normalize(
                        getattr(resource, "meta", None)
                        or getattr(resource, "_meta", None)
                    ),
                    content_sha256=await _read_resource_hash(dbt_mcp, uri),
                )
            )

        return ContractSnapshot(
            included_toolsets=sorted(t.value for t in INCLUDED_TOOLSETS),
            excluded_toolsets=sorted(
                t.value for t in (set(Toolset) - INCLUDED_TOOLSETS)
            ),
            server_instructions=getattr(dbt_mcp, "instructions", None),
            tools=sorted(tool_contracts, key=lambda t: t.name),
            resources=sorted(resource_contracts, key=lambda r: r.uri),
        )


def snapshot_to_json(snapshot: ContractSnapshot) -> str:
    """Render a snapshot to deterministic JSON (sorted keys, trailing newline)."""
    payload = snapshot.model_dump(mode="json")
    # ensure_ascii=True matches the repo's pretty-format-json pre-commit hook so
    # the generated file and the hook agree (otherwise they fight over non-ASCII).
    return json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=True) + "\n"


def _required(schema: dict[str, Any] | None) -> set[str]:
    if not schema:
        return set()
    required = schema.get("required")
    return set(required) if isinstance(required, list) else set()


def _properties(schema: dict[str, Any] | None) -> set[str]:
    if not schema:
        return set()
    properties = schema.get("properties")
    return set(properties) if isinstance(properties, dict) else set()


def classify_change(old: dict[str, Any], new: dict[str, Any]) -> tuple[str, list[str]]:
    """Classify the change between two rendered snapshots (advisory only).

    Returns one of ``"none"``, ``"compatible"``, ``"breaking"`` and a list of
    human-readable reasons. This is a heuristic to label PRs -- a rename is
    indistinguishable from a remove + add at the diff level, so the human
    decides the real safety. Mirrors the breaking cases OpenAI documents:
    removing a tool, making an input schema incompatible, or changing the
    content served at a published UI resource URI.
    """
    reasons: list[str] = []
    breaking: list[str] = []

    old_tools = {tool["name"]: tool for tool in old.get("tools", [])}
    new_tools = {tool["name"]: tool for tool in new.get("tools", [])}

    for name in sorted(set(old_tools) - set(new_tools)):
        breaking.append(f"tool removed or renamed: {name}")
    for name in sorted(set(new_tools) - set(old_tools)):
        reasons.append(f"tool added: {name}")

    for name in sorted(set(old_tools) & set(new_tools)):
        old_tool, new_tool = old_tools[name], new_tools[name]
        old_in, new_in = old_tool.get("input_schema"), new_tool.get("input_schema")
        removed_props = _properties(old_in) - _properties(new_in)
        added_required = _required(new_in) - _required(old_in)
        if removed_props:
            breaking.append(f"{name}: input property removed: {sorted(removed_props)}")
        if added_required:
            breaking.append(
                f"{name}: input property newly required: {sorted(added_required)}"
            )
        if old_in != new_in and not removed_props and not added_required:
            reasons.append(f"{name}: input schema changed (compatible)")
        if old_tool.get("output_schema") != new_tool.get("output_schema"):
            reasons.append(f"{name}: output schema changed")
        for field in ("title", "description", "annotations", "meta"):
            if old_tool.get(field) != new_tool.get(field):
                reasons.append(f"{name}: {field} changed")

    old_res = {res["uri"]: res for res in old.get("resources", [])}
    new_res = {res["uri"]: res for res in new.get("resources", [])}
    for uri in sorted(set(old_res) - set(new_res)):
        breaking.append(f"UI resource removed: {uri}")
    for uri in sorted(set(new_res) - set(old_res)):
        reasons.append(f"UI resource added: {uri}")
    for uri in sorted(set(old_res) & set(new_res)):
        if old_res[uri].get("content_sha256") != new_res[uri].get("content_sha256"):
            breaking.append(f"UI resource content changed: {uri}")
        if old_res[uri].get("mime_type") != new_res[uri].get("mime_type"):
            breaking.append(f"UI resource mime type changed: {uri}")

    if old.get("server_instructions") != new.get("server_instructions"):
        reasons.append("server instructions changed")

    if breaking:
        return "breaking", breaking + reasons
    if reasons:
        return "compatible", reasons
    return "none", []


def lint_claude_connector(snapshot: ContractSnapshot) -> list[str]:
    """Advisory checks for the Claude connector directory hard gates.

    These do not affect the snapshot itself; they flag metadata the Claude
    connector review rejects (tool name length, missing title, missing safety
    annotation). Advisory only.
    """
    warnings: list[str] = []
    for tool in snapshot.tools:
        if len(tool.name) > 64:
            warnings.append(f"{tool.name}: name exceeds 64 characters")
        if not tool.title:
            warnings.append(f"{tool.name}: missing title")
        annotations = tool.annotations or {}
        if "readOnlyHint" not in annotations and "destructiveHint" not in annotations:
            warnings.append(
                f"{tool.name}: missing safety annotation "
                "(readOnlyHint or destructiveHint)"
            )
    return warnings


_LABEL_FOR_LEVEL = {
    "breaking": "contract-breaking",
    "compatible": "contract-change",
    "none": "",
}


def _run_classify(old_snapshot_path: str) -> None:
    """Compare the committed snapshot to an older one and report the change level."""
    new = json.loads(SNAPSHOT_PATH.read_text())
    old_path = Path(old_snapshot_path)
    old = json.loads(old_path.read_text()) if old_path.exists() else {}
    level, reasons = classify_change(old, new)
    label = _LABEL_FOR_LEVEL[level]

    logger.info(f"contract change level: {level}")
    for reason in reasons:
        logger.info(f"  - {reason}")

    github_output = os.environ.get("GITHUB_OUTPUT")
    if github_output:
        with open(github_output, "a") as handle:
            handle.write(f"level={level}\n")
            handle.write(f"label={label}\n")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate or validate the published-app contract snapshot."
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="Fail if the committed snapshot is out of date (do not write).",
    )
    parser.add_argument(
        "--lint",
        action="store_true",
        help="Print advisory Claude-connector warnings.",
    )
    parser.add_argument(
        "--classify-against",
        metavar="OLD_SNAPSHOT",
        help=(
            "Compare the committed snapshot against OLD_SNAPSHOT and print the "
            "advisory change level (none/compatible/breaking). Writes "
            "level/label to $GITHUB_OUTPUT when set. Does not regenerate."
        ),
    )
    args = parser.parse_args()

    # Configure logging here (not at import time) so this module can be imported
    # by the test suite without overriding the repo's logging setup.
    logging.basicConfig(level=logging.INFO, format="%(message)s")

    if args.classify_against:
        _run_classify(args.classify_against)
        return

    snapshot = asyncio.run(generate_snapshot())
    rendered = snapshot_to_json(snapshot)

    if args.lint:
        for warning in lint_claude_connector(snapshot):
            logger.warning(f"⚠ claude-connector: {warning}")

    if args.check:
        current = SNAPSHOT_PATH.read_text() if SNAPSHOT_PATH.exists() else ""
        if current == rendered:
            logger.info("✓ contract snapshot is up to date")
            sys.exit(0)
        logger.error(
            "✗ contract snapshot is out of date.\n"
            "The ChatGPT-published contract changed. Run: task contract:generate\n"
            "Commit the regenerated snapshot and submit a new app version for review."
        )
        sys.exit(1)

    SNAPSHOT_PATH.parent.mkdir(parents=True, exist_ok=True)
    SNAPSHOT_PATH.write_text(rendered)
    logger.info(f"Contract snapshot written to {SNAPSHOT_PATH}")


if __name__ == "__main__":
    main()
