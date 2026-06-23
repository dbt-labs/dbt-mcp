"""Guard the published-app contract snapshot.

If this test fails, the metadata cached by app hosts (notably the ChatGPT app)
changed. Regenerate and commit the snapshot with `task contract:generate`, then
submit a new app version for review. See `dbt_mcp.contract.snapshot`.
"""

import json

from dbt_mcp.contract.snapshot import (
    SNAPSHOT_PATH,
    classify_change,
    expected_tool_names,
    generate_snapshot,
    lint_claude_connector,
    snapshot_to_json,
)


def _snap(tools=None, resources=None, instructions="hi"):
    return {
        "tools": tools or [],
        "resources": resources or [],
        "server_instructions": instructions,
    }


def _tool(name, *, input_schema=None, description="d", output_schema=None):
    return {
        "name": name,
        "title": name,
        "description": description,
        "input_schema": input_schema,
        "output_schema": output_schema,
        "annotations": {"readOnlyHint": True},
        "meta": None,
    }


async def test_contract_snapshot_matches_committed():
    """The committed snapshot must match what the server currently exposes."""
    assert SNAPSHOT_PATH.exists(), (
        f"Contract snapshot missing at {SNAPSHOT_PATH}. Run: task contract:generate"
    )
    snapshot = await generate_snapshot()
    rendered = snapshot_to_json(snapshot)
    committed = SNAPSHOT_PATH.read_text()
    assert rendered == committed, (
        "Published-app contract snapshot is out of date. The metadata cached by "
        "ChatGPT (and reviewed by the Claude connector directory) changed.\n"
        "Run `task contract:generate`, commit the snapshot, and submit a new app "
        "version for review."
    )


async def test_contract_snapshot_has_expected_tools():
    """The snapshot covers exactly the included, non-proxied tools."""
    snapshot = await generate_snapshot()
    assert {tool.name for tool in snapshot.tools} == expected_tool_names()


async def test_contract_snapshot_excludes_proxied_tools():
    """Proxied tools are owned by the remote endpoint and must never appear."""
    from dbt_mcp.tools.toolsets import proxied_tools

    proxied_names = {tool.value for tool in proxied_tools}
    assert SNAPSHOT_PATH.exists(), (
        f"Contract snapshot missing at {SNAPSHOT_PATH}. Run: task contract:generate"
    )
    committed = json.loads(SNAPSHOT_PATH.read_text())
    snapshot_names = {tool["name"] for tool in committed["tools"]}
    assert not (snapshot_names & proxied_names)


async def test_committed_snapshot_passes_claude_connector_lint():
    """Advisory Claude hard-gate rules should hold for the freshly generated
    snapshot. test_contract_snapshot_matches_committed guarantees this equals
    the committed contract_snapshot.json."""
    snapshot = await generate_snapshot()
    assert lint_claude_connector(snapshot) == []


def test_classify_no_change():
    snap = _snap([_tool("a")])
    assert classify_change(snap, snap) == ("none", [])


def test_classify_added_tool_is_compatible():
    old = _snap([_tool("a")])
    new = _snap([_tool("a"), _tool("b")])
    level, reasons = classify_change(old, new)
    assert level == "compatible"
    assert any("tool added: b" in r for r in reasons)


def test_classify_removed_tool_is_breaking():
    old = _snap([_tool("a"), _tool("b")])
    new = _snap([_tool("a")])
    level, reasons = classify_change(old, new)
    assert level == "breaking"
    assert any("tool removed or renamed: b" in r for r in reasons)


def test_classify_new_required_input_is_breaking():
    old = _snap([_tool("a", input_schema={"properties": {"x": {}}, "required": []})])
    new = _snap([_tool("a", input_schema={"properties": {"x": {}}, "required": ["x"]})])
    level, _ = classify_change(old, new)
    assert level == "breaking"


def test_classify_added_optional_input_is_compatible():
    old = _snap([_tool("a", input_schema={"properties": {"x": {}}})])
    new = _snap([_tool("a", input_schema={"properties": {"x": {}, "y": {}}})])
    level, _ = classify_change(old, new)
    assert level == "compatible"


def test_classify_resource_content_change_is_breaking():
    old = _snap(resources=[{"uri": "ui://a", "content_sha256": "1", "mime_type": "t"}])
    new = _snap(resources=[{"uri": "ui://a", "content_sha256": "2", "mime_type": "t"}])
    level, reasons = classify_change(old, new)
    assert level == "breaking"
    assert any("content changed" in r for r in reasons)


def _union_schema(*, with_extra_field: bool):
    """An output schema shaped like pydantic's `A | B` union output, where the
    real fields live inside `$defs` (not at the top level)."""
    response_props = {"values": {"type": "array"}, "truncated": {"type": "boolean"}}
    if with_extra_field:
        response_props["extra"] = {"type": "string"}
    return {
        "anyOf": [
            {"$ref": "#/$defs/Response"},
            {"$ref": "#/$defs/Error"},
        ],
        "$defs": {
            "Response": {
                "type": "object",
                "properties": response_props,
                "required": list(response_props),
            },
            "Error": {
                "type": "object",
                "properties": {"error": {"type": "string"}},
                "required": ["error"],
            },
        },
    }


def test_classify_removed_field_inside_defs_is_breaking():
    """A field removed from a $defs sub-schema must be caught (not just top level)."""
    old = _snap([_tool("a", output_schema=_union_schema(with_extra_field=True))])
    new = _snap([_tool("a", output_schema=_union_schema(with_extra_field=False))])
    level, reasons = classify_change(old, new)
    assert level == "breaking"
    assert any("property removed" in r and "extra" in r for r in reasons)


def test_classify_added_field_inside_defs_is_breaking_when_required():
    """A newly-required field inside a $defs sub-schema is breaking."""
    old = _snap([_tool("a", output_schema=_union_schema(with_extra_field=False))])
    new = _snap([_tool("a", output_schema=_union_schema(with_extra_field=True))])
    level, reasons = classify_change(old, new)
    assert level == "breaking"
    assert any("newly required" in r and "extra" in r for r in reasons)
