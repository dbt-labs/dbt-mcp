from dbt_mcp.discovery.tools import register_discovery_tools
from dbt_mcp.discovery.tools_multiproject import (
    register_multiproject_discovery_tools,
)
from tests.mocks.config import (
    MockDiscoveryConfigProvider,
    MockMultiProjectDiscoveryConfigProvider,
)

DEPRECATED_TOOLS = ["get_model_parents", "get_model_children"]


def _register_single_project(mock_fastmcp):
    fastmcp, _ = mock_fastmcp
    register_discovery_tools(
        fastmcp,
        MockDiscoveryConfigProvider(),
        disabled_tools=set(),
        enabled_tools=None,
        enabled_toolsets=set(),
        disabled_toolsets=set(),
    )
    return fastmcp


def _register_multiproject(mock_fastmcp):
    fastmcp, _ = mock_fastmcp
    register_multiproject_discovery_tools(
        fastmcp,
        MockMultiProjectDiscoveryConfigProvider(),
        disabled_tools=set(),
        enabled_tools=None,
        enabled_toolsets=set(),
        disabled_toolsets=set(),
    )
    return fastmcp


def test_single_project_deprecated_tools_carry_deprecation_signal(mock_fastmcp):
    fastmcp = _register_single_project(mock_fastmcp)

    for tool_name in DEPRECATED_TOOLS:
        kwargs = fastmcp.tool_kwargs[tool_name]
        assert kwargs["meta"]["deprecated"] is True
        assert kwargs["meta"]["replacement"] == "get_lineage"
        assert kwargs["description"].startswith("**DEPRECATED")


def test_multiproject_deprecated_tools_carry_deprecation_signal(mock_fastmcp):
    fastmcp = _register_multiproject(mock_fastmcp)

    for tool_name in DEPRECATED_TOOLS:
        kwargs = fastmcp.tool_kwargs[tool_name]
        assert kwargs["meta"]["deprecated"] is True
        assert kwargs["meta"]["replacement"] == "get_lineage"
        assert kwargs["description"].startswith("**DEPRECATED")


def test_deprecated_tools_are_still_registered(mock_fastmcp):
    """Deprecation must not remove the tools — they remain registered."""
    fastmcp = _register_single_project(mock_fastmcp)
    for tool_name in DEPRECATED_TOOLS:
        assert tool_name in fastmcp.tools


def test_non_deprecated_tool_has_no_deprecation_meta(mock_fastmcp):
    """get_lineage (the replacement) must not be marked deprecated."""
    fastmcp = _register_single_project(mock_fastmcp)
    meta = fastmcp.tool_kwargs["get_lineage"].get("meta")
    assert not (meta and meta.get("deprecated"))
    assert not fastmcp.tool_kwargs["get_lineage"]["description"].startswith(
        "**DEPRECATED"
    )


def test_deprecated_tool_description_is_trimmed(mock_fastmcp):
    """The deprecated description should be a short, blunt line — not the

    original prompt body prepended with a banner. A shorter description makes
    the model less likely to pick the tool, which speeds the usage soak.
    """
    fastmcp = _register_single_project(mock_fastmcp)
    description = fastmcp.tool_kwargs["get_model_parents"]["description"]
    assert "Retrieves the parent models" not in description
    assert len(description) < 200
