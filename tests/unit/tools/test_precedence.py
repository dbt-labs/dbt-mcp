"""Unit tests for tool registration precedence logic."""

from dbt_mcp.tools.register import should_register_tool
from dbt_mcp.tools.tool_names import ToolName
from dbt_mcp.tools.toolsets import Toolset


class TestShouldRegisterTool:
    """Test the should_register_tool precedence logic."""

    def test_precedence_1_individual_enable_highest(self):
        """Test that individual enable has highest precedence."""
        # Enable query_metrics individually, but disable the entire toolset
        result = should_register_tool(
            tool_name=ToolName.QUERY_METRICS,
            enabled_tools={ToolName.QUERY_METRICS},
            disabled_tools=set(),
            enabled_toolsets=set(),
            disabled_toolsets={Toolset.SEMANTIC_LAYER},
        )
        assert result is True, "Individual enable should override toolset disable"

    def test_precedence_2_individual_disable_overrides_toolset_enable(self):
        """Test that individual disable overrides toolset enable."""
        # Enable semantic layer toolset, but disable query_metrics specifically
        result = should_register_tool(
            tool_name="query_metrics",
            enabled_tools=set(),
            disabled_tools={ToolName.QUERY_METRICS},
            enabled_toolsets={Toolset.SEMANTIC_LAYER},
            disabled_toolsets=set(),
        )
        assert result is False, "Individual disable should override toolset enable"

    def test_precedence_3_toolset_enable_works(self):
        """Test that toolset enable enables all tools in toolset."""
        result = should_register_tool(
            tool_name=ToolName.QUERY_METRICS,
            enabled_tools=set(),
            disabled_tools=set(),
            enabled_toolsets={Toolset.SEMANTIC_LAYER},
            disabled_toolsets=set(),
        )
        assert result is True, "Toolset enable should enable tool"

    def test_precedence_4_toolset_disable_works(self):
        """Test that toolset disable disables all tools in toolset."""
        result = should_register_tool(
            tool_name=ToolName.QUERY_METRICS,
            enabled_tools=set(),
            disabled_tools=set(),
            enabled_toolsets=set(),
            disabled_toolsets={Toolset.SEMANTIC_LAYER},
        )
        assert result is False, "Toolset disable should disable tool"

    def test_precedence_5_default_enabled_when_no_enables_set(self):
        """Test that default is enabled when no explicit enables configured."""
        result = should_register_tool(
            tool_name=ToolName.QUERY_METRICS,
            enabled_tools=set(),
            disabled_tools=set(),
            enabled_toolsets=set(),
            disabled_toolsets=set(),
        )
        assert result is True, "Default should be enabled when no enables set"

    def test_precedence_5_default_disabled_when_enables_exist(self):
        """Test that default is disabled when any explicit enables exist."""
        # Enable a different tool, this tool should be disabled by default
        result = should_register_tool(
            tool_name=ToolName.QUERY_METRICS,
            enabled_tools={ToolName.LIST_METRICS},  # Different tool enabled
            disabled_tools=set(),
            enabled_toolsets=set(),
            disabled_toolsets=set(),
        )
        assert result is False, "Default should be disabled when other tools enabled"

    def test_precedence_5_default_disabled_when_toolset_enabled(self):
        """Test that default is disabled when any toolset is enabled."""
        # Enable a different toolset, this tool's toolset not enabled
        result = should_register_tool(
            tool_name=ToolName.QUERY_METRICS,  # SEMANTIC_LAYER tool
            enabled_tools=set(),
            disabled_tools=set(),
            enabled_toolsets={Toolset.ADMIN_API},  # Different toolset
            disabled_toolsets=set(),
        )
        assert result is False, "Default should be disabled when any toolset enabled"

    def test_complex_scenario_toolset_with_exclusion(self):
        """Test enabling toolset but disabling specific tool within it."""
        # Enable semantic layer, but disable get_dimensions
        result_enabled = should_register_tool(
            tool_name=ToolName.QUERY_METRICS,
            enabled_tools=set(),
            disabled_tools={ToolName.GET_DIMENSIONS},
            enabled_toolsets={Toolset.SEMANTIC_LAYER},
            disabled_toolsets=set(),
        )
        assert result_enabled is True, "query_metrics should be enabled"

        result_disabled = should_register_tool(
            tool_name=ToolName.GET_DIMENSIONS,
            enabled_tools=set(),
            disabled_tools={ToolName.GET_DIMENSIONS},
            enabled_toolsets={Toolset.SEMANTIC_LAYER},
            disabled_toolsets=set(),
        )
        assert result_disabled is False, "get_dimensions should be disabled"

    def test_complex_scenario_multiple_toolsets(self):
        """Test enabling multiple toolsets."""
        # Enable both semantic layer and admin API
        for tool_name in [
            ToolName.QUERY_METRICS,
            ToolName.TRIGGER_JOB_RUN,
        ]:
            result = should_register_tool(
                tool_name=ToolName(tool_name),
                enabled_tools=set(),
                disabled_tools=set(),
                enabled_toolsets={Toolset.SEMANTIC_LAYER, Toolset.ADMIN_API},
                disabled_toolsets=set(),
            )
            assert result is True, f"{tool_name} should be enabled"

        # Tool from different toolset should be disabled
        result = should_register_tool(
            tool_name=ToolName.GET_ALL_MODELS,  # Discovery tool
            enabled_tools=set(),
            disabled_tools=set(),
            enabled_toolsets={Toolset.SEMANTIC_LAYER, Toolset.ADMIN_API},
            disabled_toolsets=set(),
        )
        assert result is False, "Discovery tool should be disabled"

    def test_individual_enable_overrides_everything(self):
        """Test that individual enable overrides all other settings."""
        # Even with toolset disabled AND tool in disabled_tools, individual enable wins
        result = should_register_tool(
            tool_name=ToolName.QUERY_METRICS,
            enabled_tools={ToolName.QUERY_METRICS},  # Precedence 1
            disabled_tools={ToolName.QUERY_METRICS},  # Precedence 2 (ignored)
            enabled_toolsets=set(),
            disabled_toolsets={Toolset.SEMANTIC_LAYER},  # Precedence 4 (ignored)
        )
        assert result is True, "Individual enable should override everything"
