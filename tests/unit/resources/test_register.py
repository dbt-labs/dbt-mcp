"""Unit tests for resource registration."""

from unittest.mock import MagicMock, call, patch

from dbt_mcp.resources.definitions import ResourceDefinition
from dbt_mcp.resources.resources import register_resources


def test_register_resources_basic():
    """Test basic resource registration."""
    mock_fastmcp = MagicMock()

    def content_fn() -> str:
        return "test content"

    resource_def = ResourceDefinition(
        uri="test://resource",
        name="Test Resource",
        description="A test resource",
        mime_type="text/plain",
        content_fn=content_fn,
    )

    with patch(
        "dbt_mcp.resources.resources.FunctionResource"
    ) as mock_function_resource:
        mock_resource_instance = MagicMock()
        mock_function_resource.from_function.return_value = mock_resource_instance

        register_resources(mock_fastmcp, [resource_def])

        # Verify FunctionResource.from_function was called with correct args
        mock_function_resource.from_function.assert_called_once_with(
            fn=content_fn,
            uri="test://resource",
            name="Test Resource",
            description="A test resource",
            mime_type="text/plain",
        )

        # Verify add_resource was called with the created resource
        mock_fastmcp.add_resource.assert_called_once_with(mock_resource_instance)


def test_register_multiple_resources():
    """Test registering multiple resources."""
    mock_fastmcp = MagicMock()

    def content_fn_1() -> str:
        return "content 1"

    def content_fn_2() -> str:
        return "content 2"

    resource_defs = [
        ResourceDefinition(
            uri="test://resource1",
            name="Resource 1",
            description="First resource",
            mime_type="text/plain",
            content_fn=content_fn_1,
        ),
        ResourceDefinition(
            uri="test://resource2",
            name="Resource 2",
            description="Second resource",
            mime_type="application/json",
            content_fn=content_fn_2,
        ),
    ]

    with patch(
        "dbt_mcp.resources.resources.FunctionResource"
    ) as mock_function_resource:
        mock_resource_1 = MagicMock()
        mock_resource_2 = MagicMock()
        mock_function_resource.from_function.side_effect = [
            mock_resource_1,
            mock_resource_2,
        ]

        register_resources(mock_fastmcp, resource_defs)

        # Verify both resources were created
        assert mock_function_resource.from_function.call_count == 2

        # Verify both were registered
        assert mock_fastmcp.add_resource.call_count == 2
        mock_fastmcp.add_resource.assert_has_calls(
            [call(mock_resource_1), call(mock_resource_2)]
        )


def test_register_empty_list():
    """Test registering an empty list of resources."""
    mock_fastmcp = MagicMock()

    register_resources(mock_fastmcp, [])

    # Should not call add_resource for empty list
    mock_fastmcp.add_resource.assert_not_called()


def test_register_resources_logs_info(caplog):
    """Test that resource registration logs info messages."""
    mock_fastmcp = MagicMock()

    def content_fn() -> str:
        return "test"

    resource_def = ResourceDefinition(
        uri="test://logged",
        name="Logged",
        description="Logged resource",
        mime_type="text/plain",
        content_fn=content_fn,
    )

    with patch("dbt_mcp.resources.resources.FunctionResource"):
        with caplog.at_level("INFO"):
            register_resources(mock_fastmcp, [resource_def])

        # Check that logging occurred
        assert "Registering resource: test://logged" in caplog.text
