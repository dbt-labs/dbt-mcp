"""Unit tests for resource definitions."""

from dbt_mcp.resources.definitions import ResourceDefinition, dbt_mcp_resource


def test_resource_definition_creation():
    """Test creating a ResourceDefinition."""

    def content_fn() -> str:
        return "test content"

    resource = ResourceDefinition(
        uri="test://resource",
        name="Test Resource",
        description="A test resource",
        mime_type="text/plain",
        content_fn=content_fn,
    )

    assert resource.uri == "test://resource"
    assert resource.name == "Test Resource"
    assert resource.description == "A test resource"
    assert resource.mime_type == "text/plain"
    assert resource.get_uri() == "test://resource"
    assert resource.get_content() == "test content"


def test_resource_definition_get_content():
    """Test that get_content calls the content function."""

    def content_fn() -> str:
        return "dynamic content"

    resource = ResourceDefinition(
        uri="test://dynamic",
        name="Dynamic",
        description="Dynamic resource",
        mime_type="text/plain",
        content_fn=content_fn,
    )

    content = resource.get_content()
    assert content == "dynamic content"


def test_dbt_mcp_resource_decorator():
    """Test the dbt_mcp_resource decorator."""

    @dbt_mcp_resource(
        uri="test://decorated",
        name="Decorated Resource",
        description="A decorated resource",
        mime_type="application/json",
    )
    def my_resource() -> str:
        return '{"key": "value"}'

    assert isinstance(my_resource, ResourceDefinition)
    assert my_resource.uri == "test://decorated"
    assert my_resource.name == "Decorated Resource"
    assert my_resource.description == "A decorated resource"
    assert my_resource.mime_type == "application/json"
    assert my_resource.get_content() == '{"key": "value"}'


def test_dbt_mcp_resource_decorator_default_mime_type():
    """Test that the decorator uses text/plain as default mime_type."""

    @dbt_mcp_resource(
        uri="test://default",
        name="Default",
        description="Default resource",
    )
    def my_resource() -> str:
        return "text content"

    assert my_resource.mime_type == "text/plain"


def test_resource_with_complex_content_function():
    """Test resource with a more complex content function."""
    call_count = 0

    def content_fn() -> str:
        nonlocal call_count
        call_count += 1
        return f"Called {call_count} times"

    resource = ResourceDefinition(
        uri="test://counter",
        name="Counter",
        description="Counter resource",
        mime_type="text/plain",
        content_fn=content_fn,
    )

    # Each call should increment the counter
    assert resource.get_content() == "Called 1 times"
    assert resource.get_content() == "Called 2 times"
    assert resource.get_content() == "Called 3 times"
