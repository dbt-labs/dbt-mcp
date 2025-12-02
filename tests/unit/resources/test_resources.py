"""Unit tests for concrete resource implementations."""

from dbt_mcp.resources.definitions import ResourceDefinition
from dbt_mcp.resources.resources import list_resource_definitions


def test_list_resource_definitions_returns_list():
    """Test that list_resource_definitions returns a list."""
    resources = list_resource_definitions()

    assert isinstance(resources, list)
    assert len(resources) > 0


def test_list_resource_definitions_all_valid():
    """Test that all resources in the list are valid ResourceDefinitions."""
    resources = list_resource_definitions()

    for resource in resources:
        assert isinstance(resource, ResourceDefinition)
        assert resource.uri is not None
        assert resource.name is not None
        assert resource.description is not None
        assert resource.mime_type is not None
        assert callable(resource.content_fn)


def test_default_styleguide_resource_exists():
    """Test that the default styleguide resource is registered."""
    resources = list_resource_definitions()

    styleguide_resource = next(
        (r for r in resources if r.uri == "dbt://default-styleguide"), None
    )

    assert styleguide_resource is not None
    assert styleguide_resource.name == "dbt_default_styleguide"
    assert styleguide_resource.mime_type == "text/plain"
    assert "dbt styleguide" in styleguide_resource.description.lower()


def test_default_styleguide_content():
    """Test that the default styleguide resource returns valid content."""
    resources = list_resource_definitions()

    styleguide_resource = next(
        (r for r in resources if r.uri == "dbt://default-styleguide"), None
    )

    assert styleguide_resource is not None

    # Call the content function
    content = styleguide_resource.content_fn()

    # Verify content is a non-empty string
    assert isinstance(content, str)
    assert len(content) > 0

    # Verify it contains expected style guide content
    assert "## Naming fields and tables" in content
    assert "## Styling SQL" in content
    assert "## Styling Jinja" in content
    assert "## Styling YAML" in content

    # Verify some specific guidelines are present
    assert "snake_case" in content
    assert "Use trailing commas" in content
    assert "{{ ref() }}" in content
    assert "{{ source() }}" in content
