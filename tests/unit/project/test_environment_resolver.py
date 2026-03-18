"""Tests for environment resolution logic."""

from dbt_mcp.oauth.dbt_platform import DbtPlatformEnvironmentResponse
from dbt_mcp.project.environment_resolver import resolve_environments


def test_resolve_prod_and_dev_environments():
    """Resolve both prod and dev from environment list."""
    environments = [
        DbtPlatformEnvironmentResponse(
            id=1, name="Production", deployment_type="production"
        ),
        DbtPlatformEnvironmentResponse(
            id=2, name="Development", deployment_type="development"
        ),
        DbtPlatformEnvironmentResponse(id=3, name="Staging", deployment_type="staging"),
    ]
    prod, dev = resolve_environments(environments)
    assert prod is not None
    assert prod.id == 1
    assert prod.name == "Production"
    assert dev is not None
    assert dev.id == 2
    assert dev.name == "Development"


def test_resolve_no_prod_environment():
    """Returns None for prod when no production environment exists."""
    environments = [
        DbtPlatformEnvironmentResponse(
            id=2, name="Development", deployment_type="development"
        ),
    ]
    prod, dev = resolve_environments(environments)
    assert prod is None
    assert dev is not None
    assert dev.id == 2


def test_resolve_no_dev_environment():
    """Returns None for dev when no development environment exists."""
    environments = [
        DbtPlatformEnvironmentResponse(
            id=1, name="Production", deployment_type="production"
        ),
    ]
    prod, dev = resolve_environments(environments)
    assert prod is not None
    assert prod.id == 1
    assert dev is None


def test_resolve_empty_environments():
    """Returns None for both when environment list is empty."""
    prod, dev = resolve_environments([])
    assert prod is None
    assert dev is None


def test_resolve_with_explicit_prod_environment_id():
    """When prod_environment_id is given, use that specific environment."""
    environments = [
        DbtPlatformEnvironmentResponse(
            id=1, name="Production", deployment_type="production"
        ),
        DbtPlatformEnvironmentResponse(id=3, name="Staging", deployment_type="staging"),
        DbtPlatformEnvironmentResponse(
            id=2, name="Development", deployment_type="development"
        ),
    ]
    prod, dev = resolve_environments(environments, prod_environment_id=3)
    assert prod is not None
    assert prod.id == 3
    assert prod.name == "Staging"
    assert dev is not None
    assert dev.id == 2
