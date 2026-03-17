import logging

import httpx

from dbt_mcp.oauth.dbt_platform import (
    DbtPlatformEnvironment,
    DbtPlatformEnvironmentResponse,
)

logger = logging.getLogger(__name__)


async def _get_all_environments_for_project(
    *,
    dbt_platform_url: str,
    account_id: int,
    project_id: int,
    headers: dict[str, str],
    page_size: int = 100,
) -> list[DbtPlatformEnvironmentResponse]:
    """Fetch all environments for a project using offset/page_size pagination."""
    offset = 0
    environments: list[DbtPlatformEnvironmentResponse] = []
    async with httpx.AsyncClient() as client:
        while True:
            response = await client.get(
                f"{dbt_platform_url}/api/v3/accounts/{account_id}/projects/{project_id}/environments/?state=1&offset={offset}&limit={page_size}",
                headers=headers,
            )
            response.raise_for_status()
            page = response.json()["data"]
            environments.extend(
                DbtPlatformEnvironmentResponse(**environment) for environment in page
            )
            if len(page) < page_size:
                break
            offset += page_size
    return environments


def resolve_environments(
    environments: list[DbtPlatformEnvironmentResponse],
    *,
    prod_environment_id: int | None = None,
) -> tuple[DbtPlatformEnvironment | None, DbtPlatformEnvironment | None]:
    """Resolve prod and dev environments from a list of environment responses.

    Returns a tuple of (prod_environment, dev_environment).

    If prod_environment_id is provided, that specific environment is used as prod.
    Otherwise, auto-detects based on deployment_type == "production".
    Dev environment is always auto-detected based on deployment_type == "development".
    """
    prod_environment: DbtPlatformEnvironment | None = None
    dev_environment: DbtPlatformEnvironment | None = None

    if prod_environment_id:
        for environment in environments:
            if environment.id == prod_environment_id:
                prod_environment = DbtPlatformEnvironment(
                    id=environment.id,
                    name=environment.name,
                    deployment_type=environment.deployment_type or "production",
                )
                break
    else:
        for environment in environments:
            if (
                environment.deployment_type
                and environment.deployment_type.lower() == "production"
            ):
                prod_environment = DbtPlatformEnvironment(
                    id=environment.id,
                    name=environment.name,
                    deployment_type=environment.deployment_type,
                )
                break

    for environment in environments:
        if (
            environment.deployment_type
            and environment.deployment_type.lower() == "development"
        ):
            dev_environment = DbtPlatformEnvironment(
                id=environment.id,
                name=environment.name,
                deployment_type=environment.deployment_type,
            )
            break

    return prod_environment, dev_environment


async def get_environments_for_project(
    *,
    dbt_platform_url: str,
    account_id: int,
    project_id: int,
    headers: dict[str, str],
    prod_environment_id: int | None = None,
) -> tuple[DbtPlatformEnvironment | None, DbtPlatformEnvironment | None]:
    """Fetch environments for a project and resolve prod/dev.

    Returns a tuple of (prod_environment, dev_environment).
    """
    environments = await _get_all_environments_for_project(
        dbt_platform_url=dbt_platform_url,
        account_id=account_id,
        project_id=project_id,
        headers=headers,
    )
    return resolve_environments(
        environments,
        prod_environment_id=prod_environment_id,
    )
