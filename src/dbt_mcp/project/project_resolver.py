import logging

import httpx

from dbt_mcp.oauth.dbt_platform import (
    DbtPlatformAccount,
    DbtPlatformProject,
)

logger = logging.getLogger(__name__)


async def get_all_accounts(
    *,
    dbt_platform_url: str,
    headers: dict[str, str],
) -> list[DbtPlatformAccount]:
    async with httpx.AsyncClient() as client:
        response = await client.get(
            url=f"{dbt_platform_url}/api/v3/accounts/",
            headers=headers,
        )
        response.raise_for_status()
        data = response.json()
    return [DbtPlatformAccount(**account) for account in data["data"]]


async def get_all_projects_for_account(
    *,
    dbt_platform_url: str,
    account: DbtPlatformAccount,
    headers: dict[str, str],
    page_size: int = 100,
) -> list[DbtPlatformProject]:
    """Fetch all projects for an account using offset/page_size pagination."""
    offset = 0
    projects: list[DbtPlatformProject] = []
    async with httpx.AsyncClient() as client:
        while True:
            response = await client.get(
                f"{dbt_platform_url}/api/v3/accounts/{account.id}/projects/?state=1&offset={offset}&limit={page_size}",
                headers=headers,
            )
            response.raise_for_status()
            page = response.json()["data"]
            projects.extend(
                DbtPlatformProject(**project, account_name=account.name)
                for project in page
            )
            if len(page) < page_size:
                break
            offset += page_size
    return projects
