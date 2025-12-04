import httpx

from dbt_mcp.config.config_providers import SemanticLayerConfig
from dbt_mcp.gql.errors import raise_gql_error


async def submit_request(
    sl_config: SemanticLayerConfig,
    payload: dict,
) -> dict:
    if "variables" not in payload:
        payload["variables"] = {}
    payload["variables"]["environmentId"] = sl_config.prod_environment_id

    async with httpx.AsyncClient() as client:
        response = await client.post(
            sl_config.url,
            json=payload,
            headers=sl_config.headers_provider.get_headers(),
        )
        response.raise_for_status()
        result = response.json()
        raise_gql_error(result)
        return result
