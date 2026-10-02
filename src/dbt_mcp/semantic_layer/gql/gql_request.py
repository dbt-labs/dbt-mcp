import httpx

from dbt_mcp.config.config_providers import SemanticLayerConfig
from dbt_mcp.config.settings import SEMANTIC_LAYER_GQL_TIMEOUT
from dbt_mcp.gql.errors import raise_gql_error
from dbt_mcp.http import API_REQUEST_GATE, BOUNDED_RESPONSE_HOOKS


async def submit_request(
    sl_config: SemanticLayerConfig,
    payload: dict,
    timeout: float = SEMANTIC_LAYER_GQL_TIMEOUT,
) -> dict:
    if "variables" not in payload:
        payload["variables"] = {}
    payload["variables"]["environmentId"] = sl_config.prod_environment_id

    async with (
        API_REQUEST_GATE.enter(),
        httpx.AsyncClient(
            headers={"Accept-Encoding": "gzip, deflate"},
            timeout=timeout,
            event_hooks=BOUNDED_RESPONSE_HOOKS,
        ) as client,
    ):
        response = await client.post(
            sl_config.url,
            json=payload,
            headers=sl_config.headers_provider.get_headers(),
        )
        response.raise_for_status()
        result = response.json()
        raise_gql_error(result)
        return result
