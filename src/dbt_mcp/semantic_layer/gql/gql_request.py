import requests

from dbt_mcp.config.config import SemanticLayerConfig
from dbt_mcp.gql.errors import raise_gql_error


def submit_request(
    sl_config: SemanticLayerConfig,
    payload: dict,
) -> dict:
    if "variables" not in payload:
        payload["variables"] = {}
    payload["variables"]["environmentId"] = sl_config.prod_environment_id
    r = requests.post(sl_config.url, json=payload, headers=sl_config.headers)
    result = r.json()
    raise_gql_error(result)
    return result
