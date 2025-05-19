import os
from dataclasses import dataclass

from dotenv import load_dotenv


@dataclass
class SemanticLayerConfig:
    multicell_account_prefix: str | None
    host: str
    prod_environment_id: int
    service_token: str


@dataclass
class DiscoveryConfig:
    multicell_account_prefix: str | None
    host: str
    environment_id: int
    token: str


@dataclass
class DbtCliConfig:
    project_dir: str
    dbt_path: str


@dataclass
class RemoteConfig:
    user_id: int
    dev_environment_id: int
    prod_environment_id: int
    token: str
    remote_mcp_base_url: str


@dataclass
class Config:
    remote_config: RemoteConfig | None
    dbt_cli_config: DbtCliConfig | None
    discovery_config: DiscoveryConfig | None
    semantic_layer_config: SemanticLayerConfig | None


def load_config() -> Config:
    load_dotenv()

    host = os.environ.get("DBT_HOST")
    prod_environment_id = os.environ.get("DBT_PROD_ENV_ID")
    legacy_prod_environment_id = os.environ.get("DBT_ENV_ID")
    dev_environment_id = os.environ.get("DBT_DEV_ENV_ID")
    user_id = os.environ.get("DBT_USER_ID")
    token = os.environ.get("DBT_TOKEN")
    project_dir = os.environ.get("DBT_PROJECT_DIR")
    dbt_path = os.environ.get("DBT_PATH", "dbt")
    disable_dbt_cli = os.environ.get("DISABLE_DBT_CLI", "false") == "true"
    disable_semantic_layer = os.environ.get("DISABLE_SEMANTIC_LAYER", "false") == "true"
    disable_discovery = os.environ.get("DISABLE_DISCOVERY", "false") == "true"
    disable_remote = os.environ.get("DISABLE_REMOTE", "true") == "true"
    multicell_account_prefix = os.environ.get("MULTICELL_ACCOUNT_PREFIX", None)

    errors = []
    if not disable_semantic_layer or not disable_discovery:
        if not host:
            errors.append(
                "DBT_HOST environment variable is required when semantic layer or discovery is enabled."
            )
        if not prod_environment_id and not legacy_prod_environment_id:
            errors.append(
                "DBT_PROD_ENV_ID environment variable is required when semantic layer or discovery is enabled."
            )
        if not token:
            errors.append(
                "DBT_TOKEN environment variable is required when semantic layer or discovery is enabled."
            )
        if host and (host.startswith("metadata") or host.startswith("semantic-layer")):
            errors.append(
                "DBT_HOST must not start with 'metadata' or 'semantic-layer'."
            )
    if not disable_remote:
        if not dev_environment_id:
            errors.append(
                "DBT_DEV_ENV_ID environment variable is required when remote MCP is enabled."
            )
        if not user_id:
            errors.append(
                "DBT_USER_ID environment variable is required when remote MCP is enabled."
            )
    if not disable_dbt_cli:
        if not project_dir:
            errors.append(
                "DBT_PROJECT_DIR environment variable is required when dbt CLI MCP is enabled."
            )
        if not dbt_path:
            errors.append(
                "DBT_PATH environment variable is required when dbt CLI MCP is enabled."
            )

    if errors:
        raise ValueError("Errors found in configuration:\n\n" + "\n".join(errors))

    # Allowing for configuration with legacy DBT_ENV_ID environment variable
    actual_prod_environment_id = (
        int(prod_environment_id)
        if prod_environment_id
        else int(legacy_prod_environment_id)
        if legacy_prod_environment_id
        else None
    )

    remote_config = None
    if (
        not disable_remote
        and user_id
        and token
        and dev_environment_id
        and actual_prod_environment_id
        and host
    ):
        remote_config = RemoteConfig(
            user_id=int(user_id),
            token=token,
            dev_environment_id=int(dev_environment_id),
            prod_environment_id=actual_prod_environment_id,
            remote_mcp_base_url=(
                "http://" if host and host.startswith("localhost") else "https://"
            )
            + f"{host}/mcp",
        )

    dbt_cli_config = None
    if not disable_dbt_cli and project_dir and dbt_path:
        dbt_cli_config = DbtCliConfig(
            project_dir=project_dir,
            dbt_path=dbt_path,
        )

    discovery_config = None
    if not disable_dbt_cli and host and actual_prod_environment_id and token:
        discovery_config = DiscoveryConfig(
            multicell_account_prefix=multicell_account_prefix,
            host=host,
            environment_id=actual_prod_environment_id,
            token=token,
        )

    semantic_layer_config = None
    if not disable_semantic_layer and host and actual_prod_environment_id and token:
        semantic_layer_config = SemanticLayerConfig(
            multicell_account_prefix=multicell_account_prefix,
            host=host,
            prod_environment_id=actual_prod_environment_id,
            service_token=token,
        )

    return Config(
        remote_config=remote_config,
        dbt_cli_config=dbt_cli_config,
        discovery_config=discovery_config,
        semantic_layer_config=semantic_layer_config,
    )
