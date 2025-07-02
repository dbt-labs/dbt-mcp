import os
from dataclasses import dataclass
from pathlib import Path

import yaml
from dotenv import load_dotenv


@dataclass
class TrackingConfig:
    host: str | None
    multicell_account_prefix: str | None
    prod_environment_id: int | None
    dev_environment_id: int | None
    dbt_cloud_user_id: int | None
    local_user_id: str | None


@dataclass
class SemanticLayerConfig:
    multicell_account_prefix: str | None
    host: str
    prod_environment_id: int
    service_token: str


@dataclass
class DiscoveryConfig:
    url: str
    headers: dict[str, str]
    environment_id: int


@dataclass
class DbtCliConfig:
    project_dir: str
    dbt_path: str


@dataclass
class RemoteConfig:
    multicell_account_prefix: str | None
    host: str
    user_id: int
    dev_environment_id: int
    prod_environment_id: int
    token: str


@dataclass
class Config:
    tracking_config: TrackingConfig
    remote_config: RemoteConfig | None
    dbt_cli_config: DbtCliConfig | None
    discovery_config: DiscoveryConfig | None
    semantic_layer_config: SemanticLayerConfig | None


def load_config() -> Config:
    load_dotenv()

    host = os.environ.get("DBT_HOST")
    cursor_host = os.environ.get("DBT_MCP_HOST")
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

    # set default warn error options if not provided
    if os.environ.get("DBT_WARN_ERROR_OPTIONS") is None:
        warn_error_options = '{"error": ["NoNodesForSelectionCriteria"]}'
        os.environ["DBT_WARN_ERROR_OPTIONS"] = warn_error_options

    # Devon: I messed up and uploaded the wrong
    # env var here https://docs.cursor.com/tools.
    # So, we are supporting this alias now.
    actual_host = host or cursor_host

    errors = []
    if not disable_semantic_layer or not disable_discovery or not disable_remote:
        if not actual_host:
            errors.append(
                "DBT_HOST environment variable is required when semantic layer, discovery, or remote tools are enabled."
            )
        if not prod_environment_id and not legacy_prod_environment_id:
            errors.append(
                "DBT_PROD_ENV_ID environment variable is required when semantic layer, discovery, or remote tools are enabled."
            )
        if not token:
            errors.append(
                "DBT_TOKEN environment variable is required when semantic layer, discovery, or remote tools are enabled."
            )
        if actual_host and (
            actual_host.startswith("metadata")
            or actual_host.startswith("semantic-layer")
        ):
            errors.append(
                "DBT_HOST must not start with 'metadata' or 'semantic-layer'."
            )
    if not disable_remote:
        if not dev_environment_id:
            errors.append(
                "DBT_DEV_ENV_ID environment variable is required when remote tools are enabled."
            )
        if not user_id:
            errors.append(
                "DBT_USER_ID environment variable is required when remote tools are enabled."
            )
    if not disable_dbt_cli:
        if not project_dir:
            errors.append(
                "DBT_PROJECT_DIR environment variable is required when dbt CLI tools are enabled."
            )
        if not dbt_path:
            errors.append(
                "DBT_PATH environment variable is required when dbt CLI tools are enabled."
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
        and actual_host
    ):
        remote_config = RemoteConfig(
            multicell_account_prefix=multicell_account_prefix,
            user_id=int(user_id),
            token=token,
            dev_environment_id=int(dev_environment_id),
            prod_environment_id=actual_prod_environment_id,
            host=actual_host,
        )

    dbt_cli_config = None
    if not disable_dbt_cli and project_dir and dbt_path:
        dbt_cli_config = DbtCliConfig(
            project_dir=project_dir,
            dbt_path=dbt_path,
        )

    discovery_config = None
    if not disable_discovery and actual_host and actual_prod_environment_id and token:
        if multicell_account_prefix:
            url = f"https://{multicell_account_prefix}.metadata.{actual_host}/graphql"
        else:
            url = f"https://metadata.{actual_host}/graphql"
        discovery_config = DiscoveryConfig(
            url=url,
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
            },
            environment_id=actual_prod_environment_id,
        )

    semantic_layer_config = None
    if (
        not disable_semantic_layer
        and actual_host
        and actual_prod_environment_id
        and token
    ):
        semantic_layer_config = SemanticLayerConfig(
            multicell_account_prefix=multicell_account_prefix,
            host=actual_host,
            prod_environment_id=actual_prod_environment_id,
            service_token=token,
        )

    local_user_id = None
    try:
        home = os.environ.get("HOME")
        user_path = Path(f"{home}/.dbt/.user.yml")
        if home and user_path.exists():
            with open(user_path) as f:
                local_user_id = yaml.safe_load(f).get("id")
    except Exception:
        pass

    return Config(
        tracking_config=TrackingConfig(
            host=actual_host,
            multicell_account_prefix=multicell_account_prefix,
            prod_environment_id=actual_prod_environment_id,
            dev_environment_id=int(dev_environment_id) if dev_environment_id else None,
            dbt_cloud_user_id=int(user_id) if user_id else None,
            local_user_id=local_user_id,
        ),
        remote_config=remote_config,
        dbt_cli_config=dbt_cli_config,
        discovery_config=discovery_config,
        semantic_layer_config=semantic_layer_config,
    )
