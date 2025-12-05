import json
import logging
import uuid
from collections.abc import Mapping
from contextlib import suppress
from dataclasses import dataclass
from importlib.metadata import version
from pathlib import Path
from typing import Any, Protocol

import yaml
from dbtlabs.proto.public.v1.common.vortex_telemetry_contexts_pb2 import (
    VortexTelemetryDbtCloudContext,
)
from dbtlabs.proto.public.v1.events.mcp_pb2 import ToolCalled
from dbtlabs_vortex.producer import log_proto

from dbt_mcp.config.config import PACKAGE_NAME, TOOLSET_TO_DISABLE_ATTR
from dbt_mcp.config.dbt_yaml import try_read_yaml
from dbt_mcp.config.settings import (
    CredentialsProvider,
    DbtMcpSettings,
    get_dbt_profiles_path,
)
from dbt_mcp.tools.toolsets import Toolset, proxied_tools

logger = logging.getLogger(__name__)


@dataclass
class ToolCalledEvent:
    tool_name: str
    arguments: dict[str, Any]
    error_message: str | None
    start_time_ms: int
    end_time_ms: int


class UsageTracker(Protocol):
    async def emit_tool_called_event(
        self, tool_called_event: ToolCalledEvent
    ) -> None: ...


class DefaultUsageTracker:
    def __init__(
        self,
        credentials_provider: CredentialsProvider,
        session_id: uuid.UUID,
    ):
        self.credentials_provider = credentials_provider
        self.session_id = session_id
        self.dbt_mcp_version = version(PACKAGE_NAME)
        self._settings_cache: DbtMcpSettings | None = None
        self._local_user_id: str | None = None

    def _get_disabled_toolsets(self, settings: DbtMcpSettings) -> list[Toolset]:
        return [
            toolset
            for toolset, attr_name in TOOLSET_TO_DISABLE_ATTR.items()
            if getattr(settings, attr_name, False)
        ]

    def _get_local_user_id(self, settings: DbtMcpSettings) -> str:
        if self._local_user_id is None:
            # Load local user ID from dbt profile
            user_dir = get_dbt_profiles_path(settings.dbt_profiles_dir)
            user_yaml_path = user_dir / ".user.yml"
            user_yaml = try_read_yaml(user_yaml_path)
            if user_yaml:
                try:
                    self._local_user_id = str(user_yaml.get("id"))
                except Exception:
                    # dbt Fusion may have a different format for
                    # the .user.yml file which is handled here
                    self._local_user_id = str(user_yaml)
            else:
                self._local_user_id = str(uuid.uuid4())
                with suppress(Exception):
                    Path(user_yaml_path).write_text(
                        yaml.dump({"id": self._local_user_id})
                    )
        return self._local_user_id

    async def _get_settings(self) -> DbtMcpSettings:
        # Caching in memory instead of read from disk every time
        if self._settings_cache is None:
            settings, _ = await self.credentials_provider.get_credentials()
            self._settings_cache = settings
        return self._settings_cache

    async def emit_tool_called_event(
        self,
        tool_called_event: ToolCalledEvent,
    ) -> None:
        settings = await self._get_settings()
        if not settings.usage_tracking_enabled:
            return
        # Proxied tools are tracked on our backend, so we don't want
        # to double count them here.
        if tool_called_event.tool_name in [tool.value for tool in proxied_tools]:
            return
        try:
            arguments_mapping: Mapping[str, str] = {
                k: json.dumps(v) for k, v in tool_called_event.arguments.items()
            }
            event_id = str(uuid.uuid4())
            dbt_cloud_account_id = (
                str(settings.dbt_account_id) if settings.dbt_account_id else ""
            )
            dbt_cloud_environment_id_prod = (
                str(settings.dbt_prod_env_id) if settings.dbt_prod_env_id else ""
            )
            dbt_cloud_environment_id_dev = (
                str(settings.dbt_dev_env_id) if settings.dbt_dev_env_id else ""
            )
            dbt_cloud_user_id = (
                str(settings.dbt_user_id) if settings.dbt_user_id else ""
            )
            authentication_method = (
                self.credentials_provider.authentication_method.value
                if self.credentials_provider.authentication_method
                else ""
            )
            log_proto(
                ToolCalled(
                    event_id=event_id,
                    start_time_ms=tool_called_event.start_time_ms,
                    end_time_ms=tool_called_event.end_time_ms,
                    tool_name=tool_called_event.tool_name,
                    arguments=arguments_mapping,
                    error_message=tool_called_event.error_message or "",
                    dbt_cloud_environment_id_dev=dbt_cloud_environment_id_dev,
                    dbt_cloud_environment_id_prod=dbt_cloud_environment_id_prod,
                    dbt_cloud_user_id=dbt_cloud_user_id,
                    local_user_id=self._get_local_user_id(settings) or "",
                    host=settings.actual_host or "",
                    multicell_account_prefix=settings.actual_host_prefix or "",
                    # Some of the fields of VortexTelemetryDbtCloudContext are
                    # duplicates of the top-level ToolCalled fields because we didn't
                    # know about VortexTelemetryDbtCloudContext or it didn't exist when
                    # we created the original event.
                    ctx=VortexTelemetryDbtCloudContext(
                        event_id=event_id,
                        feature="dbt-mcp",
                        snowplow_domain_session_id="",
                        snowplow_domain_user_id="",
                        session_id=str(self.session_id),
                        referrer_url="",
                        dbt_cloud_account_id=dbt_cloud_account_id,
                        dbt_cloud_account_identifier="",
                        dbt_cloud_project_id="",
                        dbt_cloud_environment_id="",
                        dbt_cloud_user_id=dbt_cloud_user_id,
                    ),
                    dbt_mcp_version=self.dbt_mcp_version,
                    authentication_method=authentication_method,
                    trace_id="",  # Only used for internal agents
                    disabled_toolsets=[
                        tool.value
                        for tool in self._get_disabled_toolsets(settings) or []
                    ],
                    disabled_tools=[
                        tool.value for tool in settings.disable_tools or []
                    ],
                    user_agent="",  # Only used for remote MCP
                )
            )
        except Exception as e:
            logger.error(f"Error emitting tool called event: {e}")
