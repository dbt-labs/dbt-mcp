from dataclasses import dataclass
from typing import Any

from mcp.server.fastmcp import FastMCP

from dbt_mcp.config.config_providers import AdminApiConfig, ConfigProvider
from dbt_mcp.dbt_admin.onboarding.client import OnboardingClient
from dbt_mcp.dbt_admin.onboarding.models import (
    OnboardingApplyResult,
    OnboardingGetResult,
    OnboardingModel,
    OnboardingValidateResult,
)
from dbt_mcp.prompts.prompts import get_prompt
from dbt_mcp.tools.definitions import dbt_mcp_tool
from dbt_mcp.tools.register import register_tools
from dbt_mcp.tools.tool_names import ToolName
from dbt_mcp.tools.toolsets import Toolset


@dataclass
class OnboardingToolContext:
    admin_api_config_provider: ConfigProvider[AdminApiConfig]
    onboarding_client: OnboardingClient


@dbt_mcp_tool(
    description=get_prompt("admin_api/onboarding_get"),
    title="Get Onboarding Status",
    read_only_hint=True,
    destructive_hint=False,
    idempotent_hint=True,
)
async def dbt_admin_onboarding_get(
    context: OnboardingToolContext,
) -> OnboardingGetResult:
    """Return the current onboarding record, or null if none has been started."""
    config = await context.admin_api_config_provider.get_config()
    data = await context.onboarding_client.get(config.account_id)
    return OnboardingGetResult(
        onboarding=OnboardingModel.from_api(data) if data is not None else None
    )


@dbt_mcp_tool(
    description=get_prompt("admin_api/onboarding_validate"),
    title="Validate Onboarding Data",
    read_only_hint=True,
    destructive_hint=False,
    idempotent_hint=True,
)
async def dbt_admin_onboarding_validate(
    context: OnboardingToolContext,
    data: dict[str, Any],
) -> OnboardingValidateResult:
    """Validate onboarding data without applying it; returns what is missing or invalid."""
    config = await context.admin_api_config_provider.get_config()
    raw = await context.onboarding_client.validate(config.account_id, data)
    status = raw.get("status", {})
    if status.get("is_success"):
        return OnboardingValidateResult(valid=True)
    developer_message = status.get("developer_message", "")
    errors = [developer_message] if developer_message else []
    return OnboardingValidateResult(valid=False, errors=errors)


@dbt_mcp_tool(
    description=get_prompt("admin_api/onboarding_apply"),
    title="Apply Onboarding Data",
    read_only_hint=False,
    destructive_hint=False,
    idempotent_hint=True,
)
async def dbt_admin_onboarding_apply(
    context: OnboardingToolContext,
    data: dict[str, Any],
) -> OnboardingApplyResult:
    """Submit collected onboarding data; safe to call multiple times with partial data."""
    config = await context.admin_api_config_provider.get_config()
    raw = await context.onboarding_client.apply(config.account_id, data)
    return OnboardingApplyResult(onboarding=OnboardingModel.from_api(raw))


ONBOARDING_TOOLS = [
    dbt_admin_onboarding_get,
    dbt_admin_onboarding_validate,
    dbt_admin_onboarding_apply,
]


def register_onboarding_tools(
    dbt_mcp: FastMCP,
    admin_api_config_provider: ConfigProvider[AdminApiConfig],
    *,
    disabled_tools: set[ToolName],
    enabled_tools: set[ToolName] | None,
    enabled_toolsets: set[Toolset],
    disabled_toolsets: set[Toolset],
) -> None:
    """Register dbt onboarding tools."""
    onboarding_client = OnboardingClient(admin_api_config_provider)

    def bind_context() -> OnboardingToolContext:
        return OnboardingToolContext(
            admin_api_config_provider=admin_api_config_provider,
            onboarding_client=onboarding_client,
        )

    register_tools(
        dbt_mcp,
        tool_definitions=[
            tool.adapt_context(bind_context) for tool in ONBOARDING_TOOLS
        ],
        disabled_tools=disabled_tools,
        enabled_tools=enabled_tools,
        enabled_toolsets=enabled_toolsets,
        disabled_toolsets=disabled_toolsets,
    )
