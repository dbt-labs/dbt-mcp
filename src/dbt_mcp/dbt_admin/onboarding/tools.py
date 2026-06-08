from dataclasses import dataclass

from mcp.server.fastmcp import FastMCP

from dbt_mcp.config.config_providers import AdminApiConfig, ConfigProvider
from dbt_mcp.dbt_admin.onboarding.client import OnboardingClient
from dbt_mcp.dbt_admin.onboarding.models import (
    OnboardingInitResult,
    OnboardingModel,
    OnboardingStateResult,
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
    description=get_prompt("admin_api/onboarding_init"),
    title="Start or Resume Onboarding",
    read_only_hint=False,
    destructive_hint=False,
    idempotent_hint=True,
)
async def dbt_admin_onboarding_init(
    context: OnboardingToolContext,
) -> OnboardingInitResult:
    """Start or resume an onboarding session for the account."""
    config = await context.admin_api_config_provider.get_config()
    account_id = config.account_id

    existing = await context.onboarding_client.get(account_id)
    if existing is not None:
        return OnboardingInitResult(
            onboarding=OnboardingModel.from_api(existing),
            created=False,
        )

    data = await context.onboarding_client.create_or_get(account_id)
    return OnboardingInitResult(
        onboarding=OnboardingModel.from_api(data),
        created=True,
    )


@dbt_mcp_tool(
    description=get_prompt("admin_api/onboarding_state"),
    title="Get Onboarding State",
    read_only_hint=True,
    destructive_hint=False,
    idempotent_hint=True,
)
async def dbt_admin_onboarding_state(
    context: OnboardingToolContext,
) -> OnboardingStateResult:
    """Return the current onboarding state for the account."""
    config = await context.admin_api_config_provider.get_config()
    account_id = config.account_id

    data = await context.onboarding_client.get(account_id)
    if data is None:
        return OnboardingStateResult(onboarding=None)

    return OnboardingStateResult(onboarding=OnboardingModel.from_api(data))


ONBOARDING_TOOLS = [
    dbt_admin_onboarding_init,
    dbt_admin_onboarding_state,
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
