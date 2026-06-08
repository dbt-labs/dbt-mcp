from dataclasses import dataclass

from mcp.server.fastmcp import FastMCP

from dbt_mcp.config.config_providers import AdminApiConfig, ConfigProvider
from dbt_mcp.dbt_admin.onboarding.client import OnboardingClient
from dbt_mcp.dbt_admin.onboarding.decision_points import get_decision_points
from dbt_mcp.dbt_admin.onboarding.models import (
    OnboardingInitResult,
    OnboardingStateResult,
    ServerOnboardingState,
    decision_points_to_dicts,
)
from dbt_mcp.dbt_admin.onboarding.session import InMemorySessionStore
from dbt_mcp.prompts.prompts import get_prompt
from dbt_mcp.tools.definitions import dbt_mcp_tool
from dbt_mcp.tools.register import register_tools
from dbt_mcp.tools.tool_names import ToolName
from dbt_mcp.tools.toolsets import Toolset


@dataclass
class OnboardingToolContext:
    admin_api_config_provider: ConfigProvider[AdminApiConfig]
    session_store: InMemorySessionStore
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

    session = await context.session_store.get_or_create(account_id)

    return OnboardingInitResult(
        session_id=session.session_id,
        phase=session.phase.value,
        account_id=account_id,
        decision_points=decision_points_to_dicts(get_decision_points()),
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
    """Return the current onboarding session state for the account."""
    config = await context.admin_api_config_provider.get_config()
    account_id = config.account_id

    session = context.session_store.get(account_id)

    if session is None:
        return OnboardingStateResult(
            phase=None,
            session_id=None,
            server_state=None,
        )

    server_state: ServerOnboardingState | None = None
    if session.has_server_state():
        raw = await context.onboarding_client.get_state(account_id)
        data = raw.get("data") or {}
        server_state = ServerOnboardingState(
            status=data.get("status", raw.get("status", "unknown")),
            data=data,
        )

    return OnboardingStateResult(
        phase=session.phase.value,
        session_id=session.session_id,
        server_state=server_state,
    )


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
    session_store = InMemorySessionStore()
    onboarding_client = OnboardingClient(admin_api_config_provider)

    def bind_context() -> OnboardingToolContext:
        return OnboardingToolContext(
            admin_api_config_provider=admin_api_config_provider,
            session_store=session_store,
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
