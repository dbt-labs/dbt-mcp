from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from mcp.server.fastmcp import FastMCP

from dbt_mcp.config.config_providers import AdminApiConfig, ConfigProvider
from dbt_mcp.dbt_admin.onboarding.account_client import AccountClient
from dbt_mcp.dbt_admin.onboarding.client import OnboardingClient
from dbt_mcp.dbt_admin.onboarding.models import (
    AccountCreateResult,
    OnboardingApplyResult,
    OnboardingGetResult,
    OnboardingModel,
    OnboardingValidateResult,
)
from dbt_mcp.oauth.token_provider import StaticTokenProvider
from dbt_mcp.prompts.prompts import get_prompt
from dbt_mcp.tools.definitions import dbt_mcp_tool
from dbt_mcp.tools.register import register_tools
from dbt_mcp.tools.tool_names import ToolName
from dbt_mcp.tools.toolsets import Toolset

if TYPE_CHECKING:
    from dbt_mcp.config.credentials import CredentialsProvider


@dataclass
class OnboardingToolContext:
    admin_api_config_provider: ConfigProvider[AdminApiConfig]
    onboarding_client: OnboardingClient
    credentials_provider: "CredentialsProvider"
    account_client: AccountClient


@dbt_mcp_tool(
    description=get_prompt("admin_api/account_create"),
    title="Create dbt Platform Account",
    read_only_hint=False,
    destructive_hint=False,
    idempotent_hint=False,
)
async def dbt_admin_account_create(
    context: OnboardingToolContext,
    name: str,
    owner_email: str,
    created_via: str | None = None,
) -> AccountCreateResult:
    """Bootstrap a brand-new dbt platform account and owner token.

    Use this only when the user has no account yet — it is billable and creates
    a trial account. The returned owner token is stashed for the rest of this
    session, so subsequent admin/onboarding tools authenticate automatically.
    """
    data = await context.account_client.create(
        name=name,
        owner_email=owner_email,
        created_via=created_via,
    )
    account_id = int(data["account_id"])
    owner_token = data["owner_token"]

    # Stash the new identity so later account-scoped tools authenticate.
    context.credentials_provider.settings.dbt_account_id = account_id
    context.credentials_provider.token_provider = StaticTokenProvider(token=owner_token)

    return AccountCreateResult(account_id=account_id, owner_token=owner_token)


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
    dbt_admin_account_create,
    dbt_admin_onboarding_get,
    dbt_admin_onboarding_validate,
    dbt_admin_onboarding_apply,
]


def register_onboarding_tools(
    dbt_mcp: FastMCP,
    admin_api_config_provider: ConfigProvider[AdminApiConfig],
    credentials_provider: "CredentialsProvider",
    *,
    disabled_tools: set[ToolName],
    enabled_tools: set[ToolName] | None,
    enabled_toolsets: set[Toolset],
    disabled_toolsets: set[Toolset],
) -> None:
    """Register dbt onboarding tools."""
    onboarding_client = OnboardingClient(admin_api_config_provider)
    account_client = AccountClient(credentials_provider)

    def bind_context() -> OnboardingToolContext:
        return OnboardingToolContext(
            admin_api_config_provider=admin_api_config_provider,
            onboarding_client=onboarding_client,
            credentials_provider=credentials_provider,
            account_client=account_client,
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
