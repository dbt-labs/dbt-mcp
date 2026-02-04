import logging
from typing import cast
from urllib.parse import quote

from authlib.integrations.requests_client import OAuth2Session
from fastapi import FastAPI, Request
from fastapi.responses import RedirectResponse
from fastapi.staticfiles import StaticFiles
from starlette.types import Receive, Scope, Send
from uvicorn import Server

from dbt_mcp.config.config_providers import (
    AdminApiConfig,
    StaticConfigProvider,
)
from dbt_mcp.config.headers import AdminApiHeadersProvider
from dbt_mcp.dbt_admin.client import DbtAdminAPIClient
from dbt_mcp.errors.hints import with_multicell_hint
from dbt_mcp.oauth.context_manager import DbtPlatformContextManager
from dbt_mcp.oauth.dbt_platform import (
    DbtPlatformContext,
    DbtPlatformProject,
    SelectedProjectsRequest,
    dbt_platform_context_from_token_response,
)
from dbt_mcp.oauth.token import (
    DecodedAccessToken,
)
from dbt_mcp.oauth.token_provider import StaticTokenProvider
from dbt_mcp.project.project_resolver import (
    get_all_accounts,
    get_all_projects_for_account,
)

logger = logging.getLogger(__name__)


def error_redirect(error_code: str, description: str) -> RedirectResponse:
    return RedirectResponse(
        url=f"/index.html#status=error&error={quote(error_code)}&error_description={quote(description)}",
        status_code=302,
    )


class NoCacheStaticFiles(StaticFiles):
    """
    Custom StaticFiles class that adds cache-control headers to prevent caching.
    """

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        # Create a wrapper for the send function to modify headers
        async def send_wrapper(message):
            if message["type"] == "http.response.start":
                # Add no-cache headers to prevent client-side caching
                headers = dict(message.get("headers", []))
                headers[b"cache-control"] = b"no-cache, no-store, must-revalidate"
                headers[b"pragma"] = b"no-cache"
                headers[b"expires"] = b"0"
                message["headers"] = list(headers.items())
            await send(message)

        # Call the parent class with our modified send function
        await super().__call__(scope, receive, send_wrapper)


def create_app(
    *,
    oauth_client: OAuth2Session,
    state_to_verifier: dict[str, str],
    dbt_platform_url: str,
    static_dir: str,
    dbt_platform_context_manager: DbtPlatformContextManager,
) -> FastAPI:
    app = FastAPI()

    app.state.decoded_access_token = cast(DecodedAccessToken | None, None)
    app.state.server_ref = cast(Server | None, None)
    app.state.dbt_platform_context = cast(DbtPlatformContext | None, None)

    @app.get("/")
    def oauth_callback(request: Request) -> RedirectResponse:
        logger.info("OAuth callback received")
        # Only handle OAuth callback when provider returns with code or error.
        params = request.query_params
        if "error" in params or "error_description" in params:
            error_code = params.get("error", "unknown_error")
            error_desc = params.get("error_description", "An error occurred")
            return error_redirect(error_code, error_desc)
        if "code" not in params:
            return RedirectResponse(url="/index.html", status_code=302)
        state = params.get("state")
        if not state:
            logger.error("Missing state in OAuth callback")
            return error_redirect(
                "missing_state", "State parameter missing in OAuth callback"
            )
        try:
            code_verifier = state_to_verifier.pop(state, None)
            if not code_verifier:
                logger.error("No code_verifier found for provided state")
                return error_redirect(
                    "invalid_state", "Invalid or expired state parameter"
                )
            logger.info("Fetching initial access token")
            # Fetch the initial access token
            token_response = oauth_client.fetch_token(
                url=f"{dbt_platform_url}/oauth/token",
                authorization_response=str(request.url),
                code_verifier=code_verifier,
            )
            dbt_platform_context = dbt_platform_context_from_token_response(
                token_response, dbt_platform_url
            )
            dbt_platform_context_manager.write_context_to_file(dbt_platform_context)
            assert dbt_platform_context.decoded_access_token
            app.state.decoded_access_token = dbt_platform_context.decoded_access_token
            app.state.dbt_platform_context = dbt_platform_context
            return RedirectResponse(
                url="/index.html#status=success",
                status_code=302,
            )
        except Exception as e:
            logger.exception("OAuth callback failed")
            default_msg = "An unexpected error occurred during authentication"
            error_message = str(e) if str(e) else default_msg
            # Add hint for SSL errors (common with multi-cell misconfiguration)
            error_message = with_multicell_hint(error_message)
            return error_redirect("oauth_failed", error_message)

    @app.post("/shutdown")
    def shutdown_server() -> dict[str, bool]:
        logger.info("Shutdown server received")
        server = app.state.server_ref
        if server is not None:
            server.should_exit = True
        return {"ok": True}

    @app.get("/projects")
    async def projects() -> list[DbtPlatformProject]:
        if app.state.decoded_access_token is None:
            raise RuntimeError("Access token missing; OAuth flow not completed")
        access_token = app.state.decoded_access_token.access_token_response.access_token
        headers = {
            "Accept": "application/json",
            "Authorization": f"Bearer {access_token}",
        }
        accounts = await get_all_accounts(
            dbt_platform_url=dbt_platform_url,
            headers=headers,
        )
        projects: list[DbtPlatformProject] = []
        for account in [a for a in accounts if a.state == 1 and not a.locked]:
            projects.extend(
                await get_all_projects_for_account(
                    dbt_platform_url=dbt_platform_url,
                    account=account,
                    headers=headers,
                )
            )
        return projects

    @app.get("/dbt_platform_context")
    def get_dbt_platform_context() -> DbtPlatformContext:
        logger.info("Selected project received")
        return dbt_platform_context_manager.read_context() or DbtPlatformContext()

    @app.post("/selected_projects")
    async def set_selected_projects(
        selected_projects_request: SelectedProjectsRequest,
    ) -> DbtPlatformContext:
        logger.info("Selected projects received")
        if app.state.decoded_access_token is None:
            raise RuntimeError("Access token missing; OAuth flow not completed")
        access_token = app.state.decoded_access_token.access_token_response.access_token
        headers = {
            "Accept": "application/json",
            "Authorization": f"Bearer {access_token}",
        }
        accounts = await get_all_accounts(
            dbt_platform_url=dbt_platform_url,
            headers=headers,
        )
        account = next(
            (a for a in accounts if a.id == selected_projects_request.account_id),
            None,
        )
        if account is None:
            raise ValueError(
                f"Account {selected_projects_request.account_id} not found"
            )

        admin_client = DbtAdminAPIClient(
            StaticConfigProvider(
                config=AdminApiConfig(
                    url=dbt_platform_url,
                    headers_provider=AdminApiHeadersProvider(
                        token_provider=StaticTokenProvider(access_token)
                    ),
                    account_id=selected_projects_request.account_id,
                    prod_environment_id=None,
                )
            )
        )

        prod_environment = None
        dev_environment = None
        selected_project_ids = None
        if len(selected_projects_request.project_ids) == 1:
            # Single project: auto-detect environments and route to single-project
            # tools (no project_id param) by leaving selected_project_ids unset.
            environments = await admin_client.fetch_project_environment_responses(
                selected_projects_request.project_ids[0],
                page_size=100,
            )
            prod_environment, dev_environment = DbtAdminAPIClient.resolve_environments(
                environments
            )
        else:
            # Multiple projects: set selected_project_ids so the dispatcher routes
            # to multi-project tools (with project_id param).
            selected_project_ids = selected_projects_request.project_ids

        dbt_platform_context = dbt_platform_context_manager.update_context(
            new_dbt_platform_context=DbtPlatformContext(
                decoded_access_token=app.state.decoded_access_token,
                host_prefix=account.host_prefix,
                account_id=account.id,
                selected_project_ids=selected_project_ids,
                prod_environment=prod_environment,
                dev_environment=dev_environment,
            ),
        )
        app.state.dbt_platform_context = dbt_platform_context
        return dbt_platform_context

    app.mount(
        path="/",
        app=NoCacheStaticFiles(directory=static_dir, html=True),
    )

    return app
