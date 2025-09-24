import logging
from typing import cast

import requests
from authlib.integrations.requests_client import OAuth2Session
from fastapi import FastAPI, Request
from fastapi.responses import RedirectResponse
from fastapi.staticfiles import StaticFiles
from starlette.types import Receive, Scope, Send
from uvicorn import Server

from dbt_mcp.oauth.context_manager import DbtPlatformContextManager
from dbt_mcp.oauth.dbt_platform import (
    DbtPlatformAccount,
    DbtPlatformContext,
    DbtPlatformEnvironment,
    DbtPlatformEnvironmentResponse,
    DbtPlatformProject,
    SelectedProjectRequest,
    dbt_platform_context_from_token_response,
)
from dbt_mcp.oauth.token import (
    DecodedAccessToken,
)

logger = logging.getLogger(__name__)


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


def _get_all_accounts(
    *,
    dbt_platform_url: str,
    headers: dict[str, str],
) -> list[DbtPlatformAccount]:
    accounts_response = requests.get(
        url=f"{dbt_platform_url}/api/v3/accounts/",
        headers=headers,
    )
    accounts_response.raise_for_status()
    return [
        DbtPlatformAccount(**account) for account in accounts_response.json()["data"]
    ]


def _get_all_projects_for_account(
    *,
    dbt_platform_url: str,
    account: DbtPlatformAccount,
    headers: dict[str, str],
    page_size: int = 100,
) -> list[DbtPlatformProject]:
    """Fetch all projects for an account using offset/page_size pagination."""
    offset = 0
    projects: list[DbtPlatformProject] = []
    while True:
        projects_response = requests.get(
            f"{dbt_platform_url}/api/v3/accounts/{account.id}/projects/?state=1&offset={offset}&limit={page_size}",
            headers=headers,
        )
        projects_response.raise_for_status()
        page = projects_response.json()["data"]
        projects.extend(
            DbtPlatformProject(**project, account_name=account.name) for project in page
        )
        if len(page) < page_size:
            break
        offset += page_size
    return projects


def _get_all_environments_for_project(
    *,
    dbt_platform_url: str,
    account_id: int,
    project_id: int,
    headers: dict[str, str],
    page_size: int = 100,
) -> list[DbtPlatformEnvironmentResponse]:
    """Fetch all environments for a project using offset/page_size pagination."""
    offset = 0
    environments: list[DbtPlatformEnvironmentResponse] = []
    while True:
        environments_response = requests.get(
            f"{dbt_platform_url}/api/v3/accounts/{account_id}/projects/{project_id}/environments/?state=1&offset={offset}&limit={page_size}",
            headers=headers,
        )
        environments_response.raise_for_status()
        page = environments_response.json()["data"]
        environments.extend(
            DbtPlatformEnvironmentResponse(**environment) for environment in page
        )
        if len(page) < page_size:
            break
        offset += page_size
    return environments


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
        if "error" in params:
            return RedirectResponse(url="/index.html#status=error", status_code=302)
        if "code" not in params:
            return RedirectResponse(url="/index.html", status_code=302)
        state = params.get("state")
        if not state:
            logger.error("Missing state in OAuth callback")
            return RedirectResponse(url="/index.html#status=error", status_code=302)
        try:
            code_verifier = state_to_verifier.pop(state, None)
            if not code_verifier:
                logger.error("No code_verifier found for provided state")
                return RedirectResponse(url="/index.html#status=error", status_code=302)
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
        except Exception:
            logger.exception("OAuth callback failed")
            return RedirectResponse(url="/index.html#status=error", status_code=302)

    @app.post("/shutdown")
    def shutdown_server() -> dict[str, bool]:
        logger.info("Shutdown server received")
        server = app.state.server_ref
        if server is not None:
            server.should_exit = True
        return {"ok": True}

    @app.get("/projects")
    def projects() -> list[DbtPlatformProject]:
        if app.state.decoded_access_token is None:
            raise RuntimeError("Access token missing; OAuth flow not completed")
        access_token = app.state.decoded_access_token.access_token_response.access_token
        headers = {
            "Accept": "application/json",
            "Authorization": f"Bearer {access_token}",
        }
        accounts = _get_all_accounts(
            dbt_platform_url=dbt_platform_url,
            headers=headers,
        )
        projects: list[DbtPlatformProject] = []
        for account in [a for a in accounts if a.state == 1 and not a.locked]:
            projects.extend(
                _get_all_projects_for_account(
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

    @app.post("/selected_project")
    def set_selected_project(
        selected_project_request: SelectedProjectRequest,
    ) -> DbtPlatformContext:
        logger.info("Selected project received")
        if app.state.decoded_access_token is None:
            raise RuntimeError("Access token missing; OAuth flow not completed")
        access_token = app.state.decoded_access_token.access_token_response.access_token
        headers = {
            "Accept": "application/json",
            "Authorization": f"Bearer {access_token}",
        }
        accounts = _get_all_accounts(
            dbt_platform_url=dbt_platform_url,
            headers=headers,
        )
        account = next(
            (a for a in accounts if a.id == selected_project_request.account_id), None
        )
        if account is None:
            raise ValueError(f"Account {selected_project_request.account_id} not found")
        environments = _get_all_environments_for_project(
            dbt_platform_url=dbt_platform_url,
            account_id=selected_project_request.account_id,
            project_id=selected_project_request.project_id,
            headers=headers,
            page_size=100,
        )
        prod_environment = None
        dev_environment = None
        for environment in environments:
            if (
                environment.deployment_type
                and environment.deployment_type.lower() == "production"
            ):
                prod_environment = DbtPlatformEnvironment(
                    id=environment.id,
                    name=environment.name,
                    deployment_type=environment.deployment_type,
                )
            elif (
                environment.deployment_type
                and environment.deployment_type.lower() == "development"
            ):
                dev_environment = DbtPlatformEnvironment(
                    id=environment.id,
                    name=environment.name,
                    deployment_type=environment.deployment_type,
                )
        dbt_platform_context = dbt_platform_context_manager.update_context(
            new_dbt_platform_context=DbtPlatformContext(
                decoded_access_token=app.state.decoded_access_token,
                dev_environment=dev_environment,
                prod_environment=prod_environment,
                host_prefix=account.host_prefix,
                account_id=account.id,
            ),
        )
        app.state.dbt_platform_context = dbt_platform_context
        return dbt_platform_context

    app.mount(
        path="/",
        app=NoCacheStaticFiles(directory=static_dir, html=True),
    )

    return app
