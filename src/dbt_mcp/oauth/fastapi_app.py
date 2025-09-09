import logging
from pathlib import Path
from typing import Any, cast

import jwt
import requests
import yaml
from authlib.integrations.requests_client import OAuth2Session
from fastapi import FastAPI, Request
from fastapi.responses import RedirectResponse
from fastapi.staticfiles import StaticFiles
from jwt import PyJWKClient
from uvicorn import Server

from dbt_mcp.oauth.dbt_platform import (
    DbtPlatformAccount,
    DbtPlatformContext,
    DbtPlatformEnvironment,
    DbtPlatformEnvironmentResponse,
    DbtPlatformProject,
    SelectedProjectRequest,
)
from dbt_mcp.oauth.token import AccessTokenResponse, DecodedAccessToken

logger = logging.getLogger(__name__)


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


def _fetch_jwks_and_verify_token(
    access_token: str, dbt_platform_url: str
) -> dict[str, Any]:
    jwks_url = f"{dbt_platform_url}/.well-known/jwks.json"
    jwks_client = PyJWKClient(jwks_url)
    signing_key = jwks_client.get_signing_key_from_jwt(access_token)
    claims = jwt.decode(
        access_token,
        signing_key.key,
        algorithms=["RS256"],
        options={"verify_aud": False},
    )
    return claims


def create_app(
    *,
    oauth_client: OAuth2Session,
    state_to_verifier: dict[str, str],
    dbt_platform_url: str,
    static_dir: str,
    config_location: Path,
) -> FastAPI:
    app = FastAPI()

    app.state.decoded_access_token = cast(DecodedAccessToken | None, None)
    app.state.server_ref = cast(Server | None, None)
    app.state.dbt_platform_context = cast(DbtPlatformContext | None, None)
    app.state.state_to_verifier = cast(dict[str, str], state_to_verifier)

    def _read_dbt_platform_context() -> DbtPlatformContext:
        return DbtPlatformContext(**(yaml.safe_load(config_location.read_text())))

    def _update_dbt_platform_context(
        new_dbt_platform_context: DbtPlatformContext,
    ) -> DbtPlatformContext:
        existing_dbt_platform_context = _read_dbt_platform_context()
        next_dbt_platform_context = existing_dbt_platform_context.override(
            new_dbt_platform_context
        )
        app.state.dbt_platform_context = next_dbt_platform_context
        config_location.write_text(
            data=yaml.safe_dump(
                next_dbt_platform_context.model_dump(),
                sort_keys=True,
            )
        )
        return next_dbt_platform_context

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
            logger.info("Fetching access token")
            code_verifier = app.state.state_to_verifier.pop(state, None)
            if not code_verifier:
                logger.error("No code_verifier found for provided state")
                return RedirectResponse(url="/index.html#status=error", status_code=302)
            access_token_response = AccessTokenResponse(
                **oauth_client.fetch_token(
                    url=f"{dbt_platform_url}/oauth/token",
                    authorization_response=str(request.url),
                    code_verifier=code_verifier,
                )
            )
            logger.info("Access token fetched successfully")
            decoded_claims = _fetch_jwks_and_verify_token(
                access_token_response.access_token, dbt_platform_url
            )
            logger.info("JWT token verified successfully")
            app.state.decoded_access_token = DecodedAccessToken(
                access_token_response=access_token_response,
                decoded_claims=decoded_claims,
            )
            _update_dbt_platform_context(
                DbtPlatformContext(user_id=int(decoded_claims["sub"]))
            )
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
        return _read_dbt_platform_context()

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
        dbt_platform_context = _update_dbt_platform_context(
            new_dbt_platform_context=DbtPlatformContext(
                user_id=int(app.state.decoded_access_token.decoded_claims["sub"]),
                dev_environment=dev_environment,
                prod_environment=prod_environment,
                host_prefix=account.host_prefix,
            ),
        )
        return dbt_platform_context

    app.mount(
        path="/",
        app=StaticFiles(directory=static_dir, html=True),
    )

    return app
