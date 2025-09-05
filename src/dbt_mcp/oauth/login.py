import errno
import logging
import secrets
import webbrowser
from importlib import resources
from pathlib import Path

from authlib.integrations.requests_client import OAuth2Session
from pydantic import BaseModel
from uvicorn import Config, Server

from dbt_mcp.oauth.dbt_platform import DbtPlatformContext
from dbt_mcp.oauth.fastapi_app import create_app
from dbt_mcp.oauth.logging import disable_server_logs
from dbt_mcp.oauth.token import DecodedAccessToken

logger = logging.getLogger(__name__)


class LoginResult(BaseModel):
    decoded_access_token: DecodedAccessToken
    dbt_platform_context: DbtPlatformContext


def login(
    *,
    dbt_platform_url: str,
    port: int,
    client_id: str,
    config_location: Path,
) -> LoginResult:
    """Start OAuth login flow with PKCE using authlib and return
    the decoded access token
    """
    # OAuth2 configuration
    redirect_uri = f"http://localhost:{port}"
    authorization_endpoint = f"{dbt_platform_url}/oauth/authorize"

    # 'offline_access' scope indicates that we want to request a refresh token
    # 'user_access' is equivalent to a PAT
    scope = "user_access offline_access"

    # Create OAuth2Session with PKCE support
    client = OAuth2Session(
        client_id=client_id,
        redirect_uri=redirect_uri,
        scope=scope,
        code_challenge_method="S256",
    )

    # Generate code_verifier
    code_verifier = secrets.token_urlsafe(32)

    # Generate authorization URL with PKCE
    authorization_url, state = client.create_authorization_url(
        url=authorization_endpoint,
        code_verifier=code_verifier,
    )

    try:
        # Resolve static assets directory from package
        package_root = resources.files("dbt_mcp")
        packaged_dist = package_root / "ui" / "dist"
        if not packaged_dist.is_dir():
            raise FileNotFoundError(f"{packaged_dist} not found in packaged resources")
        static_dir = str(packaged_dist)

        # Create FastAPI app and Uvicorn server
        app = create_app(
            oauth_client=client,
            state_to_verifier={state: code_verifier},
            dbt_platform_url=dbt_platform_url,
            static_dir=static_dir,
            config_location=config_location,
        )
        config = Config(
            app=app,
            host="127.0.0.1",
            port=port,
        )
        server = Server(config)
        app.state.server_ref = server

        logger.info("Opening authorization URL")
        webbrowser.open(authorization_url)
        # Logs have to be disabled because they mess up stdio MCP communication
        disable_server_logs()
        server.run()

        if not app.state.decoded_access_token or not app.state.dbt_platform_context:
            raise ValueError("Undefined login state")
        logger.info("Login successful")
        return LoginResult(
            decoded_access_token=app.state.decoded_access_token,
            dbt_platform_context=app.state.dbt_platform_context,
        )
    except OSError as e:
        if e.errno == errno.EADDRINUSE:
            logger.error(f"Error: Port {port} is already in use.")
        raise
