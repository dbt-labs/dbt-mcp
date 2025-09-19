import errno
import logging
import secrets
import webbrowser
from importlib import resources

from authlib.integrations.requests_client import OAuth2Session
from uvicorn import Config, Server

from dbt_mcp.oauth.client_id import OAUTH_CLIENT_ID
from dbt_mcp.oauth.context_manager import DbtPlatformContextManager
from dbt_mcp.oauth.dbt_platform import DbtPlatformContext
from dbt_mcp.oauth.fastapi_app import create_app
from dbt_mcp.oauth.logging import disable_server_logs

logger = logging.getLogger(__name__)


async def login(
    *,
    dbt_platform_url: str,
    port: int,
    dbt_platform_context_manager: DbtPlatformContextManager,
) -> DbtPlatformContext:
    """Start OAuth login flow with PKCE using authlib and return
    the decoded access token
    """
    # 'offline_access' scope indicates that we want to request a refresh token
    # 'user_access' is equivalent to a PAT
    scope = "user_access offline_access"

    # Create OAuth2Session with PKCE support
    client = OAuth2Session(
        client_id=OAUTH_CLIENT_ID,
        redirect_uri=f"http://localhost:{port}",
        scope=scope,
        code_challenge_method="S256",
    )

    # Generate code_verifier
    code_verifier = secrets.token_urlsafe(32)

    # Generate authorization URL with PKCE
    authorization_url, state = client.create_authorization_url(
        url=f"{dbt_platform_url}/oauth/authorize",
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
            dbt_platform_context_manager=dbt_platform_context_manager,
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
        await server.serve()

        if not app.state.dbt_platform_context:
            raise ValueError("Undefined login state")
        logger.info("Login successful")
        return app.state.dbt_platform_context
    except OSError as e:
        if e.errno == errno.EADDRINUSE:
            logger.error(f"Error: Port {port} is already in use.")
        raise
