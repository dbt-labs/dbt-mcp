import logging
import secrets
import threading
import time
import webbrowser
from collections.abc import AsyncIterator, Sequence
from contextlib import (
    asynccontextmanager,
)
from importlib import resources
from pathlib import Path
from typing import Any

import uvicorn
from authlib.integrations.requests_client.oauth2_session import OAuth2Session
from dbtlabs_vortex.producer import shutdown
from mcp.server.auth.provider import AccessToken, TokenVerifier
from mcp.server.auth.settings import AuthSettings
from mcp.server.fastmcp import FastMCP
from mcp.types import (
    ContentBlock,
    TextContent,
)
from pydantic import AnyHttpUrl

from dbt_mcp.config.config import Config
from dbt_mcp.dbt_admin.tools import register_admin_api_tools
from dbt_mcp.dbt_cli.tools import register_dbt_cli_tools
from dbt_mcp.discovery.tools import register_discovery_tools
from dbt_mcp.oauth.fastapi_app import create_app
from dbt_mcp.semantic_layer.tools import register_sl_tools
from dbt_mcp.sql.tools import SqlToolsManager
from dbt_mcp.tracking.tracking import UsageTracker

logger = logging.getLogger(__name__)


@asynccontextmanager
async def app_lifespan(server: FastMCP) -> AsyncIterator[None]:
    logger.info("Starting MCP server")
    try:
        yield
    except Exception as e:
        logger.error(f"Error in MCP server: {e}")
        raise e
    finally:
        logger.info("Shutting down MCP server")
        try:
            await SqlToolsManager.close()
        except Exception:
            logger.exception("Error closing SQL tools manager")
        try:
            shutdown()
        except Exception:
            logger.exception("Error shutting down MCP server")


class DbtMCP(FastMCP):
    def __init__(
        self,
        config: Config,
        usage_tracker: UsageTracker,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.usage_tracker = usage_tracker
        self.config = config

    async def call_tool(
        self, name: str, arguments: dict[str, Any]
    ) -> Sequence[ContentBlock] | dict[str, Any]:
        logger.info(f"Calling tool: {name}")
        result = None
        start_time = int(time.time() * 1000)
        try:
            result = await super().call_tool(
                name,
                arguments,
            )
        except Exception as e:
            end_time = int(time.time() * 1000)
            logger.error(
                f"Error calling tool: {name} with arguments: {arguments} "
                + f"in {end_time - start_time}ms: {e}"
            )
            self.usage_tracker.emit_tool_called_event(
                config=self.config.tracking_config,
                tool_name=name,
                arguments=arguments,
                start_time_ms=start_time,
                end_time_ms=end_time,
                error_message=str(e),
            )
            return [
                TextContent(
                    type="text",
                    text=str(e),
                )
            ]
        end_time = int(time.time() * 1000)
        logger.info(f"Tool {name} called successfully in {end_time - start_time}ms")
        self.usage_tracker.emit_tool_called_event(
            config=self.config.tracking_config,
            tool_name=name,
            arguments=arguments,
            start_time_ms=start_time,
            end_time_ms=end_time,
            error_message=None,
        )
        return result


class SimpleTokenVerifier(TokenVerifier):
    """Simple token verifier for demonstration."""

    async def verify_token(self, token: str) -> AccessToken | None:
        print(token)
        return None


def create_dbt_mcp(config: Config) -> FastMCP:
    # OAuth2 configuration
    dbt_platform_url = "https://rr558.us.staging.dbt.com"
    redirect_uri = "http://localhost:8000"
    authorization_endpoint = f"{dbt_platform_url}/oauth/authorize"

    # 'offline_access' scope indicates that we want to request a refresh token
    # 'user_access' is equivalent to a PAT
    scope = "user_access offline_access"

    # Create OAuth2Session with PKCE support
    client = OAuth2Session(
        client_id="34ec61e834cdffd9dd90a32231937821",
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

    # Resolve static assets directory from package
    package_root = resources.files("dbt_mcp")
    packaged_dist = package_root / "ui" / "dist"
    if not packaged_dist.is_dir():
        raise FileNotFoundError(f"{packaged_dist} not found in packaged resources")
    static_dir = str(packaged_dist)
    app = create_app(
        oauth_client=client,
        state_to_verifier={state: code_verifier},
        dbt_platform_url=dbt_platform_url,
        static_dir=static_dir,
        config_location=Path("~/.dbt/mcp.yml"),
    )

    # Start uvicorn server in background thread
    def run_server():
        uvicorn.run(
            app,
            host="127.0.0.1",
            port=8000,
            reload=False,  # Disable reload in background thread to avoid issues
        )

    server_thread = threading.Thread(target=run_server, daemon=True)
    server_thread.start()
    logger.info("Started uvicorn server in background thread on http://127.0.0.1:8000")

    dbt_mcp = DbtMCP(
        config=config,
        usage_tracker=UsageTracker(),
        name="dbt",
        lifespan=app_lifespan,
        token_verifier=SimpleTokenVerifier(),
        # Auth settings for RFC 9728 Protected Resource Metadata
        auth=AuthSettings(
            issuer_url=AnyHttpUrl(url=authorization_url),  # Authorization Server URL
            resource_server_url=AnyHttpUrl(
                "http://localhost:8000"
            ),  # This server's URL
            required_scopes=["user_access", "offline_access"],
        ),
    )
    webbrowser.open(authorization_url)

    if config.semantic_layer_config:
        logger.info("Registering semantic layer tools")
        register_sl_tools(dbt_mcp, config.semantic_layer_config, config.disable_tools)

    if config.discovery_config:
        logger.info("Registering discovery tools")
        register_discovery_tools(dbt_mcp, config.discovery_config, config.disable_tools)

    if config.dbt_cli_config:
        logger.info("Registering dbt cli tools")
        register_dbt_cli_tools(dbt_mcp, config.dbt_cli_config, config.disable_tools)

    if config.admin_api_config:
        logger.info("Registering dbt admin API tools")
        register_admin_api_tools(dbt_mcp, config.admin_api_config, config.disable_tools)

    # if config.sql_config:
    #     logger.info("Registering SQL tools")
    #     await register_sql_tools(dbt_mcp, config.sql_config, config.disable_tools)

    @dbt_mcp.tool()
    def sum(a: int, b: int) -> int:
        """Add two numbers together."""
        return a + b

    return dbt_mcp
