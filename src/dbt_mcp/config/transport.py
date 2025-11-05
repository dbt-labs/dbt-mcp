import logging
from typing import Literal

logger = logging.getLogger(__name__)

TransportType = Literal["stdio", "sse", "streamable-http"]
VALID_TRANSPORTS: set[TransportType] = {"stdio", "sse", "streamable-http"}


def validate_transport(transport: str) -> TransportType:
    """Validate and return the MCP transport type."""
    transport = transport.strip().lower()

    if transport not in VALID_TRANSPORTS:
        valid_options = ", ".join(sorted(VALID_TRANSPORTS))
        raise ValueError(
            f"Invalid MCP_TRANSPORT: '{transport}'. Must be one of: {valid_options}"
        )

    logger.debug(f"Using MCP transport: {transport}")
    return transport  # type: ignore[return-value]
