import pytest

from dbt_mcp.config.transport import validate_transport


class TestValidateTransport:
    def test_valid_transports(self):
        assert validate_transport("stdio") == "stdio"
        assert validate_transport("sse") == "sse"
        assert validate_transport("streamable-http") == "streamable-http"

    def test_case_insensitive_and_whitespace(self):
        assert validate_transport("  STDIO  ") == "stdio"
        assert validate_transport("SSE") == "sse"

    def test_invalid_transport_raises_error(self):
        with pytest.raises(ValueError) as exc_info:
            validate_transport("invalid")

        assert "Invalid MCP_TRANSPORT: 'invalid'" in str(exc_info.value)
        assert "sse, stdio, streamable-http" in str(exc_info.value)
