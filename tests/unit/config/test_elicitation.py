"""Tests for the general-purpose MCP elicitation infrastructure."""

from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import yaml

from dbt_mcp.config.elicitation import (
    ConfigPersistence,
    elicit_or_raise,
    get_mcp_session,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_session(*, elicitation_supported: bool, client_params_none: bool = False):
    """Build a mock ServerSession with controllable elicitation capability."""
    session = MagicMock()
    if client_params_none:
        session.client_params = None
    else:
        caps = MagicMock()
        caps.elicitation = MagicMock() if elicitation_supported else None
        session.client_params = MagicMock()
        session.client_params.capabilities = caps
    return session


def _make_request_context(session, request_id="req-1"):
    """Build a mock RequestContext."""
    ctx = MagicMock()
    ctx.session = session
    ctx.request_id = request_id
    return ctx


# ---------------------------------------------------------------------------
# TestGetMcpSession
# ---------------------------------------------------------------------------


class TestGetMcpSession:
    def test_returns_none_outside_request_context(self):
        """No contextvar set → None."""
        ctx_var = MagicMock()
        ctx_var.get.return_value = None

        with patch("mcp.server.lowlevel.server.request_ctx", ctx_var):
            result = get_mcp_session()

        assert result is None
        ctx_var.get.assert_called_once_with(None)

    def test_returns_none_when_no_elicitation_capability(self):
        session = _make_session(elicitation_supported=False)
        ctx = _make_request_context(session)

        with patch("mcp.server.lowlevel.server.request_ctx") as mock_ctx_var:
            mock_ctx_var.get.return_value = ctx
            result = get_mcp_session()

        assert result is None

    def test_returns_none_when_client_params_is_none(self):
        session = _make_session(elicitation_supported=False, client_params_none=True)
        ctx = _make_request_context(session)

        with patch("mcp.server.lowlevel.server.request_ctx") as mock_ctx_var:
            mock_ctx_var.get.return_value = ctx
            result = get_mcp_session()

        assert result is None

    def test_returns_session_when_elicitation_supported(self):
        session = _make_session(elicitation_supported=True)
        ctx = _make_request_context(session, request_id="req-42")

        with patch("mcp.server.lowlevel.server.request_ctx") as mock_ctx_var:
            mock_ctx_var.get.return_value = ctx
            result = get_mcp_session()

        assert result is not None
        returned_session, returned_request_id = result
        assert returned_session is session
        assert returned_request_id == "req-42"


# ---------------------------------------------------------------------------
# TestElicitOrRaise
# ---------------------------------------------------------------------------


class TestElicitOrRaise:
    @pytest.mark.asyncio
    async def test_raises_original_error_when_no_session(self):
        original = ValueError("something is missing")

        with patch("dbt_mcp.config.elicitation.get_mcp_session", return_value=None):
            with pytest.raises(ValueError) as exc_info:
                await elicit_or_raise(original, MagicMock, "provide it")

        assert exc_info.value is original

    @pytest.mark.asyncio
    async def test_raises_original_error_when_user_declines(self):
        from mcp.server.elicitation import DeclinedElicitation

        original = ValueError("need config")
        session = _make_session(elicitation_supported=True)

        with (
            patch(
                "dbt_mcp.config.elicitation.get_mcp_session",
                return_value=(session, "req-1"),
            ),
            patch(
                "dbt_mcp.config.elicitation.elicit_with_validation",
                new_callable=AsyncMock,
                return_value=DeclinedElicitation(),
            ),
        ):
            with pytest.raises(ValueError) as exc_info:
                await elicit_or_raise(original, MagicMock, "provide it")

        assert exc_info.value is original

    @pytest.mark.asyncio
    async def test_raises_original_error_when_user_cancels(self):
        from mcp.server.elicitation import CancelledElicitation

        original = ValueError("need config")
        session = _make_session(elicitation_supported=True)

        with (
            patch(
                "dbt_mcp.config.elicitation.get_mcp_session",
                return_value=(session, "req-1"),
            ),
            patch(
                "dbt_mcp.config.elicitation.elicit_with_validation",
                new_callable=AsyncMock,
                return_value=CancelledElicitation(),
            ),
        ):
            with pytest.raises(ValueError) as exc_info:
                await elicit_or_raise(original, MagicMock, "provide it")

        assert exc_info.value is original

    @pytest.mark.asyncio
    async def test_returns_validated_data_when_user_accepts(self):
        from pydantic import BaseModel

        from mcp.server.elicitation import AcceptedElicitation

        class TestSchema(BaseModel):
            host: str

        accepted_data = TestSchema(host="cloud.getdbt.com")
        session = _make_session(elicitation_supported=True)

        with (
            patch(
                "dbt_mcp.config.elicitation.get_mcp_session",
                return_value=(session, "req-1"),
            ),
            patch(
                "dbt_mcp.config.elicitation.elicit_with_validation",
                new_callable=AsyncMock,
                return_value=AcceptedElicitation(data=accepted_data),
            ),
        ):
            result = await elicit_or_raise(
                ValueError("missing"), TestSchema, "provide host"
            )

        assert result.host == "cloud.getdbt.com"


# ---------------------------------------------------------------------------
# TestConfigPersistence
# ---------------------------------------------------------------------------


class TestConfigPersistence:
    def test_read_returns_empty_dict_when_no_file(self, tmp_path: Path):
        p = ConfigPersistence(config_path=tmp_path / "nonexistent.yml")
        assert p.read() == {}

    def test_write_creates_file_and_dirs(self, tmp_path: Path):
        nested = tmp_path / "a" / "b" / "config.yml"
        p = ConfigPersistence(config_path=nested)
        p.write("key", "value")

        assert nested.exists()
        data = yaml.safe_load(nested.read_text())
        assert data == {"key": "value"}

    def test_write_preserves_existing_keys(self, tmp_path: Path):
        config_file = tmp_path / "config.yml"
        config_file.write_text(yaml.dump({"existing": "one"}))

        p = ConfigPersistence(config_path=config_file)
        p.write("new_key", "two")

        data = yaml.safe_load(config_file.read_text())
        assert data == {"existing": "one", "new_key": "two"}

    def test_read_value_returns_none_for_missing_key(self, tmp_path: Path):
        config_file = tmp_path / "config.yml"
        config_file.write_text(yaml.dump({"a": 1}))

        p = ConfigPersistence(config_path=config_file)
        assert p.read_value("nonexistent") is None

    def test_read_value_returns_stored_value(self, tmp_path: Path):
        config_file = tmp_path / "config.yml"
        p = ConfigPersistence(config_path=config_file)
        p.write("host", "cloud.getdbt.com")

        assert p.read_value("host") == "cloud.getdbt.com"

    def test_handles_empty_file(self, tmp_path: Path):
        config_file = tmp_path / "config.yml"
        config_file.write_text("")

        p = ConfigPersistence(config_path=config_file)
        assert p.read() == {}

    def test_handles_corrupt_yaml(self, tmp_path: Path):
        config_file = tmp_path / "config.yml"
        config_file.write_text(": : : not valid yaml [[[")

        p = ConfigPersistence(config_path=config_file)
        assert p.read() == {}
