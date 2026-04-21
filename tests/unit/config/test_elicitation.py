"""Tests for the general-purpose MCP elicitation infrastructure."""

import asyncio
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dbt_mcp.errors.common import MissingHostError
from dbt_mcp.config.elicitation import (
    DbtHostSchema,
    ElicitingCredentialsProvider,
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
    @pytest.mark.parametrize(
        "ctx_value",
        [
            pytest.param(None, id="no_request_context"),
            pytest.param(
                lambda: _make_request_context(
                    _make_session(elicitation_supported=False)
                ),
                id="no_elicitation_capability",
            ),
            pytest.param(
                lambda: _make_request_context(
                    _make_session(elicitation_supported=False, client_params_none=True)
                ),
                id="client_params_none",
            ),
        ],
    )
    def test_returns_none_when_elicitation_unavailable(self, ctx_value):
        resolved = ctx_value() if callable(ctx_value) else ctx_value

        with patch("mcp.server.lowlevel.server.request_ctx") as mock_ctx_var:
            mock_ctx_var.get.return_value = resolved
            assert get_mcp_session() is None

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
    @pytest.mark.parametrize(
        "elicitation_response",
        [
            pytest.param("DeclinedElicitation", id="declined"),
            pytest.param("CancelledElicitation", id="cancelled"),
        ],
    )
    async def test_raises_original_error_when_user_rejects(self, elicitation_response):
        import mcp.server.elicitation as elicitation_mod

        response_cls = getattr(elicitation_mod, elicitation_response)
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
                return_value=response_cls(),
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
# TestDbtHostSchema
# ---------------------------------------------------------------------------


class TestDbtHostSchema:
    def test_valid_host(self):
        schema = DbtHostSchema(dbt_host="cloud.getdbt.com")
        assert schema.dbt_host == "cloud.getdbt.com"

    def test_strips_whitespace(self):
        schema = DbtHostSchema(dbt_host="  cloud.getdbt.com  ")
        assert schema.dbt_host == "cloud.getdbt.com"

    @pytest.mark.parametrize(
        "invalid_input",
        [
            pytest.param("", id="empty_string"),
            pytest.param("   ", id="whitespace_only"),
        ],
    )
    def test_rejects_invalid_host(self, invalid_input):
        with pytest.raises(ValueError):
            DbtHostSchema(dbt_host=invalid_input)


# ---------------------------------------------------------------------------
# TestElicitingCredentialsProvider
# ---------------------------------------------------------------------------


class TestElicitingCredentialsProvider:
    def _make_inner(self):
        """Build a mock CredentialsProvider that succeeds by default."""
        inner = MagicMock()
        inner.settings = MagicMock()
        inner.settings.dbt_host = None
        inner.token_provider = MagicMock()
        inner.authentication_method = MagicMock()
        inner.account_identifier = "test-account"
        inner.get_credentials = AsyncMock(
            return_value=(inner.settings, inner.token_provider)
        )
        return inner

    def _make_elicitation_wrapper(self, inner, *, host="cloud.getdbt.com"):
        """Set up inner to fail once with missing DBT_HOST, then succeed on retry."""
        inner.get_credentials.side_effect = [
            MissingHostError("DBT_HOST is a required environment variable"),
            (inner.settings, inner.token_provider),
        ]
        wrapper = ElicitingCredentialsProvider(inner)
        accepted = DbtHostSchema(dbt_host=host)
        return wrapper, accepted

    @pytest.mark.asyncio
    async def test_delegates_to_inner_when_credentials_available(self):
        inner = self._make_inner()
        wrapper = ElicitingCredentialsProvider(inner)

        result = await wrapper.get_credentials()

        assert result == (inner.settings, inner.token_provider)
        inner.get_credentials.assert_called_once()

    @pytest.mark.asyncio
    async def test_elicits_dbt_host_on_missing_host_error(self):
        inner = self._make_inner()
        wrapper, accepted = self._make_elicitation_wrapper(inner)

        with patch(
            "dbt_mcp.config.elicitation.elicit_or_raise",
            new_callable=AsyncMock,
            return_value=accepted,
        ):
            result = await wrapper.get_credentials()

        assert result == (inner.settings, inner.token_provider)
        assert inner.settings.dbt_host == "cloud.getdbt.com"

    @pytest.mark.asyncio
    async def test_reraises_non_host_errors(self):
        original = ValueError("something else entirely")
        inner = self._make_inner()
        inner.get_credentials = AsyncMock(side_effect=original)
        wrapper = ElicitingCredentialsProvider(inner)

        with pytest.raises(ValueError) as exc_info:
            await wrapper.get_credentials()

        assert exc_info.value is original

    @pytest.mark.asyncio
    async def test_concurrent_calls_elicit_only_once(self):
        inner = self._make_inner()
        inner.get_credentials.side_effect = [
            MissingHostError("DBT_HOST is a required environment variable"),
            (inner.settings, inner.token_provider),
            (inner.settings, inner.token_provider),
        ]
        wrapper = ElicitingCredentialsProvider(inner)
        accepted = DbtHostSchema(dbt_host="cloud.getdbt.com")

        real_elicit_call_count = 0

        async def slow_elicit(*args: Any, **kwargs: Any) -> DbtHostSchema:
            nonlocal real_elicit_call_count
            real_elicit_call_count += 1
            await asyncio.sleep(0)
            return accepted

        with patch(
            "dbt_mcp.config.elicitation.elicit_or_raise",
            side_effect=slow_elicit,
        ):
            task1 = asyncio.create_task(wrapper.get_credentials())
            await asyncio.sleep(0)
            task2 = asyncio.create_task(wrapper.get_credentials())
            results = await asyncio.gather(task1, task2)

        assert real_elicit_call_count == 1
        assert all(r == (inner.settings, inner.token_provider) for r in results)

    @pytest.mark.asyncio
    async def test_resets_host_when_retry_fails(self):
        inner = self._make_inner()
        inner.get_credentials.side_effect = [
            MissingHostError("DBT_HOST is a required environment variable"),
            RuntimeError("OAuth failed for invalid host"),
        ]
        wrapper = ElicitingCredentialsProvider(inner)
        accepted = DbtHostSchema(dbt_host="bad-host.example.com")

        with (
            patch(
                "dbt_mcp.config.elicitation.elicit_or_raise",
                new_callable=AsyncMock,
                return_value=accepted,
            ),
            pytest.raises(RuntimeError, match="OAuth failed"),
        ):
            await wrapper.get_credentials()

        assert inner.settings.dbt_host is None

    def test_transparent_property_delegation(self):
        inner = self._make_inner()
        wrapper = ElicitingCredentialsProvider(inner)

        assert wrapper.settings is inner.settings
        assert wrapper.token_provider is inner.token_provider
        assert wrapper.authentication_method is inner.authentication_method
        assert wrapper.account_identifier == "test-account"
