"""Tests for the general-purpose MCP elicitation infrastructure."""

import asyncio
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dbt_mcp.errors.common import MissingHostError
from dbt_mcp.config.elicitation import (
    DbtHostSchema,
    ElicitingCredentialsProvider,
    _host_schema_with_hint,
    elicit_or_raise,
    get_elicitation_session,
    validate_host_reachable,
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

        with patch("dbt_mcp.config.elicitation.request_ctx") as mock_ctx_var:
            mock_ctx_var.get.return_value = resolved
            assert get_elicitation_session() is None

    def test_returns_session_when_elicitation_supported(self):
        session = _make_session(elicitation_supported=True)
        ctx = _make_request_context(session, request_id="req-42")

        with patch("dbt_mcp.config.elicitation.request_ctx") as mock_ctx_var:
            mock_ctx_var.get.return_value = ctx
            result = get_elicitation_session()

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

        with patch(
            "dbt_mcp.config.elicitation.get_elicitation_session", return_value=None
        ):
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
                "dbt_mcp.config.elicitation.get_elicitation_session",
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
                "dbt_mcp.config.elicitation.get_elicitation_session",
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
        "raw_input,expected",
        [
            pytest.param(
                "https://cloud.getdbt.com", "cloud.getdbt.com", id="https_prefix"
            ),
            pytest.param(
                "http://cloud.getdbt.com", "cloud.getdbt.com", id="http_prefix"
            ),
            pytest.param("cloud.getdbt.com/", "cloud.getdbt.com", id="trailing_slash"),
            pytest.param(
                "https://ab123.us1.dbt.com/", "ab123.us1.dbt.com", id="full_url"
            ),
            pytest.param(
                "  https://cloud.getdbt.com/  ",
                "cloud.getdbt.com",
                id="whitespace_and_url",
            ),
        ],
    )
    def test_normalizes_host_input(self, raw_input, expected):
        schema = DbtHostSchema(dbt_host=raw_input)
        assert schema.dbt_host == expected

    @pytest.mark.parametrize(
        "invalid_input",
        [
            pytest.param("", id="empty_string"),
            pytest.param("   ", id="whitespace_only"),
            pytest.param("https://", id="scheme_only"),
        ],
    )
    def test_rejects_invalid_host(self, invalid_input):
        with pytest.raises(ValueError):
            DbtHostSchema(dbt_host=invalid_input)


# ---------------------------------------------------------------------------
# TestHostSchemaWithHint
# ---------------------------------------------------------------------------


class TestHostSchemaWithHint:
    def test_returns_base_schema_when_no_hint(self):
        assert _host_schema_with_hint(None) is DbtHostSchema
        assert _host_schema_with_hint("") is DbtHostSchema

    def test_hint_appears_in_field_title(self):
        hint = "Could not reach 'bad.example.com'. Please double-check the hostname."
        schema_cls = _host_schema_with_hint(hint)
        json_schema = schema_cls.model_json_schema()
        assert json_schema["properties"]["dbt_host"]["title"] == hint

    def test_inherits_normalization_validator(self):
        schema_cls = _host_schema_with_hint("some error hint")
        instance = schema_cls(dbt_host="https://cloud.getdbt.com/")
        assert instance.dbt_host == "cloud.getdbt.com"

    def test_inherits_min_length_validation(self):
        schema_cls = _host_schema_with_hint("some error hint")
        with pytest.raises(ValueError):
            schema_cls(dbt_host="")


# ---------------------------------------------------------------------------
# TestElicitingCredentialsProvider
# ---------------------------------------------------------------------------


class TestElicitingCredentialsProvider:
    def _make_inner(self):
        """Build a mock CredentialsProvider that succeeds by default."""
        inner = MagicMock()
        inner.settings = MagicMock()
        inner.settings.dbt_host = None
        inner.settings.dbt_profiles_dir = None
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

        with (
            patch(
                "dbt_mcp.config.elicitation.elicit_or_raise",
                new_callable=AsyncMock,
                return_value=accepted,
            ),
            patch(
                "dbt_mcp.config.elicitation.validate_host_reachable",
                new_callable=AsyncMock,
                return_value=True,
            ),
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
        call_count = 0

        async def order_independent_credentials():
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise MissingHostError("DBT_HOST is a required environment variable")
            return (inner.settings, inner.token_provider)

        inner.get_credentials = AsyncMock(side_effect=order_independent_credentials)
        wrapper = ElicitingCredentialsProvider(inner)
        accepted = DbtHostSchema(dbt_host="cloud.getdbt.com")

        real_elicit_call_count = 0

        async def slow_elicit(*args: Any, **kwargs: Any) -> DbtHostSchema:
            nonlocal real_elicit_call_count
            real_elicit_call_count += 1
            await asyncio.sleep(0)
            return accepted

        with (
            patch(
                "dbt_mcp.config.elicitation.elicit_or_raise",
                side_effect=slow_elicit,
            ),
            patch(
                "dbt_mcp.config.elicitation.validate_host_reachable",
                new_callable=AsyncMock,
                return_value=True,
            ),
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
            patch(
                "dbt_mcp.config.elicitation.validate_host_reachable",
                new_callable=AsyncMock,
                return_value=True,
            ),
            patch(
                "dbt_mcp.config.elicitation._clear_persisted_host",
            ) as mock_clear,
            pytest.raises(RuntimeError, match="OAuth failed"),
        ):
            await wrapper.get_credentials()

        assert inner.settings.dbt_host is None
        mock_clear.assert_called_once()

    @pytest.mark.asyncio
    async def test_timeout_resets_host_and_clears_persisted(self):
        """Timeout resets in-memory host, clears mcp.yml, and re-elicits until retries exhausted."""
        inner = self._make_inner()
        inner.get_credentials = AsyncMock(
            side_effect=MissingHostError("DBT_HOST is a required environment variable"),
        )
        wrapper = ElicitingCredentialsProvider(inner)
        accepted = DbtHostSchema(dbt_host="bad-host.example.com")

        with (
            patch(
                "dbt_mcp.config.elicitation.elicit_or_raise",
                new_callable=AsyncMock,
                return_value=accepted,
            ),
            patch(
                "dbt_mcp.config.elicitation.validate_host_reachable",
                new_callable=AsyncMock,
                return_value=True,
            ),
            patch(
                "dbt_mcp.config.elicitation.asyncio.wait_for",
                side_effect=TimeoutError(),
            ),
            patch(
                "dbt_mcp.config.elicitation._clear_persisted_host",
            ) as mock_clear,
            pytest.raises(MissingHostError, match="Authentication timed out"),
        ):
            await wrapper.get_credentials()

        assert inner.settings.dbt_host is None
        assert mock_clear.call_count == 3  # once per retry

    @pytest.mark.asyncio
    async def test_unreachable_host_re_elicits_then_succeeds(self):
        """When host is unreachable, re-prompt with hinted schema; succeed on second try."""
        inner = self._make_inner()
        inner.get_credentials.side_effect = [
            MissingHostError("DBT_HOST is a required environment variable"),
            (inner.settings, inner.token_provider),
        ]
        wrapper = ElicitingCredentialsProvider(inner)
        bad_host = DbtHostSchema(dbt_host="bad-host.example.com")
        good_host = DbtHostSchema(dbt_host="cloud.getdbt.com")

        schemas_received: list[type] = []

        async def elicit_side_effect(
            error: Exception, schema: type, message: str
        ) -> DbtHostSchema:
            schemas_received.append(schema)
            return bad_host if len(schemas_received) == 1 else good_host

        validate_calls = 0

        async def validate_side_effect(host: str, **kwargs: Any) -> bool:
            nonlocal validate_calls
            validate_calls += 1
            return host != "bad-host.example.com"

        with (
            patch(
                "dbt_mcp.config.elicitation.elicit_or_raise",
                side_effect=elicit_side_effect,
            ),
            patch(
                "dbt_mcp.config.elicitation.validate_host_reachable",
                side_effect=validate_side_effect,
            ),
        ):
            result = await wrapper.get_credentials()

        assert result == (inner.settings, inner.token_provider)
        assert len(schemas_received) == 2
        assert validate_calls == 2
        assert inner.settings.dbt_host == "cloud.getdbt.com"

        # First call uses default schema, second uses hinted schema
        assert schemas_received[0] is DbtHostSchema
        assert schemas_received[1] is not DbtHostSchema
        hinted_title = schemas_received[1].model_json_schema()["properties"][
            "dbt_host"
        ]["title"]
        assert "bad-host.example.com" in hinted_title

    @pytest.mark.asyncio
    async def test_unreachable_host_exhausts_retries(self):
        """After MAX_ELICITATION_RETRIES unreachable hosts, raise MissingHostError."""
        inner = self._make_inner()
        inner.get_credentials = AsyncMock(
            side_effect=MissingHostError("DBT_HOST is a required environment variable"),
        )
        wrapper = ElicitingCredentialsProvider(inner)
        accepted = DbtHostSchema(dbt_host="unreachable.example.com")

        with (
            patch(
                "dbt_mcp.config.elicitation.elicit_or_raise",
                new_callable=AsyncMock,
                return_value=accepted,
            ),
            patch(
                "dbt_mcp.config.elicitation.validate_host_reachable",
                new_callable=AsyncMock,
                return_value=False,
            ) as mock_validate,
            pytest.raises(MissingHostError, match="Could not reach"),
        ):
            await wrapper.get_credentials()

        assert mock_validate.call_count == 3
        # Host should never have been set since validation always failed
        assert inner.settings.dbt_host is None

    def test_transparent_property_delegation(self):
        inner = self._make_inner()
        wrapper = ElicitingCredentialsProvider(inner)

        assert wrapper.settings is inner.settings
        assert wrapper.token_provider is inner.token_provider
        assert wrapper.authentication_method is inner.authentication_method
        assert wrapper.account_identifier == "test-account"


# ---------------------------------------------------------------------------
# TestValidateHostReachable
# ---------------------------------------------------------------------------


class TestValidateHostReachable:
    @pytest.mark.asyncio
    async def test_reachable_host_returns_true(self):
        mock_writer = MagicMock()
        mock_writer.close = MagicMock()
        mock_writer.wait_closed = AsyncMock()

        with patch(
            "dbt_mcp.config.elicitation.asyncio.open_connection",
            new_callable=AsyncMock,
            return_value=(MagicMock(), mock_writer),
        ):
            assert await validate_host_reachable("cloud.getdbt.com") is True

        mock_writer.close.assert_called_once()
        mock_writer.wait_closed.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_unreachable_host_returns_false(self):
        with patch(
            "dbt_mcp.config.elicitation.asyncio.open_connection",
            new_callable=AsyncMock,
            side_effect=OSError("Connection refused"),
        ):
            assert await validate_host_reachable("nonexistent.invalid") is False

    @pytest.mark.asyncio
    async def test_timeout_returns_false(self):
        with patch(
            "dbt_mcp.config.elicitation.asyncio.open_connection",
            new_callable=AsyncMock,
            side_effect=TimeoutError(),
        ):
            assert await validate_host_reachable("slow.example.com") is False


# ---------------------------------------------------------------------------
# TestClearPersistedHost
# ---------------------------------------------------------------------------


class TestClearPersistedHost:
    def test_clears_host_from_existing_context(self, tmp_path):
        from dbt_mcp.config.elicitation import _clear_persisted_host
        from dbt_mcp.oauth.context_manager import DbtPlatformContextManager
        from dbt_mcp.oauth.dbt_platform import DbtPlatformContext

        mcp_yml = tmp_path / "mcp.yml"
        ctx_manager = DbtPlatformContextManager(mcp_yml)
        ctx_manager.write_context_to_file(
            DbtPlatformContext(dbt_host="stale-host.example.com", account_id=123)
        )

        _clear_persisted_host(str(tmp_path))

        updated = ctx_manager.read_context()
        assert updated is not None
        assert updated.dbt_host is None
        assert updated.account_id == 123  # other fields preserved

    def test_noop_when_no_mcp_yml(self, tmp_path):
        from dbt_mcp.config.elicitation import _clear_persisted_host

        _clear_persisted_host(str(tmp_path))  # should not raise
