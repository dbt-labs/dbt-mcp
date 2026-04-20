"""Tests for the general-purpose MCP elicitation infrastructure."""

import asyncio
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import yaml

from dbt_mcp.errors.common import MissingHostError
from dbt_mcp.config.elicitation import (
    ConfigPersistence,
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
        inner.token_provider = MagicMock()
        inner.authentication_method = MagicMock()
        inner.account_identifier = "test-account"
        inner.get_credentials = AsyncMock(
            return_value=(inner.settings, inner.token_provider)
        )
        return inner

    def _make_elicitation_wrapper(self, inner, tmp_path, *, host="cloud.getdbt.com"):
        """Set up inner to fail once with missing DBT_HOST, then succeed on retry."""
        inner.get_credentials.side_effect = [
            MissingHostError("DBT_HOST is a required environment variable"),
            (inner.settings, inner.token_provider),
        ]
        persistence = ConfigPersistence(config_path=tmp_path / "cfg.yml")
        wrapper = ElicitingCredentialsProvider(inner, persistence)
        accepted = DbtHostSchema(dbt_host=host)
        return wrapper, persistence, accepted

    @pytest.mark.asyncio
    async def test_delegates_to_inner_when_credentials_available(self, tmp_path: Path):
        inner = self._make_inner()
        persistence = ConfigPersistence(config_path=tmp_path / "cfg.yml")
        wrapper = ElicitingCredentialsProvider(inner, persistence)

        result = await wrapper.get_credentials()

        assert result == (inner.settings, inner.token_provider)
        inner.get_credentials.assert_called_once()

    @pytest.mark.asyncio
    async def test_elicits_dbt_host_on_missing_host_error(self, tmp_path: Path):
        inner = self._make_inner()
        wrapper, _, accepted = self._make_elicitation_wrapper(inner, tmp_path)

        with patch(
            "dbt_mcp.config.elicitation.elicit_or_raise",
            new_callable=AsyncMock,
            return_value=accepted,
        ):
            result = await wrapper.get_credentials()

        assert result == (inner.settings, inner.token_provider)
        assert inner.settings.dbt_host == "cloud.getdbt.com"

    @pytest.mark.asyncio
    async def test_reraises_non_host_errors(self, tmp_path: Path):
        original = ValueError("something else entirely")
        inner = self._make_inner()
        inner.get_credentials = AsyncMock(side_effect=original)
        persistence = ConfigPersistence(config_path=tmp_path / "cfg.yml")
        wrapper = ElicitingCredentialsProvider(inner, persistence)

        with pytest.raises(ValueError) as exc_info:
            await wrapper.get_credentials()

        assert exc_info.value is original

    @pytest.mark.asyncio
    async def test_persists_elicited_host(self, tmp_path: Path):
        inner = self._make_inner()
        wrapper, persistence, accepted = self._make_elicitation_wrapper(
            inner, tmp_path, host="emea.dbt.com"
        )

        with patch(
            "dbt_mcp.config.elicitation.elicit_or_raise",
            new_callable=AsyncMock,
            return_value=accepted,
        ):
            await wrapper.get_credentials()

        assert persistence.read_value("dbt_host") == "emea.dbt.com"

    @pytest.mark.asyncio
    async def test_concurrent_calls_elicit_only_once(self, tmp_path: Path):
        inner = self._make_inner()
        # First call raises, all subsequent succeed
        inner.get_credentials.side_effect = [
            MissingHostError("DBT_HOST is a required environment variable"),
            (inner.settings, inner.token_provider),
            (inner.settings, inner.token_provider),
        ]
        persistence = ConfigPersistence(config_path=tmp_path / "cfg.yml")
        wrapper = ElicitingCredentialsProvider(inner, persistence)
        accepted = DbtHostSchema(dbt_host="cloud.getdbt.com")

        # Gate forces first call to yield at elicitation, giving second call
        # a chance to contend on the lock — proving real serialization.
        gate = asyncio.Event()

        real_elicit_call_count = 0

        async def slow_elicit(*args: Any, **kwargs: Any) -> DbtHostSchema:
            nonlocal real_elicit_call_count
            real_elicit_call_count += 1
            gate.set()  # signal that elicitation is in progress
            await asyncio.sleep(0)  # yield to event loop so second task runs
            return accepted

        with patch(
            "dbt_mcp.config.elicitation.elicit_or_raise",
            side_effect=slow_elicit,
        ):
            # Start both tasks; second will block on the lock while first elicits
            task1 = asyncio.create_task(wrapper.get_credentials())
            await asyncio.sleep(0)  # let task1 start and hit the lock
            task2 = asyncio.create_task(wrapper.get_credentials())
            results = await asyncio.gather(task1, task2)

        # Lock serializes: first call elicits, second sees the already-set host
        assert real_elicit_call_count == 1
        assert all(r == (inner.settings, inner.token_provider) for r in results)

    @pytest.mark.asyncio
    async def test_does_not_persist_when_retry_fails(self, tmp_path: Path):
        inner = self._make_inner()
        inner.get_credentials.side_effect = [
            MissingHostError("DBT_HOST is a required environment variable"),
            RuntimeError("OAuth failed for invalid host"),
        ]
        persistence = ConfigPersistence(config_path=tmp_path / "cfg.yml")
        wrapper = ElicitingCredentialsProvider(inner, persistence)
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

        assert persistence.read_value("dbt_host") is None

    def test_transparent_property_delegation(self, tmp_path: Path):
        inner = self._make_inner()
        persistence = ConfigPersistence(config_path=tmp_path / "cfg.yml")
        wrapper = ElicitingCredentialsProvider(inner, persistence)

        assert wrapper.settings is inner.settings
        assert wrapper.token_provider is inner.token_provider
        assert wrapper.authentication_method is inner.authentication_method
        assert wrapper.account_identifier == "test-account"
