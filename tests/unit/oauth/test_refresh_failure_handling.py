"""Synthetic tests for terminal-stop and bounded transient retry of OAuth refresh.

No network, tokens or caches are touched: the token endpoint is replaced by a
fake that returns scripted outcomes.
"""

import asyncio
import threading
import time
from unittest.mock import MagicMock, patch

import pytest
from authlib.integrations.base_client.errors import OAuthError
from authlib.oauth2.rfc6749.errors import InvalidGrantError, InvalidScopeError

from dbt_mcp.oauth.refresh import is_terminal_refresh_error
from dbt_mcp.oauth.refresh_strategy import DefaultRefreshStrategy, MockRefreshStrategy
from dbt_mcp.oauth.token import AccessTokenResponse
from dbt_mcp.oauth.token_provider import OAuthTokenProvider

REFRESH_PATH = "dbt_mcp.oauth.token_provider.refresh_oauth_token"


def _response(
    *, expires_at: int, access_token: str = "tok", refresh_token: str = "rt0"
) -> AccessTokenResponse:
    return AccessTokenResponse(
        access_token=access_token,
        refresh_token=refresh_token,
        expires_in=3600,
        scope="user_access offline_access",
        token_type="Bearer",
        expires_at=expires_at,
    )


def _context(response: AccessTokenResponse) -> MagicMock:
    ctx = MagicMock()
    ctx.decoded_access_token.access_token_response = response
    return ctx


def _provider(
    *, expires_at: int, strategy: MockRefreshStrategy | None = None, **kwargs: int
) -> OAuthTokenProvider:
    return OAuthTokenProvider(
        access_token_response=_response(expires_at=expires_at),
        dbt_platform_url="https://cloud.getdbt.com",
        context_manager=MagicMock(),
        refresh_strategy=strategy or MockRefreshStrategy(wait_seconds=0),
        **({"min_refresh_interval_seconds": 0} | kwargs),
    )


class TestClassification:
    @pytest.mark.parametrize(
        "code", ["invalid_grant", "invalid_client", "invalid_scope"]
    )
    def test_definitive_oauth_errors_are_terminal(self, code: str):
        assert is_terminal_refresh_error(OAuthError(error=code))

    def test_oauth_error_class_is_terminal(self):
        assert is_terminal_refresh_error(InvalidGrantError())
        assert is_terminal_refresh_error(InvalidScopeError())

    @pytest.mark.parametrize(
        "code",
        ["server_error", "temporarily_unavailable", "rate_limited", "forbidden", ""],
    )
    def test_other_oauth_codes_stay_retryable(self, code: str):
        assert not is_terminal_refresh_error(OAuthError(error=code))

    @pytest.mark.parametrize("code", [{"a": 1}, ["invalid_grant"], None, 5])
    def test_non_string_code_does_not_crash(self, code: object):
        error = OAuthError(error="x")
        error.error = code  # type: ignore[assignment]
        assert not is_terminal_refresh_error(error)

    @pytest.mark.parametrize(
        "error",
        [
            ConnectionError("down"),
            TimeoutError(),
            OSError("tls"),
            ValueError("bad json"),
        ],
    )
    def test_non_oauth_failures_are_transient(self, error: Exception):
        assert not is_terminal_refresh_error(error)


class TestBackgroundWorker:
    @pytest.mark.asyncio
    async def test_stops_after_invalid_grant_with_one_request(self):
        provider = _provider(expires_at=int(time.time()) + 3600)
        with patch(REFRESH_PATH, side_effect=OAuthError(error="invalid_grant")) as m:
            await asyncio.wait_for(provider._background_refresh_worker(), timeout=2)
        assert m.call_count == 1
        assert provider.refresh_strategy.error_wait_calls == 0  # type: ignore[attr-defined]

    @pytest.mark.asyncio
    async def test_transient_failures_are_bounded(self):
        strategy = MockRefreshStrategy(wait_seconds=0)
        provider = _provider(
            expires_at=int(time.time()) + 3600,
            strategy=strategy,
            max_consecutive_refresh_failures=4,
        )
        with patch(REFRESH_PATH, side_effect=ConnectionError("down")) as m:
            await asyncio.wait_for(provider._background_refresh_worker(), timeout=2)
        assert m.call_count == 4
        assert strategy.error_wait_attempts == [1, 2, 3]

    @pytest.mark.asyncio
    async def test_success_resets_failure_count(self):
        strategy = MockRefreshStrategy(wait_seconds=0)
        provider = _provider(
            expires_at=int(time.time()) + 3600,
            strategy=strategy,
            max_consecutive_refresh_failures=3,
        )
        good = _context(
            _response(expires_at=int(time.time()) + 3600, refresh_token="rt1")
        )
        outcomes = [
            ConnectionError("x"),
            ConnectionError("x"),
            good,
            ConnectionError("x"),
            ConnectionError("x"),
            OAuthError(error="invalid_grant"),
        ]
        with patch(REFRESH_PATH, side_effect=outcomes) as m:
            await asyncio.wait_for(provider._background_refresh_worker(), timeout=2)
        assert m.call_count == 6

    @pytest.mark.asyncio
    async def test_short_lived_tokens_cannot_cause_request_loop(self):
        strategy = MockRefreshStrategy(wait_seconds=0)
        provider = _provider(
            expires_at=int(time.time()) + 3600,
            strategy=strategy,
            max_consecutive_refresh_failures=3,
        )
        # Server keeps issuing tokens that are already inside the refresh buffer.
        short = _context(_response(expires_at=int(time.time()) + 1))
        with patch(REFRESH_PATH, return_value=short) as m:
            await asyncio.wait_for(provider._background_refresh_worker(), timeout=2)
        assert m.call_count == 3

    @pytest.mark.asyncio
    async def test_cancellation_still_propagates(self):
        provider = _provider(
            expires_at=int(time.time()) + 3600,
            strategy=MockRefreshStrategy(wait_seconds=10),
        )
        task = asyncio.create_task(provider._background_refresh_worker())
        await asyncio.sleep(0)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task


class TestInlineRefresh:
    def test_terminal_failure_short_circuits_later_calls(self):
        provider = _provider(expires_at=int(time.time()) - 100)
        with patch(REFRESH_PATH, side_effect=OAuthError(error="invalid_grant")) as m:
            for _ in range(5):
                with pytest.raises(ValueError, match="re-authenticate"):
                    provider.get_token()
        assert m.call_count == 1

    def test_transient_failure_does_not_latch(self):
        provider = _provider(expires_at=int(time.time()) - 100)
        fresh = _context(
            _response(expires_at=int(time.time()) + 3600, access_token="new")
        )
        with patch(REFRESH_PATH, side_effect=[ConnectionError("x"), fresh]) as m:
            with pytest.raises(ValueError):
                provider.get_token()
            assert provider.get_token() == "new"
        assert m.call_count == 2

    def test_terminal_failure_stops_background_worker_request(self):
        provider = _provider(expires_at=int(time.time()) - 100)
        with patch(REFRESH_PATH, side_effect=OAuthError(error="invalid_grant")):
            with pytest.raises(ValueError):
                provider.get_token()

        async def run() -> None:
            with patch(REFRESH_PATH) as m:
                await asyncio.wait_for(provider._background_refresh_worker(), timeout=2)
                m.assert_not_called()

        asyncio.run(run())

    def test_concurrent_callers_spend_refresh_token_once(self):
        provider = _provider(expires_at=int(time.time()) - 100)
        fresh = _context(
            _response(
                expires_at=int(time.time()) + 3600,
                access_token="new",
                refresh_token="rt1",
            )
        )
        calls = 0

        def slow_refresh(**_: str) -> MagicMock:
            nonlocal calls
            calls += 1
            time.sleep(0.2)
            return fresh

        results: list[str] = []
        with patch(REFRESH_PATH, side_effect=slow_refresh):
            threads = [
                threading.Thread(target=lambda: results.append(provider.get_token()))
                for _ in range(4)
            ]
            for t in threads:
                t.start()
            for t in threads:
                t.join()
        assert calls == 1
        assert results == ["new"] * 4

    @pytest.mark.asyncio
    async def test_rotation_race_is_not_mistaken_for_terminal(self):
        """Worker holding a stale refresh token after an inline refresh skips quietly."""
        provider = _provider(expires_at=int(time.time()) + 3600)
        provider._refresh_token_value = "rt1"
        with patch(REFRESH_PATH) as m:
            provider._refresh_token(stale_refresh_token="rt0")
            m.assert_not_called()
        assert provider._terminal_error is None


class TestBackoff:
    @pytest.mark.asyncio
    async def test_delay_doubles_and_is_capped(self):
        strategy = DefaultRefreshStrategy(error_retry_delay=5, max_error_retry_delay=30)
        sleeps: list[float] = []

        async def fake_sleep(d: float) -> None:
            sleeps.append(d)

        with patch("dbt_mcp.oauth.refresh_strategy.asyncio.sleep", fake_sleep):
            for attempt in (1, 2, 3, 4, 100_000):
                await strategy.wait_after_error(attempt)
        assert sleeps == [5, 10, 20, 30, 30]

    @pytest.mark.asyncio
    @pytest.mark.parametrize("attempt", [0, -3])
    async def test_nonpositive_attempt_uses_base_delay(self, attempt: int):
        strategy = DefaultRefreshStrategy(error_retry_delay=5)
        sleeps: list[float] = []

        async def fake_sleep(d: float) -> None:
            sleeps.append(d)

        with patch("dbt_mcp.oauth.refresh_strategy.asyncio.sleep", fake_sleep):
            await strategy.wait_after_error(attempt)
        assert sleeps == [5]


class TestNewFindings:
    @pytest.mark.asyncio
    @pytest.mark.parametrize("lifetime", [31, 60, 120, 299])
    async def test_tokens_inside_strategy_buffer_do_not_loop(self, lifetime: int):
        strategy = MockRefreshStrategy(wait_seconds=0)
        provider = _provider(
            expires_at=int(time.time()) + 3600,
            strategy=strategy,
            max_consecutive_refresh_failures=3,
        )
        short = _context(_response(expires_at=int(time.time()) + lifetime))
        with patch(REFRESH_PATH, return_value=short) as m:
            await asyncio.wait_for(provider._background_refresh_worker(), timeout=2)
        assert m.call_count == 3

    def test_failure_after_rotation_keeps_new_refresh_token_and_does_not_latch(self):
        provider = _provider(expires_at=int(time.time()) - 100)
        seen: list[str] = []

        def refresh(*, refresh_token: str, on_token_response, **_: str) -> MagicMock:
            seen.append(refresh_token)
            if len(seen) == 1:
                on_token_response({"refresh_token": "rt1"})
                raise ConnectionError("key fetch failed after rotation")
            return _context(
                _response(expires_at=int(time.time()) + 3600, refresh_token="rt2")
            )

        with patch(REFRESH_PATH, side_effect=refresh):
            with pytest.raises(ValueError):
                provider.get_token()
            provider.get_token()
        assert seen == ["rt0", "rt1"]
        assert provider._terminal_error is None

    def test_inline_rechecks_expiry_inside_lock(self):
        provider = _provider(expires_at=int(time.time()) + 3600)
        with patch(REFRESH_PATH) as m:
            provider._refresh_token(only_if=lambda: False)
            m.assert_not_called()


class TestRound2:
    @pytest.mark.asyncio
    @pytest.mark.parametrize("lifetime", [301, 302, 305, 359])
    async def test_lifetimes_just_above_buffer_do_not_loop(self, lifetime: int):
        strategy = MockRefreshStrategy(wait_seconds=0)
        provider = _provider(
            expires_at=int(time.time()) + 3600,
            strategy=strategy,
            max_consecutive_refresh_failures=3,
        )
        short = _context(_response(expires_at=int(time.time()) + lifetime))
        with patch(REFRESH_PATH, return_value=short) as m:
            await asyncio.wait_for(provider._background_refresh_worker(), timeout=2)
        assert m.call_count == 3

    def test_persistence_failure_does_not_fail_refresh(self):
        provider = _provider(expires_at=int(time.time()) - 100)
        provider.context_manager.update_context.side_effect = OSError("disk")  # type: ignore[attr-defined]
        fresh = _context(
            _response(expires_at=int(time.time()) + 3600, access_token="new")
        )
        with patch(REFRESH_PATH, return_value=fresh):
            assert provider.get_token() == "new"

    def test_rejection_adopts_token_persisted_by_other_instance(self):
        provider = _provider(expires_at=int(time.time()) - 100)
        other = MagicMock()
        other.decoded_access_token.access_token_response.refresh_token = "rt-other"
        provider.context_manager.read_context.return_value = other  # type: ignore[attr-defined]
        fresh = _context(
            _response(expires_at=int(time.time()) + 3600, access_token="new")
        )
        seen: list[str] = []

        def refresh(*, refresh_token: str, **_: object) -> MagicMock:
            seen.append(refresh_token)
            if refresh_token == "rt0":
                raise OAuthError(error="invalid_grant")
            return fresh

        with patch(REFRESH_PATH, side_effect=refresh):
            with pytest.raises(ValueError):
                provider.get_token()
            assert provider.get_token() == "new"
        assert seen == ["rt0", "rt-other"]
        assert provider._terminal_error is None

    def test_same_persisted_token_still_latches(self):
        provider = _provider(expires_at=int(time.time()) - 100)
        same = MagicMock()
        same.decoded_access_token.access_token_response.refresh_token = "rt0"
        provider.context_manager.read_context.return_value = same  # type: ignore[attr-defined]
        with patch(REFRESH_PATH, side_effect=OAuthError(error="invalid_grant")):
            with pytest.raises(ValueError):
                provider.get_token()
        assert provider._terminal_error is not None

    @pytest.mark.asyncio
    async def test_validation_error_text_not_logged(
        self, caplog: pytest.LogCaptureFixture
    ):
        from pydantic import BaseModel

        class M(BaseModel):
            refresh_token: int

        try:
            M(refresh_token="SECRETVALUE")  # type: ignore[arg-type]
        except Exception as e:
            err = e
        provider = _provider(
            expires_at=int(time.time()) + 3600,
            max_consecutive_refresh_failures=1,
        )
        with patch(REFRESH_PATH, side_effect=err):
            await asyncio.wait_for(provider._background_refresh_worker(), timeout=2)
        assert "SECRETVALUE" not in caplog.text


class TestMinimumInterval:
    @pytest.mark.asyncio
    async def test_worker_enforces_interval_for_any_strategy_and_yields(self):
        class NeverWaits:
            async def wait_until_refresh_needed(self, expires_at: int) -> None:
                return None

            async def wait_after_error(self, attempt: int = 1) -> None:
                return None

        sleeps: list[float] = []
        real_sleep = asyncio.sleep

        async def fake_sleep(d: float) -> None:
            sleeps.append(d)
            await real_sleep(0)

        provider = OAuthTokenProvider(
            access_token_response=_response(expires_at=int(time.time()) + 400),
            dbt_platform_url="https://cloud.getdbt.com",
            context_manager=MagicMock(),
            refresh_strategy=NeverWaits(),
            max_consecutive_refresh_failures=3,
            min_refresh_interval_seconds=60,
        )
        ok = _context(_response(expires_at=int(time.time()) + 100))
        with (
            patch(REFRESH_PATH, return_value=ok) as m,
            patch("dbt_mcp.oauth.token_provider.asyncio.sleep", fake_sleep),
        ):
            await asyncio.wait_for(provider._background_refresh_worker(), timeout=2)
        assert m.call_count == 3
        assert all(d > 50 for d in sleeps[1:])
