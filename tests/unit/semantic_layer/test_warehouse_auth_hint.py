from unittest.mock import AsyncMock, MagicMock

from dbtsl.error import QueryFailedError

from dbt_mcp.config.config_providers import SemanticLayerConfig
from dbt_mcp.semantic_layer.client import SemanticLayerFetcher
from dbt_mcp.semantic_layer.types import (
    DimensionValuesError,
    GetMetricsCompiledSqlError,
    QueryMetricsError,
)

HINT = "HINT: reconnect at https://example.com/settings/profile/credentials/10"
AUTH_ERROR = QueryFailedError(
    message=" SSO authentication has expired, please re-connect to Snowflake: https://x",
    status=5,
)
OTHER_ERROR = QueryFailedError(message="Metric 'foo' not found", status=3)


def _config(*, hint_provider: MagicMock | None) -> SemanticLayerConfig:
    return SemanticLayerConfig(
        url="https://semantic-layer.example.com/api/graphql",
        host="semantic-layer.example.com",
        prod_environment_id=777,
        token_provider=MagicMock(),
        headers_provider=MagicMock(),
        warehouse_auth_hint_provider=hint_provider,
    )


def _hint_provider() -> MagicMock:
    provider = MagicMock()
    provider.get_hint = AsyncMock(return_value=HINT)
    return provider


def _fetcher(error: Exception) -> SemanticLayerFetcher:
    sl_client = MagicMock()
    session = MagicMock()
    session.__enter__ = MagicMock(return_value=sl_client)
    session.__exit__ = MagicMock(return_value=False)
    sl_client.session.return_value = session
    sl_client.query.side_effect = error
    sl_client.dimension_values.side_effect = error
    sl_client.compile_sql.side_effect = error
    client_provider = MagicMock()
    client_provider.get_client = AsyncMock(return_value=sl_client)
    return SemanticLayerFetcher(client_provider=client_provider)


async def test_query_metrics_appends_hint_on_auth_error():
    provider = _hint_provider()

    result = await _fetcher(AUTH_ERROR).query_metrics(
        config=_config(hint_provider=provider), metrics=["revenue"]
    )

    assert isinstance(result, QueryMetricsError)
    assert "authentication has expired" in result.error
    assert result.error.endswith(f"<hint>{HINT}</hint>")
    provider.get_hint.assert_awaited_once_with(environment_id=777)


async def test_get_dimension_values_appends_hint_on_auth_error():
    result = await _fetcher(AUTH_ERROR).get_dimension_values(
        config=_config(hint_provider=_hint_provider()), dimension="region"
    )

    assert isinstance(result, DimensionValuesError)
    assert result.error.endswith(f"<hint>{HINT}</hint>")


async def test_get_metrics_compiled_sql_appends_hint_on_auth_error():
    result = await _fetcher(AUTH_ERROR).get_metrics_compiled_sql(
        config=_config(hint_provider=_hint_provider()), metrics=["revenue"]
    )

    assert isinstance(result, GetMetricsCompiledSqlError)
    assert result.error.endswith(f"<hint>{HINT}</hint>")


async def test_non_auth_error_is_left_untouched():
    provider = _hint_provider()

    result = await _fetcher(OTHER_ERROR).query_metrics(
        config=_config(hint_provider=provider), metrics=["revenue"]
    )

    assert isinstance(result, QueryMetricsError)
    assert "<hint>" not in result.error
    provider.get_hint.assert_not_called()


async def test_auth_error_without_hint_provider_is_left_untouched():
    result = await _fetcher(AUTH_ERROR).query_metrics(
        config=_config(hint_provider=None), metrics=["revenue"]
    )

    assert isinstance(result, QueryMetricsError)
    assert "authentication has expired" in result.error
    assert "<hint>" not in result.error


async def test_hint_provider_failure_does_not_mask_original_error():
    provider = MagicMock()
    provider.get_hint = AsyncMock(side_effect=RuntimeError("boom"))

    result = await _fetcher(AUTH_ERROR).query_metrics(
        config=_config(hint_provider=provider), metrics=["revenue"]
    )

    assert isinstance(result, QueryMetricsError)
    assert "authentication has expired" in result.error


async def test_platform_login_failure_is_not_given_warehouse_instructions():
    """A failed dbt platform token refresh is not fixed by reconnecting the warehouse."""
    provider = _hint_provider()
    login_error = RuntimeError(
        "OAuth access token is expired and inline refresh failed"
    )
    fetcher = _fetcher(login_error)

    result = await fetcher.get_metrics_compiled_sql(
        config=_config(hint_provider=provider), metrics=["revenue"]
    )

    assert isinstance(result, GetMetricsCompiledSqlError)
    assert "inline refresh failed" in result.error
    assert "<hint>" not in result.error
    provider.get_hint.assert_not_called()
