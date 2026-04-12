"""Tests for platform credential error detection in Semantic Layer tools.

Verifies that when query_metrics or get_metrics_compiled_sql fail due to
expired dbt Cloud platform credentials, the error message clearly indicates
this is a platform-side issue and directs users to the dbt Cloud UI.

Related: https://github.com/dbt-labs/dbt-mcp/issues/670
"""

import pytest

from dbt_mcp.semantic_layer.client import (
    SemanticLayerFetcher,
    _PLATFORM_CREDENTIAL_HINT,
    _is_platform_credential_error,
)


@pytest.fixture
def fetcher():
    from unittest.mock import AsyncMock

    return SemanticLayerFetcher(client_provider=AsyncMock())


class TestPlatformCredentialErrorDetection:
    """Tests for _is_platform_credential_error."""

    def test_detects_sso_authentication_expired(self) -> None:
        assert _is_platform_credential_error(
            "SSO authentication has expired, please re-connect to Snowflake"
        )

    def test_detects_reconnect_to_snowflake(self) -> None:
        assert _is_platform_credential_error(
            "Please re-connect to Snowflake: https://docs.getdbt.com/faqs"
        )

    def test_detects_refresh_snowflake_oauth_url(self) -> None:
        assert _is_platform_credential_error(
            "https://docs.getdbt.com/faqs/Troubleshooting/refresh-snowflake-oauth-credentials"
        )

    def test_detects_authentication_token_expired(self) -> None:
        assert _is_platform_credential_error(
            "Authentication token has expired for the data warehouse"
        )

    def test_detects_oauth_token_expired(self) -> None:
        assert _is_platform_credential_error(
            "OAuth access token has expired for this connection"
        )

    def test_case_insensitive(self) -> None:
        assert _is_platform_credential_error(
            "sso AUTHENTICATION Has Expired"
        )

    def test_does_not_match_generic_query_error(self) -> None:
        assert not _is_platform_credential_error(
            "column 'revenue' not found in table"
        )

    def test_does_not_match_syntax_error(self) -> None:
        assert not _is_platform_credential_error(
            "SQL compilation error: syntax error at position 42"
        )

    def test_does_not_match_timeout_error(self) -> None:
        assert not _is_platform_credential_error(
            "Query timed out after 60 seconds"
        )

    def test_does_not_match_empty_string(self) -> None:
        assert not _is_platform_credential_error("")


class TestFormatSemanticLayerErrorWithCredentialHint:
    """Tests that _format_semantic_layer_error appends the credential hint."""

    def test_sso_expired_includes_platform_hint(self, fetcher) -> None:
        error = Exception(
            "SSO authentication has expired, please re-connect to Snowflake: "
            "https://docs.getdbt.com/faqs/Troubleshooting/refresh-snowflake-oauth-credentials"
        )
        result = fetcher._format_semantic_layer_error(error)
        assert "dbt Cloud" in result
        assert "Profile → Credentials" in result
        assert "not a local authentication problem" in result

    def test_sso_expired_preserves_original_message(self, fetcher) -> None:
        error = Exception(
            "SSO authentication has expired, please re-connect to Snowflake"
        )
        result = fetcher._format_semantic_layer_error(error)
        assert "SSO authentication has expired" in result

    def test_generic_error_does_not_include_platform_hint(self, fetcher) -> None:
        error = Exception("column 'revenue' not found")
        result = fetcher._format_semantic_layer_error(error)
        assert _PLATFORM_CREDENTIAL_HINT not in result
        assert "dbt Cloud UI" not in result

    def test_query_failed_error_with_credential_message_includes_hint(
        self, fetcher
    ) -> None:
        """QueryFailedError wrapping a credential expiry should also get the hint."""
        error = Exception(
            'QueryFailedError(["SSO authentication has expired, '
            "please re-connect to Snowflake\"])"
        )
        result = fetcher._format_semantic_layer_error(error)
        assert "dbt Cloud" in result
        assert "Profile → Credentials" in result

    def test_format_query_failed_error_with_credential_message(self, fetcher) -> None:
        """_format_query_failed_error should also include the hint for credential errors."""
        from dbtsl.error import QueryFailedError

        error = QueryFailedError(
            "SSO authentication has expired, please re-connect to Snowflake"
        )
        result = fetcher._format_query_failed_error(error)
        assert "dbt Cloud" in result.error
        assert "Profile → Credentials" in result.error
