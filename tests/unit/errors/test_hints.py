from dbt_mcp.errors.hints import looks_like_ssl_error, with_multicell_hint


class TestSslErrorDetection:
    """Tests for SSL error detection and message enhancement."""

    def test_looks_like_ssl_error_with_ssl_keyword(self):
        """Errors containing 'ssl' should be detected."""
        error = Exception("SSLError: certificate verify failed")
        assert looks_like_ssl_error(error) is True

    def test_looks_like_ssl_error_with_certificate_keyword(self):
        """Errors containing 'certificate' should be detected."""
        error = Exception("certificate verify failed")
        assert looks_like_ssl_error(error) is True

    def test_looks_like_ssl_error_case_insensitive(self):
        """Detection should be case-insensitive."""
        error = Exception("SSL_CERTIFICATE_ERROR")
        assert looks_like_ssl_error(error) is True

    def test_looks_like_ssl_error_false_for_other_errors(self):
        """Non-SSL errors should not be detected."""
        error = Exception("Connection refused")
        assert looks_like_ssl_error(error) is False

    def test_with_multicell_hint_adds_hint_for_ssl_error(self):
        """SSL errors should get the multi-cell DBT_HOST hint."""
        message = "SSLError: certificate verify failed"
        enhanced = with_multicell_hint(message)
        assert "DBT_HOST" in enhanced
        assert "multi-cell" in enhanced
        assert "SSLError" in enhanced

    def test_with_multicell_hint_no_hint_for_other_errors(self):
        """Non-SSL errors should not get the hint."""
        message = "Connection refused"
        enhanced = with_multicell_hint(message)
        assert "multi-cell" not in enhanced
        assert enhanced == "Connection refused"
