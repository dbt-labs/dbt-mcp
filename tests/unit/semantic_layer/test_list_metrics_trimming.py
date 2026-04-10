from dbt_mcp.config.settings import DbtMcpSettings


def test_sl_metrics_max_response_chars_default():
    settings = DbtMcpSettings(DBT_HOST=None, _env_file=None)
    assert settings.sl_metrics_max_response_chars == 16000


def test_sl_metrics_max_response_chars_from_env(monkeypatch):
    monkeypatch.setenv("DBT_MCP_SL_MAX_RESPONSE_CHARS", "8000")
    settings = DbtMcpSettings(DBT_HOST=None, _env_file=None)
    assert settings.sl_metrics_max_response_chars == 8000


def test_sl_metrics_max_response_chars_zero_allowed(monkeypatch):
    monkeypatch.setenv("DBT_MCP_SL_MAX_RESPONSE_CHARS", "0")
    settings = DbtMcpSettings(DBT_HOST=None, _env_file=None)
    assert settings.sl_metrics_max_response_chars == 0
