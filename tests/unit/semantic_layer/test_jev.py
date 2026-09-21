"""Tests for the Jev ranker itself: scoring, thresholds, cost logging, enablement."""

import logging
import time

import pytest

from dbt_mcp.semantic_layer.jev import (
    JevCandidate,
    JevConfig,
    JevUnavailableError,
)


def test_candidate_prefers_description_then_label_then_name():
    assert JevCandidate("n", "desc", "label").as_criterion() == "desc"
    assert JevCandidate("n", "  ", "label").as_criterion() == "label"
    assert JevCandidate("n", None, None).as_criterion() == "n"
    assert JevCandidate("n", "multi\n  line").as_criterion() == "multi line"


class _FakeAnswer:
    def __init__(self, noul: float):
        self.noul = noul


class _FakeUsage:
    input_tokens = 21392
    output_tokens = 40


class _FakeResponse:
    model = "jev-1.13.0"
    usage = _FakeUsage()

    def __init__(self, answers):
        self.answers = answers


class _FakeClient:
    def __init__(self, answers=None, error: Exception | None = None, delay=0.0):
        self.answers = answers or {}
        self.error = error
        self.delay = delay

    def system_one(self, *, state, questions):
        if self.delay:
            time.sleep(self.delay)
        if self.error:
            raise self.error
        return _FakeResponse(
            {k: _FakeAnswer(self.answers.get(k, 0.0)) for k in questions}
        )


def _ranker_with(client, **overrides):
    from dbt_mcp.semantic_layer.jev import TypeSafeJevRanker

    ranker = TypeSafeJevRanker(JevConfig(api_key="k", **overrides))
    ranker._client = client
    return ranker


@pytest.mark.asyncio
async def test_ranker_logs_tokens_cost_and_latency(caplog):
    client = _FakeClient(answers={"metrics\x1frevenue_churn": 0.9})
    ranker = _ranker_with(client)
    with caplog.at_level(logging.INFO, logger="dbt_mcp.semantic_layer.jev"):
        await ranker.rank_groups(
            question="churn?",
            groups={
                "metrics": [JevCandidate("revenue_churn", "Revenue lost to churn.")]
            },
            kind="metric",
            top_k=5,
        )
    record = caplog.text
    assert "input_tokens=21392" in record
    assert "output_tokens=40" in record
    # 21392 tokens * $42/1e9
    assert "usd=0.000898" in record
    assert "latency_ms=" in record
    assert "model=jev-1.13.0" in record


@pytest.mark.asyncio
async def test_ranker_logs_latency_on_timeout(caplog):
    ranker = _ranker_with(_FakeClient(delay=0.2), timeout=0.01)
    with caplog.at_level(logging.WARNING, logger="dbt_mcp.semantic_layer.jev"):
        with pytest.raises(JevUnavailableError, match="timed out"):
            await ranker.rank_groups(
                question="q",
                groups={"metrics": [JevCandidate("m")]},
                kind="metric",
                top_k=5,
            )
    assert "jev.rank timeout" in caplog.text
    assert "latency_ms=" in caplog.text


@pytest.mark.asyncio
async def test_ranker_logs_latency_on_error(caplog):
    ranker = _ranker_with(_FakeClient(error=RuntimeError("boom")))
    with caplog.at_level(logging.WARNING, logger="dbt_mcp.semantic_layer.jev"):
        with pytest.raises(JevUnavailableError):
            await ranker.rank_groups(
                question="q",
                groups={"metrics": [JevCandidate("m")]},
                kind="metric",
                top_k=5,
            )
    assert "jev.rank failed" in caplog.text
    assert "latency_ms=" in caplog.text


@pytest.mark.asyncio
async def test_ranker_applies_floor_and_top_k_per_group():
    client = _FakeClient(
        answers={
            "a\x1fone": 0.9,
            "a\x1ftwo": 0.5,
            "a\x1fthree": 0.05,
            "b\x1ffour": 0.8,
        }
    )
    ranker = _ranker_with(client, relevance_floor=0.15)
    result = await ranker.rank_groups(
        question="q",
        groups={
            "a": [JevCandidate("one"), JevCandidate("two"), JevCandidate("three")],
            "b": [JevCandidate("four")],
        },
        kind="dimension",
        top_k=2,
    )
    assert [i.name for i in result["a"]] == ["one", "two"], "floor drops 'three'"
    assert [i.name for i in result["b"]] == ["four"]


@pytest.mark.asyncio
async def test_ranker_returns_empty_groups_without_calling_out():
    ranker = _ranker_with(_FakeClient(error=RuntimeError("should not be called")))
    assert await ranker.rank_groups(
        question="q", groups={"a": []}, kind="metric", top_k=5
    ) == {"a": []}


def _settings(**env):
    """Build settings by alias — pydantic-settings fields are alias-populated."""
    from dbt_mcp.config.settings import DbtMcpSettings

    return DbtMcpSettings(**env)  # type: ignore[call-arg]


def test_jev_requires_both_flag_and_key():
    from dbt_mcp.config.config import _build_jev_config

    assert (
        _build_jev_config(_settings(DBT_MCP_ENABLE_JEV=True, TYPESAFE_API_KEY="k"))
        is not None
    )
    assert (
        _build_jev_config(_settings(DBT_MCP_ENABLE_JEV=False, TYPESAFE_API_KEY="k"))
        is None
    ), "holding a TypeSafe key for unrelated reasons must not enable this"
    assert (
        _build_jev_config(_settings(DBT_MCP_ENABLE_JEV=True, TYPESAFE_API_KEY=None))
        is None
    )


def test_jev_flag_without_key_warns(caplog):
    from dbt_mcp.config.config import _build_jev_config

    with caplog.at_level(logging.WARNING, logger="dbt_mcp.config.config"):
        _build_jev_config(_settings(DBT_MCP_ENABLE_JEV=True, TYPESAFE_API_KEY=None))
    assert "TYPESAFE_API_KEY is missing" in caplog.text


def test_jev_config_carries_tunables_from_settings():
    from dbt_mcp.config.config import _build_jev_config

    jev = _build_jev_config(
        _settings(
            DBT_MCP_ENABLE_JEV=True,
            TYPESAFE_API_KEY="k",
            DBT_MCP_JEV_TOP_K_METRICS=3,
            DBT_MCP_JEV_RELEVANCE_FLOOR=0.4,
            DBT_MCP_JEV_DIMENSION_METRICS=1,
        )
    )
    assert jev is not None
    assert (jev.top_k_metrics, jev.relevance_floor, jev.dimension_metrics) == (
        3,
        0.4,
        1,
    )
