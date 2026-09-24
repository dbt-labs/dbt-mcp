"""Jev (TypeSafe System One) relevance ranking for Semantic Layer catalog items.

Jev returns typed judgments, not text, so it cannot write a query. What it can do
cheaply is score which catalog items are relevant to a natural-language question,
which lets `list_metrics` return a short, fully-described slice of the catalog
instead of every metric name with the descriptions trimmed away.

`Noul` is used rather than `Choice` for two reasons: `Choice` is hard-capped at 255
options (real catalogs exceed that), and it forces exactly one winner, while a
question may legitimately need several metrics.
"""

import asyncio
import logging
import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Protocol

logger = logging.getLogger(__name__)

# Jev pricing: input tokens only, output tokens are free.
_USD_PER_INPUT_TOKEN = 42 / 1_000_000_000

# Separator used to namespace question keys per group in a single system_one call.
# ASCII unit separator: cannot appear in a metric or dimension name.
_GROUP_SEP = "\x1f"


@dataclass(frozen=True)
class JevConfig:
    """Tuning for Jev relevance filtering. Env-driven; not exposed as tool params."""

    api_key: str
    top_k_metrics: int = 5
    top_k_dimensions: int = 12
    relevance_floor: float = 0.15
    # Off ranks metrics only: no get_dimensions fetch, no dimension Jev call, no
    # dimensions section in the response. Exists because dimension ranking has
    # shown mixed benchmark results distinct from metric ranking, so callers
    # need to be able to isolate the two.
    rank_dimensions: bool = True
    timeout: float = 10.0


@dataclass(frozen=True)
class JevCandidate:
    """A catalog item offered to Jev for scoring."""

    name: str
    description: str | None = None
    label: str | None = None

    def as_criterion(self) -> str:
        # Description is the disambiguating signal: catalogs routinely contain
        # near-duplicate names (revenue_churn / revenue_churn_enterprise / ...) that differ
        # only in their description. Fall back to label, then the name itself.
        for text in (self.description, self.label, self.name):
            if text and text.strip():
                return " ".join(text.split())
        return self.name


@dataclass(frozen=True)
class RankedItem:
    name: str
    score: float


class JevUnavailableError(Exception):
    """Jev could not produce a ranking. Callers fall back to unranked behavior."""


class JevRanker(Protocol):
    async def rank_groups(
        self,
        *,
        question: str,
        groups: Mapping[str, Sequence[JevCandidate]],
        kind: str,
        top_k: int,
    ) -> dict[str, list[RankedItem]]: ...


class TypeSafeJevRanker:
    """Ranks catalog items by relevance to a question using TypeSafe System One."""

    def __init__(self, config: JevConfig) -> None:
        self.config = config
        self._client = None
        self._client_lock = asyncio.Lock()

    async def _get_client(self):
        # A fresh client pays ~400-600ms of TLS/connection setup; reused, later
        # calls are ~150-300ms. Created lazily so importing this module never
        # requires the optional dependency.
        if self._client is None:
            async with self._client_lock:
                if self._client is None:
                    try:
                        from typesafe_sdk import TypeSafeClient
                    except ImportError as e:  # pragma: no cover - import guard
                        raise JevUnavailableError(
                            "typesafe-sdk is not installed; "
                            "install dbt-mcp with the 'jev' extra"
                        ) from e
                    self._client = TypeSafeClient(api_key=self.config.api_key)
        return self._client

    async def rank_groups(
        self,
        *,
        question: str,
        groups: Mapping[str, Sequence[JevCandidate]],
        kind: str,
        top_k: int,
    ) -> dict[str, list[RankedItem]]:
        """Score every candidate in every group against `question` in one call.

        Groups are namespaced into a single `system_one` call so that ranking
        dimensions for several metrics costs one round trip rather than one per
        metric. Returns the top `top_k` of each group that clear the relevance
        floor, highest first.
        """
        from typesafe_sdk import Noul

        populated = {g: list(c) for g, c in groups.items() if c}
        if not populated:
            return {g: [] for g in groups}

        questions = {
            f"{group}{_GROUP_SEP}{candidate.name}": Noul(
                instructions=(
                    f"Is the {kind} `{candidate.name}` needed to answer "
                    f"`question`? ({candidate.as_criterion()})"
                )
            )
            for group, candidates in populated.items()
            for candidate in candidates
        }

        client = await self._get_client()
        started = time.monotonic()
        try:
            response = await asyncio.wait_for(
                asyncio.to_thread(
                    client.system_one,
                    state={"question": question},
                    questions=questions,
                ),
                timeout=self.config.timeout,
            )
        except TimeoutError as e:
            # Timing matters most on the failure path: it is the signal that the
            # latency budget, not the API, is what needs adjusting.
            logger.warning(
                "jev.rank timeout kind=%s candidates=%d latency_ms=%.0f",
                kind,
                len(questions),
                (time.monotonic() - started) * 1000,
            )
            raise JevUnavailableError(
                f"Jev ranking timed out after {self.config.timeout}s"
            ) from e
        except Exception as e:
            logger.warning(
                "jev.rank failed kind=%s candidates=%d latency_ms=%.0f error=%s",
                kind,
                len(questions),
                (time.monotonic() - started) * 1000,
                e,
            )
            raise JevUnavailableError(f"Jev ranking failed: {e}") from e
        elapsed = time.monotonic() - started

        ranked: dict[str, list[RankedItem]] = {group: [] for group in groups}
        for key, answer in response.answers.items():
            group, _, name = key.partition(_GROUP_SEP)
            if group in ranked:
                ranked[group].append(RankedItem(name=name, score=answer.noul))

        for group, items in ranked.items():
            items.sort(key=lambda i: -i.score)
            ranked[group] = [
                i for i in items[:top_k] if i.score >= self.config.relevance_floor
            ]

        self._log_usage(response, kind, len(questions), elapsed, ranked)
        return ranked

    def _log_usage(self, response, kind, candidates, elapsed, ranked) -> None:
        """Log Jev's own token spend. This cost is invisible to the calling agent,
        so it is recorded here rather than in the MCP tool-call telemetry."""
        input_tokens = getattr(response.usage, "input_tokens", 0) or 0
        output_tokens = getattr(response.usage, "output_tokens", 0) or 0
        logger.info(
            "jev.rank model=%s kind=%s candidates=%d selected=%d "
            "input_tokens=%d output_tokens=%d usd=%.6f latency_ms=%.0f",
            getattr(response, "model", "unknown"),
            kind,
            candidates,
            sum(len(v) for v in ranked.values()),
            input_tokens,
            output_tokens,
            input_tokens * _USD_PER_INPUT_TOKEN,
            elapsed * 1000,
        )
