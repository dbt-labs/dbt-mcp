"""Lightweight token/char estimation for MCP tool-call telemetry.

Local dbt-mcp deliberately avoids a tokenizer dependency: it estimates tokens
with a simple ~4-characters-per-token heuristic. The exact character counts are
the durable, tokenizer-independent primitive; the token estimate is directional.

Estimation must never break a tool call, so serialization is defensive and falls
back to a zeroed measurement on any error.
"""

import json
import logging
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

logger = logging.getLogger(__name__)

# Recorded on every event so consumers can treat estimates as approximate and
# detect when the method changes. Mirrors the proto ``token_estimation_method``.
TOKEN_ESTIMATION_METHOD = "char_div_4"
_CHARS_PER_TOKEN = 4
_SKIP_CONTENT_TYPES = frozenset({"image", "audio"})


@dataclass(frozen=True)
class PayloadMeasurement:
    char_count: int
    token_estimate: int


EMPTY_MEASUREMENT = PayloadMeasurement(char_count=0, token_estimate=0)


def _to_text(obj: Any) -> str:
    """Best-effort serialization of a tool payload to the text we measure.

    Handles the shapes MCP tool calls produce without hard-importing their types:
    plain strings, argument mappings, MCP ``TextContent`` blocks (``.text``),
    objects exposing ``.content``, sequences of blocks, and pydantic models.
    """
    if obj is None:
        return ""
    if isinstance(obj, str):
        return obj
    if getattr(obj, "type", None) in _SKIP_CONTENT_TYPES:
        return ""
    if isinstance(obj, Mapping):
        return json.dumps(obj, default=str, sort_keys=True)
    text = getattr(obj, "text", None)
    if isinstance(text, str):
        return text
    content = getattr(obj, "content", None)
    if content is not None:
        return _to_text(content)
    if isinstance(obj, Sequence) and not isinstance(obj, (str, bytes, bytearray)):
        return "".join(_to_text(item) for item in obj)
    for dumper in ("model_dump_json", "json"):
        fn = getattr(obj, dumper, None)
        if callable(fn):
            try:
                return str(fn())
            except Exception:
                pass
    return str(obj)


def measure_payload(obj: Any) -> PayloadMeasurement:
    """Measure a tool payload (arguments dict or tool result) defensively.

    Never raises: returns a zeroed measurement if serialization fails, so
    telemetry cannot break the tool call it describes.
    """
    try:
        text = _to_text(obj)
    except Exception:
        logger.debug("Payload serialization failed — emitting zeroed measurement")
        return EMPTY_MEASUREMENT
    char_count = len(text)
    return PayloadMeasurement(
        char_count=char_count, token_estimate=char_count // _CHARS_PER_TOKEN
    )
