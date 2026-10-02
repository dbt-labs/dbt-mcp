import json
from dataclasses import asdict, is_dataclass
from typing import Any

from pydantic import BaseModel

from dbt_mcp.errors import InvalidParameterError

RESULT_BYTES_LIMIT = 500 * 1024


def _json_default(value: Any) -> Any:
    if isinstance(value, BaseModel):
        return value.model_dump(mode="json")
    if is_dataclass(value) and not isinstance(value, type):
        return asdict(value)
    raise TypeError(f"Unsupported JSON value: {type(value).__name__}")


def ensure_result_size(value: Any) -> None:
    size = 0
    for chunk in json.JSONEncoder(
        default=_json_default, separators=(",", ":")
    ).iterencode(value):
        size += len(chunk.encode("utf-8"))
        if size > RESULT_BYTES_LIMIT:
            raise InvalidParameterError(
                "Result size limit exceeded; reduce the page size, narrow the filters, or select a smaller resource."
            )
