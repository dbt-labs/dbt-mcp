from typing import Any

from pydantic import BaseModel, Field, model_validator
from dbt_mcp.result_limits import ensure_result_size

from dbt_mcp.errors import InvalidParameterError

LIMIT_FIELD = Field(
    default=50,
    ge=1,
    le=100,
    description="Maximum items in this page (1–100, default 50).",
)
OFFSET_FIELD = Field(
    default=0, ge=0, description="Offset returned by the previous page's next_offset."
)
AFTER_FIELD = Field(
    default=None,
    description="Cursor returned by the previous page's next_cursor; keep filters unchanged.",
)
PAGE_NUM_FIELD = Field(
    default=1,
    ge=1,
    description="One-based page number; keep filters and page size unchanged.",
)


class Pagination(BaseModel):
    has_more: bool
    next_offset: int | None = None
    next_cursor: str | None = None
    next_page: int | None = None
    total_items: int | None = None


class ResultPage[T](BaseModel):
    result: T
    pagination: Pagination

    @model_validator(mode="after")
    def check_result_size(self) -> "ResultPage[T]":
        ensure_result_size(self.result)
        return self


def validate_page_size(limit: int, offset: int = 0) -> None:
    if not isinstance(limit, int) or isinstance(limit, bool) or not 1 <= limit <= 100:
        raise InvalidParameterError("limit must be an integer between 1 and 100.")
    if not isinstance(offset, int) or isinstance(offset, bool) or offset < 0:
        raise InvalidParameterError("offset must be a non-negative integer.")


def offset_pagination(
    response: dict[str, Any], *, count: int, limit: int, offset: int
) -> Pagination:
    if count > limit:
        raise InvalidParameterError(
            "API exceeded the requested page size; narrow the request."
        )
    total = ((response.get("extra") or {}).get("pagination") or {}).get("total_count")
    has_more = offset + count < total if isinstance(total, int) else count == limit
    if has_more and not count:
        raise InvalidParameterError(
            "API pagination did not advance; retry the request."
        )
    return Pagination(
        has_more=has_more,
        next_offset=offset + count if has_more else None,
        total_items=total,
    )


def numbered_pagination(response: dict[str, Any]) -> Pagination:
    page = response["pageNum"]
    has_more = page < response["totalPages"]
    return Pagination(
        has_more=has_more,
        next_page=page + 1 if has_more else None,
        total_items=response["totalItems"],
    )
