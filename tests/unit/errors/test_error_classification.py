"""Tests for typed union exhaustiveness of ToolCallError classification."""

from typing import get_args

from dbt_mcp.errors import (
    ClientToolCallError,
    ServerToolCallError,
    ToolCallError,
)
from dbt_mcp.errors.base import ToolCallError as BaseToolCallError


def _get_all_subclasses(cls: type) -> set[type]:
    """Recursively collect all subclasses of a class."""
    result = set()
    for subclass in cls.__subclasses__():
        result.add(subclass)
        result.update(_get_all_subclasses(subclass))
    return result


class TestUnionExhaustiveness:
    """Ensure every ToolCallError subclass is classified in exactly one union."""

    def test_all_subclasses_are_classified(self) -> None:
        client_types = set(get_args(ClientToolCallError))
        server_types = set(get_args(ServerToolCallError))
        all_classified = client_types | server_types

        all_subclasses = _get_all_subclasses(BaseToolCallError)

        unclassified = set()
        for subclass in all_subclasses:
            # A subclass is classified if it is in a union OR if any of its
            # parent classes (other than ToolCallError) is in a union.
            classified = False
            for cls in subclass.__mro__:
                if cls in all_classified:
                    classified = True
                    break
            if not classified:
                unclassified.add(subclass)

        assert unclassified == set(), (
            f"Unclassified ToolCallError subclasses: {unclassified}. "
            "Add them to ClientToolCallError or ServerToolCallError in errors/__init__.py."
        )

    def test_no_overlap_between_client_and_server(self) -> None:
        client_types = set(get_args(ClientToolCallError))
        server_types = set(get_args(ServerToolCallError))
        overlap = client_types & server_types
        assert overlap == set(), (
            f"Types appear in both ClientToolCallError and ServerToolCallError: {overlap}"
        )


class TestIsInstanceWorks:
    """Verify isinstance checks work with the typed unions at runtime."""

    def test_isinstance_client_error(self) -> None:
        for error_type in get_args(ClientToolCallError):
            instance = error_type("test")
            assert isinstance(instance, ClientToolCallError)

    def test_isinstance_server_error(self) -> None:
        for error_type in get_args(ServerToolCallError):
            instance = error_type("test")
            assert isinstance(instance, ServerToolCallError)
