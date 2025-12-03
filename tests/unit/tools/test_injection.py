import inspect
from typing import get_type_hints

import pytest

from dbt_mcp.tools.injection import AdaptError, adapt_with_mapper, adapt_with_mappers


class Context:
    """Test context class for carrying data."""

    def __init__(self, user_id: int, name: str = "test"):
        self.user_id = user_id
        self.name = name


class Data:
    """Another test class for different data types."""

    def __init__(self, value: str):
        self.value = value


# Test mappers
def extract_user_id(ctx: Context) -> int:
    return ctx.user_id


def extract_name(ctx: Context) -> str:
    return ctx.name


def extract_value(data: Data) -> str:
    return data.value


async def async_extract_user_id(ctx: Context) -> int:
    return ctx.user_id


# Test target functions
def greet_user_id(user_id: int) -> str:
    return f"Hello, user {user_id}!"


async def async_greet_user_id(user_id: int) -> str:
    return f"Hello, user {user_id}!"


# Core functionality tests
def test_adapt_with_mapper_basic_sync_adaptation():
    """Test basic synchronous function adaptation."""
    adapted = adapt_with_mapper(greet_user_id, extract_user_id)

    # Check that the adapted function works
    ctx = Context(42)
    result = adapted(ctx)
    assert result == "Hello, user 42!"

    # Check signature is correct
    sig = inspect.signature(adapted)
    assert len(sig.parameters) == 1
    param = next(iter(sig.parameters.values()))
    assert param.annotation == Context


def test_adapted_function_preserves_name_and_docstring():
    """Test that the adapted function preserves the name and docstring."""
    adapted = adapt_with_mapper(greet_user_id, extract_user_id)
    assert adapted.__name__ == "greet_user_id"
    assert adapted.__doc__ == greet_user_id.__doc__


def test_adapts_with_no_argument_mapper():
    """Test that an adapter with no argument mapper works."""

    def no_argument_mapper() -> int:
        return 42

    adapted = adapt_with_mapper(greet_user_id, no_argument_mapper)
    assert adapted() == "Hello, user 42!"


def test_adapt_with_mapper_multiple_parameters():
    """Test adaptation with multiple parameters."""

    def complex_func(user_id: int, extra: str, name: str) -> str:
        return f"{name} (ID: {user_id}) - {extra}"

    adapted = adapt_with_mapper(complex_func, extract_user_id)

    ctx = Context(42, "Alice")
    result = adapted(ctx, "test", "Bob")
    assert result == "Bob (ID: 42) - test"


@pytest.mark.asyncio
async def test_adapt_with_mapper_async_function_sync_mapper():
    """Test async function with sync mapper."""
    adapted = adapt_with_mapper(async_greet_user_id, extract_user_id)

    ctx = Context(42)
    result = await adapted(ctx)
    assert result == "Hello, user 42!"


@pytest.mark.asyncio
async def test_adapt_with_mapper_both_async():
    """Test both function and mapper are async."""
    adapted = adapt_with_mapper(async_greet_user_id, async_extract_user_id)

    ctx = Context(42)
    result = await adapted(ctx)
    assert result == "Hello, user 42!"


def test_adapt_with_mapper_no_target_parameters_lenient_skip():
    """Test lenient behavior when no target parameters found."""

    def no_match_func(other: str) -> str:
        return f"Other: {other}"

    adapted = adapt_with_mapper(no_match_func, extract_user_id)

    # Should return original function unchanged
    assert adapted is no_match_func
    assert adapted("test") == "Other: test"


def test_adapt_with_mapper_error_missing_parameter_annotation():
    """Test error when mapper parameter lacks type annotation."""

    def bad_mapper(ctx) -> int:  # No annotation
        return 42

    with pytest.raises(AdaptError, match="mapper must have type-annotated parameters"):
        adapt_with_mapper(greet_user_id, bad_mapper)


def test_adapt_with_mapper_error_missing_return_annotation():
    """Test error when mapper lacks return annotation."""

    def bad_mapper(ctx: Context):  # No return annotation
        return ctx.user_id

    with pytest.raises(AdaptError, match="mapper must have a return type annotation"):
        adapt_with_mapper(greet_user_id, bad_mapper)


# Signature preservation tests
def test_adapt_with_mapper_parameter_defaults_preserved():
    """Test that default parameter values are preserved."""

    def func_with_defaults(user_id: int, suffix: str = "!") -> str:
        return f"User {user_id}{suffix}"

    adapted = adapt_with_mapper(func_with_defaults, extract_user_id)

    sig = inspect.signature(adapted)
    params = list(sig.parameters.values())
    assert len(params) == 2
    assert params[0].annotation == Context  # Replaced parameter
    assert params[1].name == "suffix"
    assert params[1].default == "!"


def test_adapt_with_mapper_parameter_kinds_preserved():
    """Test that parameter kinds (positional, keyword-only, etc.) are preserved."""

    def func_with_kinds(user_id: int, *, keyword_only: str) -> str:
        return f"User {user_id}, {keyword_only}"

    adapted = adapt_with_mapper(func_with_kinds, extract_user_id)

    sig = inspect.signature(adapted)
    params = list(sig.parameters.values())
    assert len(params) == 2
    assert params[0].annotation == Context
    assert params[1].name == "keyword_only"
    assert params[1].kind == inspect.Parameter.KEYWORD_ONLY


# adapt_with_mappers function tests
def test_adapt_with_mappers_multiple_mappers():
    """Test adapt_with_mappers with multiple mappers applied in sequence.

    When chaining mappers that use the same carrier type, both mappers should
    work correctly and the carrier should be passed through properly.
    """

    def use_both(user_id: int, name: str) -> str:
        return f"Hello {name}, your ID is {user_id}"

    adapted = adapt_with_mappers(use_both, [extract_user_id, extract_name])

    ctx = Context(42, "Alice")
    result = adapted(ctx)
    assert result == "Hello Alice, your ID is 42"


def test_adapt_with_mappers_empty_mappers_list():
    """Test adapt_with_mappers with empty mappers list returns original function."""
    adapted = adapt_with_mappers(greet_user_id, [])
    assert adapted is greet_user_id


def test_adapt_with_mappers_different_carrier_types():
    """Test adapt_with_mappers with mappers for different carrier types."""

    def use_both_types(user_id: int, value: str) -> str:
        return f"User {user_id} has value: {value}"

    adapted = adapt_with_mappers(use_both_types, [extract_user_id, extract_value])

    # Need to provide both carrier types
    sig = inspect.signature(adapted)
    params = list(sig.parameters.values())
    assert len(params) == 2

    # Check that both carrier types are in the signature
    param_types = {p.annotation for p in params}
    assert Context in param_types
    assert Data in param_types


# Real-world usage pattern tests
def test_dependency_injection_pattern():
    """Test a realistic dependency injection pattern."""

    class DatabaseContext:
        def __init__(self, connection_string: str):
            self.connection_string = connection_string

    class UserService:
        def __init__(self, db_connection: str):
            self.db_connection = db_connection

    def extract_db_connection(ctx: DatabaseContext) -> str:
        return ctx.connection_string

    def create_user_service(db_connection: str) -> UserService:
        return UserService(db_connection)

    adapted = adapt_with_mapper(create_user_service, extract_db_connection)

    ctx = DatabaseContext("postgresql://localhost:5432/test")
    service = adapted(ctx)
    assert isinstance(service, UserService)
    assert service.db_connection == "postgresql://localhost:5432/test"


# Edge cases and corner cases
def test_function_with_no_parameters():
    """Test adaptation of function with no parameters."""

    def no_params() -> str:
        return "no params"

    # Should return original function since no target to adapt
    adapted = adapt_with_mapper(no_params, extract_user_id)
    assert adapted is no_params
    assert adapted() == "no params"


def test_mapper_parameter_name_used_in_adaptation():
    """Test that the mapper's parameter name is used in the adapted signature."""

    def custom_extract(my_context: Context) -> int:
        return my_context.user_id

    adapted = adapt_with_mapper(greet_user_id, custom_extract)

    sig = inspect.signature(adapted)
    param_name = next(iter(sig.parameters.keys()))
    assert param_name == "my_context"


def test_adapt_with_mapper_provides_type_hints():
    """Test that the adapted function provides type hints."""

    adapted = adapt_with_mapper(greet_user_id, extract_user_id)

    hints = get_type_hints(adapted)

    assert hints["ctx"] is Context
    assert hints["return"] is str


def test_adapt_with_mapper_provides_type_hints_for_async_functions():
    adapted = adapt_with_mapper(async_greet_user_id, extract_user_id)

    hints = get_type_hints(adapted)

    assert hints["ctx"] is Context
    assert hints["return"] is str
