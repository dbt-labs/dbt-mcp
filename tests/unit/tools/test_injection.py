import inspect

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


def greet_user_with_name(user_id: int, name: str) -> str:
    return f"Hello, {name} (ID: {user_id})!"


async def async_greet_user_id(user_id: int) -> str:
    return f"Hello, user {user_id}!"


def function_with_existing_carrier(ctx: Context, user_id: int) -> str:
    return f"Context name: {ctx.name}, extracted ID: {user_id}"


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
async def test_adapt_with_mapper_sync_function_async_mapper():
    """Test sync function with async mapper."""
    adapted = adapt_with_mapper(greet_user_id, async_extract_user_id)

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


def test_adapt_with_mapper_existing_carrier_parameter():
    """Test when function already has carrier type parameter.

    When the target function already has a parameter with the carrier type,
    the adapter should reuse that parameter and remove the target parameter.
    """
    adapted = adapt_with_mapper(function_with_existing_carrier, extract_user_id)

    # Should remove the user_id parameter and keep the ctx parameter
    sig = inspect.signature(adapted)
    assert len(sig.parameters) == 1
    param = next(iter(sig.parameters.values()))
    assert param.annotation == Context
    assert param.name == "ctx"

    # Should work correctly - both ctx and mapped user_id are passed
    ctx = Context(42, "Alice")
    result = adapted(ctx)
    assert result == "Context name: Alice, extracted ID: 42"


def test_adapt_with_mapper_no_target_parameters_lenient_skip():
    """Test lenient behavior when no target parameters found."""

    def no_match_func(other: str) -> str:
        return f"Other: {other}"

    adapted = adapt_with_mapper(no_match_func, extract_user_id)

    # Should return original function unchanged
    assert adapted is no_match_func
    assert adapted("test") == "Other: test"


def test_adapt_with_mapper_ambiguous_parameters_error():
    """Test error when multiple parameters match target type."""

    def ambiguous_func(user_id1: int, user_id2: int) -> str:
        return f"IDs: {user_id1}, {user_id2}"

    with pytest.raises(AdaptError, match="Ambiguous.*user_id1, user_id2.*int"):
        adapt_with_mapper(ambiguous_func, extract_user_id)


# Test removed - was testing on_ambiguous="first" behavior which no longer exists


# Error condition tests
def test_adapt_with_mapper_error_wrong_parameter_count():
    """Test error when mapper has wrong number of parameters."""

    def bad_mapper(ctx: Context, extra: str) -> int:
        return ctx.user_id

    with pytest.raises(AdaptError, match="mapper must take exactly one parameter"):
        adapt_with_mapper(greet_user_id, bad_mapper)


def test_adapt_with_mapper_error_missing_parameter_annotation():
    """Test error when mapper parameter lacks type annotation."""

    def bad_mapper(ctx) -> int:  # No annotation
        return 42

    with pytest.raises(AdaptError, match="mapper's parameter must be type-annotated"):
        adapt_with_mapper(greet_user_id, bad_mapper)


def test_adapt_with_mapper_error_missing_return_annotation():
    """Test error when mapper lacks return annotation."""

    def bad_mapper(ctx: Context):  # No return annotation
        return ctx.user_id

    with pytest.raises(AdaptError, match="mapper must have a return annotation"):
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


def test_adapt_with_mappers_ambiguous_parameters_error():
    """Test that adapt_with_mappers raises error on ambiguous parameters."""

    def ambiguous_func(user_id1: int, user_id2: int) -> str:
        return f"IDs: {user_id1}, {user_id2}"

    # Should raise error due to ambiguous parameters
    with pytest.raises(AdaptError, match="Ambiguous.*user_id1, user_id2.*int"):
        adapt_with_mappers(ambiguous_func, [extract_user_id])


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


def test_function_with_varargs():
    """Test function with *args and **kwargs.

    The adapter should properly handle functions with varargs and varkwargs,
    preserving the positional and keyword argument structure.
    """

    def func_with_varargs(user_id: int, *args, **kwargs) -> str:
        return f"User {user_id}, args: {args}, kwargs: {kwargs}"

    adapted = adapt_with_mapper(func_with_varargs, extract_user_id)

    ctx = Context(42)
    result = adapted(ctx, "extra", "args", key="value")
    # Should correctly preserve varargs and varkwargs structure
    assert result == "User 42, args: ('extra', 'args'), kwargs: {'key': 'value'}"


def test_mapper_parameter_name_used_in_adaptation():
    """Test that the mapper's parameter name is used in the adapted signature."""

    def custom_extract(my_context: Context) -> int:
        return my_context.user_id

    adapted = adapt_with_mapper(greet_user_id, custom_extract)

    sig = inspect.signature(adapted)
    param_name = next(iter(sig.parameters.keys()))
    assert param_name == "my_context"
