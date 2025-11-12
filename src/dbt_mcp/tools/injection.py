import inspect
from collections.abc import Callable, Iterable
from typing import Any, TypeVar

from makefun import wraps

R = TypeVar("R")


class AdaptError(TypeError): ...


def adapt_with_mapper[R](
    func: Callable[..., R],
    mapper: Callable[..., Any],
) -> Callable[..., R]:
    """
    Transform a function to accept a different input type by using a mapper function.

    Instead of calling `greet(user_id)`, you can call `greet(context)` where the
    `user_id` gets automatically extracted from the `context`.

    Example:
        ```python
        # Original function expects just a user ID
        def greet(user_id: int) -> str:
            return f"Hello, user {user_id}!"

        # You have a context object that contains the user ID
        class Context:
            def __init__(self, user_id: int):
                self.user_id = user_id

        # Create a mapper that extracts the user ID from context
        def extract_user_id(ctx: Context) -> int:
            return ctx.user_id

        # Adapt the function to accept Context instead of int
        adapted_greet = adapt_with_mapper(greet, extract_user_id)

        # Now you can call it with a Context object!
        result = adapted_greet(Context(42))  # returns: "Hello, user 42!"
        ```

    How it works:
        - Finds the parameter in `func` that matches the return type of `mapper`
        - Replaces that parameter with the input type that `mapper` expects
        - When called, automatically runs `mapper` to convert the input

    Args:
        func: The function you want to adapt
        mapper: A function that converts from the new input type to the expected type
                (must have type annotations: mapper(new_input: NewType) -> ExpectedType)

    Returns:
        A new function that accepts the mapper's input type instead of the original type

    Notes:
        - Works with both sync and async functions (automatically detects and handles)
        - If the original function already accepts the mapper's input type, it reuses that parameter
        - Raises an error if there are multiple parameters with the same type (ambiguous)
        - Skips adaptation if no matching parameter types are found

    Raises:
        AdaptError: If mapper signature is invalid or multiple parameters match the same type
    """

    func_sig = inspect.signature(func)
    mapper_sig = inspect.signature(mapper)
    is_func_async = inspect.iscoroutinefunction(func)
    is_mapper_async = inspect.iscoroutinefunction(mapper)

    # Extract and validate types from mapper signature
    mapper_param_name, carrier_type, target_type = _validate_and_extract_types(
        mapper_sig
    )

    # Find target parameters in the function signature
    params = list(func_sig.parameters.values())
    target_result = _find_target_parameters(params, target_type)

    if target_result is None:
        return func  # lenient skip - no target parameters found

    target_idx, target_param = target_result

    # Check for existing carrier parameters
    carrier_indices = _find_carrier_parameters(params, carrier_type)

    # Build the new signature for the adapted function (using closure)
    def build_adapted_signature() -> tuple[inspect.Signature, str]:
        """Build new signature, closing over outer variables."""
        new_params = params.copy()

        if carrier_indices:
            # Reuse existing carrier, delete target
            del new_params[target_idx]
            carrier_name = params[carrier_indices[0]].name
        else:
            # Introduce new carrier in place of target
            carrier_name = mapper_param_name
            replacement = inspect.Parameter(
                name=carrier_name,
                kind=target_param.kind,
                default=target_param.default,
                annotation=carrier_type,
            )
            new_params[target_idx] = replacement

        new_sig = func_sig.replace(parameters=new_params)
        return new_sig, carrier_name

    # Create wrapper function (using closure)
    def create_wrapper(
        new_sig: inspect.Signature, carrier_name: str
    ) -> Callable[..., Any]:
        """Create wrapper function, closing over outer variables."""
        is_async = is_func_async or is_mapper_async

        # Pre-compute parameter info for better runtime performance
        new_sig_params = new_sig.parameters  # Cache parameter dict
        func_sig_items = list(func_sig.parameters.items())  # Pre-convert to list
        target_param_name = target_param.name
        has_carrier_indices = bool(carrier_indices)

        # Pre-compute parameter kinds for faster access
        VAR_POSITIONAL = inspect.Parameter.VAR_POSITIONAL
        VAR_KEYWORD = inspect.Parameter.VAR_KEYWORD

        # Check if function has varargs for optimization path selection
        has_varargs = any(
            p.kind in (VAR_POSITIONAL, VAR_KEYWORD)
            for p in func_sig.parameters.values()
        )

        def reconstruct_call_arguments(
            adapted_bound_args: inspect.BoundArguments,
            mapped_value: Any,
            carrier_obj: Any,
        ) -> tuple[list[Any], dict[str, Any]]:
            """Optimized argument reconstruction with pre-computed values."""
            # Initialize with optimal sizing when possible
            varargs = []
            varkwargs = None  # Lazy initialization
            remaining_kwargs = {}

            # Fast path for functions without varargs (most common case)
            if not has_varargs:
                remaining_kwargs = dict(adapted_bound_args.arguments)
            else:
                # Process arguments with minimal lookups for varargs case
                arguments = adapted_bound_args.arguments

                for name, value in arguments.items():
                    param_kind = new_sig_params[name].kind
                    if param_kind == VAR_POSITIONAL:
                        varargs.extend(value)
                    elif param_kind == VAR_KEYWORD:
                        if varkwargs is None:
                            varkwargs = {}
                        varkwargs.update(value)
                    else:
                        remaining_kwargs[name] = value

            # Build final kwargs efficiently - modify in place when possible
            remaining_kwargs[target_param_name] = mapped_value
            if has_carrier_indices:
                remaining_kwargs[carrier_name] = carrier_obj
            if varkwargs:
                remaining_kwargs.update(varkwargs)

            # Early return for simple case (no positional reconstruction needed)
            if not func_sig_items:
                return [], remaining_kwargs

            # Optimized positional args building
            positional_args = []

            for param_name, param in func_sig_items:
                param_kind = param.kind
                if param_kind == VAR_POSITIONAL:
                    positional_args.extend(varargs)
                elif param_kind != VAR_KEYWORD and param_name in remaining_kwargs:
                    positional_args.append(remaining_kwargs.pop(param_name))

            return positional_args, remaining_kwargs

        # Pre-compute async flags for runtime optimization
        mapper_is_async = is_mapper_async
        func_is_async = is_func_async

        if is_async:

            @wraps(func, new_sig=new_sig)
            async def async_wrapper(*args: Any, **kwargs: Any) -> Any:
                adapted_bound_args = new_sig.bind_partial(*args, **kwargs)
                adapted_bound_args.apply_defaults()

                if carrier_name not in adapted_bound_args.arguments:
                    raise TypeError(f"Missing required argument: {carrier_name!r}")
                carrier_obj = adapted_bound_args.arguments.pop(carrier_name)

                # Optimized async handling - avoid _maybe_await call overhead
                if mapper_is_async:
                    mapped_value = await mapper(carrier_obj)
                else:
                    mapped_value = mapper(carrier_obj)

                positional_args, keyword_args = reconstruct_call_arguments(
                    adapted_bound_args, mapped_value, carrier_obj
                )

                # Optimized async handling for function result
                result = func(*positional_args, **keyword_args)
                if func_is_async:
                    return await result  # type: ignore[misc]  # func_is_async guarantees this is awaitable
                else:
                    return result

            return async_wrapper
        else:

            @wraps(func, new_sig=new_sig)
            def sync_wrapper(*args: Any, **kwargs: Any) -> Any:
                adapted_bound_args = new_sig.bind_partial(*args, **kwargs)
                adapted_bound_args.apply_defaults()

                if carrier_name not in adapted_bound_args.arguments:
                    raise TypeError(f"Missing required argument: {carrier_name!r}")
                carrier_obj = adapted_bound_args.arguments.pop(carrier_name)

                mapped_value = mapper(carrier_obj)
                # Fast check for async result in sync context
                if mapper_is_async or inspect.isawaitable(mapped_value):
                    raise AdaptError(
                        "Async mapper used with sync function; "
                        "adapt_with_mapper made a sync wrapper.\n"
                        "Either make the original function async or use an async mapper "
                        "only when func is async."
                    )

                positional_args, keyword_args = reconstruct_call_arguments(
                    adapted_bound_args, mapped_value, carrier_obj
                )
                return func(*positional_args, **keyword_args)

            return sync_wrapper

    # Execute the closure-based functions
    new_sig, carrier_name = build_adapted_signature()
    wrapper = create_wrapper(new_sig, carrier_name)
    return wrapper


def adapt_with_mappers[R](
    func: Callable[..., R],
    mappers: Iterable[Callable[..., Any]],
) -> Callable[..., R]:
    """Apply multiple mappers to a function in sequence.

    This is like calling adapt_with_mapper multiple times in a row.
    Each mapper transforms the function to accept a different input type.

    Example:
        ```python
        # Function that needs both user_id and name
        def greet_user(user_id: int, name: str) -> str:
            return f"Hello {name} (ID: {user_id})!"

        # Apply multiple mappers to accept Context for both parameters
        adapted = adapt_with_mappers(greet_user, [extract_user_id, extract_name])

        # Now accepts Context instead of separate int and str
        result = adapted(Context(42, "Alice"))  # "Hello Alice (ID: 42)!"
        ```

    Args:
        func: The function you want to adapt
        mappers: List of mapper functions to apply one after another

    Returns:
        A new function with all the mappers applied in sequence
    """
    adapted_function = func
    for mapper in mappers:
        adapted_function = adapt_with_mapper(adapted_function, mapper)
    return adapted_function


def _validate_and_extract_types(mapper_sig: inspect.Signature) -> tuple[str, Any, Any]:
    """Validate mapper signature and extract type information.

    Args:
        mapper_sig: The mapper function's signature

    Returns:
        Tuple of (mapper_param_name, carrier_type, target_type)

    Raises:
        AdaptError: If mapper signature is invalid
    """
    if len(mapper_sig.parameters) != 1:
        raise AdaptError("mapper must take exactly one parameter (the carrier object)")

    (mapper_param_name, mapper_param) = next(iter(mapper_sig.parameters.items()))
    carrier_type = mapper_param.annotation
    if carrier_type is inspect._empty:
        raise AdaptError("mapper's parameter must be type-annotated")

    target_type = mapper_sig.return_annotation
    if target_type is inspect._empty:
        raise AdaptError("mapper must have a return annotation")

    return mapper_param_name, carrier_type, target_type


def _find_target_parameters(
    params: list[inspect.Parameter], target_type: Any
) -> tuple[int, inspect.Parameter] | None:
    """Find parameters matching the target type.

    Args:
        params: List of function parameters
        target_type: The type to match

    Returns:
        Tuple of (index, parameter) if exactly one match found, None if no matches.

    Raises:
        AdaptError: If multiple parameters match the target type.
    """
    matches = [
        (i, param)
        for i, param in enumerate(params)
        if param.annotation is not inspect._empty and param.annotation == target_type
    ]

    if not matches:
        return None

    if len(matches) > 1:
        param_names = [param.name for _, param in matches]
        names = ", ".join(param_names)
        raise AdaptError(f"Ambiguous: [{names}] are annotated as {target_type!r}")

    return matches[0]


def _find_carrier_parameters(
    params: list[inspect.Parameter], carrier_type: Any
) -> list[int]:
    """Find indices of parameters matching the carrier type.

    Args:
        params: List of function parameters
        carrier_type: The type to match

    Returns:
        List of indices of parameters matching the carrier type
    """
    return [
        i
        for i, param in enumerate(params)
        if param.annotation is not inspect._empty and param.annotation == carrier_type
    ]
