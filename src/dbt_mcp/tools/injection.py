import inspect
from collections import OrderedDict
from collections.abc import Callable, Iterable
from functools import wraps
from typing import Annotated, Any, cast, get_args, get_origin


class AdaptError(TypeError): ...


def adapt_with_mapper[R](
    func: Callable[..., R],
    mapper: Callable[..., Any],
    **parameter_mappers: Callable[..., Any],
) -> Callable[..., R]:
    """Convenience adapter for contexts identified by their annotated type."""
    return_type = inspect.signature(mapper).return_annotation
    if return_type is inspect.Parameter.empty:
        raise AdaptError("mapper must have a return type annotation")
    destinations = {
        name: mapper
        for name, parameter in inspect.signature(func).parameters.items()
        if parameter.annotation == return_type
    }
    overlap = destinations.keys() & parameter_mappers.keys()
    if overlap:
        raise AdaptError(f"Multiple mappers for: {', '.join(sorted(overlap))}")
    return adapt_with_mappers(func, **destinations, **parameter_mappers)


def adapt_with_mappers[R](
    func: Callable[..., R],
    mappers: Iterable[Callable[..., Any]] = (),
    /,
    **parameter_mappers: Callable[..., Any],
) -> Callable[..., R]:
    """Inject named parameters from mappers, sharing their caller inputs.

    The positional iterable retains sequential type-based context adaptation.
    Named mappers link directly to parameters and each run once per invocation.
    """
    for mapper in mappers:
        func = adapt_with_mapper(func, mapper)
    if not parameter_mappers:
        return func

    original = inspect.signature(func)
    unknown = parameter_mappers.keys() - original.parameters.keys()
    if unknown:
        raise AdaptError(f"Unknown mapper destinations: {', '.join(sorted(unknown))}")

    signatures = {}
    inputs: dict[str, inspect.Parameter] = {}
    for destination, mapper in parameter_mappers.items():
        signature = inspect.signature(mapper)
        if signature.return_annotation is inspect.Parameter.empty:
            raise AdaptError("mapper must have a return type annotation")
        signatures[destination] = signature
        for name, parameter in signature.parameters.items():
            if parameter.annotation is inspect.Parameter.empty:
                raise AdaptError("mapper must have type-annotated parameters")
            canonical = original.parameters.get(name)
            if canonical is not None and get_origin(canonical.annotation) is Annotated:
                value_type = (
                    get_args(parameter.annotation)[0]
                    if get_origin(parameter.annotation) is Annotated
                    else parameter.annotation
                )
                parameter = parameter.replace(
                    annotation=Annotated[
                        value_type, *get_args(canonical.annotation)[1:]
                    ]
                )
            if name in inputs and inputs[name] != parameter:
                raise AdaptError(f"Conflicting mapper input declarations for {name}")
            inputs[name] = parameter

    parameters = [
        *inputs.values(),
        *(
            parameter
            for name, parameter in original.parameters.items()
            if name not in parameter_mappers and name not in inputs
        ),
    ]
    parameters.sort(key=lambda p: (p.kind, p.default is not inspect.Parameter.empty))
    exposed = original.replace(parameters=parameters)
    is_async = inspect.iscoroutinefunction(func)
    if not is_async and any(
        inspect.iscoroutinefunction(mapper) for mapper in parameter_mappers.values()
    ):
        raise AdaptError("Async mapper used with sync function")

    def caller_arguments(
        args: tuple[Any, ...], kwargs: dict[str, Any]
    ) -> dict[str, Any]:
        bound = exposed.bind(*args, **kwargs)
        bound.apply_defaults()
        return dict(bound.arguments)

    def map_argument(destination: str, values: dict[str, Any]) -> Any:
        signature = signatures[destination]
        bound = inspect.BoundArguments(
            signature,
            OrderedDict((name, values[name]) for name in signature.parameters),
        )
        return parameter_mappers[destination](*bound.args, **bound.kwargs)

    def invoke(values: dict[str, Any]) -> Any:
        bound = inspect.BoundArguments(
            original, OrderedDict((name, values[name]) for name in original.parameters)
        )
        return func(*bound.args, **bound.kwargs)

    if is_async:

        @wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            inputs = caller_arguments(args, kwargs)
            values = dict(inputs)
            mapped: dict[int, Any] = {}
            for destination, mapper in parameter_mappers.items():
                if id(mapper) not in mapped:
                    value = map_argument(destination, inputs)
                    mapped[id(mapper)] = (
                        await value if inspect.isawaitable(value) else value
                    )
                values[destination] = mapped[id(mapper)]
            return await invoke(values)

    else:

        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> R:
            inputs = caller_arguments(args, kwargs)
            values = dict(inputs)
            mapped = {}
            for destination, mapper in parameter_mappers.items():
                if id(mapper) not in mapped:
                    mapped[id(mapper)] = map_argument(destination, inputs)
                values[destination] = mapped[id(mapper)]
            return invoke(values)

    wrapper.__signature__ = exposed  # type: ignore[attr-defined]
    wrapper.__annotations__ = {
        "return": exposed.return_annotation,
        **{name: p.annotation for name, p in exposed.parameters.items()},
    }
    return cast(Callable[..., R], wrapper)
