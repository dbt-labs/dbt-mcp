import inspect
from collections import OrderedDict
from collections.abc import Callable
from functools import wraps
from types import UnionType
from typing import Annotated, Any, Union, cast, get_args, get_origin, get_type_hints


class AdaptError(TypeError): ...


def _signature(func: Callable[..., Any]) -> inspect.Signature:
    declaration = inspect.signature(func)
    hints = get_type_hints(func, include_extras=True)
    return declaration.replace(
        parameters=[
            p.replace(annotation=hints.get(p.name, p.annotation))
            for p in declaration.parameters.values()
        ],
        return_annotation=hints.get("return", declaration.return_annotation),
    )


def _value_type(annotation: Any) -> Any:
    return (
        get_args(annotation)[0] if get_origin(annotation) is Annotated else annotation
    )


def _accepts(destination: Any, source: Any) -> bool:
    """Check the value types used by injection, without interpreting metadata."""
    destination, source = _value_type(destination), _value_type(source)
    if destination is Any or source is Any or destination == source:
        return True
    if get_origin(source) in (Union, UnionType):
        return all(_accepts(destination, member) for member in get_args(source))
    if get_origin(destination) in (Union, UnionType):
        return any(_accepts(member, source) for member in get_args(destination))
    destination_origin, source_origin = get_origin(destination), get_origin(source)
    if destination_origin or source_origin:
        if destination_origin != source_origin:
            return False
        if len(get_args(destination)) != len(get_args(source)):
            return False
        return all(
            _accepts(expected, actual)
            for expected, actual in zip(
                get_args(destination), get_args(source), strict=True
            )
        )
    return (
        isinstance(source, type)
        and isinstance(destination, type)
        and issubclass(source, destination)
    )


def remove_body_parameters[R](
    func: Callable[..., R], /, *names: str
) -> Callable[..., R]:
    """Omit parameters from invocation, leaving the original defaults in place."""
    original = _signature(func)
    unknown = set(names) - original.parameters.keys()
    if unknown:
        raise AdaptError(f"Unknown body parameters: {', '.join(sorted(unknown))}")
    required = {
        name
        for name in names
        if original.parameters[name].default is inspect.Parameter.empty
    }
    if required:
        raise AdaptError(
            f"Cannot remove required body parameters: {', '.join(sorted(required))}"
        )
    if not names:
        return func
    exposed = original.replace(
        parameters=[p for name, p in original.parameters.items() if name not in names]
    )

    def invoke(args: tuple[Any, ...], kwargs: dict[str, Any]) -> Any:
        values = exposed.bind(*args, **kwargs)
        body = inspect.BoundArguments(original, values.arguments)
        return func(*body.args, **body.kwargs)

    if inspect.iscoroutinefunction(func):

        @wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            return await invoke(args, kwargs)

    else:

        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> R:
            return invoke(args, kwargs)

    return cast(Callable[..., R], _with_signature(wrapper, exposed))


def adapt_with_mappers[R](
    func: Callable[..., R], /, **parameter_mappers: Callable[..., Any]
) -> Callable[..., R]:
    """Inject named parameters from mappers, sharing their caller inputs.

    Mapper inputs form the exposed signature; each mapper runs once per invocation.
    """
    if not parameter_mappers:
        return func

    original = _signature(func)
    unknown = parameter_mappers.keys() - original.parameters.keys()
    if unknown:
        raise AdaptError(f"Unknown mapper destinations: {', '.join(sorted(unknown))}")

    signatures = {}
    inputs: dict[str, inspect.Parameter] = {}
    for destination, mapper in parameter_mappers.items():
        signature = _signature(mapper)
        if signature.return_annotation is inspect.Parameter.empty:
            raise AdaptError("mapper must have a return type annotation")
        if not _accepts(
            original.parameters[destination].annotation, signature.return_annotation
        ):
            raise AdaptError(
                f"{destination}: mapper return type {signature.return_annotation!r} "
                f"is incompatible with {original.parameters[destination].annotation!r}"
            )
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
                metadata = get_args(canonical.annotation)[1:]
                parameter = parameter.replace(
                    annotation=Annotated[value_type, *metadata]
                    if metadata
                    else value_type
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
            original,
            OrderedDict((name, values[name]) for name in original.parameters),
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

    return cast(Callable[..., R], _with_signature(wrapper, exposed))


def _with_signature(
    func: Callable[..., Any], exposed: inspect.Signature
) -> Callable[..., Any]:
    func.__signature__ = exposed  # type: ignore[attr-defined]
    func.__annotations__ = {
        "return": exposed.return_annotation,
        **{name: p.annotation for name, p in exposed.parameters.items()},
    }
    return func
