import inspect
from collections.abc import Callable, Iterable
from functools import wraps
from dataclasses import dataclass
from typing import Annotated, Any, TypeVar, cast, get_args, get_origin
from dbt_mcp.tools.binding import merge_bound_arguments

R = TypeVar("R")


class AdaptError(TypeError): ...


@dataclass(frozen=True)
class BoundContext[T]:
    """A mapped context and arguments resolved alongside it for the tool call."""

    context: T
    arguments: dict[str, Any]


def adapt_with_mapper[R](
    func: Callable[..., R],
    mapper: Callable[..., Any],
    *,
    bound_arguments: frozenset[str] = frozenset(),
) -> Callable[..., R]:
    """
    Transform a function to accept a different input type by using a mapper function.

    Instead of calling `greet(user_id)`, you can call `greet(context)` where the
    `user_id` gets automatically extracted from the `context`.
    """

    func_sig = inspect.signature(func)
    mapper_sig = inspect.signature(mapper)

    mapper_return_type = mapper_sig.return_annotation
    returns_bound_context = get_origin(mapper_return_type) is BoundContext
    if returns_bound_context:
        mapper_return_type = get_args(mapper_return_type)[0]

    if mapper_return_type is inspect._empty:
        raise AdaptError("mapper must have a return type annotation")

    any_replacements = False
    mapper_argument_types = set(
        param.annotation for param in mapper_sig.parameters.values()
    )
    if inspect._empty in mapper_argument_types:
        raise AdaptError("mapper must have type-annotated parameters")

    new_params = []
    for name, parameter in mapper_sig.parameters.items():
        original = func_sig.parameters.get(name)
        if original is not None and get_origin(original.annotation) is Annotated:
            # Keep declarations on the canonical argument while allowing a mapper
            # to make the caller's input more specific (e.g. a required project ID).
            value_type = (
                get_args(parameter.annotation)[0]
                if get_origin(parameter.annotation) is Annotated
                else parameter.annotation
            )
            parameter = parameter.replace(
                annotation=Annotated[value_type, *get_args(original.annotation)[1:]]
            )
        new_params.append(parameter)
    for func_sig_param in func_sig.parameters.values():
        if func_sig_param.annotation == mapper_return_type:
            any_replacements = True
        elif (
            func_sig_param.name not in mapper_sig.parameters
            and func_sig_param.name not in bound_arguments
        ):
            new_params.append(func_sig_param)

    if not any_replacements:
        return func

    # Mapper defaults can precede required operation inputs. Preserve order within
    # each group while producing a valid Python signature for schema generation.
    new_params.sort(key=lambda p: (p.kind, p.default is not inspect._empty))
    new_sig = func_sig.replace(parameters=new_params)

    def get_annotations(sig: inspect.Signature) -> dict[str, Any]:
        annotations = {}
        annotations["return"] = sig.return_annotation
        for param in sig.parameters.values():
            if param.annotation is not inspect._empty:
                annotations[param.name] = param.annotation
        return annotations

    def bind_args(*args, **kwargs) -> inspect.BoundArguments:
        bound_args = new_sig.bind(*args, **kwargs)
        bound_args.apply_defaults()
        return bound_args

    def invoke_mapper(bound_args: inspect.BoundArguments) -> Any:
        mapper_args = {}
        for mapper_param in mapper_sig.parameters.values():
            mapper_args[mapper_param.name] = bound_args.arguments[mapper_param.name]
        return mapper(**mapper_args)

    def invoke_func(bound_args: inspect.BoundArguments, mapped_value: Any) -> Any:
        values = dict(bound_args.arguments)
        if returns_bound_context:
            if set(mapped_value.arguments) != bound_arguments:
                raise AdaptError(
                    "Context bindings must match the declared bound arguments"
                )
            values = merge_bound_arguments(values, mapped_value.arguments)
            mapped_value = mapped_value.context
        func_args = {}
        for func_param in func_sig.parameters.values():
            if func_param.annotation == mapper_return_type:
                func_args[func_param.name] = mapped_value
            else:
                func_args[func_param.name] = values[func_param.name]
        return func(**func_args)

    if inspect.iscoroutinefunction(func):

        @wraps(func)
        async def awrapper(*args: Any, **kwargs: Any) -> Any:
            bound_args = bind_args(*args, **kwargs)
            if inspect.iscoroutinefunction(mapper):
                mapped_value = await invoke_mapper(bound_args)
            else:
                mapped_value = invoke_mapper(bound_args)
            return await invoke_func(bound_args, mapped_value)

        awrapper.__signature__ = new_sig  # type: ignore[attr-defined]
        awrapper.__annotations__ = get_annotations(new_sig)
        return cast(Callable[..., R], awrapper)

    else:
        if inspect.iscoroutinefunction(mapper):
            raise AdaptError("Async mapper used with sync function")

        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> R:
            bound_args = bind_args(*args, **kwargs)
            mapped_value = invoke_mapper(bound_args)
            return invoke_func(bound_args, mapped_value)

        wrapper.__signature__ = new_sig  # type: ignore[attr-defined]
        wrapper.__annotations__ = get_annotations(new_sig)

        return wrapper


def adapt_with_mappers[R](
    func: Callable[..., R],
    mappers: Iterable[Callable[..., Any]],
) -> Callable[..., R]:
    for mapper in mappers:
        func = adapt_with_mapper(func, mapper)
    return func
