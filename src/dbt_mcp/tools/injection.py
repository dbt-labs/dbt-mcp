import inspect
from collections.abc import Callable, Iterable
from functools import wraps
from typing import Any, TypeVar, cast

R = TypeVar("R")


class AdaptError(TypeError): ...


def adapt_with_mapper[R](
    func: Callable[..., R], mapper: Callable[..., Any]
) -> Callable[..., R]:
    """
    Transform a function to accept a different input type by using a mapper function.

    Instead of calling `greet(user_id)`, you can call `greet(context)` where the
    `user_id` gets automatically extracted from the `context`.
    """

    func_sig = inspect.signature(func)
    mapper_sig = inspect.signature(mapper)

    mapper_return_type = mapper_sig.return_annotation

    if mapper_return_type is inspect._empty:
        raise AdaptError("mapper must have a return type annotation")

    any_replacements = False
    mapper_argument_types = set(
        param.annotation for param in mapper_sig.parameters.values()
    )
    if inspect._empty in mapper_argument_types:
        raise AdaptError("mapper must have type-annotated parameters")

    new_params = list(mapper_sig.parameters.values())
    for func_sig_param in func_sig.parameters.values():
        if func_sig_param.annotation == mapper_return_type:
            any_replacements = True
        elif func_sig_param.annotation not in mapper_argument_types:
            new_params.append(func_sig_param)

    if not any_replacements:
        return func

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
        func_args = {}
        for func_param in func_sig.parameters.values():
            if func_param.annotation == mapper_return_type:
                func_args[func_param.name] = mapped_value
            else:
                func_args[func_param.name] = bound_args.arguments[func_param.name]
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
