from __future__ import annotations

import asyncio
import hashlib
import importlib
import inspect
from collections.abc import AsyncIterable, Iterable, Mapping
from typing import Any, AsyncIterator, Callable
from urllib.parse import urldefrag

from .exceptions import ConfigError


async def maybe_await(value: Any) -> Any:
    if inspect.isawaitable(value):
        return await value
    return value


async def iter_results(value: Any) -> AsyncIterator[Any]:
    """Normalize spider/middleware return values into a stream.

    Supports ``None``, a single result, sync iterables/generators, async
    iterables/generators, and coroutines. Mappings are treated as a single
    result so an ``Item`` dict is not iterated key by key.
    """

    value = await maybe_await(value)
    if value is None:
        return
    if isinstance(value, (str, bytes, bytearray, Mapping)):
        yield value
        return
    if hasattr(value, "__aiter__"):
        async for item in value:  # type: ignore[union-attr]
            yield item
        return
    if isinstance(value, AsyncIterable):
        async for item in value:
            yield item
        return
    if isinstance(value, Iterable):
        for item in value:
            yield item
        return
    yield value


def load_object(path: str) -> Any:
    """Load ``module:attr`` or ``module.attr``."""

    if ":" in path:
        module_name, attr_path = path.split(":", 1)
    else:
        module_name, _, attr_path = path.rpartition(".")
    if not module_name or not attr_path:
        raise ValueError(f"invalid object path: {path!r}")
    obj: Any = importlib.import_module(module_name)
    for part in attr_path.split("."):
        obj = getattr(obj, part)
    return obj


def object_path(obj: Any) -> str:
    return f"{obj.__module__}:{obj.__qualname__}"


def component_entries(values: Any) -> list[tuple[Any, int | float]]:
    """Normalize a Scrapy-style component declaration.

    Accepts a mapping ``{component: priority}``, a sequence of components, or
    a sequence of ``(component, priority)`` pairs. Lower priority runs first.
    """

    if not values:
        return []
    if isinstance(values, Mapping):
        return sorted(values.items(), key=lambda item: item[1])
    try:
        items = list(values)
    except TypeError:
        items = [values]
    entries: list[tuple[Any, int | float]] = []
    for index, item in enumerate(items):
        if (
            isinstance(item, tuple)
            and len(item) == 2
            and isinstance(item[1], (int, float))
        ):
            entries.append((item[0], item[1]))
        else:
            entries.append((item, index))
    return sorted(entries, key=lambda item: item[1])


def build_components(values: Any, settings: Any) -> list[Any]:
    result: list[Any] = []
    for value, _priority in component_entries(values):
        if isinstance(value, str):
            value = load_object(value)
        if isinstance(value, type):
            factory = getattr(value, "from_settings", None)
            value = factory(settings) if factory else value()
        result.append(value)
    return result


def accepts_ctx(func: Callable[..., Any]) -> bool:
    """Whether a bound method can receive a context argument.

    ``start_tasks`` is commonly written as ``start_tasks(self)`` in Scrapy-like
    code or ``start_tasks(self, ctx)`` when access to settings/resources is
    needed. Supporting both keeps the simple path simple.
    """

    try:
        signature = inspect.signature(func)
    except (TypeError, ValueError):
        return False
    positional = [
        p
        for p in signature.parameters.values()
        if p.kind
        in (
            inspect.Parameter.POSITIONAL_ONLY,
            inspect.Parameter.POSITIONAL_OR_KEYWORD,
        )
    ]
    return bool(positional) or any(
        p.kind is inspect.Parameter.VAR_POSITIONAL
        for p in signature.parameters.values()
    )


def _positional_signature(func: Callable[..., Any]) -> tuple[int, bool, bool]:
    try:
        signature = inspect.signature(func)
    except (TypeError, ValueError):
        return 1, False, False
    count = 0
    has_varargs = False
    has_varkw = False
    for param in signature.parameters.values():
        if param.kind in (
            inspect.Parameter.POSITIONAL_ONLY,
            inspect.Parameter.POSITIONAL_OR_KEYWORD,
        ):
            count += 1
        elif param.kind is inspect.Parameter.VAR_POSITIONAL:
            has_varargs = True
        elif param.kind is inspect.Parameter.VAR_KEYWORD:
            has_varkw = True
    return count, has_varargs, has_varkw


def accepts_no_required_args(func: Callable[..., Any]) -> bool:
    try:
        signature = inspect.signature(func)
    except (TypeError, ValueError):
        return True
    for param in signature.parameters.values():
        if param.kind in (
            inspect.Parameter.POSITIONAL_ONLY,
            inspect.Parameter.POSITIONAL_OR_KEYWORD,
            inspect.Parameter.KEYWORD_ONLY,
        ):
            if param.default is inspect.Parameter.empty:
                return False
    return True


def accepts_named_parameter(func: Callable[..., Any], name: str) -> bool:
    try:
        signature = inspect.signature(func)
    except (TypeError, ValueError):
        return False
    return name in signature.parameters


async def call_spider_callback(
    func: Callable[..., Any],
    value: Any,
    ctx: Any,
    cb_kwargs: Mapping[str, Any] | None = None,
) -> Any:
    """Call a spider callback with Scrapy-style and pcrawl-style signatures.

    Supported shapes: ``callback(value)``, ``callback(value, ctx)``,
    ``callback(value, **cb_kwargs)`` and ``callback(value, ctx, **cb_kwargs)``.
    """

    kwargs = dict(cb_kwargs or {})
    try:
        signature = inspect.signature(func)
    except (TypeError, ValueError):
        return await maybe_await(func(value, **kwargs))
    if "ctx" in signature.parameters or "context" in signature.parameters:
        name = "ctx" if "ctx" in signature.parameters else "context"
        return await maybe_await(func(value, **{name: ctx}, **kwargs))
    positional = [
        p
        for p in signature.parameters.values()
        if p.kind
        in (
            inspect.Parameter.POSITIONAL_ONLY,
            inspect.Parameter.POSITIONAL_OR_KEYWORD,
        )
    ]
    if len(positional) > 1 and positional[1].name in ("ctx", "context"):
        return await maybe_await(func(value, ctx, **kwargs))
    return await maybe_await(func(value, **kwargs))


async def call_component_hook(
    func: Callable[..., Any],
    *args: Any,
    ctx: Any,
) -> Any:
    """Call a middleware/pipeline hook with an optional trailing ``ctx``."""

    if accepts_named_parameter(func, "ctx"):
        return await maybe_await(func(*args, ctx=ctx))
    count, has_varargs, _ = _positional_signature(func)
    if count > len(args) or has_varargs:
        return await maybe_await(func(*args, ctx))
    return await maybe_await(func(*args))


def serialize_callable(value: Any) -> str | None:
    """Serialize a callback for shared-memory and Redis queues."""

    if value is None or isinstance(value, str):
        return value
    if inspect.ismethod(value):
        return value.__name__
    if inspect.isfunction(value):
        return object_path(value)
    raise ConfigError(
        "request callbacks/errbacks must be strings, module-level functions "
        "or bound spider methods to be queued across processes"
    )


def request_fingerprint(request: Any) -> str:
    """Stable fingerprint for request deduplication."""

    url = urldefrag(request.url)[0]
    body = request.body
    if isinstance(body, str):
        body = body.encode()
    digest = hashlib.sha1()
    digest.update(str(request.method).upper().encode())
    digest.update(b"\0")
    digest.update(url.encode())
    digest.update(b"\0")
    digest.update(body or b"")
    return digest.hexdigest()


async def call_with_context(
    func: Callable[..., Any],
    first_arg: Any,
    ctx: Any,
) -> Any:
    """Call ``func(first_arg[, ctx])`` based on its declared signature."""

    count, has_varargs, has_varkw = _positional_signature(func)
    if count == 0:
        if has_varargs:
            return await maybe_await(func(first_arg, ctx))
        return await maybe_await(func())
    if count == 1:
        if has_varkw:
            return await maybe_await(func(first_arg, ctx=ctx))
        if has_varargs:
            return await maybe_await(func(first_arg, ctx))
        return await maybe_await(func(first_arg))
    return await maybe_await(func(first_arg, ctx))


async def call_with_task_context(
    func: Callable[..., Any],
    task: Any,
    ctx: Any,
) -> Any:
    """Call ``func([task[, ctx]])`` for per-task hooks.

    Supports Scrapy-style zero-argument ``start_requests()`` overrides as
    well as ``start_requests(task)`` and ``start_requests(task, ctx)``.
    """

    count, has_varargs, has_varkw = _positional_signature(func)
    if count == 0:
        if has_varargs:
            return await maybe_await(func(task, ctx))
        return await maybe_await(func())
    if count == 1:
        if has_varkw:
            return await maybe_await(func(task, ctx=ctx))
        if has_varargs:
            return await maybe_await(func(task, ctx))
        return await maybe_await(func(task))
    return await maybe_await(func(task, ctx))


async def call_maybe_ctx(func: Callable[..., Any], ctx: Any = None) -> Any:
    if accepts_ctx(func):
        return await maybe_await(func(ctx))
    return await maybe_await(func())


def resolve_callable(
    spider: Any,
    value: Any,
    default: Callable[..., Any] | None = None,
) -> Callable[..., Any] | None:
    if value is None:
        return default
    if isinstance(value, str):
        resolved = getattr(spider, value, None)
        if resolved is not None:
            if not callable(resolved):
                raise TypeError(f"spider attribute {value!r} is not callable")
            return resolved
        try:
            resolved = load_object(value)
        except (ValueError, ImportError, AttributeError) as exc:
            raise AttributeError(
                f"spider {getattr(spider, 'name', spider)!r} has no method {value!r} "
                "and it is not an importable function"
            ) from exc
        if not callable(resolved):
            raise TypeError(f"{value!r} is not callable")
        return resolved
    if callable(value):
        return value
    raise TypeError(f"callback must be a string or callable, got {type(value)!r}")


async def sleep(seconds: float) -> None:
    if seconds > 0:
        await asyncio.sleep(seconds)
