from __future__ import annotations

import logging
from typing import Any

from .http import Request, Response
from .settings import Settings
from .utils import build_components, call_component_hook, maybe_await

logger = logging.getLogger("pcrawl.middleware")


class DownloaderMiddlewareManager:
    def __init__(self, settings: Settings) -> None:
        self.middlewares = build_components(settings.downloader_middlewares, settings)

    async def open(self) -> None:
        for middleware in self.middlewares:
            hook = getattr(middleware, "open", None)
            if hook:
                await maybe_await(hook())

    async def close(self) -> None:
        for middleware in reversed(self.middlewares):
            hook = getattr(middleware, "close", None)
            if hook:
                await maybe_await(hook())

    async def process_request(self, request: Request, spider: Any, ctx: Any):
        for middleware in self.middlewares:
            hook = getattr(middleware, "process_request", None)
            if not hook:
                continue
            result = await call_component_hook(hook, request, spider, ctx=ctx)
            if result is not None:
                return result
        return None

    async def process_response(
        self,
        request: Request,
        response: Response,
        spider: Any,
        ctx: Any,
    ):
        current = response
        for middleware in reversed(self.middlewares):
            hook = getattr(middleware, "process_response", None)
            if not hook:
                continue
            result = await call_component_hook(hook, request, current, spider, ctx=ctx)
            if result is not None:
                current = result
        return current

    async def process_exception(
        self,
        request: Request,
        exc: BaseException,
        spider: Any,
        ctx: Any,
    ):
        for middleware in reversed(self.middlewares):
            hook = getattr(middleware, "process_exception", None)
            if not hook:
                continue
            result = await call_component_hook(hook, request, exc, spider, ctx=ctx)
            if result is not None:
                return result
        return None


class SpiderMiddlewareManager:
    def __init__(self, settings: Settings) -> None:
        self.middlewares = build_components(settings.spider_middlewares, settings)

    async def process_task(self, task: Any, spider: Any, ctx: Any):
        current = task
        for middleware in self.middlewares:
            hook = getattr(middleware, "process_task", None)
            if hook:
                result = await call_component_hook(hook, current, spider, ctx=ctx)
                if result is not None:
                    current = result
        return current

    async def process_item(self, item: Any, spider: Any, ctx: Any):
        current = item
        for middleware in self.middlewares:
            hook = getattr(middleware, "process_item", None)
            if hook:
                result = await call_component_hook(hook, current, spider, ctx=ctx)
                if result is not None:
                    current = result
        return current
