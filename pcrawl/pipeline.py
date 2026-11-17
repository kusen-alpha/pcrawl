from __future__ import annotations

import logging
from typing import Any

from .item import Item
from .settings import Settings
from .utils import build_components, call_component_hook, maybe_await

logger = logging.getLogger("pcrawl.pipeline")


class PipelineManager:
    def __init__(self, settings: Settings) -> None:
        self.pipelines = build_components(settings.item_pipelines, settings)

    async def open(self, spider: Any) -> None:
        for pipeline in self.pipelines:
            hook = getattr(pipeline, "open_spider", None)
            if hook:
                await maybe_await(hook(spider))

    async def close(self, spider: Any) -> None:
        for pipeline in reversed(self.pipelines):
            hook = getattr(pipeline, "close_spider", None)
            if hook:
                await maybe_await(hook(spider))

    async def process_item(self, item: Item, spider: Any, ctx: Any) -> Item:
        current = item
        for pipeline in self.pipelines:
            hook = getattr(pipeline, "process_item", None)
            if not hook:
                continue
            result = await call_component_hook(hook, current, spider, ctx=ctx)
            if result is not None:
                current = result if isinstance(result, Item) else Item(result)
        return current
