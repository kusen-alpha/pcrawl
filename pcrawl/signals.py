from __future__ import annotations

import logging
from collections import defaultdict
from collections.abc import Callable
from typing import Any

from .utils import maybe_await

logger = logging.getLogger("pcrawl.signals")

SPIDER_OPENED = "spider_opened"
SPIDER_CLOSED = "spider_closed"
SESSION_OPENED = "session_opened"
SESSION_CLOSED = "session_closed"
TASK_RECEIVED = "task_received"
TASK_SUCCEEDED = "task_succeeded"
TASK_FAILED = "task_failed"
ITEM_SCRAPED = "item_scraped"
WORKER_STARTED = "worker_started"
WORKER_STOPPED = "worker_stopped"


class SignalManager:
    def __init__(self) -> None:
        self._handlers: dict[str, list[Callable[..., Any]]] = defaultdict(list)

    def connect(self, signal: str, handler: Callable[..., Any]) -> None:
        self._handlers[signal].append(handler)

    def disconnect(self, signal: str, handler: Callable[..., Any]) -> None:
        if handler in self._handlers[signal]:
            self._handlers[signal].remove(handler)

    async def send(self, signal: str, **kwargs: Any) -> None:
        for handler in list(self._handlers.get(signal, ())):
            try:
                await maybe_await(handler(**kwargs))
            except Exception:
                logger.exception("signal handler failed: %s", signal)
