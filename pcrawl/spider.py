from __future__ import annotations

import logging
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, ClassVar, Literal

from .exceptions import ConfigError
from .http import Request, Response
from .resources import LeaseHandle, ResourcePlan, ResourceSetup, WorkerSession
from .settings import Settings
from .task import Task

ExecutionMode = Literal["inline", "async", "thread", "process"]


@dataclass
class ResourceContext:
    """Lightweight context available before resource pools are started."""

    spider: "Spider"
    settings: Settings
    mode: ExecutionMode


@dataclass
class SpiderContext:
    """Per-worker context passed to Spider callbacks."""

    spider: "Spider"
    settings: Settings
    session: WorkerSession
    stats: Any
    signals: Any
    worker_id: str
    runtime: Any
    downloader: Any

    def get_lease(self, name: str) -> LeaseHandle:
        return self.session.get(name)

    @property
    def user(self) -> dict[str, Any]:
        return self.session.user


class Spider:
    """Base spider.

    The API follows Scrapy's shape (``start_requests`` / ``parse`` /
    ``Request`` / ``Response``) and adds a durable ``Task`` layer plus a
    resource-aware ``WorkerSession``. Browser objects, account objects and proxy
    objects are deliberately *not* defined here: a spider subclass owns them.

    The execution mode is defined by the Spider class itself and cannot be
    changed through ``Settings``. Subclass ``AsyncSpider``,
    ``MultiThreadSpider`` or ``MultiProcessSpider`` to select another mode.
    """

    execution_mode: ClassVar[ExecutionMode] = "inline"
    name: str = ""
    start_urls: tuple[str, ...] = ()
    resources: ResourcePlan | dict[str, Any] | None = None
    # Class-level ``settings`` keeps the legacy override path;
    # ``custom_settings`` is the Scrapy-style per-spider partial override.
    settings: Settings | Mapping[str, Any] | None = None
    custom_settings: Mapping[str, Any] = {}

    def __init__(self) -> None:
        if not getattr(self, "name", ""):
            self.name = type(self).__name__

    @property
    def logger(self) -> logging.Logger:
        return logging.getLogger(f"pcrawl.spider.{self.name or type(self).__name__}")

    @property
    def resource_plan(self) -> ResourcePlan:
        return ResourcePlan.from_value(self.resources)

    def start_tasks(self, ctx: SpiderContext | None = None):
        """Yield initial durable ``Task`` objects for the engine queue.

        The base implementation turns each ``start_urls`` entry into a
        ``Task(payload={"url": url})``. Override this method to read tasks from
        a database, file, API, or any other source. Yield ``Task`` objects
        only; HTTP work belongs in ``start_requests(task, ctx)``.
        """

        for url in self.start_urls:
            yield Task(payload={"url": url})

    def start_requests(
        self,
        task: Task | None = None,
        ctx: SpiderContext | None = None,
    ):
        """Build requests from a durable ``Task``.

        The base implementation understands tasks produced by the base
        ``start_tasks``: ``task.payload["url"]`` becomes a ``Request`` with
        ``self.parse`` as the callback. Override this method for custom task
        payloads, or yield ``Item`` objects directly when no HTTP work is
        needed.
        """

        if task is None:
            for url in self.start_urls:
                yield Request(url=url, callback=self.parse)
            return
        url = task.payload.get("url")
        if not url:
            raise ConfigError(
                "the default start_requests expects task.payload['url']; "
                "override start_requests(task, ctx) for custom task payloads"
            )
        yield Request(url=url, callback=self.parse)

    def parse(self, response: Response, ctx: SpiderContext | None = None):
        """Parse a response; override in a subclass."""

        return None

    def open_spider(self) -> None:
        """Run once before workers start.

        Called after resources are ready and before ``start_tasks``. In
        process mode this runs once in the parent process; each child creates
        its own Spider instance and uses ``open_session`` for worker-local
        setup. Sync or async implementations are both supported.
        """

        return None

    def close_spider(self, reason: str) -> None:
        """Run once after workers stop.

        ``reason`` is ``"finished"`` on normal completion, ``"error"`` when
        the run raises, or the value from a raised :class:`CloseSpider`.
        Called before the queue and resources are closed. Sync or async
        implementations are both supported.
        """

        return None

    def open_session(self, session: WorkerSession) -> None:
        """Create browser/account state here.

        Raise :class:`pcrawl.exceptions.ResourceInvalid` when the leased
        resource is unusable so the runtime can release it with the right
        outcome.
        """

        return None

    def close_session(self, session: WorkerSession, reason: str) -> None:
        """Tear down browser/account state created in ``open_session``."""

        return None

    def session_routes(self, session: WorkerSession) -> set[str] | None:
        """Routes this session can serve; ``None`` means any route."""

        return None

    def bind_resource(self, request: Request, name: str, handle: LeaseHandle) -> None:
        """Expose a request-scoped lease to downloader middlewares.

        The default puts a snapshot under ``request.meta['pcrawl_resources']``.
        Override this to turn a proxy lease into ``request.proxies`` or any
        other client-specific shape.
        """

        request.meta.setdefault("pcrawl_resources", {})[name] = {
            "resource_id": handle.resource_id,
            "payload": handle.payload,
            "resource": handle.resource,
        }

    def start_resources(self, ctx: ResourceContext | None = None) -> ResourceSetup | None:
        """Declare purser pools and their initial records.

        The engine calls this once before workers start and chooses the
        concrete backend from the Spider execution mode. Return a
        :class:`ResourceSetup` (or a mapping); do not return live
        ``Purser``/``ResourcePool`` objects.
        """

        return None

    def setup_resources(self, resources: Any, ctx: ResourceContext | None = None) -> None:
        """Optional hook for live resource work after pools are started.

        Use ``start_resources`` for normal declarations. Override this when
        records must be imported lazily or generated after the backend starts.
        """

        return None

    def on_task_error(self, task: Task, exc: BaseException, ctx: SpiderContext) -> None:
        return None


class AsyncSpider(Spider):
    """Async preset with coroutine-capable hooks and configurable concurrency."""

    execution_mode = "async"


class MultiThreadSpider(Spider):
    """Thread preset.

    Thread workers share one Spider instance, so per-worker state belongs in
    ``ctx.user`` / ``session.user`` rather than mutable Spider attributes.
    """

    execution_mode = "thread"


class MultiProcessSpider(Spider):
    """Process preset; every child process creates its own Spider instance."""

    execution_mode = "process"
