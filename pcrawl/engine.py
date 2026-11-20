from __future__ import annotations

import asyncio
import logging
import multiprocessing
import os
import threading
import time
from collections.abc import Mapping
from queue import Empty
from types import ModuleType, SimpleNamespace
from typing import Any

from .exceptions import CloseSpider, ConfigError
from .http import Request
from .item import Item
from .queue import (
    MemoryTaskQueue,
    RedisRequestQueue,
    RedisTaskQueue,
    RequestQueue,
    SharedMemoryRequestQueue,
    SharedMemoryTaskQueue,
    TaskQueue,
    make_request_queue,
    make_task_queue,
    redis_request_queue_config,
    redis_task_queue_config,
    request_queue_from_config,
    shared_request_queue_config,
    shared_task_queue_config,
    task_queue_from_config,
)
from .resources import (
    ResourceManager,
    ResourceRuntime,
    WorkerSession,
    make_backend,
    resource_type_paths,
)
from .runtime import WorkerRuntime
from .settings import Settings
from .signals import SPIDER_CLOSED, SPIDER_OPENED, SignalManager
from .spider import ExecutionMode, ResourceContext, Spider, SpiderContext
from .stats import Stats
from .task import Task
from .utils import (
    accepts_no_required_args,
    call_maybe_ctx,
    call_with_context,
    iter_results,
    load_object,
    maybe_await,
    object_path,
)

logger = logging.getLogger("pcrawl.engine")


class CrawlerEngine:
    """Owns execution mode, resource backend, queue and worker lifecycle."""

    def __init__(
        self,
        spider: type[Spider] | Spider,
        settings: Settings | Mapping[str, Any] | None = None,
        *,
        settings_module: str | ModuleType | None = None,
        task_queue: TaskQueue | None = None,
        request_queue: RequestQueue | None = None,
        signals: SignalManager | None = None,
        stats: Stats | None = None,
    ) -> None:
        self.spider_cls: type[Spider] = spider if isinstance(spider, type) else type(spider)
        self.spider = spider() if isinstance(spider, type) else spider
        self.settings = self._resolve_settings(settings, settings_module)
        self.mode = self._resolve_execution_mode()
        self.settings.validate()
        if self.mode in ("thread", "process") and self.settings.purser_mode == "embedded":
            raise ConfigError(
                "embedded purser cannot be shared across threads/processes; "
                "use purser_mode='local' or leave it on 'auto'"
            )
        self.spider.settings = self.settings
        self.task_queue = task_queue
        self.request_queue = request_queue
        self.signals = signals or SignalManager()
        self.stats = stats or Stats()
        self.resources: ResourceManager | None = None
        self.resource_runtime: ResourceRuntime | None = None
        self._manager: Any = None
        self._task_queue: TaskQueue | None = None
        self._request_queue: RequestQueue | None = None
        self._resource_type_paths: dict[str, str] = {}

    def _resolve_settings(
        self,
        settings: Settings | Mapping[str, Any] | None,
        settings_module: str | ModuleType | None,
    ) -> Settings:
        resolved = Settings.load(settings_module=settings_module)
        self._apply_settings(resolved, getattr(self.spider_cls, "settings", None), "Spider.settings")
        self._apply_settings(
            resolved,
            getattr(self.spider_cls, "custom_settings", None),
            "Spider.custom_settings",
        )
        self._apply_settings(resolved, settings, "engine settings")
        return resolved

    def _resolve_execution_mode(self) -> ExecutionMode:
        mode = getattr(self.spider_cls, "execution_mode", None)
        if mode not in ("inline", "async", "thread", "process"):
            raise ConfigError(
                f"{self.spider_cls.__name__}.execution_mode must be one of "
                "'inline', 'async', 'thread' or 'process'; got {mode!r}"
            )
        return mode

    @staticmethod
    def _apply_settings(
        target: Settings,
        source: Settings | Mapping[str, Any] | None,
        label: str,
    ) -> None:
        if source is None:
            return
        if isinstance(source, Settings):
            target.update(source.explicit_values())
            return
        if isinstance(source, Mapping):
            target.update(source)
            return
        raise ConfigError(f"{label} must be a Settings object or a mapping")

    def run(self) -> dict[str, Any]:
        return asyncio.run(self.run_async())

    async def run_async(self) -> dict[str, Any]:
        try:
            await self._start_resources()
        except BaseException:
            await self._close_resources()
            raise

        spider_opened = False
        close_reason = "finished"
        try:
            await self._call_open_spider()
            spider_opened = True
            await self.signals.send(SPIDER_OPENED, spider=self.spider)

            self._task_queue = self._build_task_queue()
            self._request_queue = self._build_request_queue()
            await self._task_queue.open()
            await self._request_queue.open()
            await self._seed_start_requests(self._request_queue)
            await self._seed_tasks(self._task_queue)

            if self.mode == "process":
                run_reason = await self._run_process(
                    self._task_queue,
                    self._request_queue,
                )
            else:
                run_reason = await self._run_in_process(
                    self._task_queue,
                    self._request_queue,
                )
            if run_reason is not None:
                close_reason = run_reason
        except CloseSpider as exc:
            close_reason = exc.reason
        except BaseException:
            close_reason = "error"
            raise
        finally:
            if spider_opened:
                await self._call_close_spider(close_reason)
            if self._request_queue is not None:
                await self._request_queue.close()
                self._request_queue = None
            if self._task_queue is not None:
                await self._task_queue.close()
                self._task_queue = None
            await self._close_resources()
            await self.signals.send(SPIDER_CLOSED, spider=self.spider, stats=self.stats)
        return self.stats.snapshot()

    async def _call_open_spider(self) -> None:
        await maybe_await(self.spider.open_spider())

    async def _call_close_spider(self, reason: str) -> None:
        try:
            await maybe_await(self.spider.close_spider(reason))
        except Exception:
            logger.exception("spider.close_spider failed")

    async def _start_resources(self) -> None:
        ctx = ResourceContext(spider=self.spider, settings=self.settings, mode=self.mode)
        legacy = getattr(self.spider, "resources", None)
        if legacy:
            raise ConfigError(
                "Spider.resources has been removed; declare pools inside "
                "start_resources(resources) with resources.create_pool(...)"
            )

        runtime = ResourceRuntime(self.settings, self.mode)
        self.resource_runtime = runtime
        hook = getattr(self.spider, "start_resources", None)
        if hook is not None:
            result = await call_with_context(hook, runtime, ctx)
            if result is not None:
                raise ConfigError(
                    "start_resources must not return a value; use the resources "
                    "argument to register adapters, create pools and import records"
                )

        await runtime.start()
        self._resource_type_paths = resource_type_paths(runtime.adapters)
        self.resources = ResourceManager(runtime.backend, self.settings)
        await self.resources.start()

    async def _close_resources(self) -> None:
        if self.resources is not None:
            try:
                await self.resources.close()
            finally:
                self.resources = None
        if self._manager is not None:
            self._manager.shutdown()
            self._manager = None
        if self.resource_runtime is not None:
            await self.resource_runtime.close()
            self.resource_runtime = None

    def _build_task_queue(self) -> TaskQueue:
        if self.task_queue is not None:
            return self.task_queue
        if self.mode == "process" and self.settings.queue_backend == "memory":
            ctx = multiprocessing.get_context(self.settings.mp_start_method)
            if self._manager is None:
                self._manager = ctx.Manager()
            return SharedMemoryTaskQueue(
                self._manager.list(),
                self._manager.Lock(),
                order=self.settings.queue_order,
            )
        return _make_task_queue(self.settings)

    def _build_request_queue(self) -> RequestQueue:
        if self.request_queue is not None:
            return self.request_queue
        if self.mode == "process":
            if self.settings.queue_backend == "redis":
                return RedisRequestQueue(self.settings)
            ctx = multiprocessing.get_context(self.settings.mp_start_method)
            if self._manager is None:
                self._manager = ctx.Manager()
            return SharedMemoryRequestQueue(
                self._manager.list(),
                self._manager.Lock(),
                self._manager.dict(),
                order=self.settings.queue_order,
                dedup_ttl=self.settings.request_dedup_ttl,
            )
        return make_request_queue(self.settings)

    async def _seed_start_requests(self, queue: RequestQueue) -> None:
        start_requests = self.spider.start_requests
        if getattr(start_requests, "__func__", None) is Spider.start_requests:
            # The base implementation builds requests per task from start_tasks.
            return

        if not accepts_no_required_args(start_requests):
            # A task-oriented override needs a Task; nothing to seed here.
            return
        async for result in iter_results(start_requests()):
            if not isinstance(result, Request):
                raise TypeError(
                    "start_requests() without a task must yield Request objects, "
                    f"got {type(result)!r}"
                )
            await queue.put(result)

    async def _seed_tasks(self, queue: TaskQueue) -> None:
        if self.resources is None:
            raise ConfigError("resource manager was not initialized")
        seed_session = WorkerSession(
            manager=self.resources,
            worker_id="seed",
            heartbeat_interval=max(0.1, self.settings.lease_ttl / 3),
        )
        ctx = SpiderContext(
            spider=self.spider,
            settings=self.settings,
            session=seed_session,
            stats=self.stats,
            signals=self.signals,
            worker_id="seed",
            runtime=None,
            downloader=None,
        )
        try:
            results = await call_maybe_ctx(self.spider.start_tasks, ctx)
            async for result in iter_results(results):
                if isinstance(result, Task):
                    await queue.put(result)
                elif isinstance(result, Request):
                    raise TypeError(
                        "start_tasks must yield Task objects; build Requests in "
                        "start_requests(task, ctx)"
                    )
                elif isinstance(result, (Item, Mapping)):
                    raise TypeError("start_tasks must yield Task objects, not items")
                else:
                    raise TypeError(f"unsupported start_tasks result: {type(result)!r}")
        finally:
            await seed_session.release_all()

    async def _run_in_process(
        self,
        task_queue: TaskQueue,
        request_queue: RequestQueue,
    ) -> str | None:
        if self.resources is None:
            raise ConfigError("resource manager was not initialized")
        if self.mode == "thread":
            return await self._run_threads(task_queue, request_queue)

        stop_event = threading.Event()
        count = 1 if self.mode == "inline" else self.settings.concurrency
        runtimes = [
            self._new_runtime(
                task_queue,
                request_queue,
                f"{self.settings.name}-{index}",
                resources=self.resources,
                external_stop=stop_event,
                spider=self.spider,
            )
            for index in range(count)
        ]
        await asyncio.gather(*(runtime.run() for runtime in runtimes))
        return self._first_close_reason(runtimes)

    async def _run_threads(
        self,
        task_queue: TaskQueue,
        request_queue: RequestQueue,
    ) -> str | None:
        errors: list[BaseException] = []
        close_reasons: list[str] = []
        stop_event = threading.Event()

        def target(index: int) -> None:
            try:
                runtime = self._new_runtime(
                    task_queue,
                    request_queue,
                    f"{self.settings.name}-thread-{index}",
                    resources=self.resources,
                    external_stop=stop_event,
                    spider=self.spider,
                )
                asyncio.run(runtime.run())
                if runtime.close_reason is not None:
                    close_reasons.append(runtime.close_reason)
            except BaseException as exc:  # pragma: no cover - surfaced below
                errors.append(exc)

        threads = [
            threading.Thread(target=target, args=(index,), name=f"pcrawl-{index}")
            for index in range(self.settings.workers)
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        if errors:
            raise errors[0]
        return close_reasons[0] if close_reasons else None

    @staticmethod
    def _first_close_reason(runtimes: list[WorkerRuntime]) -> str | None:
        for runtime in runtimes:
            if runtime.close_reason is not None:
                return runtime.close_reason
        return None

    def _new_runtime(
        self,
        task_queue: TaskQueue,
        request_queue: RequestQueue,
        worker_id: str,
        resources: ResourceManager,
        external_stop: Any = None,
        spider: Spider | None = None,
    ) -> WorkerRuntime:
        spider = spider or self.spider_cls()
        return WorkerRuntime(
            spider=spider,
            settings=self.settings,
            task_queue=task_queue,
            request_queue=request_queue,
            resources=resources,
            worker_id=worker_id,
            signals=self.signals,
            stats=self.stats,
            external_stop=external_stop,
        )

    async def _run_process(
        self,
        task_queue: TaskQueue,
        request_queue: RequestQueue,
    ) -> str | None:
        if self.settings.purser_mode == "embedded":
            raise ConfigError("process mode cannot use an embedded purser; use local or off")

        ctx = multiprocessing.get_context(self.settings.mp_start_method)
        stop_event = ctx.Event()
        if isinstance(task_queue, SharedMemoryTaskQueue):
            tq_config = shared_task_queue_config(task_queue)
        elif isinstance(task_queue, RedisTaskQueue):
            tq_config = redis_task_queue_config(task_queue.settings)
        else:
            raise ConfigError(
                "process mode requires a shared-memory queue or a Redis queue; "
                "pass task_queue=None to let the engine choose"
            )
        if isinstance(request_queue, SharedMemoryRequestQueue):
            rq_config = shared_request_queue_config(request_queue)
        elif isinstance(request_queue, RedisRequestQueue):
            rq_config = redis_request_queue_config(request_queue.settings)
        else:
            raise ConfigError(
                "process mode requires a shared-memory request queue or a Redis "
                "request queue; pass request_queue=None to let the engine choose"
            )
        stats_queue = ctx.Queue()
        close_reason_queue = ctx.Queue()

        scheduler_address = (
            self.resource_runtime.scheduler_address
            if self.resource_runtime is not None
            else None
        )
        worker_config = {
            "settings": self.settings.to_dict(),
            "spider_path": object_path(self.spider_cls),
            "task_queue": tq_config,
            "request_queue": rq_config,
            "scheduler_address": scheduler_address,
            "resource_types": self._resource_type_paths,
            "stop_event": stop_event,
            "stats_queue": stats_queue,
            "close_reason_queue": close_reason_queue,
        }

        processes: list[Any] = []
        for index in range(self.settings.workers):
            process = ctx.Process(
                target=run_process_worker,
                args=(worker_config, index),
                name=f"pcrawl-worker-{index}",
            )
            process.start()
            processes.append(process)

        try:
            while any(process.is_alive() for process in processes):
                for process in processes:
                    process.join(timeout=0.2)
                if stop_event.is_set():
                    break
        except KeyboardInterrupt:
            stop_event.set()
        finally:
            deadline = time.monotonic() + self.settings.shutdown_timeout
            for process in processes:
                remaining = max(0.0, deadline - time.monotonic())
                process.join(timeout=remaining)
                if process.is_alive():
                    process.terminate()
                    process.join(timeout=5)
            self._drain_stats(stats_queue)
            stats_queue.close()
            close_reason = self._drain_close_reason(close_reason_queue)
            close_reason_queue.close()
        return close_reason

    def _drain_stats(self, stats_queue: Any) -> None:
        while True:
            try:
                worker_stats = stats_queue.get_nowait()
            except Empty:
                return
            except (EOFError, OSError):
                return
            if isinstance(worker_stats, Mapping):
                self.stats.merge(dict(worker_stats))

    @staticmethod
    def _drain_close_reason(reason_queue: Any) -> str | None:
        reason = None
        while True:
            try:
                value = reason_queue.get_nowait()
            except Empty:
                return reason
            except (EOFError, OSError):
                return reason
            if reason is None:
                reason = str(value)


def _make_task_queue(settings: Settings) -> TaskQueue:
    if settings.queue_backend == "redis":
        return RedisTaskQueue(settings)
    return MemoryTaskQueue(
        poll_interval=min(0.05, settings.claim_poll_interval),
        order=settings.queue_order,
    )


def run_process_worker(config: dict[str, Any], index: int) -> None:
    """Entry point for a long-lived child process (must stay importable)."""

    _configure_logging(config["settings"].get("log_level", "INFO"))
    settings = Settings.from_dict(config["settings"])
    spider_cls = load_object(config["spider_path"])
    spider = spider_cls()
    spider.settings = settings
    task_queue = task_queue_from_config(config["task_queue"])
    request_queue = request_queue_from_config(config["request_queue"])

    scheduler = None
    if config.get("scheduler_address"):
        from purser.local.protocol import SchedulerAddress

        address = SchedulerAddress.from_env(config["scheduler_address"])
        scheduler = SimpleNamespace(address=address)

    resource_types: dict[str, type] = {}
    for kind, path in (config.get("resource_types") or {}).items():
        try:
            resource_types[str(kind)] = load_object(path)
        except Exception:
            logger.warning(
                "could not restore resource type %r from %r", kind, path, exc_info=True
            )
    backend = make_backend(settings, scheduler, resource_types=resource_types)
    resources = ResourceManager(backend, settings)
    stats = Stats()
    stats_queue = config.get("stats_queue")
    close_reason_queue = config.get("close_reason_queue")

    async def main() -> None:
        await resources.start()
        await task_queue.open()
        await request_queue.open()
        runtime = None
        try:
            runtime = WorkerRuntime(
                spider=spider,
                settings=settings,
                task_queue=task_queue,
                request_queue=request_queue,
                resources=resources,
                worker_id=f"{settings.name}-proc-{os.getpid()}-{index}",
                stats=stats,
                external_stop=config["stop_event"],
            )
            await runtime.run()
        finally:
            await request_queue.close()
            await task_queue.close()
            await resources.close()
            if stats_queue is not None:
                stats_queue.put(stats.snapshot())
            if close_reason_queue is not None and runtime is not None:
                if runtime.close_reason is not None:
                    close_reason_queue.put(runtime.close_reason)

    try:
        asyncio.run(main())
    except Exception:
        logger.exception("pcrawl worker process failed")
        raise


def _configure_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, str(level).upper(), logging.INFO),
        format="%(asctime)s %(levelname)s [%(process)d] %(name)s: %(message)s",
    )
