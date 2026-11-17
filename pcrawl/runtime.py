from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Mapping
from typing import Any

from .downloader import HttpDownloader
from .exceptions import (
    CloseSpider,
    IgnoreRequest,
    ResourceInvalid,
    ResourceUnavailable,
)
from .http import Request, RequestFailure, Response
from .item import Item
from .middleware import DownloaderMiddlewareManager, SpiderMiddlewareManager
from .outcomes import ResourceOutcome
from .pipeline import PipelineManager
from .queue import RequestQueue, TaskQueue
from .resources import LeaseHandle, ResourceManager, ResourceSpec, WorkerSession
from .settings import Settings
from .signals import (
    ITEM_SCRAPED,
    SESSION_CLOSED,
    SESSION_OPENED,
    TASK_FAILED,
    TASK_RECEIVED,
    TASK_SUCCEEDED,
    WORKER_STARTED,
    WORKER_STOPPED,
    SignalManager,
)
from .spider import Spider, SpiderContext
from .stats import Stats
from .task import Task
from .utils import (
    call_spider_callback,
    call_with_task_context,
    iter_results,
    maybe_await,
    resolve_callable,
)

logger = logging.getLogger("pcrawl.runtime")


class _RequestLeaseGroup:
    """Acquire/release request-scoped resources around one HTTP fetch."""

    def __init__(
        self,
        runtime: "WorkerRuntime",
        request: Request,
        session: WorkerSession,
        ctx: SpiderContext,
    ) -> None:
        self.runtime = runtime
        self.request = request
        self.session = session
        self.ctx = ctx
        self.handles: list[tuple[str, LeaseHandle]] = []

    async def __aenter__(self) -> "_RequestLeaseGroup":
        specs = {
            name: ResourceSpec.from_value(value)
            for name, value in self.request.resource_specs.items()
        }
        try:
            for name, spec in specs.items():
                handle = await self.runtime.resources.acquire(
                    name,
                    spec,
                    consumer=f"{self.session.worker_id}:{self.session.id}:request",
                )
                await maybe_await(self.runtime.spider.bind_resource(self.request, name, handle))
                self.handles.append((name, handle))
        except BaseException:
            await self._release(ResourceOutcome.NETWORK_ERROR)
            raise
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        if exc_type is not None:
            outcome = self.request.meta.get(
                "pcrawl_resource_outcome",
                ResourceOutcome.NETWORK_ERROR,
            )
        else:
            outcome = self.request.meta.get("pcrawl_resource_outcome", ResourceOutcome.OK)
        if not isinstance(outcome, ResourceOutcome):
            outcome = ResourceOutcome(str(outcome))
        await self._release(outcome)

    async def _release(self, outcome: ResourceOutcome) -> None:
        for name, handle in reversed(self.handles):
            try:
                await handle.release(outcome=outcome)
            except Exception:
                logger.exception("failed to release request resource %s", name)
        self.handles.clear()


class WorkerRuntime:
    def __init__(
        self,
        *,
        spider: Spider,
        settings: Settings,
        task_queue: TaskQueue,
        request_queue: RequestQueue,
        resources: ResourceManager,
        worker_id: str,
        signals: SignalManager | None = None,
        stats: Stats | None = None,
        external_stop: Any = None,
    ) -> None:
        self.spider = spider
        self.settings = settings
        self.task_queue = task_queue
        self.request_queue = request_queue
        self.resources = resources
        self.worker_id = worker_id
        self.signals = signals or SignalManager()
        self.stats = stats or Stats()
        self.external_stop = external_stop
        self.close_reason: str | None = None
        self._stop = False
        self._prefer_request = False
        self.downloader: HttpDownloader | None = None
        self.downloader_middlewares: DownloaderMiddlewareManager | None = None
        self.spider_middlewares: SpiderMiddlewareManager | None = None
        self.pipelines: PipelineManager | None = None
        self.session: WorkerSession | None = None

    def stop(self) -> None:
        self._stop = True

    def request_close(self, reason: str) -> None:
        if self.close_reason is None:
            self.close_reason = reason
        self._stop = True
        if self.external_stop is not None:
            self.external_stop.set()

    def _should_stop(self) -> bool:
        if self._stop:
            return True
        return self.external_stop is not None and self.external_stop.is_set()

    async def run(self) -> None:
        self.downloader = HttpDownloader(self.settings)
        self.downloader_middlewares = DownloaderMiddlewareManager(self.settings)
        self.spider_middlewares = SpiderMiddlewareManager(self.settings)
        self.pipelines = PipelineManager(self.settings)
        await self.downloader_middlewares.open()
        await self.pipelines.open(self.spider)
        await self.signals.send(WORKER_STARTED, worker_id=self.worker_id, spider=self.spider)

        try:
            while not self._should_stop():
                if not self.settings.keep_alive and await self._pending_size() == 0:
                    break
                try:
                    session = await self._open_session_with_retry()
                except ResourceUnavailable as exc:
                    logger.error("worker %s could not open session: %s", self.worker_id, exc)
                    break
                if session is None:
                    break
                self.session = session
                try:
                    await self._run_session(session)
                finally:
                    await self._close_session(
                        session,
                        self.close_reason or "worker_loop",
                    )
                    self.session = None
        finally:
            if self.pipelines is not None:
                await self.pipelines.close(self.spider)
            if self.downloader_middlewares is not None:
                await self.downloader_middlewares.close()
            if self.downloader is not None:
                await self.downloader.close()
            await self.signals.send(WORKER_STOPPED, worker_id=self.worker_id, spider=self.spider)

    async def _pending_size(self) -> int:
        return await self.task_queue.size() + await self.request_queue.size()

    async def _open_session_with_retry(self) -> WorkerSession | None:
        deadline = (
            None
            if self.settings.resource_wait_timeout <= 0
            else time.monotonic() + self.settings.resource_wait_timeout
        )
        while not self._should_stop():
            try:
                return await self._open_session()
            except CloseSpider as exc:
                self.request_close(exc.reason)
                return None
            except ResourceInvalid as exc:
                logger.warning("session resource invalid: %s", exc)
            except ResourceUnavailable as exc:
                logger.info("waiting for resources: %s", exc)
            if deadline is not None and time.monotonic() >= deadline:
                raise ResourceUnavailable("timed out waiting for session resources")
            await asyncio.sleep(0.5)
        return None

    async def _open_session(self) -> WorkerSession:
        plan = self.spider.resource_plan
        session = WorkerSession(
            manager=self.resources,
            plan=plan,
            worker_id=self.worker_id,
        )
        try:
            for name, spec in plan.session_specs().items():
                try:
                    await session.acquire(name, spec)
                except ResourceInvalid:
                    raise
                except Exception as exc:
                    raise ResourceUnavailable(
                        f"could not acquire session resource {name!r}: {exc}"
                    ) from exc
            if plan.session_specs():
                session.start_heartbeat(max(0.1, self.settings.lease_ttl / 3))
            await maybe_await(self.spider.open_session(session))
        except ResourceInvalid as exc:
            session.invalidate(exc.outcome)
            await session.release_all()
            raise
        except BaseException:
            await session.release_all()
            raise

        await self.signals.send(SESSION_OPENED, worker_id=self.worker_id, session=session)
        return session

    async def _close_session(self, session: WorkerSession, reason: str) -> None:
        try:
            await maybe_await(self.spider.close_session(session, reason))
        except Exception:
            logger.exception("spider.close_session failed")
        finally:
            await session.release_all()
            await self.signals.send(SESSION_CLOSED, worker_id=self.worker_id, session=session)

    def _context(self, session: WorkerSession) -> SpiderContext:
        return SpiderContext(
            spider=self.spider,
            settings=self.settings,
            session=session,
            stats=self.stats,
            signals=self.signals,
            worker_id=self.worker_id,
            runtime=self,
            downloader=self.downloader,
        )

    async def _claim_next(
        self,
        routes: set[str] | None,
        timeout: float,
    ) -> tuple[str, Any] | None:
        """Round-robin between task and request queues."""

        deadline = time.monotonic() + max(0.0, timeout)
        while not self._should_stop():
            if self._prefer_request:
                request = await self.request_queue.claim(routes, timeout=0.0)
                if request is not None:
                    self._prefer_request = False
                    return "request", request
                task = await self.task_queue.claim(routes, timeout=0.0)
                if task is not None:
                    self._prefer_request = True
                    return "task", task
            else:
                task = await self.task_queue.claim(routes, timeout=0.0)
                if task is not None:
                    self._prefer_request = True
                    return "task", task
                request = await self.request_queue.claim(routes, timeout=0.0)
                if request is not None:
                    self._prefer_request = False
                    return "request", request

            if time.monotonic() >= deadline:
                return None
            await asyncio.sleep(min(0.05, max(0.001, self.settings.claim_poll_interval)))
        return None

    async def _run_session(self, session: WorkerSession) -> None:
        ctx = self._context(session)
        routes = await maybe_await(self.spider.session_routes(session))
        idle_since: float | None = None

        while not self._should_stop():
            claimed = await self._claim_next(
                routes,
                timeout=self.settings.claim_poll_interval,
            )
            if claimed is None:
                if self.settings.keep_alive:
                    continue
                if idle_since is None:
                    idle_since = time.monotonic()
                elif time.monotonic() - idle_since >= self.settings.idle_timeout:
                    return
                continue

            idle_since = None
            kind, item = claimed
            if kind == "task":
                try:
                    await self._process_task(item, ctx)
                except ResourceInvalid as exc:
                    session.invalidate(exc.outcome)
                    await self._retry_or_drop(item, exc, ctx)
                    return
                except Exception as exc:
                    await self._retry_or_drop(item, exc, ctx)
                continue

            try:
                await self._process_request(item, ctx)
            except IgnoreRequest:
                self.stats.inc("request_ignored")
                await self.request_queue.ack(item)
            except ResourceInvalid as exc:
                session.invalidate(exc.outcome)
                await self._on_request_error(item, exc, ctx)
                return
            except Exception:
                logger.exception("request %s callback failed", item.url)
                raise

    async def _process_task(self, task: Task, ctx: SpiderContext) -> None:
        self.stats.inc("task_received")
        await self.signals.send(TASK_RECEIVED, task=task, worker_id=self.worker_id)
        assert self.spider_middlewares is not None
        try:
            task = await self.spider_middlewares.process_task(task, self.spider, ctx)
            results = await call_with_task_context(self.spider.start_requests, task, ctx)
            await self._consume(results, ctx, parent_task_id=task.id)
        except CloseSpider as exc:
            await self.task_queue.ack(task)
            self.request_close(exc.reason)
            return
        except IgnoreRequest:
            self.stats.inc("task_ignored")
            await self.task_queue.ack(task)
            return
        self.stats.inc("task_succeeded")
        await self.task_queue.ack(task)
        await self.signals.send(TASK_SUCCEEDED, task=task, worker_id=self.worker_id)

    async def _retry_or_drop(self, task: Task, exc: BaseException, ctx: SpiderContext) -> None:
        max_retries = (
            task.max_retries
            if task.max_retries is not None
            else self.settings.task_max_retries
        )
        task.retries += 1
        try:
            await maybe_await(self.spider.on_task_error(task, exc, ctx))
        except Exception:
            logger.exception("spider.on_task_error failed")
        await self.signals.send(TASK_FAILED, task=task, exception=exc, worker_id=self.worker_id)
        if task.retries <= max_retries:
            self.stats.inc("task_retried")
            await self.task_queue.nack(task, delay=self.settings.task_retry_delay)
            logger.warning(
                "task %s failed (retry %s/%s): %s",
                task.id,
                task.retries,
                max_retries,
                exc,
            )
        else:
            self.stats.inc("task_dropped")
            await self.task_queue.ack(task)
            logger.error(
                "task %s dropped after retries",
                task.id,
                exc_info=(type(exc), exc, exc.__traceback__),
            )

    async def _process_request(self, request: Request, ctx: SpiderContext) -> None:
        self.stats.inc("request_received")
        assert self.downloader_middlewares is not None
        assert self.downloader is not None

        try:
            if request.resource_specs:
                async with _RequestLeaseGroup(self, request, ctx.session, ctx):
                    response = await self._download_with_middlewares(request, ctx)
            else:
                response = await self._download_with_middlewares(request, ctx)
        except CloseSpider as exc:
            await self.request_queue.ack(request)
            self.request_close(exc.reason)
            return
        except IgnoreRequest:
            self.stats.inc("request_ignored")
            await self.request_queue.ack(request)
            return
        except ResourceInvalid:
            raise
        except Exception as exc:
            await self._on_request_error(request, exc, ctx)
            return

        if isinstance(response, Request):
            if not response.parent_task_id:
                response.parent_task_id = request.parent_task_id
            if not await self.request_queue.put(response):
                self.stats.inc("request_filtered")
            await self.request_queue.ack(request)
            return

        if response.status in self._retry_http_codes():
            await self._on_request_error(request, None, ctx, response=response)
            return

        callback = resolve_callable(self.spider, request.callback, self.spider.parse)
        try:
            results = await call_spider_callback(
                callback,
                response,
                ctx,
                request.cb_kwargs,
            )
            await self._consume(results, ctx, parent_task_id=request.parent_task_id)
        except CloseSpider as exc:
            await self.request_queue.ack(request)
            self.request_close(exc.reason)
            return
        except IgnoreRequest:
            self.stats.inc("request_ignored")
            await self.request_queue.ack(request)
            return
        except Exception:
            await self.request_queue.ack(request)
            raise

        self.stats.inc("request_succeeded")
        await self.request_queue.ack(request)

    def _retry_http_codes(self) -> set[int]:
        return {int(code) for code in self.settings.request_retry_http_codes}

    def _retry_delay(self, response: Response | None) -> float:
        if response is not None:
            retry_after = response.headers.get("Retry-After")
            if retry_after is None:
                retry_after = response.headers.get("retry-after")
            if retry_after is not None:
                try:
                    seconds = float(retry_after)
                except ValueError:
                    seconds = 0.0
                if seconds > 0:
                    return min(seconds, self.settings.request_retry_max_delay)
        return max(0.0, self.settings.request_retry_delay)

    async def _on_request_error(
        self,
        request: Request,
        exc: BaseException | None,
        ctx: SpiderContext,
        response: Response | None = None,
    ) -> None:
        max_retries = (
            request.max_retries
            if request.max_retries is not None
            else self.settings.request_max_retries
        )
        max_retries = max(0, max_retries)
        failure = RequestFailure(
            request=request,
            exception=exc,
            response=response,
            retries=request.retries,
        )
        errback = resolve_callable(self.spider, request.errback, None)
        if errback is not None:
            try:
                results = await call_spider_callback(errback, failure, ctx)
                await self._consume(results, ctx, parent_task_id=request.parent_task_id)
            except CloseSpider as exc:
                await self.request_queue.ack(request)
                self.request_close(exc.reason)
                return
            except Exception:
                logger.exception("errback failed for request %s", request.url)
            self.stats.inc("request_failed")
            await self.request_queue.ack(request)
            return

        if request.retries < max_retries:
            request.retries += 1
            delay = self._retry_delay(response)
            request.priority += self.settings.request_retry_priority_adjust
            self.stats.inc("request_retried")
            await self.request_queue.nack(request, delay=delay)
            logger.warning(
                "request %s failed (retry %s/%s): %s",
                request.url,
                request.retries,
                max_retries,
                exc or f"HTTP {response.status}" if response is not None else exc,
            )
            return

        self.stats.inc("request_failed")
        await self.request_queue.ack(request)
        logger.error(
            "request %s dropped after %s retries",
            request.url,
            request.retries,
            exc_info=(type(exc), exc, exc.__traceback__) if exc is not None else None,
        )

    async def _consume(
        self,
        results: Any,
        ctx: SpiderContext,
        parent_task_id: str | None = None,
    ) -> None:
        async for result in iter_results(results):
            if isinstance(result, Task):
                if not result.id:
                    result.id = Task().id
                await self.task_queue.put(result)
            elif isinstance(result, Request):
                if not result.parent_task_id:
                    result.parent_task_id = parent_task_id
                if not await self.request_queue.put(result):
                    self.stats.inc("request_filtered")
            elif isinstance(result, Item):
                await self._process_item(result, ctx)
            elif isinstance(result, Mapping):
                await self._process_item(Item.from_mapping(result), ctx)
            else:
                raise TypeError(
                    f"unsupported spider result {type(result)!r}; "
                    "expected Task, Request, Item or mapping"
                )

    async def _process_item(self, item: Item, ctx: SpiderContext) -> None:
        assert self.pipelines is not None
        item = await self.pipelines.process_item(item, self.spider, ctx)
        self.stats.inc("item_scraped")
        await self.signals.send(ITEM_SCRAPED, item=item, worker_id=self.worker_id)

    async def _download_with_middlewares(
        self,
        request: Request,
        ctx: SpiderContext,
    ) -> Response | Request:
        assert self.downloader_middlewares is not None
        assert self.downloader is not None

        result = await self.downloader_middlewares.process_request(request, self.spider, ctx)
        if isinstance(result, Response):
            response = result
        elif isinstance(result, Request):
            return result
        else:
            try:
                response = await self.downloader.fetch(request)
            except Exception as exc:
                recovered = await self.downloader_middlewares.process_exception(
                    request, exc, self.spider, ctx
                )
                if isinstance(recovered, (Response, Request)):
                    response = recovered
                else:
                    raise

        if isinstance(response, Request):
            return response
        processed = await self.downloader_middlewares.process_response(
            request, response, self.spider, ctx
        )
        return processed
