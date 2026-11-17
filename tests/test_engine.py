from __future__ import annotations

import asyncio
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

from pcrawl import (
    AsyncSpider,
    CloseSpider,
    ConfigError,
    CrawlerEngine,
    Item,
    LeaseHandle,
    MemoryRequestQueue,
    MemoryTaskQueue,
    MultiProcessSpider,
    PoolSpec,
    RedisRequestQueue,
    RedisTaskQueue,
    Request,
    RequestFailure,
    ResourceOutcome,
    ResourcePlan,
    ResourceSetup,
    ResourceSpec,
    ResourceInvalid,
    Response,
    Settings,
    Spider,
    Task,
    WorkerSession,
)
from pcrawl.resources import ResourceBackend, ResourceManager
from tests.helpers import (
    AsyncCallbackSpider,
    AsyncSessionSpider,
    AutoInlineSessionSpider,
    AutoProcessSessionSpider,
    AutoSessionSpider,
    AutoThreadSessionSpider,
    ProcessCloseSpider,
    ProcessHttpSpider,
    ProcessItemSpider,
    ProcessLifecycleSpider,
    ProcessSessionSpider,
)


class _ItemSpider(Spider):
    name = "items"
    count = 5

    def start_tasks(self):
        for index in range(self.count):
            yield Task(payload={"index": index})

    def start_requests(self, task, ctx):
        yield Item(index=task.payload["index"])


class _AsyncPresetItemSpider(AsyncSpider):
    name = "async-preset-items"
    count = 4

    def start_tasks(self):
        for index in range(self.count):
            yield Task(payload={"index": index})

    def start_requests(self, task, ctx):
        yield Item(index=task.payload["index"])


class _ProcessItemSpider(_ItemSpider):
    execution_mode = "process"


def _fast_settings(**changes) -> Settings:
    values = {
        "idle_timeout": 0.15,
        "claim_poll_interval": 0.01,
        "shutdown_timeout": 10,
        "task_retry_delay": 0.01,
        "request_retry_delay": 0.01,
        "request_retry_max_delay": 1.0,
    }
    values.update(changes)
    return Settings(**values)


@pytest.mark.parametrize(
    "mode,extra",
    [
        ("inline", {}),
        ("async", {"concurrency": 3}),
        ("thread", {"workers": 3}),
    ],
)
def test_local_modes_process_all_tasks(mode, extra):
    spider = type("S", (_ItemSpider,), {"count": 18, "execution_mode": mode})
    stats = CrawlerEngine(spider, _fast_settings(**extra)).run()
    assert stats["task_received"] == 18
    assert stats["task_succeeded"] == 18
    assert stats["item_scraped"] == 18


def test_async_callback_is_supported():
    stats = CrawlerEngine(AsyncCallbackSpider, _fast_settings()).run()
    assert stats["item_scraped"] == AsyncCallbackSpider.count


def test_async_spider_preset_applies_async_mode():
    settings = Settings(
        idle_timeout=0.15,
        claim_poll_interval=0.01,
        shutdown_timeout=10,
    )
    engine = CrawlerEngine(_AsyncPresetItemSpider, settings)
    assert engine.mode == "async"
    stats = engine.run()
    assert stats["item_scraped"] == _AsyncPresetItemSpider.count


def test_spider_lifecycle_hooks_wrap_the_run():
    events = []

    class LifecycleSpider(Spider):
        name = "lifecycle"

        async def open_spider(self):
            events.append("open")

        def start_tasks(self):
            events.append("seed")
            yield Task(payload={"n": 1})

        async def start_requests(self, task, ctx):
            events.append("task")
            yield Item(n=task.payload["n"])

        def close_spider(self, reason):
            events.append(f"close:{reason}")

    stats = CrawlerEngine(LifecycleSpider, _fast_settings()).run()

    assert stats["item_scraped"] == 1
    assert events == ["open", "seed", "task", "close:finished"]


def test_spider_close_hook_receives_error_reason():
    events = []

    class FailingSpider(Spider):
        name = "lifecycle-error"

        def open_spider(self):
            events.append("open")

        def start_tasks(self):
            events.append("seed")
            raise RuntimeError("boom")

        async def close_spider(self, reason):
            events.append(f"close:{reason}")

    with pytest.raises(RuntimeError, match="boom"):
        CrawlerEngine(FailingSpider, _fast_settings()).run()

    assert events == ["open", "seed", "close:error"]


def test_close_spider_from_callback_stops_the_run_and_reports_reason():
    class ClosingSpider(Spider):
        name = "closing"
        closed_reason: str | None = None

        def start_requests(self):
            for index in range(5):
                yield Request(
                    url=f"http://example.com/{index}",
                    callback=self.parse,
                )

        def parse(self, response):
            raise CloseSpider("enough")

        def close_spider(self, reason):
            self.closed_reason = reason

    spider = ClosingSpider()
    stats = CrawlerEngine(spider, _fast_settings()).run()

    assert spider.closed_reason == "enough"
    assert stats["request_received"] == 1
    assert stats.get("request_succeeded", 0) == 0


def test_close_spider_reason_crosses_process_workers():
    ProcessCloseSpider.closed_reason = None
    stats = CrawlerEngine(ProcessCloseSpider, _fast_settings(workers=2)).run()

    assert ProcessCloseSpider.closed_reason == "enough"
    assert stats["item_scraped"] >= 1
    assert stats["task_received"] < ProcessCloseSpider.count


def test_process_mode_runs_spider_hooks_once_in_parent():
    ProcessLifecycleSpider.events.clear()
    stats = CrawlerEngine(ProcessLifecycleSpider, _fast_settings(workers=2)).run()

    assert stats["item_scraped"] == ProcessLifecycleSpider.count
    assert ProcessLifecycleSpider.events == ["open", "close:finished"]


def test_process_mode_uses_shared_memory_queue_and_aggregates_stats():
    settings = _fast_settings(workers=2)
    stats = CrawlerEngine(ProcessItemSpider, settings).run()
    assert stats["task_received"] == ProcessItemSpider.count
    assert stats["task_succeeded"] == ProcessItemSpider.count
    assert stats["item_scraped"] == ProcessItemSpider.count


def test_process_mode_uses_shared_request_queue():
    with _Server() as server:
        ProcessHttpSpider.start_urls = (server.base_url + "/",)
        try:
            stats = CrawlerEngine(ProcessHttpSpider, _fast_settings(workers=2)).run()
        finally:
            ProcessHttpSpider.start_urls = ()

    assert stats["request_received"] == 1
    assert stats["request_succeeded"] == 1
    assert stats["item_scraped"] == 1


def test_process_mode_with_local_scheduler_session_resources():
    settings = _fast_settings(workers=2).replace(
        purser_mode="local",
        purser_pools={"account": {"kind": "account", "max_slots": 1}},
        resource_wait_timeout=8,
    )
    stats = CrawlerEngine(ProcessSessionSpider, settings).run()
    assert stats["task_received"] == ProcessSessionSpider.count
    assert stats["item_scraped"] == ProcessSessionSpider.count


def test_embedded_purser_session_resources():
    settings = _fast_settings(
        concurrency=2,
        purser_mode="embedded",
        purser_pools={"account": {"kind": "account", "max_slots": 1}},
        resource_wait_timeout=5,
    )
    stats = CrawlerEngine(AsyncSessionSpider, settings).run()
    assert stats["item_scraped"] == AsyncSessionSpider.count


def test_resource_setup_normalizes_pool_declarations():
    setup = ResourceSetup(
        pools={
            "account": {
                "kind": "account",
                "max_slots": 1,
                "seed": {"id": "a1", "username": "u1"},
                "import_options": {"import_validate": "on_import"},
            }
        }
    )

    pool = setup.pools["account"]
    assert isinstance(pool, PoolSpec)
    assert pool.kind == "account"
    assert pool.config == {"max_slots": 1}
    assert pool.seed == ({"id": "a1", "username": "u1"},)
    assert ResourceSetup.from_value({"pools": setup.pools}) == setup


@pytest.mark.parametrize(
    "spider_cls,extra,expected_mode",
    [
        (AutoInlineSessionSpider, {}, "embedded"),
        (AutoSessionSpider, {"concurrency": 2}, "embedded"),
        (AutoThreadSessionSpider, {"workers": 2}, "local"),
        (AutoProcessSessionSpider, {"workers": 2}, "local"),
    ],
)
def test_start_resources_auto_selects_backend_per_mode(
    spider_cls,
    extra,
    expected_mode,
):
    AutoSessionSpider.seen_modes.clear()
    engine = CrawlerEngine(spider_cls, _fast_settings(**extra))
    stats = engine.run()

    assert stats["task_received"] == AutoSessionSpider.count
    assert stats["item_scraped"] == AutoSessionSpider.count
    assert engine.settings.purser_mode == expected_mode
    assert AutoSessionSpider.seen_modes == [engine.mode]


def test_auto_purser_mode_is_off_without_pools():
    engine = CrawlerEngine(AsyncSpider, _fast_settings())
    engine.run()
    assert engine.settings.purser_mode == "off"


def test_process_mode_runs_without_resources():
    engine = CrawlerEngine(_ProcessItemSpider, _fast_settings(workers=2))
    stats = engine.run()

    assert engine.settings.purser_mode == "off"
    assert engine.scheduler is None
    assert stats["task_received"] == _ProcessItemSpider.count
    assert stats["task_succeeded"] == _ProcessItemSpider.count
    assert stats["item_scraped"] == _ProcessItemSpider.count


def test_auto_resource_plan_requires_pool_declaration():
    class DeclaredOnlySpider(AsyncSpider):
        resources = {"account": {"pool": "account"}}

    with pytest.raises(ConfigError, match="no resource pools"):
        CrawlerEngine(DeclaredOnlySpider, _fast_settings()).run()


class _FakeResource:
    def __init__(self, tags=("t1",)):
        self.tags = frozenset(tags)


class _FakeLease:
    def __init__(self, token: str, resource_id: str, payload: dict):
        self.token = token
        self.resource_id = resource_id
        self.payload = payload
        self.resource = _FakeResource()
        self.used = 0
        self.outcome = None


class _FakeBackend(ResourceBackend):
    def __init__(self):
        self.released: list[tuple[str, str, int]] = []
        self.renewed = 0

    async def acquire(self, pool, *, name, tags, slots, use_limit, timeout, consumer, **kwargs):
        raw = _FakeLease(f"tok-{name}", f"rid-{name}", {"username": name})
        return LeaseHandle(
            name=name,
            pool=pool,
            raw=raw,
            backend=self,
            use_limit=use_limit,
        )

    async def release(self, handle, *, outcome, used):
        self.released.append((handle.pool, outcome.value, used))
        return True

    async def renew(self, handle):
        self.renewed += 1
        return True


def test_session_marks_usage_and_releases_outcome():
    async def scenario():
        backend = _FakeBackend()
        manager = ResourceManager(backend, Settings())
        session = WorkerSession(
            manager=manager,
            plan=ResourcePlan.from_value({"account": ResourceSpec("account", use_limit=2)}),
            worker_id="w1",
        )
        handle = await session.acquire("account")
        assert handle.payload["username"] == "account"
        assert handle.tags == frozenset({"t1"})
        handle.mark_used()
        handle.mark_used()
        with pytest.raises(ResourceInvalid):
            handle.mark_used(1)
        session.invalidate(ResourceOutcome.BANNED)
        await session.release_all()
        assert backend.released == [("account", "banned", 3)]

    asyncio.run(scenario())


def test_session_heartbeat_renews_session_leases():
    async def scenario():
        backend = _FakeBackend()
        manager = ResourceManager(backend, Settings())
        session = WorkerSession(
            manager=manager,
            plan=ResourcePlan.from_value({"account": ResourceSpec("account")}),
            worker_id="w1",
        )
        await session.acquire("account")
        session.start_heartbeat(0.01)
        await asyncio.sleep(0.05)
        await session.release_all()
        assert backend.renewed >= 1

    asyncio.run(scenario())


def test_memory_queue_route_filter_and_fifo():
    async def scenario():
        queue = MemoryTaskQueue(poll_interval=0.001)
        await queue.put(Task(route="b", priority=0, payload={"n": 1}))
        await queue.put(Task(route="a", priority=1, payload={"n": 2}))
        await queue.put(Task(route="a", priority=5, payload={"n": 3}))
        first = await queue.claim(routes={"a"}, timeout=0.01)
        second = await queue.claim(routes={"a"}, timeout=0.01)
        third = await queue.claim(timeout=0.01)
        assert first.payload["n"] == 2
        assert second.payload["n"] == 3
        assert third.route == "b"

    asyncio.run(scenario())


def test_memory_queue_priority_order_is_opt_in():
    async def scenario():
        queue = MemoryTaskQueue(poll_interval=0.001, order="priority")
        await queue.put(Task(route="b", priority=0, payload={"n": 1}))
        await queue.put(Task(route="a", priority=1, payload={"n": 2}))
        await queue.put(Task(route="a", priority=5, payload={"n": 3}))
        first = await queue.claim(routes={"a"}, timeout=0.01)
        second = await queue.claim(routes={"a"}, timeout=0.01)
        third = await queue.claim(timeout=0.01)
        assert first.payload["n"] == 3
        assert second.payload["n"] == 2
        assert third.route == "b"

    asyncio.run(scenario())


def test_memory_request_queue_dedup_and_dont_filter():
    async def scenario():
        queue = MemoryRequestQueue(poll_interval=0.001)
        first = Request("http://example.com/")
        duplicate = Request("http://example.com/")
        bypass = Request("http://example.com/", dont_filter=True)
        assert await queue.put(first) is True
        assert await queue.put(duplicate) is False
        assert await queue.put(bypass) is True
        assert await queue.size() == 2
        assert (await queue.claim(timeout=0.01)).id == first.id
        assert (await queue.claim(timeout=0.01)).id == bypass.id

    asyncio.run(scenario())


def test_memory_request_queue_priority_and_routes():
    async def scenario():
        queue = MemoryRequestQueue(poll_interval=0.001, order="priority")
        low = Request("http://example.com/low", route="a", priority=0)
        high = Request("http://example.com/high", route="a", priority=5)
        other = Request("http://example.com/other", route="b", priority=9)
        await queue.put(low)
        await queue.put(high)
        await queue.put(other)
        assert (await queue.claim(routes={"a"}, timeout=0.01)).id == high.id
        assert (await queue.claim(routes={"a"}, timeout=0.01)).id == low.id
        assert (await queue.claim(timeout=0.01)).id == other.id

    asyncio.run(scenario())


def test_request_serialization_preserves_bytes_and_callable_name():
    spider = _ItemSpider()
    request = Request(
        "http://example.com/",
        method="POST",
        body=b"\x00\xff",
        callback=spider.parse,
        resource_specs={"proxy": {"pool": "proxy"}},
        parent_task_id="task-1",
    )
    restored = Request.from_dict(request.to_dict())
    assert restored.body == b"\x00\xff"
    assert restored.callback == "parse"
    assert restored.resource_specs == {"proxy": {"pool": "proxy"}}
    assert restored.parent_task_id == "task-1"
    assert restored.id == request.id

    legacy = request.to_dict()
    legacy["resources"] = legacy.pop("resource_specs")
    assert Request.from_dict(legacy).resource_specs == request.resource_specs


def test_redis_queue_visibility_timeout_requeues_crashed_task(monkeypatch):
    pytest.importorskip("fakeredis")
    import redis.asyncio as redis_async
    from fakeredis.aioredis import FakeRedis

    fake = FakeRedis(decode_responses=True)
    monkeypatch.setattr(
        redis_async.Redis,
        "from_url",
        classmethod(lambda cls, url, decode_responses=True: fake),
    )

    async def scenario():
        settings = Settings(
            queue_backend="redis",
            visibility_timeout=0.01,
            claim_poll_interval=0.001,
        )
        queue = RedisTaskQueue(settings)
        await queue.put(Task(route="a", payload={"n": 1}))
        first = await queue.claim(timeout=0.05)
        assert first is not None
        await asyncio.sleep(0.03)
        second = await queue.claim(timeout=0.05)
        assert second is not None and second.id == first.id
        await queue.ack(second)
        assert await queue.size() == 0
        await queue.close()

    asyncio.run(scenario())


def test_redis_request_queue_visibility_and_dedup(monkeypatch):
    pytest.importorskip("fakeredis")
    import redis.asyncio as redis_async
    from fakeredis.aioredis import FakeRedis

    fake = FakeRedis(decode_responses=True)
    monkeypatch.setattr(
        redis_async.Redis,
        "from_url",
        classmethod(lambda cls, url, decode_responses=True: fake),
    )

    async def scenario():
        settings = Settings(
            queue_backend="redis",
            visibility_timeout=0.01,
            claim_poll_interval=0.001,
        )
        queue = RedisRequestQueue(settings)
        request = Request("http://example.com/")
        assert await queue.put(request) is True
        assert await queue.put(Request("http://example.com/")) is False
        first = await queue.claim(timeout=0.05)
        assert first is not None
        await asyncio.sleep(0.03)
        second = await queue.claim(timeout=0.05)
        assert second is not None and second.id == first.id
        await queue.ack(second)
        assert await queue.size() == 0
        await queue.close()

    asyncio.run(scenario())


class _Server:
    def __init__(self):
        self.requests: list[dict] = []
        outer = self

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                outer.requests.append({"path": self.path, "headers": dict(self.headers)})
                body = b'{"next": "/follow"}' if self.path == "/" else b'{"done": true}'
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, *args):
                return None

        self.httpd = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.thread = threading.Thread(target=self.httpd.serve_forever, daemon=True)

    @property
    def base_url(self) -> str:
        host, port = self.httpd.server_address[:2]
        return f"http://{host}:{port}"

    def __enter__(self):
        self.thread.start()
        return self

    def __exit__(self, *exc):
        self.httpd.shutdown()
        self.httpd.server_close()
        self.thread.join(timeout=5)


class _FlakyServer:
    def __init__(self, failures: int = 1, status: int = 503):
        self.remaining = failures
        self.status = status
        self.requests = 0
        outer = self

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                outer.requests += 1
                if outer.remaining > 0:
                    outer.remaining -= 1
                    code = outer.status
                    body = b"busy"
                else:
                    code = 200
                    body = b'{"ok": true}'
                self.send_response(code)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, *args):
                return None

        self.httpd = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.thread = threading.Thread(target=self.httpd.serve_forever, daemon=True)

    @property
    def base_url(self) -> str:
        host, port = self.httpd.server_address[:2]
        return f"http://{host}:{port}"

    def __enter__(self):
        self.thread.start()
        return self

    def __exit__(self, *exc):
        self.httpd.shutdown()
        self.httpd.server_close()
        self.thread.join(timeout=5)


class _HtmlServer:
    def __init__(self, html: bytes):
        self.html = html
        outer = self

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                body = outer.html if self.path == "/" else b"ok"
                self.send_response(200)
                self.send_header("Content-Type", "text/html")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, *args):
                return None

        self.httpd = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.thread = threading.Thread(target=self.httpd.serve_forever, daemon=True)

    @property
    def base_url(self) -> str:
        host, port = self.httpd.server_address[:2]
        return f"http://{host}:{port}"

    def __enter__(self):
        self.thread.start()
        return self

    def __exit__(self, *exc):
        self.httpd.shutdown()
        self.httpd.server_close()
        self.thread.join(timeout=5)


class _HeaderMiddleware:
    def __init__(self):
        self.requests = []
        self.responses = 0

    def process_request(self, request, spider):
        request.headers["X-Pcrawl"] = "yes"
        self.requests.append(request.url)

    def process_response(self, request, response, spider):
        self.responses += 1
        return response


class _CollectPipeline:
    def __init__(self):
        self.items = []

    def process_item(self, item, spider):
        self.items.append(dict(item))
        return item


def test_http_request_callback_middleware_and_pipeline():
    middleware = _HeaderMiddleware()
    pipeline = _CollectPipeline()
    with _Server() as server:
        class HttpSpider(Spider):
            name = "http"
            start_urls = (server.base_url + "/",)
            settings = Settings()

            def parse(self, response: Response):
                data = response.json()
                if "next" in data:
                    yield Item(path=data["next"])
                    yield response.follow(data["next"], self.parse)
                else:
                    yield Item(done=True)

        settings = _fast_settings(
            downloader_middlewares=(middleware,),
            item_pipelines=(pipeline,),
        )
        stats = CrawlerEngine(HttpSpider, settings).run()

    assert stats["request_received"] == 2
    assert stats["item_scraped"] == 2
    assert len(pipeline.items) == 2
    assert middleware.responses == 2
    assert len(server.requests) == 2
    assert server.requests[0]["headers"].get("X-Pcrawl") == "yes"


def test_start_urls_flow_through_base_task_and_request_queues():
    with _Server() as server:
        class UrlSpider(Spider):
            name = "url-spider"
            start_urls = (server.base_url + "/a", server.base_url + "/b")

            def parse(self, response: Response):
                yield Item(url=response.request.url)

        stats = CrawlerEngine(UrlSpider, _fast_settings()).run()

    assert stats["task_received"] == 2
    assert stats["task_succeeded"] == 2
    assert stats["request_received"] == 2
    assert stats["item_scraped"] == 2


def test_base_start_tasks_and_start_requests_translate_start_urls():
    class UrlSpider(Spider):
        name = "url-unit"
        start_urls = ("http://example.com/a", "http://example.com/b")

        def parse(self, response: Response):
            return None

    spider = UrlSpider()
    tasks = list(spider.start_tasks())

    assert [task.payload for task in tasks] == [
        {"url": "http://example.com/a"},
        {"url": "http://example.com/b"},
    ]

    requests = list(spider.start_requests(tasks[0]))

    assert [request.url for request in requests] == ["http://example.com/a"]
    assert requests[0].callback == spider.parse


def test_start_tasks_feed_queue_and_start_requests_receives_task():
    with _Server() as server:
        class TaskSpider(Spider):
            name = "task-requests"
            seen: list[bool] = []

            def start_tasks(self):
                yield Task(
                    payload={"from_external_source": True, "url": server.base_url + "/"},
                )

            def start_requests(self, task, ctx):
                self.seen.append(task.payload["from_external_source"])
                yield Request(url=task.payload["url"], callback=self.parse)

            def parse(self, response: Response):
                yield Item(done=True)

        stats = CrawlerEngine(TaskSpider, _fast_settings()).run()

    assert stats["item_scraped"] == 1
    assert TaskSpider.seen == [True]


def test_start_tasks_rejects_requests():
    class BadSpider(Spider):
        name = "bad-seed"

        def start_tasks(self):
            yield Request("http://example.com/")

    with pytest.raises(TypeError, match="Task objects"):
        CrawlerEngine(BadSpider, _fast_settings()).run()


def test_task_retry_then_success():
    class RetrySpider(Spider):
        name = "retry"

        def __init__(self):
            super().__init__()
            self.attempts = 0

        def start_tasks(self):
            yield Task(payload={"n": 1})

        def start_requests(self, task, ctx):
            self.attempts += 1
            if self.attempts == 1:
                raise RuntimeError("transient")
            yield Item(attempts=self.attempts)

    settings = _fast_settings(task_max_retries=1)
    stats = CrawlerEngine(RetrySpider, settings).run()
    assert stats["task_retried"] == 1
    assert stats["task_succeeded"] == 1
    assert stats["item_scraped"] == 1


def test_request_retry_on_http_status_then_success():
    with _FlakyServer(failures=1, status=503) as server:
        class RetryRequestSpider(Spider):
            name = "request-retry"
            start_urls = (server.base_url + "/",)
            settings = Settings(
                request_max_retries=2,
                request_retry_delay=0.01,
                request_retry_http_codes=(429, 500, 502, 503, 504),
            )

            def parse(self, response: Response):
                yield Item(ok=response.json()["ok"])

        stats = CrawlerEngine(RetryRequestSpider, _fast_settings()).run()

    assert server.requests == 2
    assert stats["request_retried"] == 1
    assert stats["request_succeeded"] == 1
    assert stats["item_scraped"] == 1


def test_request_errback_receives_failure():
    with _FlakyServer(failures=10, status=500) as server:
        collected: list[dict] = []

        class ErrbackSpider(Spider):
            name = "request-errback"
            settings = Settings(
                request_max_retries=0,
                request_retry_http_codes=(500,),
            )

            def start_requests(self):
                yield Request(
                    server.base_url + "/",
                    callback=self.parse,
                    errback=self.on_error,
                )

            def parse(self, response: Response):
                yield Item(unexpected=True)

            def on_error(self, failure: RequestFailure):
                collected.append(
                    {
                        "status": failure.response.status,
                        "retries": failure.retries,
                        "url": failure.request.url,
                    }
                )
                yield Item(error=failure.response.status)

        stats = CrawlerEngine(
            ErrbackSpider,
            _fast_settings(
                item_pipelines=(_CollectPipeline(),),
                request_retry_http_codes=(500,),
            ),
        ).run()

    assert collected == [{"status": 500, "retries": 0, "url": server.base_url + "/"}]
    assert stats["request_failed"] == 1
    assert stats["item_scraped"] == 1


def test_request_dedup_filters_duplicate_follow_links():
    html = b'<html><body><a href="/same">a</a><a href="/same">b</a></body></html>'
    with _HtmlServer(html) as server:
        class DedupSpider(Spider):
            name = "dedup"
            start_urls = (server.base_url + "/",)

            def parse(self, response: Response):
                if response.url.endswith("/"):
                    yield from response.follow_all(
                        response.css("a::attr(href)"),
                        self.parse,
                    )
                else:
                    yield Item(done=True)

        stats = CrawlerEngine(DedupSpider, _fast_settings()).run()

    assert stats["request_received"] == 2
    assert stats["request_filtered"] == 1
    assert stats["item_scraped"] == 1


def test_response_selector_urljoin_and_follow():
    request = Request("https://example.com/")
    html = (
        b"<html><head><base href='https://example.com/a/'></head><body>"
        b"<a class='next' href='b'>B</a></body></html>"
    )
    response = Response("https://example.com/", 200, {}, html, request)

    assert response.css("a.next::attr(href)").get() == "b"
    assert response.xpath("//a/@href").get() == "b"
    assert response.urljoin("b") == "https://example.com/a/b"
    followed = response.follow(response.css("a.next::attr(href)").get(), "parse")
    assert followed.url == "https://example.com/a/b"
    assert followed.callback == "parse"


def test_cb_kwargs_are_passed_to_callback():
    with _Server() as server:
        pipeline = _CollectPipeline()

        class CbSpider(Spider):
            name = "cb-kwargs"
            start_urls = (server.base_url + "/",)

            def start_requests(self):
                yield Request(
                    url=server.base_url + "/",
                    callback="parse_tag",
                    cb_kwargs={"tag": "x"},
                )

            def parse_tag(self, response: Response, tag: str):
                yield Item(tag=tag)

        stats = CrawlerEngine(
            CbSpider,
            _fast_settings(item_pipelines=(pipeline,)),
        ).run()

    assert stats["item_scraped"] == 1
    assert pipeline.items == [{"tag": "x"}]


def test_spider_can_yield_direct_item_and_follow_up_task():
    class FollowSpider(Spider):
        name = "follow"

        def start_tasks(self):
            yield Task(payload={"n": 1})

        def start_requests(self, task, ctx):
            n = task.payload["n"]
            yield Item(n=n)
            if n < 3:
                yield Task(payload={"n": n + 1})

    stats = CrawlerEngine(FollowSpider, _fast_settings()).run()
    assert stats["item_scraped"] == 3
    assert stats["task_received"] == 3
