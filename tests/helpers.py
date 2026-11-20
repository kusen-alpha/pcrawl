from __future__ import annotations

from pcrawl import (
    AsyncSpider,
    CloseSpider,
    Item,
    MultiProcessSpider,
    Request,
    Response,
    Spider,
    Task,
)


class ProcessItemSpider(MultiProcessSpider):
    name = "process-items"
    count = 12

    def start_tasks(self):
        for index in range(self.count):
            yield Task(payload={"index": index})

    def start_requests(self, task, ctx):
        yield Item(index=task.payload["index"], worker=ctx.worker_id)


class ProcessLifecycleSpider(MultiProcessSpider):
    name = "process-lifecycle"
    count = 4
    events: list[str] = []

    def open_spider(self):
        type(self).events.append("open")

    def start_tasks(self):
        for index in range(self.count):
            yield Task(payload={"index": index})

    def start_requests(self, task, ctx):
        yield Item(index=task.payload["index"])

    def close_spider(self, reason):
        type(self).events.append(f"close:{reason}")


class ProcessCloseSpider(MultiProcessSpider):
    name = "process-close"
    count = 6
    closed_reason: str | None = None

    def start_tasks(self):
        for index in range(self.count):
            yield Task(payload={"index": index})

    def start_requests(self, task, ctx):
        yield Item(index=task.payload["index"])
        raise CloseSpider("enough")

    def close_spider(self, reason):
        type(self).closed_reason = reason


class CallbackSpider(Spider):
    name = "callback"
    count = 6

    def start_requests(self):
        for index in range(self.count):
            yield Request(url=f"http://local/{index}", callback="parse_item")

    def parse_item(self, response: Response):
        yield Item(url=response.url, source=response.meta.get("source", "seed"))


class ProcessHttpSpider(MultiProcessSpider):
    name = "process-http"
    start_urls: tuple[str, ...] = ()
    count = 2

    def parse(self, response: Response):
        yield Item(done=True)


class AsyncCallbackSpider(Spider):
    name = "async-callback"
    count = 3

    def start_tasks(self):
        for index in range(self.count):
            yield Task(payload={"index": index})

    async def start_requests(self, task, ctx):
        await _yield_one()
        yield Item(index=task.payload["index"])


async def _yield_one():
    return None


class SessionSpider(Spider):
    name = "session"
    count = 5

    async def start_resources(self, resources):
        resources.create_pool("account", "account", config={"max_slots": 1})
        await resources.import_resources(
            "account",
            [
                {"id": "a1", "username": "u1"},
                {"id": "a2", "username": "u2"},
            ],
            import_validate="on_import",
        )

    def start_tasks(self):
        for index in range(self.count):
            yield Task(payload={"index": index})

    async def open_session(self, session):
        session.user["account"] = (await session.acquire("account")).payload

    def start_requests(self, task, ctx):
        ctx.get_lease("account").mark_used()
        yield Item(index=task.payload["index"], account=ctx.user["account"]["username"])

    def close_session(self, session, reason):
        session.user.clear()


class ProcessSessionSpider(SessionSpider):
    execution_mode = "process"


class AsyncSessionSpider(SessionSpider):
    execution_mode = "async"


class AutoSessionSpider(AsyncSpider):
    name = "auto-session"
    count = 4
    seen_modes: list[str] = []

    async def start_resources(self, resources, ctx=None):
        if ctx is not None:
            type(self).seen_modes.append(ctx.mode)
        resources.create_pool("account", "account", config={"max_slots": 1})
        await resources.import_resources(
            "account",
            [
                {"id": "auto-a1", "username": "auto-u1"},
                {"id": "auto-a2", "username": "auto-u2"},
            ],
            import_validate="on_import",
        )

    def start_tasks(self):
        for index in range(self.count):
            yield Task(payload={"index": index})

    async def open_session(self, session):
        session.user["account"] = (await session.acquire("account")).payload

    def start_requests(self, task, ctx):
        ctx.get_lease("account").mark_used()
        yield Item(index=task.payload["index"], account=ctx.user["account"]["username"])

    def close_session(self, session, reason):
        session.user.clear()


class AutoInlineSessionSpider(AutoSessionSpider):
    execution_mode = "inline"


class AutoThreadSessionSpider(AutoSessionSpider):
    execution_mode = "thread"


class AutoProcessSessionSpider(AutoSessionSpider):
    execution_mode = "process"
