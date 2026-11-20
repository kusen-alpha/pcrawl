from __future__ import annotations

from pcrawl import AsyncSpider, Item, Settings, Task
from purser import AcquireRequest


class AccountSessionSpider(AsyncSpider):
    """Generic session-resource example.

    pcrawl only knows that ``account`` is a lease. How the account payload is
    used (HTTP login, browser context, API client, ...) stays in this spider.
    ``start_resources`` uses the native purser provisioning API; the engine
    picks an embedded purser for inline/async and a LocalScheduler for
    thread/process.
    """

    name = "account-session"
    settings = Settings(concurrency=2, resource_wait_timeout=5)

    async def start_resources(self, resources):
        resources.create_pool("account", "account", config={"max_slots": 1})
        await resources.import_resources(
            "account",
            [
                {"id": "acct-1", "username": "user-1", "password": "secret-1"},
                {"id": "acct-2", "username": "user-2", "password": "secret-2"},
            ],
            import_validate="on_import",
        )

    def start_tasks(self):
        for index in range(6):
            yield Task(payload={"query": f"item-{index}"})

    async def open_session(self, session):
        account = await session.acquire(
            "account",
            AcquireRequest(use_limit=10),
        )
        session.user["account"] = account.payload

    async def start_requests(self, task, ctx):
        account = ctx.user["account"]
        ctx.get_lease("account").mark_used()
        yield Item(
            query=task.payload["query"],
            account=account.get("username") or account.get("id"),
            session_id=ctx.session.id,
        )

    def close_session(self, session, reason):
        session.user.clear()
